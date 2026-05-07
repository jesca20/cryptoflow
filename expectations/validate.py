"""
Data quality validation for CryptoFlow using Great Expectations 1.x.
Checks run against the latest batch in raw.coins:
  - Null checks on critical columns
  - Price anomaly detection (price must be > 0)
  - Data freshness (latest row within the last 20 minutes)
"""

import sys
import psycopg2
import pandas as pd
from datetime import datetime, timezone, timedelta

import great_expectations as gx
from great_expectations.expectations import (
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToBeBetween,
    ExpectTableRowCountToBeBetween,
)

sys.path.insert(0, "/Users/jesic/Desktop/Cryptoflow")
from db.config import DB_CONFIG

FRESHNESS_MINUTES = 20


def fetch_latest_batch() -> pd.DataFrame:
    """Pull the most recent ingestion batch from raw.coins."""
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql(
        """
        SELECT id, symbol, name, current_price, market_cap,
               total_volume, price_change_24h, ingested_at
        FROM raw.coins
        WHERE ingested_at = (SELECT MAX(ingested_at) FROM raw.coins)
        """,
        conn,
    )
    conn.close()
    return df


def check_freshness(df: pd.DataFrame) -> bool:
    """Return True if the latest row is within FRESHNESS_MINUTES."""
    if df.empty:
        return False
    latest = pd.to_datetime(df["ingested_at"]).max()
    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=timezone.utc)
    age = datetime.now(timezone.utc) - latest
    fresh = age <= timedelta(minutes=FRESHNESS_MINUTES)
    status = "PASS" if fresh else "FAIL"
    print(f"  [{status}] Freshness check — latest row is {int(age.total_seconds() // 60)}m old "
          f"(threshold: {FRESHNESS_MINUTES}m)")
    return fresh


def run_gx_validations(df: pd.DataFrame) -> bool:
    """Run GX 1.x expectations against the dataframe."""
    context = gx.get_context(mode="ephemeral")

    ds = context.data_sources.add_pandas("cryptoflow_pandas")
    asset = ds.add_dataframe_asset("coins_asset")
    batch_def = asset.add_batch_definition_whole_dataframe("latest_batch")

    suite = context.suites.add(gx.ExpectationSuite(name="coins_suite"))

    # Null checks
    for col in ["id", "symbol", "name", "current_price", "ingested_at"]:
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column=col))

    # Price must be positive
    suite.add_expectation(
        ExpectColumnValuesToBeBetween(
            column="current_price",
            min_value=0,
            strict_min=True,
        )
    )

    # Row count sanity check (expect 1–100 coins per batch)
    suite.add_expectation(
        ExpectTableRowCountToBeBetween(min_value=1, max_value=100)
    )

    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    results = batch.validate(suite)

    all_passed = True
    for result in results.results:
        expectation_type = result.expectation_config.type
        column = result.expectation_config.column if hasattr(result.expectation_config, "column") else "table"
        passed = result.success
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {expectation_type} — {column}")
        if not passed:
            all_passed = False

    return all_passed


def run():
    print("\n── CryptoFlow Data Quality Validation ──")
    print(f"  Run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    df = fetch_latest_batch()
    print(f"  Rows in latest batch: {len(df)}\n")

    if df.empty:
        print("  [FAIL] No data found in raw.coins. Aborting.")
        sys.exit(1)

    freshness_ok = check_freshness(df)
    gx_ok = run_gx_validations(df)

    print()
    if freshness_ok and gx_ok:
        print("  ✓ All checks passed.")
    else:
        print("  ✗ One or more checks failed.")
        sys.exit(1)


if __name__ == "__main__":
    run()
