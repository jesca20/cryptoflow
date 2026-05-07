import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from db.config import DB_CONFIG

API_URL = "https://api.coingecko.com/api/v3/coins/markets"
PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 50,   
    "page": 1,
    "sparkline": False
}

def fetch_coins():
    print("→ Fetching from CoinGecko...")
    response = requests.get(API_URL, params=PARAMS, timeout=10)
    response.raise_for_status()
    data = response.json()
    print(f"  ✓ {len(data)} coins received")
    return data

def parse(data):
    df = pd.DataFrame(data)
    df = df[[
        "id", "symbol", "name",
        "current_price", "market_cap",
        "total_volume", "price_change_percentage_24h"
    ]].copy()

    # Enforce types — prevent bad data from breaking the insert
    df["current_price"]  = pd.to_numeric(df["current_price"],  errors="coerce")
    df["market_cap"]     = pd.to_numeric(df["market_cap"],     errors="coerce").astype("Int64")
    df["total_volume"]   = pd.to_numeric(df["total_volume"],   errors="coerce").astype("Int64")
    df["price_change_24h"] = pd.to_numeric(df["price_change_percentage_24h"], errors="coerce")
    df.drop(columns=["price_change_percentage_24h"], inplace=True)

    df["ingested_at"] = datetime.now(timezone.utc)
    return df

def load(df, conn):
    cur = conn.cursor()
    def to_py(v):
        try:
            if pd.isna(v):
                return None
        except (TypeError, ValueError):
            pass
        return v.item() if hasattr(v, "item") else v

    rows = [tuple(to_py(v) for v in row) for row in df.itertuples(index=False)]
    execute_values(cur, """
        INSERT INTO raw.coins (
            id, symbol, name,
            current_price, market_cap, total_volume,
            price_change_24h, ingested_at
        ) VALUES %s
    """, rows)
    conn.commit()
    cur.close()

def log_run(conn, rows_fetched, status, error=None):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO raw.ingestion_log (run_at, rows_fetched, status, error_message)
        VALUES (%s, %s, %s, %s)
    """, (datetime.now(), rows_fetched, status, error))
    conn.commit()
    cur.close()

def run():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        data  = fetch_coins()
        df    = parse(data)
        load(df, conn)
        log_run(conn, len(df), "success")
        print(f"  ✓ {len(df)} rows inserted into raw.coins")
    except Exception as e:
        log_run(conn, 0, "failed", str(e))
        print(f"  ✗ Ingestion failed: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    run()