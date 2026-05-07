"""
Local scheduler using APScheduler.
Runs the full CryptoFlow pipeline every 10 minutes:
  1. Ingest from CoinGecko → raw.coins
  2. Run dbt → staging.stg_coins, mart.mart_top_movers

Note: An Airflow DAG (dags/cryptoflow_dag.py) is also included
for orchestration design reference.
"""

import subprocess
import sys
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

# Add project root to path
sys.path.insert(0, "/Users/jesic/Desktop/Cryptoflow")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

DBT_PROJECT_DIR = "/Users/jesic/Desktop/Cryptoflow/dbt/cryptoflow"
DBT_PROFILES_DIR = "/Users/jesic/.dbt"
VENV_DBT = "/Users/jesic/Desktop/Cryptoflow/venv/bin/dbt"


def run_pipeline():
    log.info("=" * 50)
    log.info("Pipeline run starting")

    # Step 1: Ingest
    try:
        from ingestion.fetch import run
        run()
    except Exception as e:
        log.error(f"Ingestion failed: {e}")
        return

    # Step 2: dbt
    try:
        result = subprocess.run(
            [VENV_DBT, "run",
             "--project-dir", DBT_PROJECT_DIR,
             "--profiles-dir", DBT_PROFILES_DIR],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            log.info("dbt run succeeded")
        else:
            log.error(f"dbt run failed:\n{result.stderr}")
    except Exception as e:
        log.error(f"dbt step failed: {e}")
        return

    log.info("Pipeline run complete")


if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_pipeline,
        trigger=IntervalTrigger(minutes=10),
        id="cryptoflow_pipeline",
        name="CryptoFlow full pipeline",
        next_run_time=__import__("datetime").datetime.now(),  # run immediately on start
    )

    log.info("Scheduler started — pipeline runs every 10 minutes. Ctrl+C to stop.")
    try:
        scheduler.start()
    except KeyboardInterrupt:
        log.info("Scheduler stopped.")
