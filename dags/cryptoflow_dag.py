from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

# Make the project root importable
sys.path.insert(0, "/Users/jesic/Desktop/Cryptoflow")

default_args = {
    "owner": "cryptoflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="cryptoflow_pipeline",
    description="Fetch crypto data, load to Postgres, run dbt",
    schedule="*/10 * * * *",   # every 10 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["cryptoflow"],
) as dag:

    def ingest():
        from ingestion.fetch import run
        run()

    t1_ingest = PythonOperator(
        task_id="ingest_from_coingecko",
        python_callable=ingest,
    )

    t2_dbt = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "source /Users/jesic/Desktop/Cryptoflow/venv/bin/activate && "
            "cd /Users/jesic/Desktop/Cryptoflow/dbt/cryptoflow && "
            "dbt run --profiles-dir /Users/jesic/.dbt"
        ),
    )

    t1_ingest >> t2_dbt
