"""DAG Silver citations :

Déclenché automatiquement quand citations_bronze écrit de nouvelles données (Dataset Airflow).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator

CITATIONS_BRONZE = Dataset("s3://citations-raw")
CITATIONS_SILVER = Dataset("s3://citations-clean")

default_args = {"owner": "scipulse", "retries": 1, "retry_delay": timedelta(minutes=5)}


def _clean_silver(**_):
    from scipulse.cleaning import citations

    return citations.run()


with DAG(
    dag_id="citations_silver",
    description="Citations : citations-raw (Bronze) -> citations-clean (Silver)",
    start_date=datetime(2026, 1, 1),
    schedule=[CITATIONS_BRONZE],
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["scipulse", "silver", "citations", "module2"],
) as dag:
    PythonOperator(task_id="clean_silver", python_callable=_clean_silver,
                   execution_timeout=timedelta(hours=2), outlets=[CITATIONS_SILVER])
