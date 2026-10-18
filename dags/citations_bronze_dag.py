"""DAG Bronze citations : OpenAlex → MinIO citations-raw.
Exécuté chaque jour pour consommer le quota journalier d'OpenAlex :
chaque passage reprend là où le précédent s'est arrêté.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator

# Nouvelles données de citations : déclenche le DAG citations_silver
CITATIONS_BRONZE = Dataset("s3://citations-raw")

default_args = {"owner": "scipulse", "retries": 1, "retry_delay": timedelta(minutes=10)}


def _ingest(**_):
    from scipulse.ingestion import openalex

    return openalex.ingest()


with DAG(
    dag_id="citations_bronze",
    description="Citations : API OpenAlex → MinIO citations-raw (Bronze), chaque jour",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["scipulse", "bronze", "citations", "module2"],
) as dag:
    PythonOperator(task_id="ingest_openalex", python_callable=_ingest,
                   execution_timeout=timedelta(hours=4), outlets=[CITATIONS_BRONZE])
