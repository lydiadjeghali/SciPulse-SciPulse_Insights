"""DAG batch ArXiv : dump Kaggle → MinIO arxiv-raw (Bronze).

Le dump Kaggle est mis à jour chaque semaine, d'où une exécution hebdomadaire.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {"owner": "scipulse", "retries": 2, "retry_delay": timedelta(minutes=5)}


def _ingest_bronze(ds: str, **_):
    from scipulse.ingestion import arxiv

    return arxiv.ingest(snapshot=ds)


with DAG(
    dag_id="arxiv_batch",
    description="ArXiv : dump Kaggle → MinIO arxiv-raw (Bronze)",
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["scipulse", "bronze", "arxiv"],
) as dag:
    ingest_bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=_ingest_bronze,
        execution_timeout=timedelta(hours=3),
    )
