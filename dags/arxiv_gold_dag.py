"""DAG Gold ArXiv (module 1) : arxiv-clean (Silver) → index Elasticsearch arxiv-papers.

Déclenché automatiquement quand la tâche clean_silver du DAG arxiv_batch
met à jour la Silver ArXiv (Dataset Airflow), ou à la main.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator

ARXIV_SILVER = Dataset("s3://arxiv-clean")

default_args = {"owner": "scipulse", "retries": 1, "retry_delay": timedelta(minutes=5)}


def _index_gold(**_):
    from scipulse.indexing import arxiv

    return arxiv.run()


with DAG(
    dag_id="arxiv_gold",
    description="ArXiv : arxiv-clean (Silver) → index Elasticsearch arxiv-papers (Gold)",
    start_date=datetime(2026, 1, 1),
    schedule=[ARXIV_SILVER],
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["scipulse", "gold", "arxiv", "elasticsearch"],
) as dag:
    PythonOperator(task_id="index_arxiv_papers", python_callable=_index_gold,
                   execution_timeout=timedelta(hours=3))
