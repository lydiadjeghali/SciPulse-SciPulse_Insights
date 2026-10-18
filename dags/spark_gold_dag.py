"""DAG Spark : jointure des trois sources Silver.

Déclenché quand la Silver ArXiv ou la Silver des citations est mise à jour (Datasets Airflow).
Le job est soumis au cluster Spark par le SparkSubmitOperator (connexion spark_default).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

ARXIV_SILVER = Dataset("s3://arxiv-clean")
CITATIONS_SILVER = Dataset("s3://citations-clean")

default_args = {"owner": "scipulse", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="spark_gold",
    description="Spark : jointure arxiv-clean + hn-clean + citations-clean",
    start_date=datetime(2026, 1, 1),
    schedule=(ARXIV_SILVER | CITATIONS_SILVER),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["scipulse", "gold", "spark", "module2"],
) as dag:
    SparkSubmitOperator(
        task_id="join_sources",
        application="/opt/scipulse/spark_jobs/join_sources.py",
        conn_id="spark_default",
        name="scipulse-jointure",
        driver_memory="1g",
        executor_memory="1g",
        executor_cores=2,
        execution_timeout=timedelta(hours=1),
    )
