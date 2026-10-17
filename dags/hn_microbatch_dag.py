"""DAGs Hacker News.

- hn_microbatch : appelle l'API toutes les 5 minutes (Bronze).
- hn_backfill   : récupère une fois l'historique des liens vers arxiv.org (Bronze).
- hn_silver     : nettoie et valide toutes les données HN chaque heure (Silver).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {"owner": "scipulse", "retries": 3, "retry_delay": timedelta(minutes=1)}


def _poll_live(**_):
    from scipulse.ingestion import hn

    return hn.poll_live()


def _backfill(**_):
    from scipulse.ingestion import hn

    return hn.backfill_arxiv_stories()


def _clean_silver(**_):
    from scipulse.cleaning import hn

    return hn.run()


with DAG(
    dag_id="hn_microbatch",
    description="Hacker News : API toutes les 5 min → MinIO hn-raw/live (Bronze)",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=5),
    default_args=default_args,
    tags=["scipulse", "bronze", "hn", "micro-batch"],
) as dag_live:
    PythonOperator(task_id="poll_hn_api", python_callable=_poll_live,
                   execution_timeout=timedelta(minutes=4))

with DAG(
    dag_id="hn_backfill",
    description="Hacker News : historique des liens arxiv.org (Algolia) → hn-raw/backfill (Bronze)",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["scipulse", "bronze", "hn"],
) as dag_backfill:
    PythonOperator(task_id="backfill_arxiv_stories", python_callable=_backfill,
                   execution_timeout=timedelta(hours=1))

with DAG(
    dag_id="hn_silver",
    description="Hacker News : hn-raw (Bronze) → hn-clean (Silver), chaque heure",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["scipulse", "silver", "hn"],
) as dag_silver:
    PythonOperator(task_id="clean_silver", python_callable=_clean_silver,
                   execution_timeout=timedelta(minutes=30))
