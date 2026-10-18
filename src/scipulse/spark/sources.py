"""Chargement des trois couches Silver depuis MinIO dans des DataFrames Spark."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from scipulse.ingestion.arxiv import latest_snapshot_key

DATE_COLUMNS = {
    "arxiv": ["submitted_date", "last_version_date", "update_date"],
    "hn": ["created_at", "observed_at"],
    "citations": ["publication_date"],
}


def _fix_dates(df: DataFrame, columns: list[str]) -> DataFrame:
    """Dates écrites par pandas en nanosecondes (lues comme entiers) → timestamp Spark."""
    for name in columns:
        if isinstance(df.schema[name].dataType, LongType):
            df = df.withColumn(name, F.timestamp_seconds(F.col(name) / F.lit(1_000_000_000)))
    return df


def load_arxiv(spark: SparkSession) -> DataFrame:
    snapshot = latest_snapshot_key().split("/")[0]
    df = spark.read.parquet(f"s3a://arxiv-clean/{snapshot}/")
    return _fix_dates(df, DATE_COLUMNS["arxiv"])


def load_hn(spark: SparkSession) -> DataFrame:
    return _fix_dates(spark.read.parquet("s3a://hn-clean/hn_stories.parquet"), DATE_COLUMNS["hn"])


def load_citations(spark: SparkSession) -> DataFrame:
    return _fix_dates(spark.read.parquet("s3a://citations-clean/openalex/"), DATE_COLUMNS["citations"])
