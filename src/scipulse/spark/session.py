"""Session Spark du projet : cluster standalone + accès MinIO (s3a)."""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

from scipulse import config


def get_spark(app_name: str = "scipulse") -> SparkSession:
    """Session connectée au master Spark, qui lit et écrit dans MinIO via s3a://."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
        # Mémoire calibrée pour le worker (1,5 Go, 2 cœurs) et le scheduler Airflow
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "2")
        .config("spark.driver.memory", "1536m")
        .config("spark.sql.shuffle.partitions", "16")
        # Délais longs : le driver tourne dans le scheduler Airflow, parfois très chargé
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")
        # Accès à MinIO 
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", config.MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Dates écrites par pandas en nanosecondes : lues comme entiers, converties au chargement
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
    )
    spark = builder.getOrCreate()
    # Points de reprise dans le volume partagé par le driver et le worker
    spark.sparkContext.setCheckpointDir("file:///opt/scipulse/tmp/spark-checkpoints")
    return spark
