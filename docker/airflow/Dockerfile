# Image commune Airflow + Spark
# Airflow et les noeuds Spark utilisent la même image
# pour avoir la même version de Python et de PySpark.
FROM apache/airflow:2.10.5-python3.11

# Java est nécessaire pour Spark
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Librairies Python (versions compatibles avec Airflow 2.10.5)
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"

# Spark fourni par PySpark
ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=python3

# Connecteurs Spark : MinIO (s3a) et Elasticsearch
RUN cd "${SPARK_HOME}/jars" \
 && curl -sSfLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -sSfLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
 && curl -sSfLO https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.15.3/elasticsearch-spark-30_2.12-8.15.3.jar

# Code du projet
ENV PYTHONPATH=/opt/scipulse/src
