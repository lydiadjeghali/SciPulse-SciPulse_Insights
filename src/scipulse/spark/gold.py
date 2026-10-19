"""Couche Gold finale : écriture du DataFrame enrichi dans l'index arxiv-papers-enriched.
- mapping : es/mappings/arxiv-papers-enriched.json (créé avant l'écriture, mode strict) ;
- identifiant de document = arxiv_id : réécrire remplace, sans doublon ;
"""
from __future__ import annotations

import json
from urllib.parse import urlparse

from elasticsearch import Elasticsearch
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from scipulse import config

MAPPING_FILE = config.HOME / "es" / "mappings" / "arxiv-papers-enriched.json"


def gold_columns() -> list[str]:
    """Colonnes de l'index (celles du mapping)."""
    return list(json.loads(MAPPING_FILE.read_text(encoding="utf-8"))["mappings"]["properties"])


def create_index(es: Elasticsearch, recreate: bool = True) -> None:
    """(Re)crée l'index avec son mapping."""
    if recreate and es.indices.exists(index=config.INDEX_ENRICHED):
        es.indices.delete(index=config.INDEX_ENRICHED)
    if not es.indices.exists(index=config.INDEX_ENRICHED):
        body = json.loads(MAPPING_FILE.read_text(encoding="utf-8"))
        es.indices.create(index=config.INDEX_ENRICHED, settings=body["settings"], mappings=body["mappings"])


def write_gold(df: DataFrame) -> int:
    """Écrit le DataFrame enrichi dans Elasticsearch ; renvoie le nombre de documents de l'index."""
    es = Elasticsearch(config.ES_HOST, request_timeout=120)
    create_index(es)
    es.indices.put_settings(index=config.INDEX_ENRICHED, settings={"refresh_interval": "-1"})
    host = urlparse(config.ES_HOST)
    # Chaque ligne est sérialisée en JSON par Spark, puis envoyée telle quelle par le connecteur
    # (es.input.json) : il ne sait pas convertir les tableaux d'objets imbriqués comme counts_by_year.
    # Avec du JSON en entrée, le connecteur attend un RDD de chaînes (et non un DataFrame).
    documents = (df.withColumn("indexed_at", F.current_timestamp())
                 .select(F.to_json(F.struct(*gold_columns())).alias("document"))
                 .rdd.map(lambda row: ("key", row.document)))
    es_conf = {
        "es.resource": config.INDEX_ENRICHED,
        "es.nodes": host.hostname,
        "es.port": str(host.port or 9200),
        "es.nodes.wan.only": "true",           # un seul nœud joint par son nom Docker
        "es.index.auto.create": "false",       # le mapping est créé avant l'écriture
        "es.input.json": "true",
        "es.mapping.id": "arxiv_id",
        "es.batch.size.entries": "2000",
    }
    try:
        documents.saveAsNewAPIHadoopFile(
            path="-",
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable",
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
            conf=es_conf,
        )
    finally:
        es.indices.put_settings(index=config.INDEX_ENRICHED, settings={"refresh_interval": "1s"})
        es.indices.refresh(index=config.INDEX_ENRICHED)
    return es.count(index=config.INDEX_ENRICHED)["count"]
