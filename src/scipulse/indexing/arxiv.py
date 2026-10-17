"""Gold (module 1) : indexe les articles ArXiv Silver dans l'index Elasticsearch arxiv-papers.

- le mapping est défini dans es/mappings/arxiv-papers.json ;
- chaque article a pour identifiant son arxiv_id : réindexer remplace, sans doublon ;
- les fichiers Parquet sont indexés un par un, chacun dans un processus enfant ;
- pendant le chargement, le rafraîchissement de l'index est coupé (indexation plus rapide).
"""
from __future__ import annotations

import io
import json
import time
from datetime import datetime, timezone

import pandas as pd
from elasticsearch import Elasticsearch, helpers

from scipulse import config, monitoring, storage
from scipulse.ingestion.arxiv import latest_snapshot_key
from scipulse.isolation import run_isolated

MAPPING_FILE = config.HOME / "es" / "mappings" / "arxiv-papers.json"
DATE_FIELDS = ["submitted_date", "last_version_date", "update_date"]


def es_client() -> Elasticsearch:
    return Elasticsearch(config.ES_HOST, request_timeout=120, retry_on_timeout=True, max_retries=5)


def create_index(es: Elasticsearch, recreate: bool = False) -> bool:
    """Crée l'index avec son mapping (le supprime d'abord si recreate=True)."""
    if recreate and es.indices.exists(index=config.INDEX_ARXIV):
        es.indices.delete(index=config.INDEX_ARXIV)
    if es.indices.exists(index=config.INDEX_ARXIV):
        return False
    body = json.loads(MAPPING_FILE.read_text(encoding="utf-8"))
    es.indices.create(index=config.INDEX_ARXIV, settings=body["settings"], mappings=body["mappings"])
    return True


def _clean_value(value):
    """Valeurs compatibles JSON : tableaux numpy → listes, NaN / NA → null."""
    if hasattr(value, "tolist"):                   # tableaux numpy et entiers numpy
        return value.tolist()
    if isinstance(value, list):
        return value
    return None if pd.isna(value) else value


def to_documents(df: pd.DataFrame, indexed_at: str):
    """Transforme les lignes Silver en documents Elasticsearch."""
    df = df.copy()
    for field in DATE_FIELDS:
        df[field] = df[field].dt.strftime("%Y-%m-%d")
    df["has_doi"] = df["doi"].notna()
    for record in df.to_dict(orient="records"):
        source = {k: _clean_value(v) for k, v in record.items()}
        source["indexed_at"] = indexed_at
        yield {"_index": config.INDEX_ARXIV, "_id": source["arxiv_id"], "_source": source}


def _index_file(key: str, indexed_at: str) -> dict:
    """Indexe un fichier Parquet Silver (exécuté dans un processus enfant)."""
    body = storage.s3_client().get_object(Bucket=config.SILVER["arxiv"], Key=key)["Body"].read()
    df = pd.read_parquet(io.BytesIO(body))
    ok, errors = helpers.bulk(es_client(), to_documents(df, indexed_at), chunk_size=2000,
                              raise_on_error=False, stats_only=True)
    return {"lignes": len(df), "indexes": ok, "erreurs": errors}


def run(max_files: int = 0, recreate: bool = False) -> dict:
    """Silver → Gold : indexe tous les fichiers Parquet du dernier snapshot ArXiv."""
    start = time.time()
    es = es_client()
    create_index(es, recreate=recreate)
    snapshot = latest_snapshot_key().split("/")[0]
    keys = [k for k in storage.list_keys(config.SILVER["arxiv"], f"{snapshot}/") if k.endswith(".parquet")]
    if max_files:
        keys = keys[:max_files]

    indexed_at = datetime.now(timezone.utc).isoformat()
    stats = {"fichiers": 0, "lignes": 0, "indexes": 0, "erreurs": 0}
    es.indices.put_settings(index=config.INDEX_ARXIV, settings={"refresh_interval": "-1"})
    try:
        for key in keys:
            result = run_isolated(_index_file, key, indexed_at)
            for name, value in result.items():
                stats[name] += value
            stats["fichiers"] += 1
            print(f"{key} : {result['indexes']} documents indexés, {result['erreurs']} erreur(s)")
    finally:
        es.indices.put_settings(index=config.INDEX_ARXIV, settings={"refresh_interval": "1s"})
        es.indices.refresh(index=config.INDEX_ARXIV)

    total = es.count(index=config.INDEX_ARXIV)["count"]
    monitoring.emit("indexing", "gold", "arxiv", status="success" if not stats["erreurs"] else "failed",
                    records=stats["indexes"], records_in=stats["lignes"], errors=stats["erreurs"],
                    index_total=total, duration_s=round(time.time() - start, 1))
    return {**stats, "total_index": total}
