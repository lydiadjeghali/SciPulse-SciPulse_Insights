from __future__ import annotations

import io
import json
import time
from datetime import datetime, timezone

import pandas as pd
import requests

from scipulse import config, monitoring, storage
from scipulse.ingestion.arxiv import latest_snapshot_key

API = "https://api.openalex.org/works"
BATCH_SIZE = 100                                    # nombre maximal de valeurs dans un filtre OpenAlex
STATE_KEY = "_state/requested_ids.json"
# Champs demandés : ce dont les modules Silver et Spark ont besoin
FIELDS = ("id,doi,ids,title,publication_year,publication_date,type,cited_by_count,counts_by_year,"
          "primary_topic,referenced_works,locations,updated_date")


class QuotaReached(Exception):
    """OpenAlex refuse les requêtes (quota atteint ou accès refusé) """


def _params(extra: dict) -> dict:
    params = {"select": FIELDS, "per-page": 100, **extra}
    if config.OPENALEX_API_KEY:
        params["api_key"] = config.OPENALEX_API_KEY
    if config.OPENALEX_MAILTO:
        params["mailto"] = config.OPENALEX_MAILTO
    return params


def fetch_batch(arxiv_ids: list[str], session: requests.Session) -> tuple[list[dict], int]:
    """Toutes les pages de résultats pour un lot d'identifiants. Renvoie (works, nombre de requêtes)."""
    doi_filter = "doi:" + "|".join(f"10.48550/arxiv.{i}" for i in arxiv_ids)
    works, cursor, calls = [], "*", 0
    while cursor:
        page = _get_page({"filter": doi_filter, "cursor": cursor}, session)
        calls += 1
        works += page["results"]
        cursor = page["meta"].get("next_cursor") if page["results"] else None
    return works, calls


def _get_page(extra: dict, session: requests.Session, attempts: int = 5) -> dict:
    """Une page de l'API, avec nouvelles tentatives sur toute erreur réseau ou serveur."""
    for attempt in range(attempts):
        try:
            resp = session.get(API, params=_params(extra), timeout=60)
            if resp.status_code in (401, 403, 429):  # quota atteint ou accès refusé
                raise QuotaReached(f"HTTP {resp.status_code} : {resp.text[:300]}")
            resp.raise_for_status()
            return resp.json()
        except QuotaReached:
            raise
        except (requests.RequestException, ValueError):  # coupure, réponse tronquée ou illisible, 5xx
            if attempt == attempts - 1:
                raise
            time.sleep(2 ** attempt * 5)


def priority_ids() -> list[str]:
    """Identifiants arXiv à demander : d'abord ceux cités sur HN, puis un ordre aléatoire fixe.

    L'aléatoire évite de ne garder que les articles récents, qui ont encore peu de citations."""
    snapshot = latest_snapshot_key().split("/")[0]
    arxiv = storage.read_parquet_prefix(config.SILVER["arxiv"], f"{snapshot}/",
                                        columns=["arxiv_id", "submitted_date"])
    body = storage.s3_client().get_object(Bucket=config.SILVER["hn"], Key="hn_stories.parquet")["Body"].read()
    hn_ids = set(pd.read_parquet(io.BytesIO(body), columns=["arxiv_id"])["arxiv_id"].dropna())
    arxiv = arxiv.sample(frac=1, random_state=42)          # ordre aléatoire reproductible
    arxiv["on_hn"] = arxiv["arxiv_id"].isin(hn_ids)
    arxiv = arxiv.sort_values("on_hn", ascending=False, kind="stable")
    return arxiv["arxiv_id"].tolist()


def _read_state() -> set[str]:
    try:
        body = storage.s3_client().get_object(Bucket=config.BRONZE["citations"], Key=STATE_KEY)["Body"]
        return set(json.loads(body.read()))
    except Exception:
        return set()


def _write_state(done: set[str]) -> None:
    storage.s3_client().put_object(Bucket=config.BRONZE["citations"], Key=STATE_KEY,
                                   Body=json.dumps(sorted(done)).encode())


def ingest(max_requests: int | None = None, limit: int = 0) -> dict:
    """Un passage d'ingestion : demande les articles pas encore traités, dans la limite du quota."""
    start, now = time.time(), datetime.now(timezone.utc)
    if max_requests is None:
        max_requests = 9000 if config.OPENALEX_API_KEY else 900
    done = _read_state()
    budget = max(config.CITATIONS_MAX_ARTICLES - len(done), 0)    # limite totale d'articles
    todo = [i for i in priority_ids() if i not in done][:budget]
    if limit:
        todo = todo[:limit]

    session = requests.Session()
    stats = {"requetes": 0, "lots": 0, "ids_demandes": 0, "works": 0, "statut": "termine"}
    run = f"run={now:%Y-%m-%dT%H%M%S}"
    try:
        for i in range(0, len(todo), BATCH_SIZE):
            if stats["requetes"] >= max_requests:
                stats["statut"] = "limite_requetes"
                break
            batch = todo[i:i + BATCH_SIZE]
            works, calls = fetch_batch(batch, session)
            stats["requetes"] += calls
            key = f"{run}/batch-{stats['lots']:05d}.jsonl"
            stats["works"] += storage.put_jsonl(config.BRONZE["citations"], key, works)
            done.update(batch)
            stats["lots"] += 1
            stats["ids_demandes"] += len(batch)
            if stats["lots"] % 50 == 0:              # sauvegarde régulière de l'avancement
                _write_state(done)
    except QuotaReached as exc:
        stats["statut"] = "quota_atteint"
        stats["message"] = str(exc)
        print(f"Arrêt propre : {exc}")
    finally:
        _write_state(done)

    stats["restants"] = len(todo) - stats["ids_demandes"]
    monitoring.emit("ingestion", "bronze", "citations", records=stats["works"],
                    requested=stats["ids_demandes"], api_calls=stats["requetes"],
                    remaining=stats["restants"], stop_reason=stats["statut"],
                    duration_s=round(time.time() - start, 1))
    return stats
