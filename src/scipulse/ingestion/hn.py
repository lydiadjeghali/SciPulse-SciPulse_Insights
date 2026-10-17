"""Bronze Hacker News : réponses des API écrites telles quelles dans MinIO.

- live/     : API officielle (Firebase), appelée toutes les 5 minutes par Airflow.
- backfill/ : API de recherche Algolia, pour l'historique des liens vers arxiv.org.
              Sans cet historique, le polling seul donnerait très peu de liens ArXiv.

La date d'ingestion et le flux sont dans le nom du fichier : le contenu n'est pas modifié.
"""
from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests

from scipulse import config, monitoring, storage

STATE_KEY = "_state/live_last_max_id.json"
SESSION = requests.Session()


def _get(url: str, params: dict | None = None, retries: int = 4):
    """Appel GET avec nouvelles tentatives en cas d'erreur réseau."""
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, params=params, timeout=20)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)


def _now() -> datetime:
    return datetime.now(timezone.utc)


# --- Micro-batch (temps réel) ---------------------------------------------

def _read_last_id() -> int:
    """Dernier identifiant de story déjà ingéré (0 au premier passage)."""
    try:
        body = storage.s3_client().get_object(Bucket=config.BRONZE["hn"], Key=STATE_KEY)["Body"]
        return int(json.loads(body.read())["last_max_id"])
    except Exception:
        return 0


def _fetch_items(ids: list[int]) -> list[dict]:
    """Récupère les stories en parallèle (les identifiants supprimés renvoient null)."""
    with ThreadPoolExecutor(max_workers=16) as pool:
        items = pool.map(lambda i: _get(f"{config.HN_API}/item/{i}.json"), ids)
    return [item for item in items if item]


def poll_live(max_new: int = 500) -> dict:
    """Un micro-batch : nouvelles stories depuis le dernier passage + top stories."""
    start, now = time.time(), _now()
    last_id = _read_last_id()

    new_ids = [i for i in _get(f"{config.HN_API}/newstories.json") if i > last_id][:max_new]
    # Les top stories sont relues à chaque passage : leur score évolue
    top_ids = _get(f"{config.HN_API}/topstories.json")[:200]

    total = 0
    for feed, ids in (("newstories", new_ids), ("topstories", top_ids)):
        key = f"live/date={now:%Y-%m-%d}/hn_{now:%Y%m%dT%H%M%S}_{feed}.jsonl"
        total += storage.put_jsonl(config.BRONZE["hn"], key, _fetch_items(ids))

    if new_ids:
        storage.s3_client().put_object(
            Bucket=config.BRONZE["hn"], Key=STATE_KEY,
            Body=json.dumps({"last_max_id": max(new_ids), "updated_at": now.isoformat()}).encode(),
        )
    monitoring.emit("ingestion", "bronze", "hn", records=total, feed="live",
                    new_items=len(new_ids), duration_s=round(time.time() - start, 1))
    return {"records": total, "new_items": len(new_ids)}


# --- Historique (backfill) --------------------------------------------------

def _search_window(start_ts: int, end_ts: int) -> list[dict]:
    """Stories liant arxiv.org sur une période (coupée en deux si plus de 1000 résultats)."""
    params = {
        "query": "arxiv.org",
        "restrictSearchableAttributes": "url",
        "tags": "story",
        "hitsPerPage": 1000,
        "numericFilters": f"created_at_i>={start_ts},created_at_i<{end_ts}",
    }
    data = _get(f"{config.HN_ALGOLIA_API}/search_by_date", params)
    if data["nbHits"] > len(data["hits"]) and end_ts - start_ts > 3600:
        mid = (start_ts + end_ts) // 2
        return _search_window(start_ts, mid) + _search_window(mid, end_ts)
    return data["hits"]


def backfill_arxiv_stories(from_year: int = 2007) -> dict:
    """Récupère mois par mois toutes les stories HN qui pointent vers arxiv.org."""
    start, now = time.time(), _now()
    total, year, month = 0, from_year, 1
    while (year, month) <= (now.year, now.month):
        lo = datetime(year, month, 1, tzinfo=timezone.utc)
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
        hi = datetime(year, month, 1, tzinfo=timezone.utc)
        hits = _search_window(int(lo.timestamp()), int(hi.timestamp()))
        if hits:
            key = f"backfill/month={lo:%Y-%m}/hn_arxiv_{lo:%Y%m}_ingested_{now:%Y%m%dT%H%M%S}.jsonl"
            total += storage.put_jsonl(config.BRONZE["hn"], key, hits)
    monitoring.emit("ingestion", "bronze", "hn", records=total, feed="backfill",
                    duration_s=round(time.time() - start, 1))
    return {"records": total}
