"""Silver Hacker News : harmonise les flux live et backfill, écrit hn-clean/hn_stories.parquet.

Règles de nettoyage (issues de l'analyse de qualité des données Bronze) :
- un seul schéma pour les deux API (Firebase : id/score/by, Algolia : objectID/points/author) ;
- uniquement les stories (les offres d'emploi « job » sont exclues), ni supprimées ni « dead » ;
- une ligne par story : on garde la lecture la plus récente (score le plus à jour) ;
- URL : schéma et domaine en minuscules ; domaine et identifiant arXiv extraits de l'URL ;
- dates en UTC ;
- stories sans URL (Ask HN) conservées avec url = null.
"""
from __future__ import annotations

import re
import time
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd

from scipulse import config, monitoring, storage
from scipulse.cleaning.common import arxiv_id_from_url, clean_text
from scipulse.quality import validation

OUTPUT_KEY = "hn_stories.parquet"
COLUMNS = ["hn_id", "title", "url", "domain", "author", "score", "num_comments", "created_at",
           "story_kind", "arxiv_id", "has_arxiv_link", "source", "feed", "observed_at"]


def _ingested_at(key: str) -> datetime:
    """Date d'ingestion lue dans le nom du fichier Bronze."""
    stamp = re.findall(r"(\d{8}T\d{6})", key)[-1]
    return datetime.strptime(stamp, "%Y%m%dT%H%M%S")


def _live_rows() -> list[dict]:
    rows = []
    for key in storage.list_keys(config.BRONZE["hn"], "live/"):
        feed = re.search(r"_(newstories|topstories)\.jsonl$", key).group(1)
        for item in storage.iter_jsonl(config.BRONZE["hn"], key):
            if item.get("type") != "story" or item.get("dead") or item.get("deleted"):
                continue
            rows.append({
                "hn_id": item["id"], "title": item.get("title"), "url": item.get("url"),
                "author": item.get("by"), "score": item.get("score"),
                "num_comments": item.get("descendants"),
                "created_at": pd.to_datetime(item["time"], unit="s"),
                "source": "live", "feed": feed, "observed_at": _ingested_at(key),
            })
    return rows


def _backfill_rows() -> list[dict]:
    rows = []
    for key in storage.list_keys(config.BRONZE["hn"], "backfill/"):
        for hit in storage.iter_jsonl(config.BRONZE["hn"], key):
            if "story" not in hit.get("_tags", []):
                continue
            rows.append({
                "hn_id": int(hit["objectID"]), "title": hit.get("title"), "url": hit.get("url"),
                "author": hit.get("author"), "score": hit.get("points"),
                "num_comments": hit.get("num_comments"),
                "created_at": pd.to_datetime(hit["created_at_i"], unit="s"),
                "source": "backfill", "feed": "algolia", "observed_at": _ingested_at(key),
            })
    return rows


def _normalize_url(url: str | None) -> str | None:
    """URL sans espaces, schéma et domaine en minuscules (ex. « Https://ArXiv.org » trouvé en Bronze)."""
    if not isinstance(url, str) or not url.strip():
        return None
    parts = urlparse(url.strip())
    return parts._replace(scheme=parts.scheme.lower(), netloc=parts.netloc.lower()).geturl()


def _story_kind(title: str) -> str:
    lower = title.lower()
    if lower.startswith("show hn"):
        return "show_hn"
    if lower.startswith("ask hn"):
        return "ask_hn"
    return "story"


def clean(rows: list[dict]) -> pd.DataFrame:
    """Nettoie et déduplique les stories HN (une ligne par story)."""
    df = pd.DataFrame(rows)
    df["title"] = df["title"].map(clean_text)
    df = df[df["title"].notna()]
    df["url"] = df["url"].map(_normalize_url)
    df["domain"] = df["url"].map(
        lambda u: urlparse(u).netloc.lower().removeprefix("www.") if u else None
    )
    df["score"] = df["score"].fillna(0).astype("int64")
    df["num_comments"] = df["num_comments"].fillna(0).astype("int64")
    df["story_kind"] = df["title"].map(_story_kind)
    df["arxiv_id"] = df["url"].map(arxiv_id_from_url)
    df["has_arxiv_link"] = df["arxiv_id"].notna()

    # Une ligne par story : la lecture la plus récente
    df = df.sort_values(["hn_id", "observed_at"]).drop_duplicates("hn_id", keep="last")
    return df[COLUMNS].reset_index(drop=True)


def run() -> dict:
    """Bronze → Silver pour Hacker News (reconstruit tout le fichier Silver)."""
    start = time.time()
    rows = _live_rows() + _backfill_rows()
    df = clean(rows)

    result = validation.validate(df, "hn_silver", run_name=f"hn_{datetime.utcnow():%Y%m%dT%H%M%S}")
    if not result["success"]:
        raise ValueError(f"Données HN refusées par Great Expectations : {result['failed']}")

    storage.put_parquet(config.SILVER["hn"], OUTPUT_KEY, df)
    stats = {"lus": len(rows), "ecrits": len(df), "liens_arxiv": int(df["has_arxiv_link"].sum())}
    monitoring.emit("cleaning", "silver", "hn", records=len(df), records_in=len(rows),
                    duplicates=len(rows) - len(df), arxiv_links=stats["liens_arxiv"],
                    duration_s=round(time.time() - start, 1))
    return stats
