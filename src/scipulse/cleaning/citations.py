"""Silver citations : normalise les réponses OpenAlex et écrit citations-clean/ en Parquet.

Règles de nettoyage :
- une ligne par article arXiv (clé de jointure commune aux trois sources).
- identifiant arXiv retrouvé dans les « locations » d'OpenAlex : lien arxiv.org (abs, pdf) ou DOI arXiv 10.48550/arxiv.<id>.
- un work OpenAlex qui regroupe plusieurs dépôts arXiv donne une ligne par identifiant.
- DOI en minuscules, sans préfixe https://doi.org/ (même fonction que pour ArXiv).
- citations par année gardées en liste (année, nombre) pour la vélocité de citation.
- champ disciplinaire extrait de primary_topic (domaine, champ, sous-champ, thème).
- doublons retirés : un seul work par article (celui qui a le plus de citations).
"""
from __future__ import annotations

import re
import time

import pandas as pd

from scipulse import config, monitoring, storage
from scipulse.cleaning.common import arxiv_id_from_url, clean_text, normalize_arxiv_id, normalize_doi
from scipulse.isolation import run_isolated
from scipulse.quality import validation

FILES_PER_CHUNK = 200
OUTPUT_PREFIX = "openalex/"
ARXIV_DOI_RE = re.compile(r"10\.48550/arxiv\.(.+)$", re.IGNORECASE)


def _short_id(url: str | None) -> str | None:
    """https://openalex.org/W123 → W123."""
    return url.rsplit("/", 1)[-1] if isinstance(url, str) and url else None


def arxiv_ids_of(work: dict) -> list[str]:
    """Identifiants arXiv d'un work OpenAlex, trouvés dans ses locations."""
    found = []
    for loc in work.get("locations") or []:
        for url in (loc.get("landing_page_url"), loc.get("pdf_url")):
            arxiv_id = arxiv_id_from_url(url)
            if arxiv_id:
                found.append(arxiv_id)
        match = ARXIV_DOI_RE.search(loc.get("doi") or "")
        if match:
            found.append(normalize_arxiv_id(match.group(1)))
    return sorted({i for i in found if i})


def clean_work(work: dict) -> list[dict]:
    """Une ligne par identifiant arXiv du work."""
    topic = work.get("primary_topic") or {}
    years = [{"year": int(c["year"]), "cited_by_count": int(c["cited_by_count"])}
             for c in work.get("counts_by_year") or []]
    references = [_short_id(r) for r in work.get("referenced_works") or []]
    base = {
        "openalex_id": _short_id(work.get("id")),
        "doi": normalize_doi(work.get("doi")),
        "title": clean_text(work.get("title")),
        "publication_year": work.get("publication_year"),
        "publication_date": work.get("publication_date"),
        "work_type": work.get("type"),
        "cited_by_count": int(work.get("cited_by_count") or 0),
        "counts_by_year": sorted(years, key=lambda c: c["year"]),
        "topic": (topic.get("display_name")),
        "subfield": (topic.get("subfield") or {}).get("display_name"),
        "field": (topic.get("field") or {}).get("display_name"),
        "domain": (topic.get("domain") or {}).get("display_name"),
        "referenced_works": references,
        "referenced_works_count": len(references),
    }
    return [{"arxiv_id": arxiv_id, **base} for arxiv_id in arxiv_ids_of(work)]


def _process_chunk(part: int, keys: list[str], seen: set[str]) -> dict:
    """Nettoie, valide et écrit un paquet de fichiers Bronze."""
    works = [w for key in keys for w in storage.iter_jsonl(config.BRONZE["citations"], key)]
    rows = [row for w in works for row in clean_work(w)]
    df = pd.DataFrame(rows)
    stats = {"works": len(works), "sans_arxiv": sum(1 for w in works if not arxiv_ids_of(w))}

    # Un seul work par article : celui qui a le plus de citations
    df = df.sort_values("cited_by_count", ascending=False).drop_duplicates("arxiv_id")
    duplicate = df["arxiv_id"].isin(seen)
    stats["doublons"] = int(duplicate.sum()) + (len(rows) - len(df))
    df = df[~duplicate].reset_index(drop=True)
    df["publication_date"] = pd.to_datetime(df["publication_date"], errors="coerce")
    df["publication_year"] = df["publication_year"].astype("Int64")

    result = validation.validate(df, "citations_silver", run_name=f"citations_part-{part:05d}")
    if not result["success"]:
        raise ValueError(f"Paquet {part} refusé par Great Expectations : {result['failed']}")
    storage.put_parquet(config.SILVER["citations"], f"{OUTPUT_PREFIX}part-{part:05d}.parquet", df)
    stats["ecrits"] = len(df)
    return {"stats": stats, "ids": df["arxiv_id"].tolist()}


def run(max_chunks: int = 0) -> dict:
    """Bronze -> Silver pour les citations (reconstruit tout citations-clean/openalex/)."""
    start = time.time()
    keys = sorted(k for k in storage.list_keys(config.BRONZE["citations"]) if k.endswith(".jsonl"))
    chunks = [keys[i:i + FILES_PER_CHUNK] for i in range(0, len(keys), FILES_PER_CHUNK)]
    if max_chunks:
        chunks = chunks[:max_chunks]
    storage.delete_prefix(config.SILVER["citations"], OUTPUT_PREFIX)

    seen: set[str] = set()
    stats = {"works": 0, "sans_arxiv": 0, "doublons": 0, "ecrits": 0, "paquets": 0}
    for part, chunk in enumerate(chunks):
        result = run_isolated(_process_chunk, part, chunk, seen)
        seen.update(result["ids"])
        for name, value in result["stats"].items():
            stats[name] += value
        stats["paquets"] += 1
        print(f"Paquet {part} : {result['stats']['ecrits']} articles écrits")

    monitoring.emit("cleaning", "silver", "citations", records=stats["ecrits"], records_in=stats["works"],
                    duplicates=stats["doublons"], invalid=stats["sans_arxiv"],
                    duration_s=round(time.time() - start, 1))
    return stats
