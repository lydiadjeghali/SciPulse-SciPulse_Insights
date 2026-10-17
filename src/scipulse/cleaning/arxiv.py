"""Silver ArXiv : nettoie le dump Bronze et l'écrit en Parquet dans arxiv-clean/.

Règles de nettoyage (issues de l'analyse de qualité des données Bronze) :
- identifiant arXiv sans version ; doublons supprimés (on garde la première occurrence) ;
- titres et résumés sans retours à la ligne ni espaces en trop ;
- DOI en minuscules, sans préfixe ni espace parasite ; si plusieurs DOI, le premier ;
  un DOI irrécupérable (tronqué, mal formé) devient null ;
- catégories découpées en liste ; catégorie et domaine principaux extraits ;
- dates normalisées (soumission = première version) ;
- valeurs manquantes : licence → « inconnue », champs texte optionnels → null ;
- lignes sans identifiant, titre ou résumé supprimées.

Le dump est traité par paquets ; chaque paquet est validé par Great Expectations
avant d'être écrit (un paquet refusé arrête la tâche).
"""
from __future__ import annotations

import time
from itertools import islice
from typing import Iterator

import pandas as pd

from scipulse import config, monitoring, storage
from scipulse.cleaning.common import clean_text, license_label, normalize_arxiv_id, normalize_doi
from scipulse.ingestion.arxiv import latest_snapshot_key
from scipulse.isolation import run_isolated
from scipulse.quality import validation

# 50 000 articles par paquet : environ 500 Mo de mémoire
CHUNK_SIZE = 50_000
VERSION_DATE_FORMAT = "%a, %d %b %Y %H:%M:%S %Z"


def _chunks(key: str, size: int, limit: int = 0) -> Iterator[list[dict]]:
    """Découpe le dump Bronze en paquets de `size` articles."""
    records = storage.iter_jsonl(config.BRONZE["arxiv"], key)
    if limit:
        records = islice(records, limit)
    while chunk := list(islice(records, size)):
        yield chunk


def clean_chunk(records: list[dict]) -> pd.DataFrame:
    """Nettoie un paquet d'articles bruts."""
    raw = pd.DataFrame(records)
    df = pd.DataFrame({"arxiv_id": raw["id"].map(normalize_arxiv_id)})

    # Texte
    df["title"] = raw["title"].map(clean_text)
    df["abstract"] = raw["abstract"].map(clean_text)
    df["submitter"] = raw["submitter"].map(clean_text)
    df["comments"] = raw["comments"].map(clean_text)
    df["journal_ref"] = raw["journal-ref"].map(clean_text)
    df["report_no"] = raw["report-no"].map(clean_text)

    # Auteurs : liste « Prénom Nom » à partir de authors_parsed ([nom, prénom, suffixe])
    df["authors"] = raw["authors_parsed"].map(
        lambda parts: [clean_text(f"{a[1]} {a[0]} {a[2] if len(a) > 2 else ''}") for a in parts or []]
    )
    df["n_authors"] = df["authors"].map(len)

    # Catégories
    df["categories"] = raw["categories"].fillna("").str.split()
    df["primary_category"] = df["categories"].map(lambda c: c[0] if c else None)
    df["primary_domain"] = df["primary_category"].str.split(".").str[0]
    df["n_categories"] = df["categories"].map(len)

    # Clé de jointure DOI et licence
    df["doi"] = raw["doi"].map(normalize_doi)
    df["license"] = raw["license"].map(license_label)

    # Dates : soumission = première version, dernière version, mise à jour
    df["submitted_date"] = pd.to_datetime(
        raw["versions"].map(lambda v: v[0]["created"] if v else None), format=VERSION_DATE_FORMAT, utc=True
    ).dt.tz_localize(None).dt.normalize()
    df["last_version_date"] = pd.to_datetime(
        raw["versions"].map(lambda v: v[-1]["created"] if v else None), format=VERSION_DATE_FORMAT, utc=True
    ).dt.tz_localize(None).dt.normalize()
    df["n_versions"] = raw["versions"].map(len)
    df["update_date"] = pd.to_datetime(raw["update_date"], format="%Y-%m-%d")
    df["published_year"] = df["submitted_date"].dt.year.astype("Int64")

    # Indicateurs simples, utiles aux contrôles et aux dashboards
    df["title_len"] = df["title"].str.len().astype("Int64")
    df["abstract_len"] = df["abstract"].str.len().astype("Int64")
    return df


def _in_scope(df: pd.DataFrame) -> pd.Series:
    """Périmètre défini dans .env (par défaut : tout le corpus)."""
    keep = pd.Series(True, index=df.index)
    if config.ARXIV_CATEGORY_PREFIXES:
        keep &= df["primary_category"].fillna("").str.startswith(config.ARXIV_CATEGORY_PREFIXES)
    if config.ARXIV_MIN_YEAR:
        keep &= df["published_year"] >= config.ARXIV_MIN_YEAR
    return keep


def _process_chunk(part: int, records: list[dict], seen: set[str], prefix: str, snapshot: str) -> dict:
    """Nettoie, valide et écrit un paquet. Renvoie les compteurs et les identifiants écrits."""
    df = clean_chunk(records)
    stats = {"lus": len(df)}

    valid = df["arxiv_id"].notna() & df["title"].notna() & df["abstract"].notna()
    stats["invalides"] = int((~valid).sum())
    df = df[valid]

    duplicate = df["arxiv_id"].isin(seen) | df["arxiv_id"].duplicated()
    stats["doublons"] = int(duplicate.sum())
    df = df[~duplicate]
    ids = df["arxiv_id"].tolist()

    scope = _in_scope(df)
    stats["hors_perimetre"] = int((~scope).sum())
    df = df[scope].reset_index(drop=True)

    # Contrôle qualité : le paquet n'est écrit que s'il respecte la suite Silver
    result = validation.validate(df, "arxiv_silver", run_name=f"{snapshot}_part-{part:05d}")
    if not result["success"]:
        raise ValueError(f"Paquet {part} refusé par Great Expectations : {result['failed']}")

    storage.put_parquet(config.SILVER["arxiv"], f"{prefix}part-{part:05d}.parquet", df)
    stats["ecrits"] = len(df)
    return {"stats": stats, "ids": ids}


def run(sample_size: int = config.ARXIV_SAMPLE_SIZE, chunk_size: int = CHUNK_SIZE) -> dict:
    """Bronze → Silver pour ArXiv : nettoyage, validation Great Expectations, écriture Parquet."""
    start = time.time()
    key = latest_snapshot_key()
    snapshot = key.split("/")[0]                     # ex. snapshot=2026-10-11
    prefix = f"{snapshot}/"
    storage.delete_prefix(config.SILVER["arxiv"], prefix)   # réécriture complète (idempotent)

    seen: set[str] = set()
    stats = {"lus": 0, "invalides": 0, "doublons": 0, "hors_perimetre": 0, "ecrits": 0, "paquets": 0}
    for part, records in enumerate(_chunks(key, chunk_size, sample_size)):
        result = run_isolated(_process_chunk, part, records, seen, prefix, snapshot)
        seen.update(result["ids"])
        for name, value in result["stats"].items():
            stats[name] += value
        stats["paquets"] += 1
        print(f"Paquet {part} : {result['stats']['ecrits']} articles écrits")

    monitoring.emit("cleaning", "silver", "arxiv", records=stats["ecrits"], records_in=stats["lus"],
                    duplicates=stats["doublons"], invalid=stats["invalides"],
                    out_of_scope=stats["hors_perimetre"], duration_s=round(time.time() - start, 1))
    return stats
