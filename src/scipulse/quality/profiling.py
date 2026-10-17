"""Analyse de la qualité des données Bronze (avant nettoyage).

Les résultats guident les règles de nettoyage (cleaning/) et les suites
Great Expectations (quality/suites.py).
"""
from __future__ import annotations

import re
from collections import Counter

from scipulse import config, storage
from scipulse.cleaning.common import ARXIV_ID_RE


def profile_arxiv(key: str, limit: int = 0) -> dict:
    """Parcourt le dump ArXiv brut (en streaming) et compte les anomalies."""
    n = dup = old_ids = bad_ids = 0
    nulls, empties, n_categories, domains, years, licenses = (Counter() for _ in range(6))
    doi = Counter()
    title_ws = abstract_ws = 0
    seen: set[str] = set()
    examples: dict[str, dict] = {}

    for r in storage.iter_jsonl(config.BRONZE["arxiv"], key):
        n += 1
        for field, value in r.items():
            if value is None:
                nulls[field] += 1
            elif isinstance(value, str) and not value.strip():
                empties[field] += 1

        arxiv_id = r["id"]
        if arxiv_id in seen:
            dup += 1
            examples.setdefault("doublon", {"id": arxiv_id})
        seen.add(arxiv_id)
        old_ids += "/" in arxiv_id
        bad_ids += not ARXIV_ID_RE.match(arxiv_id)

        value = r.get("doi")
        if value:
            doi["present"] += 1
            if value != value.lower():
                doi["majuscules"] += 1
                examples.setdefault("doi_majuscules", {"id": arxiv_id, "doi": value})
            if "doi.org" in value.lower() or value.lower().startswith("doi:"):
                doi["prefixe"] += 1
                examples.setdefault("doi_prefixe", {"id": arxiv_id, "doi": value})
            if " " in value.strip():
                doi["multiples"] += 1
                examples.setdefault("doi_multiples", {"id": arxiv_id, "doi": value})

        if "\n" in r["title"] or "  " in r["title"]:
            title_ws += 1
            examples.setdefault("titre_espaces", {"id": arxiv_id, "title": r["title"]})
        abstract_ws += "\n" in r["abstract"]

        cats = (r.get("categories") or "").split()
        n_categories[len(cats)] += 1
        if cats:
            domains[cats[0].split(".")[0]] += 1
        years[int(r["versions"][0]["created"].split()[3])] += 1
        licenses[r.get("license")] += 1
        if limit and n >= limit:
            break

    return {
        "articles": n, "doublons": dup, "ids_ancien_format": old_ids, "ids_invalides": bad_ids,
        "valeurs_nulles": dict(nulls), "chaines_vides": dict(empties), "doi": dict(doi),
        "titres_avec_retours_ou_espaces": title_ws, "resumes_avec_retours": abstract_ws,
        "nb_categories": dict(sorted(n_categories.items())), "domaines": dict(domains.most_common()),
        "annees": dict(sorted(years.items())), "licences": dict(licenses.most_common()),
        "exemples": examples,
    }


def profile_hn() -> dict:
    """Compare les deux flux HN bruts : schémas, doublons, types, valeurs manquantes."""
    result = {}
    for feed, id_field, score_field in [("live/", "id", "score"), ("backfill/", "objectID", "points")]:
        rows = [r for k in storage.list_keys(config.BRONZE["hn"], feed)
                for r in storage.iter_jsonl(config.BRONZE["hn"], k)]
        ids = [r.get(id_field) for r in rows]
        urls = [r.get("url") or "" for r in rows]
        result[feed.rstrip("/")] = {
            "lignes": len(rows),
            "stories_distinctes": len(set(ids)),
            "champs": dict(Counter(f for r in rows for f in r).most_common()),
            "types": dict(Counter(r.get("type", "story") for r in rows)),
            "sans_url": sum(1 for u in urls if not u),
            "url_schema_majuscule": sum(1 for u in urls if re.match(r"^[A-Z][a-zA-Z]*://", u)),
            "score_min_max": [min(r.get(score_field) or 0 for r in rows), max(r.get(score_field) or 0 for r in rows)],
        }
    return result
