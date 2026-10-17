"""Fonctions de nettoyage communes aux sources (clés de jointure, texte)."""
from __future__ import annotations

import re

# Identifiant arXiv : nouveau format (2101.00001) ou ancien format (hep-th/9901001, math.GT/0309136)
ARXIV_ID_PATTERN = r"(?:\d{4}\.\d{4,5}|[a-z\-]+(?:\.[A-Z]{2})?/\d{7})"
ARXIV_ID_RE = re.compile(rf"^{ARXIV_ID_PATTERN}$")
# Identifiant arXiv dans une URL (abs, pdf, html), sans le numéro de version
ARXIV_URL_RE = re.compile(rf"arxiv\.org/(?:abs|pdf|html|format)/({ARXIV_ID_PATTERN})(?:v\d+)?", re.IGNORECASE)


def clean_text(value: str | None) -> str | None:
    """Supprime les retours à la ligne et les espaces en trop."""
    if not isinstance(value, str):             # None ou NaN selon la version de pandas
        return None
    text = " ".join(value.split())
    return text or None


DOI_SEARCH_RE = re.compile(r"10\.\d{4,9}/[^\s;,]+")


def normalize_doi(value: str | None) -> str | None:
    """Premier DOI trouvé dans le champ, en minuscules et sans préfixe.

    Corrige sans rien deviner : espaces parasites (« 10. 1145/… », « 10.1029/ 2011… »),
    préfixes (« https://doi.org/ », « doi.org/ », « doi: »), texte autour (« 27E8;10.1214/… »).
    Un DOI tronqué ou mal formé (« 10.1063 », « 0.1103/… ») devient null.
    """
    if not isinstance(value, str) or not value.strip():
        return None
    text = re.sub(r"\b10\.\s+(\d)", r"10.\1", value.strip())   # espace après « 10. »
    text = re.sub(r"(10\.\d{4,9}/)\s+(\w)", r"\1\2", text)       # espace après « / »
    match = DOI_SEARCH_RE.search(text)
    return match.group(0).lower() if match else None


def normalize_arxiv_id(value: str | None) -> str | None:
    """Identifiant arXiv sans espace ni numéro de version (2101.00001v2 → 2101.00001)."""
    if not isinstance(value, str) or not value.strip():
        return None
    arxiv_id = re.sub(r"v\d+$", "", value.strip())
    return arxiv_id if ARXIV_ID_RE.match(arxiv_id) else None


def arxiv_id_from_url(url: str | None) -> str | None:
    """Extrait l'identifiant arXiv d'une URL (https://arxiv.org/abs/2101.00001v2 → 2101.00001)."""
    if not isinstance(url, str):
        return None
    match = ARXIV_URL_RE.search(url)
    return match.group(1) if match else None


def license_label(url: str | None) -> str:
    """Code court de licence (ex. cc-by-4.0) à partir de l'URL arXiv."""
    if not isinstance(url, str) or not url:
        return "inconnue"
    if "nonexclusive-distrib" in url:
        return "arxiv-non-exclusive"
    match = re.search(r"creativecommons\.org/(?:licenses|publicdomain)/([a-z\-]+)/(\d\.\d)", url)
    if match:
        kind = "cc0" if match.group(1) == "zero" else f"cc-{match.group(1)}"
        return f"{kind}-{match.group(2)}"
    return "autre"
