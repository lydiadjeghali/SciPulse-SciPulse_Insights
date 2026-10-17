"""Paramètres du projet, lus depuis les variables d'environnement (.env)."""
import os
from pathlib import Path

HOME = Path(os.getenv("SCIPULSE_HOME", Path(__file__).resolve().parents[2]))

# MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "scipulse")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "scipulse-minio-secret")

# Buckets par couche
BRONZE = {"arxiv": "arxiv-raw", "hn": "hn-raw", "citations": "citations-raw"}
SILVER = {"arxiv": "arxiv-clean", "hn": "hn-clean", "citations": "citations-clean"}

# Elasticsearch
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
INDEX_ARXIV = "arxiv-papers"                 # module 1
INDEX_ENRICHED = "arxiv-papers-enriched"     # module 2

# Périmètre ArXiv
ARXIV_DUMP_URL = os.getenv(
    "ARXIV_DUMP_URL",
    "https://www.kaggle.com/api/v1/datasets/download/Cornell-University/arxiv",
)
ARXIV_CATEGORY_PREFIXES = tuple(
    p.strip() for p in os.getenv("ARXIV_CATEGORY_PREFIXES", "cs.").split(",") if p.strip()
)
ARXIV_MIN_YEAR = int(os.getenv("ARXIV_MIN_YEAR", "2020") or 0)
ARXIV_SAMPLE_SIZE = int(os.getenv("ARXIV_SAMPLE_SIZE", "0") or 0)

# Sources externes
OPENALEX_API_KEY = os.getenv("OPENALEX_API_KEY", "")
OPENALEX_MAILTO = os.getenv("OPENALEX_MAILTO", "")
HN_API = "https://hacker-news.firebaseio.com/v0"
HN_ALGOLIA_API = "https://hn.algolia.com/api/v1"

# Dossiers
TMP_DIR = Path(os.getenv("SCIPULSE_TMP", HOME / "tmp"))
LOG_DIR = Path(os.getenv("SCIPULSE_LOG_DIR", HOME / "logs"))
GE_ROOT = HOME / "gx"
