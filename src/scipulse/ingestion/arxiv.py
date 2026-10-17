"""Bronze ArXiv : télécharge le dump Kaggle et l'écrit tel quel dans MinIO.

Le snapshot porte la date de publication du fichier Kaggle. Si le fichier n'a pas
changé depuis la dernière ingestion (même ETag), il n'est pas re-téléchargé.
"""
from __future__ import annotations

import json
import time
import zipfile
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests
from boto3.s3.transfer import TransferConfig

from scipulse import config, monitoring, storage

# Fichier contenu dans le zip Kaggle (un article JSON par ligne)
DUMP_MEMBER = "arxiv-metadata-oai-snapshot.json"
STATE_KEY = "_state/last_ingest.json"


class _LineCountingReader:
    """Lit le fichier du zip en comptant les lignes (= articles)."""

    def __init__(self, raw, max_lines: int = 0):
        self.raw, self.max_lines = raw, max_lines
        self.lines, self.bytes = 0, 0

    def read(self, size: int = -1) -> bytes:
        if self.max_lines and self.lines >= self.max_lines:
            return b""
        chunk = self.raw.read(size)
        # Mode test : on s'arrête après max_lines lignes
        if self.max_lines:
            remaining = self.max_lines - self.lines
            parts = chunk.split(b"\n")
            if len(parts) - 1 >= remaining:
                chunk = b"\n".join(parts[:remaining]) + b"\n"
        self.lines += chunk.count(b"\n")
        self.bytes += len(chunk)
        return chunk


def source_version(url: str = config.ARXIV_DUMP_URL) -> dict:
    """Version du fichier Kaggle (ETag et date), lue sans le télécharger (1 octet demandé)."""
    with requests.get(url, headers={"Range": "bytes=0-0"}, timeout=60) as resp:
        resp.raise_for_status()
        published = parsedate_to_datetime(resp.headers["Last-Modified"])
        return {"etag": resp.headers.get("ETag", "").strip('"'), "published": published.date().isoformat()}


def _read_state() -> dict:
    try:
        body = storage.s3_client().get_object(Bucket=config.BRONZE["arxiv"], Key=STATE_KEY)["Body"]
        return json.loads(body.read())
    except Exception:
        return {}


def download_dump(dest: Path, url: str = config.ARXIV_DUMP_URL) -> Path:
    """Télécharge le zip Kaggle (ignoré s'il est déjà complet sur le disque)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        expected = int(resp.headers.get("Content-Length", 0))
        if dest.exists() and expected and dest.stat().st_size == expected:
            print(f"Zip déjà téléchargé ({expected / 1e9:.2f} Go)")
            return dest
        with open(dest, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=8 << 20):
                fh.write(chunk)
    print(f"Zip téléchargé : {dest.stat().st_size / 1e9:.2f} Go")
    return dest


def ingest(sample_size: int = config.ARXIV_SAMPLE_SIZE, keep_zip: bool = False) -> dict:
    """Envoie le dump dans arxiv-raw/snapshot=<date du fichier>/ sans le modifier."""
    start = time.time()
    version = source_version()
    snapshot = version["published"]
    key = f"snapshot={snapshot}/{DUMP_MEMBER}"

    # Fichier source inchangé depuis la dernière ingestion : rien à faire
    state = _read_state()
    if state.get("etag") == version["etag"] and storage.object_exists(config.BRONZE["arxiv"], state["key"]):
        print(f"Dump Kaggle inchangé (publié le {snapshot}) : déjà présent dans {state['key']}")
        monitoring.emit("ingestion", "bronze", "arxiv", status="skipped", records=0,
                        duration_s=round(time.time() - start, 1), snapshot=snapshot)
        return {"key": state["key"], "records": state["records"], "skipped": True}

    zip_path = download_dump(config.TMP_DIR / "arxiv.zip")

    # Le fichier est lu dans le zip et envoyé directement vers MinIO
    with zipfile.ZipFile(zip_path) as zf, zf.open(DUMP_MEMBER) as member:
        reader = _LineCountingReader(member, max_lines=sample_size)
        storage.s3_client().upload_fileobj(
            reader, config.BRONZE["arxiv"], key,
            Config=TransferConfig(multipart_chunksize=64 << 20, max_concurrency=2),
        )
    if not keep_zip:
        zip_path.unlink(missing_ok=True)

    # Mémorise la version ingérée (pas d'état en mode échantillon)
    if not sample_size:
        storage.s3_client().put_object(
            Bucket=config.BRONZE["arxiv"], Key=STATE_KEY,
            Body=json.dumps({**version, "key": key, "records": reader.lines}).encode(),
        )
    monitoring.emit("ingestion", "bronze", "arxiv", records=reader.lines, bytes=reader.bytes,
                    duration_s=round(time.time() - start, 1), snapshot=snapshot)
    return {"key": key, "records": reader.lines, "bytes": reader.bytes}


def latest_snapshot_key() -> str:
    """Chemin du dernier dump ArXiv présent en Bronze."""
    keys = [k for k in storage.list_keys(config.BRONZE["arxiv"]) if k.endswith(DUMP_MEMBER)]
    if not keys:
        raise FileNotFoundError("Aucun dump ArXiv en Bronze : lancer d'abord l'ingestion.")
    return sorted(keys)[-1]
