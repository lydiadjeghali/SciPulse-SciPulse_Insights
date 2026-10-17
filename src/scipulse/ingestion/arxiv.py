"""Bronze ArXiv : télécharge le dump Kaggle et l'écrit tel quel dans MinIO."""
from __future__ import annotations

import time
import zipfile
from datetime import date
from pathlib import Path

import requests
from boto3.s3.transfer import TransferConfig

from scipulse import config, monitoring, storage

# Fichier contenu dans le zip Kaggle (un article JSON par ligne)
DUMP_MEMBER = "arxiv-metadata-oai-snapshot.json"


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


def ingest(snapshot: str | None = None, sample_size: int = config.ARXIV_SAMPLE_SIZE,
           keep_zip: bool = False) -> dict:
    """Envoie le dump dans arxiv-raw/snapshot=<date>/ sans le modifier."""
    start = time.time()
    snapshot = snapshot or date.today().isoformat()
    zip_path = download_dump(config.TMP_DIR / "arxiv.zip")
    key = f"snapshot={snapshot}/{DUMP_MEMBER}"

    # Le fichier est lu dans le zip et envoyé directement vers MinIO
    with zipfile.ZipFile(zip_path) as zf, zf.open(DUMP_MEMBER) as member:
        reader = _LineCountingReader(member, max_lines=sample_size)
        storage.s3_client().upload_fileobj(
            reader, config.BRONZE["arxiv"], key,
            Config=TransferConfig(multipart_chunksize=64 << 20, max_concurrency=2),
        )
    if not keep_zip:
        zip_path.unlink(missing_ok=True)

    monitoring.emit("ingestion", "bronze", "arxiv", records=reader.lines, bytes=reader.bytes,
                    duration_s=round(time.time() - start, 1), snapshot=snapshot)
    return {"key": key, "records": reader.lines, "bytes": reader.bytes}


def latest_snapshot_key() -> str:
    """Chemin du dernier dump ArXiv présent en Bronze."""
    keys = [k for k in storage.list_keys(config.BRONZE["arxiv"]) if k.endswith(DUMP_MEMBER)]
    if not keys:
        raise FileNotFoundError("Aucun dump ArXiv en Bronze : lancer d'abord l'ingestion.")
    return sorted(keys)[-1]
