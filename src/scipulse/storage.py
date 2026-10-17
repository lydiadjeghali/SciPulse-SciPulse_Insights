"""Lecture et écriture des fichiers dans MinIO (JSON Lines et Parquet)."""
from __future__ import annotations

import io
import json
from typing import Iterable, Iterator

import boto3
import pandas as pd
from botocore.client import Config
from botocore.exceptions import ClientError

from scipulse import config


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_USER,
        aws_secret_access_key=config.MINIO_PASSWORD,
        config=Config(signature_version="s3v4", retries={"max_attempts": 5}),
        region_name="us-east-1",
    )


def list_keys(bucket: str, prefix: str = "") -> list[str]:
    paginator = s3_client().get_paginator("list_objects_v2")
    return [
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
    ]


def object_exists(bucket: str, key: str) -> bool:
    try:
        s3_client().head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def put_jsonl(bucket: str, key: str, records: Iterable[dict]) -> int:
    """Écrit les données brutes en JSON Lines et renvoie le nombre de lignes."""
    lines = [json.dumps(r, ensure_ascii=False) for r in records]
    body = ("\n".join(lines) + "\n").encode("utf-8") if lines else b""
    s3_client().put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/x-ndjson")
    return len(lines)


def iter_jsonl(bucket: str, key: str, block_size: int = 64 << 20) -> Iterator[dict]:
    """Lit un fichier JSON Lines ligne par ligne.

    Le fichier est lu par tranches (requêtes Range) : aucune connexion ne reste ouverte
    pendant le traitement des lignes (MinIO coupait le flux du dump de 5,6 Go).
    """
    client = s3_client()
    size = client.head_object(Bucket=bucket, Key=key)["ContentLength"]
    rest = b""
    for start in range(0, size, block_size):
        end = min(start + block_size, size) - 1
        block = client.get_object(Bucket=bucket, Key=key, Range=f"bytes={start}-{end}")["Body"].read()
        lines = (rest + block).split(b"\n")
        rest = lines.pop()                      # dernière ligne peut-être incomplète
        for line in lines:
            if line.strip():
                yield json.loads(line)
    if rest.strip():
        yield json.loads(rest)


def read_jsonl_prefix(bucket: str, prefix: str = "") -> pd.DataFrame:
    rows = [r for key in list_keys(bucket, prefix) for r in iter_jsonl(bucket, key)]
    return pd.DataFrame(rows)


def put_parquet(bucket: str, key: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
    s3_client().put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


def read_parquet_prefix(bucket: str, prefix: str = "", columns: list[str] | None = None) -> pd.DataFrame:
    frames = []
    for key in list_keys(bucket, prefix):
        if key.endswith(".parquet"):
            body = s3_client().get_object(Bucket=bucket, Key=key)["Body"].read()
            frames.append(pd.read_parquet(io.BytesIO(body), columns=columns))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=columns)


def delete_prefix(bucket: str, prefix: str) -> int:
    """Supprime tous les fichiers d'un dossier."""
    keys = list_keys(bucket, prefix)
    client = s3_client()
    for i in range(0, len(keys), 1000):
        client.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[i : i + 1000]]}
        )
    return len(keys)
