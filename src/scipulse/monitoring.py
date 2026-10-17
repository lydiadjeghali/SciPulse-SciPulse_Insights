"""Événements de suivi du pipeline (envoyés à Kibana par Logstash)."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from scipulse import config

EVENTS_FILE = "pipeline_events.jsonl"


def emit(stage: str, layer: str, source: str, status: str = "success", **metrics) -> dict:
    """Écrit un événement à la fin d'une étape.

    stage  : ingestion, validation, cleaning, indexing ou spark
    layer  : bronze, silver ou gold
    source : arxiv, hn, citations ou enriched
    metrics: nombre de lignes, durée, etc.
    """
    event = {
        "event_id": uuid.uuid4().hex,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "stage": stage,
        "layer": layer,
        "source": source,
        "status": status,
        **metrics,
    }
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(config.LOG_DIR / EVENTS_FILE, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(event, ensure_ascii=False, default=str) + "\n")
    print(f"[monitoring] {json.dumps(event, default=str)}")
    return event
