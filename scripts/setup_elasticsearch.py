"""Réglages Elasticsearch pour un cluster à un seul nœud.

Sans réplique possible, les index doivent avoir 0 réplique,
sinon le cluster reste en état « yellow ».
"""
import json
import sys
import urllib.request

ES = "http://localhost:9200"
PATTERNS = ["scipulse-*", "arxiv-*"]


def put(path: str, body: dict) -> dict:
    req = urllib.request.Request(f"{ES}{path}", data=json.dumps(body).encode(), method="PUT",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def main() -> int:
    # Modèle appliqué aux futurs index du projet
    put("/_index_template/scipulse-defaults", {
        "index_patterns": PATTERNS,
        "priority": 1,
        "template": {"settings": {"number_of_replicas": 0}},
    })
    # Index déjà existants
    for pattern in PATTERNS:
        put(f"/{pattern}/_settings?allow_no_indices=true", {"index": {"number_of_replicas": 0}})
    print("Elasticsearch : 0 réplique pour les index du projet.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
