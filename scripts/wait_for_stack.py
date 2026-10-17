"""Attend que tous les services répondent, puis affiche leur état.

Usage : python scripts/wait_for_stack.py [--once] [--timeout 900]
"""
import argparse
import sys
import time
import urllib.request

# Service : (URL de test, texte attendu dans la réponse)
CHECKS = {
    "Elasticsearch": ("http://localhost:9200/_cluster/health", '"status"'),
    "Kibana": ("http://localhost:5601/api/status", '"available"'),
    "MinIO": ("http://localhost:9000/minio/health/live", ""),
    "Airflow": ("http://localhost:8080/health", '"healthy"'),
    "Spark master": ("http://localhost:8081/json/", '"ALIVE"'),
}


def probe(url: str, needle: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return resp.status == 200 and needle in resp.read().decode("utf-8", "ignore")
    except Exception:
        return False


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--timeout", type=int, default=900)
    args = parser.parse_args()

    # Réessaie toutes les 10 secondes jusqu'au délai maximum
    deadline = time.time() + (0 if args.once else args.timeout)
    while True:
        status = {name: probe(*check) for name, check in CHECKS.items()}
        if all(status.values()) or time.time() >= deadline:
            break
        time.sleep(10)

    for name, ok in status.items():
        print(f"  {'OK ' if ok else 'KO '} {name:<14} {CHECKS[name][0]}")
    healthy = all(status.values())
    print("Stack SciPulse opérationnelle." if healthy else "Certains services ne répondent pas.")
    return 0 if healthy else 1


if __name__ == "__main__":
    sys.exit(main())
