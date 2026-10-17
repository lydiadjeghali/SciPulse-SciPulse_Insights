"""Importe dans Kibana les fichiers kibana/*.ndjson (vues et dashboards).

Les objets existants sont remplacés : on peut relancer le script sans risque.
"""
import json
import sys
import urllib.request
import uuid
from pathlib import Path

KIBANA = "http://localhost:5601"
ROOT = Path(__file__).resolve().parent.parent


def import_ndjson(path: Path) -> dict:
    # Envoi du fichier via l'API d'import de Kibana
    boundary = uuid.uuid4().hex
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{path.name}"\r\n'
        "Content-Type: application/ndjson\r\n\r\n"
    ).encode() + path.read_bytes() + f"\r\n--{boundary}--\r\n".encode()
    req = urllib.request.Request(
        f"{KIBANA}/api/saved_objects/_import?overwrite=true",
        data=body,
        method="POST",
        headers={"kbn-xsrf": "true", "Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def set_advanced_settings() -> None:
    """Dates affichées en UTC (comme dans les données), quel que soit le fuseau du navigateur."""
    req = urllib.request.Request(
        f"{KIBANA}/api/kibana/settings", method="POST",
        data=json.dumps({"changes": {"dateFormat:tz": "UTC"}}).encode(),
        headers={"kbn-xsrf": "true", "Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=30).read()


def main() -> int:
    set_advanced_settings()
    files = sorted((ROOT / "kibana").glob("*.ndjson"))
    if not files:
        print("Aucun objet Kibana à importer.")
        return 0
    ok = True
    for f in files:
        result = import_ndjson(f)
        ok &= result.get("success", False)
        print(f"  {f.name}: {result.get('successCount', 0)} objet(s) importé(s)"
              + ("" if result.get("success") else f" — erreurs : {result.get('errors')}"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
