"""Crée le fichier .env à partir de .env.example.

Si .env existe déjà, il est conservé.
"""
import base64
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
example, target = ROOT / ".env.example", ROOT / ".env"

if target.exists():
    print(".env existe déjà — conservé.")
else:
    # Génère la clé de chiffrement d'Airflow
    fernet = base64.urlsafe_b64encode(os.urandom(32)).decode()
    content = example.read_text(encoding="utf-8").replace(
        "AIRFLOW_FERNET_KEY=\n", f"AIRFLOW_FERNET_KEY={fernet}\n"
    )
    target.write_text(content, encoding="utf-8", newline="\n")
    print(".env créé à partir de .env.example.")

# Dossier lu par Logstash
(ROOT / "logs" / "pipeline").mkdir(parents=True, exist_ok=True)
