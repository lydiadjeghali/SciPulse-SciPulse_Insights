# SciPulse & SciPulse Insights

Plateforme de veille scientifique à grande échelle — pipeline DataOps Medallion (Bronze → Silver → Gold) sur ArXiv, Hacker News et OpenAlex, avec Airflow, MinIO, Great Expectations, Spark et Elastic Stack.

> Projet #4 — Lydia Djeghali

## Démarrage rapide

```bash
make up      # démarre toute la stack de zéro
make status  # vérifie l'état des services
```

| Interface | URL |
|---|---|
| Airflow | http://localhost:8080 |
| Kibana | http://localhost:5601 |
| MinIO console | http://localhost:9001 |
| Spark master | http://localhost:8081 |
