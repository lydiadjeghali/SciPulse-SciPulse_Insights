# Commandes du projet SciPulse
#   make up     : démarre toute la stack
#   make down   : arrête la stack
#   make clean  : arrête et supprime les données
#   make status : état des services
COMPOSE = docker compose
PYTHON ?= python

.PHONY: env build up down clean status logs kibana trigger-all test

# Crée le fichier .env
env:
	$(PYTHON) scripts/make_env.py

build: env
	$(COMPOSE) build

# Démarre la stack et attend que tout soit prêt
up: env
	$(COMPOSE) up -d --build
	$(PYTHON) scripts/wait_for_stack.py
	$(PYTHON) scripts/setup_kibana.py

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v --remove-orphans

status:
	$(COMPOSE) ps
	$(PYTHON) scripts/wait_for_stack.py --once

logs:
	$(COMPOSE) logs -f --tail=100

# Importe les dashboards Kibana
kibana:
	$(PYTHON) scripts/setup_kibana.py

# Lance tout le pipeline
trigger-all:
	$(COMPOSE) exec airflow-scheduler airflow dags unpause scipulse_full_pipeline
	$(COMPOSE) exec airflow-scheduler airflow dags trigger scipulse_full_pipeline

# Tests unitaires
test:
	$(COMPOSE) exec airflow-scheduler python -m pytest -q /opt/scipulse/tests
