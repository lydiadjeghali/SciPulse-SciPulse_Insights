# SciPulse & SciPulse Insights

SciPulse est une plateforme de veille scientifique construite comme un pipeline DataOps en architecture Medallion (Bronze, Silver, Gold). Elle ingère les publications ArXiv, suit leur écho sur Hacker News et récupère leurs citations académiques via OpenAlex, puis expose le résultat dans Kibana.

Le projet est découpé en deux modules :

- **SciPulse** : ingestion ArXiv et Hacker News, contrôle qualité avec Great Expectations, indexation dans Elasticsearch, dashboards de monitoring et d'exploration.
- **SciPulse Insights** : ajout des citations OpenAlex, traitement Spark (jointure des trois sources, indicateurs temporels, TF-IDF, graphe de citations), score d'impact composite et dashboard augmenté.

Lydia Djeghali 

## Sommaire

- [Architecture](#architecture)
- [Prérequis](#prérequis)
- [Démarrage](#démarrage)
- [Choix techniques](#choix-techniques)
- [Structure du dépôt](#structure-du-dépôt)
- [Documentation](#documentation)

## Architecture

### Flux de données

```
  ┌───────────────┐   ┌───────────────┐   ┌───────────────┐
  │     ArXiv     │   │  Hacker News  │   │   OpenAlex    │
  │  dump Kaggle  │   │ API Firebase  │   │   API REST    │
  │ hebdomadaire  │   │   + Algolia   │   │   quotidien   │
  └───────┬───────┘   └───────┬───────┘   └───────┬───────┘
          ▼                   ▼                   ▼
┌─ BRONZE · MinIO · JSON Lines ────────────────────────────┐
│    arxiv-raw/            hn-raw/         citations-raw/  │
└─────────┬───────────────────┬───────────────────┬────────┘
          ▼                   ▼                   ▼
┌──────────────────────────────────────────────────────────┐
│        nettoyage + validation Great Expectations         │
└─────────┬───────────────────┬───────────────────┬────────┘
          ▼                   ▼                   ▼
┌─ SILVER · MinIO · Parquet ───────────────────────────────┐
│   arxiv-clean/          hn-clean/       citations-clean/ │
└─────────┬───────────────────┬───────────────────┬────────┘
          ├─────────┐         │                   │
          ▼         └─────────┼───────────────────┘
┌────────────────┐ ┌──────────┴────────────────────────────┐
│   Indexation   │ │ Spark : jointure des 3 sources,       │
│     Python     │ │ indicateurs, TF-IDF, graphe,          │
│   (module 1)   │ │ score d'impact      (module 2)        │
└───────┬────────┘ └───────────────────┬───────────────────┘
        ▼                              ▼ elasticsearch-hadoop
┌─ GOLD · Elasticsearch ───────────────────────────────────┐
│ arxiv-papers               arxiv-papers-enriched         │
│ scipulse-monitoring ◄── Logstash ◄── événements          │
└────────────────────────────┬─────────────────────────────┘
                             ▼
                           Kibana
```

Chaque couche a un rôle précis :

- **Bronze** garde les réponses des sources telles quelles, en JSON Lines. On peut toujours rejouer le pipeline à partir de ces fichiers.
- **Silver** contient des données nettoyées, normalisées et validées, en Parquet. Un fichier n'y est écrit que s'il passe la suite Great Expectations de sa source.
- **Gold** est la seule couche exposée : Kibana n'interroge qu'Elasticsearch. Bronze et Silver restent dans MinIO et ne servent qu'au pipeline.

### Services

Toute l'infrastructure est décrite dans `docker-compose.yml`.

| Service | Rôle | Accès | Mémoire max |
|---|---|---|---|
| `airflow-webserver` | Interface d'Airflow | http://localhost:8080 | 1 Go |
| `airflow-scheduler` | Planifie les DAGs, exécute les tâches Python et le driver Spark | — | 3 Go |
| `airflow-init` | Initialise la base d'Airflow et le compte admin, puis s'arrête | — | — |
| `postgres` | Base de métadonnées d'Airflow | — | 256 Mo |
| `minio` | Data lake S3 : un bucket par couche et par source | http://localhost:9001 (console), `:9000` (API) | 384 Mo |
| `spark-master` | Maître du cluster Spark standalone | http://localhost:8081 | 512 Mo |
| `spark-worker` | Exécute les jobs Spark (2 cœurs, 1,5 Go) | http://localhost:8082 | 2 Go |
| `elasticsearch` | Couche Gold, nœud unique | http://localhost:9200 | 1,5 Go |
| `kibana` | Dashboards | http://localhost:5601 | 1 Go |
| `logstash` | Envoie les événements du pipeline dans l'index `scipulse-monitoring` | — | 640 Mo |

### Orchestration

```
 arxiv_batch (hebdomadaire)      citations_bronze (quotidien)
 ingest_bronze ─► clean_silver   ingest_openalex
        │                                │
        │ Dataset                        │ Dataset
        │ arxiv-clean                    │ citations-raw
        │                                ▼
        │                         citations_silver
        │                                │
        │                                │ Dataset
        │                                │ citations-clean
        ├───────────────┐                │
        ▼               ▼                │
   arxiv_gold       spark_gold ◄─────────┘
        │             ▲    │
        ▼             │    ▼
  arxiv-papers        │  arxiv-papers-enriched
                      │
                      │ hn-clean, relu par Spark
                      │
 hn_backfill ───┐     │
 (une fois)     ├─► hn_silver (toutes les heures)
 hn_microbatch ─┘
 (toutes les 5 min)
```

Les DAGs Gold ne sont pas planifiés à heure fixe : ils sont déclenchés par les **Datasets** d'Airflow. `arxiv_gold` démarre dès que la Silver ArXiv est mise à jour, `spark_gold` dès que la Silver ArXiv ou celle des citations l'est. Une nouvelle donnée en Bronze se propage ainsi jusqu'au Gold sans intervention.

| DAG | Planification | Tâches |
|---|---|---|
| `arxiv_batch` | hebdomadaire | `ingest_bronze` (dump Kaggle vers `arxiv-raw`), puis `clean_silver` (vers `arxiv-clean`) |
| `hn_microbatch` | toutes les 5 minutes | `poll_hn_api` : nouvelles stories et top stories vers `hn-raw` |
| `hn_backfill` | manuel, une fois | historique des stories qui pointent vers arxiv.org (API Algolia) |
| `hn_silver` | toutes les heures | nettoyage et validation de `hn-raw` vers `hn-clean` |
| `arxiv_gold` | Dataset `arxiv-clean` | indexation de la Silver ArXiv dans `arxiv-papers` |
| `citations_bronze` | quotidienne | `ingest_openalex` : citations OpenAlex vers `citations-raw` |
| `citations_silver` | Dataset `citations-raw` | nettoyage et validation vers `citations-clean` |
| `spark_gold` | Datasets `arxiv-clean` ou `citations-clean` | job Spark `gold_enrichment` (`SparkSubmitOperator`) vers `arxiv-papers-enriched` |

## Prérequis

- **Docker Desktop** avec Docker Compose v2 (moteur WSL 2 sous Windows).
- **Python 3.11** sur la machine hôte. Les scripts appelés par le `Makefile` n'utilisent que la bibliothèque standard : aucune installation n'est nécessaire.
- **Optionnel** : une clé OpenAlex gratuite (openalex.org). Sans clé, l'API limite à environ 1 000 requêtes par jour ; avec une clé, 10 000.

Les jobs Spark et le nettoyage du corpus ArXiv complet sont longs : il vaut mieux désactiver la mise en veille de la machine pendant qu'ils tournent.

## Démarrage

### 1. Lancer la stack

```bash
git clone https://github.com/lydiadjeghali/SciPulse-SciPulse_Insights.git
cd SciPulse-SciPulse_Insights
make up
```

`make up` fait tout depuis zéro :

1. crée `.env` à partir de `.env.example` et y génère la clé de chiffrement d'Airflow
2. construit l'image commune Airflow / Spark et démarre les conteneurs 
3. attend que chaque service réponde à son contrôle de santé 
4. règle Elasticsearch pour un nœud unique (0 réplique) et importe les vues et dashboards dans Kibana.

`make status` affiche ensuite l'état de chaque service.

### 2. Configurer (optionnel)

Les paramètres sont dans `.env`, qui n'est jamais versionné. Après une modification, relancer `make up`.

| Variable | Rôle | Valeur par défaut |
|---|---|---|
| `OPENALEX_API_KEY` | Clé OpenAlex, pour un quota plus élevé | vide |
| `OPENALEX_MAILTO` | Adresse transmise à OpenAlex avec chaque requête | — |
| `CITATIONS_MAX_ARTICLES` | Nombre d'articles dont on récupère les citations | `400000` |
| `ARXIV_SAMPLE_SIZE` | Limite le nombre d'articles ArXiv, pour un essai rapide (`0` = tout) | `0` |
| `ARXIV_CATEGORY_PREFIXES`, `ARXIV_MIN_YEAR` | Restreignent le corpus à certaines catégories ou à partir d'une année (vide = tout) | vide |
| `SPARK_WORKER_MEMORY`, `SPARK_WORKER_CORES` | Ressources du worker Spark | `1536m`, `2` |
| `ES_JAVA_OPTS`, `LS_JAVA_OPTS` | Mémoire JVM d'Elasticsearch et de Logstash | `768m`, `256m` |

La mémoire de l'exécuteur Spark du DAG `spark_gold` se règle sans toucher au code, avec la variable Airflow `spark_executor_memory` (1 Go par défaut).

### 3. Accéder aux interfaces

| Interface | URL | Identifiants (développement) |
|---|---|---|
| Airflow | http://localhost:8080 | `admin` / `scipulse-admin` |
| Kibana | http://localhost:5601 | — |
| Console MinIO | http://localhost:9001 | `scipulse` / `scipulse-minio-secret` |
| Spark | http://localhost:8081 | — |
| Elasticsearch | http://localhost:9200 | — |

Ces identifiants viennent de `.env.example` et ne conviennent qu'à un usage local.

### 4. Exécuter le pipeline

Les DAGs sont créés en pause. On les active dans Airflow avec l'interrupteur à gauche de chaque DAG, dans cet ordre :

1. **Hacker News** : activer `hn_microbatch`, `hn_silver` et `hn_backfill`, puis lancer `hn_backfill` une fois .
2. **ArXiv** : activer `arxiv_gold`, puis activer et lancer `arxiv_batch`. Une fois la Silver écrite, `arxiv_gold` démarre seul.
3. **Citations** : une fois la Silver ArXiv disponible (l'ingestion OpenAlex y lit la liste des articles), activer `citations_silver` et `spark_gold`, puis activer et lancer `citations_bronze`. La Silver des citations et le job Spark s'enchaînent ensuite automatiquement. Si le quota OpenAlex est atteint, le passage du lendemain reprend où le précédent s'est arrêté.

Pour un premier essai, on peut mettre `ARXIV_SAMPLE_SIZE=20000` dans `.env` avant de lancer `arxiv_batch` : le pipeline tourne alors sur un échantillon et se termine beaucoup plus vite.

Les trois dashboards se trouvent dans Kibana, menu *Dashboards* :

- **SciPulse — Monitoring du pipeline** : volume par couche, résultats Great Expectations, fraîcheur des données
- **SciPulse — Exploration scientifique ArXiv** : catégories, évolution des publications, articles récents, recherche plein texte
- **SciPulse Insights — Score d'impact et citations** : score d'impact, citations, signal Hacker News, PageRank et communautés.

### 5. Commandes utiles

| Commande | Effet |
|---|---|
| `make up` | Démarre (ou met à jour) toute la stack |
| `make status` | État des services |
| `make logs` | Journaux de tous les conteneurs |
| `make kibana` | Réimporte les vues et dashboards Kibana |
| `make down` | Arrête la stack ; les données sont conservées |
| `make clean` | Arrête la stack et supprime toutes les données (volumes Docker) |

## Choix techniques


### MinIO comme data lake

MinIO est compatible S3 : le même code fonctionnerait sur AWS, et Spark le lit directement avec `s3a://`. Chaque couche et chaque source a son bucket (`arxiv-raw`, `hn-clean`, `citations-clean`…), créé au démarrage.

### Bronze : rester fidèle à la source

La couche Bronze n'applique aucune transformation : les réponses sont écrites telles quelles, en JSON Lines. La date d'ingestion et le flux figurent dans le nom du fichier, jamais dans les données.

- **ArXiv** : le zip Kaggle est décompressé en flux et envoyé vers MinIO par morceaux, sans jamais écrire sur le disque. Le snapshot porte la date de publication du fichier : un dump inchangé n'est pas réingéré.
- **Hacker News** : le polling toutes les 5 minutes récupère les nouvelles stories et les 200 top stories, dont le score évolue. Le dernier identifiant lu est conservé dans `hn-raw/_state/`. L'historique des stories pointant vers arxiv.org est récupéré une fois via l'API Algolia : sans lui, le polling seul donnerait trop peu de liens à croiser avec ArXiv.
- **OpenAlex** : les articles sont demandés par lots de 100, avec un filtre sur leur DOI arXiv (`10.48550/arxiv.<id>`) et une pagination par curseur. L'avancement est sauvegardé : si le quota est atteint, le passage suivant reprend au même endroit, et un passage sans nouvelle donnée ne relance pas la suite du pipeline.

### Silver : la qualité avant l'écriture

Les règles de nettoyage découlent de l'analyse de qualité des données Bronze : identifiants arXiv sans numéro de version, DOI en minuscules sans préfixe `https://doi.org/`, dates normalisées, catégories découpées en liste avec catégorie et domaine principaux, licences encodées, doublons supprimés, schéma commun pour les deux API Hacker News. Le code est dans `src/scipulse/cleaning/`.

Les trois suites Great Expectations (`arxiv_silver`, `hn_silver`, `citations_silver`) forment un contrôle bloquant : chaque paquet est validé avant d'être écrit et, si une règle échoue, rien n'est écrit. Les résultats sont publiés dans les Data Docs.

Les clés de jointure `arxiv_id` et `doi` sont normalisées par les mêmes fonctions pour les trois sources : c'est ce qui permet ensuite de les relier.

Le corpus ArXiv complet ne tient pas en mémoire. Il est traité par paquets, chacun dans un processus séparé : la mémoire est entièrement rendue au système après chaque paquet.

Les transformations sont écrites en Python (pandas) et en Spark, directement sur les fichiers Parquet de MinIO. Sans entrepôt SQL dans l'architecture, dbt n'apporterait rien ici.

### Source de citations : OpenAlex

| Besoin | OpenAlex | Semantic Scholar |
|---|---|---|
| Nombre de citations | `cited_by_count` | `citationCount` |
| Citations par année (vélocité) | fournies directement (`counts_by_year`) | non fournies |
| Champ disciplinaire | `primary_topic` : domaine, champ, sous-champ | `fieldsOfStudy` |
| Références entre articles | `referenced_works`, dans la même réponse | appels séparés |


OpenAlex a été retenu parce qu'il donne directement les citations année par année, la donnée nécessaire pour calculer la vélocité de citation. Ses références entre articles alimentent aussi le graphe de citations.

L'ingestion est limitée à 400 000 articles (`CITATIONS_MAX_ARTICLES`) pour tenir dans le quota de l'API : d'abord tous les articles cités sur Hacker News, indispensables pour croiser les trois sources, puis un échantillon aléatoire reproductible du reste du corpus.

### Spark : jointure, indicateurs et TF-IDF

Les trois Silver sont lues depuis MinIO. Les articles ArXiv forment la table centrale, citations et Hacker News y sont rattachés par `arxiv_id` en jointure externe à gauche, pour garder aussi les articles jamais cités ni discutés. Seules les colonnes utiles sont lues dans les fichiers Parquet.

La stratégie de jointure a été choisie en comparant les plans d'exécution (`explain`) : le broadcast join l'emporte nettement sur le sort-merge join. Les tables rattachées sont petites et tiennent en mémoire sur chaque exécuteur , la table ArXiv n'a alors besoin d'être ni redistribuée ni triée.

Les indicateurs par catégorie utilisent des window functions : croissance annuelle des publications et des citations (`lag` sur l'année), vélocité de citation de chaque article (citations des deux dernières années complètes). Le TF-IDF (MLlib) porte sur les résumés, avec un vocabulaire de 50 000 termes présents dans au moins 5 documents , les 5 termes au plus fort poids deviennent les mots-clés de l'article.

### Score d'impact composite

```
score = 100 × (0,35·C + 0,10·P + 0,15·V + 0,25·H + 0,15·R)
```

| | Composante | Calcul | Poids |
|---|---|---|---|
| C | Citations | rang percentile parmi les articles du même domaine et de la même année | 35 % |
| P | PageRank | rang percentile dans le domaine | 10 % |
| V | Vélocité | rang percentile dans le domaine | 15 % |
| H | Hacker News | `log(1 + points + commentaires)`, rapporté au maximum | 25 % |
| R | Récence | `exp(-âge / 3 ans)` | 15 % |

Les citations restent le signal principal de l'impact scientifique. Elles sont comparées au sein de leur cohorte : un article de l'année dernière n'est pas jugé face à un article de dix ans, ni les mathématiques face à l'informatique, dont les habitudes de citation diffèrent. Le PageRank donne plus de poids à une citation venant d'un article lui-même influent ; son poids reste modéré car le graphe ne couvre que les articles du périmètre. La vélocité mesure l'élan actuel d'un article. Le signal Hacker News est passé au logarithme, sans quoi quelques stories virales écraseraient tout le reste. La récence compense le fait qu'un article récent n'a pas encore eu le temps d'être cité.

Les cinq composantes de chaque article sont conservées dans l'index, ce qui permet d'expliquer n'importe quel score.

### Couche Gold

L'écriture vers `arxiv-papers-enriched` passe par le connecteur elasticsearch-hadoop. Chaque ligne est sérialisée en JSON par Spark avant l'envoi : le connecteur ne sait pas convertir seul les tableaux d'objets comme les citations par année. L'index est créé avant l'écriture avec un mapping `strict`, et l'identifiant de chaque document est son `arxiv_id` : republier remplace les documents, sans doublon. L'actualisation de l'index est suspendue pendant l'écriture.

Le résultat est d'abord écrit en Parquet dans un bucket technique (`gold-staging`). Si l'écriture Elasticsearch échoue, on republie sans refaire les calculs (`--publish-only`).

Les deux index partagent l'analyseur `scientific_english` : minuscules, suppression des accents et des mots vides, et une racinisation légère qui ne confond pas des termes techniques proches. Les champs de filtre sont en `keyword`, les champs de recherche en `text`.

### Robustesse du job Spark

Plusieurs mesures évitent qu'un incident oblige à tout recalculer : points de reprise sur disque (`checkpoint`) après la jointure et après le graphe, délais réseau allongés pour un driver très sollicité, et algorithmes de graphe en version GraphX, dont le plan d'exécution ne grossit pas à chaque itération.

### Graphe de citations (option B)

Parmi les trois extensions proposées, le graphe de citations a été retenu : il enrichit directement le score d'impact et reste léger pour la machine. GraphFrames (`graphframes:0.8.3-spark3.5-s_2.12`) est intégré à l'image Spark, sans nouveau service Docker.

Chaque article est un sommet, chaque référence entre deux articles du périmètre une arête orientée. Le PageRank (probabilité de saut 0,15, 10 itérations) entre dans le score d'impact. Les composantes connexes et les communautés (propagation de labels) sont comparées aux catégories ArXiv dans le dashboard augmenté.

### Monitoring et dashboards

Chaque tâche écrit un événement JSON (volume, durée, statut, résultat des validations) dans `logs/pipeline/`. Logstash les envoie dans l'index `scipulse-monitoring`, qui alimente le dashboard de monitoring. Le pipeline ne dépend pas de Logstash : s'il est arrêté, les événements l'attendent dans le fichier.

Les vues et dashboards Kibana sont décrits en Python (`scripts/build_kibana.py`) puis exportés dans `kibana/scipulse.ndjson` : ils sont versionnés et réimportés par `make up`, sans manipulation manuelle. Kibana affiche les dates en UTC, comme dans les données.

### Ressources et sécurité

Chaque service a une limite mémoire, calibrée pour une machine de 8 Go dont 6 sont donnés à Docker. Les secrets sont dans `.env`, généré à partir de `.env.example` et exclu du dépôt. La sécurité d'Elasticsearch est désactivée : c'est un environnement local de développement.

## Structure du dépôt

```
.
├── docker-compose.yml     infrastructure complète
├── Makefile               commandes make
├── .env.example           modèle du fichier .env
├── docker/
│   ├── airflow/           image Airflow / Spark
│   └── logstash/          pipeline du monitoring
├── dags/                  DAGs Airflow
├── src/scipulse/
│   ├── config.py          paramètres (.env)
│   ├── storage.py         accès à MinIO
│   ├── monitoring.py      événements du pipeline
│   ├── isolation.py       un paquet, un processus
│   ├── ingestion/         Bronze
│   ├── quality/           Great Expectations
│   ├── cleaning/          Silver
│   ├── indexing/          Gold du module 1
│   └── spark/             Gold du module 2
├── spark_jobs/            jobs soumis à Spark
├── gx/                    projet Great Expectations
├── es/mappings/           mappings Elasticsearch
├── kibana/                dashboards Kibana
├── scripts/               scripts du Makefile
├── doc/                   documentation par étape
└── Issues/                problèmes et solutions
```

## Documentation

Chaque étape est documentée en détail dans `doc/`, avec des captures prises au moment de sa réalisation :

- module 1 : [infrastructure](doc/01_infrastructure.md), [Bronze](doc/02_bronze.md), [Silver](doc/03_silver.md), [Gold et dashboards](doc/04_gold.md) ;
- module 2 : [Bronze citations](doc/m2_01_bronze_citations.md), [Silver citations](doc/m2_02_silver_citations.md), [jointure Spark](doc/m2_03_spark_jointures.md), [indicateurs et score d'impact](doc/m2_04_spark_score_impact.md), [Gold Elasticsearch](doc/m2_05_gold_elasticsearch.md), [dashboard augmenté](doc/m2_06_dashboard_augmente.md) ;
- option B : [graphe de citations et PageRank](doc/m2_07_option_b_graphe_pagerank.md).

