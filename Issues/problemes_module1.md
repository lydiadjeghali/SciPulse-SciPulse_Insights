# Problèmes rencontrés 
# Module 1


*Lydia Djeghali*

Chaque problème a été noté au moment où il a été rencontré, avec sa cause et la solution retenue. 


## Étape 1 : Infrastructure

| # | Problème | Cause | Solution |
|---|---|---|---|
| 1 | Docker n'avait que 3,9 Go de RAM, pas assez pour toute la stack | WSL2 donne par défaut 50 % de la RAM (8 Go) | Fichier `.wslconfig` : 6 Go de RAM + 8 Go de swap. Limite mémoire pour chaque service |
| 2 | Erreur à la construction de l'image (`ResolutionImpossible` sur PySpark 3.5.3) | Airflow 2.10.5 impose PySpark 3.5.4 | Passage à PySpark 3.5.4 |
| 3 | Impossible de télécharger les images `minio/minio` et `minio/mc` |  | Image `bitnamilegacy/minio:2025.4.22`. Les buckets sont créés par la variable `MINIO_DEFAULT_BUCKETS` |
| 4 | Interface Airflow lente | Le webserver atteignait sa limite de 768 Mo | Limite passée à 1 Go |
| 5 | Elasticsearch en état `yellow` | Un index créé avec 1 réplique, impossible sur un seul nœud | Script `setup_elasticsearch.py` : 0 réplique pour les index du projet |

## Étape 2 : Couche Bronze

| # | Problème | Cause | Solution |
|---|---|---|---|
| 1 | L'ingestion ArXiv échoue : `PermissionError` sur `/opt/scipulse/tmp/arxiv.zip` | Le volume Docker `airflow-tmp` est créé avec le propriétaire `root` ; les tâches tournent sous l'utilisateur `airflow` | Le conteneur `airflow-init` est lancé en root et donne le dossier à l'utilisateur `airflow` (`chown`) avant de préparer Airflow |

## Étape 3 : Couche Silver

| # | Problème | Cause | Solution |
|---|---|---|---|
| 1 | `PermissionError: /opt/scipulse/gx` à la création du projet Great Expectations | GE 0.18 crée son projet dans un dossier nommé `gx/` (et non `great_expectations/`) | Projet renommé `gx/` |
| 2 | Validation HN en échec : `url` ne commence pas par `http(s)://` | 2 URL `Https://arxiv.org/…` dans l'historique Algolia | Le contrôle bloquant a fonctionné ; règle de nettoyage ajoutée : schéma et domaine de l'URL en minuscules |
| 3 | Scheduler Airflow (ram à 100%), nettoyage très lent | Paquets de 200 000 articles (avec résumés) trop gros | Paquets de 50 000 articles (~500 Mo) |
| 4 | `IncompleteRead` après le premier paquet | Le dump de 5,6 Go était lu en un seul flux HTTP, inactif pendant le traitement de chaque paquet : MinIO coupait la connexion | Lecture par tranches de 64 Mo (requêtes `Range`) : aucune connexion ne reste ouverte pendant le traitement |
| 5 | Événements de tests mélangés au monitoring | Les essais sur échantillon écrivaient dans le même journal d'événements | Événements de test retirés (journal + index `scipulse-monitoring`). Les essais écrivent désormais dans un dossier `test/` séparé |
| 6 | Paquet 17 refusé par Great Expectations : règle `doi` en échec | DOI brut `« 10. 1145/3154499 »` (espace parasite) coupé en `« 10. »` | Normalisation du DOI revue : espaces parasites et préfixes retirés, motif DOI recherché dans le champ ; 123 DOI irrécupérables (tronqués, Handle, chiffre manquant) passent à null. Résultat : 1 338 209 DOI conformes, 0 non conforme |
| 7 | Paquet 60 (sur 65) refusé par Great Expectations : règle « dernière version ≥ première version » en échec | 1 seul article (`hep-ph/9701266`) a ses versions dans le désordre dans la source (v2 datée de 1995, v1 de 1996) | la Silver contient les paquets 0 à 59, soit 3 000 000 d'articles sur 3 205 432 (94 %).|

## Étape 4 : Couche Gold 

| # | Problème | Cause | Solution |
|---|---|---|---|
| 1 | Le graphique « 5 premiers domaines par année » affichait 12 domaines | Le top 5 était calculé année par année (regroupement par année d'abord) | Regroupement par domaine placé avant l'année : top 5 sur toute la période + « Other » |