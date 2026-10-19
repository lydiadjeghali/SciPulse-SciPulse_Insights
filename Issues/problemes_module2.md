# Problèmes rencontrés — Module 2

**Lydia Djeghali**

Chaque problème est noté au moment où il est rencontré, avec sa cause et la solution retenue.

## Étape 1 — Couche Bronze : citations OpenAlex

| # | Problème | Cause | Solution |
|---|---|---|---|
| 1 | Quota OpenAlex limité sans clé : environ 1 000 requêtes par jour | Depuis 2025, OpenAlex facture chaque requête (0,0001 $) sur un budget gratuit de 0,10 $ par jour et par adresse IP sans clé (1 $ par jour avec une clé gratuite) | Lots de 100 articles par requête (~100 000 articles par jour) ; priorité aux articles cités sur Hacker News ; DAG quotidien qui reprend là où il s'est arrêté ; clé gratuite optionnelle (`OPENALEX_API_KEY`) |
| 2 | Le DOI renvoyé par OpenAlex n'est souvent pas le DOI arXiv demandé (ex. `1706.03762` → DOI de l'éditeur) | OpenAlex fusionne la prépublication arXiv et la version publiée en un seul « work » | La correspondance avec l'article arXiv se fera en Silver grâce au champ `locations` (lien `arxiv.org/abs/<id>`) |
| 3 | Ingestion arrêtée après 4 lots : erreur `401 Unauthorized` | Refus ponctuel d'OpenAlex sur le budget partagé sans clé (les requêtes repassaient quelques minutes plus tard, quota non épuisé) | Clé gratuite créée et ajoutée dans `.env` (non versionné) : quota propre de 10 000 requêtes par jour. Les réponses 401, 403 et 429 arrêtent désormais le passage proprement (avancement sauvegardé, message conservé) au lieu de faire échouer la tâche |
| 4 | Récupérer les citations des 3 millions d'articles demanderait ~7,7 Go de données brutes, alors qu'il ne reste que 12 Go libres sur le disque | Les réponses OpenAlex incluent les listes de références (~0,26 Mo pour 100 articles) | Limite à **400 000 articles** (~1 Go), réglable par `CITATIONS_MAX_ARTICLES` : d'abord les ~18 700 articles cités sur Hacker News, puis un **échantillon aléatoire reproductible** (graine 42) de tout le corpus. Un tri du plus récent au plus ancien aurait gardé surtout des articles de 2025-2026, encore peu cités |
| 5 | Ingestion arrêtée à ~314 500 articles : `ConnectionError (RemoteDisconnected)` | Coupure de connexion côté OpenAlex ; le code ne réessayait qu'en cas d'erreur serveur (5xx) | Les coupures réseau et les délais dépassés déclenchent maintenant jusqu'à 5 nouvelles tentatives avec attente croissante ; l'avancement sauvegardé a permis de reprendre sans perte |
| 6 | Nouvel arrêt à ~357 700 articles : `ChunkedEncodingError` (réponse interrompue en cours de lecture) | La correction n° 5 ne couvrait que l'établissement de la connexion, pas la lecture de la réponse | Toute erreur réseau (connexion, lecture, délai, réponse tronquée ou illisible, erreur serveur) déclenche une nouvelle tentative ; logique regroupée dans une seule fonction `_get_page` |

## Étape 2 — Couche Silver : citations

| # | Problème | Cause | Solution |
|---|---|---|---|
| 7 | 3 % des réponses OpenAlex n'ont pas de lien `arxiv.org/abs/…` dans leurs `locations` | OpenAlex range parfois le dépôt arXiv sous un lien PDF, ou seulement sous son DOI DataCite | Identifiant arXiv recherché aussi dans les liens PDF et dans le DOI `10.48550/arxiv.<id>` des locations : 0 réponse sans identifiant arXiv |
| 8 | 2 % des works contiennent plusieurs identifiants arXiv | OpenAlex fusionne parfois plusieurs dépôts arXiv d'un même travail | Une ligne par identifiant arXiv ; si un article apparaît dans plusieurs works, on garde celui qui a le plus de citations (188 doublons retirés) |

## Étape 3 — Spark : jointures multi-sources

| # | Problème | Cause | Solution |
|---|---|---|---|
| 9 | Spark refuse de lire les fichiers Silver : `illegalParquetTypeError` | pandas écrit les dates Parquet en nanosecondes, format non lu nativement par Spark 3.5 | Spark lit ces dates comme des entiers (`spark.sql.legacy.parquet.nanosAsLong`) puis les convertit en timestamp au chargement ; les prochaines écritures Silver se font en microsecondes (`coerce_timestamps="us"`). Le chargeur accepte les deux formats, sans recalculer les 3 M d'articles |
| 10 | Job de jointure très long (48 min) | La comparaison lisait toutes les colonnes ArXiv, résumés compris, trois fois (broadcast, sort-merge, taux de correspondance) | Seules les colonnes utiles à la jointure sont lues (élagage des colonnes Parquet) ; la comparaison des stratégies devient une option (`--compare`) : le DAG n'exécute que la jointure en broadcast |
| 11 | Capture du plan d'exécution impossible (délai dépassé) | Le texte complet de `.explain('formatted')` détaille chaque nœud (137 et 205 lignes) | La capture montre l'arbre du plan ; le texte complet est conservé dans `logs/spark/plan_*.txt` |
| 12 | Interface Airflow : « DAG spark_gold seems to be missing from DagBag », 2 DAGs absents de la liste | Affichage pendant qu'Airflow relisait les fichiers de DAGs (après `dags reserialize`) | Passager : vérifié par `dags list` (8 DAGs) et `list-import-errors` (aucune erreur) ; capture refaite ensuite |
| 13 | 2 exécutions en échec sur ~180 pour `hn_microbatch` | Exécutions interrompues (`SIGTERM`, délai de 5 min du DAG dépassé) pendant que le scheduler était chargé par les jobs Spark | Passager, les exécutions suivantes ont réussi ; les données manquantes sont reprises au passage suivant (état `_state/`) |
| 14 | Exécution `arxiv_batch` restée « en cours » depuis l'arrêt au paquet 60 (tâche `clean_silver` en attente de nouvelle tentative, DAG en pause) | Le DAG a été mis en pause pendant l'attente de la nouvelle tentative | Exécution marquée **échouée** : réactiver le DAG ne relancera pas `clean_silver`, qui aurait effacé les 60 paquets Silver avant d'échouer de nouveau |

## Étape 4 — Spark : analytique avancée et score d'impact

| # | Problème | Cause | Solution |
|---|---|---|---|
| 15 | Calcul complet bloqué : un exécuteur s'arrête (`Driver disassociated`), puis le suivant recalcule tout (`Missing an output location for shuffle`) | Le driver Spark, dans le scheduler Airflow très chargé, n'a pas répondu pendant 120 s ; les résultats intermédiaires en mémoire ont été perdus avec l'exécuteur | Job arrêté (après accord) ; délais réseau portés à 600 s, driver à 1,5 Go, **points de reprise sur disque** après le TF-IDF et après le score (volume partagé driver / worker) ; maximum HN calculé une seule fois au lieu d'une fenêtre globale. Retest sur 5 % : mêmes résultats |
| 16 | Calcul complet de 18 h 30 au lieu d'environ 1 h 30 | L'ordinateur s'est mis en veille pendant la nuit : une tâche TF-IDF a duré de 20 h 36 à 10 h 15 ; un exécuteur a ensuite été arrêté | Grâce aux points de reprise (n° 15), le nouvel exécuteur est reparti du TF-IDF déjà calculé et le job a réussi (389 804 articles notés). À l'avenir : désactiver la mise en veille pendant les calculs longs |

## Étape 5 — Couche Gold : écriture Elasticsearch et intégration Airflow

| # | Problème | Cause | Solution |
|---|---|---|---|
| 17 | Écriture vers Elasticsearch arrêtée après 628 documents : `Cannot handle type GenericRowWithSchema within type Tuple2` | Limite du connecteur elasticsearch-hadoop sur les tableaux d'objets imbriqués (`counts_by_year`) | Chaque ligne est sérialisée en JSON par Spark (`to_json`) et envoyée telle quelle par le connecteur (`es.input.json=true`) ; même mapping, même identifiant de document (`arxiv_id`) |
| 18 | Test sur 5 % très lent (~4 min par tâche, 30 min sans atteindre l'écriture Elasticsearch) | Le périmètre n'était gardé qu'en mémoire (`persist`) ; avec 1 Go par exécuteur, Spark relisait les 3 M d'articles et refaisait la jointure à chaque étape du TF-IDF. Les résultats du calcul complet précédent avaient été supprimés avec les points de reprise | Point de reprise sur disque juste après la jointure ; résultat écrit en Parquet (bucket technique `gold-staging`) puis publié dans Elasticsearch (`--publish-only` pour republier sans recalcul) ; mémoire de Spark réglée au lancement (variable Airflow `spark_executor_memory`) au lieu d'être figée dans le code |
| 19 | Mémoire insuffisante pour l'exécuteur Spark (1 Go) | Machine de 8 Go, Docker limité à 6 Go | **Temporairement**, pendant le calcul : Kibana et Logstash arrêtés (événements de monitoring conservés dans le fichier, envoyés au redémarrage), worker Spark à 3 Go (conteneur 3,5 Go), exécuteur à 2,5 Go. Réglages d'origine restaurés après le calcul |
| 20 | Publication en échec : `JsonParseException: Unexpected character '('` | Avec `es.input.json`, le connecteur attend des chaînes JSON ; un DataFrame lui transmettait la représentation texte de la ligne Spark | Écriture d'un RDD de chaînes JSON avec `EsOutputFormat` (méthode documentée par Elastic pour PySpark) ; test de publication seule : 1 978 documents en 32 s, identifiant `arxiv_id`, objets imbriqués conservés |

## Étape 6 — Dashboard Kibana augmenté

| # | Problème | Cause | Solution |
|---|---|---|---|
| 21 | Le cas d'usage « sous-domaines en croissance selon les deux métriques » est impossible à construire dans Kibana | Les croissances par catégorie (publications, citations) étaient calculées par Spark mais seulement écrites dans le rapport `gold_report.json`, pas dans l'index ; `counts_by_year` est un tableau d'objets aplati par Elasticsearch, inutilisable pour recalculer une croissance | Les indicateurs de la catégorie de l'année complète la plus récente (`publications_categorie`, `croissance_publications_categorie`, `citations_categorie`, `croissance_citations_categorie`) sont ajoutés à chaque document au moment de la publication ; republication sans recalcul du score (`--publish-only`) |
| 22 | Kibana bloqué sur « Loading Elastic », puis « Elastic did not load properly » (y compris pour les dashboards du module 1) | Machine saturée juste après la republication (Elasticsearch, scheduler et Kibana au-delà de 100 % de CPU) : délais dépassés entre Kibana et Elasticsearch, puis fichiers JavaScript de Kibana coupés en cours de transfert (`ERR_CONNECTION_RESET`) | Redémarrage de Kibana seul (`docker compose restart kibana`) une fois la charge retombée ; aucune donnée ni objet Kibana touché |
