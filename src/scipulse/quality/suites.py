"""Suites Great Expectations de la couche Silver (une par source).

Chaque règle a une note qui explique pourquoi elle existe.
"""
from datetime import datetime

from scipulse.cleaning.common import ARXIV_ID_PATTERN

CURRENT_YEAR = datetime.now().year
ARXIV_ID_REGEX = rf"^{ARXIV_ID_PATTERN}$"

ARXIV_COLUMNS = [
    "arxiv_id", "title", "abstract", "submitter", "comments", "journal_ref", "report_no",
    "authors", "n_authors", "categories", "primary_category", "primary_domain", "n_categories",
    "doi", "license", "submitted_date", "last_version_date", "n_versions", "update_date",
    "published_year", "title_len", "abstract_len",
]
HN_COLUMNS = [
    "hn_id", "title", "url", "domain", "author", "score", "num_comments", "created_at",
    "story_kind", "arxiv_id", "has_arxiv_link", "source", "feed", "observed_at",
]

# (type d'expectation, paramètres, note)
SUITES = {
    "arxiv_silver": [
        ("expect_table_columns_to_match_set", {"column_set": ARXIV_COLUMNS, "exact_match": True},
         "Le schéma Silver est stable : Spark s'appuie dessus."),
        ("expect_column_values_to_not_be_null", {"column": "arxiv_id"},
         "L'identifiant arXiv est la clé de jointure principale."),
        ("expect_column_values_to_be_unique", {"column": "arxiv_id"},
         "92 doublons trouvés en Bronze : un article = une ligne."),
        ("expect_column_values_to_match_regex", {"column": "arxiv_id", "regex": ARXIV_ID_REGEX},
         "Format nouveau (2101.00001) ou ancien (hep-th/9901001), sans numéro de version."),
        ("expect_column_values_to_not_be_null", {"column": "title"},
         "Un article sans titre est inutilisable."),
        ("expect_column_values_to_not_be_null", {"column": "abstract"},
         "Le résumé alimente la recherche plein texte et le TF-IDF."),
        ("expect_column_values_to_match_regex", {"column": "doi", "regex": r"^10\.[^A-Z\s]+$"},
         "DOI normalisé : minuscules, sans préfixe https://doi.org/ (578 684 DOI en majuscules en Bronze)."),
        ("expect_column_values_to_match_regex",
         {"column": "primary_category", "regex": r"^[a-z\-]+(\.[A-Za-z\-]+)?$"},
         "Catégorie principale au format arXiv (ex. cs.LG, hep-th)."),
        ("expect_column_values_to_be_between",
         {"column": "published_year", "min_value": 1986, "max_value": CURRENT_YEAR},
         "Les plus anciennes soumissions datent de 1986."),
        ("expect_column_values_to_be_between", {"column": "n_authors", "min_value": 1},
         "Tout article a au moins un auteur."),
        ("expect_column_values_to_match_regex",
         {"column": "license",
          "regex": r"^(inconnue|autre|arxiv-non-exclusive|cc0-\d\.\d|cc-[a-z\-]+-\d\.\d)$"},
         "Licence encodée en code court ; « inconnue » si absente (452 790 cas en Bronze)."),
        ("expect_column_pair_values_a_to_be_greater_than_b",
         {"column_A": "last_version_date", "column_B": "submitted_date", "or_equal": True},
         "La dernière version ne peut pas précéder la première."),
    ],
    "hn_silver": [
        ("expect_table_columns_to_match_set", {"column_set": HN_COLUMNS, "exact_match": True},
         "Un seul schéma pour les flux live (Firebase) et backfill (Algolia)."),
        ("expect_column_values_to_not_be_null", {"column": "hn_id"},
         "L'identifiant HN est la clé de la story."),
        ("expect_column_values_to_be_unique", {"column": "hn_id"},
         "Les top stories sont relues à chaque passage : une seule ligne par story."),
        ("expect_column_values_to_not_be_null", {"column": "title"},
         "Une story sans titre est inutilisable."),
        ("expect_column_values_to_be_between", {"column": "score", "min_value": 0},
         "Le score HN ne peut pas être négatif."),
        ("expect_column_values_to_be_between", {"column": "num_comments", "min_value": 0},
         "Le nombre de commentaires ne peut pas être négatif."),
        ("expect_column_values_to_be_between",
         {"column": "created_at", "min_value": "2006-10-01", "max_value": f"{CURRENT_YEAR + 1}-01-01"},
         "Hacker News existe depuis octobre 2006."),
        ("expect_column_values_to_be_in_set",
         {"column": "story_kind", "value_set": ["story", "show_hn", "ask_hn"]},
         "Type de story encodé (les offres d'emploi sont exclues)."),
        ("expect_column_values_to_be_in_set", {"column": "source", "value_set": ["live", "backfill"]},
         "Origine de la ligne : micro-batch ou historique."),
        ("expect_column_values_to_match_regex", {"column": "url", "regex": r"^https?://"},
         "URL absolue (null autorisé pour les Ask HN)."),
        ("expect_column_values_to_match_regex", {"column": "arxiv_id", "regex": ARXIV_ID_REGEX},
         "Identifiant arXiv extrait de l'URL, même format que dans arxiv-clean."),
    ],
}
