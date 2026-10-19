"""Génère les objets Kibana du projet dans kibana/scipulse.ndjson.

Vues de données, visualisations Lens, recherche enregistrée et dashboards sont
décrits ici en Python : ils sont versionnés et réimportés par `make up`
(scripts/setup_kibana.py), sans manipulation manuelle dans Kibana.

Usage : python scripts/build_kibana.py
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "kibana" / "scipulse.ndjson"

DV_ARXIV = "dv-arxiv-papers"
DV_MONITORING = "dv-scipulse-monitoring"
DV_ENRICHED = "dv-arxiv-papers-enriched"
LAYER = "layer1"


# --------------------------------------------------------------- colonnes Lens
def count(label):
    return {"label": label, "dataType": "number", "operationType": "count", "isBucketed": False,
            "scale": "ratio", "sourceField": "___records___", "params": {"emptyAsNull": True},
            "customLabel": True}


def metric(op, field, label, fmt=None):
    col = {"label": label, "dataType": "number", "operationType": op, "isBucketed": False,
           "scale": "ratio", "sourceField": field, "params": {"emptyAsNull": True}, "customLabel": True}
    if fmt:
        col["params"]["format"] = fmt
    return col


def last_value(field, sort_field, label, data_type="string"):
    return {"label": label, "dataType": data_type, "operationType": "last_value", "isBucketed": False,
            "scale": "ordinal", "sourceField": field, "customLabel": True,
            "params": {"sortField": sort_field, "showArrayValues": False}}


def max_date(field, label):
    return {"label": label, "dataType": "date", "operationType": "max", "isBucketed": False,
            "scale": "ratio", "sourceField": field, "customLabel": True, "params": {}}


def terms(field, label, size, order_col, other=False):
    return {"label": label, "dataType": "string", "operationType": "terms", "isBucketed": True,
            "scale": "ordinal", "sourceField": field, "customLabel": True,
            "params": {"size": size, "orderBy": {"type": "column", "columnId": order_col},
                       "orderDirection": "desc", "otherBucket": other, "missingBucket": False,
                       "parentFormat": {"id": "terms"}}}


def terms_by_count(field, label, size):
    """Regroupement trié par nombre de documents (catégories les plus représentées)."""
    col = terms(field, label, size, None)
    col["params"]["orderBy"] = {"type": "custom"}
    col["params"]["orderAgg"] = {"label": "Nombre", "dataType": "number", "operationType": "count",
                                 "isBucketed": False, "scale": "ratio", "sourceField": "___records___",
                                 "params": {"emptyAsNull": True}}
    return col


def date_hist(field, label, interval):
    return {"label": label, "dataType": "date", "operationType": "date_histogram", "isBucketed": True,
            "scale": "interval", "sourceField": field, "customLabel": True,
            "params": {"interval": interval, "includeEmptyRows": True, "dropPartials": False}}


PERCENT = {"id": "percent", "params": {"decimals": 1}}
NUMBER = {"id": "number", "params": {"decimals": 0}}
DECIMAL = {"id": "number", "params": {"decimals": 1}}


# -------------------------------------------------------------- objets Lens
def lens(obj_id, title, dv, columns, visualization, vis_type, query=""):
    """Visualisation Lens à une couche de données."""
    return {
        "id": obj_id, "type": "lens", "coreMigrationVersion": "8.8.0", "typeMigrationVersion": "8.9.0",
        "attributes": {
            "title": title, "description": "", "visualizationType": vis_type,
            "state": {
                "datasourceStates": {"formBased": {"layers": {LAYER: {
                    "columns": columns,
                    # Lens exige les colonnes de regroupement avant les mesures
                    "columnOrder": sorted(columns, key=lambda c: not columns[c]["isBucketed"]),
                    "incompleteColumns": {}}}}},
                "visualization": visualization,
                "query": {"query": query, "language": "kuery"},
                "filters": [], "internalReferences": [], "adHocDataViews": {},
            },
        },
        "references": [{"type": "index-pattern", "id": dv, "name": f"indexpattern-datasource-layer-{LAYER}"}],
    }


def metric_vis(obj_id, title, dv, col, query="", subtitle=""):
    vis = {"layerId": LAYER, "layerType": "data", "metricAccessor": "m"}
    if subtitle:
        vis["subtitle"] = subtitle
    return lens(obj_id, title, dv, {"m": col}, vis, "lnsMetric", query)


def xy_vis(obj_id, title, dv, x_col, y_col, series="bar_stacked", split_col=None, query="", legend=True,
           split_first=False):
    """split_first : le top N de la ventilation est calculé sur toute la période, pas par barre."""
    columns = {"s": split_col, "x": x_col, "y": y_col} if split_col and split_first else {"x": x_col, "y": y_col}
    layer = {"layerId": LAYER, "layerType": "data", "seriesType": series, "xAccessor": "x",
             "accessors": ["y"], "position": "top", "showGridlines": False}
    if split_col:
        columns["s"] = split_col
        layer["splitAccessor"] = "s"
    vis = {"legend": {"isVisible": legend, "position": "right"}, "valueLabels": "hide",
           "preferredSeriesType": series, "layers": [layer],
           "axisTitlesVisibilitySettings": {"x": False, "yLeft": True, "yRight": True}}
    return lens(obj_id, title, dv, columns, vis, "lnsXY", query)


def table_vis(obj_id, title, dv, columns, query="", widths=None):
    """widths : largeur en pixels de certaines colonnes (ex. titres d'articles)."""
    widths = widths or {}
    vis = {"layerId": LAYER, "layerType": "data",
           "columns": [{"columnId": c, **({"width": widths[c]} if c in widths else {})} for c in columns]}
    return lens(obj_id, title, dv, columns, vis, "lnsDatatable", query)


def donut_vis(obj_id, title, dv, group_col, value_col, query=""):
    vis = {"shape": "donut", "layers": [{
        "layerId": LAYER, "layerType": "data", "primaryGroups": ["g"], "metrics": ["v"],
        "numberDisplay": "percent", "categoryDisplay": "default", "legendDisplay": "default",
        "nestedLegend": False}]}
    return lens(obj_id, title, dv, {"g": group_col, "v": value_col}, vis, "lnsPie", query)


# ------------------------------------------------------------- vues et dashboards
def data_view(obj_id, title, name, time_field, day_fields=()):
    """day_fields : dates sans heure, affichées au format AAAA-MM-JJ."""
    formats = {f: {"id": "date", "params": {"pattern": "YYYY-MM-DD"}} for f in day_fields}
    return {"id": obj_id, "type": "index-pattern", "coreMigrationVersion": "8.8.0",
            "typeMigrationVersion": "8.0.0",
            "attributes": {"title": title, "name": name, "timeFieldName": time_field,
                           "fieldFormatMap": json.dumps(formats)}, "references": []}


def saved_search(obj_id, title, dv, columns, sort, query=""):
    search_source = {"query": {"query": query, "language": "kuery"}, "filter": [],
                     "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"}
    return {"id": obj_id, "type": "search", "coreMigrationVersion": "8.8.0", "typeMigrationVersion": "8.0.0",
            "attributes": {"title": title, "description": "", "columns": columns, "sort": sort,
                           "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps(search_source)}},
            "references": [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                            "type": "index-pattern", "id": dv}]}


def options_controls(dv, fields):
    """Listes de choix (filtres) affichées en haut du dashboard."""
    panels, refs = {}, []
    for order, (field, title) in enumerate(fields):
        cid = f"ctrl-{field.replace('.', '-')}"
        panels[cid] = {"order": order, "width": "medium", "grow": True, "type": "optionsListControl",
                       "explicitInput": {"id": cid, "fieldName": field, "title": title,
                                         "searchTechnique": "prefix", "selectedOptions": []}}
        refs.append({"name": f"controlGroup_{cid}:optionsListDataView", "type": "index-pattern", "id": dv})
    control_group = {
        "controlStyle": "oneLine", "chainingSystem": "HIERARCHICAL", "showApplySelections": False,
        "panelsJSON": json.dumps(panels),
        "ignoreParentSettingsJSON": json.dumps({"ignoreFilters": False, "ignoreQuery": False,
                                                "ignoreTimerange": False, "ignoreValidations": False}),
    }
    return control_group, refs


def dashboard(obj_id, title, description, panels, time_from, time_to, controls=None, refresh=None):
    """panels : liste de (objet, x, y, largeur, hauteur) sur une grille de 48 colonnes."""
    panels_json, refs = [], []
    for i, (obj, x, y, w, h) in enumerate(panels):
        pid = str(i + 1)
        panels_json.append({"type": obj["type"], "panelIndex": pid, "panelRefName": f"panel_{pid}",
                            "gridData": {"x": x, "y": y, "w": w, "h": h, "i": pid},
                            "embeddableConfig": {"enhancements": {}}})
        refs.append({"name": f"{pid}:panel_{pid}", "type": obj["type"], "id": obj["id"]})
    attributes = {
        "title": title, "description": description, "panelsJSON": json.dumps(panels_json),
        "optionsJSON": json.dumps({"useMargins": True, "syncColors": False, "syncCursor": True,
                                   "syncTooltips": False, "hidePanelTitles": False}),
        "timeRestore": True, "timeFrom": time_from, "timeTo": time_to,
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps(
            {"query": {"query": "", "language": "kuery"}, "filter": []})},
    }
    if refresh:
        attributes["refreshInterval"] = {"pause": False, "value": refresh}
    if controls:
        attributes["controlGroupInput"], control_refs = controls
        refs += control_refs
    return {"id": obj_id, "type": "dashboard", "coreMigrationVersion": "8.8.0",
            "typeMigrationVersion": "8.9.0", "attributes": attributes, "references": refs}


# ================================================================ contenu
def monitoring_objects():
    dv = DV_MONITORING
    ingest = 'stage : "ingestion" and status : "success"'
    v = {
        "bronze": metric_vis("mon-bronze-total", "Lignes ingérées en Bronze", dv,
                             metric("sum", "records", "Lignes ingérées", NUMBER), ingest),
        "silver": metric_vis("mon-silver-total", "Lignes en Silver (dernier traitement)", dv,
                             last_value("records", "@timestamp", "Lignes Silver", "number"),
                             'stage : "cleaning" and source : "arxiv"', "ArXiv"),
        "gold": metric_vis("mon-gold-total", "Documents dans l'index Gold arxiv-papers", dv,
                           last_value("index_total", "@timestamp", "Documents indexés", "number"),
                           'stage : "indexing"'),
        "ge_rate": metric_vis("mon-ge-success", "Taux de réussite Great Expectations", dv,
                              metric("average", "success_percent", "Réussite moyenne (%)",
                                     {"id": "number", "params": {"decimals": 1, "suffix": " %"}}),
                              'stage : "validation"'),
        "volume_layer": table_vis("mon-volume-layer", "Volume par couche et par source (dernière exécution)", dv, {
            "l": terms("layer.keyword", "Couche", 5, "n"),
            "s": terms("source.keyword", "Source", 5, "n"),
            "n": last_value("records", "@timestamp", "Lignes (dernière exécution)", "number"),
            "t": max_date("@timestamp", "Dernière exécution"),
        }, 'stage : ("ingestion" or "cleaning" or "indexing")'),
        "freshness": table_vis("mon-freshness", "Fraîcheur des données : dernière exécution par étape", dv, {
            "s": terms("source.keyword", "Source", 5, "t"),
            "e": terms("stage.keyword", "Étape", 5, "t"),
            "t": max_date("@timestamp", "Dernière exécution"),
            "st": last_value("status.keyword", "@timestamp", "Dernier statut"),
        }),
        "ge_runs": xy_vis("mon-ge-runs", "Validations Great Expectations par statut", dv,
                          terms("suite.keyword", "Suite", 5, "y"), count("Validations"),
                          "bar_stacked", terms("status.keyword", "Statut", 3, "y"), 'stage : "validation"'),
        "ge_table": table_vis("mon-ge-table", "Détail des validations (taux d'erreur)", dv, {
            "s": terms("suite.keyword", "Suite", 5, "n"),
            "n": count("Validations"),
            "f": metric("sum", "failed_expectations", "Règles en échec", NUMBER),
            "p": metric("min", "success_percent", "Pire taux de réussite (%)"),
        }, 'stage : "validation"'),
        "hn_live": xy_vis("mon-hn-live", "Micro-batch Hacker News : lignes ingérées toutes les 5 minutes", dv,
                          date_hist("@timestamp", "Heure", "5m"),
                          metric("sum", "records", "Lignes ingérées", NUMBER), "bar",
                          query='source : "hn" and feed : "live"', legend=False),
        "durations": xy_vis("mon-durations", "Durée moyenne par étape (secondes)", dv,
                            terms("stage.keyword", "Étape", 5, "y"),
                            metric("average", "duration_s", "Durée moyenne (s)"), "bar_horizontal",
                            terms("source.keyword", "Source", 5, "y")),
    }
    board = dashboard(
        "dash-monitoring", "SciPulse — Monitoring du pipeline",
        "Volume ingéré par couche, taux d'erreur Great Expectations et fraîcheur des données.",
        [(v["bronze"], 0, 0, 12, 8), (v["silver"], 12, 0, 12, 8), (v["gold"], 24, 0, 12, 8),
         (v["ge_rate"], 36, 0, 12, 8),
         (v["volume_layer"], 0, 8, 24, 13), (v["freshness"], 24, 8, 24, 13),
         (v["ge_runs"], 0, 21, 24, 13), (v["ge_table"], 24, 21, 24, 13),
         (v["hn_live"], 0, 34, 32, 13), (v["durations"], 32, 34, 16, 13)],
        "now-24h", "now", refresh=60000)
    return list(v.values()) + [board]


def exploration_objects():
    dv = DV_ARXIV
    v = {
        "total": metric_vis("exp-total", "Articles indexés", dv, count("Articles")),
        "categories_n": metric_vis("exp-categories", "Catégories différentes", dv,
                                   metric("unique_count", "primary_category", "Catégories")),
        "authors": metric_vis("exp-authors", "Auteurs par article (moyenne)", dv,
                              metric("average", "n_authors", "Auteurs",
                                     {"id": "number", "params": {"decimals": 1}})),
        "doi": metric_vis("exp-doi", "Articles avec DOI", dv, count("Articles avec DOI"), "has_doi : true"),
        "domains": xy_vis("exp-domains", "Répartition des articles par domaine", dv,
                          terms("primary_domain", "Domaine", 15, "y"), count("Articles"),
                          "bar_horizontal", legend=False),
        "cats": donut_vis("exp-cats", "15 premières catégories", dv,
                          terms("primary_category", "Catégorie", 15, "v", other=True), count("Articles")),
        "timeline": xy_vis("exp-timeline", "Évolution des publications par année (5 premiers domaines)", dv,
                           date_hist("submitted_date", "Année", "1y"), count("Articles"),
                           "bar_stacked", terms("primary_domain", "Domaine", 5, "y", other=True),
                           split_first=True),
        "recent_by_domain": table_vis("exp-recent-domain", "Article le plus récent par domaine", dv, {
            "d": terms("primary_domain", "Domaine", 20, "n"),
            "t": max_date("submitted_date", "Soumis le"),
            "ti": last_value("title.raw", "submitted_date", "Titre"),
            "id": last_value("arxiv_id", "submitted_date", "arxiv_id"),
            "n": count("Articles du domaine"),
        }),
    }
    search = saved_search("search-recent", "Articles les plus récents", dv,
                          ["primary_domain", "primary_category", "title", "authors", "doi"],
                          [["submitted_date", "desc"]])
    controls = options_controls(dv, [("primary_domain", "Domaine"), ("primary_category", "Catégorie"),
                                     ("license", "Licence")])
    board = dashboard(
        "dash-exploration", "SciPulse — Exploration scientifique ArXiv",
        "Distribution des catégories, évolution temporelle et articles les plus récents par domaine. "
        "Recherche plein texte via la barre de requête (titre, résumé, auteurs).",
        [(v["total"], 0, 0, 12, 7), (v["categories_n"], 12, 0, 12, 7), (v["authors"], 24, 0, 12, 7),
         (v["doi"], 36, 0, 12, 7),
         (v["timeline"], 0, 7, 48, 14),
         (v["domains"], 0, 21, 24, 15), (v["cats"], 24, 21, 24, 15),
         (v["recent_by_domain"], 0, 36, 48, 15),
         (search, 0, 51, 48, 18)],
        "1986-01-01T00:00:00.000Z", "now", controls=controls)
    return list(v.values()) + [search, board]


def article_table(obj_id, title, dv, measures, query=""):
    """Tableau d'articles (un par ligne) ; measures : colonnes chiffrées, la première sert au tri."""
    columns = {"id": terms("arxiv_id", "arxiv_id", 20, "m0"),
               "ti": last_value("title.raw", "submitted_date", "Titre"),
               "c": last_value("primary_category", "submitted_date", "Catégorie")}
    columns.update({f"m{i}": col for i, col in enumerate(measures)})
    return table_vis(obj_id, title, dv, columns, query, widths={"ti": 560})


def insights_objects():
    dv = DV_ENRICHED
    v = {
        "total": metric_vis("ins-total", "Articles notés", dv, count("Articles")),
        "score": metric_vis("ins-score", "Score d'impact moyen (sur 100)", dv,
                            metric("average", "impact_score", "Score moyen", DECIMAL)),
        "cited": metric_vis("ins-cited", "Citations OpenAlex reçues", dv,
                            metric("sum", "cited_by_count", "Citations", NUMBER)),
        "hn": metric_vis("ins-hn", "Articles passés sur Hacker News", dv, count("Articles sur HN"), "on_hn : true"),
        "top": article_table("ins-top", "Articles au plus fort score d'impact", dv, [
            metric("max", "impact_score", "Score d'impact", DECIMAL),
            metric("max", "cited_by_count", "Citations", NUMBER),
            metric("max", "hn_score_total", "Points HN", NUMBER)]),
        "score_domain": xy_vis("ins-score-domain", "Score d'impact moyen par domaine", dv,
                               terms("primary_domain", "Domaine", 15, "y"),
                               metric("average", "impact_score", "Score moyen", DECIMAL),
                               "bar_horizontal", legend=False),
        "citations_year": xy_vis("ins-citations-year", "Citations reçues selon l'année de publication", dv,
                                 date_hist("submitted_date", "Année", "1y"),
                                 metric("sum", "cited_by_count", "Citations", NUMBER), "bar", legend=False),
        "velocity": xy_vis("ins-velocity", "Vélocité de citation moyenne des 15 catégories les plus publiées", dv,
                           terms_by_count("primary_category", "Catégorie", 15),
                           metric("average", "velocite_citations", "Citations par an (2 dernières années)",
                                  DECIMAL), "bar_horizontal", legend=False),
        # Cas d'usage 1 : reconnus par la recherche, ignorés par la communauté tech
        "cited_not_hn": article_table("ins-cited-not-hn", "Très cités, jamais mentionnés sur Hacker News", dv, [
            metric("max", "cited_by_count", "Citations", NUMBER),
            metric("max", "velocite_citations", "Citations par an", DECIMAL),
            metric("max", "impact_score", "Score d'impact", DECIMAL)], "on_hn : false"),
        # Cas d'usage 2 : succès sur HN, citations dans le quart inférieur de leur cohorte (domaine, année)
        "viral_low_cited": article_table(
            "ins-viral-low-cited", "Viraux sur Hacker News mais peu cités (quart inférieur de leur cohorte)", dv, [
                metric("max", "hn_score_total", "Points HN", NUMBER),
                metric("max", "cited_by_count", "Citations", NUMBER),
                metric("max", "impact_score", "Score d'impact", DECIMAL)],
            "on_hn : true and score_citations < 0.25"),
        # Cas d'usage 3 : croissance des publications ET des citations (dernière année complète)
        "growth": table_vis("ins-growth", "Catégories en croissance : publications et citations (dernière année)", dv, {
            "c": terms("primary_category", "Catégorie", 20, "gc"),
            "p": metric("max", "publications_categorie", "Publications", NUMBER),
            "gp": metric("max", "croissance_publications_categorie", "Croissance publications", PERCENT),
            "n": metric("max", "citations_categorie", "Citations reçues", NUMBER),
            "gc": metric("max", "croissance_citations_categorie", "Croissance citations", PERCENT),
        }, "croissance_publications_categorie > 0 and croissance_citations_categorie > 0 "
           "and publications_categorie >= 500"),
    }
    controls = options_controls(dv, [("primary_domain", "Domaine"), ("primary_category", "Catégorie"),
                                     ("on_hn", "Passé sur HN")])
    board = dashboard(
        "dash-insights", "SciPulse Insights — Score d'impact et citations",
        "Couche Gold complète (arxiv-papers-enriched) : score d'impact composite, indicateurs de citation "
        "OpenAlex, signal Hacker News et cas d'usage analytiques.",
        [(v["total"], 0, 0, 12, 7), (v["score"], 12, 0, 12, 7), (v["cited"], 24, 0, 12, 7),
         (v["hn"], 36, 0, 12, 7),
         (v["top"], 0, 7, 48, 15),
         (v["score_domain"], 0, 22, 16, 14), (v["citations_year"], 16, 22, 16, 14),
         (v["velocity"], 32, 22, 16, 14),
         (v["cited_not_hn"], 0, 36, 48, 15),
         (v["viral_low_cited"], 0, 51, 48, 15),
         (v["growth"], 0, 66, 48, 15)],
        "1986-01-01T00:00:00.000Z", "now", controls=controls)
    return list(v.values()) + [board]


def main():
    objects = [
        data_view(DV_ARXIV, "arxiv-papers", "ArXiv — articles (Gold)", "submitted_date",
                  day_fields=("submitted_date", "last_version_date", "update_date")),
        data_view(DV_MONITORING, "scipulse-monitoring", "SciPulse — monitoring du pipeline", "@timestamp"),
        data_view(DV_ENRICHED, "arxiv-papers-enriched", "ArXiv enrichi — score d'impact (Gold)", "submitted_date",
                  day_fields=("submitted_date",)),
        *monitoring_objects(),
        *exploration_objects(),
        *insights_objects(),
    ]
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text("\n".join(json.dumps(o, ensure_ascii=False) for o in objects) + "\n", encoding="utf-8")
    print(f"{len(objects)} objets Kibana écrits dans {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
