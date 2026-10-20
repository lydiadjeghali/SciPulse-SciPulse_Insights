"""Job Spark Gold : indicateurs par catégorie, graphe de citations (PageRank), TF-IDF,
score d'impact composite et écriture dans l'index Elasticsearch arxiv-papers-enriched.

    spark-submit --master spark://spark-master:7077 spark_jobs/gold_enrichment.py

Périmètre du score : articles avec citations OpenAlex et/ou passés sur Hacker News.
Le résultat est d'abord écrit en Parquet (bucket technique gold-staging), puis publié dans l'index arxiv-papers-enriched
Résultats : index arxiv-papers-enriched et logs/spark/gold_report.json.
"""
import argparse
import json
import time
from datetime import date

from pyspark.sql import functions as F

from scipulse import config, monitoring
from scipulse.ingestion.arxiv import latest_snapshot_key
from scipulse.spark import analytics, gold, graph, joins, sources
from scipulse.spark.session import get_spark

ARXIV_COLUMNS = ["arxiv_id", "title", "abstract", "authors", "categories", "primary_category", "primary_domain",
                 "doi", "submitted_date", "published_year"]


def rows(df, n=10):
    return [r.asDict() for r in df.limit(n).collect()]


STAGING_PATH = f"s3a://{config.GOLD_STAGING}/arxiv-papers-enriched/"


def last_complete_year() -> int:
    snapshot_date = latest_snapshot_key().split("/")[0].split("=")[1]
    return date.fromisoformat(snapshot_date).year - 1


def publish(spark, year: int, growth=None) -> dict:
    """Publie dans Elasticsearch le dernier résultat calculé, avec les indicateurs de sa catégorie."""
    start = time.time()
    scored = spark.read.parquet(STAGING_PATH)
    if growth is None:
        growth = analytics.publication_growth(sources.load_arxiv(spark).select("primary_category", "published_year"))
    velocity_by_cat = analytics.citation_velocity_by_category(scored.where("has_citations"))
    categories = analytics.category_indicators(growth, velocity_by_cat, year)
    total = gold.write_gold(scored.join(F.broadcast(categories), "primary_category", "left"))
    result = {"documents_index": total, "duree_ecriture_es_s": round(time.time() - start, 1)}
    monitoring.emit("indexing", "gold", "enriched", records=total, index_total=total,
                    duration_s=result["duree_ecriture_es_s"])
    return result


def main(sample: float = 0.0, write_es: bool = True) -> dict:
    start = time.time()
    spark = get_spark("scipulse-gold")
    snapshot_date = latest_snapshot_key().split("/")[0].split("=")[1]
    year = last_complete_year()

    arxiv = sources.load_arxiv(spark).select(*ARXIV_COLUMNS)
    citations = sources.load_citations(spark)
    hn_agg = joins.hn_by_paper(sources.load_hn(spark)).cache()

    # Croissance annuelle des publications par catégorie (corpus complet)
    growth = analytics.publication_growth(arxiv).cache()

    #  Jointure (broadcast) et périmètre du score
    scope = joins.join_sources(arxiv, citations, hn_agg, "broadcast").where("has_citations OR on_hn")
    if sample:
        scope = scope.sample(fraction=sample, seed=42)
    # Point de reprise sur disque : la jointure et la lecture des 3 M d'articles ne sont faites qu'une fois
    scope = analytics.citation_velocity(scope, year).checkpoint()
    velocity_by_cat = analytics.citation_velocity_by_category(scope.where("has_citations"))

    # Graphe de citations (GraphFrames) : PageRank, composantes connexes, communautés ; point de reprise
    citation_graph = graph.citation_graph(citations, scope)
    metrics = graph.graph_metrics(citation_graph).checkpoint()
    graph_report = graph.graph_summary(citation_graph, metrics, scope)
    scope = scope.join(metrics, "arxiv_id", "left").checkpoint()

    #  TF-IDF des résumés et mots-clés ; point de reprise sur disque
    enriched, vocab_size = analytics.tfidf(scope)
    enriched = enriched.drop("tfidf", "abstract").checkpoint()

    # Score d'impact composite, enregistré en Parquet avant publication
    analytics.impact_score(enriched, snapshot_date).write.mode("overwrite").parquet(STAGING_PATH)
    scored = spark.read.parquet(STAGING_PATH)

    report = {
        "snapshot": snapshot_date, "annees_velocite": [year - 1, year],
        "articles_score": scored.count(), "vocabulaire_tfidf": vocab_size,
        "ponderations": analytics.WEIGHTS,
        "score_stats": scored.select(*[F.round(f(F.col("impact_score")), 2).alias(n) for n, f in
                                       [("min", F.min), ("moyenne", F.avg), ("max", F.max)]]).first().asDict(),
        "graphe": graph_report,
        "top_pagerank": rows(scored.orderBy(F.desc("pagerank")).select(
            "arxiv_id", "title", "primary_category", "cited_by_count", "citations_graphe", "pagerank",
            "impact_score")),
        "top_score": rows(scored.orderBy(F.desc("impact_score")).select(
            "arxiv_id", "title", "primary_category", "cited_by_count", "hn_score_total",
            "published_year", "mots_cles", "impact_score")),
        "croissance_categories": rows(growth.where(
            (F.col("published_year") == year) & (F.col("publications_annee_precedente") >= 500))
            .orderBy(F.desc("croissance_publications"))),
        "velocite_categories": rows(velocity_by_cat.where(
            (F.col("annee") == year) & (F.col("citations_recues") >= 1000))
            .orderBy(F.desc("croissance_citations"))),
    }
    # Couche Gold : publication dans Elasticsearch (elasticsearch-hadoop)
    if write_es:
        report.update(publish(spark, year, growth))
    report["duree_totale_s"] = round(time.time() - start, 1)
    out = config.LOG_DIR / "spark"
    out.mkdir(parents=True, exist_ok=True)
    (out / "gold_report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str),
                                          encoding="utf-8")
    monitoring.emit("spark", "gold", "enriched", records=report["articles_score"], job="gold_enrichment",
                    sample=sample, duration_s=report["duree_totale_s"])
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    spark.stop()
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=float, default=0.0, help="fraction du périmètre pour un test")
    parser.add_argument("--no-es", action="store_true", help="ne pas écrire dans Elasticsearch")
    parser.add_argument("--publish-only", action="store_true", help="publier le dernier résultat sans recalcul")
    args = parser.parse_args()
    if args.publish_only:
        session = get_spark("scipulse-gold-publication")
        print(publish(session, last_complete_year()))
        session.stop()
    else:
        main(args.sample, write_es=not args.no_es)
