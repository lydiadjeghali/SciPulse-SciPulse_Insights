"""Job Spark Gold : indicateurs par catégorie, TF-IDF et score d'impact composite.

    spark-submit --master spark://spark-master:7077 spark_jobs/gold_enrichment.py

Périmètre du score : articles avec citations OpenAlex et/ou passés sur Hacker News.
Résultats : logs/spark/gold_report.json.
"""
import argparse
import json
import time
from datetime import date

from pyspark.sql import functions as F

from scipulse import config, monitoring
from scipulse.ingestion.arxiv import latest_snapshot_key
from scipulse.spark import analytics, joins, sources
from scipulse.spark.session import get_spark

ARXIV_COLUMNS = ["arxiv_id", "title", "abstract", "authors", "categories", "primary_category", "primary_domain",
                 "doi", "submitted_date", "published_year"]


def rows(df, n=10):
    return [r.asDict() for r in df.limit(n).collect()]


def main(sample: float = 0.0) -> dict:
    start = time.time()
    spark = get_spark("scipulse-gold")
    snapshot_date = latest_snapshot_key().split("/")[0].split("=")[1]
    last_complete_year = date.fromisoformat(snapshot_date).year - 1

    arxiv = sources.load_arxiv(spark).select(*ARXIV_COLUMNS)
    citations = sources.load_citations(spark)
    hn_agg = joins.hn_by_paper(sources.load_hn(spark)).cache()

    # Croissance annuelle des publications par catégorie (corpus complet)
    growth = analytics.publication_growth(arxiv).cache()

    #  Jointure (broadcast) et périmètre du score
    scope = joins.join_sources(arxiv, citations, hn_agg, "broadcast").where("has_citations OR on_hn")
    if sample:
        scope = scope.sample(fraction=sample, seed=42)
    scope = analytics.citation_velocity(scope, last_complete_year).persist()
    velocity_by_cat = analytics.citation_velocity_by_category(scope.where("has_citations"))

    #  TF-IDF des résumés et mots-clés ; point de reprise sur disque
    enriched, vocab_size = analytics.tfidf(scope)
    enriched = enriched.drop("tfidf", "abstract").checkpoint()

    # Score d'impact composite
    scored = analytics.impact_score(enriched, snapshot_date).checkpoint()

    report = {
        "snapshot": snapshot_date, "annees_velocite": [last_complete_year - 1, last_complete_year],
        "articles_score": scored.count(), "vocabulaire_tfidf": vocab_size,
        "ponderations": analytics.WEIGHTS,
        "score_stats": scored.select(*[F.round(f(F.col("impact_score")), 2).alias(n) for n, f in
                                       [("min", F.min), ("moyenne", F.avg), ("max", F.max)]]).first().asDict(),
        "top_score": rows(scored.orderBy(F.desc("impact_score")).select(
            "arxiv_id", "title", "primary_category", "cited_by_count", "hn_score_total",
            "published_year", "mots_cles", "impact_score")),
        "croissance_categories": rows(growth.where(
            (F.col("published_year") == last_complete_year) & (F.col("publications_annee_precedente") >= 500))
            .orderBy(F.desc("croissance_publications"))),
        "velocite_categories": rows(velocity_by_cat.where(
            (F.col("annee") == last_complete_year) & (F.col("citations_recues") >= 1000))
            .orderBy(F.desc("croissance_citations"))),
    }
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
    main(parser.parse_args().sample)
