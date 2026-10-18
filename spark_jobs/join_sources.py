"""Job Spark : jointure des trois sources Silver et taux de correspondance.

    spark-submit --master spark://spark-master:7077 spark_jobs/join_sources.py [--compare]
--compare : exécute aussi la jointure en sort-merge pour comparer les deux stratégies.
Résultats : logs/spark/join_report.json et les plans d'exécution (logs/spark/plan_*.txt).
"""
import argparse
import json
import time
from pathlib import Path

from scipulse import config, monitoring
from scipulse.spark import joins, sources
from scipulse.spark.session import get_spark

REPORT_DIR = config.LOG_DIR / "spark"


# Colonnes ArXiv utiles à la jointure : Spark ne lit que ces colonnes dans les fichiers Parquet
JOIN_COLUMNS = ["arxiv_id", "primary_domain", "primary_category", "submitted_date"]


def main(compare: bool = False) -> dict:
    start = time.time()
    spark = get_spark("scipulse-jointure")
    arxiv = sources.load_arxiv(spark).select(*JOIN_COLUMNS)
    hn, citations = sources.load_hn(spark), sources.load_citations(spark)
    hn_agg = joins.hn_by_paper(hn).cache()

    comparison = joins.compare_strategies(spark, arxiv, citations, hn_agg) if compare else {}
    cover = joins.coverage(joins.join_sources(arxiv, citations, hn_agg, "broadcast"), citations, hn_agg)

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    for strategy, res in comparison.items():
        Path(REPORT_DIR / f"plan_{strategy}.txt").write_text(res.pop("plan"), encoding="utf-8")
    report = {"taux_de_correspondance": cover, "strategies": comparison,
              "duree_totale_s": round(time.time() - start, 1)}
    Path(REPORT_DIR / "join_report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False),
                                                     encoding="utf-8")
    monitoring.emit("spark", "gold", "enriched", records=cover["articles"], with_citations=cover["avec_citations"],
                    on_hn=cover["sur_hn"], three_sources=cover["trois_sources"],
                    duration_s=report["duree_totale_s"], job="join_sources")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    spark.stop()
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--compare", action="store_true", help="comparer broadcast et sort-merge")
    main(parser.parse_args().compare)
