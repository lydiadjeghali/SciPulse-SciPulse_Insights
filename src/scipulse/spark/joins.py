"""Jointure des trois sources Silver, taux de correspondance et choix de la stratégie de jointure.

Table centrale : les articles ArXiv. Jointures externes à gauche sur arxiv_id :
on garde tous les articles, même sans citations ni passage sur Hacker News
(indispensable pour trouver « très cités mais jamais sur HN »).
"""
from __future__ import annotations

import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

CITATION_COLUMNS = ["arxiv_id", "openalex_id", "cited_by_count", "counts_by_year", "publication_year",
                    "topic", "subfield", "field", "domain", "referenced_works_count"]


def hn_by_paper(hn: DataFrame) -> DataFrame:
    """Une ligne par article cité sur HN (plusieurs stories peuvent pointer vers le même article)."""
    return (
        hn.where(F.col("has_arxiv_link"))
        .groupBy("arxiv_id")
        .agg(
            F.count("*").alias("hn_stories"),
            F.sum("score").alias("hn_score_total"),
            F.max("score").alias("hn_score_max"),
            F.sum("num_comments").alias("hn_comments_total"),
            F.min("created_at").alias("hn_first_seen"),
            F.max("created_at").alias("hn_last_seen"),
        )
    )


def join_sources(arxiv: DataFrame, citations: DataFrame, hn_agg: DataFrame, strategy: str) -> DataFrame:
    """Jointure à trois sources. strategy : « broadcast » ou « sortmerge »."""
    cit = citations.select(*CITATION_COLUMNS).withColumnRenamed("publication_year", "openalex_year")
    if strategy == "broadcast":
        cit, hn_agg = F.broadcast(cit), F.broadcast(hn_agg)
    elif strategy == "sortmerge":
        cit, hn_agg = cit.hint("merge"), hn_agg.hint("merge")
    else:
        raise ValueError(f"Stratégie inconnue : {strategy}")
    return (
        arxiv.join(cit, "arxiv_id", "left")
        .join(hn_agg, "arxiv_id", "left")
        .withColumn("has_citations", F.col("openalex_id").isNotNull())
        .withColumn("on_hn", F.col("hn_stories").isNotNull())
    )


def coverage(joined: DataFrame, citations: DataFrame, hn_agg: DataFrame) -> dict:
    """Taux de correspondance entre les sources."""
    row = joined.agg(
        F.count("*").alias("articles"),
        F.sum(F.col("has_citations").cast("int")).alias("avec_citations"),
        F.sum(F.col("on_hn").cast("int")).alias("sur_hn"),
        F.sum((F.col("has_citations") & F.col("on_hn")).cast("int")).alias("trois_sources"),
    ).first().asDict()
    cit_total, hn_total = citations.count(), hn_agg.count()
    row.update({
        "citations_total": cit_total,
        "citations_orphelines": cit_total - row["avec_citations"],   # sans article dans arxiv-clean
        "hn_articles_total": hn_total,
        "hn_orphelins": hn_total - row["sur_hn"],
    })
    n = row["articles"]
    row.update({
        "taux_citations": round(row["avec_citations"] / n, 4),
        "taux_hn": round(row["sur_hn"] / n, 4),
        "taux_hn_avec_citations": round(row["trois_sources"] / max(row["sur_hn"], 1), 4),
        "taux_citations_rattachees": round(row["avec_citations"] / max(cit_total, 1), 4),
        "taux_hn_rattaches": round(row["sur_hn"] / max(hn_total, 1), 4),
    })
    return row


def physical_plan(df: DataFrame, spark: SparkSession) -> str:
    """Plan d'exécution physique, comme df.explain('formatted'), renvoyé sous forme de texte."""
    mode = spark._jvm.org.apache.spark.sql.execution.ExplainMode.fromString("formatted")
    return df._jdf.queryExecution().explainString(mode)


def timed_run(df: DataFrame) -> float:
    """Exécute entièrement la jointure (sans rien écrire) et renvoie sa durée en secondes."""
    start = time.time()
    df.write.format("noop").mode("overwrite").save()
    return round(time.time() - start, 1)


def compare_strategies(spark: SparkSession, arxiv: DataFrame, citations: DataFrame, hn_agg: DataFrame) -> dict:
    """Même jointure en broadcast puis en sort-merge : plans d'exécution et durées."""
    result = {}
    for strategy in ("broadcast", "sortmerge"):
        joined = join_sources(arxiv, citations, hn_agg, strategy)
        plan = physical_plan(joined, spark)
        result[strategy] = {
            "duree_s": timed_run(joined),
            "broadcast_hash_joins": plan.count("BroadcastHashJoin"),
            "sort_merge_joins": plan.count("SortMergeJoin"),
            "echanges_shuffle": plan.count("Exchange hashpartitioning"),
            "plan": plan,
        }
    return result
