"""Analytique avancée : indicateurs temporels par catégorie, TF-IDF et score d'impact composite."""
from __future__ import annotations

from pyspark.ml import Pipeline
from pyspark.ml.feature import IDF, CountVectorizer, RegexTokenizer, StopWordsRemover
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

# Pondérations du score d'impact
WEIGHTS = {"citations": 0.35, "pagerank": 0.10, "velocite": 0.15, "hn": 0.25, "recence": 0.15}
RECENCY_TAU_YEARS = 3.0          
VELOCITY_YEARS = 2               # vélocité = citations des 2 dernières années complètes


# ------------------------------------------------------------- indicateurs
def publication_growth(arxiv: DataFrame) -> DataFrame:
    """Publications par catégorie et par année, et croissance par rapport à l'année précédente."""
    by_year = arxiv.groupBy("primary_category", "published_year").agg(F.count("*").alias("publications"))
    w = Window.partitionBy("primary_category").orderBy("published_year")
    previous = F.lag("publications").over(w)
    return by_year.withColumn("publications_annee_precedente", previous).withColumn(
        "croissance_publications", F.round((F.col("publications") - previous) / previous, 4))


def citation_velocity(df: DataFrame, last_complete_year: int) -> DataFrame:
    """Vélocité de citation par article : citations reçues pendant les dernières années complètes."""
    years = list(range(last_complete_year - VELOCITY_YEARS + 1, last_complete_year + 1))
    recent = F.filter("counts_by_year", lambda c: c["year"].isin(years))
    return df.withColumn(
        "velocite_citations",
        F.coalesce(F.aggregate(recent, F.lit(0).cast("long"), lambda acc, c: acc + c["cited_by_count"]),
                   F.lit(0).cast("long")) / F.lit(VELOCITY_YEARS))


def citation_velocity_by_category(df: DataFrame) -> DataFrame:
    """Citations reçues par catégorie et par année, et croissance d'une année sur l'autre."""
    per_year = (df.select("primary_category", F.explode("counts_by_year").alias("c"))
                .groupBy("primary_category", F.col("c.year").alias("annee"))
                .agg(F.sum("c.cited_by_count").alias("citations_recues")))
    w = Window.partitionBy("primary_category").orderBy("annee")
    previous = F.lag("citations_recues").over(w)
    return per_year.withColumn("croissance_citations",
                               F.round((F.col("citations_recues") - previous) / previous, 4))



def category_indicators(growth: DataFrame, velocity_by_cat: DataFrame, year: int) -> DataFrame:
    """Indicateurs de la catégorie pour une année, à ajouter à chaque article de la catégorie."""
    publications = growth.where(F.col("published_year") == year).select(
        "primary_category", F.col("publications").alias("publications_categorie"),
        F.col("croissance_publications").alias("croissance_publications_categorie"))
    citations = velocity_by_cat.where(F.col("annee") == year).select(
        "primary_category", F.col("citations_recues").alias("citations_categorie"),
        F.col("croissance_citations").alias("croissance_citations_categorie"))
    return publications.join(citations, "primary_category", "full")

# ------------------------------------------------------------- TF-IDF
def tfidf(df: DataFrame, vocab_size: int = 50_000, min_df: int = 5, top_k: int = 5) -> tuple[DataFrame, int]:
    """TF-IDF des résumés (Spark MLlib). Renvoie les données avec les mots-clés de chaque article
    (termes au plus fort poids TF-IDF) et la taille du vocabulaire."""
    pipeline = Pipeline(stages=[
        RegexTokenizer(inputCol="abstract", outputCol="mots", pattern="[^a-zA-Z]+", minTokenLength=3),
        StopWordsRemover(inputCol="mots", outputCol="mots_utiles"),
        CountVectorizer(inputCol="mots_utiles", outputCol="tf", vocabSize=vocab_size, minDF=min_df),
        IDF(inputCol="tf", outputCol="tfidf"),
    ])
    model = pipeline.fit(df)
    vocabulary = model.stages[2].vocabulary
    vocab = df.sparkSession.sparkContext.broadcast(vocabulary)

    @F.udf(ArrayType(StringType()))
    def top_terms(vector):
        if vector is None or vector.numNonzeros() == 0:
            return []
        best = sorted(zip(vector.values, vector.indices), reverse=True)[:top_k]
        return [vocab.value[int(i)] for _, i in best]

    out = model.transform(df).withColumn("mots_cles", top_terms("tfidf"))
    return out.drop("mots", "mots_utiles", "tf"), len(vocabulary)


# ------------------------------------------------------------- score d'impact
def impact_score(df: DataFrame, reference_date: str) -> DataFrame:
    """Score composite sur 100 = 0,35·C + 0,10·P + 0,15·V + 0,25·H + 0,15·R (chaque composante entre 0 et 1).

    C : rang percentile des citations dans sa cohorte (même domaine, même année)
    P : rang percentile du PageRank (graphe de citations) dans son domaine
    V : rang percentile de la vélocité de citation dans son domaine
    H : signal Hacker News en échelle logarithmique, 0 si l'article n'est jamais passé sur HN
    R : récence
    """
    cohort = Window.partitionBy("primary_domain", "published_year").orderBy("cited_by_count")
    domain = Window.partitionBy("primary_domain").orderBy("velocite_citations")
    domain_pagerank = Window.partitionBy("primary_domain").orderBy("pagerank")
    hn_log = F.log1p(F.coalesce(F.col("hn_score_total"), F.lit(0)) + F.coalesce(F.col("hn_comments_total"), F.lit(0)))
    age_years = F.datediff(F.to_date(F.lit(reference_date)), F.col("submitted_date")) / F.lit(365.25)

    df = df.withColumn("hn_log", hn_log)
    hn_log_max = df.agg(F.max("hn_log")).first()[0] or 1.0      # calculé une seule fois
    scored = (
        df.withColumn("score_citations", F.percent_rank().over(cohort))
        .withColumn("score_pagerank", F.percent_rank().over(domain_pagerank))
        .withColumn("score_velocite", F.percent_rank().over(domain))
        .withColumn("score_hn", F.col("hn_log") / F.lit(hn_log_max))
        .withColumn("score_recence", F.exp(-F.greatest(age_years, F.lit(0)) / F.lit(RECENCY_TAU_YEARS)))
    )
    total = (F.lit(WEIGHTS["citations"]) * F.col("score_citations")
             + F.lit(WEIGHTS["pagerank"]) * F.col("score_pagerank")
             + F.lit(WEIGHTS["velocite"]) * F.col("score_velocite")
             + F.lit(WEIGHTS["hn"]) * F.col("score_hn")
             + F.lit(WEIGHTS["recence"]) * F.col("score_recence"))
    return scored.withColumn("impact_score", F.round(F.lit(100) * total, 2)).drop("hn_log")
