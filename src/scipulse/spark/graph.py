"""Option B : graphe de citations (GraphFrames), PageRank, composantes connexes et communautés.

Sommets : les articles du périmètre du score (identifiant arXiv).
Arêtes : « l'article A cite l'article B », quand A et B sont tous deux dans le périmètre.
"""
from __future__ import annotations

from graphframes import GraphFrame
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

PAGERANK_RESET = 0.15        
PAGERANK_ITERATIONS = 10
COMMUNITY_ITERATIONS = 5     


def citation_graph(citations: DataFrame, articles: DataFrame) -> GraphFrame:
    """Graphe orienté des citations entre les articles donnés (colonne arxiv_id)."""
    vertices = articles.select(F.col("arxiv_id").alias("id")).distinct()
    work_to_article = citations.select("openalex_id", F.col("arxiv_id").alias("dst"))
    edges = (
        citations.select(F.col("arxiv_id").alias("src"), F.explode("referenced_works").alias("openalex_id"))
        .join(work_to_article, "openalex_id")                         # référence -> article arXiv cité
        .where(F.col("src") != F.col("dst"))
        .select("src", "dst").distinct()
        .join(vertices.withColumnRenamed("id", "src"), "src", "left_semi")   # les deux extrémités
        .join(vertices.withColumnRenamed("id", "dst"), "dst", "left_semi")   # dans le périmètre
    )
   
    return GraphFrame(vertices.checkpoint(), edges.checkpoint())


def graph_metrics(graph: GraphFrame) -> DataFrame:
    """Par article : PageRank, citations reçues dans le graphe, composante connexe et communauté."""
    pagerank = graph.pageRank(resetProbability=PAGERANK_RESET, maxIter=PAGERANK_ITERATIONS).vertices
    in_degree = graph.inDegrees.withColumnRenamed("inDegree", "citations_graphe")
    # Version GraphX (comme PageRank et la propagation de labels) : plan d'exécution qui ne grossit pas
    components = graph.connectedComponents(algorithm="graphx").withColumnRenamed("component", "composante")
    communities = graph.labelPropagation(maxIter=COMMUNITY_ITERATIONS).withColumnRenamed("label", "communaute")
    return (
        pagerank.select(F.col("id").alias("arxiv_id"), "pagerank")
        .join(in_degree.withColumnRenamed("id", "arxiv_id"), "arxiv_id", "left")
        .join(components.select(F.col("id").alias("arxiv_id"), "composante"), "arxiv_id", "left")
        .join(communities.select(F.col("id").alias("arxiv_id"), "communaute"), "arxiv_id", "left")
        .fillna({"citations_graphe": 0})
    )


def graph_summary(graph: GraphFrame, metrics: DataFrame, articles: DataFrame, top: int = 10) -> dict:
    """Taille du graphe, composantes, et comparaison des communautés avec les catégories ArXiv."""
    with_category = metrics.join(articles.select("arxiv_id", "primary_category"), "arxiv_id")
    components = metrics.groupBy("composante").count()

    # Communauté -> catégorie ArXiv la plus fréquente et sa part (pureté)
    by_category = with_category.groupBy("communaute", "primary_category").count()
    w = Window.partitionBy("communaute").orderBy(F.desc("count"))
    communities = (
        by_category.withColumn("rang", F.row_number().over(w))
        .groupBy("communaute")
        .agg(F.sum("count").alias("articles"),
             F.max(F.when(F.col("rang") == 1, F.col("primary_category"))).alias("categorie_dominante"),
             F.max(F.when(F.col("rang") == 1, F.col("count"))).alias("articles_categorie_dominante"))
        .where("articles >= 2")                                        # les articles isolés ne forment pas une communauté
        .withColumn("purete", F.round(F.col("articles_categorie_dominante") / F.col("articles"), 3))
        .cache()
    )
    totals = communities.agg(F.count("*").alias("n"), F.sum("articles").alias("articles"),
                             F.sum("articles_categorie_dominante").alias("dominants")).first()
    return {
        "sommets": graph.vertices.count(),
        "aretes": graph.edges.count(),
        "articles_relies": graph.degrees.count(),                  # au moins une arête
        "articles_cites_dans_le_graphe": metrics.where("citations_graphe > 0").count(),
        "composantes": components.count(),
        "plus_grande_composante": components.agg(F.max("count")).first()[0],
        "communautes_2_articles_et_plus": totals["n"],
        "articles_dans_ces_communautes": totals["articles"],
        "purete_moyenne_ponderee": round(totals["dominants"] / max(totals["articles"] or 1, 1), 3),
        "plus_grandes_communautes": [r.asDict() for r in communities.orderBy(F.desc("articles")).limit(top).collect()],
    }
