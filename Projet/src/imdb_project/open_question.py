"""Open question: is the IMDb acting world a "small world", and who is its
real centre?

The co-star graph has one vertex per actor / actress and an edge between two
people who appear in the same feature film (``title.principals``, so the
billed cast only, at most 10 actors per film). Only feature films with at
least ``MIN_VOTES`` votes are used.

The distance between two actors is the number of steps of the shortest chain
actor -> film -> actor -> ... joining them (two co-stars are at distance 1).

All graph algorithms are breadth-first searches (BFS) written with DataFrame
joins: at each level, the frontier (people reached at the previous level) is
joined with the edges, and the people already visited are removed with an
anti-join. Each level is written to Parquet and read back, which keeps the
Spark plan (lineage) short whatever the number of levels.

Several BFS run at once (multi-source BFS): every row carries the ``source``
it comes from.

Finding the real centre without computing all the pairwise distances
(impossible: about 10^10 pairs):

1. the giant connected component is found by a BFS from the most connected
   actor;
2. ``sample_size`` actors of that component, drawn at random with a fixed
   seed (the same on every run), are used as sources: the average distance
   of every actor to them estimates their closeness (Eppstein & Wang,
   2004, "Fast approximation of centrality");
3. the best ``candidates`` actors of that estimate, the ``top_degree``
   actors with the most co-stars (a safety net: the estimate alone can miss
   the real centre) and Kevin Bacon get an exact BFS; the centre is the one
   with the smallest exact average distance to all the other actors of the
   component.
"""

import shutil
import uuid

from pyspark.sql import functions as F

from imdb_project.definitions import ACTING_CATEGORIES, MIN_VOTES, feature_films, rated

KEVIN_BACON = "nm0000102"
DECADES = [(1980, 1989), (1990, 1999), (2000, 2009), (2010, 2019), (2020, 2029)]
SAMPLE_SIZE = 100
CANDIDATES = 10
TOP_DEGREE = 10
SEED = 42


# --------------------------------------------------------------------------
# Graph
# --------------------------------------------------------------------------


def costar_edges(titles, ratings, principals, first_year=None, last_year=None,
                 min_votes=MIN_VOTES):
    """Undirected co-star edges ``(src, dst, tconst, shared_films)``.

    Each pair of actors appears in both directions; ``tconst`` is one of
    their shared films (the oldest), used to display paths.
    """
    films = rated(
        feature_films(titles).where(F.col("startYear").isNotNull())
        .select("tconst", "startYear"),
        ratings, min_votes,
    )
    if first_year is not None:
        films = films.where(F.col("startYear") >= first_year)
    if last_year is not None:
        films = films.where(F.col("startYear") <= last_year)
    cast = (
        principals.where(F.col("category").isin(*ACTING_CATEGORIES))
        .select("tconst", "nconst").distinct()
        .join(films.select("tconst", "startYear"), "tconst")
    )
    a, b = cast.alias("a"), cast.alias("b")
    pairs = (
        a.join(b, "tconst")
        .where(F.col("a.nconst") < F.col("b.nconst"))
        .groupBy(F.col("a.nconst").alias("src"), F.col("b.nconst").alias("dst"))
        .agg(
            F.min_by("tconst", F.col("a.startYear")).alias("tconst"),
            F.count(F.lit(1)).alias("shared_films"),
        )
    )
    return pairs.unionByName(
        pairs.select(F.col("dst").alias("src"), F.col("src").alias("dst"),
                     "tconst", "shared_films")
    )


def degrees(edges):
    """Number of distinct co-stars of each actor."""
    return edges.groupBy(F.col("src").alias("node")).agg(
        F.count(F.lit(1)).alias("degree"))


# --------------------------------------------------------------------------
# Breadth-first search
# --------------------------------------------------------------------------


def multi_source_bfs(spark, edges, sources, work_dir, max_depth=50):
    """Distances from every source to every actor it can reach.

    ``sources`` is a list of ``nconst``. Returns a DataFrame
    ``(source, node, distance)`` read from ``work_dir`` (one Parquet folder
    per level), including ``distance = 0`` for the sources themselves.
    """
    work_dir = work_dir / f"bfs_{uuid.uuid4().hex[:8]}"
    links = edges.select("src", "dst")
    level = spark.createDataFrame([(s, s) for s in sources], "source string, node string")
    level.write.parquet(str(work_dir / "distance=0"))
    depth = 0
    while depth < max_depth:
        frontier = spark.read.parquet(str(work_dir / f"distance={depth}"))
        visited = spark.read.parquet(str(work_dir)).select("source", "node")
        reached = (
            frontier.join(links, frontier.node == links.src)
            .select("source", F.col("dst").alias("node"))
            .distinct()
            .join(visited, ["source", "node"], "left_anti")
        )
        depth += 1
        reached.write.parquet(str(work_dir / f"distance={depth}"))
        if spark.read.parquet(str(work_dir / f"distance={depth}")).isEmpty():
            break
    return spark.read.parquet(str(work_dir)).select(
        "source", "node", F.col("distance").cast("int"))


def shortest_path(spark, edges, start, goal, work_dir, max_depth=50):
    """Shortest chain of actors and films from ``start`` to ``goal``.

    BFS from ``start`` that keeps, for each actor reached, one parent and the
    shared film; it stops as soon as ``goal`` is reached. Returns a list of
    ``(nconst, tconst)`` steps (``tconst`` is the film joining the step to
    the previous one), or ``None`` when the two actors are not connected.
    """
    work_dir = work_dir / f"path_{uuid.uuid4().hex[:8]}"
    spark.createDataFrame([(start, None, None)],
                          "node string, parent string, tconst string") \
        .write.parquet(str(work_dir / "distance=0"))
    depth, found = 0, start == goal
    while not found and depth < max_depth:
        frontier = spark.read.parquet(str(work_dir / f"distance={depth}")).select("node")
        visited = spark.read.parquet(str(work_dir)).select("node")
        reached = (
            frontier.join(edges, frontier.node == edges.src)
            .groupBy(F.col("dst").alias("node"))
            .agg(F.min_by(F.struct("src", "tconst"), "src").alias("p"))
            .select("node", F.col("p.src").alias("parent"), F.col("p.tconst").alias("tconst"))
            .join(visited, "node", "left_anti")
        )
        depth += 1
        reached.write.parquet(str(work_dir / f"distance={depth}"))
        level = spark.read.parquet(str(work_dir / f"distance={depth}"))
        if level.isEmpty():
            return None
        found = not level.where(F.col("node") == goal).isEmpty()
    if not found:
        return None
    tree = {r["node"]: (r["parent"], r["tconst"]) for r in
            spark.read.parquet(str(work_dir)).where(F.col("distance") > 0).collect()}
    steps, node = [], goal
    while node != start:
        parent, film = tree[node]
        steps.append((node, film))
        node = parent
    steps.append((start, None))
    shutil.rmtree(work_dir, ignore_errors=True)
    return list(reversed(steps))


# --------------------------------------------------------------------------
# Small world and centre of one graph
# --------------------------------------------------------------------------


def analyse_graph(spark, edges, people, work_dir, sample_size=SAMPLE_SIZE,
                  candidates=CANDIDATES, top_degree=TOP_DEGREE, seed=SEED,
                  extra=(KEVIN_BACON,)):
    """Small-world statistics and centre of a co-star graph.

    Returns a dict of DataFrames / values:

    - ``summary``: one-row DataFrame (sizes, giant component, average
      separation, centre...);
    - ``closeness``: exact average distance of the candidates (best
      estimates and most connected actors) and of the ``extra`` actors
      present in the giant component; ``estimated`` and ``top_degree`` say
      why each actor was selected;
    - ``centre_distances``: number of actors at each distance from the
      centre and from each ``extra`` actor.
    """
    edges = edges.cache()
    degree = degrees(edges).cache()
    actors = degree.count()
    edge_count = edges.count() // 2

    # 1. Giant component: everything reachable from the most connected actor.
    hub = degree.orderBy(F.desc("degree"), "node").first()["node"]
    giant = multi_source_bfs(spark, edges, [hub], work_dir).select("node").cache()
    giant_size = giant.count()

    # 2. Estimated closeness from a random sample of the giant component.
    # The order comes from a hash of the actor id and the seed, not from
    # F.rand(seed): rand depends on the order of the rows in the partitions,
    # which changes between runs, so the sample would change too.
    sample = [r["node"] for r in giant.orderBy(F.xxhash64("node", F.lit(seed)), "node")
              .limit(sample_size).collect()]
    sampled = multi_source_bfs(spark, edges, sample, work_dir).cache()
    separation = sampled.where("distance > 0").agg(
        F.avg("distance").alias("avg_separation"),
        F.percentile("distance", 0.5).alias("median_separation"),
        F.max("distance").alias("max_distance_seen"),
        F.avg((F.col("distance") <= 6).cast("int")).alias("share_within_6"),
    ).first()
    estimate = (
        sampled.groupBy("node")
        .agg(F.avg("distance").alias("estimated_avg_distance"),
             F.count(F.lit(1)).alias("sources_reached"))
        .where(F.col("sources_reached") == len(sample))
    )
    best = [r["node"] for r in
            estimate.orderBy("estimated_avg_distance", "node").limit(candidates).collect()]
    hubs = [r["node"] for r in degree.join(giant, "node")
            .orderBy(F.desc("degree"), "node").limit(top_degree).collect()]
    in_giant = {r["node"] for r in giant.where(F.col("node").isin(*extra)).collect()}
    exact_sources = list(dict.fromkeys(best + hubs + [e for e in extra if e in in_giant]))

    # 3. Exact closeness of the candidates.
    exact = multi_source_bfs(spark, edges, exact_sources, work_dir).cache()
    closeness = (
        exact.where("distance > 0").groupBy(F.col("source").alias("nconst"))
        .agg(F.avg("distance").alias("avg_distance"),
             F.max("distance").alias("eccentricity"),
             F.count(F.lit(1)).alias("reached"))
        .join(degree.select(F.col("node").alias("nconst"), "degree"), "nconst")
        .join(people.select("nconst", "primaryName"), "nconst", "left")
        .withColumn("estimated", F.col("nconst").isin(*best))
        .withColumn("top_degree", F.col("nconst").isin(*hubs))
        .select("nconst", "primaryName", "degree", F.round("avg_distance", 3)
                .alias("avg_distance"), "eccentricity", "reached", "estimated",
                "top_degree")
        .orderBy("avg_distance")
    )
    closeness_rows = closeness.collect()
    centre = closeness_rows[0]
    bacon = next((r for r in closeness_rows if r["nconst"] == KEVIN_BACON), None)
    bacon_rank = (1 + sum(1 for r in closeness_rows if r["avg_distance"] < bacon["avg_distance"])
                  if bacon else None)

    shown = [centre["nconst"]] + [e for e in extra if e in in_giant]
    centre_distances = (
        exact.where(F.col("source").isin(*shown))
        .groupBy("source", "distance").count()
        .join(people.select(F.col("nconst").alias("source"), "primaryName"), "source", "left")
        .orderBy("source", "distance")
    )

    summary = spark.createDataFrame([(
        actors, edge_count, giant_size, round(100 * giant_size / actors, 2),
        round(separation["avg_separation"], 3), float(separation["median_separation"]),
        int(separation["max_distance_seen"]), round(100 * separation["share_within_6"], 2),
        centre["nconst"], centre["primaryName"], float(centre["avg_distance"]),
        int(centre["degree"]),
        float(bacon["avg_distance"]) if bacon else None, bacon_rank,
    )], """actors long, edges long, giant_component long, giant_component_pct double,
           avg_separation double, median_separation double, max_distance_seen int,
           pct_pairs_within_6 double, centre_nconst string, centre_name string,
           centre_avg_distance double, centre_degree int,
           bacon_avg_distance double, bacon_rank int""")
    edges.unpersist()
    degree.unpersist()
    return {"summary": summary, "closeness": closeness,
            "centre_distances": centre_distances, "centre": centre["nconst"]}
