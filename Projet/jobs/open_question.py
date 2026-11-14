"""Open question: is the IMDb acting world a small world, who is its real
centre, and how does this change from one decade to the next?

Usage (inside the Spark container, from the ``projet`` folder):

    python jobs/open_question.py                     # all graphs (about 10 min)
    python jobs/open_question.py path nm0000102 nm0000821   # chain between two actors

Writes to ``data/results/``:

- ``open_graph_summary``: one row per graph (all years, then each decade):
  size, giant component, average separation, centre, Kevin Bacon;
- ``open_closeness``: exact average distance of the candidate centres;
- ``open_distance_distribution``: number of actors at each distance from the
  centre and from Kevin Bacon;
- ``open_example_paths``: a few chains of actors and films.
"""

import argparse
import shutil
from functools import reduce

from pyspark.sql import functions as F

from imdb_project import open_question as oq
from imdb_project.config import RESULTS_DIR
from imdb_project.io import read_silver
from imdb_project.spark import get_spark

WORK_DIR = RESULTS_DIR / "open_tmp"
# Example chains from Kevin Bacon (all-years graph).
EXAMPLES = ["nm0000821", "nm0001536"]   # Amitabh Bachchan, Toshiro Mifune


def save_and_show(df, name, rows=40):
    path = RESULTS_DIR / name
    df.write.mode("overwrite").parquet(str(path / "parquet"))
    df = df.sparkSession.read.parquet(str(path / "parquet"))
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(path / "csv"))
    print(f"\n=== {name} ===")
    df.show(rows, truncate=False)


def build_edges(spark, t, first_year, last_year, name):
    path = WORK_DIR / f"edges_{name}"
    oq.costar_edges(t["title.basics"], t["title.ratings"], t["title.principals"],
                    first_year, last_year).write.mode("overwrite").parquet(str(path))
    return spark.read.parquet(str(path))


def describe_path(spark, steps, people, titles):
    names = {r["nconst"]: r["primaryName"] for r in
             people.where(F.col("nconst").isin([s[0] for s in steps])).collect()}
    films = {r["tconst"]: f"{r['primaryTitle']} ({r['startYear']})" for r in
             titles.where(F.col("tconst").isin([s[1] for s in steps if s[1]])).collect()}
    text = names.get(steps[0][0], steps[0][0])
    for nconst, tconst in steps[1:]:
        text += f" -[{films.get(tconst, tconst)}]- {names.get(nconst, nconst)}"
    return text


def run_all(spark, t):
    graphs = [("all years", None, None)] + [
        (f"{a}-{min(b, 2025)}", a, b) for a, b in oq.DECADES]
    summaries, closeness, distances = [], [], []
    edges_all = None
    for order, (label, first, last) in enumerate(graphs):
        print(f"\n##### Graph: {label}")
        edges = build_edges(spark, t, first, last, label.replace(" ", "_"))
        if label == "all years":
            edges_all = edges
        result = oq.analyse_graph(spark, edges, t["name.basics"], WORK_DIR)
        tag = F.lit(label).alias("graph")
        summaries.append(result["summary"].select(F.lit(order).alias("graph_order"), tag, "*"))
        closeness.append(result["closeness"].select(tag, "*"))
        distances.append(result["centre_distances"].select(tag, "*"))
        result["summary"].show(truncate=False)

    def union(dfs):
        return reduce(lambda a, b: a.unionByName(b), dfs)

    save_and_show(union(summaries).orderBy("graph_order"), "open_graph_summary")
    save_and_show(union(closeness), "open_closeness", rows=80)
    save_and_show(union(distances), "open_distance_distribution", rows=120)

    centre = spark.read.parquet(str(RESULTS_DIR / "open_graph_summary" / "parquet")) \
        .where("graph = 'all years'").first()["centre_nconst"]
    rows = []
    for goal in [centre] + EXAMPLES:
        steps = oq.shortest_path(spark, edges_all, oq.KEVIN_BACON, goal, WORK_DIR)
        if steps:
            rows.append((oq.KEVIN_BACON, goal, len(steps) - 1,
                         describe_path(spark, steps, t["name.basics"], t["title.basics"])))
    save_and_show(spark.createDataFrame(
        rows, "start string, goal string, distance int, path string"),
        "open_example_paths")


def run_path(spark, t, start, goal):
    edges = build_edges(spark, t, None, None, "all_years")
    steps = oq.shortest_path(spark, edges, start, goal, WORK_DIR)
    if steps is None:
        print("These two actors are not connected in the co-star graph.")
    else:
        print(f"Distance {len(steps) - 1}:")
        print(describe_path(spark, steps, t["name.basics"], t["title.basics"]))


def main():
    parser = argparse.ArgumentParser(description="Open question: co-star graph")
    parser.add_argument("step", nargs="?", default="all", choices=["all", "path"])
    parser.add_argument("actors", nargs="*", help="two nconst for the 'path' step")
    args = parser.parse_args()
    if args.step == "path" and len(args.actors) != 2:
        parser.error("the 'path' step needs two nconst, e.g. nm0000102 nm0000821")

    spark = get_spark("imdb-open-question")
    spark.sparkContext.setLogLevel("ERROR")
    t = {n: read_silver(spark, n) for n in
         ["title.basics", "title.principals", "title.ratings", "name.basics"]}
    shutil.rmtree(WORK_DIR, ignore_errors=True)
    if args.step == "all":
        run_all(spark, t)
    else:
        run_path(spark, t, *args.actors)
    shutil.rmtree(WORK_DIR, ignore_errors=True)
    spark.stop()


if __name__ == "__main__":
    main()
