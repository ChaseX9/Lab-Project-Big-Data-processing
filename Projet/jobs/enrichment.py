"""Data enrichment with Wikipedia (Spark steps).

Two sub-commands, run before and after ``scripts/fetch_wikipedia.py``:

    python jobs/enrichment.py targets   # select films and principals
    python scripts/fetch_wikipedia.py   # download (outside Spark)
    python jobs/enrichment.py build     # clean, enforce schema, join

Outputs (under ``data/enrichment/``):

- ``targets/films``, ``targets/people`` (Parquet) and ``targets/*.txt`` (ids
  for the download script);
- ``enriched_films`` (Parquet and JSON): one row per film with its synopsis,
  its principals' biographies, source URLs and notes;
- ``coverage`` (CSV): how many targets have a synopsis / biography.
"""

import argparse

from pyspark.sql import functions as F

from imdb_project import enrichment as en
from imdb_project.config import ENRICHMENT_DIR
from imdb_project.io import read_silver
from imdb_project.spark import get_spark

TARGETS = ENRICHMENT_DIR / "targets"
RAW = ENRICHMENT_DIR / "raw"


def write_id_list(df, column, name):
    ids = sorted(r[0] for r in df.select(column).distinct().collect())
    TARGETS.mkdir(parents=True, exist_ok=True)
    (TARGETS / f"{name}.txt").write_text("\n".join(ids) + "\n", encoding="utf-8")
    return len(ids)


def targets(spark):
    films, people = en.select_targets(
        read_silver(spark, "title.basics"),
        read_silver(spark, "title.ratings"),
        read_silver(spark, "title.principals"),
        read_silver(spark, "name.basics"),
    )
    films.write.mode("overwrite").parquet(str(TARGETS / "films"))
    people.write.mode("overwrite").parquet(str(TARGETS / "people"))
    films = spark.read.parquet(str(TARGETS / "films"))
    people = spark.read.parquet(str(TARGETS / "people"))
    n_films = write_id_list(films, "tconst", "films")
    n_people = write_id_list(people, "nconst", "people")
    print(f"{n_films:,} distinct films, {people.count():,} (film, principal) pairs, "
          f"{n_people:,} distinct people")
    films.orderBy(F.desc("numVotes")).show(5, truncate=60)


def build(spark):
    film_targets = spark.read.parquet(str(TARGETS / "films"))
    people_targets = spark.read.parquet(str(TARGETS / "people"))
    synopses = en.build_film_synopses(en.read_raw(spark, RAW / "films.jsonl"))
    biographies = en.build_biographies(en.read_raw(spark, RAW / "people.jsonl"))

    enriched = en.build_enriched_films(film_targets, people_targets, synopses,
                                       biographies)
    output = ENRICHMENT_DIR / "enriched_films"
    enriched.write.mode("overwrite").parquet(str(output / "parquet"))
    enriched = spark.read.parquet(str(output / "parquet"))
    enriched.coalesce(1).write.mode("overwrite").json(str(output / "json"))

    people_rows = enriched.select(F.explode("principals").alias("p")).select("p.*")
    coverage = spark.createDataFrame(
        [
            ("films", enriched.count(),
             enriched.where(F.col("synopsis").isNotNull()).count()),
            ("film principals", people_rows.count(),
             people_rows.where(F.col("biography").isNotNull()).count()),
        ],
        "item string, targets long, enriched long",
    ).withColumn("enriched_pct", F.round(100 * F.col("enriched") / F.col("targets"), 1))
    coverage.coalesce(1).write.mode("overwrite").option("header", True).csv(
        str(ENRICHMENT_DIR / "coverage"))

    print("\n=== Coverage ===")
    coverage.show(truncate=False)
    print("=== Notes for missing synopses ===")
    enriched.groupBy("synopsis_note").count().orderBy(F.desc("count")).show(truncate=False)
    print("=== Notes for missing biographies ===")
    people_rows.groupBy("biography_note").count().orderBy(F.desc("count")).show(
        truncate=False)
    print("=== Sample ===")
    enriched.where("synopsis is not null").orderBy(F.desc("numVotes")).select(
        "tconst", "primaryTitle", "startYear", "synopsis_words",
        F.substring("synopsis", 1, 120).alias("synopsis_start"),
        "synopsis_source_url",
    ).show(5, truncate=False)


def main():
    parser = argparse.ArgumentParser(description="Wikipedia enrichment (Spark steps)")
    parser.add_argument("step", choices=["targets", "build"])
    args = parser.parse_args()
    spark = get_spark("imdb-enrichment")
    {"targets": targets, "build": build}[args.step](spark)
    spark.stop()


if __name__ == "__main__":
    main()
