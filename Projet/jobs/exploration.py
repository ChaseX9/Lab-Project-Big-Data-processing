"""Run the data exploration questions on the silver tables.

Results are written to ``data/results/<name>/`` (Parquet and CSV) and a
preview is printed.

Usage (inside the Spark container, from the ``projet`` folder):

    python jobs/exploration.py            # all questions
    python jobs/exploration.py q1 q4      # some questions only
"""

import argparse

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from imdb_project import exploration as ex
from imdb_project.config import ENRICHMENT_DIR, RESULTS_DIR
from imdb_project.io import read_silver
from imdb_project.spark import get_spark


# Records written by scripts/fetch_languages.py.
LANGUAGES_SCHEMA = StructType([
    StructField("imdb_id", StringType()),
    StructField("wikidata_id", StringType()),
    StructField("languages", ArrayType(StructType([
        StructField("id", StringType()),
        StructField("label", StringType()),
    ]))),
    StructField("status", StringType()),
    StructField("source_url", StringType()),
    StructField("retrieved_at", StringType()),
])


def save(df, name):
    """Write ``df`` as Parquet and as a single CSV file (arrays joined by '|')."""
    path = RESULTS_DIR / name
    df.write.mode("overwrite").parquet(str(path / "parquet"))
    flat = df.select(
        [
            F.array_join(c, "|").alias(c) if t.startswith("array") else F.col(c)
            for c, t in df.dtypes
        ]
    )
    flat.coalesce(1).write.mode("overwrite").option("header", True).csv(str(path / "csv"))
    return RESULTS_DIR.parent.joinpath(path)


def load(spark, name):
    return spark.read.parquet(str(RESULTS_DIR / name / "parquet"))


def title(text):
    print(f"\n{'=' * 80}\n{text}\n{'=' * 80}")


def q1(spark, t):
    outliers, stats, _ = ex.runtime_outliers_by_genre(t["title.basics"])
    save(outliers, "q1_runtime_outliers")
    save(stats, "q1_genre_runtime_stats")
    outliers = load(spark, "q1_runtime_outliers")
    title("Q1: feature films outside [Q1 - 3 IQR, Q3 + 3 IQR] of their genre's runtime")
    load(spark, "q1_genre_runtime_stats").orderBy(F.desc("films")).show(30, truncate=False)
    print(f"{outliers.count():,} (genre, film) outliers, "
          f"{outliers.select('tconst').distinct().count():,} distinct films")
    outliers.orderBy(F.desc("iqr_distance")).show(10, truncate=40)


def q2_q3(spark, t):
    popularity = ex.genre_popularity_by_year(t["title.basics"], t["title.ratings"])
    save(popularity, "q2_genre_popularity_by_year")
    popularity = load(spark, "q2_genre_popularity_by_year")

    labels = {
        "votes": "total votes of the genre's feature films",
        "films": "number of feature films of the genre",
        "avg_rating": f"average rating of the films with >= {ex.MIN_VOTES} votes",
    }
    for method, label in labels.items():
        save(ex.top_genres_per_year(popularity, method), f"q2_top3_by_{method}")
        save(ex.top_genre_frequency(popularity, method), f"q2_top3_frequency_by_{method}")
        title(f"Q2: top 3 genres per year (popularity = {label})")
        load(spark, f"q2_top3_by_{method}").show(50, truncate=False)
        load(spark, f"q2_top3_frequency_by_{method}").show(30, truncate=False)

    save(ex.top_vs_rest(popularity), "q3_top3_vs_rest")
    title("Q3: top 3 genres (by total votes) compared with the other genres, per year")
    load(spark, "q3_top3_vs_rest").show(50, truncate=False)


def q4(spark, t):
    result = ex.one_hit_directors(t["title.basics"], t["title.crew"],
                                  t["title.ratings"], t["name.basics"])
    save(result, "q4_one_hit_directors")
    result = load(spark, "q4_one_hit_directors")
    title("Q4: directors with exactly one feature film >= 8.0 and >= 2 other "
          "feature films, each rated <= 6.0")
    print(f"{result.count():,} directors")
    result.show(20, truncate=40)


def q5(spark, t):
    result = ex.series_declining_episodes(t["title.basics"], t["title.episode"],
                                          t["title.ratings"])
    save(result, "q5_series_worst_last_best_first")
    result = load(spark, "q5_series_worst_last_best_first")
    title("Q5: series (>= 3 seasons of >= 10 episodes), worst episode in the last "
          f"season, best in seasons 1-2 (episodes with >= {ex.EPISODE_MIN_VOTES} votes)")
    print(f"{result.count():,} series")
    result.show(20, truncate=30)


def q6(spark, t):
    successful = ex.successful_films(t["title.basics"], t["title.ratings"])
    # 1. Films whose original language is needed, for scripts/fetch_languages.py.
    targets = ENRICHMENT_DIR / "targets" / "language_films.txt"
    targets.parent.mkdir(parents=True, exist_ok=True)
    ids = sorted(r["tconst"] for r in successful.select("tconst").collect())
    targets.write_text("\n".join(ids) + "\n", encoding="utf-8")
    title(f"Q6: {len(ids):,} successful feature films (rating >= "
          f"{ex.SUCCESS_MIN_RATING}, votes >= {ex.SUCCESS_MIN_VOTES})")

    # 2. Original languages downloaded from Wikidata.
    raw = ENRICHMENT_DIR / "raw" / "languages.jsonl"
    if not raw.exists():
        print(f"Run `python scripts/fetch_languages.py` first ({targets} written).")
        return
    # A line cut by an interrupted run is dropped (its film is fetched again).
    fetched = (
        spark.read.schema(LANGUAGES_SCHEMA).option("mode", "DROPMALFORMED").json(str(raw))
        .where(F.col("imdb_id").isNotNull())
        .dropDuplicates(["imdb_id"])
    )
    fetched = fetched.join(successful.select(F.col("tconst").alias("imdb_id")), "imdb_id")
    print("Wikidata coverage of the successful films:")
    fetched.groupBy("status").count().show()
    missing = len(ids) - fetched.count()
    if missing:
        print(f"{missing:,} films not fetched yet: run scripts/fetch_languages.py again.")
    languages = fetched.where("status = 'ok'").select(
        F.col("imdb_id").alias("tconst"),
        F.col("languages")[0]["label"].alias("wikidata_language"),
        F.size("languages").alias("listed_languages"),
    )
    print("Films with several original languages on Wikidata (first one kept): "
          f"{languages.where('listed_languages > 1').count():,}")
    languages = languages.withColumn("original_language",
                                     ex.group_language("wikidata_language"))
    print("Languages merged into a group (LANGUAGE_GROUPS):")
    languages.where("original_language != wikidata_language").groupBy(
        "wikidata_language", "original_language").count().orderBy("original_language").show(
        truncate=False)
    languages = languages.select("tconst", "original_language")

    # 3. Actors with successful films in several languages.
    result = ex.multilingual_successful_actors(
        successful, languages,
        t["title.principals"], t["name.basics"])
    save(result, "q6_multilingual_actors")
    result = load(spark, "q6_multilingual_actors")
    title("Q6: actors with successful feature films in several original languages")
    print(f"{result.count():,} people with >= 2 languages, "
          f"{result.where('languages >= 3').count():,} with >= 3 languages")
    result.groupBy("languages").count().orderBy("languages").show()
    result.where("languages >= 3").show(30, truncate=70)
    result.where("languages = 2").show(15, truncate=70)


def q7(spark, t):
    result = ex.early_peak_actors(t["title.basics"], t["title.ratings"],
                                  t["title.principals"], t["name.basics"])
    save(result, "q7_early_peak_actors")
    result = load(spark, "q7_early_peak_actors")
    title("Q7: actors with 20+ year careers and 15+ feature films who peaked early")
    print(f"{result.count():,} people")
    result.show(20, truncate=30)


def q8(spark, t):
    people, combos, comparison = ex.multi_talented(
        t["title.basics"], t["title.crew"], t["title.principals"],
        t["title.ratings"], t["name.basics"])
    save(people, "q8_multi_talented_people")
    save(combos, "q8_role_combination_performance")
    save(comparison, "q8_multi_vs_single_role")
    people = load(spark, "q8_multi_talented_people")
    comparison = load(spark, "q8_multi_vs_single_role")
    title("Q8: multi-talented people (>= 2 of acting / directing / writing)")
    people.groupBy("roles").count().orderBy(F.desc("count")).show(truncate=False)
    title("Q8: average rating per combination of roles held on the same movie")
    load(spark, "q8_role_combination_performance").show(truncate=False)
    title("Q8: multi-role vs single-role movies, per person")
    comparison.agg(
        F.count(F.lit(1)).alias("people"),
        F.round(F.avg("multi_role_avg_rating"), 2).alias("avg_multi_role"),
        F.round(F.avg("single_role_avg_rating"), 2).alias("avg_single_role"),
        F.round(F.avg("difference"), 2).alias("avg_difference"),
        F.round(100 * F.avg((F.col("difference") > 0).cast("int")), 1)
        .alias("pct_better_multi_role"),
    ).show()
    comparison.where("multi_role_movies >= 5 and single_role_movies >= 5").orderBy(
        F.desc("difference")).show(10, truncate=30)


QUESTIONS = {"q1": q1, "q2": q2_q3, "q3": q2_q3, "q4": q4, "q5": q5, "q6": q6,
             "q7": q7, "q8": q8}
TABLES = ["title.basics", "title.ratings", "title.crew", "title.episode",
          "title.akas", "title.principals", "name.basics"]


def main():
    parser = argparse.ArgumentParser(description="IMDb data exploration")
    parser.add_argument("questions", nargs="*", help=f"any of {list(QUESTIONS)}")
    args = parser.parse_args()
    selected = args.questions or list(QUESTIONS)
    unknown = sorted(set(selected) - set(QUESTIONS))
    if unknown:
        parser.error(f"unknown questions {unknown}, choose from {list(QUESTIONS)}")

    spark = get_spark("imdb-exploration")
    tables = {name: read_silver(spark, name) for name in TABLES}
    done = set()
    for name in selected:
        job = QUESTIONS[name]
        if job not in done:
            job(spark, tables)
            done.add(job)
    spark.stop()


if __name__ == "__main__":
    main()
