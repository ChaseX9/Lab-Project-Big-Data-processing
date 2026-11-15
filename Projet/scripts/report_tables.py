"""Write the result tables into REPORT.md.

Each "Results" section of ``REPORT.md`` contains a pair of markers such as

    <!-- results:preparation:start -->
    <!-- results:preparation:end -->

This script replaces what is between each pair with Markdown tables built
from the outputs of the jobs (``data/reports``, ``data/results``,
``data/enrichment``, ``data/streaming``). Text outside the markers (the
methodological explanations) is never modified. Run it again after
re-running a job to refresh the tables.

Usage (inside the Spark container, from the ``projet`` folder):

    python scripts/report_tables.py
"""

import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pyspark.sql import functions as F  # noqa: E402

from imdb_project.config import (  # noqa: E402
    ENRICHMENT_DIR,
    PROJECT_DIR,
    REPORTS_DIR,
    RESULTS_DIR,
    STREAMING_DIR,
)
from imdb_project.spark import get_spark  # noqa: E402

# Repository root REPORT.md (mounted in the Spark container, see compose.yaml).
REPORT = Path(os.environ.get("REPORT_PATH", PROJECT_DIR.parent / "REPORT.md"))


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def fmt(value):
    if value is None:
        return ""
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return f"{value:,.3f}".rstrip("0").rstrip(".") if value % 1 else f"{value:,.0f}"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, (list, tuple)):
        return ", ".join(fmt(v) for v in value)
    return str(value).replace("|", "\\|").replace("\n", " ")


def table(df, columns=None, limit=None, title=None):
    """Markdown table of a Spark DataFrame."""
    if columns:
        df = df.select(*columns)
    if limit:
        df = df.limit(limit)
    rows = df.collect()
    names = df.columns
    lines = []
    if title:
        lines += [f"**{title}**", ""]
    lines.append("| " + " | ".join(names) + " |")
    lines.append("|" + "---|" * len(names))
    for row in rows:
        lines.append("| " + " | ".join(fmt(row[c]) for c in names) + " |")
    return "\n".join(lines) + "\n"


def csv(spark, folder):
    return spark.read.option("header", True).option("inferSchema", True).csv(str(folder))


def result(spark, name):
    return spark.read.parquet(str(RESULTS_DIR / name / "parquet"))


def blocks(*parts):
    return "\n".join(p for p in parts if p)


# --------------------------------------------------------------------------
# Sections
# --------------------------------------------------------------------------


def preparation(spark):
    missing = csv(spark, REPORTS_DIR / "missing_values")
    issues = csv(spark, REPORTS_DIR / "quality_issues")
    return blocks(
        table(missing.orderBy(F.desc("missing_pct")),
              ["table", "column", "rows", "missing_count", "missing_pct"], 5,
              "Five columns with the highest share of missing values (%)"),
        table(missing.orderBy("table", F.desc("missing_pct")),
              ["table", "column", "null_count", "empty_count", "missing_pct"],
              title="Missing values per column (%)"),
        table(issues.where("affected_rows > 0").orderBy("category", F.desc("affected_rows")),
              ["table", "category", "check", "description", "affected_rows", "affected_pct"],
              title="Other data quality issues (checks with at least one affected row)"),
        table(issues.where("affected_rows = 0").orderBy("table", "check"),
              ["table", "check", "description"],
              title="Checks without any affected row"),
        table(csv(spark, REPORTS_DIR / "cleaning_summary"),
              title="Cleaning summary (bronze -> silver)"),
    )


def exploration(spark):
    q1 = result(spark, "q1_runtime_outliers")
    q2 = {m: result(spark, f"q2_top3_by_{m}") for m in ["votes", "films", "avg_rating"]}
    q2_side = q2["votes"].select(
        "year", F.concat_ws(", ", "genre_1", "genre_2", "genre_3").alias("total_votes")
    ).join(q2["films"].select(
        "year", F.concat_ws(", ", "genre_1", "genre_2", "genre_3").alias("number_of_films")),
        "year").join(q2["avg_rating"].select(
            "year", F.concat_ws(", ", "genre_1", "genre_2", "genre_3")
            .alias("average_rating")), "year").orderBy("year")
    q5 = result(spark, "q5_series_worst_last_best_first")
    q6 = result(spark, "q6_multilingual_actors")
    q7 = result(spark, "q7_early_peak_actors")
    q8 = result(spark, "q8_multi_vs_single_role")
    return blocks(
        "#### Q1 – Runtime outliers per genre\n",
        f"{q1.count():,} (genre, film) outliers, "
        f"{q1.select('tconst').distinct().count():,} distinct films.\n",
        table(result(spark, "q1_genre_runtime_stats").orderBy(F.desc("films")),
              title="Runtime statistics and bounds per genre (minutes)"),
        table(q1.orderBy(F.desc("iqr_distance")), limit=20,
              title="Largest outliers (distance outside the fence, in IQRs)"),
        "#### Q2 – Top three genres per year\n",
        table(q2_side, title="Top 3 genres per year, for each popularity measure"),
        *[table(result(spark, f"q2_top3_frequency_by_{m}"),
                title=f"Years in the top 3 (popularity = {m})")
          for m in ["votes", "films", "avg_rating"]],
        "#### Q3 – Top three genres compared with the other genres\n",
        table(result(spark, "q3_top3_vs_rest").orderBy("year"),
              title="Top 3 genres (by total votes) vs the other genres, per year"),
        "#### Q4 – Directors with a single hit\n",
        table(result(spark, "q4_one_hit_directors"),
              ["primaryName", "hit_title", "hit_year", "hit_rating", "hit_votes",
               "other_movies", "other_avg_rating", "other_best_rating"]),
        "#### Q5 – Series whose worst episode is in the last season\n",
        f"{q5.count():,} series.\n",
        table(q5.orderBy(F.desc("best_averageRating")),
              ["series_title", "series_start", "seasons", "rated_episodes",
               "best_seasonNumber", "best_episode_title", "best_averageRating",
               "worst_seasonNumber", "worst_episode_title", "worst_averageRating"], 30,
              "Series (30 first, by best episode rating)"),
        "#### Q6 – Actors successful in several languages\n",
        table(spark.read.json(str(ENRICHMENT_DIR / "raw" / "languages.jsonl"))
              .groupBy("status").count().orderBy(F.desc("count")),
              title="Wikidata original language of the successful films"),
        f"{q6.count():,} people with successful films in at least 2 languages, "
        f"{q6.where('languages >= 3').count():,} in at least 3 languages.\n",
        table(q6.groupBy("languages").count().orderBy("languages"),
              title="Number of people per number of languages"),
        table(q6.where("languages >= 3").orderBy(F.desc("languages"), F.desc("successful_films")),
              ["primaryName", "languages", "language_list", "successful_films"],
              title="People with successful films in at least 3 languages"),
        "#### Q7 – Long careers that peaked early\n",
        f"{q7.count():,} people.\n",
        table(q7.orderBy(F.desc("rating_drop")),
              ["primaryName", "first_year", "last_year", "movies", "rated_movies",
               "best_title", "best_year", "best_rating", "early_avg_rating",
               "late_avg_rating", "rating_drop"], 25,
              "Largest drops (25 first)"),
        "#### Q8 – Multi-talented people\n",
        table(result(spark, "q8_multi_talented_people").groupBy("roles").count()
              .orderBy(F.desc("count")), title="Multi-talented people per role families"),
        table(result(spark, "q8_role_combination_performance"),
              title="Average rating per combination of roles held on the same film"),
        table(q8.agg(
            F.count(F.lit(1)).alias("people"),
            F.round(F.avg("multi_role_avg_rating"), 2).alias("avg_multi_role"),
            F.round(F.avg("single_role_avg_rating"), 2).alias("avg_single_role"),
            F.round(F.avg("difference"), 2).alias("avg_difference"),
            F.round(100 * F.avg((F.col("difference") > 0).cast("int")), 1)
            .alias("pct_better_multi_role")),
            title="Multi-role vs single-role films, per person"),
    )


def enrichment(spark):
    enriched = spark.read.parquet(str(ENRICHMENT_DIR / "enriched_films" / "parquet"))
    people = enriched.select(F.explode("principals").alias("p")).select("p.*")
    return blocks(
        table(csv(spark, ENRICHMENT_DIR / "coverage"), title="Coverage"),
        table(enriched.groupBy("synopsis_note").count().orderBy(F.desc("count")),
              title="Notes for the films without a synopsis"),
        table(people.groupBy("biography_note").count().orderBy(F.desc("count")),
              title="Notes for the principals without a biography"),
        table(enriched.where("synopsis is not null").orderBy(F.desc("numVotes")),
              ["tconst", "primaryTitle", "startYear", "synopsis_words",
               "synopsis_source_url"], 5, "Sample of enriched films"),
    )


def streaming(spark):
    matches = spark.read.parquet(str(STREAMING_DIR / "matches"))
    streamed = spark.read.parquet(str(STREAMING_DIR / "windows"))
    final = spark.read.parquet(str(STREAMING_DIR / "windows_final"))
    progress = spark.read.json(str(STREAMING_DIR / "progress.jsonl"))
    keys = ["window_start", "window_end", "entity_type"]
    return blocks(
        table(matches.groupBy("entity_type").agg(
            F.count(F.lit(1)).alias("changes"),
            F.countDistinct("title").alias("pages"),
            F.sum(F.col("bot").cast("int")).alias("bot_changes"),
            F.round(F.avg("ingestion_delay_s"), 1).alias("avg_delay_s"),
            F.max("ingestion_delay_s").alias("max_delay_s"),
        ).orderBy(F.desc("changes")), title="Relevant changes per entity type"),
        table(matches.groupBy("imdb_id", "imdb_name", "entity_type").count()
              .orderBy(F.desc("count"), "imdb_name"), limit=15,
              title="Most changed IMDb entities"),
        table(final.select(*keys, F.col("changes").alias("changes_batch_at_end")).join(
            streamed.select(*keys, F.col("changes").alias("changes_written_by_stream")),
            keys, "left").orderBy(*keys),
            title="5-minute windows: counts written by the stream (watermark) and "
                  "batch counts at the end"),
        table(progress.groupBy("name").agg(
            F.count(F.lit(1)).alias("micro_batches"),
            F.sum("numInputRows").alias("input_rows"),
            F.round(F.avg("inputRowsPerSecond"), 1).alias("avg_input_rows_per_s"),
            F.round(F.avg("processedRowsPerSecond"), 1).alias("avg_processed_rows_per_s"),
            F.percentile("durationMs.triggerExecution", 0.5).alias("median_batch_ms"),
            F.max("durationMs.triggerExecution").alias("max_batch_ms"),
        ).orderBy("name"), title="Streaming metrics per query"),
    )


def open_question(spark):
    # "all years" first, then the decades in order.
    summary = result(spark, "open_graph_summary").orderBy("graph_order")
    order = summary.select("graph", "graph_order")
    distances = result(spark, "open_distance_distribution")
    pivot = distances.where("graph = 'all years'").groupBy("distance").pivot(
        "primaryName").agg(F.first("count")).orderBy("distance")
    return blocks(
        table(summary, ["graph", "actors", "connected_actors", "edges", "giant_component",
                        "giant_component_pct", "avg_separation", "median_separation",
                        "max_distance_seen", "pct_pairs_within_6"],
              title="Co-star graph per period (giant_component_pct is out of all the "
                    "credited actors)"),
        table(summary, ["graph", "centre_name", "centre_avg_distance", "centre_degree",
                        "candidates", "bacon_avg_distance", "bacon_rank_among_candidates",
                        "bacon_estimated_rank", "giant_component"],
              title="Centre of each graph and rank of Kevin Bacon (among the exact "
                    "candidates, and among the giant component on the estimate)"),
        table(result(spark, "open_closeness").join(order, "graph")
              .orderBy("graph_order", "avg_distance", "nconst"),
              ["graph", "primaryName", "degree", "avg_distance", "eccentricity",
               "estimated", "top_degree"],
              title="Exact average distance of the candidate centres"),
        table(pivot, title="Number of actors at each distance (all years)"),
        table(result(spark, "open_example_paths"), ["distance", "path"],
              title="Example chains from Kevin Bacon"),
    )


SECTIONS = {
    "preparation": preparation,
    "exploration": exploration,
    "enrichment": enrichment,
    "streaming": streaming,
    "open-question": open_question,
}


def main():
    spark = get_spark("report-tables")
    spark.sparkContext.setLogLevel("ERROR")
    text = REPORT.read_text(encoding="utf-8")
    for name, build in SECTIONS.items():
        start, end = f"<!-- results:{name}:start -->", f"<!-- results:{name}:end -->"
        if start not in text:
            print(f"[skip] markers for {name} not found in REPORT.md")
            continue
        try:
            content = build(spark)
        except Exception as error:  # a missing output should not stop the others
            print(f"[skip] {name}: {type(error).__name__}: {str(error)[:200]}")
            continue
        pattern = re.compile(re.escape(start) + r".*?" + re.escape(end), re.S)
        text = pattern.sub(lambda _: f"{start}\n\n{content}\n{end}", text)
        print(f"[ok] {name}")
    REPORT.write_text(text, encoding="utf-8")
    spark.stop()


if __name__ == "__main__":
    main()
