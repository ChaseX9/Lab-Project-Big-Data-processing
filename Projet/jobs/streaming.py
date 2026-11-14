"""Spark Structured Streaming of Wikipedia changes relevant to IMDb.

Sub-commands:

    python jobs/streaming.py lookup                 # build the lookup (batch)
    python jobs/streaming.py run --minutes 25       # run the streaming query
    python jobs/streaming.py report                 # summarise the outputs

``lookup`` lists the Wikipedia articles of the enriched films and principals
(run it after ``jobs/enrichment.py build``). ``run`` reads the Kafka topic
filled by ``scripts/wiki_producer.py``, keeps the changes made to these
articles and writes (under ``data/streaming/``):

- ``matches/``: every relevant change (append, Parquet);
- ``windows/``: matches per 5-minute event-time window and entity type,
  finalised by a 5-minute watermark (append, Parquet);
- ``windows_final/``: the same counts computed in batch from ``matches/``
  when the query stops, so the last windows (never finalised by the stream)
  are included;
- ``progress.jsonl``: Spark streaming metrics of every micro-batch.

Checkpoints are in ``checkpoints/streaming/`` so a stopped query restarts
where it left off. A query that restarts from its checkpoint ignores
``--starting-offsets``; ``run --fresh`` deletes the checkpoints and the
outputs first, for a new test from scratch.
"""

import argparse
import json
import os
import shutil
import time

from pyspark.sql import functions as F

from imdb_project import streaming as st
from imdb_project.config import ENRICHMENT_DIR, PROJECT_DIR, STREAMING_DIR
from imdb_project.spark import get_spark, kafka_bootstrap_servers

TOPIC = os.environ.get("KAFKA_TOPIC", "wikipedia-changes")
CHECKPOINTS = PROJECT_DIR / "checkpoints" / "streaming"
# Everything written by `run` (the lookup is kept).
RUN_OUTPUTS = ["matches", "windows", "windows_final", "progress.jsonl"]


def lookup(spark):
    from imdb_project.enrichment import read_raw

    raw = ENRICHMENT_DIR / "raw"
    targets = ENRICHMENT_DIR / "targets"
    films = (
        read_raw(spark, raw / "films.jsonl").where("status = 'ok'")
        .join(spark.read.parquet(str(targets / "films"))
              .select(F.col("tconst").alias("imdb_id"),
                      F.col("primaryTitle").alias("imdb_name")).distinct(),
              "imdb_id")
        .select("wikipedia_title", "imdb_id", F.lit("movie").alias("entity_type"),
                "imdb_name")
    )
    people = (
        read_raw(spark, raw / "people.jsonl").where("status = 'ok'")
        .join(spark.read.parquet(str(targets / "people"))
              .select(F.col("nconst").alias("imdb_id"),
                      F.col("primaryName").alias("imdb_name")).distinct(),
              "imdb_id")
        .select("wikipedia_title", "imdb_id", F.lit("person").alias("entity_type"),
                "imdb_name")
    )
    table = st.build_lookup(films.unionByName(people))
    table.write.mode("overwrite").parquet(str(STREAMING_DIR / "lookup"))
    table = spark.read.parquet(str(STREAMING_DIR / "lookup"))
    print(f"Lookup: {table.count():,} Wikipedia articles")
    table.groupBy("entity_type").count().show()


def clear_previous_run():
    """Delete the checkpoints and the outputs of the previous run.

    Both must go together: a Parquet sink keeps the list of the batches it
    has written in ``_spark_metadata``, so with new checkpoints (batch ids
    starting again from 0) it would skip these batches without any error.
    """
    shutil.rmtree(CHECKPOINTS, ignore_errors=True)
    for name in RUN_OUTPUTS:
        path = STREAMING_DIR / name
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()
    print("Previous checkpoints and outputs deleted")


def check_previous_run(starting_offsets):
    """Warn when the query resumes, refuse when the outputs would be wrong."""
    if CHECKPOINTS.exists():
        print(f"WARNING: checkpoints found in {CHECKPOINTS}: the query resumes "
              f"from them and --starting-offsets {starting_offsets} is ignored. "
              "Use --fresh to start a new test from scratch.")
        return
    leftovers = [name for name in ("matches", "windows")
                 if (STREAMING_DIR / name / "_spark_metadata").exists()]
    if leftovers:
        raise SystemExit(
            f"Outputs of a previous run found ({', '.join(leftovers)}) but no "
            "checkpoint: the new batches would be skipped by the Parquet sinks. "
            "Run again with --fresh.")


def has_parquet(path):
    """True if a Parquet output has at least one data file.

    A streaming sink that has not written anything yet only contains
    ``_spark_metadata``, and ``spark.read.parquet`` cannot infer its schema.
    """
    return path.is_dir() and any(path.glob("*.parquet"))


def run(spark, minutes, starting_offsets, fresh):
    if fresh:
        clear_previous_run()
    else:
        check_previous_run(starting_offsets)

    lookup_table = spark.read.parquet(str(STREAMING_DIR / "lookup")).cache()
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers())
        .option("subscribe", TOPIC)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )
    relevant = st.relevant_changes(st.parse_changes(kafka), lookup_table)

    matches = (
        relevant.writeStream.format("parquet")
        .option("path", str(STREAMING_DIR / "matches"))
        .option("checkpointLocation", str(CHECKPOINTS / "matches"))
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .queryName("matches")
        .start()
    )
    windows = (
        st.windowed_counts(relevant)
        .writeStream.format("parquet")
        .option("path", str(STREAMING_DIR / "windows"))
        .option("checkpointLocation", str(CHECKPOINTS / "windows"))
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .queryName("windows")
        .start()
    )
    console = (
        relevant.select("event_time", "entity_type", "title", "imdb_id", "imdb_name")
        .writeStream.format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .queryName("console")
        .start()
    )
    queries = [matches, windows, console]

    progress_file = STREAMING_DIR / "progress.jsonl"
    progress_file.parent.mkdir(parents=True, exist_ok=True)
    seen = set()
    stop = time.time() + minutes * 60
    stopped = []
    with open(progress_file, "a", encoding="utf-8") as out:
        while time.time() < stop and not stopped:
            time.sleep(10)
            for query in (matches, windows):
                for p in query.recentProgress:
                    # Spark 4 returns StreamingQueryProgress objects, older
                    # versions plain dicts.
                    data = json.loads(p.json) if hasattr(p, "json") else p
                    key = (data["name"], data["batchId"])
                    if key not in seen:
                        seen.add(key)
                        out.write(json.dumps(data) + "\n")
            out.flush()
            stopped = [query for query in queries if not query.isActive]
    for query in queries:
        query.stop()
    if stopped:
        # A query stops by itself only when it fails.
        for query in stopped:
            error = str(query.exception()).splitlines()[0]
            print(f"Query '{query.name}' stopped: {error}")
        raise SystemExit("Streaming query failed; outputs are incomplete")
    print(f"Stopped after {minutes} minutes; metrics in {progress_file}")

    # Last windows: the stream only writes a window once the watermark has
    # passed its end, which never happens for the final ones.
    if has_parquet(STREAMING_DIR / "matches"):
        saved = spark.read.parquet(str(STREAMING_DIR / "matches"))
    else:
        print("No relevant change saved during this run")
        saved = spark.createDataFrame([], relevant.schema)
    st.final_window_counts(saved).write.mode("overwrite").parquet(
        str(STREAMING_DIR / "windows_final"))
    print(f"Final window counts written to {STREAMING_DIR / 'windows_final'}")


def report(spark):
    if not has_parquet(STREAMING_DIR / "matches"):
        print("No relevant change saved yet: run `jobs/streaming.py run` "
              "(with the producer) first.")
        return
    matches = spark.read.parquet(str(STREAMING_DIR / "matches"))
    print("\n=== Relevant changes per entity type ===")
    matches.groupBy("entity_type").agg(
        F.count(F.lit(1)).alias("changes"),
        F.countDistinct("title").alias("pages"),
        F.sum(F.col("bot").cast("int")).alias("bot_changes"),
        F.round(F.avg("ingestion_delay_s"), 1).alias("avg_delay_s"),
        F.max("ingestion_delay_s").alias("max_delay_s"),
    ).orderBy(F.desc("changes")).show(truncate=False)
    print("=== Most changed IMDb entities ===")
    matches.groupBy("imdb_id", "imdb_name", "entity_type").count().orderBy(
        F.desc("count")).show(15, truncate=False)

    final = spark.read.parquet(str(STREAMING_DIR / "windows_final"))
    if has_parquet(STREAMING_DIR / "windows"):
        streamed = spark.read.parquet(str(STREAMING_DIR / "windows"))
    else:
        # Short run: no window was finalised by the watermark.
        streamed = spark.createDataFrame([], final.schema)
    keys = ["window_start", "window_end", "entity_type"]
    print("=== Windows: finalised by the stream (watermark) vs batch count at the end ===")
    final.select(*keys, F.col("changes").alias("changes_batch")).join(
        streamed.select(*keys, F.col("changes").alias("changes_streamed")), keys, "left"
    ).orderBy(*keys).show(50, truncate=False)

    progress = spark.read.json(str(STREAMING_DIR / "progress.jsonl"))
    print("=== Streaming metrics per query ===")
    progress.groupBy("name").agg(
        F.count(F.lit(1)).alias("micro_batches"),
        F.sum("numInputRows").alias("input_rows"),
        F.round(F.avg("inputRowsPerSecond"), 1).alias("avg_input_rows_per_s"),
        F.round(F.avg("processedRowsPerSecond"), 1).alias("avg_processed_rows_per_s"),
        F.percentile("durationMs.triggerExecution", 0.5).alias("median_batch_ms"),
        F.max("durationMs.triggerExecution").alias("max_batch_ms"),
    ).show(truncate=False)


def main():
    parser = argparse.ArgumentParser(description="Wikipedia streaming (Spark)")
    parser.add_argument("step", choices=["lookup", "run", "report"])
    parser.add_argument("--minutes", type=float, default=25)
    parser.add_argument("--starting-offsets", default="earliest",
                        choices=["earliest", "latest"],
                        help="only used when the query has no checkpoint")
    parser.add_argument("--fresh", action="store_true",
                        help="delete the checkpoints and outputs of the previous run")
    args = parser.parse_args()
    spark = get_spark("imdb-streaming", with_kafka=args.step == "run")
    spark.sparkContext.setLogLevel("WARN")
    if args.step == "lookup":
        lookup(spark)
    elif args.step == "run":
        run(spark, args.minutes, args.starting_offsets, args.fresh)
    else:
        report(spark)
    spark.stop()


if __name__ == "__main__":
    main()
