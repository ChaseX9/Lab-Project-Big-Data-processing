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
where it left off.
"""

import argparse
import json
import os
import time

from pyspark.sql import functions as F

from imdb_project import streaming as st
from imdb_project.config import ENRICHMENT_DIR, PROJECT_DIR, STREAMING_DIR
from imdb_project.spark import get_spark, kafka_bootstrap_servers

TOPIC = os.environ.get("KAFKA_TOPIC", "wikipedia-changes")
CHECKPOINTS = PROJECT_DIR / "checkpoints" / "streaming"


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


def run(spark, minutes, starting_offsets):
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

    progress_file = STREAMING_DIR / "progress.jsonl"
    progress_file.parent.mkdir(parents=True, exist_ok=True)
    seen = set()
    stop = time.time() + minutes * 60
    with open(progress_file, "a", encoding="utf-8") as out:
        while time.time() < stop and matches.isActive:
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
    for query in (console, matches, windows):
        query.stop()
    print(f"Stopped after {minutes} minutes; metrics in {progress_file}")

    # Last windows: the stream only writes a window once the watermark has
    # passed its end, which never happens for the final ones.
    saved = spark.read.parquet(str(STREAMING_DIR / "matches"))
    st.final_window_counts(saved).write.mode("overwrite").parquet(
        str(STREAMING_DIR / "windows_final"))
    print(f"Final window counts written to {STREAMING_DIR / 'windows_final'}")


def report(spark):
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

    streamed = spark.read.parquet(str(STREAMING_DIR / "windows"))
    final = spark.read.parquet(str(STREAMING_DIR / "windows_final"))
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
                        choices=["earliest", "latest"])
    args = parser.parse_args()
    spark = get_spark("imdb-streaming", with_kafka=args.step == "run")
    spark.sparkContext.setLogLevel("WARN")
    if args.step == "lookup":
        lookup(spark)
    elif args.step == "run":
        run(spark, args.minutes, args.starting_offsets)
    else:
        report(spark)
    spark.stop()


if __name__ == "__main__":
    main()
