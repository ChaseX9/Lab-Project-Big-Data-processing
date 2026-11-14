"""Structured Streaming of Wikipedia changes relevant to the IMDb dataset.

A change is relevant when its page is the exact Wikipedia article of a film
or a principal of the enrichment step (the articles were resolved from IMDb
ids through Wikidata, so there is no guess and no homonym). Relevance is
decided by joining each change with a lookup table built by
``build_lookup``; page titles are compared case-insensitively.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)


CHANGE_SCHEMA = StructType(
    [
        StructField("id", LongType()),
        StructField("type", StringType()),
        StructField("namespace", LongType()),
        StructField("title", StringType()),
        StructField("title_url", StringType()),
        StructField("comment", StringType()),
        StructField("timestamp", LongType()),
        StructField("user", StringType()),
        StructField("bot", BooleanType()),
        StructField("minor", BooleanType()),
        StructField("server_name", StringType()),
        StructField("wiki", StringType()),
        StructField(
            "length",
            StructType([StructField("old", LongType()), StructField("new", LongType())]),
        ),
        StructField(
            "meta",
            StructType([StructField("dt", StringType()), StructField("id", StringType())]),
        ),
    ]
)


def page_key(column):
    """Case-insensitive page title, with spaces instead of underscores."""
    return F.lower(F.trim(F.regexp_replace(column, "_", " ")))


def build_lookup(articles):
    """Lookup table ``page key -> IMDb entity`` used to filter the stream.

    ``articles`` has one row per enriched entity: ``wikipedia_title``,
    ``imdb_id``, ``entity_type`` (``movie`` / ``person``) and ``imdb_name``.
    When two entities share an article, the first id is kept.
    """
    first = Window.partitionBy("key").orderBy("imdb_id")
    return (
        articles.where(F.col("wikipedia_title").isNotNull())
        .select(
            page_key(F.col("wikipedia_title")).alias("key"),
            "wikipedia_title",
            "imdb_id",
            "entity_type",
            "imdb_name",
        )
        .withColumn("rk", F.row_number().over(first))
        .where("rk = 1")
        .drop("rk")
    )


def parse_changes(kafka_df):
    """Kafka records -> typed change events with an event-time column."""
    return (
        kafka_df.select(
            F.from_json(F.col("value").cast("string"), CHANGE_SCHEMA).alias("e"),
            F.col("timestamp").alias("kafka_time"),
        )
        .select("e.*", "kafka_time")
        .withColumn("event_time", F.to_timestamp(F.col("timestamp")))
        .where(F.col("event_time").isNotNull())
    )


def relevant_changes(changes, lookup):
    """Keep the changes made to an enriched article, with the IMDb entity."""
    return (
        changes.withColumn("key", page_key(F.col("title")))
        .join(F.broadcast(lookup), "key")
        .select(
            "event_time",
            "kafka_time",
            "type",
            "namespace",
            "title",
            "title_url",
            "user",
            "bot",
            (F.col("length.new") - F.col("length.old")).alias("size_change"),
            "imdb_id",
            "entity_type",
            "imdb_name",
            ((F.unix_timestamp("kafka_time") - F.unix_timestamp("event_time")))
            .alias("ingestion_delay_s"),
        )
    )


WINDOW = "5 minutes"
WATERMARK = "5 minutes"


def window_aggregates(df):
    """Aggregates of the windowed counts (shared by streaming and batch)."""
    return [
        F.count(F.lit(1)).alias("changes"),
        F.approx_count_distinct("title").alias("distinct_pages"),
        F.sum(F.col("bot").cast("int")).alias("bot_changes"),
        F.round(F.avg("ingestion_delay_s"), 1).alias("avg_ingestion_delay_s"),
    ]


def windowed_counts(relevant, window=WINDOW, watermark=WATERMARK):
    """Matches per event-time window and entity type.

    The watermark bounds how late an event may arrive: an event older than
    ``max(event_time) - watermark`` is dropped from the aggregation, and a
    window is final (and written) once the watermark has passed its end.
    """
    return (
        relevant.withWatermark("event_time", watermark)
        .groupBy(F.window("event_time", window), "entity_type")
        .agg(*window_aggregates(relevant))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "entity_type",
            "changes",
            "distinct_pages",
            "bot_changes",
            "avg_ingestion_delay_s",
        )
    )


def final_window_counts(matches, window=WINDOW):
    """Batch version of ``windowed_counts`` over all the saved matches.

    Run after the streaming query stops: in append mode, the last windows
    are never written by the stream (the watermark only moves when newer
    events arrive), and events later than the watermark are left out of the
    streamed counts. This batch count covers the whole test period.
    """
    return (
        matches.groupBy(F.window("event_time", window), "entity_type")
        .agg(*window_aggregates(matches))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "entity_type",
            "changes",
            "distinct_pages",
            "bot_changes",
            "avg_ingestion_delay_s",
        )
    )
