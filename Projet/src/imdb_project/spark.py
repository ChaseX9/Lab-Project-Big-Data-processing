"""Creation of the Spark session shared by all jobs and notebooks."""

import os

import pyspark
from pyspark.sql import SparkSession


def kafka_package():
    """Maven coordinates of the Kafka connector matching the installed Spark."""
    return f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"


def get_spark(app_name="imdb-project", with_kafka=False):
    """Return a local Spark session configured from environment variables.

    ``SPARK_DRIVER_MEMORY`` and ``SPARK_SHUFFLE_PARTITIONS`` are set in
    ``compose.yaml`` (and can be overridden in ``.env``). Pass
    ``with_kafka=True`` to load the Kafka connector for Structured Streaming.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "6g"))
        .config(
            "spark.sql.shuffle.partitions",
            os.environ.get("SPARK_SHUFFLE_PARTITIONS", "64"),
        )
        .config("spark.sql.session.timeZone", "UTC")
    )
    if with_kafka:
        builder = builder.config("spark.jars.packages", kafka_package())
    return builder.getOrCreate()


def kafka_bootstrap_servers():
    """Kafka address: ``kafka:19092`` in Docker, ``localhost:9092`` on the host."""
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
