"""Creation of the Spark session shared by all jobs and notebooks."""

import os

from pyspark.sql import SparkSession


def get_spark(app_name="imdb-project"):
    """Return a local Spark session configured from environment variables.

    ``SPARK_DRIVER_MEMORY`` and ``SPARK_SHUFFLE_PARTITIONS`` are set in
    ``compose.yaml`` (and can be overridden in ``.env``).
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
    return builder.getOrCreate()


def kafka_bootstrap_servers():
    """Kafka address: ``kafka:19092`` in Docker, ``localhost:9092`` on the host."""
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
