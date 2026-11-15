"""Check that the environment works: Spark starts, bronze tables are readable
and Kafka answers.

Usage (inside the Spark container, from the ``projet`` folder):

    python jobs/check_setup.py
"""

import socket

from imdb_project.config import IMDB_TABLES, bronze_path
from imdb_project.io import read_bronze
from imdb_project.spark import get_spark, kafka_bootstrap_servers


def check_kafka():
    host, port = kafka_bootstrap_servers().split(":")
    try:
        with socket.create_connection((host, int(port)), timeout=5):
            print(f"[ok  ] Kafka reachable at {host}:{port}")
            return True
    except OSError as error:
        print(f"[fail] Kafka not reachable at {host}:{port}: {error}")
        return False


def check_tables(spark):
    ok = True
    for table in IMDB_TABLES:
        if not bronze_path(table).exists():
            print(f"[fail] {table}: no bronze table, run jobs/ingest_imdb.py")
            ok = False
            continue
        df = read_bronze(spark, table)
        print(f"[ok  ] {table}: {df.count():,} rows, columns {df.columns}")
    return ok


def main():
    spark = get_spark("imdb-check-setup")
    print(f"[ok  ] Spark {spark.version} started")
    results = [check_tables(spark), check_kafka()]
    spark.stop()
    print("\nSetup OK" if all(results) else "\nSetup incomplete, see messages above")


if __name__ == "__main__":
    main()
