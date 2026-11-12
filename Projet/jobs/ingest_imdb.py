"""Convert the raw IMDb files (``data/raw``) to Parquet (``data/bronze``).

Every column is kept as a string and ``\\N`` becomes null; no row is changed
or dropped. Typing and cleaning are done later by the data preparation step.

Usage (inside the Spark container, from the ``projet`` folder):

    python jobs/ingest_imdb.py                 # all tables
    python jobs/ingest_imdb.py title.ratings   # some tables only
"""

import argparse
import sys

from imdb_project.config import IMDB_TABLES, bronze_path, raw_path
from imdb_project.io import read_bronze, read_raw, write_parquet
from imdb_project.spark import get_spark


def main():
    parser = argparse.ArgumentParser(description="IMDb raw TSV to Parquet")
    parser.add_argument("tables", nargs="*", help="tables to ingest (default: all)")
    args = parser.parse_args()

    tables = args.tables or IMDB_TABLES
    unknown = sorted(set(tables) - set(IMDB_TABLES))
    if unknown:
        parser.error(f"unknown tables {unknown}, choose from {IMDB_TABLES}")

    missing = [t for t in tables if not raw_path(t).exists()]
    if missing:
        print("Missing raw files, run scripts/download_imdb.py first:")
        for table in missing:
            print(f"  - {raw_path(table)}")
        sys.exit(1)

    spark = get_spark("imdb-ingest")
    for table in tables:
        print(f"[ingest] {table} -> {bronze_path(table)}")
        write_parquet(read_raw(spark, table), bronze_path(table))
        rows = read_bronze(spark, table).count()
        print(f"[ingest] {table}: {rows:,} rows")
    spark.stop()


if __name__ == "__main__":
    main()
