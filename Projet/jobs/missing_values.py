"""Percentage of null / empty values for every column of every IMDb table.

Writes the full report to ``data/reports/missing_values`` (CSV) and prints
it, followed by the five columns with the highest share of null values.

Usage (inside the Spark container, from the ``projet`` folder):

    python jobs/missing_values.py
"""

from functools import reduce

from pyspark.sql import functions as F

from imdb_project.config import IMDB_TABLES, REPORTS_DIR
from imdb_project.io import read_bronze
from imdb_project.quality import missing_report
from imdb_project.spark import get_spark


def main():
    spark = get_spark("imdb-missing-values")

    reports = [missing_report(spark, read_bronze(spark, t), t) for t in IMDB_TABLES]
    report = reduce(lambda a, b: a.unionByName(b), reports).cache()

    output = REPORTS_DIR / "missing_values"
    report.coalesce(1).write.mode("overwrite").option("header", True).csv(str(output))

    print("\n=== Missing values per column ===")
    report.orderBy("table", "column").show(100, truncate=False)

    print("=== Top 5 columns by percentage of null values ===")
    report.orderBy(F.desc("null_pct"), F.desc("missing_pct")).select(
        "table", "column", "null_count", "null_pct", "missing_pct"
    ).show(5, truncate=False)

    print(f"Report written to {output}")
    spark.stop()


if __name__ == "__main__":
    main()
