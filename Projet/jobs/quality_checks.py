"""Detect data quality issues in the IMDb tables (beyond missing values).

Runs the checks of ``imdb_project.quality`` on the bronze tables cast to
their expected types, plus a count of values that cannot be cast.
Writes ``data/reports/quality_issues`` and ``data/reports/invalid_casts``.

Usage (inside the Spark container, from the ``projet`` folder):

    python jobs/quality_checks.py
"""

from pyspark.sql import functions as F

from imdb_project.config import IMDB_TABLES, REPORTS_DIR
from imdb_project.io import read_bronze
from imdb_project.quality import invalid_casts, issue_report, typed
from imdb_project.spark import get_spark


def write_csv(df, name):
    path = REPORTS_DIR / name
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(path))
    return path


def main():
    spark = get_spark("imdb-quality-checks")
    bronze = {t: read_bronze(spark, t) for t in IMDB_TABLES}

    cast_rows = [
        (table, column, kind, count)
        for table, df in bronze.items()
        for column, kind, count in invalid_casts(df, table)
    ]
    casts = spark.createDataFrame(
        cast_rows, "table string, column string, type string, invalid_values long"
    )
    write_csv(casts, "invalid_casts")
    print("\n=== Values that cannot be cast to their expected type ===")
    casts.show(50, truncate=False)

    tables = {t: typed(df, t) for t, df in bronze.items()}
    issues = issue_report(spark, tables).cache()
    path = write_csv(issues, "quality_issues")
    print("=== Data quality issues ===")
    issues.orderBy("category", "table", F.desc("affected_rows")).show(100, truncate=False)

    print(f"Reports written to {path.parent}")
    spark.stop()


if __name__ == "__main__":
    main()
