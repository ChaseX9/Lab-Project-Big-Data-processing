"""Clean the IMDb tables: ``data/bronze`` -> ``data/silver``.

The cleaning rules are described in ``imdb_project.preparation``. A summary
(rows in, rows out, flagged rows per table) is written to
``data/reports/cleaning_summary``.

Usage (inside the Spark container, from the ``projet`` folder):

    python jobs/prepare_data.py
"""

from pyspark.sql import functions as F

from imdb_project import preparation as prep
from imdb_project.config import REPORTS_DIR, silver_path
from imdb_project.io import read_bronze, read_silver, write_parquet
from imdb_project.spark import get_spark


def main():
    spark = get_spark("imdb-prepare-data")

    def bronze(table):
        return read_bronze(spark, table)

    # Parent tables first: the other tables are checked against them.
    write_parquet(prep.clean_titles(bronze("title.basics")), silver_path("title.basics"))
    write_parquet(prep.clean_names(bronze("name.basics")), silver_path("name.basics"))
    titles = read_silver(spark, "title.basics")
    names = read_silver(spark, "name.basics")

    children = {
        "title.akas": lambda: prep.clean_akas(bronze("title.akas"), titles),
        "title.crew": lambda: prep.clean_crew(bronze("title.crew"), titles, names),
        "title.ratings": lambda: prep.clean_ratings(bronze("title.ratings"), titles),
        "title.episode": lambda: prep.clean_episodes(bronze("title.episode"), titles),
        "title.principals": lambda: prep.clean_principals(
            bronze("title.principals"), titles, names
        ),
    }
    for table, build in children.items():
        print(f"[prepare] {table}")
        write_parquet(build(), silver_path(table))

    summary = []
    for table in ["title.basics", "name.basics", *children]:
        silver = read_silver(spark, table)
        rows_in = bronze(table).count()
        stats = silver.agg(
            F.count(F.lit(1)).alias("rows_out"),
            F.sum((F.size("dq_flags") > 0).cast("long")).alias("flagged_rows"),
        ).first()
        summary.append(
            (table, rows_in, stats["rows_out"], rows_in - stats["rows_out"],
             stats["flagged_rows"] or 0)
        )
    report = spark.createDataFrame(
        summary,
        "table string, rows_in long, rows_out long, dropped_rows long, flagged_rows long",
    )
    report.coalesce(1).write.mode("overwrite").option("header", True).csv(
        str(REPORTS_DIR / "cleaning_summary")
    )
    print("\n=== Cleaning summary ===")
    report.show(truncate=False)

    print("=== Flags per table ===")
    for table in ["title.basics", "name.basics", "title.episode", "title.principals"]:
        (
            read_silver(spark, table)
            .select(F.explode("dq_flags").alias("flag"))
            .groupBy("flag").count()
            .withColumn("table", F.lit(table))
            .select("table", "flag", "count")
            .show(truncate=False)
        )
    spark.stop()


if __name__ == "__main__":
    main()
