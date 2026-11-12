"""Reading and writing the IMDb tables."""

from imdb_project.config import bronze_path, raw_path, silver_path
from imdb_project.schemas import string_schema


def read_raw(spark, table):
    """Read an IMDb ``.tsv.gz`` file with every column as a string.

    IMDb files are tab-separated, have a header and use ``\\N`` for missing
    values. Quote handling is turned off because titles contain unbalanced
    double quotes that would otherwise merge several lines into one.
    """
    return (
        spark.read.option("sep", "\t")
        .option("header", True)
        .option("quote", "")
        .option("nullValue", "\\N")
        .option("mode", "PERMISSIVE")
        .schema(string_schema(table))
        .csv(str(raw_path(table)))
    )


def read_bronze(spark, table):
    """Read the Parquet copy of a raw table (all columns are strings)."""
    return spark.read.parquet(str(bronze_path(table)))


def read_silver(spark, table):
    """Read a cleaned table produced by the data preparation step."""
    return spark.read.parquet(str(silver_path(table)))


def write_parquet(df, path, mode="overwrite"):
    df.write.mode(mode).parquet(str(path))
