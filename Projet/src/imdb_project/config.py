"""Project paths and data layout.

Data is organised in layers under ``DATA_DIR``:

- ``raw/``: the original ``*.tsv.gz`` files downloaded from IMDb.
- ``bronze/``: the raw files converted to Parquet, every column kept as a
  string and ``\\N`` turned into null. Nothing else is changed, so the data
  quality analysis sees the data exactly as IMDb publishes it.
- ``silver/``: cleaned and typed tables produced by the data preparation step.
"""

import os
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = Path(os.environ.get("IMDB_DATA_DIR", PROJECT_DIR / "data"))

RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
REPORTS_DIR = DATA_DIR / "reports"
RESULTS_DIR = DATA_DIR / "results"
ENRICHMENT_DIR = DATA_DIR / "enrichment"
STREAMING_DIR = DATA_DIR / "streaming"

IMDB_BASE_URL = "https://datasets.imdbws.com"

IMDB_TABLES = [
    "name.basics",
    "title.akas",
    "title.basics",
    "title.crew",
    "title.episode",
    "title.principals",
    "title.ratings",
]


def table_dir_name(table):
    """Return the folder name of a table, e.g. ``title.basics`` -> ``title_basics``."""
    return table.replace(".", "_")


def raw_path(table):
    return RAW_DIR / f"{table}.tsv.gz"


def bronze_path(table):
    return BRONZE_DIR / table_dir_name(table)


def silver_path(table):
    return SILVER_DIR / table_dir_name(table)
