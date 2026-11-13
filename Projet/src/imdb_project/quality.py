"""Data quality measures on the IMDb tables.

Two kinds of measures are provided:

- ``missing_report``: share of null / empty values for every column.
- ``issue_report``: rule-based checks (impossible values, cross-table
  inconsistencies, orphan foreign keys, duplicate keys). Each check counts the
  affected rows; nothing is modified here.
"""

import datetime
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from imdb_project.schemas import expected_types

CURRENT_YEAR = datetime.date.today().year
# Oldest known motion picture (Roundhay Garden Scene, 1888) with some margin.
FIRST_FILM_YEAR = 1874
MAX_PLAUSIBLE_AGE = 110
# Categories where a principal can appear after death (archive material).
ARCHIVE_CATEGORIES = ["archive_footage", "archive_sound"]

MISSING_REPORT_SCHEMA = StructType(
    [
        StructField("table", StringType(), False),
        StructField("column", StringType(), False),
        StructField("rows", LongType(), False),
        StructField("null_count", LongType(), False),
        StructField("empty_count", LongType(), False),
        StructField("missing_count", LongType(), False),
        StructField("null_pct", DoubleType(), False),
        StructField("missing_pct", DoubleType(), False),
    ]
)

ISSUE_REPORT_SCHEMA = StructType(
    [
        StructField("table", StringType(), False),
        StructField("category", StringType(), False),
        StructField("check", StringType(), False),
        StructField("description", StringType(), False),
        StructField("affected_rows", LongType(), False),
        StructField("total_rows", LongType(), False),
        StructField("affected_pct", DoubleType(), False),
    ]
)


def _pct(part, total):
    return round(100.0 * part / total, 4) if total else 0.0


# --------------------------------------------------------------------------
# Missing values
# --------------------------------------------------------------------------


def missing_report(spark, df, table):
    """Count null and empty values of every column of ``df`` in one pass.

    - ``null_count``: real nulls (``\\N`` in the IMDb files).
    - ``empty_count``: non-null strings that are empty or only whitespace.
    - ``missing_count``: the sum of both.

    Returns a DataFrame with one row per column (see ``MISSING_REPORT_SCHEMA``).
    """
    aggregations = [F.count(F.lit(1)).alias("__rows")]
    for name in df.columns:
        column = F.col(name)
        is_empty = column.isNotNull() & (F.trim(column.cast("string")) == "")
        aggregations.append(F.sum(column.isNull().cast("long")).alias(f"{name}__null"))
        aggregations.append(F.sum(is_empty.cast("long")).alias(f"{name}__empty"))

    counts = df.agg(*aggregations).first()
    rows = counts["__rows"]
    report = []
    for name in df.columns:
        nulls = counts[f"{name}__null"] or 0
        empty = counts[f"{name}__empty"] or 0
        report.append(
            (
                table,
                name,
                rows,
                nulls,
                empty,
                nulls + empty,
                _pct(nulls, rows),
                _pct(nulls + empty, rows),
            )
        )
    return spark.createDataFrame(report, MISSING_REPORT_SCHEMA)


# --------------------------------------------------------------------------
# Typing
# --------------------------------------------------------------------------


def typed(df, table):
    """Cast the bronze string columns to their expected types.

    ``try_cast`` turns values that cannot be converted into null instead of
    failing; ``invalid_casts`` counts how many values that affects.
    """
    return df.select(
        [
            F.col(name) if kind == "string" else F.col(name).try_cast(kind).alias(name)
            for name, kind in expected_types(table).items()
        ]
    )


def invalid_casts(bronze_df, table):
    """Return ``[(column, type, count)]`` of non-null values that fail to cast."""
    numeric = [(n, k) for n, k in expected_types(table).items() if k != "string"]
    if not numeric:
        return []
    counts = bronze_df.agg(
        *[
            F.sum(
                (F.col(n).isNotNull() & F.col(n).try_cast(k).isNull()).cast("long")
            ).alias(n)
            for n, k in numeric
        ]
    ).first()
    return [(n, k, counts[n] or 0) for n, k in numeric]


# --------------------------------------------------------------------------
# Rule-based checks
# --------------------------------------------------------------------------


def _split(column):
    """Comma-separated IMDb list column -> array (null stays null).

    Empty items (``"nm1,"``) are removed so they are not counted as orphans.
    """
    return F.array_remove(F.split(F.col(column), ","), "")


def _row_checks():
    """Checks evaluated row by row on a single (typed) table.

    Returns ``{table: [(category, check, description, condition)]}``.
    """
    year = CURRENT_YEAR
    return {
        "name.basics": [
            ("impossible_value", "death_before_birth",
             "deathYear is earlier than birthYear",
             F.col("deathYear") < F.col("birthYear")),
            ("impossible_value", "birth_in_future",
             f"birthYear is after {year}",
             F.col("birthYear") > year),
            ("impossible_value", "death_in_future",
             f"deathYear is after {year}",
             F.col("deathYear") > year),
            ("implausible_value", "lifespan_over_max",
             f"deathYear - birthYear is over {MAX_PLAUSIBLE_AGE} years",
             (F.col("deathYear") - F.col("birthYear")) > MAX_PLAUSIBLE_AGE),
            ("implausible_value", "probably_missing_death_year",
             f"no deathYear but born more than {MAX_PLAUSIBLE_AGE} years ago",
             F.col("deathYear").isNull()
             & (F.col("birthYear") < year - MAX_PLAUSIBLE_AGE)),
        ],
        "title.basics": [
            ("impossible_value", "end_before_start",
             "endYear is earlier than startYear",
             F.col("endYear") < F.col("startYear")),
            ("impossible_value", "start_before_cinema",
             f"startYear is before {FIRST_FILM_YEAR}",
             F.col("startYear") < FIRST_FILM_YEAR),
            ("implausible_value", "start_in_future",
             f"startYear is after {year} (announced or wrong)",
             F.col("startYear") > year),
            ("impossible_value", "non_positive_runtime",
             "runtimeMinutes is 0 or negative",
             F.col("runtimeMinutes") <= 0),
            ("implausible_value", "runtime_over_24h",
             "runtimeMinutes is over 1440 (24 hours)",
             F.col("runtimeMinutes") > 1440),
            ("impossible_value", "invalid_is_adult",
             "isAdult is not 0 or 1",
             ~F.col("isAdult").isin(0, 1)),
            ("inconsistent_value", "end_year_on_non_series",
             "endYear is set on a title that is not a series",
             F.col("endYear").isNotNull()
             & ~F.col("titleType").isin("tvSeries", "tvMiniSeries")),
        ],
        "title.akas": [
            ("impossible_value", "invalid_is_original_title",
             "isOriginalTitle is not 0 or 1",
             ~F.col("isOriginalTitle").isin(0, 1)),
        ],
        "title.episode": [
            ("impossible_value", "episode_is_own_parent",
             "parentTconst equals tconst",
             F.col("tconst") == F.col("parentTconst")),
            ("impossible_value", "negative_season_or_episode",
             "seasonNumber or episodeNumber is negative",
             (F.col("seasonNumber") < 0) | (F.col("episodeNumber") < 0)),
            ("inconsistent_value", "episode_number_without_season",
             "only one of seasonNumber / episodeNumber is set",
             F.col("seasonNumber").isNull() != F.col("episodeNumber").isNull()),
        ],
        "title.ratings": [
            ("impossible_value", "rating_out_of_range",
             "averageRating is outside [1, 10]",
             (F.col("averageRating") < 1) | (F.col("averageRating") > 10)),
            ("impossible_value", "non_positive_votes",
             "numVotes is 0 or negative",
             F.col("numVotes") <= 0),
        ],
    }


def row_checks(table):
    """``[(category, check, condition)]`` of the row checks of ``table``.

    Used by the cleaning (``preparation``) so that every value counted here is
    also flagged in the silver tables.
    """
    return [(category, check, cond) for category, check, _, cond in _row_checks()[table]]


def not_archive(category):
    """True when ``category`` is not an archive category (null counts as not archive)."""
    return ~F.coalesce(category.isin(*ARCHIVE_CATEGORIES), F.lit(False))


def _count_conditions(df, conditions):
    counts = df.agg(
        F.count(F.lit(1)).alias("__rows"),
        *[
            F.sum(F.coalesce(cond, F.lit(False)).cast("long")).alias(f"c{i}")
            for i, cond in enumerate(conditions)
        ],
    ).first()
    return counts["__rows"], [counts[f"c{i}"] or 0 for i in range(len(conditions))]


def _orphans(child, child_column, parent, parent_column, explode=False):
    """Count child rows whose key has no match in the parent table.

    With ``explode=True`` the child column is a comma-separated list and the
    count is on individual references.
    """
    if explode:
        keys = child.select(F.explode(_split(child_column)).alias("key"))
    else:
        keys = child.select(F.col(child_column).alias("key")).where(F.col("key").isNotNull())
    parents = parent.select(F.col(parent_column).alias("key")).distinct()
    total = keys.count()
    orphans = keys.join(parents, "key", "left_anti").count()
    return orphans, total


def _duplicates(df, keys):
    total = df.count()
    distinct = df.select(*keys).distinct().count()
    return total - distinct, total


def issue_report(spark, tables):
    """Run every check on ``tables`` (``{name: typed DataFrame}``)."""
    rows = []

    def add(table, category, check, description, affected, total):
        rows.append(
            (table, category, check, description, affected, total, _pct(affected, total))
        )

    # 1. Row-level checks, one pass per table.
    for table, checks in _row_checks().items():
        total, counts = _count_conditions(tables[table], [c[3] for c in checks])
        for (category, check, description, _), affected in zip(checks, counts):
            add(table, category, check, description, affected, total)

    # 2. Cross-table checks on principals.
    names = tables["name.basics"].select("nconst", "birthYear", "deathYear")
    titles = tables["title.basics"].select("tconst", "startYear")
    principals = (
        tables["title.principals"]
        .select("tconst", "nconst", "category")
        .join(titles, "tconst")
        .join(names, "nconst")
    )
    total, (born_after, died_before, died_before_no_archive) = _count_conditions(
        principals,
        [
            F.col("birthYear") > F.col("startYear"),
            F.col("deathYear") < F.col("startYear"),
            (F.col("deathYear") < F.col("startYear")) & not_archive(F.col("category")),
        ],
    )
    add("title.principals", "cross_table", "principal_born_after_title",
        "principal born after the title's startYear", born_after, total)
    add("title.principals", "cross_table", "principal_died_before_title",
        "principal died before the title's startYear (all categories)",
        died_before, total)
    add("title.principals", "cross_table", "principal_died_before_title_not_archive",
        "same, excluding archive_footage / archive_sound",
        died_before_no_archive, total)

    episodes = (
        tables["title.episode"]
        .join(titles, "tconst")
        .join(
            titles.select(F.col("tconst").alias("parentTconst"),
                          F.col("startYear").alias("parentStartYear")),
            "parentTconst",
        )
    )
    total, (before_series,) = _count_conditions(
        episodes, [F.col("startYear") < F.col("parentStartYear")]
    )
    add("title.episode", "cross_table", "episode_before_series",
        "episode startYear is earlier than its series startYear", before_series, total)

    # 3. Orphan foreign keys.
    basics, people = tables["title.basics"], tables["name.basics"]
    foreign_keys = [
        ("title.akas", "titleId", basics, "tconst", False),
        ("title.crew", "tconst", basics, "tconst", False),
        ("title.crew", "directors", people, "nconst", True),
        ("title.crew", "writers", people, "nconst", True),
        ("title.episode", "tconst", basics, "tconst", False),
        ("title.episode", "parentTconst", basics, "tconst", False),
        ("title.principals", "tconst", basics, "tconst", False),
        ("title.principals", "nconst", people, "nconst", False),
        ("title.ratings", "tconst", basics, "tconst", False),
        ("name.basics", "knownForTitles", basics, "tconst", True),
    ]
    for table, column, parent, parent_column, explode in foreign_keys:
        affected, total = _orphans(tables[table], column, parent, parent_column, explode)
        parent_name = "title.basics" if parent is basics else "name.basics"
        unit = "references" if explode else "rows"
        add(table, "orphan_foreign_key", f"orphan_{column}",
            f"{unit} in {column} with no match in {parent_name}.{parent_column}",
            affected, total)

    # 4. Duplicate primary keys.
    primary_keys = {
        "name.basics": ["nconst"],
        "title.basics": ["tconst"],
        "title.crew": ["tconst"],
        "title.episode": ["tconst"],
        "title.ratings": ["tconst"],
        "title.akas": ["titleId", "ordering"],
        "title.principals": ["tconst", "ordering"],
    }
    for table, keys in primary_keys.items():
        affected, total = _duplicates(tables[table], keys)
        add(table, "duplicate", "duplicate_key",
            f"extra rows sharing the same ({', '.join(keys)})", affected, total)

    return spark.createDataFrame(rows, ISSUE_REPORT_SCHEMA)


def union_all(dataframes):
    return reduce(lambda a, b: a.unionByName(b), dataframes)
