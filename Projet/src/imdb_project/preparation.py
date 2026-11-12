"""Cleaning of the IMDb tables: bronze (raw strings) -> silver (clean, typed).

Rules applied (see ``data/reports/quality_issues`` for the counts):

1. Types: every column is cast to its expected type (``schemas.TABLE_COLUMNS``);
   0/1 flags become booleans.
2. Lists: comma-separated columns become arrays; ``characters`` (a JSON
   array in the source) is parsed into an array.
3. Impossible values are set to null (e.g. a runtime of 0 minutes, birth and
   death years when the death comes first).
4. Suspicious but possible values are kept and listed in a ``dq_flags`` array
   column (e.g. an actor who died before the release: archive footage,
   posthumous release...).
5. Rows that belong to a title missing from ``title.basics`` are dropped:
   they cannot be linked to anything. Rows referencing a missing person are
   kept and flagged, because the title side is still valid. Unknown people
   in the ``directors`` / ``writers`` lists of ``title.crew`` are removed
   from the lists (the row is kept and flagged).

Every silver table has a ``dq_flags`` column (empty array when nothing was
found), so ``F.size("dq_flags") == 0`` selects fully clean rows.
"""

from pyspark.sql import functions as F

from imdb_project.quality import (
    ARCHIVE_CATEGORIES,
    CURRENT_YEAR,
    MAX_PLAUSIBLE_AGE,
    typed,
)

LIST_COLUMNS = {
    "name.basics": ["primaryProfession", "knownForTitles"],
    "title.akas": ["types", "attributes"],
    "title.basics": ["genres"],
    "title.crew": ["directors", "writers"],
}
BOOLEAN_COLUMNS = {
    "title.akas": ["isOriginalTitle"],
    "title.basics": ["isAdult"],
}


def flags(*rules):
    """Build an array column with the names of the rules that are true.

    ``rules`` are ``(name, condition)`` pairs; null conditions count as false.
    """
    if not rules:
        return F.array().cast("array<string>")
    candidates = F.array(*[F.when(cond, F.lit(name)) for name, cond in rules])
    return F.filter(candidates, lambda flag: flag.isNotNull())


def _base(df, table):
    """Cast types, split list columns and convert 0/1 columns to booleans."""
    df = typed(df, table)
    for column in LIST_COLUMNS.get(table, []):
        df = df.withColumn(column, F.split(F.col(column), ","))
    for column in BOOLEAN_COLUMNS.get(table, []):
        df = df.withColumn(column, F.col(column) == 1)
    return df


def clean_names(bronze):
    df = _base(bronze, "name.basics")
    death_before_birth = F.col("deathYear") < F.col("birthYear")
    df = df.withColumn(
        "dq_flags",
        flags(
            ("death_before_birth", death_before_birth),
            ("lifespan_over_max",
             (F.col("deathYear") - F.col("birthYear")) > MAX_PLAUSIBLE_AGE),
            ("probably_missing_death_year",
             F.col("deathYear").isNull()
             & (F.col("birthYear") < CURRENT_YEAR - MAX_PLAUSIBLE_AGE)),
        ),
    )
    # Which of the two years is wrong cannot be known: both are removed.
    for column in ("birthYear", "deathYear"):
        df = df.withColumn(
            column, F.when(death_before_birth, None).otherwise(F.col(column))
        )
    return df


def clean_titles(bronze):
    df = _base(bronze, "title.basics")
    df = df.withColumn(
        "dq_flags",
        flags(
            ("non_positive_runtime", F.col("runtimeMinutes") <= 0),
            ("runtime_over_24h", F.col("runtimeMinutes") > 1440),
            ("start_in_future", F.col("startYear") > CURRENT_YEAR),
            ("end_before_start", F.col("endYear") < F.col("startYear")),
        ),
    )
    df = df.withColumn(
        "runtimeMinutes",
        F.when(F.col("runtimeMinutes") <= 0, None).otherwise(F.col("runtimeMinutes")),
    )
    return df.withColumn(
        "endYear",
        F.when(F.col("endYear") < F.col("startYear"), None).otherwise(F.col("endYear")),
    )


def _keep_known_titles(df, titles, column="tconst"):
    known = titles.select(F.col("tconst").alias(column))
    return df.join(known, column, "left_semi")


def clean_akas(bronze, titles):
    df = _base(bronze, "title.akas")
    df = _keep_known_titles(df, titles, "titleId")
    return df.withColumn("dq_flags", flags())


def _drop_unknown_people(df, column, names):
    """Remove from the ``column`` arrays the ids missing from ``name.basics``.

    The order of the remaining ids is kept. Adds a boolean ``__removed_<column>``
    column, true when at least one id was removed. A list whose ids are all
    unknown becomes null, like a list that was empty in the source.
    """
    known = names.select(F.col("nconst").alias("__person"))
    kept = (
        df.select("tconst", F.posexplode(column).alias("__pos", "__person"))
        .join(known, "__person", "left_semi")
        .groupBy("tconst")
        .agg(F.sort_array(F.collect_list(F.struct("__pos", "__person"))).alias("__kept"))
        .select("tconst", F.col("__kept.__person").alias("__kept"))
    )
    df = df.join(kept, "tconst", "left")
    removed = F.coalesce(F.size(column), F.lit(0)) > F.coalesce(F.size("__kept"), F.lit(0))
    return (
        df.withColumn(f"__removed_{column}", F.coalesce(removed, F.lit(False)))
        .withColumn(column, F.col("__kept"))
        .drop("__kept")
    )


def clean_crew(bronze, titles, names):
    df = _base(bronze, "title.crew")
    df = _keep_known_titles(df, titles)
    # Unknown people cannot be linked to anything: they are removed from the
    # lists, and the row is flagged so the removal stays visible.
    for column in ("directors", "writers"):
        df = _drop_unknown_people(df, column, names)
    df = df.withColumn(
        "dq_flags",
        flags(
            ("orphan_director_removed", F.col("__removed_directors")),
            ("orphan_writer_removed", F.col("__removed_writers")),
        ),
    )
    return df.select("tconst", "directors", "writers", "dq_flags")


def clean_ratings(bronze, titles):
    df = _base(bronze, "title.ratings")
    df = _keep_known_titles(df, titles)
    return df.withColumn("dq_flags", flags())


def clean_episodes(bronze, titles):
    df = _base(bronze, "title.episode")
    df = _keep_known_titles(df, titles)
    df = _keep_known_titles(df, titles, "parentTconst")
    years = titles.select("tconst", "startYear")
    df = df.join(years, "tconst", "left").join(
        years.select(
            F.col("tconst").alias("parentTconst"),
            F.col("startYear").alias("parentStartYear"),
        ),
        "parentTconst",
        "left",
    )
    df = df.withColumn(
        "dq_flags",
        flags(("episode_before_series",
               F.col("startYear") < F.col("parentStartYear"))),
    )
    return df.select("tconst", "parentTconst", "seasonNumber", "episodeNumber", "dq_flags")


def clean_principals(bronze, titles, names):
    df = _base(bronze, "title.principals")
    df = _keep_known_titles(df, titles)
    df = df.withColumn("characters", F.from_json("characters", "array<string>"))

    people = names.select(
        "nconst", "birthYear", "deathYear", F.lit(True).alias("__known_person")
    )
    df = df.join(titles.select("tconst", "startYear"), "tconst", "left").join(
        people, "nconst", "left"
    )
    df = df.withColumn(
        "dq_flags",
        flags(
            ("orphan_nconst", F.col("__known_person").isNull()),
            ("principal_born_after_title", F.col("birthYear") > F.col("startYear")),
            ("principal_died_before_title",
             (F.col("deathYear") < F.col("startYear"))
             & ~F.col("category").isin(*ARCHIVE_CATEGORIES)),
        ),
    )
    return df.select(
        "tconst", "ordering", "nconst", "category", "job", "characters", "dq_flags"
    )
