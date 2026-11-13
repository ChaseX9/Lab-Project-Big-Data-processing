"""Cleaning of the IMDb tables: bronze (raw strings) -> silver (clean, typed).

Rules applied (see ``data/reports/quality_issues`` for the counts):

1. Types: every column is cast to its expected type (``schemas.TABLE_COLUMNS``);
   0/1 flags become booleans (any other value becomes null and is flagged).
2. Lists: comma-separated columns become arrays (empty items are removed);
   ``characters`` (a JSON array in the source) is parsed into an array.
3. Every row check of ``quality.py`` is also a flag of the silver row, in a
   ``dq_flags`` array column. The checks classified ``impossible_value`` set
   the columns involved to null (``IMPOSSIBLE_VALUE_COLUMNS``, e.g. a runtime
   of 0 minutes, birth and death years when the death comes first).
4. Suspicious but possible values are kept and only flagged (e.g. an actor
   who died before the release: archive footage, posthumous release...).
5. Rows that belong to a title missing from ``title.basics`` are dropped:
   they cannot be linked to anything. Rows referencing a missing person are
   kept and flagged, because the title side is still valid. Unknown people
   in the ``directors`` / ``writers`` lists of ``title.crew`` are removed
   from the lists (the row is kept and flagged).

Every silver table has a ``dq_flags`` column (empty array when nothing was
found), so ``F.size("dq_flags") == 0`` selects fully clean rows.
"""

from pyspark.sql import functions as F

from imdb_project.quality import not_archive, row_checks, typed

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
# Columns set to null when an ``impossible_value`` check of ``quality.py`` is
# true. When two years contradict each other, which one is wrong cannot be
# known: both are removed.
IMPOSSIBLE_VALUE_COLUMNS = {
    "death_before_birth": ["birthYear", "deathYear"],
    "birth_in_future": ["birthYear"],
    "death_in_future": ["deathYear"],
    "end_before_start": ["endYear"],
    "start_before_cinema": ["startYear"],
    "non_positive_runtime": ["runtimeMinutes"],
    "invalid_is_adult": ["isAdult"],
    "invalid_is_original_title": ["isOriginalTitle"],
    "episode_is_own_parent": ["parentTconst"],
    "negative_season_or_episode": ["seasonNumber", "episodeNumber"],
    "rating_out_of_range": ["averageRating"],
    "non_positive_votes": ["numVotes"],
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
    """Cast types and split list columns.

    Empty items are removed from the lists (``"nm1,"`` gives ``["nm1"]``); a
    list left empty becomes null, like a missing list in the source.
    """
    df = typed(df, table)
    for column in LIST_COLUMNS.get(table, []):
        items = F.array_remove(F.split(F.col(column), ","), "")
        df = df.withColumn(column, F.when(F.size(items) > 0, items))
    return df


def _flag_and_clean(df, table, *extra_rules):
    """Add ``dq_flags`` and apply the impossible-value rules of ``table``.

    The flags are computed first, on the original values: setting a column to
    null cannot hide another check. Then every column listed in
    ``IMPOSSIBLE_VALUE_COLUMNS`` for a raised flag is set to null, and the 0/1
    columns become booleans.
    """
    rules = [(check, cond) for _, check, cond in row_checks(table)]
    df = df.withColumn("dq_flags", flags(*rules, *extra_rules))
    impossible = [
        check for category, check, _ in row_checks(table)
        if category == "impossible_value"
    ]
    for check in impossible:
        for column in IMPOSSIBLE_VALUE_COLUMNS[check]:
            df = df.withColumn(
                column,
                F.when(F.array_contains("dq_flags", check), None).otherwise(F.col(column)),
            )
    for column in BOOLEAN_COLUMNS.get(table, []):
        df = df.withColumn(
            column,
            F.when(F.col(column) == 1, True).when(F.col(column) == 0, False),
        )
    return df


def clean_names(bronze):
    return _flag_and_clean(_base(bronze, "name.basics"), "name.basics")


def clean_titles(bronze):
    return _flag_and_clean(_base(bronze, "title.basics"), "title.basics")


def _keep_known_titles(df, titles, column="tconst"):
    known = titles.select(F.col("tconst").alias(column))
    return df.join(known, column, "left_semi")


def clean_akas(bronze, titles):
    df = _base(bronze, "title.akas")
    df = _keep_known_titles(df, titles, "titleId")
    return _flag_and_clean(df, "title.akas")


def _drop_unknown_people(df, column, names):
    """Remove from the ``column`` arrays the ids missing from ``name.basics``.

    The order of the remaining ids is kept. Adds a boolean ``__removed_<column>``
    column, true when at least one id was removed. A list whose ids are all
    unknown becomes null, like a list that was empty in the source.

    The kept ids are grouped by ``(tconst, original list)``, not by ``tconst``
    alone: if ``title.crew`` has two rows for the same title, their lists are
    not merged. Identical rows give identical lists (``collect_set``).
    """
    known = names.select(F.col("nconst").alias("__person"))
    kept = (
        df.select(
            "tconst",
            F.col(column).alias("__list"),
            F.posexplode(column).alias("__pos", "__person"),
        )
        .join(known, "__person", "left_semi")
        .groupBy("tconst", "__list")
        .agg(F.sort_array(F.collect_set(F.struct("__pos", "__person"))).alias("__kept"))
        .select(
            "tconst",
            F.col("__list").alias(column),
            F.col("__kept.__person").alias("__kept"),
        )
    )
    df = df.join(kept, ["tconst", column], "left")
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
    return _flag_and_clean(df, "title.ratings")


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
    df = _flag_and_clean(
        df,
        "title.episode",
        ("episode_before_series", F.col("startYear") < F.col("parentStartYear")),
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
             (F.col("deathYear") < F.col("startYear")) & not_archive(F.col("category"))),
        ),
    )
    return df.select(
        "tconst", "ordering", "nconst", "category", "job", "characters", "dq_flags"
    )
