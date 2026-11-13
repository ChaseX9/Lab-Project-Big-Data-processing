"""Data enrichment with Wikipedia: target selection and Spark processing.

The pipeline has three steps:

1. ``select_targets`` (Spark): the films to enrich (top N feature films per
   genre and year) and their first principals.
2. ``scripts/fetch_wikipedia.py`` (plain Python): downloads the Wikipedia
   articles of the targets, through Wikidata to map IMDb ids to articles.
   Raw responses are stored as JSON lines with their source URL.
3. ``build_enriched_*`` (Spark): schema enforcement, synopsis / biography
   extraction, cleaning, notes for missing information, and the final join.

Nothing depends on the size of the target list: the same code runs on the
full IMDb dataset by changing the parameters of ``select_targets``.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from imdb_project.definitions import MIN_VOTES, feature_films, rated

SOURCE_NAME = "en.wikipedia.org"

# Schema of the raw records written by scripts/fetch_wikipedia.py.
RAW_SCHEMA = StructType(
    [
        StructField("imdb_id", StringType(), False),
        StructField("kind", StringType(), False),
        StructField("wikidata_id", StringType(), True),
        StructField("wikipedia_title", StringType(), True),
        StructField("source_url", StringType(), True),
        StructField("wikidata_url", StringType(), True),
        StructField("retrieved_at", StringType(), True),
        StructField("status", StringType(), False),
        StructField("text", StringType(), True),
    ]
)

# Section titles that hold a film synopsis in English Wikipedia articles.
SYNOPSIS_SECTIONS = ["Plot", "Plot summary", "Synopsis", "Premise", "Story",
                     "Summary", "Storyline"]

NOTES = {
    "no_article": "No English Wikipedia article is linked to this IMDb id in Wikidata.",
    "missing_page": "The Wikipedia article linked in Wikidata could not be retrieved.",
    "no_synopsis_section": "The Wikipedia article has no plot or synopsis section.",
    "empty_text": "The Wikipedia article has no text for this field.",
    "not_fetched": "Not fetched yet: run scripts/fetch_wikipedia.py.",
}


# --------------------------------------------------------------------------
# 1. Targets
# --------------------------------------------------------------------------


def select_targets(titles, ratings, principals, people, first_year=1980,
                   last_year=2025, films_per_genre=10, principals_per_film=3,
                   min_votes=MIN_VOTES):
    """Films to enrich and their principals.

    Films: the ``films_per_genre`` best-rated feature films of each (genre,
    year) between ``first_year`` and ``last_year``, among the films with at
    least ``min_votes`` votes (ties broken by the number of votes). A (genre,
    year) with fewer such films gives fewer targets. Principals: the first
    ``principals_per_film`` people of each film by IMDb credit order.

    Returns ``(film_targets, people_targets)``; a film appearing in several
    genres is listed once with all its (genre, rank) pairs.
    """
    films = rated(
        feature_films(titles).where(F.col("startYear").between(first_year, last_year)),
        ratings,
        min_votes,
    ).select("tconst", "primaryTitle", "startYear", "averageRating", "numVotes",
             F.explode("genres").alias("genre"))
    by_genre_year = Window.partitionBy("genre", "startYear").orderBy(
        F.desc("averageRating"), F.desc("numVotes"), "tconst"
    )
    top = films.withColumn("rank", F.row_number().over(by_genre_year)).where(
        F.col("rank") <= films_per_genre
    )
    film_targets = top.groupBy("tconst", "primaryTitle", "startYear", "averageRating",
                               "numVotes").agg(
        F.array_sort(F.collect_list(F.struct("genre", "rank"))).alias("top_in")
    )

    by_film = Window.partitionBy("tconst").orderBy("ordering")
    people_targets = (
        principals.join(film_targets.select("tconst"), "tconst", "left_semi")
        .where(~F.array_contains("dq_flags", "orphan_nconst"))
        .select("tconst", "nconst", "category", "ordering")
        .withColumn("position", F.row_number().over(by_film))
        .where(F.col("position") <= principals_per_film)
        .join(people.select("nconst", "primaryName"), "nconst")
    )
    return film_targets, people_targets


# --------------------------------------------------------------------------
# 3. Spark processing of the raw Wikipedia records
# --------------------------------------------------------------------------


def read_raw(spark, path):
    """Read the raw JSON lines with the enforced schema (bad lines dropped)."""
    return (
        spark.read.schema(RAW_SCHEMA)
        .option("mode", "DROPMALFORMED")
        .json(str(path))
        # A record may be fetched again: keep the latest one per id.
        .withColumn(
            "_latest",
            F.row_number().over(
                Window.partitionBy("kind", "imdb_id").orderBy(F.desc("retrieved_at"))
            ),
        )
        .where(F.col("_latest") == 1)
        .drop("_latest")
    )


def clean_text(column):
    """Normalise Wikipedia plain text: drop leftover markup, collapse spaces."""
    text = F.regexp_replace(column, r"\[\d+\]", "")          # [1] references
    text = F.regexp_replace(text, r"\{\\displaystyle[^}]*\}", "")  # math markup
    text = F.regexp_replace(text, r"[ \t]+", " ")
    text = F.regexp_replace(text, r"\n{2,}", "\n")
    return F.trim(text)


def extract_section(column, section_names):
    """Text of the first ``== Section ==`` among ``section_names``."""
    names = "|".join(section_names)
    pattern = rf"(?s)\n==\s*(?:{names})\s*==\s*\n(.*?)(?=\n==[^=]|$)"
    return F.regexp_extract(column, pattern, 1)


def _note(status, text_column, empty_reason):
    return (
        F.when(F.col("status") == "ok",
               F.when(F.length(text_column) == 0, F.lit(NOTES[empty_reason])))
        .when(F.col("status") == "no_article", F.lit(NOTES["no_article"]))
        .when(F.col("status") == "missing_page", F.lit(NOTES["missing_page"]))
        .otherwise(F.lit(NOTES["not_fetched"]))
    )


def build_film_synopses(raw_films):
    """One row per film: cleaned synopsis, lineage and a note when missing."""
    df = raw_films.withColumn(
        "synopsis", clean_text(extract_section(F.concat(F.lit("\n"), "text"),
                                               SYNOPSIS_SECTIONS))
    )
    return df.select(
        F.col("imdb_id").alias("tconst"),
        F.when(F.length("synopsis") > 0, F.col("synopsis")).alias("synopsis"),
        F.when(F.length("synopsis") > 0, F.size(F.split("synopsis", r"\s+")))
        .alias("synopsis_words"),
        _note("status", F.col("synopsis"), "no_synopsis_section").alias("synopsis_note"),
        F.lit(SOURCE_NAME).alias("source"),
        F.col("source_url").alias("synopsis_source_url"),
        F.col("wikidata_url"),
        F.to_timestamp("retrieved_at").alias("retrieved_at"),
        F.lit(True).alias("synopsis_fetched"),
    )


def build_biographies(raw_people):
    """One row per person: cleaned biography (article lead), lineage, note."""
    df = raw_people.withColumn("biography", clean_text(F.col("text")))
    return df.select(
        F.col("imdb_id").alias("nconst"),
        F.when(F.length("biography") > 0, F.col("biography")).alias("biography"),
        _note("status", F.col("biography"), "empty_text").alias("biography_note"),
        F.lit(SOURCE_NAME).alias("source"),
        F.col("source_url").alias("biography_source_url"),
        F.col("wikidata_url"),
        F.to_timestamp("retrieved_at").alias("retrieved_at"),
        F.lit(True).alias("biography_fetched"),
    )


def build_enriched_films(film_targets, people_targets, synopses, biographies):
    """Final dataset: one row per target film with its synopsis and the
    biographies of its principals (all with lineage fields).

    Targets that were not fetched yet are kept with a note, so the output
    always covers every target.
    """
    principals = (
        people_targets.join(biographies.drop("wikidata_url", "source", "retrieved_at"),
                            "nconst", "left")
        .withColumn(
            "biography_note",
            F.when(F.col("biography_fetched").isNull(), F.lit(NOTES["not_fetched"]))
            .otherwise(F.col("biography_note")),
        )
        .groupBy("tconst")
        .agg(
            F.array_sort(
                F.collect_list(
                    F.struct(
                        "position", "nconst", "primaryName", "category", "biography",
                        "biography_note", "biography_source_url",
                    )
                )
            ).alias("principals")
        )
    )
    return (
        film_targets.join(synopses, "tconst", "left")
        .withColumn(
            "synopsis_note",
            F.when(F.col("synopsis_fetched").isNull(), F.lit(NOTES["not_fetched"]))
            .otherwise(F.col("synopsis_note")),
        )
        .join(principals, "tconst", "left")
        .select(
            "tconst", "primaryTitle", "startYear", "numVotes", "top_in",
            "synopsis", "synopsis_words", "synopsis_note", "synopsis_source_url",
            "wikidata_url", "principals",
        )
    )
