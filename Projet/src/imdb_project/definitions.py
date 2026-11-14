"""Definitions shared by several parts of the project.

What a feature film, a reliable rating and an actor are is used by the
exploration and the enrichment: it is defined once here.
"""

from pyspark.sql import functions as F

# A feature film runs more than 40 minutes (AMPAS, BFI and AFI definition).
FEATURE_MIN_RUNTIME = 40
# Ratings based on fewer votes are considered unreliable.
MIN_VOTES = 1000
# Acting credits in title.principals.
ACTING_CATEGORIES = ["actor", "actress"]


def feature_films(titles, min_runtime=FEATURE_MIN_RUNTIME):
    """Movies (``titleType = movie``) longer than ``min_runtime`` minutes."""
    return titles.where(
        (F.col("titleType") == "movie") & (F.col("runtimeMinutes") > min_runtime)
    )


def rated(df, ratings, min_votes=0):
    """Join ratings and keep titles with at least ``min_votes`` votes."""
    return df.join(
        ratings.select("tconst", "averageRating", "numVotes"), "tconst"
    ).where(F.col("numVotes") >= min_votes)
