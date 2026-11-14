"""Data exploration queries on the silver IMDb tables.

Each function takes silver DataFrames and returns result DataFrames. The
definitions shared with the other parts (feature film, reliable rating) are in
``definitions.py``; the ones specific to a question are module constants or
parameters, so they can be changed in one place.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from imdb_project.definitions import (
    ACTING_CATEGORIES,
    MIN_VOTES,
    feature_films,
    rated,
)

# A film is "successful" when it is well rated by enough people (Q6).
SUCCESS_MIN_RATING = 8.0
SUCCESS_MIN_VOTES = 1000


# --------------------------------------------------------------------------
# Q1. Feature films whose runtime is far from their genre's average
# --------------------------------------------------------------------------


# Flags whose films are left out of the genre statistics. Empty: quartiles are
# not moved by a few extreme runtimes, so the films over 24 hours are kept.
Q1_EXCLUDED_FLAGS = []


def runtime_outliers_by_genre(titles, k=3.0, min_genre_size=30, log_scale=False,
                              excluded_flags=Q1_EXCLUDED_FLAGS):
    """Feature films whose runtime is notably different from their genre's.

    Robust method (Tukey fences): for each genre, ``Q1`` and ``Q3`` are the
    first and third quartiles of the runtime (or of its logarithm when
    ``log_scale`` is true) and ``IQR = Q3 - Q1``. A film is an outlier in a
    genre when its value is below ``Q1 - k * IQR`` or above ``Q3 + k * IQR``
    (``k = 3``: "far out" values). Quartiles do not move when a few extreme
    values are added, unlike the mean and the standard deviation.

    A film with several genres is evaluated in each of them. Genres with fewer
    than ``min_genre_size`` films are skipped. Films carrying one of
    ``excluded_flags`` are left out of the statistics.

    Returns ``(outliers, genre_stats, excluded)``; ``excluded`` lists the
    films left out because of their flags.
    """
    value = F.log("runtimeMinutes") if log_scale else F.col("runtimeMinutes")
    films = feature_films(titles).select(
        "tconst",
        "primaryTitle",
        "startYear",
        "runtimeMinutes",
        "dq_flags",
        value.alias("x"),
        F.explode("genres").alias("genre"),
    )
    flagged = F.arrays_overlap(
        "dq_flags", F.array(*[F.lit(f) for f in excluded_flags])
    ) if excluded_flags else F.lit(False)
    excluded = films.where(flagged).drop("x")
    films = films.where(~flagged)

    stats = (
        films.groupBy("genre")
        .agg(
            F.count(F.lit(1)).alias("films"),
            F.avg("runtimeMinutes").alias("avg_runtime"),
            F.percentile("runtimeMinutes", 0.5).alias("median_runtime"),
            F.percentile("x", 0.25).alias("q1_x"),
            F.percentile("x", 0.75).alias("q3_x"),
        )
        .where(F.col("films") >= min_genre_size)
        .withColumn("iqr_x", F.col("q3_x") - F.col("q1_x"))
        .withColumn("low_x", F.col("q1_x") - k * F.col("iqr_x"))
        .withColumn("high_x", F.col("q3_x") + k * F.col("iqr_x"))
    )

    # Distance outside the fence, in IQRs (> k for every outlier).
    distance = (
        F.when(F.col("x") > F.col("q3_x"), (F.col("x") - F.col("q3_x")) / F.col("iqr_x"))
        .when(F.col("x") < F.col("q1_x"), (F.col("q1_x") - F.col("x")) / F.col("iqr_x"))
        .otherwise(F.lit(0.0))
    )
    outliers = (
        films.join(F.broadcast(stats), "genre")
        .where((F.col("x") < F.col("low_x")) | (F.col("x") > F.col("high_x")))
        .withColumn(
            "direction",
            F.when(F.col("x") > F.col("high_x"), F.lit("longer")).otherwise(F.lit("shorter")),
        )
        .select(
            "genre",
            "tconst",
            "primaryTitle",
            "startYear",
            "runtimeMinutes",
            F.round("median_runtime", 1).alias("genre_median_runtime"),
            F.round(distance, 2).alias("iqr_distance"),
            "direction",
        )
    )

    bound = (lambda v: F.exp(v)) if log_scale else (lambda v: v)
    counts = outliers.groupBy("genre").agg(
        F.sum((F.col("direction") == "longer").cast("int")).alias("longer"),
        F.sum((F.col("direction") == "shorter").cast("int")).alias("shorter"),
    )
    genre_stats = (
        stats.join(counts, "genre", "left")
        .fillna(0, ["longer", "shorter"])
        .select(
            "genre",
            "films",
            F.round("avg_runtime", 1).alias("avg_runtime"),
            F.round("median_runtime", 1).alias("median_runtime"),
            F.round(bound(F.col("q1_x")), 1).alias("q1_min"),
            F.round(bound(F.col("q3_x")), 1).alias("q3_min"),
            F.round(bound(F.col("low_x")), 1).alias("lower_bound_min"),
            F.round(bound(F.col("high_x")), 1).alias("upper_bound_min"),
            "longer",
            "shorter",
            (F.col("longer") + F.col("shorter")).alias("outliers"),
        )
    )
    return outliers, genre_stats, excluded.select(
        "genre", "tconst", "primaryTitle", "startYear", "runtimeMinutes", "dq_flags")


# --------------------------------------------------------------------------
# Q2 / Q3. Most popular genres per year
# --------------------------------------------------------------------------


# Popularity measures compared in Q2: name -> (rank column, value column).
POPULARITY_METHODS = {
    "votes": ("rank_votes", "total_votes"),
    "films": ("rank_films", "films"),
    "avg_rating": ("rank_avg_rating", "avg_rating"),
}
# A genre needs this many reliably rated films (>= MIN_VOTES votes) in a year
# to be ranked by its average rating; otherwise one or two films decide.
MIN_FILMS_FOR_RATING = 10


def genre_popularity_by_year(titles, ratings, first_year=1980, last_year=2025,
                             min_votes=MIN_VOTES,
                             min_films_for_rating=MIN_FILMS_FOR_RATING):
    """Popularity of each genre per year, measured in three ways.

    Uses the rated feature films released between ``first_year`` and
    ``last_year``; a film with several genres counts fully in each of them.

    - ``rank_votes``: total number of votes of the genre's films (audience);
    - ``rank_films``: number of films of the genre (production);
    - ``rank_avg_rating``: plain average rating of the genre's films that have
      at least ``min_votes`` votes (every film weighs the same); genres with
      fewer than ``min_films_for_rating`` such films are not ranked (null).

    ``weighted_rating`` (average rating weighted by the votes) is also given.
    ``rank`` is ``rank_votes``, used by Q3.
    """
    films = rated(
        feature_films(titles).where(
            F.col("startYear").between(first_year, last_year)
        ),
        ratings,
    ).select(
        "tconst", "startYear", "averageRating", "numVotes",
        F.explode("genres").alias("genre"),
    )
    reliable = F.col("numVotes") >= min_votes
    per_genre = films.groupBy("startYear", "genre").agg(
        F.count(F.lit(1)).alias("films"),
        F.sum("numVotes").alias("total_votes"),
        F.sum(reliable.cast("int")).alias("rated_films"),
        F.round(F.avg(F.when(reliable, F.col("averageRating"))), 2).alias("avg_rating"),
        F.round(
            F.sum(F.col("averageRating") * F.col("numVotes")) / F.sum("numVotes"), 2
        ).alias("weighted_rating"),
    ).withColumn(
        "avg_rating",
        F.when(F.col("rated_films") >= min_films_for_rating, F.col("avg_rating")),
    )
    by_year = Window.partitionBy("startYear")

    def rank_by(column):
        ranked = F.row_number().over(by_year.orderBy(F.desc_nulls_last(column), "genre"))
        return F.when(F.col(column).isNotNull(), ranked)

    return (
        per_genre.withColumn("rank_votes", rank_by("total_votes"))
        .withColumn("rank_films", rank_by("films"))
        .withColumn("rank_avg_rating", rank_by("avg_rating"))
        .withColumn("rank", F.col("rank_votes"))
        .withColumn(
            "vote_share_pct",
            F.round(100 * F.col("total_votes") / F.sum("total_votes").over(by_year), 2),
        )
        .withColumnRenamed("startYear", "year")
    )


def top_genres_per_year(popularity, method="votes", top=3):
    """The ``top`` genres per year for one popularity ``method``, one row per year."""
    rank_col, value_col = POPULARITY_METHODS[method]
    return (
        popularity.where(F.col(rank_col) <= top)
        .groupBy("year")
        .agg(
            F.array_sort(F.collect_list(F.struct(F.col(rank_col).alias("r"), "genre",
                                                 F.col(value_col).alias("v"))))
            .alias("top")
        )
        .select(
            "year",
            *[F.col("top")[i]["genre"].alias(f"genre_{i + 1}") for i in range(top)],
            *[F.col("top")[i]["v"].alias(f"{value_col}_{i + 1}") for i in range(top)],
        )
        .orderBy("year")
    )


def top_vs_rest(popularity, top=3):
    """Compare, for each year, the top genres with all the other genres.

    Votes and film counts are summed per group (a film with several genres
    is counted once per genre); ratings are averaged over the genres of the
    group.
    """
    grouped = popularity.withColumn(
        "group", F.when(F.col("rank") <= top, F.lit("top")).otherwise(F.lit("rest"))
    )
    per_group = grouped.groupBy("year", "group").agg(
        F.count(F.lit(1)).alias("genres"),
        F.sum("films").alias("films"),
        F.sum("total_votes").alias("votes"),
        F.sum("vote_share_pct").alias("vote_share_pct"),
        F.avg("avg_rating").alias("avg_rating"),
    )
    top_df = per_group.where(F.col("group") == "top").drop("group")
    rest_df = per_group.where(F.col("group") == "rest").drop("group")

    def pick(df, p):
        return df.select(
            "year", *[F.col(c).alias(f"{p}_{c}") for c in df.columns if c != "year"]
        )

    comparison = pick(top_df, "top").join(pick(rest_df, "rest"), "year", "left")
    return comparison.select(
        "year",
        F.round("top_vote_share_pct", 1).alias("top_vote_share_pct"),
        (F.col("top_films") / (F.col("top_films") + F.col("rest_films")) * 100)
        .cast("decimal(5,1)").alias("top_film_share_pct"),
        F.round(F.col("top_votes") / F.col("top_films"), 0).alias("top_votes_per_film"),
        F.round(F.col("rest_votes") / F.col("rest_films"), 0).alias("rest_votes_per_film"),
        F.round("top_avg_rating", 2).alias("top_avg_rating"),
        F.round("rest_avg_rating", 2).alias("rest_avg_rating"),
        F.col("rest_genres").alias("other_genres"),
    ).orderBy("year")


def top_genre_frequency(popularity, method="votes", top=3):
    """How many years each genre was in the top ``top`` for one ``method``."""
    rank_col, _ = POPULARITY_METHODS[method]
    return (
        popularity.where(F.col(rank_col) <= top)
        .groupBy("genre")
        .agg(
            F.count(F.lit(1)).alias("years_in_top"),
            F.sum((F.col(rank_col) == 1).cast("int")).alias("years_first"),
            F.min("year").alias("first_year"),
            F.max("year").alias("last_year"),
        )
        .orderBy(F.desc("years_in_top"), F.desc("years_first"))
    )


# --------------------------------------------------------------------------
# Q4. Directors with a single hit and several poorly rated movies
# --------------------------------------------------------------------------


def one_hit_directors(titles, crew, ratings, people, hit_rating=8.0,
                      low_rating=6.0, min_other_movies=2, min_votes=MIN_VOTES,
                      others_rule="each"):
    """Directors with exactly one feature film rated >= ``hit_rating`` and at
    least ``min_other_movies`` other feature films rated <= ``low_rating``.

    ``others_rule`` says how the other films are compared with
    ``low_rating``: ``"each"`` (every other film is rated <= ``low_rating``)
    or ``"average"`` (their average rating is <= ``low_rating``). Only feature
    films with at least ``min_votes`` votes are considered.
    """
    directed = rated(feature_films(titles).select("tconst", "primaryTitle", "startYear"),
                     ratings, min_votes).join(
        crew.select("tconst", F.explode("directors").alias("nconst")), "tconst"
    )
    is_hit = F.col("averageRating") >= hit_rating
    per_director = directed.groupBy("nconst").agg(
        F.sum(is_hit.cast("int")).alias("hits"),
        F.sum((~is_hit).cast("int")).alias("other_movies"),
        F.avg(F.when(~is_hit, F.col("averageRating"))).alias("other_avg_rating"),
        F.max(F.when(~is_hit, F.col("averageRating"))).alias("other_max_rating"),
        F.max(F.when(is_hit, F.struct("averageRating", "primaryTitle", "startYear",
                                      "numVotes"))).alias("hit"),
    )
    return (
        per_director.where(
            (F.col("hits") == 1)
            & (F.col("other_movies") >= min_other_movies)
            & (F.col("other_max_rating" if others_rule == "each" else "other_avg_rating")
               <= low_rating)
        )
        .join(people.select("nconst", "primaryName"), "nconst", "left")
        .select(
            "nconst",
            "primaryName",
            F.col("hit.primaryTitle").alias("hit_title"),
            F.col("hit.startYear").alias("hit_year"),
            F.col("hit.averageRating").alias("hit_rating"),
            F.col("hit.numVotes").alias("hit_votes"),
            "other_movies",
            F.round("other_avg_rating", 2).alias("other_avg_rating"),
            F.col("other_max_rating").alias("other_best_rating"),
        )
        .orderBy(F.desc("hit_votes"))
    )


# --------------------------------------------------------------------------
# Q5. Series whose worst episode is in the last season and best early on
# --------------------------------------------------------------------------


# Episodes get far fewer votes than films (median 22 against 78 for feature
# films), so Q5 uses its own, lower, threshold.
EPISODE_MIN_VOTES = 100
# Comparing the best and worst episodes needs enough rated episodes.
MIN_RATED_EPISODES = 10


def series_declining_episodes(titles, episodes, ratings, min_seasons=3,
                              min_episodes=10, min_votes=EPISODE_MIN_VOTES,
                              min_rated_episodes=MIN_RATED_EPISODES):
    """TV series with at least ``min_seasons`` seasons of ``min_episodes``+
    episodes, whose worst-rated episode is in the last season and whose
    best-rated episode is in season 1 or 2.

    Seasons are numbered from 1 (season 0 / unknown seasons are ignored); the
    last season is the highest season number. Only episodes with at least
    ``min_votes`` votes are compared, and the series needs at least
    ``min_rated_episodes`` of them. Ties are strict: *every* episode sharing
    the lowest rating must be in the last season, and *every* episode sharing
    the highest rating must be in season 1 or 2. The episode shown for each
    side is the one with the most votes.
    """
    series = titles.where(F.col("titleType").isin("tvSeries", "tvMiniSeries")).select(
        F.col("tconst").alias("parentTconst"),
        F.col("primaryTitle").alias("series_title"),
        F.col("startYear").alias("series_start"),
    )
    eps = episodes.where(F.col("seasonNumber") >= 1).join(series, "parentTconst")

    seasons = eps.groupBy("parentTconst", "seasonNumber").agg(
        F.count(F.lit(1)).alias("episodes")
    )
    per_series = seasons.groupBy("parentTconst").agg(
        F.max("seasonNumber").alias("last_season"),
        F.sum((F.col("episodes") >= min_episodes).cast("int")).alias("full_seasons"),
        F.count(F.lit(1)).alias("seasons"),
    ).where(F.col("full_seasons") >= min_seasons)

    rated_eps = rated(
        eps.join(per_series.select("parentTconst"), "parentTconst", "left_semi"),
        ratings,
        min_votes,
    ).join(titles.select("tconst", F.col("primaryTitle").alias("episode_title")),
           "tconst")
    by_series = Window.partitionBy("parentTconst")
    rated_eps = (
        rated_eps.withColumn("min_rating", F.min("averageRating").over(by_series))
        .withColumn("max_rating", F.max("averageRating").over(by_series))
    )
    is_worst = F.col("averageRating") == F.col("min_rating")
    is_best = F.col("averageRating") == F.col("max_rating")
    extremes = rated_eps.groupBy("parentTconst").agg(
        F.min(F.when(is_worst, F.col("seasonNumber"))).alias("worst_first_season"),
        F.sum(is_worst.cast("int")).alias("worst_ties"),
        F.max(F.when(is_best, F.col("seasonNumber"))).alias("best_last_season"),
        F.sum(is_best.cast("int")).alias("best_ties"),
        F.count(F.lit(1)).alias("rated_episodes"),
        F.max_by(F.struct("seasonNumber", "episodeNumber", "episode_title",
                          "averageRating"),
                 F.when(is_worst, F.col("numVotes"))).alias("worst"),
        F.max_by(F.struct("seasonNumber", "episodeNumber", "episode_title",
                          "averageRating"),
                 F.when(is_best, F.col("numVotes"))).alias("best"),
    )
    return (
        per_series.join(extremes, "parentTconst")
        .where(
            (F.col("worst_first_season") == F.col("last_season"))
            & (F.col("best_last_season") <= 2)
            & (F.col("rated_episodes") >= min_rated_episodes)
        )
        .join(series, "parentTconst")
        .select(
            F.col("parentTconst").alias("tconst"),
            "series_title",
            "series_start",
            "seasons",
            "full_seasons",
            "last_season",
            "rated_episodes",
            F.col("best.seasonNumber").alias("best_seasonNumber"),
            F.col("best.episodeNumber").alias("best_episodeNumber"),
            F.col("best.episode_title").alias("best_episode_title"),
            F.col("best.averageRating").alias("best_averageRating"),
            "best_ties",
            F.col("worst.seasonNumber").alias("worst_seasonNumber"),
            F.col("worst.episodeNumber").alias("worst_episodeNumber"),
            F.col("worst.episode_title").alias("worst_episode_title"),
            F.col("worst.averageRating").alias("worst_averageRating"),
            "worst_ties",
        )
        .orderBy(F.desc("best_averageRating"))
    )


# --------------------------------------------------------------------------
# Q6. Actors successful in films of several original languages
# --------------------------------------------------------------------------

# Wikidata labels that name varieties of one language, which their speakers
# understand without difficulty, are merged into one language (Q6).
LANGUAGE_GROUPS = {
    "Serbian": "Serbo-Croatian",
    "Croatian": "Serbo-Croatian",
    "Bosnian": "Serbo-Croatian",
    "Montenegrin": "Serbo-Croatian",
    "Hindi": "Hindustani",
    "Urdu": "Hindustani",
    "American English": "English",
    "British English": "English",
    "Quebec French": "French",
    "joual": "French",
    "Brazilian Portuguese": "Portuguese",
    "Modern Greek": "Greek",
    "Egyptian Arabic": "Arabic",
    "Mandarin": "Mandarin Chinese",
    "Standard Chinese": "Mandarin Chinese",
    "Putonghua": "Mandarin Chinese",
    "Standard Taiwanese Mandarin": "Mandarin Chinese",
}


def group_language(column):
    """Replace a language label by its group (``LANGUAGE_GROUPS``) if any."""
    mapping = F.create_map(*[F.lit(x) for pair in LANGUAGE_GROUPS.items() for x in pair])
    return F.coalesce(mapping[F.col(column)], F.col(column))


def successful_films(titles, ratings, min_rating=SUCCESS_MIN_RATING,
                     min_votes=SUCCESS_MIN_VOTES):
    """Feature films rated >= ``min_rating`` with at least ``min_votes`` votes."""
    return rated(
        feature_films(titles).select("tconst", "primaryTitle", "startYear"),
        ratings, min_votes,
    ).where(F.col("averageRating") >= min_rating)


def multilingual_successful_actors(successful, languages, principals, people):
    """Actors / actresses with successful films in several original languages.

    ``successful`` comes from ``successful_films``. ``languages`` has one row
    per film (``tconst``, ``original_language``): the first original
    language given by Wikidata (property P364). Films without a known
    language are left out. Returns one row per person with the distinct
    languages of their successful films (``languages`` >= 2).
    """
    films = successful.join(languages, "tconst")
    casts = principals.where(F.col("category").isin(*ACTING_CATEGORIES)).select(
        "tconst", "nconst"
    ).distinct()
    per_person = (
        casts.join(films, "tconst")
        .groupBy("nconst")
        .agg(
            F.array_sort(F.collect_set("original_language")).alias("language_list"),
            F.count(F.lit(1)).alias("successful_films"),
            F.array_sort(F.collect_list(
                F.concat_ws(" ", "primaryTitle",
                            F.concat(F.lit("("), "original_language", F.lit(")")))
            )).alias("films"),
        )
        .withColumn("languages", F.size("language_list"))
        .where(F.col("languages") >= 2)
    )
    return per_person.join(
        people.select("nconst", "primaryName"), "nconst", "left"
    ).select("nconst", "primaryName", "languages", "language_list",
             "successful_films", "films").orderBy(F.desc("languages"),
                                                  F.desc("successful_films"))


# --------------------------------------------------------------------------
# Q7. Long careers whose best movies came early
# --------------------------------------------------------------------------


def early_peak_actors(titles, ratings, principals, people, min_career_years=20,
                      min_movies=15, min_votes=MIN_VOTES, min_rated=5,
                      min_rated_per_half=2):
    """Actors / actresses with a career of ``min_career_years``+ years and
    ``min_movies``+ feature films whose ratings peaked early.

    The career (first and last year, number of films) is measured on all the
    actor's released feature films. Ratings are compared on the films with at
    least ``min_votes`` votes only. A career "peaked early" when both are
    true:

    - the best-rated of these films was released in the first half of the
      career;
    - their average rating in the first half is higher than in the second
      half.

    To compare the halves on enough films, the person needs at least
    ``min_rated`` rated films, with at least ``min_rated_per_half`` in each
    half.
    """
    casts = principals.where(F.col("category").isin(*ACTING_CATEGORIES)).select(
        "tconst", "nconst"
    ).distinct()
    released = feature_films(titles).where(
        F.col("startYear").isNotNull()
        & ~F.array_contains("dq_flags", "start_in_future")
    ).select("tconst", "primaryTitle", "startYear")
    films = released.join(
        ratings.select("tconst", "averageRating", "numVotes"), "tconst", "left"
    ).join(casts, "tconst")

    by_person = Window.partitionBy("nconst")
    reliable = F.col("numVotes") >= min_votes
    films = (
        films.withColumn("first_year", F.min("startYear").over(by_person))
        .withColumn("last_year", F.max("startYear").over(by_person))
        .withColumn("mid_year", (F.col("first_year") + F.col("last_year")) / 2)
        .withColumn("early", F.col("startYear") <= F.col("mid_year"))
    )
    rating = F.when(reliable, F.col("averageRating"))
    per_person = films.groupBy("nconst", "first_year", "last_year").agg(
        F.count(F.lit(1)).alias("movies"),
        F.sum(reliable.cast("int")).alias("rated_movies"),
        F.sum((reliable & F.col("early")).cast("int")).alias("early_rated"),
        F.sum((reliable & ~F.col("early")).cast("int")).alias("late_rated"),
        F.avg(F.when(F.col("early"), rating)).alias("early_avg"),
        F.avg(F.when(~F.col("early"), rating)).alias("late_avg"),
        F.max(F.when(reliable, F.struct("averageRating", "numVotes", "startYear",
                                        "primaryTitle"))).alias("best"),
    )
    return (
        per_person.withColumn("career_years", F.col("last_year") - F.col("first_year"))
        .where(
            (F.col("career_years") >= min_career_years)
            & (F.col("movies") >= min_movies)
            & (F.col("rated_movies") >= min_rated)
            & (F.col("early_rated") >= min_rated_per_half)
            & (F.col("late_rated") >= min_rated_per_half)
            & (F.col("best.startYear") <= (F.col("first_year") + F.col("last_year")) / 2)
            & (F.col("early_avg") > F.col("late_avg"))
        )
        .join(people.select("nconst", "primaryName"), "nconst", "left")
        .select(
            "nconst",
            "primaryName",
            "first_year",
            "last_year",
            "career_years",
            "movies",
            "rated_movies",
            F.col("best.primaryTitle").alias("best_title"),
            F.col("best.startYear").alias("best_year"),
            F.col("best.averageRating").alias("best_rating"),
            F.round("early_avg", 2).alias("early_avg_rating"),
            F.round("late_avg", 2).alias("late_avg_rating"),
            F.round(F.col("early_avg") - F.col("late_avg"), 2).alias("rating_drop"),
        )
        .orderBy(F.desc("rating_drop"))
    )


# --------------------------------------------------------------------------
# Q8. Multi-talented film professionals
# --------------------------------------------------------------------------

ROLE_FAMILIES = {
    "actor": "acting",
    "actress": "acting",
    "director": "directing",
    "writer": "writing",
}


def movie_roles(titles, crew, principals):
    """One row per (feature film, person, role family), from principals and crew."""
    movie_ids = feature_films(titles).select("tconst")
    family = F.create_map(*[F.lit(x) for kv in ROLE_FAMILIES.items() for x in kv])
    from_principals = principals.select(
        "tconst", "nconst", family[F.col("category")].alias("role")
    ).where(F.col("role").isNotNull())
    from_crew = crew.select(
        "tconst", F.explode("directors").alias("nconst"), F.lit("directing").alias("role")
    ).unionByName(
        crew.select(
            "tconst", F.explode("writers").alias("nconst"), F.lit("writing").alias("role")
        )
    )
    return (
        from_principals.unionByName(from_crew)
        .join(movie_ids, "tconst", "left_semi")
        .distinct()
    )


def multi_talented(titles, crew, principals, ratings, people, min_votes=MIN_VOTES,
                   min_movies_per_role=1):
    """People with at least two role families (acting, directing, writing)
    shown on different feature films, and how they perform when combining
    roles.

    A person is multi-talented when they hold two different role families on
    two *different* films: holding several roles on one single film is not
    enough. This is the same as having at least two role families and at
    least two films (with two films and two families, a pair of different
    roles on different films always exists).

    Returns ``(people_df, combo_performance, person_comparison)``:

    - ``people_df``: multi-talented people and their movie count per role.
    - ``combo_performance``: average rating of (person, movie) pairs per
      combination of roles held on the same movie (multi-talented people only).
    - ``person_comparison``: for each multi-talented person, average rating
      of movies where they held several roles vs a single role.
    """
    roles = movie_roles(titles, crew, principals)
    per_person_role = roles.groupBy("nconst", "role").agg(
        F.countDistinct("tconst").alias("movies")
    ).where(F.col("movies") >= min_movies_per_role)
    talented = (
        per_person_role.groupBy("nconst")
        .agg(
            F.array_sort(F.collect_list("role")).alias("roles"),
            F.map_from_entries(F.collect_list(F.struct("role", "movies")))
            .alias("movies_per_role"),
        )
        .where(F.size("roles") >= 2)
        .join(
            roles.groupBy("nconst").agg(F.countDistinct("tconst").alias("films"))
            .where(F.col("films") >= 2),
            "nconst",
        )
    )
    people_df = talented.join(
        people.select("nconst", "primaryName"), "nconst", "left"
    ).select(
        "nconst",
        "primaryName",
        F.array_join("roles", "+").alias("roles"),
        F.coalesce(F.col("movies_per_role")["acting"], F.lit(0)).alias("acting_movies"),
        F.coalesce(F.col("movies_per_role")["directing"], F.lit(0)).alias("directing_movies"),
        F.coalesce(F.col("movies_per_role")["writing"], F.lit(0)).alias("writing_movies"),
    )

    per_movie = (
        roles.join(talented.select("nconst"), "nconst", "left_semi")
        .groupBy("nconst", "tconst")
        .agg(F.array_sort(F.collect_set("role")).alias("roles_on_movie"))
        .withColumn("combo", F.array_join("roles_on_movie", "+"))
        .withColumn("multi_role", F.size("roles_on_movie") >= 2)
    )
    per_movie = rated(per_movie, ratings, min_votes)

    combo_performance = (
        per_movie.groupBy("combo")
        .agg(
            F.count(F.lit(1)).alias("person_movies"),
            F.countDistinct("nconst").alias("people"),
            F.round(F.avg("averageRating"), 2).alias("avg_rating"),
            F.round(F.expr("percentile_approx(averageRating, 0.5)"), 2)
            .alias("median_rating"),
            F.round(F.avg("numVotes"), 0).alias("avg_votes"),
        )
        .orderBy(F.desc("person_movies"))
    )

    person_comparison = (
        per_movie.groupBy("nconst")
        .agg(
            F.sum(F.col("multi_role").cast("int")).alias("multi_role_movies"),
            F.sum((~F.col("multi_role")).cast("int")).alias("single_role_movies"),
            F.avg(F.when(F.col("multi_role"), F.col("averageRating")))
            .alias("multi_role_avg"),
            F.avg(F.when(~F.col("multi_role"), F.col("averageRating")))
            .alias("single_role_avg"),
        )
        .where((F.col("multi_role_movies") > 0) & (F.col("single_role_movies") > 0))
        .join(people.select("nconst", "primaryName"), "nconst", "left")
        .select(
            "nconst",
            "primaryName",
            "multi_role_movies",
            "single_role_movies",
            F.round("multi_role_avg", 2).alias("multi_role_avg_rating"),
            F.round("single_role_avg", 2).alias("single_role_avg_rating"),
            F.round(F.col("multi_role_avg") - F.col("single_role_avg"), 2)
            .alias("difference"),
        )
    )
    return people_df, combo_performance, person_comparison
