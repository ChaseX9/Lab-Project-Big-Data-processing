"""Column definitions of the IMDb tables.

Source: https://developer.imdb.com/non-commercial-datasets/

``TABLE_COLUMNS`` lists, for each table, the columns in file order with the
type they are expected to have once cleaned. Columns holding comma-separated
lists (genres, directors, ...) stay strings here; splitting them is a
preparation choice.
"""

from pyspark.sql.types import StringType, StructField, StructType

TABLE_COLUMNS = {
    "name.basics": [
        ("nconst", "string"),
        ("primaryName", "string"),
        ("birthYear", "int"),
        ("deathYear", "int"),
        ("primaryProfession", "string"),
        ("knownForTitles", "string"),
    ],
    "title.akas": [
        ("titleId", "string"),
        ("ordering", "int"),
        ("title", "string"),
        ("region", "string"),
        ("language", "string"),
        ("types", "string"),
        ("attributes", "string"),
        ("isOriginalTitle", "int"),
    ],
    "title.basics": [
        ("tconst", "string"),
        ("titleType", "string"),
        ("primaryTitle", "string"),
        ("originalTitle", "string"),
        ("isAdult", "int"),
        ("startYear", "int"),
        ("endYear", "int"),
        ("runtimeMinutes", "int"),
        ("genres", "string"),
    ],
    "title.crew": [
        ("tconst", "string"),
        ("directors", "string"),
        ("writers", "string"),
    ],
    "title.episode": [
        ("tconst", "string"),
        ("parentTconst", "string"),
        ("seasonNumber", "int"),
        ("episodeNumber", "int"),
    ],
    "title.principals": [
        ("tconst", "string"),
        ("ordering", "int"),
        ("nconst", "string"),
        ("category", "string"),
        ("job", "string"),
        ("characters", "string"),
    ],
    "title.ratings": [
        ("tconst", "string"),
        ("averageRating", "double"),
        ("numVotes", "int"),
    ],
}


def string_schema(table):
    """Schema with every column as a nullable string (used for the bronze layer)."""
    return StructType(
        [StructField(name, StringType(), True) for name, _ in TABLE_COLUMNS[table]]
    )


def expected_types(table):
    """Return ``{column: type}`` for the cleaned version of ``table``."""
    return dict(TABLE_COLUMNS[table])
