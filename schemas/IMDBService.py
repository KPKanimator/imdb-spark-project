import csv
import json
import sys
from bigquery_schema_generator.generate_schema import SchemaGenerator
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


class IMDBService(object):
    """Wrapper service class for IMDB Datasets.
    """

    title_crew_schema = StructType([
        StructField('tconst', StringType(), True),
        StructField('directors', StringType(), True),
        StructField('writers', StringType(), True),
    ])

    title_akas_schema = StructType([
        StructField("titleId", StringType(), False),
        StructField("ordering", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("region", StringType(), True),
        StructField("language", StringType(), True),
        StructField("types", IntegerType(), True),
        StructField("attributes", StringType(), False),
        StructField("isOriginalTitle", IntegerType(), True)
    ])

    title_basic_schema = StructType([
        StructField("tconst", StringType(), False),
        StructField("titleType", StringType(), True),
        StructField("primaryTitle", StringType(), True),
        StructField("originalTitle", StringType(), True),
        StructField("isAdult", IntegerType(), True),
        StructField("startYear", IntegerType(), True),
        StructField("endYear", IntegerType(), False),
        StructField("runtimeMinutes", IntegerType(), True),
        StructField("genres", StringType(), True)
    ])

    name_basics_schema = StructType([
        StructField('nconst', StringType(), True),
        StructField('primaryName', StringType(), True),
        StructField('birthYear', IntegerType(), True),
        StructField('deathYear', StringType(), True),
        StructField('primaryProfession', StringType(), True),
        StructField('knownForTitles', StringType(), True),
    ])

    title_principals_schema = StructType([
        StructField('tconst', StringType(), True),
        StructField('ordering', IntegerType(), True),
        StructField('nconst', StringType(), True),
        StructField('category', StringType(), True),
        StructField('job', StringType(), True),
        StructField('characters', StringType(), True),
    ])

    title_episode_schema = StructType([
        StructField('tconst', StringType(), True),
        StructField('parentTconst', StringType(), True),
        StructField('seasonNumber', IntegerType(), True),
        StructField('episodeNumber', IntegerType(), True),
    ])

    title_ratings_schema = StructType([
        StructField('tconst', StringType(), False),
        StructField('averageRating', FloatType(), True),
        StructField('numVotes', IntegerType(), True),
    ])
