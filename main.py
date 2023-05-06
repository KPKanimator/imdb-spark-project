import pyspark.sql.types as t
from pyspark.sql import SparkSession, Window as w
from helper.IMDBHelper import IMDBHelper as hp
from schemas.IMDBService import IMDBService as im
import pyspark.sql.functions as f
from pyspark.sql.functions import col


def task1(spark_session):
    """
    Transformation Stage Task 1
    Get all titles of series/movies etc. that are available in Ukrainian.
    :param spark_session: SparkSession object
    :return: dataframe
    """
    config = hp.load_configs_from_local()
    scheme = im.title_akas_schema
    df = hp.read_tsv_file_load_as_df(spark_session, config['titleAkasPath'], scheme)
    df = df.withColumn('language', f.when(f.col('language').isin(r'\N', None), None).otherwise(f.col('language')))
    df = df.select('title', 'language').filter(f.col('language') == 'uk')

    return df


def task2(spark_session):
    """
    Transformation Stage Task 2
    Get the list of people's names, who were born in the 19th century.
    :param spark_session: SparkSession object
    :return: dataframe
    """
    config = hp.load_configs_from_local()
    scheme = im.name_basics_schema
    df = hp.read_tsv_file_load_as_df(spark_session, config['nameBasicsPath'], scheme)
    df = df.select('primaryName', 'birthYear').filter(f.col('birthYear') < 1901)

    return df


def task3(spark_session):
    """
    Transformation Stage Task 3
    Get titles of all movies that last more than 2 hours.
    :param spark_session: SparkSession object
    :return: dataframe
    """
    config = hp.load_configs_from_local()
    scheme = im.title_basic_schema
    df = hp.read_tsv_file_load_as_df(spark_session, config['titleBasicsPath'], scheme)
    df = df.select('primaryTitle', 'runtimeMinutes').filter(f.col('runtimeMinutes') > 120)
    # df.show(truncate=False)

    return df


def task4(spark_session):
    """
    Transformation Stage Task 4
    Get names of people, corresponding movies/series and characters they played in those films.
    :param spark_session: SparkSession object
    :return: dataframe
    """
    config = hp.load_configs_from_local()
    df_title_basics = hp.read_tsv_file_load_as_df(spark_session, config['titleBasicsPath'], im.title_basic_schema)
    df_name_basics = hp.read_tsv_file_load_as_df(spark_session, config['nameBasicsPath'], im.name_basics_schema)
    df_title_principals = \
        hp.read_tsv_file_load_as_df(spark_session, config['titlePrincipalsPath'], im.title_principals_schema)
    df_title_principals = df_title_principals.withColumn('characters',
                                                         f.when(f.col('characters').isin(r'\N', None), None).otherwise(
                                                             f.col('characters')))
    df = df_title_principals.join(df_name_basics, df_title_principals.nconst == df_name_basics.nconst, "inner") \
        .join(df_title_basics, df_title_principals.tconst == df_title_basics.tconst, "inner") \
        .select(df_name_basics.primaryName, df_title_basics.primaryTitle, df_title_principals.characters).filter(
        f.col('characters') != 'null')

    return df


def task5(spark_session):
    """
    Transformation Stage Task 5
    Get information about how many adult movies/series etc. there are per
    region. Get the top 100 of them from the region with the biggest count to
    the region with the smallest one.
    :param spark_session: SparkSession object
    :return: dataframe
    """
    config = hp.load_configs_from_local()
    df_title_akas = hp.read_tsv_file_load_as_df(spark_session, config['titleAkasPath'], im.title_akas_schema)
    df_title_akas = df_title_akas.withColumn('region', f.when(f.col('region').isin(r'\N', None), None) \
                                             .otherwise(f.col('region')))
    df_title_basics = hp.read_tsv_file_load_as_df(spark_session, config['titleBasicsPath'], im.title_basic_schema)
    df = df_title_akas.join(df_title_basics, df_title_akas.titleId == df_title_basics.tconst, "inner") \
        .select(df_title_basics.primaryTitle, df_title_akas.region, df_title_basics.isAdult) \
        .filter((f.col('isAdult') == 1) & (f.col('region') != 'null'))
    df = df.groupBy('region').count().orderBy('count', ascending=False).limit(100)
    df.show(20)

    return df


def task6(spark_session):
    """
    Transformation Stage Task 6
    Get information about how many episodes in each TV Series. Get the top
    50 of them starting from the TV Series with the biggest quantity of
    episodes.
    :param spark_session: SparkSession object
    :return: dataframe
    """
    config = hp.load_configs_from_local()
    df_title_episode = hp.read_tsv_file_load_as_df(spark_session, config['titleEpisodePath'], im.title_episode_schema)
    df_title_basics = hp.read_tsv_file_load_as_df(spark_session, config['titleBasicsPath'], im.title_basic_schema)

    df = df_title_basics.join(df_title_episode, df_title_basics.tconst == df_title_episode.parentTconst, "inner") \
        .select(df_title_basics.primaryTitle, df_title_episode.episodeNumber) \
        .filter(f.col('episodeNumber') > 0)
    df = df.groupBy('primaryTitle').agg({'episodeNumber': 'count'}).orderBy('count(episodeNumber)', ascending=False)
    df = df.limit(50)
    # df.show()

    return df


def task7(spark_session):
    """
    Transformation Stage Task 7
    Get 10 titles of the most popular movies/series etc. by each decade.
    :param spark_session: SparkSession object
    :return: dataframe
    """
    config = hp.load_configs_from_local()
    df_title_basics = hp.read_tsv_file_load_as_df(spark_session, config['titleBasicsPath'], im.title_basic_schema)
    df_title_ratings = hp.read_tsv_file_load_as_df(spark_session, config['titleRatingsPath'], im.title_ratings_schema)
    df = df_title_ratings.join(df_title_basics, on='tconst').select('primaryTitle', 'startYear', 'averageRating')

    df = df.withColumn('centure', f.col('startYear').substr(startPos=1, length=2))
    df = df.withColumn('dec', f.col('startYear').substr(startPos=3, length=1))
    df = df.withColumn('centure', df['centure'].cast(t.IntegerType()))
    df = df.withColumn('dec', df['dec'].cast(t.IntegerType()))
    df = df.withColumn('decade_number', f.col('centure')*10+f.col('dec'))
    decade_df = df.select('primaryTitle', 'startYear', 'averageRating', 'decade_number').filter(f.col('decade_number') > 0)

    window = w.orderBy(f.col('averageRating').desc()).partitionBy('decade_number')
    df = decade_df.withColumn('top', f.row_number().over(window))
    df = df.filter(f.col('top') < 11)

    df = df.withColumn('years_int', f.col('decade_number')*10)
    df = df.withColumn('years_str', df['years_int'].cast(t.StringType()))
    df = df.withColumn('str_const', f.lit('-s'))
    df = df.withColumn('str_decade', f.concat(f.col('years_str'), f.col('str_const')))

    df = df.select('str_decade', 'primaryTitle', 'top')

    return df


def task8(spark_session):
    """
    Transformation Stage Task 8
    Get 10 titles of the most popular movies/series etc. by each genre.
    :param spark_session: SparkSession object
    :return: dataframe
    """
    config = hp.load_configs_from_local()
    df_title_basics = hp.read_tsv_file_load_as_df(spark_session, config['titleBasicsPath'], im.title_basic_schema)
    # col_names_title_basics = [col.lower().replace(' ', '_') for col in df_title_basics.columns]
    df_title_ratings = hp.read_tsv_file_load_as_df(spark_session, config['titleRatingsPath'], im.title_ratings_schema)
    df = df_title_basics.join(df_title_ratings, on='tconst').select('primaryTitle', 'genres', 'averageRating')
    # agg_df = df.groupBy('genres').agg({'*': 'count'}).show(10)
    window = w.orderBy(col('averageRating').desc()).partitionBy('genres', 'averageRating')
    agg_df = df.withColumn('ng', f.row_number().over(window))
    agg_df = agg_df.select('genres', 'primaryTitle', 'ng').filter(f.col("ng") < 11)

    return agg_df


if __name__ == "__main__":
    print("Application Started ...")
    spark = SparkSession \
        .builder \
        .appName("IMDB Py Spark") \
        .master("local") \
        .config("spark.logConf", "true") \
        .getOrCreate()

    # https://stackoverflow.com/questions/40163996/how-to-save-a-dataframe-as-compressed-gzipped-csv
    task1(spark) \
        .write \
        .mode('overwrite') \
        .format("com.databricks.spark.csv") \
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec") \
        .save("./output/task1")
    task2(spark) \
        .write \
        .mode('overwrite') \
        .format("com.databricks.spark.csv") \
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec") \
        .save("./output/task2")
    task3(spark) \
        .write \
        .mode('overwrite') \
        .format("com.databricks.spark.csv") \
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec") \
        .save("./output/task3")
    task4(spark) \
        .write \
        .mode('overwrite') \
        .format("com.databricks.spark.csv") \
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec") \
        .save("./output/task4")
    task5(spark) \
        .write \
        .mode('overwrite') \
        .format("com.databricks.spark.csv") \
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec") \
        .save("./output/task5")
    task6(spark) \
        .write \
        .mode('overwrite') \
        .format("com.databricks.spark.csv") \
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec") \
        .save("./output/task6")
    task7(spark) \
        .write \
        .mode('overwrite') \
        .format("com.databricks.spark.csv") \
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec") \
        .save("./output/task7")
    task8(spark) \
        .write \
        .mode('overwrite') \
        .format("com.databricks.spark.csv") \
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec") \
        .save("./output/task8")

    print("Application Completed.")

