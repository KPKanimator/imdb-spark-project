from pyspark.sql import SparkSession
from helper.IMDBHelper import IMDBHelper as hp
from schemas.IMDBService import IMDBService as im
import pyspark.sql.functions as f


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


if __name__ == "__main__":
    print("Application Started ...")
    spark = SparkSession \
        .builder \
        .appName("IMDB Py Spark") \
        .master("local") \
        .config("spark.logConf", "true") \
        .getOrCreate()

    # https://stackoverflow.com/questions/40163996/how-to-save-a-dataframe-as-compressed-gzipped-csv
    # print(task1(spark).show())
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

    print("Application Completed.")
