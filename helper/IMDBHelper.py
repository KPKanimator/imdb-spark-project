"""
IMDBHelper.py
~~~~~~~~

Module containing helper function for utility funcs
"""
from os import environ, listdir, path
import json


class IMDBHelper(object):
    @staticmethod
    def read_tsv_file_load_as_df(spark, data_file_path, schema):
        data_frame = spark.read.csv(data_file_path, sep=r'\t', header=True, schema=schema)
        return data_frame

    @staticmethod
    def load_configs_from_local():
        conf_files_dir = "./configs/"
        config_files = [filename
                        for filename in listdir(conf_files_dir)
                        if filename.endswith('config.json')]

        if config_files:
            path_to_config_file = path.join(conf_files_dir, config_files[0])
            with open(path_to_config_file, 'r') as config_file:
                config_dict = json.load(config_file)
        else:
            config_dict = None

        return config_dict

    @classmethod
    def import_imdb_akas_data(cls, spark, config, schema):
        """
        reading title.akas.tsv from local path and repartition to 200
        """
        # gh.read_tsv_file_load_as_df(spark, config['titleAkasPath'], cls.title_akas_schema)
        tmp_title_akas_data_frame = \
            spark.read.csv(config['titleAkasPath'], sep=r'\t', header=True, schema=schema)
        title_akas_data_frame = tmp_title_akas_data_frame.repartition(200)

        return title_akas_data_frame

    # from pyspark import SparkConf
    # from pyspark.sql import SparkSession, dataframe, Window
    # import pyspark.sql.types as t
    # import pyspark.sql.functions as f
    # import schemas as s
    # import read_write as rw
    #
    # def task1(spark_session):
    #     path = './data/title.akas.tsv.gz'
    #     schema1 = s.schema_title_akas()
    #     tabl1_df = rw.reading(spark_session, path, schema1)
    #
    #     tabl1_df = tabl1_df.withColumn('language',
    #                                    f.when(f.col('language').isin(r'\N', None), None).otherwise(f.col('language')))
    #     task1_df = tabl1_df.select('title', 'language').filter(f.col('language') == 'uk')
    #
    #     return task1_df

