from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("Application Started ...")

    spark = SparkSession \
        .builder \
        .appName("First Py Spark Demo") \
        .master("local") \
        .config("spark.logConf", "true") \
        .getOrCreate()

    # spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLogLevel("ERROR")

    # sc = spark.sparkContext
    # sc.setLogLevel("INFO")

    input_file_path = "file:///D://Datasets//sample_data//teach.txt"

    tech_rdd = spark.sparkContext.textFile(input_file_path)

    print("Printing data in the tech_rdd ")
    print(tech_rdd.collect())

    print("Application Completed.")