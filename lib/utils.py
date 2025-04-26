import configparser

from pyspark import SparkConf

def load_spark_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")


    for key, value in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, value)
    return spark_conf

def load_data_file(spark, data_file):
    data_df = spark.read.csv(data_file, header=True, inferSchema=True)
    return data_df

def count_by_country(df):
    changed_df = df.where("age < 40") \
                    .select("Age", "Gender", "Country", "state") \
                    .groupBy("Country") \
                    .count()
    return changed_df