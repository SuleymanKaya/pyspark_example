import configparser
import os

from pyspark import SparkConf


def get_spark_app_config():
    config = configparser.ConfigParser()
    base_dir = os.path.dirname(os.path.abspath(__file__))
    conf_file = os.path.join(base_dir, "spark.conf")
    config.read(conf_file)

    spark_conf = SparkConf()
    for key, val in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf
