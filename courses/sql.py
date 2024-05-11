from pyspark import SparkConf
from pyspark.sql import SparkSession

from logger import Log4j

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[3]").setAppName("HelloSparkSQL")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger = Log4j(spark)

    survey_df = (
        spark.read.
        option("header", "true").
        option("inferSchema", "true")
        .csv(path="data_source/surveys.csv")
    )
    survey_df.createOrReplaceTempView("survey_view")
    # survey_df2 = survey_df.toPandas() # for debugging
    sql_statement = "SELECT Country, count(*) AS count FROM survey_view where Age<40 GROUP BY Country"
    count_df = spark.sql(sqlQuery=sql_statement)
    count_df.show()
