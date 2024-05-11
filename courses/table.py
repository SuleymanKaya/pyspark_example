from pyspark.sql import SparkSession

from logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[3]").appName("SparkSink").enableHiveSupport().getOrCreate()
    logger = Log4j(spark)

    flight_time_parquet_df = spark.read.format("parquet").load("data_source/flight*.parquet")

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    (
        flight_time_parquet_df.write.mode("overwrite")  # .format('csv')
        .bucketBy(5, "OP_CARRIER", "ORIGIN")  # .partitionBy("OP_CARRIER", "ORIGIN")
        .sortBy("OP_CARRIER", "ORIGIN")
        .saveAsTable("flight_data_tbl")  # .saveAsTable("AIRLINE_DB.flight_data_tbl")
     )

    logger.info(str(spark.catalog.listTables("AIRLINE_DB")))
