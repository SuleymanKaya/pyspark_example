from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

from logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[3]").appName("SparkSink").getOrCreate()
    logger = Log4j(spark)

    flight_time_parquet_df = spark.read.format("parquet").load("data/data_source/flight*.parquet")

    logger.info("Num Partitions before: " + str(flight_time_parquet_df.rdd.getNumPartitions()))
    flight_time_parquet_df.groupBy(spark_partition_id()).count().show()

    partitioned_df = flight_time_parquet_df.repartition(5)
    logger.info("Num Partitions after: " + str(partitioned_df.rdd.getNumPartitions()))
    partitioned_df.groupBy(spark_partition_id()).count().show()

    partitioned_df.write.format("avro").mode("overwrite").option("path", "data/data_sink/avro/").save()
    (
        partitioned_df.write.format("json").mode("overwrite").option("path", "data/data_sink/json/")
        .partitionBy("OP_CARRIER", "ORIGIN").option("maxRecordsPerFile", 10000).save()
    )
