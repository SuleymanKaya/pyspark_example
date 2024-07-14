from logger import Log4j
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

if __name__ == "__main__":
    # Initialize
    spark = SparkSession.builder.appName("Shuffle and Broadcast Join Demo").master("local[3]").getOrCreate()
    logger = Log4j(spark)

    # Read dataframes
    flight_time_df1 = spark.read.json("data/join_data/d1/")
    flight_time_df2 = spark.read.json("data/join_data/d2/")

    spark.conf.set("spark.sql.shuffle.partitions", 3)
    join_expr = flight_time_df1.id == flight_time_df2.id

    # Shuffle Joins (for large to large dataframes)
    join_df = flight_time_df1.join(flight_time_df2, on=join_expr, how="inner")
    join_df = join_df.limit(10000).toPandas()

    # Broadcast Joins (for large to small dataframes)
    broadcast_join_df = flight_time_df1.join(broadcast(flight_time_df2), on=join_expr, how="inner")
    broadcast_join_df = broadcast_join_df.limit(10000).toPandas()
    
    input("press a key to stop...")