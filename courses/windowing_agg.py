from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import sum, col, dense_rank

from logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[3]").appName("windowing_agg").getOrCreate()
    logger = Log4j(spark)

    summary_df = spark.read.parquet("data/data_source/summary.parquet")

    # Sum of values with window
    running_total_window = Window.partitionBy("Country").orderBy("WeekNumber").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    summary_df.withColumn("RunningTotal", sum("InvoiceValue").over(window=running_total_window)).show()

    # Rank with window
    rank_window = Window.partitionBy("Country").orderBy(col("InvoiceValue").desc()).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    summary_df.withColumn("Rank", dense_rank().over(rank_window)).sort("Country", "WeekNumber").show()

    spark.stop()
