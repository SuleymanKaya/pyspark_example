from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import sum

from logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[3]").appName("windowing_agg").getOrCreate()
    logger = Log4j(spark)

    summary_df = spark.read.parquet("data_source/summary.parquet")

    running_total_window = Window.partitionBy("Country").orderBy("WeekNumber").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    summary_df.withColumn("RunningTotal", sum("InvoiceValue").over(window=running_total_window)).show()

    spark.stop()
