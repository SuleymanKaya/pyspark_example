from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, substring_index

from logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[3]").appName("UnstructuredLogFile").getOrCreate()
    logger = Log4j(spark)

    file_df = spark.read.text("data/data_unstructured/apache_logs.txt")
    file_df.printSchema()

    log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'
    logs_df = file_df.select(regexp_extract('value', log_reg, 1).alias('ip'),
                             regexp_extract('value', log_reg, 4).alias('date'),
                             regexp_extract('value', log_reg, 6).alias('request'),
                             regexp_extract('value', log_reg, 10).alias('referrer'))
    logs_df.printSchema()

    (logs_df.where("referrer LIKE 'http%'")
     .withColumn("referrer", substring_index("referrer", "/", 3))
     .groupBy("referrer").count()
     .orderBy('count', ascending=False)
     .show(100, truncate=False))
