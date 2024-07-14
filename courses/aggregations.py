from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, avg, countDistinct, round, expr, to_date, col, weekofyear

from logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[3]").appName("aggregations").getOrCreate()
    logger = Log4j(spark)

    invoice_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/data_source/invoices.csv")

    # Select based for single row
    invoice_df.select(count("*").alias("Count *"),
                      sum("Quantity").alias("TotalQuantity"),
                      round(avg("UnitPrice"), 2).alias("AvgPrice"),
                      countDistinct("InvoiceNo").alias("CountDistinct")
                      ).show()

    # SelectExpr based for single row
    invoice_df.selectExpr(
        "count(1) as `count 1`",
        "sum(Quantity) as TotalQuantity",
        "round(avg(UnitPrice), 2) as AvgPrice",
        "count(StockCode) as `count field`"
    ).show()

    # SQL based grouping
    sql_expression = """
              SELECT Country, InvoiceNo,
                    sum(Quantity) as TotalQuantity,
                    round(sum(Quantity*UnitPrice), 2) as InvoiceValue
              FROM sales_view
              GROUP BY Country, InvoiceNo
              """
    invoice_df.createOrReplaceTempView("sales_view")
    summary_sql = spark.sql(sqlQuery=sql_expression)
    summary_sql.show()

    # Dataframe API based
    quantity_agg = sum("Quantity").alias("TotalQuantity")
    invoice_value_agg = expr("round(sum(Quantity * UnitPrice), 2) as InvoiceValueExpr")
    invoice_value_agg2 = round(sum(expr("Quantity * UnitPrice")), 2).alias("InvoiceValue")
    summary_df = invoice_df.groupBy("Country", "InvoiceNo").agg(
        quantity_agg,
        invoice_value_agg,
        invoice_value_agg2
    )
    summary_df.show()

    # Dataframe API based-2
    num_invoices_agg = countDistinct("InvoiceNo").alias("NumInvoices")
    ex_summary_df = invoice_df \
        .withColumn("InvoiceDate", to_date(col("InvoiceDate"), "dd-MM-yyyy H.mm")) \
        .where("year(InvoiceDate) == 2010") \
        .withColumn("WeekNumber", weekofyear(col("InvoiceDate"))) \
        .groupBy("Country", "WeekNumber") \
        .agg(num_invoices_agg, quantity_agg, invoice_value_agg)

    ex_summary_df.coalesce(1).write.format("parquet").mode("overwrite").save("data/data_sink/aggregations/")
    ex_summary_df.sort("Country", "WeekNumber").show()

    spark.stop()
