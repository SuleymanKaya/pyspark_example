from logger import Log4j
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

if __name__ == "__main__":
    # Initialize
    spark = SparkSession.builder.master("local[3]").appName("joins").getOrCreate()
    logger = Log4j(spark)

    # Creating dataframes
    orders_list = [("01", "02", 350, 1),
                   ("01", "04", 580, 1),
                   ("01", "07", 320, 2),
                   ("02", "03", 450, 1),
                   ("02", "06", 220, 1),
                   ("03", "01", 195, 1),
                   ("04", "09", 270, 3),
                   ("04", "08", 410, 2),
                   ("05", "02", 350, 1)]
    order_df = spark.createDataFrame(orders_list).toDF("order_id", "prod_id", "unit_price", "qty")

    product_list = [("01", "Scroll Mouse", 250, 20),
                    ("02", "Optical Mouse", 350, 20),
                    ("03", "Wireless Mouse", 450, 50),
                    ("04", "Wireless Keyboard", 580, 50),
                    ("05", "Standard Keyboard", 360, 10),
                    ("06", "16 GB Flash Storage", 240, 100),
                    ("07", "32 GB Flash Storage", 320, 50),
                    ("08", "64 GB Flash Storage", 430, 25)]
    product_df = spark.createDataFrame(product_list).toDF("prod_id", "prod_name", "list_price", "qty")

    # Preparing for joins
    join_expr = order_df.prod_id == product_df.prod_id
    product_renamed_df = product_df.withColumnRenamed("qty", "reorder_qty")

    # INNER JOIN
    print("----------------Inner Join----------------")
    order_df.join(other=product_renamed_df, on=join_expr, how="inner") \
        .drop(product_renamed_df.prod_id) \
        .select("order_id", "prod_id", "prod_name", "unit_price", "list_price", "qty") \
        .sort("order_id") \
        .show()
    
    # OUTER JOINS
    # Full Outer Join
    print("----------------Full Outer Join----------------")
    order_df.join(other=product_renamed_df, on=join_expr, how="outer") \
        .drop(product_renamed_df.prod_id) \
        .select("order_id", "prod_id", "prod_name", "unit_price", "list_price", "qty") \
        .sort("order_id") \
        .show()
    
    # Left Outer Join
    print("----------------Left Outer Join----------------")
    order_df.join(other=product_renamed_df, on=join_expr, how="left") \
        .drop(product_renamed_df.prod_id) \
        .select("order_id", "prod_id", "prod_name", "unit_price", "list_price", "qty") \
        .withColumn("prod_name", expr("coalesce(prod_name, prod_id)")) \
        .withColumn("list_price", expr("coalesce(list_price, unit_price)")) \
        .sort("order_id") \
        .show()
    
    # Right Outer Join
    print("----------------Right Outer Join----------------")
    order_df.join(other=product_renamed_df, on=join_expr, how="right") \
        .drop(order_df.prod_id) \
        .select("order_id", "prod_id", "prod_name", "unit_price", "list_price", "qty") \
        .withColumn("unit_price", expr("coalesce(unit_price, list_price)")) \
        .withColumn("qty", expr("coalesce(qty, 0)")) \
        .sort("order_id") \
        .show()
