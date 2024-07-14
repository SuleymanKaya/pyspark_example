from logger import Log4j
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Initialize
    spark = SparkSession.builder.appName("Bucket Join Demo").master("local[3]").enableHiveSupport().getOrCreate()
    logger = Log4j(spark)

    # Read dataframes
    flight_time_df1 = spark.read.json("data/join_data/d1/")
    flight_time_df2 = spark.read.json("data/join_data/d2/")

    # Bucketing to database
    # spark.sql("CREATE DATABASE IF NOT EXISTS my_db")
    # # spark.catalog.setCurrentDatabase("my_db")
    # spark.sql("USE my_db")

    # # Bu kod satiri, flight_time_df1-2 DataFrame'lerini tek bir bölüme indirir, 
    # # "id" sütununa göre 3 kovaya bölünmüş halde yazilmasini sağlar 
    # # ve "overwrite" modunda "my_db" veritabaninda "flight_data1-2" isimli bir Hive tablolari olarak kaydeder.
    # flight_time_df1.coalesce(1).write.bucketBy(3, "id").mode("overwrite").saveAsTable("my_db.flight_data1")
    # flight_time_df2.coalesce(1).write.bucketBy(3, "id").mode("overwrite").saveAsTable("my_db.flight_data2")

    # List all databases
    spark.sql("SHOW DATABASES").show()
    # List all tables
    spark.sql("SHOW TABLES IN my_db").show()

    # Sort-Merge Join
    df3 = spark.read.table("my_db.flight_data1")
    df4 = spark.read.table("my_db.flight_data2")

    # To prevent to not use broadcast join in default
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    # Do Sort-Merge Join
    join_expr = df3.id == df4.id
    join_df = df3.join(other=df4, on=join_expr, how='inner')
    join_df.collect()
    input("press a key to stop...")
    spark.stop()