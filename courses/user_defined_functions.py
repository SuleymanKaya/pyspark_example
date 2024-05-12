import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, expr
from pyspark.sql.types import StringType

from logger import Log4j


def parse_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"
    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"


if __name__ == "__main__":
    spark = SparkSession.builder.master("local[3]").appName("UDF").getOrCreate()
    logger = Log4j(spark)

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data_source/surveys.csv")
    survey_df.show(10)

    # Dataframe based
    parse_gender_udf = udf(parse_gender, returnType=StringType())
    logger.info("Catalog Entry:")
    [(logger.info(item)) for item in spark.catalog.listFunctions() if "parse_gender" in item.name]
    survey_df2 = survey_df.withColumn("Gender", parse_gender_udf("Gender"))
    survey_df2.show(10)

    # SQL Expression based
    spark.udf.register("parse_gender_udf", parse_gender, returnType=StringType())
    logger.info("Catalog Entry:")
    [(logger.info(item)) for item in spark.catalog.listFunctions() if "parse_gender" in item.name]
    survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
    survey_df3.show(10)
