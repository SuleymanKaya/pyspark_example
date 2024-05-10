from pyspark.sql import SparkSession

from logger import Log4j
from utils import get_spark_app_config

conf = get_spark_app_config()

spark = SparkSession.builder.config(conf=conf).getOrCreate()
logger = Log4j(spark)

logger.info("Starting HelloSpark")

# Example1:
conf_output = spark.sparkContext.getConf()
logger.info(conf_output.toDebugString())

# Example2:
survey_df = spark.read.csv(path="data/surveys.csv", header=True, inferSchema=True)
partitioned_survey_df = survey_df.repartition(2)
filtered_df = partitioned_survey_df.where("Age < 40")
selected_df = filtered_df.select("Age", "Gender", "Country", "state")
grouped_df = selected_df.groupBy("Country").count()
# grouped_df.show()
logger.info(grouped_df.collect())
input("Press Enter to continue...")

logger.info("Stopping HelloSpark")

spark.stop()
