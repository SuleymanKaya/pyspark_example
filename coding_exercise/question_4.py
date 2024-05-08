# QUESTION 4:
# 1) USING PySpark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RemoveDuplicates").getOrCreate()

data = [
    ("Ronak", 21, "London"),
    ("Atul", 28, "California"),
    ("Sam", 26, "Delhi"),
    ("Rakesh", 21, "Manchester"),
    ("Yash", 29, "Mumbai")
]
columns = ["Name", "Age", "Location"]

df = spark.createDataFrame(data, schema=columns)
df_unique = df.dropDuplicates(["Name", "Age"])
rows = df_unique.collect()
data_dict = [row.asDict() for row in rows]

print("With Spark SQL: ", data_dict)

spark.stop()

"""--------------------------------------------------------------------------------"""

# 2) Using Pandas
import pandas as pd

data = {
    "Name": ["Ronak", "Atul", "Sam", "Rakesh", "Yash"],
    "Age": [21, 28, 26, 21, 29],
    "Location": ["London", "California", "Delhi", "Manchester", "Mumbai"]
}
df = pd.DataFrame(data)

df_unique = df.drop_duplicates(subset=["Name", "Age"])
data_dict = df_unique.to_dict(orient="records")

print("Without Spark SQL (Pandas): ", data_dict)
