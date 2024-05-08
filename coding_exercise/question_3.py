# QUESTION 3:
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create sample data
data = [
    "Name,Age,Gender,Occupation,Hobby",
    "Michael,x48,Male,Sales, “Loves to play guitar\, especially when it's \nraining outside.”",
    "JRobert,35,Male,Sales, “Enjoys hiking and camping in the mountains. \nHer favourite season is fall.”",
    "Jane,28,Female, Marketing, “Enjoys hiking and camping in the mountains. Her favourite season is fall.”,Married",
    "Bob,42,Male, Finance, “Likes to read\, watch movies\, \nand spend time with his family.”"
]

# Save to a CSV file
with open('sample_data.csv', 'w', newline='') as file:
    for line in data:
        file.write(line.replace('\n', '\\n').replace('\\,', '') + '\n')

# Start Spark session
spark = SparkSession.builder.appName("Data Quality Check").getOrCreate()

# Define schema
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Gender", StringType(), True),
    StructField("Occupation", StringType(), True),
    StructField("Hobby", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

# Read CSV file
df = spark.read.csv("sample_data.csv", header=True, schema=schema, mode='PERMISSIVE')

# Show the DataFrame
df.show(truncate=False)

# _corrupt_record sütunu null olmayan kayıtları filtreleme
corrupt_df = df.filter(col("_corrupt_record").isNotNull())

# Show the Corrupt DataFrame
corrupt_df.show(truncate=False)

# Stop SparkSession
spark.stop()

# Optionally, you can delete the temporary file if not needed
os.remove("sample_data.csv")

"""
mode parameter
PERMISSIVE: Bu varsayılan moddur. Hatalı verileri _corrupt_record adında bir sütunda toplar. Kayıt hatalı veri içeriyorsa, bu verileri bu sütunda saklar ve işleme devam eder. Bu mod, veriler üzerinde maksimum esneklik sağlar.
DROPMALFORMED: Bu modda, şemaya uymayan satırlar dosyadan tamamen atılır. Yani, hatalı verileri içeren herhangi bir satır okunmaz ve sonuç veri çerçevesinden çıkarılır. Bu, daha temiz veri setleri elde etmek istediğinizde kullanışlı olabilir.
FAILFAST: Bu mod, herhangi bir hatalı veriyle karşılaşıldığında işlemi durdurur ve bir hata fırlatır. Bu, veri kalitesine çok önem verdiğiniz ve hatalı verilerin işlenmesini kesinlikle istemediğiniz durumlar için uygun bir seçenektir.
"""