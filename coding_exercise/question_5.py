# QUESTION 5:
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

# 1) Generate Random CSV Data
np.random.seed(0)  # For reproducibility
df = pd.DataFrame({
    'Column1': np.random.randint(1, 100, size=100),
    'Column2': np.random.choice(['A', 'B', 'C', 'D'], size=100),
    'Column3': np.random.randn(100)
})

csv_file_path = 'random_data.csv'
df.to_csv(csv_file_path, index=False)

print(f'CSV file created at {csv_file_path}')

# 2) Read CSV and Save as Parquet Using PySpark

spark = SparkSession.builder.appName("CSVtoParquet").getOrCreate()

df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

df_repartitioned = df.repartition(10)
parquet_output_dir = 'parquet_output'
df_repartitioned.write.mode('overwrite').parquet(parquet_output_dir)

print(f'Parquet files are stored in {parquet_output_dir}')

spark.stop()

