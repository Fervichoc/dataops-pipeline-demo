from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.getOrCreate()

account_name = "sadatapipeline"
container = "bronze"

url = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"
pdf = pd.read_csv(url)

# 3. Convertir a Spark DataFrame
df = spark.createDataFrame(pdf)
df.show()

output_path = f"wasbs://{container}@{account_name}.blob.core.windows.net/titanic.csv"
print(f"📤 Guardando dataset en Azure Storage: {output_path}")
df.write.mode("overwrite").option("header", True).csv(output_path)

# df = spark.write.csv(output_path, header=True, inferSchema=True)
