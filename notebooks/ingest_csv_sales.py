from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

account_name = "sadatapipeline"
container = "bronze"

input_path = f"wasbs://{container}@{account_name}.blob.core.windows.net/sales.csv"

df = spark.read.csv(input_path, header=True, inferSchema=True)
df.show(5)
