from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import *


spark = SparkSession.builder.getOrCreate()

# Defining container variables (Key definied in the cluster configuration)
account_name = "sadatapipeline"
container = "bronze"

url = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv"
pdf = pd.read_csv(url)

df = spark.createDataFrame(pdf)
output_path = f"wasbs://{container}@{account_name}.blob.core.windows.net/iris"


df = df.filter(col("species") != "setosa")

df.show(5)

df.write.format("delta").mode("overwrite").save(output_path)