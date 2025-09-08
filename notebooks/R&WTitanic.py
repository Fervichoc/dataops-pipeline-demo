from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.getOrCreate()

# Defining container variables (Key definied in the cluster configuration)
account_name = "sadatapipeline"
container = "bronze"

# Reading Titanic dataset from a URL
url = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"
pdf = pd.read_csv(url)

# Converting to spark dataframe
df = spark.createDataFrame(pdf)
df.show()

output_path = f"wasbs://{container}@{account_name}.blob.core.windows.net/titanic.csv"

# Writing the dataframe to Azure Blob Storage
df.write.mode("overwrite").option("header", True).csv(output_path)
