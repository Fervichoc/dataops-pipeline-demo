from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import *
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

# Defining container variables (Key definied in the cluster configuration)
account_name = "sadatapipeline"
container = "bronze"

input_path = f"wasbs://{container}@{account_name}.blob.core.windows.net/iris"

df = spark.read.format("delta").load(input_path)

delta_table = DeltaTable.forPath(spark, input_path)

delta_table.toDF().show(5)

df_old = spark.read.format("delta").option("versionAsOf", 0).load(input_path)

delta_table.update(
    condition="sepal_length < 0.5",
    set={"sepal_length": "NULL"}
)

delta_table.delete("sepal_length IS NULL")

new_data = [
    (5.1, 3.5, 1.4, 0.2, "setosa"),
    (6.5, 3.0, 5.2, 2.0, "virginica")
]
columns = ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]
df_new = spark.createDataFrame(new_data, columns)

delta_table.alias("t").merge(
    df_new.alias("s"),
    "t.sepal_length = s.sepal_length AND t.sepal_width = s.sepal_width"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

delta_table.toDF().filter((col("species") == "setosa") & (col("sepal_length") == "5.1")).show(5)

spark.sql("""
OPTIMIZE delta.`wasbs://bronze@sadatapipeline.blob.core.windows.net/iris`
""")