#Transforming Titanic Dataset and uploading to silver
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

account_name = "sadatapipeline"
container = "bronze"

input_path = f"wasbs://{container}@{account_name}.blob.core.windows.net/titanic.csv"

df = spark.read.csv(input_path, header=True, inferSchema=True)

df_split_name = df.withColumn("LastName", trim(split(col("Name"), ",")[0])) \
             .withColumn("FirstName", trim(split(col("Name"), ",")[1])) \
            .drop("Name")

df_ticket_split = df_split_name.withColumn("TicketPrefix", trim(regexp_extract(col("Ticket"), r"^([A-Za-z./]+)", 1))) \
              .withColumn("TicketNumber", trim(regexp_extract(col("Ticket"), r"(\d+)$", 1)))

df_clean = df_ticket_split.na.drop(subset=["PassengerId", "Age"])

df_survived = df_clean.filter(col("Survived") == 1)
df_died = df_clean.filter(col("Survived") == 0)

account_name = "sadatapipeline"
container = "silver"

output_path_survived = f"wasbs://{container}@{account_name}.blob.core.windows.net/titanic_clean_survived"
output_path_dead = f"wasbs://{container}@{account_name}.blob.core.windows.net/titanic_clean_dead"


# Writing the dataframe to Azure Blob Storage
df_died.write.format("delta").mode("overwrite").save(output_path_dead)
df_survived.write.format("delta").mode("overwrite").save(output_path_survived)