from pyspark.sql.types import StringType
from pyspark.sql.functions import col

connectionString = "Endpoint=sb://<NAMESPACE>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<KEY>;EntityPath=<EVENTHUB_NAME>"

ehConf = {
    'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
}

# Lectura desde Event Hubs
df = (
    spark.readStream
    .format("eventhubs")
    .options(**ehConf)
    .load()
)

# Cuerpo del mensaje viene como binario -> convertir a string
messages = df.select(col("body").cast(StringType()).alias("message"))

display(messages)  # en Databricks puedes ver en tiempo real
