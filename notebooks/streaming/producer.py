from pyspark.sql.functions import to_json, struct
import time

# DataFrame de prueba
df = spark.createDataFrame(
    [(1, "A"), (2, "B"), (3, "C")],
    ["id", "value"]
)

# Convertir a JSON
json_df = df.select(to_json(struct("id", "value")).alias("body"))

ehConf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
        "Endpoint=sb://<NAMESPACE>.servicebus.windows.net/;SharedAccessKeyName=Send;SharedAccessKey=<KEY>;EntityPath=<EVENTHUB_NAME>"
    )
}

# Enviar al Event Hub como si fuera un batch
(
    json_df
    .write
    .format("eventhubs")
    .options(**ehConf)
    .save()
)
