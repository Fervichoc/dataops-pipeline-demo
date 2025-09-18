eh_namespace = "<>.servicebus.windows.net:9093"
eh_sasl = "org.apache.kafka.common.security.plain.PlainLoginModule required username='$ConnectionString' password='<>';"

df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", eh_namespace)
    .option("subscribe", "eventhub")  # el nombre del Event Hub
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", eh_sasl)
    .load()
)

df_parsed = df_kafka.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
