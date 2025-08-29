# Databricks notebook source
from pyspark.sql import SparkSession

# Spark session
spark = SparkSession.builder.getOrCreate()

# Cargar CSV dummy (dataset público de Databricks)
df = spark.read.csv("/databricks-datasets/retail-org/sales_orders/", header=True, inferSchema=True)

# Mostrar esquema
df.printSchema()

# Guardar en Delta Lake (bronze)
df.write.format("delta").mode("overwrite").save("/mnt/delta/bronze/sales_orders")
