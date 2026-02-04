# Databricks notebook source
# 01_bronze_auto_loader_ingestion.py

from pyspark.sql.functions import current_timestamp, input_file_name

# ---------- CONFIG ----------
raw_base_path = "/mnt/lakehousemart/raw"
bronze_base_path = "/mnt/lakehousemart/bronze"
checkpoint_base_path = "/mnt/lakehousemart/checkpoints/bronze"

# Utility to start an Auto Loader stream
def autoloader_ingest(entity: str, file_format: str = "json"):
    raw_path = f"{raw_base_path}/{entity}"
    target_path = f"{bronze_base_path}/{entity}"
    checkpoint_path = f"{checkpoint_base_path}/{entity}"

    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.inferColumnTypes", "true")
        .load(raw_path)
        .withColumn("ingest_ts", current_timestamp())
        .withColumn("source_file", input_file_name())
    )

    (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .start(target_path)
    )

# ---------- START STREAMS ----------
autoloader_ingest("customers", "json")
autoloader_ingest("products", "json")
autoloader_ingest("orders", "json")

# After a while, register tables (can also be done in SQL)
spark.sql("""
CREATE TABLE IF NOT EXISTS lakehousemart.bronze.customers
USING DELTA
LOCATION '/mnt/lakehousemart/bronze/customers'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehousemart.bronze.products
USING DELTA
LOCATION '/mnt/lakehousemart/bronze/products'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehousemart.bronze.orders
USING DELTA
LOCATION '/mnt/lakehousemart/bronze/orders'
""")
