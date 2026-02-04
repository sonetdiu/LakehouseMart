from pyspark.sql.functions import current_timestamp

# ----- CONFIG -----
catalog_name = "lakehouse"
bronze_schema = "bronze"

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE {bronze_schema}")

# Raw input paths (pointing to your external location URL)
raw_base_path = "s3://my-lakehouse-bucket/raw"  # or abfss://...
customers_raw_path = f"{raw_base_path}/customers"
products_raw_path  = f"{raw_base_path}/products"
orders_raw_path    = f"{raw_base_path}/orders"

# Auto Loader schema locations (also in cloud storage)
schema_base_path = "s3://my-lakehouse-bucket/autoloader-schema"
customers_schema_path = f"{schema_base_path}/customers"
products_schema_path  = f"{schema_base_path}/products"
orders_schema_path    = f"{schema_base_path}/orders"

# ----- CUSTOMERS BRONZE -----
customers_bronze_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", customers_schema_path)
        .load(customers_raw_path)
        .withColumn("ingest_ts", current_timestamp())
)

(
    customers_bronze_df
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"{schema_base_path}/_checkpoints/customers_bronze")
        .outputMode("append")
        .toTable("lakehouse.bronze.customers_bronze")
)

# ----- PRODUCTS BRONZE -----
products_bronze_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", products_schema_path)
        .load(products_raw_path)
        .withColumn("ingest_ts", current_timestamp())
)

(
    products_bronze_df
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"{schema_base_path}/_checkpoints/products_bronze")
        .outputMode("append")
        .toTable("lakehouse.bronze.products_bronze")
)

# ----- ORDERS BRONZE -----
orders_bronze_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", orders_schema_path)
        .load(orders_raw_path)
        .withColumn("ingest_ts", current_timestamp())
)

(
    orders_bronze_df
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"{schema_base_path}/_checkpoints/orders_bronze")
        .outputMode("append")
        .toTable("lakehouse.bronze.orders_bronze")
)
