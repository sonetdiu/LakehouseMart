# Databricks notebook source
# 02_dlt_silver_gold_pipeline.py

import dlt
from pyspark.sql.functions import (
    col, to_timestamp, current_timestamp, input_file_name,
    date_trunc, sum as _sum, countDistinct
)

# ---------- BRONZE (DLT-Managed, Optional) ----------

@dlt.table(
    name="bronze_customers",
    comment="Raw customers ingested from JSON"
)
def bronze_customers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/mnt/lakehousemart/raw/customers")
        .withColumn("ingest_ts", current_timestamp())
        .withColumn("source_file", input_file_name())
    )

@dlt.table(
    name="bronze_products",
    comment="Raw products ingested from JSON"
)
def bronze_products():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/mnt/lakehousemart/raw/products")
        .withColumn("ingest_ts", current_timestamp())
        .withColumn("source_file", input_file_name())
    )

@dlt.table(
    name="bronze_orders",
    comment="Raw orders ingested from JSON"
)
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/mnt/lakehousemart/raw/orders")
        .withColumn("ingest_ts", current_timestamp())
        .withColumn("source_file", input_file_name())
    )

# ---------- SILVER LAYER ----------

@dlt.table(
    name="silver_customers",
    comment="Cleaned customer master data"
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def silver_customers():
    df = dlt.read("bronze_customers")
    return (
        df
        .dropDuplicates(["customer_id"])
        .withColumn("signup_ts", to_timestamp("signup_ts"))
    )

@dlt.table(
    name="silver_products",
    comment="Cleaned product master data"
)
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
def silver_products():
    df = dlt.read("bronze_products")
    return df.dropDuplicates(["product_id"])

@dlt.table(
    name="silver_orders",
    comment="Cleansed and conformed orders"
)
@dlt.expect("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_order_ts", "order_ts IS NOT NULL")
@dlt.expect_or_fail("positive_quantity", "quantity > 0")
def silver_orders():
    df = dlt.read_stream("bronze_orders")

    df_clean = (
        df
        .withColumn("order_ts", to_timestamp(col("order_ts")))
        .withColumn("unit_price", col("unit_price").cast("double"))
        .withColumn("quantity", col("quantity").cast("int"))
        .dropDuplicates(["order_id"])
    )

    return df_clean

# ---------- GOLD DIMENSIONS ----------

@dlt.table(
    name="dim_customer",
    comment="Customer dimension"
)
def dim_customer():
    df = dlt.read("silver_customers")
    return df.select(
        col("customer_id").alias("customer_key"),
        "first_name",
        "last_name",
        "email",
        "country",
        "segment",
        "signup_ts"
    )

@dlt.table(
    name="dim_product",
    comment="Product dimension"
)
def dim_product():
    df = dlt.read("silver_products")
    return df.select(
        col("product_id").alias("product_key"),
        "product_name",
        "category",
        "sub_category",
        "brand",
        "list_price"
    )

# ---------- GOLD FACT & AGGREGATES ----------

@dlt.table(
    name="fact_orders",
    comment="Fact table for orders"
)
def fact_orders():
    orders = dlt.read("silver_orders")
    customers = dlt.read("dim_customer")
    products = dlt.read("dim_product")

    fact = (
        orders
        .join(customers, orders.customer_id == customers.customer_key, "left")
        .join(products, orders.product_id == products.product_key, "left")
        .select(
            orders.order_id,
            orders.order_ts,
            customers.customer_key,
            products.product_key,
            (orders.quantity * orders.unit_price).alias("order_amount"),
            "quantity",
            "status",
            "payment_method",
            orders.country.alias("order_country")
        )
    )

    return fact

@dlt.table(
    name="gold_daily_sales",
    comment="Daily sales metrics by date and country"
)
def gold_daily_sales():
    fact = dlt.read("fact_orders")

    return (
        fact
        .withColumn("order_date", date_trunc("DAY", "order_ts"))
        .groupBy("order_date", "order_country")
        .agg(
            _sum("order_amount").alias("total_revenue"),
            _sum("quantity").alias("total_quantity"),
            countDistinct("customer_key").alias("unique_customers")
        )
    )
