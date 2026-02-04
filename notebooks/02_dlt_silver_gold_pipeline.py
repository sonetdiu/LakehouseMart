import dlt
from pyspark.sql.functions import col, current_timestamp

# Assume pipeline "target" is set to catalog lakehouse and schema silver/gold appropriately,
# or you fully qualify table names in decorators.

# ----- BRONZE READ HELPERS -----
def bronze_table(name: str):
    return f"lakehouse.bronze.{name}"

# ----- SILVER CUSTOMERS -----
@dlt.table(
    name="customers_silver",
    comment="Cleaned and conformed customers",
)
def customers_silver():
    return (
        dlt.read_stream(bronze_table("customers_bronze"))
            .dropDuplicates(["customer_id"])
            .withColumn("load_ts", current_timestamp())
    )

# ----- SILVER PRODUCTS -----
@dlt.table(
    name="products_silver",
    comment="Cleaned and conformed products",
)
def products_silver():
    return (
        dlt.read_stream(bronze_table("products_bronze"))
            .dropDuplicates(["product_id"])
            .withColumn("load_ts", current_timestamp())
    )

# ----- SILVER ORDERS -----
@dlt.table(
    name="orders_silver",
    comment="Cleaned and conformed orders with valid FKs",
)
def orders_silver():
    orders = dlt.read_stream(bronze_table("orders_bronze"))
    customers = dlt.read("customers_silver")
    products = dlt.read("products_silver")

    joined = (
        orders.alias("o")
        .join(customers.alias("c"), "customer_id", "inner")
        .join(products.alias("p"), "product_id", "inner")
        .select(
            "o.*",
            col("c.country").alias("customer_country"),
            col("p.category").alias("product_category"),
        )
        .withColumn("load_ts", current_timestamp())
    )
    return joined

# ----- GOLD FACT TABLE -----
@dlt.table(
    name="fact_orders",
    comment="Gold fact table for orders",
)
def fact_orders():
    return (
        dlt.read("orders_silver")
            .select(
                "order_id",
                "order_ts",
                "customer_id",
                "product_id",
                "quantity",
                "unit_price",
                "status",
                "payment_method",
                "country",
                "product_category",
                "customer_country",
            )
    )

# ----- GOLD AGGREGATE EXAMPLE -----
@dlt.table(
    name="agg_revenue_by_country",
    comment="Daily revenue by country",
)
def agg_revenue_by_country():
    from pyspark.sql.functions import date_trunc, sum as _sum

    return (
        dlt.read("fact_orders")
            .withColumn("order_date", date_trunc("DAY", col("order_ts")))
            .groupBy("order_date", "country")
            .agg(_sum(col("quantity") * col("unit_price")).alias("total_revenue"))
    )
