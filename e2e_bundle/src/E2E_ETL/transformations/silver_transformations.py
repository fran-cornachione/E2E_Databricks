from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- Customers ---

@dp.materialized_view(name="e2e_databricks.silver.silver_customers")
@dp.expect_or_drop("valid_age", "age BETWEEN 0 AND 120")
def silver_customers():
    customers = spark.read.table("e2e_databricks.bronze.bronze_customers")
    window = Window.partitionBy("customer_id").orderBy(F.desc("created_at"))

    return (
        customers
            .withColumn("created_at", F.coalesce(
                F.to_timestamp(F.col("created_at"), "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp(F.col("created_at"), "dd/MM/yyyy HH:mm"),
                F.to_timestamp(F.col("created_at"), "MMM dd yyyy"),
                F.to_timestamp(F.col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            ))
            .withColumn("rank", F.row_number().over(window))
            .filter(F.col("rank") == 1)
            .drop("rank")
            .withColumn("email", F.regexp_replace(F.col("email"), r"\s+", ""))
    )

# --- Products ---

@dp.materialized_view(name="e2e_databricks.silver.silver_products")
@dp.expect_or_drop("valid_stock", "stock >= 0")
def silver_products():
    return (
        spark.read.table("e2e_databricks.bronze.bronze_products")
            .withColumn("description", F.coalesce(F.col("description"), F.lit("")))
    )

# --- Orders ---

@dp.materialized_view(name="e2e_databricks.silver.silver_orders")
@dp.expect_or_drop("valid_order_date", "order_date <= shipped_date")
def silver_orders():
    orders = spark.read.table("e2e_databricks.bronze.bronze_orders")
    window = Window.partitionBy("order_id").orderBy(F.desc("ingestion_time"))

    return (
        orders
            .withColumn("rank", F.row_number().over(window))
            .filter(F.col("rank") == 1)
            .drop("rank")
            .withColumn("customer_id",
                F.regexp_extract(F.col("customer_id"), r"(\d+)", 1).cast("int")
            )
            .withColumn("total_amount",
                F.regexp_replace(
                    F.regexp_replace(F.col("total_amount"), "[^0-9,\\.]", ""),
                    ",", "."
                ).cast("double")
            )
            .withColumn("order_date", F.coalesce(
                F.to_timestamp(F.col("order_date"), "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp(F.col("order_date"), "dd/MM/yyyy HH:mm"),
                F.to_timestamp(F.col("order_date"), "MMM dd yyyy"),
                F.to_timestamp(F.col("order_date"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            ))
            .withColumn("shipped_date", F.coalesce(
                F.to_timestamp(F.col("shipped_date"), "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp(F.col("shipped_date"), "dd/MM/yyyy HH:mm"),
                F.to_timestamp(F.col("shipped_date"), "MMM dd yyyy"),
                F.to_timestamp(F.col("shipped_date"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            ))

            .withColumn("year", F.year(F.col("order_date")))
            .withColumn("month", F.month(F.col("order_date")))
    )

    

# --- Order Items ---

@dp.materialized_view(name="e2e_databricks.silver.silver_order_items")
@dp.expect_or_drop("valid_quantity", "quantity IS NOT NULL AND quantity > 0")
def silver_order_items():
    return (
        spark.read.table("e2e_databricks.bronze.bronze_order_items")
    )

# --- Payments ---

@dp.materialized_view(name="e2e_databricks.silver.silver_payments")
def silver_payments():
    window = Window.partitionBy("order_id").orderBy(F.desc("attempt"))

    return (
        spark.read.table("e2e_databricks.bronze.bronze_payments")
            .withColumn("is_final_attempt", 
                F.row_number().over(window) == 1
            )
    )

# --- Deliveries --- 

@dp.materialized_view(name="e2e_databricks.silver.silver_deliveries")
@dp.expect_or_drop("valid_delivery_date", "delivered_at >= shipped_at")
def silver_deliveries():
    return (
        spark.read.table("e2e_databricks.bronze.bronze_deliveries")
            .withColumn("shipped_at", F.to_timestamp(F.col("shipped_at")))
            .withColumn("delivered_at", F.to_timestamp(F.col("delivered_at")))
    )

# --- Events ---

"""@dp.materialized_view(name="e2e_databricks.silver.silver_events")
def silver_events():
    window = Window.partitionBy("event_id").orderBy(F.desc("event_time"))

    return (
        spark.read.table("e2e_databricks.bronze.bronze_events")
            .withColumn("payload", F.from_json(
                F.col("payload_json"),
                "customer_id long, product_id long, ts string, event_timestamp string, event string, event_name string"
            ))
            .withColumn("customer_id", F.coalesce(
                F.col("payload.customer_id"),
                F.col("customer_id")
            ))
            .withColumn("event_name", F.coalesce(
                F.col("payload.event_name"),
                F.col("payload.event"),
                F.col("event_type")
            ))
            .withColumn("event_time", F.to_timestamp(F.col("event_time")))
            .withColumn("rank", F.row_number().over(window))
            .filter(F.col("rank") == 1)
            .drop("rank", "payload_json", "payload", "event_type")
    )"""