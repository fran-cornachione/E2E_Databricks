from pyspark import pipelines as dp

S3_PATH = "s3://e2e-databricks"

def ingest_cloud_csv(table_name):
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("header", "true")
            .load(f"{S3_PATH}/{table_name}")
    )

@dp.table
def bronze_customers():
    return ingest_cloud_csv("customers")

@dp.table
def bronze_orders():
    return ingest_cloud_csv("orders")

@dp.table
def bronze_deliveries():
    return ingest_cloud_csv("deliveries")

@dp.table
def bronze_payments():
    return ingest_cloud_csv("payments")

@dp.table
def bronze_products():
    return ingest_cloud_csv("products")

@dp.table
def bronze_order_items():
    return ingest_cloud_csv("order_items")