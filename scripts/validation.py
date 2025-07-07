from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, isnull, lit
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip
import os
import logging
import json

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables
S3_BUCKET = os.environ['S3_BUCKET_NAME']
ORDERS_PATH = os.environ['ORDERS_PATH']
ORDER_ITEMS_PATH = os.environ['ORDER_ITEMS_PATH']
PRODUCTS_PATH = os.environ.get('PRODUCTS_PATH')  

# Initialize Spark session with Delta support
builder = SparkSession.builder.appName("ValidationJob") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

class DataValidationError(Exception):
    def __init__(self, message, error_type=None):
        super().__init__(message)
        self.error_type = error_type

def read_delta_or_csv(path):
    if path.startswith('s3://'):
        path = path.replace('s3://', 's3a://')
    try:
        dt = DeltaTable.forPath(spark, path)
        logger.info(f"Reading Delta table from {path}")
        return dt.toDF()
    except Exception as e:
        logger.info(f"Path {path} is not a Delta table or not found, reading as CSV. Reason: {e}")
        return spark.read.option("header", True).csv(path)

def validate_non_nulls(df, columns, file_label):
    for col_name in columns:
        null_count = df.filter(isnull(col(col_name)) | isnan(col(col_name))).count()
        if null_count > 0:
            raise DataValidationError(f"{file_label} contains nulls in required column: {col_name}", "NULL_VALIDATION_ERROR")


def validate_referential_integrity(orders_df, order_items_df, products_df):
    # Collect distinct IDs to driver
    order_ids = [row['order_id'] for row in orders_df.select("order_id").distinct().collect()]
    product_ids = [row['id'] for row in products_df.select("id").distinct().collect()] if products_df else []

    # Check for missing orders
    missing_orders = order_items_df.filter(~col("order_id").isin(order_ids))
    missing_order_count = missing_orders.count()
    if missing_order_count > 0:
        logger.error(f"{missing_order_count} order items reference missing orders:")
        missing_orders.select("order_id").distinct().show(truncate=False)
        raise DataValidationError("Order items reference missing orders", "REFERENTIAL_ERROR")

    # Check for missing products
    if products_df is not None:
        missing_products = order_items_df.filter(~col("product_id").isin(product_ids))
        missing_product_count = missing_products.count()
        if missing_product_count > 0:
            logger.error(f"{missing_product_count} order items reference missing products:")
            missing_products.select("product_id").distinct().show(truncate=False)
            raise DataValidationError("Order items reference missing products", "REFERENTIAL_ERROR")

    logger.info("Referential integrity checks passed.")

def write_validated_delta(df, validated_base, label, partition_value=None):
    partition_col = "dt"
    df_with_partition = df.withColumn(partition_col, lit(partition_value))
    validated_path = f"s3a://{S3_BUCKET}/{validated_base}"
    logger.info(f"Writing validated {label} Delta table to {validated_path} partitioned by {partition_col}={partition_value}")
    df_with_partition.write.format("delta").mode("overwrite").partitionBy(partition_col).save(validated_path)

def main():
    try:
        logger.info("Reading orders from S3...")
        orders_df = read_delta_or_csv(ORDERS_PATH)

        logger.info("Reading order_items from S3...")
        order_items_df = read_delta_or_csv(ORDER_ITEMS_PATH)

        products_df = None
        if PRODUCTS_PATH:
            logger.info("Reading products from S3...")
            products_df = read_delta_or_csv(PRODUCTS_PATH)

        logger.info("Validating non-null fields...")
        validate_non_nulls(orders_df, ['order_id', 'user_id', 'created_at'], 'Orders')
        validate_non_nulls(order_items_df, ['id', 'order_id', 'product_id', 'created_at'], 'Order Items')
        if products_df is not None:
            validate_non_nulls(products_df, ['id', 'sku', 'cost'], 'Products')

        logger.info("Validating referential integrity...")
        validate_referential_integrity(orders_df, order_items_df, products_df)

        logger.info("Validation successful. Writing validated Delta tables to S3...")

        order_date = orders_df.select("created_at").first()["created_at"][:10]

        write_validated_delta(orders_df, "validated/orders", "Orders", order_date)
        write_validated_delta(order_items_df, "validated/order_items", "Order Items", order_date)
        if products_df is not None:
            validated_products_path = f"s3a://{S3_BUCKET}/validated/products"
            logger.info(f"Writing validated Products Delta table to {validated_products_path}")
            products_df.write.format("delta").mode("overwrite").save(validated_products_path)

        return {"status": "success", "processing_date": order_date}

    except DataValidationError as e:
        logger.error(f"Validation failed: {str(e)}")
        return {"status": "error", "error_type": e.error_type, "message": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {"status": "error", "error_type": "UNKNOWN", "message": str(e)}

if __name__ == "__main__":
    result = main()
    print(json.dumps(result))  # Required for Step Function to capture ECS stdout
    if result['status'] != 'success':
        exit(1)
