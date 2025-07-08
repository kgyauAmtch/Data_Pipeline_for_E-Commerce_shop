from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, isnull, lit
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip
import os
import logging
import json
import boto3
from functools import reduce
from pyspark.sql import DataFrame

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables
S3_BUCKET = os.environ['S3_BUCKET_NAME']
ORDERS_PATHS_JSON = os.environ.get('ORDERS_PATHS', '[]')
ORDER_ITEMS_PATHS_JSON = os.environ.get('ORDER_ITEMS_PATHS', '[]')
PRODUCTS_PATH = os.environ.get('PRODUCTS_PATH')  # optional single path

PROCESSING_RESULT_S3_KEY = os.environ.get('PROCESSING_RESULT_S3_KEY', 'validation_output/result.json')

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

# Initialize boto3 S3 client for writing output JSON
s3_client = boto3.client('s3')

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

def read_and_union(paths):
    dfs = []
    for path in paths:
        logger.info(f"Reading data from path: {path}")
        df = read_delta_or_csv(path)
        dfs.append(df)
    if dfs:
        return reduce(DataFrame.unionByName, dfs)
    else:
        return None

def validate_non_nulls(df, columns, file_label):
    for col_name in columns:
        null_count = df.filter(isnull(col(col_name)) | isnan(col(col_name))).count()
        if null_count > 0:
            raise DataValidationError(f"{file_label} contains nulls in required column: {col_name}", "NULL_VALIDATION_ERROR")

def validate_referential_integrity(orders_df, order_items_df, products_df):
    order_ids = [row['order_id'] for row in orders_df.select("order_id").distinct().collect()]
    product_ids = [row['id'] for row in products_df.select("id").distinct().collect()] if products_df else []

    missing_orders = order_items_df.filter(~col("order_id").isin(order_ids))
    missing_order_count = missing_orders.count()
    if missing_order_count > 0:
        logger.error(f"{missing_order_count} order items reference missing orders:")
        missing_orders.select("order_id").distinct().show(truncate=False)
        raise DataValidationError("Order items reference missing orders", "REFERENTIAL_ERROR")

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

def write_processing_result_to_s3(result_dict):
    """Write the processing result JSON to S3."""
    try:
        json_str = json.dumps(result_dict)
        s3_client.put_object(Bucket=S3_BUCKET, Key=PROCESSING_RESULT_S3_KEY, Body=json_str, ContentType='application/json')
        logger.info(f"Processing result written to s3://{S3_BUCKET}/{PROCESSING_RESULT_S3_KEY}")
    except Exception as e:
        logger.error(f"Failed to write processing result to S3: {e}")
        raise

def main():
    try:
        # Parse JSON arrays from environment variables
        orders_paths = json.loads(ORDERS_PATHS_JSON)
        order_items_paths = json.loads(ORDER_ITEMS_PATHS_JSON)

        if not orders_paths:
            raise Exception("No orders paths provided.")
        if not order_items_paths:
            raise Exception("No order_items paths provided.")

        logger.info("Reading and unioning orders data...")
        orders_df = read_and_union(orders_paths)
        if orders_df is None:
            raise Exception("Failed to read orders data.")

        logger.info("Reading and unioning order_items data...")
        order_items_df = read_and_union(order_items_paths)
        if order_items_df is None:
            raise Exception("Failed to read order_items data.")

        products_df = None
        if PRODUCTS_PATH:
            logger.info("Reading products data...")
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

        result = {"status": "success", "processing_date": order_date}
        write_processing_result_to_s3(result)

        return result

    except DataValidationError as e:
        logger.error(f"Validation failed: {str(e)}")
        result = {"status": "error", "error_type": e.error_type, "message": str(e)}
        write_processing_result_to_s3(result)
        return result

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        result = {"status": "error", "error_type": "UNKNOWN", "message": str(e)}
        write_processing_result_to_s3(result)
        return result

if __name__ == "__main__":
    result = main()
    print(json.dumps(result))  # For Step Function capture
    if result['status'] != 'success':
        exit(1)
