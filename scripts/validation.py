from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, isnull, lit
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip
import os
import logging

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
    # Convert s3:// to s3a://
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
    missing_orders = order_items_df.join(orders_df, order_items_df.order_id == orders_df.order_id, "left_anti")
    if missing_orders.count() > 0:
        raise DataValidationError("Order items reference missing orders", "REFERENTIAL_ERROR")

    if products_df is not None:
        missing_products = order_items_df.join(products_df, order_items_df.product_id == products_df.id, "left_anti")
        if missing_products.count() > 0:
            raise DataValidationError("Order items reference missing products", "REFERENTIAL_ERROR")

def write_validated_delta(df, validated_base, label):
    order_date = df.select("created_at").first()["created_at"][:10]
    partition_col = "dt"
    df_with_partition = df.withColumn(partition_col, lit(order_date))
    validated_path = f"s3a://{S3_BUCKET}/{validated_base}"
    logger.info(f"Writing validated {label} Delta table to {validated_path} partitioned by {partition_col}={order_date}")
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
        write_validated_delta(orders_df, "validated/orders", "Orders")
        write_validated_delta(order_items_df, "validated/order_items", "Order Items")
        if products_df is not None:
            validated_products_path = f"s3a://{S3_BUCKET}/validated/products"
            logger.info(f"Writing validated Products Delta table to {validated_products_path}")
            products_df.write.format("delta").mode("overwrite").save(validated_products_path)

        return {"status": "success"}

    except DataValidationError as e:
        logger.error(f"Validation failed: {str(e)}")
        return {"status": "error", "error_type": e.error_type, "message": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {"status": "error", "error_type": "UNKNOWN", "message": str(e)}

if __name__ == "__main__":
    result = main()
    if result['status'] != 'success':
        exit(1)
