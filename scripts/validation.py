# validation.py (PySpark Version)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, isnull
import os
import logging
from datetime import datetime

print("=== ENVIRONMENT VARIABLES DEBUG ===")
for key, val in os.environ.items():
    print(f"{key} = {val}")
print("===================================")

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables
S3_BUCKET = os.environ['S3_BUCKET_NAME']
ORDERS_PATH = os.environ['ORDERS_PATH']
ORDER_ITEMS_PATH = os.environ['ORDER_ITEMS_PATH']
PRODUCTS_PATH = os.environ.get('PRODUCTS_PATH')  # optional

# Initialize Spark session
spark = SparkSession.builder.appName("ValidationJob").getOrCreate()

class DataValidationError(Exception):
    def __init__(self, message, error_type=None):
        super().__init__(message)
        self.error_type = error_type

def read_csv(path):
    # Convert s3:// to s3a:// if needed
    if path.startswith('s3://'):
        path = path.replace('s3://', 's3a://')
    return spark.read.option("header", True).csv(path)

def validate_non_nulls(df, columns, file_label):
    for col_name in columns:
        if df.filter(isnull(col(col_name)) | isnan(col(col_name))).count() > 0:
            raise DataValidationError(f"{file_label} contains nulls in required column: {col_name}", "NULL_VALIDATION_ERROR")

def validate_referential_integrity(orders_df, order_items_df, products_df):
    missing_orders = order_items_df.join(orders_df, order_items_df.order_id == orders_df.order_id, "left_anti")
    if missing_orders.count() > 0:
        raise DataValidationError("Order items reference missing orders", "REFERENTIAL_ERROR")

    if products_df is not None:
        missing_products = order_items_df.join(products_df, order_items_df.product_id == products_df.id, "left_anti")
        if missing_products.count() > 0:
            raise DataValidationError("Order items reference missing products", "REFERENTIAL_ERROR")

def write_validated_file(df, original_path, validated_base, label):
    filename = os.path.basename(original_path)
    order_date = df.select("created_at").first()["created_at"][:10]
    partition = f"dt={order_date}"
    validated_path = f"s3a://{S3_BUCKET}/{validated_base}/{partition}/{filename}"
    logger.info(f"Writing validated {label} to {validated_path}")
    df.write.mode("overwrite").option("header", True).csv(validated_path)

def main():
    try:
        logger.info("Reading orders from S3...")
        orders_df = read_csv(ORDERS_PATH)

        logger.info("Reading order_items from S3...")
        order_items_df = read_csv(ORDER_ITEMS_PATH)

        products_df = None
        if PRODUCTS_PATH:
            logger.info("Reading products from S3...")
            products_df = read_csv(PRODUCTS_PATH)

        logger.info("Validating non-null fields...")
        validate_non_nulls(orders_df, ['order_id', 'user_id', 'created_at'], 'Orders')
        validate_non_nulls(order_items_df, ['id', 'order_id', 'product_id', 'created_at'], 'Order Items')
        if products_df is not None:
            validate_non_nulls(products_df, ['id', 'sku', 'cost'], 'Products')

        logger.info("Validating referential integrity...")
        validate_referential_integrity(orders_df, order_items_df, products_df)

        logger.info("Validation successful. Writing validated files to S3...")
        write_validated_file(orders_df, ORDERS_PATH, "validated/orders", "Orders")
        write_validated_file(order_items_df, ORDER_ITEMS_PATH, "validated/order_items", "Order Items")
        if products_df is not None:
            validated_path = f"s3a://{S3_BUCKET}/validated/products/products.csv"
            logger.info(f"Writing validated Products to {validated_path}")
            products_df.write.mode("overwrite").option("header", True).csv(validated_path)

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
