# validation.py (PySpark Version)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, isnull
import os
import logging
from datetime import datetime

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
    validated_path = f"s3://{S3_BUCKET}/{validated_base}/{partition}/{filename}"
    logger.info(f"Writing validated {label} to {validated_path}")
    df.write.mode("overwrite").option("header", True).csv(validated_path)
