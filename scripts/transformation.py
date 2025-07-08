from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, countDistinct, count, when, lit
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
import boto3
import os
import logging
import sys
import json
from functools import reduce
from pyspark.sql import DataFrame
from helperfunction import create_table_if_not_exists, upsert_category_kpi_batch, upsert_order_kpi_batch

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def convert_s3_to_s3a(path):
    if path is None:
        return None
    if path.startswith("s3://"):
        return "s3a://" + path[len("s3://"):]
    return path

# Environment variables with s3a:// conversion
CATEGORY_KPI_TABLE = os.environ['CATEGORY_KPI_TABLE']
ORDER_KPI_TABLE = os.environ['ORDER_KPI_TABLE']

# Expect JSON arrays for multiple paths
VALIDATED_ORDERS_PATHS_JSON = os.environ.get('VALIDATED_ORDERS_PATHS', '[]')
VALIDATED_ORDER_ITEMS_PATHS_JSON = os.environ.get('VALIDATED_ORDER_ITEMS_PATHS', '[]')
VALIDATED_PRODUCTS_PATH = convert_s3_to_s3a(os.environ.get('VALIDATED_PRODUCTS_PATH'))

AWS_REGION = os.environ.get('AWS_REGION', 'eu-north-1')

# Parse JSON arrays and convert paths
VALIDATED_ORDERS_PATHS = [convert_s3_to_s3a(p) for p in json.loads(VALIDATED_ORDERS_PATHS_JSON)]
VALIDATED_ORDER_ITEMS_PATHS = [convert_s3_to_s3a(p) for p in json.loads(VALIDATED_ORDER_ITEMS_PATHS_JSON)]

# Initialize Spark session with Delta support
spark = SparkSession.builder.appName("Transformationjob") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Initialize DynamoDB
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)

def ensure_tables_exist():
    """Ensure both DynamoDB tables exist"""
    category_key_schema = [
        {'AttributeName': 'category', 'KeyType': 'HASH'},
        {'AttributeName': 'order_date', 'KeyType': 'RANGE'}
    ]
    category_attribute_definitions = [
        {'AttributeName': 'category', 'AttributeType': 'S'},
        {'AttributeName': 'order_date', 'AttributeType': 'S'}
    ]
    order_key_schema = [{'AttributeName': 'order_date', 'KeyType': 'HASH'}]
    order_attribute_definitions = [{'AttributeName': 'order_date', 'AttributeType': 'S'}]

    create_table_if_not_exists(CATEGORY_KPI_TABLE, category_key_schema, category_attribute_definitions)
    create_table_if_not_exists(ORDER_KPI_TABLE, order_key_schema, order_attribute_definitions)

def read_delta_or_csv(path):
    try:
        dt = DeltaTable.forPath(spark, path)
        logger.info(f"Reading Delta table from {path}")
        return dt.toDF()
    except Exception:
        logger.info(f"Path {path} is not a Delta table or not found, reading as CSV")
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

def read_delta_table(base_path):
    try:
        dt = DeltaTable.forPath(spark, base_path)
        df = dt.toDF()
        logger.info(f"Read Delta table from {base_path}")
        return df
    except Exception as e:
        logger.error(f"Error reading Delta table at {base_path}: {e}")
        return None

def compute_and_store_kpis():
    # Placeholder for Step Function input or manual override
    step_input = {
        'execution_id': 'manual_test',
        'processing_date': None
    }
    execution_id = step_input.get('execution_id', 'unknown')
    processing_date_override = step_input.get('processing_date')
    processing_date = processing_date_override or os.environ.get('PROCESSING_DATE')

    logger.info(f"Step Function Execution ID: {execution_id}")
    logger.info(f"Processing Date Override: {processing_date_override}")
    logger.info(f"Using processing date: {processing_date}")

    if not processing_date:
        logger.error("Processing date must be provided")
        raise Exception("Processing date is required")

    logger.info(f"AWS Region: {AWS_REGION}")
    logger.info(f"Category KPI Table: {CATEGORY_KPI_TABLE}")
    logger.info(f"Order KPI Table: {ORDER_KPI_TABLE}")
    logger.info(f"Orders Paths: {VALIDATED_ORDERS_PATHS}")
    logger.info(f"Order Items Paths: {VALIDATED_ORDER_ITEMS_PATHS}")
    logger.info(f"Products Path: {VALIDATED_PRODUCTS_PATH}")

    # Ensure DynamoDB tables exist
    logger.info("Ensuring DynamoDB tables exist...")
    ensure_tables_exist()
    logger.info("DynamoDB tables are ready")

    # Read and union validated Delta tables partitioned by date
    orders_df = read_and_union(VALIDATED_ORDERS_PATHS)
    order_items_df = read_and_union(VALIDATED_ORDER_ITEMS_PATHS)
    products_df = read_delta_table(VALIDATED_PRODUCTS_PATH) if VALIDATED_PRODUCTS_PATH else None

    if products_df is None:
        logger.error("Products Delta table not found or empty - required for KPI computation")
        raise Exception("Products Delta table is required")

    files_available = {
        'orders': orders_df is not None,
        'order_items': order_items_df is not None,
        'products': True
    }
    logger.info(f"Available files for {processing_date}: {files_available}")

    try:
        if files_available['orders'] and files_available['order_items']:
            logger.info("Complete data available - computing full KPIs")
            joined_df = orders_df.alias("o").join(
                order_items_df.alias("oi"), col("o.order_id") == col("oi.order_id")
            ).join(
                products_df.alias("p"), col("oi.product_id") == col("p.id")
            )

            category_kpi = joined_df.groupBy("p.category").agg(
                _sum("oi.sale_price").alias("daily_revenue"),
                avg("oi.sale_price").alias("avg_order_value"),
                (count(when(col("oi.status") == "returned", True)) / count("oi.id")).alias("avg_return_rate")
            )
            category_kpi.foreachPartition(lambda part: upsert_category_kpi_batch(part, processing_date))
            logger.info("Category KPIs written to DynamoDB")

            order_kpi = joined_df.groupBy().agg(
                countDistinct("o.order_id").alias("total_orders"),
                _sum("oi.sale_price").alias("total_revenue"),
                count("oi.id").alias("total_items_sold"),
                (count(when(col("oi.status") == "returned", True)) / count("oi.id")).alias("return_rate"),
                countDistinct("o.user_id").alias("unique_customers")
            )
            order_kpi.foreachPartition(lambda part: upsert_order_kpi_batch(part, processing_date, "complete"))
            logger.info("Order KPIs written to DynamoDB")

        elif files_available['order_items']:
            logger.info("Only order_items available - computing partial KPIs")
            joined_df = order_items_df.alias("oi").join(
                products_df.alias("p"), col("oi.product_id") == col("p.id")
            )

            category_kpi = joined_df.groupBy("p.category").agg(
                _sum("oi.sale_price").alias("daily_revenue"),
                avg("oi.sale_price").alias("avg_order_value"),
                (count(when(col("oi.status") == "returned", True)) / count("oi.id")).alias("avg_return_rate")
            )
            category_kpi.foreachPartition(lambda part: upsert_category_kpi_batch(part, processing_date))
            logger.info("Category KPIs (partial) written to DynamoDB")

            order_kpi = joined_df.groupBy().agg(
                lit(None).alias("total_orders"),
                _sum("oi.sale_price").alias("total_revenue"),
                count("oi.id").alias("total_items_sold"),
                (count(when(col("oi.status") == "returned", True)) / count("oi.id")).alias("return_rate"),
                lit(None).alias("unique_customers")
            )
            order_kpi.foreachPartition(lambda part: upsert_order_kpi_batch(part, processing_date, "order_items_only"))
            logger.info("Order KPIs (order_items_only) written to DynamoDB")

        elif files_available['orders']:
            logger.info("Only orders available - computing partial order KPIs")
            order_kpi = orders_df.groupBy().agg(
                countDistinct("order_id").alias("total_orders"),
                lit(None).alias("total_revenue"),
                lit(None).alias("total_items_sold"),
                lit(None).alias("return_rate"),
                countDistinct("user_id").alias("unique_customers")
            )
            order_kpi.foreachPartition(lambda part: upsert_order_kpi_batch(part, processing_date, "orders_only"))
            logger.info("Order KPIs (orders_only) written to DynamoDB")

            logger.info("Category KPIs skipped - need order_items for sales data")

        logger.info(f"KPI computation completed for {processing_date}")

    except Exception as e:
        logger.error(f"Error during KPI computation: {e}")
        raise
    finally:
        logger.info("Stopping Spark session...")
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    try:
        compute_and_store_kpis()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Application failed: {e}")
        sys.exit(1)
