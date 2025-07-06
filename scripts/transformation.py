from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, countDistinct, count, when, lit
import boto3
import os
import logging
import sys
import json
from datetime import datetime
from helperfunction import create_table_if_not_exists, clean_item_for_dynamodb, read_csv_if_exists, upsert_category_kpi_batch, upsert_order_kpi_batch

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables
CATEGORY_KPI_TABLE = os.environ['CATEGORY_KPI_TABLE']
ORDER_KPI_TABLE = os.environ['ORDER_KPI_TABLE']
VALIDATED_ORDERS_PATH = os.environ.get('VALIDATED_ORDERS_PATH')
VALIDATED_ORDER_ITEMS_PATH = os.environ.get('VALIDATED_ORDER_ITEMS_PATH')
VALIDATED_PRODUCTS_PATH = os.environ.get('VALIDATED_PRODUCTS_PATH')

# Initialize Spark session
spark = SparkSession.builder.appName("Transformationjob") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com") \
    .getOrCreate().getOrCreate()

# Initialize DynamoDB with explicit region for ECS
AWS_REGION = os.environ.get('AWS_REGION', 'eu-north-1')
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
dynamodb_client = boto3.client('dynamodb', region_name=AWS_REGION)

def ensure_tables_exist():
    """Ensure both DynamoDB tables exist"""
    
    # Category KPI table schema
    category_key_schema = [
        {'AttributeName': 'category', 'KeyType': 'HASH'},
        {'AttributeName': 'order_date', 'KeyType': 'RANGE'}
    ]
    category_attribute_definitions = [
        {'AttributeName': 'category', 'AttributeType': 'S'},
        {'AttributeName': 'order_date', 'AttributeType': 'S'}
    ]
    
    # Order KPI table schema
    order_key_schema = [
        {'AttributeName': 'order_date', 'KeyType': 'HASH'}
    ]
    order_attribute_definitions = [
        {'AttributeName': 'order_date', 'AttributeType': 'S'}
    ]
    
    # Create tables
    category_table = create_table_if_not_exists(
        CATEGORY_KPI_TABLE, 
        category_key_schema, 
        category_attribute_definitions
    )
    
    order_table = create_table_if_not_exists(
        ORDER_KPI_TABLE, 
        order_key_schema, 
        order_attribute_definitions
    )
    
    return category_table, order_table

def compute_and_store_kpis():
    """Progressive KPI computation with table creation"""
    
    # Placeholder for getting Step Function input
    step_input = {
        'execution_id': 'manual_test',
        'processing_date': None
    } 

    execution_id = step_input.get('execution_id', 'unknown')
    processing_date_override = step_input.get('processing_date')
    
    logger.info(f"Step Function Execution ID: {execution_id}")
    logger.info(f"Processing Date Override: {processing_date_override}")
    
    # Log environment info for debugging
    logger.info(f"AWS Region: {AWS_REGION}")
    logger.info(f"Category KPI Table: {CATEGORY_KPI_TABLE}")
    logger.info(f"Order KPI Table: {ORDER_KPI_TABLE}")
    logger.info(f"Orders Path: {VALIDATED_ORDERS_PATH}")
    logger.info(f"Order Items Path: {VALIDATED_ORDER_ITEMS_PATH}")
    logger.info(f"Products Path: {VALIDATED_PRODUCTS_PATH}")
    
    # Test AWS connectivity
    try:
        sts_client = boto3.client('sts', region_name=AWS_REGION)
        identity = sts_client.get_caller_identity()
        logger.info(f"AWS Identity: {identity.get('Arn', 'Unknown')}")
    except Exception as e:
        logger.error(f"AWS connectivity test failed: {e}")
        raise
    
    # Ensure DynamoDB tables exist
    logger.info("Ensuring DynamoDB tables exist...")
    try:
        ensure_tables_exist()
        logger.info("DynamoDB tables are ready")
    except Exception as e:
        logger.error(f"Failed to ensure tables exist: {e}")
        raise
    
    # Read all available files
    logger.info("Reading input files...")
    orders_df = read_csv_if_exists(VALIDATED_ORDERS_PATH)
    order_items_df = read_csv_if_exists(VALIDATED_ORDER_ITEMS_PATH)
    products_df = read_csv_if_exists(VALIDATED_PRODUCTS_PATH)
    
    # Products should always be available
    if products_df is None:
        logger.error("Products file not available - this is unexpected!")
        raise Exception("Products file is required but not found")
    
    # Get processing date from any available file or override
    processing_date = processing_date_override
    if not processing_date:
        try:
            if orders_df is not None:
                processing_date = orders_df.select("created_at").head()["created_at"][:10]
            elif order_items_df is not None:
                processing_date = order_items_df.select("created_at").head()["created_at"][:10]
            else:
                logger.error("No data files available to determine processing date")
                raise Exception("No valid input files found")
        except Exception as e:
            logger.error(f"Error determining processing date: {e}")
            raise
    
    logger.info(f"Processing date: {processing_date}")
    
    # Check what files we have
    files_available = {
        'orders': orders_df is not None,
        'order_items': order_items_df is not None,
        'products': True
    }
    
    logger.info(f"Available files for {processing_date}: {files_available}")
    
    # Process based on available data
    kpi_results = {
        "processing_date": processing_date,
        "files_processed": files_available,
        "category_kpis_updated": 0,
        "order_kpis_updated": 0
    }
    
    try:
        # Both orders and order_items available (COMPLETE DATA)
        if files_available['orders'] and files_available['order_items']:
            logger.info("Complete data available - computing full KPIs")
            
            # Full join for complete KPIs
            joined_df = orders_df.alias("o").join(
                order_items_df.alias("oi"), col("o.order_id") == col("oi.order_id")
            ).join(
                products_df.alias("p"), col("oi.product_id") == col("p.id")
            )
            
            # Category KPIs (complete)
            category_kpi = joined_df.groupBy("p.category").agg(
                _sum("oi.sale_price").alias("daily_revenue"),
                avg("oi.sale_price").alias("avg_order_value"),
                (count(when(col("oi.status") == "returned", True)) / count("oi.id")).alias("avg_return_rate")
            )
            
            # Write to DynamoDB using foreachPartition
            category_kpi.foreachPartition(lambda part: upsert_category_kpi_batch(part, processing_date))
            logger.info("Category KPIs written to DynamoDB via foreachPartition")
            
            # Order KPIs (complete)
            order_kpi = joined_df.groupBy().agg(
                countDistinct("o.order_id").alias("total_orders"),
                _sum("oi.sale_price").alias("total_revenue"),
                count("oi.id").alias("total_items_sold"),
                (count(when(col("oi.status") == "returned", True)) / count("oi.id")).alias("return_rate"),
                countDistinct("o.user_id").alias("unique_customers")
            )
            
            # Write to DynamoDB using foreachPartition
            order_kpi.foreachPartition(lambda part: upsert_order_kpi_batch(part, processing_date, "complete"))
            logger.info("Order KPIs (complete) written to DynamoDB via foreachPartition")
            
        # Only order_items available (PARTIAL DATA)
        elif files_available['order_items']:
            logger.info("Only order_items available - computing partial KPIs")
            
            # Order items with products for category analysis
            joined_df = order_items_df.alias("oi").join(
                products_df.alias("p"), col("oi.product_id") == col("p.id")
            )
            
            # Category KPIs (partial - based on order_items only)
            category_kpi = joined_df.groupBy("p.category").agg(
                _sum("oi.sale_price").alias("daily_revenue"),
                avg("oi.sale_price").alias("avg_order_value"),
                (count(when(col("oi.status") == "returned", True)) / count("oi.id")).alias("avg_return_rate")
            )
            
            # Write to DynamoDB using foreachPartition
            category_kpi.foreachPartition(lambda part: upsert_category_kpi_batch(part, processing_date))
            logger.info("Category KPIs (partial) written to DynamoDB via foreachPartition")
            
            # Order KPIs (partial - financial data only)
            order_kpi = joined_df.groupBy().agg(
                lit(None).alias("total_orders"),
                _sum("oi.sale_price").alias("total_revenue"),
                count("oi.id").alias("total_items_sold"),
                (count(when(col("oi.status") == "returned", True)) / count("oi.id")).alias("return_rate"),
                lit(None).alias("unique_customers")
            )
            
            # Write to DynamoDB using foreachPartition
            order_kpi.foreachPartition(lambda part: upsert_order_kpi_batch(part, processing_date, "order_items_only"))
            logger.info("Order KPIs (order_items_only) written to DynamoDB via foreachPartition")
            
        # Only orders available (PARTIAL ORDER DATA)
        elif files_available['orders']:
            logger.info("Only orders available - computing partial order KPIs")
            
            # Order KPIs (partial - order data only)
            order_kpi = orders_df.groupBy().agg(
                countDistinct("order_id").alias("total_orders"),
                lit(None).alias("total_revenue"),
                lit(None).alias("total_items_sold"),
                lit(None).alias("return_rate"),
                countDistinct("user_id").alias("unique_customers")
            )
            
            # Write to DynamoDB using foreachPartition
            order_kpi.foreachPartition(lambda part: upsert_order_kpi_batch(part, processing_date, "orders_only"))
            logger.info("Order KPIs (orders_only) written to DynamoDB via foreachPartition")
            
            # No category KPIs possible without order_items
            logger.info("Category KPIs skipped - need order_items for sales data")
            
        logger.info(f"Progressive KPI computation completed for {processing_date}")
        
    except Exception as e:
        logger.error(f"Error during KPI computation: {e}")
        raise
    finally:
        # Stop Spark session
        logger.info("Stopping Spark session...")
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    try:
        compute_and_store_kpis()
        sys.exit(0)  # Success
    except Exception as e:
        logger.error(f"Application failed: {e}")
        sys.exit(1)  # Failure