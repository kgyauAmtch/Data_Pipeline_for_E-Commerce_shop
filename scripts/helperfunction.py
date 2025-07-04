import boto3
import os
import logging
from botocore.exceptions import ClientError

# Logging setup
logger = logging.getLogger(__name__)

def create_table_if_not_exists(table_name, key_schema, attribute_definitions):
    """Create DynamoDB table if it doesn't exist"""
    try:
        # Initialize DynamoDB client for this function
        dynamodb = boto3.resource('dynamodb', region_name=os.environ.get('AWS_REGION', 'eu-north-1'))
        # Check if table exists
        table = dynamodb.Table(table_name)
        table.load()
        logger.info(f"Table {table_name} already exists")
        return table
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.info(f"Creating table {table_name}")
            try:
                table = dynamodb.create_table(
                    TableName=table_name,
                    KeySchema=key_schema,
                    AttributeDefinitions=attribute_definitions,
                    BillingMode='PAY_PER_REQUEST'
                )
                
                # Wait for table to be created
                table.wait_until_exists()
                logger.info(f"Table {table_name} created successfully")
                return table
            except Exception as create_error:
                logger.error(f"Failed to create table {table_name}: {create_error}")
                raise
        else:
            logger.error(f"Error checking table {table_name}: {e}")
            raise

def clean_item_for_dynamodb(item):
    """Remove None values from item before writing to DynamoDB"""
    return {k: v for k, v in item.items() if v is not None}

def read_csv_if_exists(path):
    """
    Read CSV with better error handling and validation.
    Uses .limit(1).count() to check for data without full scan.
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("Transformationjob").getOrCreate()
    
    if not path:
        logger.info("Path not provided, skipping file read")
        return None
    
    try:
        df = spark.read.option("header", True).csv(path)
        if df.limit(1).count() == 0:
            logger.info(f"File at {path} is empty, returning None")
            return None
        
        row_count = df.count()
        logger.info(f"Successfully read {row_count} rows from {path}")
        return df
    except Exception as e:
        logger.error(f"Failed to read file at {path}: {e}")
        return None

def upsert_category_kpi_batch(partition_iterator, processing_date):
    """
    Writes a partition of category KPI records to DynamoDB.
    """
    dynamodb = boto3.resource('dynamodb', region_name=os.environ.get('AWS_REGION', 'eu-north-1'))
    table = dynamodb.Table(os.environ['CATEGORY_KPI_TABLE'])
    
    with table.batch_writer() as batch:
        for row in partition_iterator:
            try:
                if not row.get("category"):
                    logger.warning("Skipping row with missing category in partition")
                    continue
                    
                key = {
                    "category": str(row["category"]),
                    "order_date": processing_date
                }
                
                response = table.get_item(Key=key)
                existing_item = response.get('Item', {})
                
                merged_item = {
                    "category": str(row["category"]),
                    "order_date": processing_date,
                    "last_updated": processing_date,
                    "data_sources": existing_item.get("data_sources", [])
                }
                
                daily_revenue = row.get("daily_revenue")
                avg_order_value = row.get("avg_order_value")
                avg_return_rate = row.get("avg_return_rate")
                
                if daily_revenue is not None: merged_item["daily_revenue"] = daily_revenue
                elif existing_item.get("daily_revenue"): merged_item["daily_revenue"] = existing_item["daily_revenue"]
                    
                if avg_order_value is not None: merged_item["avg_order_value"] = avg_order_value
                elif existing_item.get("avg_order_value"): merged_item["avg_order_value"] = existing_item["avg_order_value"]
                    
                if avg_return_rate is not None: merged_item["avg_return_rate"] = avg_return_rate
                elif existing_item.get("avg_return_rate"): merged_item["avg_return_rate"] = existing_item["avg_return_rate"]
                
                for source in ["order_items", "products"]:
                    if source not in merged_item["data_sources"]:
                        merged_item["data_sources"].append(source)
                
                clean_item = clean_item_for_dynamodb(merged_item)
                batch.put_item(Item=clean_item)
                
            except Exception as e:
                logger.error(f"Error processing category KPI for {row.get('category', 'unknown')}: {e}")

def upsert_order_kpi_batch(partition_iterator, processing_date, data_source):
    """
    Writes a partition of order KPI records to DynamoDB.
    """
    dynamodb = boto3.resource('dynamodb', region_name=os.environ.get('AWS_REGION', 'eu-north-1'))
    table = dynamodb.Table(os.environ['ORDER_KPI_TABLE'])
    
    with table.batch_writer() as batch:
        for row in partition_iterator:
            try:
                key = {"order_date": processing_date}
                
                response = table.get_item(Key=key)
                existing_item = response.get('Item', {})
                
                merged_item = {
                    "order_date": processing_date,
                    "last_updated": processing_date,
                    "data_sources": existing_item.get("data_sources", [])
                }
                
                if data_source == "orders_only":
                    total_orders = row.get("total_orders")
                    unique_customers = row.get("unique_customers")
                    
                    if total_orders is not None: merged_item["total_orders"] = int(total_orders)
                    elif existing_item.get("total_orders"): merged_item["total_orders"] = existing_item["total_orders"]
                        
                    if unique_customers is not None: merged_item["unique_customers"] = int(unique_customers)
                    elif existing_item.get("unique_customers"): merged_item["unique_customers"] = existing_item["unique_customers"]
                    
                    for field in ["total_revenue", "total_items_sold", "return_rate"]:
                        if existing_item.get(field): merged_item[field] = existing_item[field]
                    
                    if "orders" not in merged_item["data_sources"]: merged_item["data_sources"].append("orders")
                        
                elif data_source == "order_items_only":
                    total_revenue = row.get("total_revenue")
                    total_items_sold = row.get("total_items_sold")
                    return_rate = row.get("return_rate")
                    
                    if total_revenue is not None: merged_item["total_revenue"] = total_revenue
                    elif existing_item.get("total_revenue"): merged_item["total_revenue"] = existing_item["total_revenue"]
                        
                    if total_items_sold is not None: merged_item["total_items_sold"] = int(total_items_sold)
                    elif existing_item.get("total_items_sold"): merged_item["total_items_sold"] = existing_item["total_items_sold"]
                        
                    if return_rate is not None: merged_item["return_rate"] = return_rate
                    elif existing_item.get("return_rate"): merged_item["return_rate"] = existing_item["return_rate"]
                    
                    for field in ["total_orders", "unique_customers"]:
                        if existing_item.get(field): merged_item[field] = existing_item[field]
                    
                    if "order_items" not in merged_item["data_sources"]: merged_item["data_sources"].append("order_items")
                        
                elif data_source == "complete":
                    merged_item.update({
                        "total_orders": int(row["total_orders"]),
                        "total_revenue": row["total_revenue"],
                        "total_items_sold": int(row["total_items_sold"]),
                        "return_rate": row["return_rate"],
                        "unique_customers": int(row["unique_customers"])
                    })
                    
                    for source in ["orders", "order_items"]:
                        if source not in merged_item["data_sources"]: merged_item["data_sources"].append(source)
                
                clean_item = clean_item_for_dynamodb(merged_item)
                batch.put_item(Item=clean_item)
                
            except Exception as e:
                logger.error(f"Error processing order KPI for {processing_date}: {e}")