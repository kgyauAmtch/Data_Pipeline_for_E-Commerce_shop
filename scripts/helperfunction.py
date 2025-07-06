import boto3
import os
import logging
from botocore.exceptions import ClientError

# Logging setup
logger = logging.getLogger(__name__)

def create_table_if_not_exists(table_name, key_schema, attribute_definitions):
    """Create DynamoDB table if it doesn't exist"""
    try:
        dynamodb = boto3.resource('dynamodb', region_name=os.environ.get('AWS_REGION', 'eu-north-1'))
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

def upsert_category_kpi_batch(partition_iterator, processing_date):
    dynamodb = boto3.resource('dynamodb', region_name=os.environ.get('AWS_REGION', 'eu-north-1'))
    table = dynamodb.Table(os.environ['CATEGORY_KPI_TABLE'])

    with table.batch_writer() as batch:
        for row in partition_iterator:
            try:
                category = getattr(row, "category", None)
                if not category:
                    logger.warning("Skipping row with missing category in partition")
                    continue

                key = {
                    "category": str(category),
                    "order_date": processing_date
                }

                response = table.get_item(Key=key)
                existing_item = response.get('Item', {})

                merged_item = {
                    "category": str(category),
                    "order_date": processing_date,
                    "last_updated": processing_date,
                    "data_sources": existing_item.get("data_sources", [])
                }

                daily_revenue = getattr(row, "daily_revenue", None)
                avg_order_value = getattr(row, "avg_order_value", None)
                avg_return_rate = getattr(row, "avg_return_rate", None)

                if daily_revenue is not None:
                    merged_item["daily_revenue"] = daily_revenue
                elif existing_item.get("daily_revenue") is not None:
                    merged_item["daily_revenue"] = existing_item["daily_revenue"]

                if avg_order_value is not None:
                    merged_item["avg_order_value"] = avg_order_value
                elif existing_item.get("avg_order_value") is not None:
                    merged_item["avg_order_value"] = existing_item["avg_order_value"]

                if avg_return_rate is not None:
                    merged_item["avg_return_rate"] = avg_return_rate
                elif existing_item.get("avg_return_rate") is not None:
                    merged_item["avg_return_rate"] = existing_item["avg_return_rate"]

                for source in ["order_items", "products"]:
                    if source not in merged_item["data_sources"]:
                        merged_item["data_sources"].append(source)

                clean_item = clean_item_for_dynamodb(merged_item)
                batch.put_item(Item=clean_item)

            except Exception as e:
                logger.error(f"Error processing category KPI for {category}: {e}")

def upsert_order_kpi_batch(partition_iterator, processing_date, data_source):
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
                    total_orders = getattr(row, "total_orders", None)
                    unique_customers = getattr(row, "unique_customers", None)

                    if total_orders is not None:
                        try:
                            merged_item["total_orders"] = int(total_orders)
                        except (ValueError, TypeError):
                            logger.warning(f"Invalid total_orders value: {total_orders}")

                    elif existing_item.get("total_orders") is not None:
                        merged_item["total_orders"] = existing_item["total_orders"]

                    if unique_customers is not None:
                        try:
                            merged_item["unique_customers"] = int(unique_customers)
                        except (ValueError, TypeError):
                            logger.warning(f"Invalid unique_customers value: {unique_customers}")

                    elif existing_item.get("unique_customers") is not None:
                        merged_item["unique_customers"] = existing_item["unique_customers"]

                    for field in ["total_revenue", "total_items_sold", "return_rate"]:
                        if existing_item.get(field) is not None:
                            merged_item[field] = existing_item[field]

                    if "orders" not in merged_item["data_sources"]:
                        merged_item["data_sources"].append("orders")

                elif data_source == "order_items_only":
                    total_revenue = getattr(row, "total_revenue", None)
                    total_items_sold = getattr(row, "total_items_sold", None)
                    return_rate = getattr(row, "return_rate", None)

                    if total_revenue is not None:
                        merged_item["total_revenue"] = total_revenue
                    elif existing_item.get("total_revenue") is not None:
                        merged_item["total_revenue"] = existing_item["total_revenue"]

                    if total_items_sold is not None:
                        try:
                            merged_item["total_items_sold"] = int(total_items_sold)
                        except (ValueError, TypeError):
                            logger.warning(f"Invalid total_items_sold value: {total_items_sold}")
                    elif existing_item.get("total_items_sold") is not None:
                        merged_item["total_items_sold"] = existing_item["total_items_sold"]

                    if return_rate is not None:
                        merged_item["return_rate"] = return_rate
                    elif existing_item.get("return_rate") is not None:
                        merged_item["return_rate"] = existing_item["return_rate"]

                    for field in ["total_orders", "unique_customers"]:
                        if existing_item.get(field) is not None:
                            merged_item[field] = existing_item[field]

                    if "order_items" not in merged_item["data_sources"]:
                        merged_item["data_sources"].append("order_items")

                elif data_source == "complete":
                    try:
                        merged_item.update({
                            "total_orders": int(getattr(row, "total_orders")),
                            "total_revenue": getattr(row, "total_revenue"),
                            "total_items_sold": int(getattr(row, "total_items_sold")),
                            "return_rate": getattr(row, "return_rate"),
                            "unique_customers": int(getattr(row, "unique_customers"))
                        })
                    except (ValueError, TypeError) as e:
                        logger.error(f"Invalid numeric value in complete data: {e}")
                        # Optionally skip or handle differently here

                    for source in ["orders", "order_items"]:
                        if source not in merged_item["data_sources"]:
                            merged_item["data_sources"].append(source)

                clean_item = clean_item_for_dynamodb(merged_item)
                batch.put_item(Item=clean_item)

            except Exception as e:
                logger.error(f"Error processing order KPI for {processing_date}, data_source {data_source}: {e}")
