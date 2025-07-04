import boto3
import csv
import json
from io import StringIO
from urllib.parse import unquote_plus
from datetime import datetime, timedelta
import os
import logging

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
stepfunctions = boto3.client('stepfunctions')

# Environment Variables
DDB_TABLE = os.environ['INGESTION_TABLE']
STEP_FUNCTION_ARN = os.environ['STEP_FUNCTION_ARN']
DEBOUNCE_SECONDS = int(os.environ.get('DEBOUNCE_SECONDS', 120))  # default 2 minutes

table = dynamodb.Table(DDB_TABLE)

REQUIRED_HEADERS = {
    'orders': ['order_id', 'user_id', 'status', 'created_at', 'returned_at', 'shipped_at', 'delivered_at', 'num_of_item'],
    'order_items': ['id', 'order_id', 'user_id', 'product_id', 'status', 'created_at', 'shipped_at', 'delivered_at', 'returned_at', 'sale_price'],
    'products': ['id', 'sku', 'cost', 'category', 'name', 'brand', 'retail_price', 'department']
}

def lambda_handler(event, context):
    records = event.get('Records')
    if not records:
        logger.warning("No 'Records' found in event; skipping processing.")
        return {"validation_status": "FAILED", "reason": "No records to process"}

    overall_success = True  # Track global validation result

    for record in records:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        logger.info(f"Processing file s3://{bucket}/{key}")

        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            lines = obj['Body'].read().decode('utf-8').splitlines()
            reader = csv.reader(lines)
            headers = next(reader, None)

            if headers is None:
                raise Exception("File has no header row")

            file_type = detect_file_type(key)
            group_key = extract_group_key(key)

            if file_type not in REQUIRED_HEADERS:
                raise Exception(f"Unsupported file type in key: {key}")

            # Validate headers
            if not set(REQUIRED_HEADERS[file_type]).issubset(set(headers)):
                handle_invalid_file(bucket, key, file_type, reason="Missing required columns")
                overall_success = False
                continue

            # Products file: always copied to same location
            if file_type == 'products':
                validated_key = 'validated/products/products.csv'
                s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=validated_key)
                if not key.endswith('/'):
                    s3.delete_object(Bucket=bucket, Key=key)

                table.update_item(
                    Key={"group_key": "latest_products"},
                    UpdateExpression="SET products_path = :path",
                    ExpressionAttributeValues={":path": validated_key}
                )
                logger.info(f"Updated latest products path to {validated_key}")
                continue

            # Extract order date
            data_row = next(reader, None)
            if data_row is None:
                raise Exception("File has no data rows")

            order_date = extract_order_date(file_type, headers, data_row)

            logger.info(f"Header validation passed for file: s3://{bucket}/{key}. Leaving in raw/")

            # Update registry (ECS will pick it up)
            update_registry(group_key, file_type, key, order_date)

        except Exception as e:
            logger.error(f"Error processing file {key}: {str(e)}")
            handle_invalid_file(bucket, key, file_type="unknown", reason=str(e))
            overall_success = False

    # Final status for Step Function Choice state
    if overall_success:
        return {"validation_status": "SUCCESS"}
    else:
        return {"validation_status": "FAILED"}

def detect_file_type(key):
    if "order_items" in key:
        return "order_items"
    elif "orders" in key:
        return "orders"
    elif "products" in key:
        return "products"
    else:
        return "unknown"

def extract_group_key(key):
    filename = key.split('/')[-1]
    parts = filename.split('_')
    last_part = parts[-1].replace('.csv', '')
    return last_part

def extract_order_date(file_type, headers, data_row):
    created_at_idx = headers.index('created_at')
    created_at = data_row[created_at_idx]
    try:
        return datetime.strptime(created_at[:10], '%Y-%m-%d').strftime('%Y-%m-%d')
    except Exception:
        logger.warning(f"Malformed created_at date: {created_at}, defaulting to today")
        return datetime.utcnow().strftime('%Y-%m-%d')

def handle_invalid_file(bucket, key, file_type, reason):
    rejected_key = key.replace('raw/', f'rejected/{file_type}/')
    s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=rejected_key)
    if not key.endswith('/'):
        s3.delete_object(Bucket=bucket, Key=key)

    reason_key = rejected_key.replace('.csv', '_reason.json')
    s3.put_object(
        Bucket=bucket,
        Key=reason_key,
        Body=json.dumps({"reason": reason, "original_key": key}),
        ContentType='application/json'
    )
    logger.info(f"Moved invalid file to {rejected_key} with reason: {reason}")

def update_registry(group_key, file_type, s3_path, order_date):
    update_expr = "SET #file_path = :path, #has_flag = :trueval, order_date = :dt"
    expr_attr_names = {
        "#file_path": f"{file_type}_path",
        "#has_flag": f"has_{file_type}"
    }
    expr_attr_values = {
        ":path": s3_path,
        ":trueval": True,
        ":dt": order_date
    }

    response = table.update_item(
        Key={"group_key": group_key},
        UpdateExpression=update_expr,
        ExpressionAttributeNames=expr_attr_names,
        ExpressionAttributeValues=expr_attr_values,
        ReturnValues="ALL_NEW"
    )

    item = response.get('Attributes', {})

    if file_type in ['orders', 'order_items']:
        if item.get("has_orders") and item.get("has_order_items"):
            logger.info(f"Both orders and order_items present for group {group_key}, triggering Step Function")
            stepfunctions.start_execution(
                stateMachineArn=STEP_FUNCTION_ARN,
                input=json.dumps({
                    "group_key": group_key,
                    "order_date": order_date,
                    "orders_path": item.get("orders_path"),
                    "order_items_path": item.get("order_items_path"),
                    "products_path": item.get("products_path")
                })
            )
        else:
            ttl = int((datetime.utcnow() + timedelta(seconds=DEBOUNCE_SECONDS)).timestamp())
            table.update_item(
                Key={"group_key": group_key},
                UpdateExpression="SET debounce_ttl = :ttl",
                ExpressionAttributeValues={":ttl": ttl}
            )
            logger.info(f"Set debounce TTL for group {group_key} to {ttl}")
