import boto3
import os
import json
import logging
from urllib.parse import unquote_plus
from datetime import datetime
import csv
from io import StringIO

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
sns = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')
stepfunctions = boto3.client('stepfunctions')

DDB_TABLE = os.environ['INGESTION_TABLE']
STEP_FUNCTION_ARN = os.environ['STEP_FUNCTION_ARN']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
DEBOUNCE_SECONDS = int(os.environ.get('DEBOUNCE_SECONDS', 120))
S3_BUCKET = os.environ['S3_BUCKET_NAME']

REQUIRED_HEADERS = {
    'orders': ['order_id', 'user_id', 'status', 'created_at', 'returned_at', 'shipped_at', 'delivered_at', 'num_of_item'],
    'order_items': ['id', 'order_id', 'user_id', 'product_id', 'status', 'created_at', 'shipped_at', 'delivered_at', 'returned_at', 'sale_price'],
    'products': ['id', 'sku', 'cost', 'category', 'name', 'brand', 'retail_price', 'department']
}

table = dynamodb.Table(DDB_TABLE)

def lambda_handler(event, context):
    orders_paths = event.get('orders_paths', [])
    order_items_paths = event.get('order_items_paths', [])
    products_path = event.get('products_path', '')

    # Normalize single string inputs to lists
    if isinstance(orders_paths, str):
        orders_paths = [orders_paths]
    if isinstance(order_items_paths, str):
        order_items_paths = [order_items_paths]

    errors = []

    # Validate orders files
    for orders_s3_key in orders_paths:
        try:
            validate_file(orders_s3_key, 'orders')
        except Exception as e:
            error_msg = f"Orders file {orders_s3_key}: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)

    # Validate order_items files
    for order_items_s3_key in order_items_paths:
        try:
            validate_file(order_items_s3_key, 'order_items')
        except Exception as e:
            error_msg = f"Order items file {order_items_s3_key}: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)

    # Validate products file (single)
    if products_path:
        try:
            validate_file(products_path, 'products')
        except Exception as e:
            error_msg = f"Products file {products_path}: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)

    if errors:
        logger.error(f"Validation completed with errors: {errors}")
        # Optionally, raise an exception to fail the Lambda and Step Function
        # raise Exception("Validation failed for one or more files.")
        return {
            "status": "failed",
            "errors": errors
        }

    logger.info("All files validated successfully.")
    return {
        "status": "success",
        "validated_orders_files": orders_paths,
        "validated_order_items_files": order_items_paths,
        "validated_products_file": products_path
    }

def validate_file(s3_key, file_type):
    bucket = S3_BUCKET
    logger.info(f"Validating {file_type} file: {s3_key}")

    try:
        prefix = f"s3://{bucket}/"
        if s3_key.startswith(prefix):
            key = s3_key[len(prefix):]
        else:
            key = s3_key

        obj = s3.get_object(Bucket=bucket, Key=key)
        csv_content = StringIO(obj['Body'].read().decode('utf-8'))
        reader = csv.reader(csv_content)
        headers = next(reader, None)

        if headers is None:
            raise Exception("File has no header row")

        validate_columns(file_type, headers)
        logger.info(f"Column validation passed for {s3_key}")

        # Additional validation logic can be added here

    except Exception as e:
        logger.error(f"Validation failed for {s3_key}: {e}")
        handle_invalid_file(bucket, key, file_type, reason=str(e))
        send_sns_alert(bucket, key, file_type, str(e))
        raise  # Re-raise to be caught in lambda_handler

def parse_filename_info(key):
    filename = key.split('/')[-1]
    if filename == 'products.csv':
        return 'products', '1'
    
    name_part, ext = filename.rsplit('.', 1)
    parts = name_part.split('_')
    
    if len(parts) < 2:
        return None, None

    part = parts[-1] 
    data_type = '_'.join(parts[:-1])  

    return data_type, part

def validate_columns(file_type, headers):
    required_cols = set(REQUIRED_HEADERS.get(file_type, []))
    if not required_cols:
        raise Exception(f"No required headers defined for file type: {file_type}")

    file_cols = set(headers)
    missing_cols = required_cols - file_cols
    if missing_cols:
        raise Exception(f"Missing required columns for {file_type}: {sorted(missing_cols)}")

def handle_invalid_file(bucket, key, file_type, reason):
    rejected_key_prefix = 'rejected/'
    base_key = key.replace('raw/', '')
    rejected_key = f"{rejected_key_prefix}{file_type}/{base_key}"

    try:
        s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=rejected_key)
        s3.delete_object(Bucket=bucket, Key=key)
        logger.info(f"Moved invalid file from {key} to {rejected_key}")

        reason_key = rejected_key.replace('.csv', '_reason.json')
        s3.put_object(
            Bucket=bucket,
            Key=reason_key,
            Body=json.dumps({"reason": reason, "original_key": key, "rejected_at": datetime.utcnow().isoformat()}),
            ContentType='application/json'
        )
        logger.info(f"Wrote rejection reason to {reason_key}")
    except Exception as copy_err:
        logger.error(f"Failed to move or log rejected file {key}: {copy_err}")

def send_sns_alert(bucket, key, file_type, reason):
    message = {
        "alert_type": "File Validation Failure",
        "bucket": bucket,
        "key": key,
        "file_type": file_type,
        "reason": reason,
        "alert_time": datetime.utcnow().isoformat() + "Z"
    }
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"DATA INGESTION ALERT: Validation Failed for {file_type} - {key}",
            Message=json.dumps(message, indent=2)
        )
        logger.info(f"SNS alert sent for file {key}")
    except Exception as sns_err:
        logger.error(f"Failed to send SNS alert for file {key}: {sns_err}")

def ready_to_process(group_key):
    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('group_key').eq(group_key),
        FilterExpression=boto3.dynamodb.conditions.Attr('processed_flag').eq(False)
    )
    items = response.get('Items', [])
    data_types_present = set(item['data_type_part'].split('#')[0] for item in items)
    required_data_types_for_processing = {'orders', 'order_items'}
    return required_data_types_for_processing.issubset(data_types_present)

def get_latest_products_path():
    try:
        response = table.get_item(
            Key={
                'group_key': 'latest_products',
                'data_type_part': 'products'
            }
        )
        item = response.get('Item')
        if item:
            logger.info(f"Found latest products path: {item['s3_path']}")
            return item['s3_path']
        else:
            logger.warning("No latest products path found in DynamoDB.")
            return None
    except Exception as e:
        logger.error(f"Error fetching latest products path: {e}")
        return None

def start_step_function(group_key):
    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('group_key').eq(group_key),
        FilterExpression=boto3.dynamodb.conditions.Attr('processed_flag').eq(False)
    )
    items = response.get('Items', [])

    orders_paths = []
    order_items_paths = []

    for item in items:
        data_type_part = item['data_type_part']
        s3_path = item['s3_path']

        if data_type_part.startswith('orders#'):
            orders_paths.append(f"s3://{S3_BUCKET}/{s3_path}")
        elif data_type_part.startswith('order_items#'):
            order_items_paths.append(f"s3://{S3_BUCKET}/{s3_path}")

    products_path = get_latest_products_path()
    if products_path:
        products_path = f"s3://{S3_BUCKET}/{products_path}"

    input_payload = {
        "group_key": group_key,
        "trigger_source": "file_arrival_lambda",
        "orders_paths": orders_paths,
        "order_items_paths": order_items_paths,
        "products_path": products_path or ""
    }

    logger.info(f"Step Function input payload: {json.dumps(input_payload)}")

    try:
        resp = stepfunctions.start_execution(
            stateMachineArn=STEP_FUNCTION_ARN,
            input=json.dumps(input_payload)
        )
        logger.info(f"Started Step Function for group_key={group_key}, executionArn={resp['executionArn']}")
    except Exception as e:
        logger.error(f"Failed to start Step Function for group_key={group_key}: {e}")

def mark_all_parts_processing_started(group_key):
    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('group_key').eq(group_key),
        FilterExpression=boto3.dynamodb.conditions.Attr('processed_flag').eq(False)
    )
    items = response.get('Items', [])
    if not items:
        logger.info(f"No unprocessed items found for group_key={group_key} to mark as processing started.")
        return

    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(
                Item={
                    **item,
                    'processed_flag': True,
                    'debounce_ttl': None
                }
            )
    logger.info(f"Marked {len(items)} parts as processing started for group_key={group_key}")
