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
S3_BUCKET = os.environ['S3_BUCKET_NAME']
BATCH_WINDOW_SECONDS = int(os.environ.get('BATCH_WINDOW_SECONDS', 90))  # Default: 90 seconds

REQUIRED_HEADERS = {
    'orders': ['order_id', 'user_id', 'status', 'created_at', 'returned_at', 'shipped_at', 'delivered_at', 'num_of_item'],
    'order_items': ['id', 'order_id', 'user_id', 'product_id', 'status', 'created_at', 'shipped_at', 'delivered_at', 'returned_at', 'sale_price'],
    'products': ['id', 'sku', 'cost', 'category', 'name', 'brand', 'retail_price', 'department']
}

table = dynamodb.Table(DDB_TABLE)

def lambda_handler(event, context):
    records = event.get('Records', [])
    if not records:
        logger.warning("No records found in event")
        return {"status": "no_records"}

    group_key = datetime.utcnow().strftime('%Y-%m-%d')
    logger.info(f"Using ingestion date as group_key: {group_key}")

    for record in records:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        logger.info(f"Received file: s3://{bucket}/{key}")

        try:
            file_type, part = parse_filename_info(key)
            if not all([file_type, part]):
                raise Exception(f"File {key} does not conform to naming conventions (e.g., orders_part1.csv).")

            logger.info(f"Starting column validation for {key}")
            obj = s3.get_object(Bucket=bucket, Key=key)
            csv_content = StringIO(obj['Body'].read().decode('utf-8'))
            reader = csv.reader(csv_content)
            headers = next(reader, None)

            if headers is None:
                raise Exception("File has no header row")

            validate_columns(file_type, headers)
            logger.info(f"Column validation passed for {file_type} ({key})")

            now_ts = int(datetime.utcnow().timestamp())

            # Get or create batch start timestamp for this group_key
            batch_start_ts = get_batch_start_timestamp(group_key)
            if batch_start_ts is None or now_ts > batch_start_ts + BATCH_WINDOW_SECONDS:
                batch_start_ts = now_ts  # Start a new batch window

            debounce_ttl = batch_start_ts + BATCH_WINDOW_SECONDS

            # Special case for products file: no #1 suffix
            if file_type == 'products':
                data_type_part = 'products'
                group_key = 'latest_products'  # fixed key for products
            else:
                data_type_part = f"{file_type}#{part}"
                group_key = datetime.utcnow().strftime('%Y-%m-%d')  # date-based for other files


            # Write metadata to DynamoDB
            table.put_item(
                Item={
                    'group_key': group_key,
                    'data_type_part': data_type_part,
                    's3_path': key,
                    'arrival_timestamp': now_ts,
                    'batch_start_timestamp': batch_start_ts,
                    'processed_flag': False,
                    'debounce_ttl': debounce_ttl
                }
            )
            logger.info(f"Stored file metadata in DynamoDB for {data_type_part} | batch_start: {batch_start_ts}, debounce_ttl: {debounce_ttl}")

        except Exception as e:
            logger.error(f"Validation failed for {key}: {e}")
            handle_invalid_file(bucket, key, file_type, reason=str(e))
            send_sns_alert(bucket, key, file_type, str(e))

    return {"status": "processed"}


def get_batch_start_timestamp(group_key):
    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('group_key').eq(group_key),
        FilterExpression=boto3.dynamodb.conditions.Attr('processed_flag').eq(False)
    )
    items = response.get('Items', [])
    if not items:
        return None
    return min(item.get('batch_start_timestamp', int(datetime.utcnow().timestamp())) for item in items)


def parse_filename_info(key):
    filename = key.split('/')[-1]
    if filename == 'products.csv':
        return 'products', '1'

    name_part, ext = filename.rsplit('.', 1)
    parts = name_part.split('_')
    if len(parts) < 2:
        return None, None

    return '_'.join(parts[:-1]), parts[-1]


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
        logger.info(f"SNS alert sent for validation failure of {key}")
    except Exception as sns_err:
        logger.error(f"Failed to send SNS alert for {key}: {sns_err}")
