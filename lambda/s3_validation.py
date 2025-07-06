import boto3
import os
import json
import logging
from urllib.parse import unquote_plus
from datetime import datetime, timedelta
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

    # Use current UTC date as group_key for all files in this event
    group_key = datetime.utcnow().strftime('%Y-%m-%d')
    logger.info(f"Using ingestion date as group_key: {group_key}")

    for record in records:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        logger.info(f"Processing file s3://{bucket}/{key}")

        try:
            file_type, part = parse_filename_info(key)
            if not all([file_type, part]):
                raise Exception(f"File {key} does not conform to naming conventions (e.g., orders_part1.csv).")

            obj = s3.get_object(Bucket=bucket, Key=key)
            csv_content = StringIO(obj['Body'].read().decode('utf-8'))
            reader = csv.reader(csv_content)
            headers = next(reader, None)

            if headers is None:
                raise Exception("File has no header row")

            validate_columns(file_type, headers)
            logger.info(f"Column validation passed for {key}")

            now_ts = int(datetime.utcnow().timestamp())
            debounce_ttl = now_ts + DEBOUNCE_SECONDS

            table.put_item(
                Item={
                    'group_key': group_key,
                    'data_type_part': f"{file_type}#{part}",
                    's3_path': key,
                    'arrival_timestamp': now_ts,
                    'processed_flag': False,
                    'debounce_ttl': debounce_ttl
                }
            )
            logger.info(f"Updated DynamoDB for group_key={group_key}, data_type={file_type}, part={part}")

            if ready_to_process(group_key):
                start_step_function(group_key)
                mark_all_parts_processing_started(group_key)
            else:
                logger.info(f"Waiting for more parts for group_key={group_key}, debounce TTL set.")

        except Exception as e:
            logger.error(f"Processing failed for file {key}: {e}")
            handle_invalid_file(bucket, key, file_type, reason=str(e))
            send_sns_alert(bucket, key, file_type, str(e))

    return {"status": "processed"}

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

def start_step_function(group_key):
    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('group_key').eq(group_key),
        FilterExpression=boto3.dynamodb.conditions.Attr('processed_flag').eq(False)
    )
    items = response.get('Items', [])
    input_payload = {
        "group_key": group_key,
        "parts_to_process": {item['data_type_part']: item['s3_path'] for item in items},
        "trigger_source": "file_arrival_lambda"
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
