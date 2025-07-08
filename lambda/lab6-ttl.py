import boto3
import os
import json
import logging
from datetime import datetime
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
stepfunctions = boto3.client('stepfunctions')

BATCH_CHUNKS_TABLE = os.environ['BATCH_CHUNKS_TABLE']
STEP_FUNCTION_ARN = os.environ['STEP_FUNCTION_ARN']

batch_chunks_table = dynamodb.Table(BATCH_CHUNKS_TABLE)

def decimal_default(obj):
    if isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def lambda_handler(event, context):
    logger.info(f"DynamoDB Stream event received with {len(event['Records'])} records")

    for record in event['Records']:
        # Process only INSERT or MODIFY events
        if record['eventName'] not in ('INSERT', 'MODIFY'):
            continue

        new_image = record['dynamodb'].get('NewImage')
        if not new_image:
            logger.warning("No NewImage found in record, skipping")
            continue

        # Parse DynamoDB JSON to Python dict
        batch_chunk = {k: deserialize_dynamodb_value(v) for k, v in new_image.items()}

        # Process only batch chunks with status 'pending'
        if batch_chunk.get('status') != 'pending':
            logger.info(f"Skipping batch chunk {batch_chunk.get('batch_id')} with status {batch_chunk.get('status')}")
            continue

        batch_id = batch_chunk['batch_id']
        group_key = batch_chunk['group_key']
        batch_start_timestamp = batch_chunk['batch_start_timestamp']
        file_paths = batch_chunk['file_paths']

        # Split file_paths into separate lists
        orders_paths = [p for p in file_paths if '/orders/' in p]
        order_items_paths = [p for p in file_paths if '/order_items/' in p]
        products_path = next((p for p in file_paths if '/products/' in p), None)

        logger.info(f"Starting Step Function for batch chunk {batch_id} with {len(file_paths)} files.")

        input_payload = {
            "batch_id": batch_id,
            "group_key": group_key,
            "batch_start_timestamp": batch_start_timestamp,
            "orders_paths": orders_paths,
            "order_items_paths": order_items_paths,
            "products_path": products_path,
            "status": "pending"
        }

        try:
            response = stepfunctions.start_execution(
                stateMachineArn=STEP_FUNCTION_ARN,
                input=json.dumps(input_payload, default=decimal_default)
            )
            execution_arn = response['executionArn']
            logger.info(f"Started Step Function execution: {execution_arn} for batch chunk {batch_id}")

            # Update batch chunk status to 'processing'
            batch_chunks_table.update_item(
                Key={
                    'group_key': group_key,
                    'batch_id': batch_id
                },
                UpdateExpression="SET #st = :s, execution_arn = :e, processing_started_at = :p",
                ExpressionAttributeNames={'#st': 'status'},
                ExpressionAttributeValues={
                    ':s': 'processing',
                    ':e': execution_arn,
                    ':p': int(datetime.utcnow().timestamp())
                }
            )
            logger.info(f"Batch chunk {batch_id} status updated to 'processing'.")

        except Exception as e:
            logger.error(f"Failed to start Step Function or update batch chunk {batch_id}: {e}")

    return {"status": "completed"}


def deserialize_dynamodb_value(value):
    """Helper to convert DynamoDB JSON format to Python native types."""
    # DynamoDB JSON format has types as keys, e.g. {"S": "string"}, {"N": "123"}
    if 'S' in value:
        return value['S']
    elif 'N' in value:
        # Convert number string to int or float
        num_str = value['N']
        if '.' in num_str:
            return float(num_str)
        else:
            return int(num_str)
    elif 'BOOL' in value:
        return value['BOOL']
    elif 'NULL' in value:
        return None
    elif 'L' in value:
        return [deserialize_dynamodb_value(v) for v in value['L']]
    elif 'M' in value:
        return {k: deserialize_dynamodb_value(v) for k, v in value['M'].items()}
    else:
        logger.warning(f"Unknown DynamoDB data type in value: {value}")
        return None
