import boto3
import os
import json
import logging
from datetime import datetime
from decimal import Decimal

# Configure logger to output INFO level logs
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS DynamoDB resource and Step Functions client
dynamodb = boto3.resource('dynamodb')
stepfunctions = boto3.client('stepfunctions')

# Environment variables for DynamoDB table and Step Function ARN
BATCH_CHUNKS_TABLE = os.environ['BATCH_CHUNKS_TABLE']
STEP_FUNCTION_ARN = os.environ['STEP_FUNCTION_ARN']

# Reference to the DynamoDB table storing batch chunks
batch_chunks_table = dynamodb.Table(BATCH_CHUNKS_TABLE)

def decimal_default(obj):
    """
    JSON serializer helper to convert DynamoDB Decimal types to int or float.
    Raises TypeError if object is not serializable.
    """
    if isinstance(obj, Decimal):
        # Convert whole numbers to int, decimals to float
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def lambda_handler(event, context):
    """
    AWS Lambda handler triggered by DynamoDB Streams.
    Processes INSERT or MODIFY events on batch chunks table:
    - Deserializes the new image data.
    - Starts a Step Function execution for batch chunks with 'pending' status.
    - Updates the batch chunk status to 'processing' with execution details.
    """
    logger.info(f"DynamoDB Stream event received with {len(event['Records'])} records")

    for record in event['Records']:
        # Process only INSERT or MODIFY events
        if record['eventName'] not in ('INSERT', 'MODIFY'):
            continue

        # Extract the new image (updated item) from the DynamoDB stream record
        new_image = record['dynamodb'].get('NewImage')
        if not new_image:
            logger.warning("No NewImage found in record, skipping")
            continue

        # Deserialize DynamoDB JSON format to Python dictionary
        batch_chunk = {k: deserialize_dynamodb_value(v) for k, v in new_image.items()}

        # Process only batch chunks with status 'pending'
        if batch_chunk.get('status') != 'pending':
            logger.info(f"Skipping batch chunk {batch_chunk.get('batch_id')} with status {batch_chunk.get('status')}")
            continue

        # Extract relevant fields from the batch chunk
        batch_id = batch_chunk['batch_id']
        group_key = batch_chunk['group_key']
        batch_start_timestamp = batch_chunk['batch_start_timestamp']
        file_paths = batch_chunk['file_paths']

        # Categorize file paths by data type for Step Function input
        orders_paths = [p for p in file_paths if '/orders/' in p]
        order_items_paths = [p for p in file_paths if '/order_items/' in p]
        products_path = next((p for p in file_paths if '/products/' in p), None)

        logger.info(f"Starting Step Function for batch chunk {batch_id} with {len(file_paths)} files.")

        # Prepare input payload for Step Function execution
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
            # Start the Step Function execution with serialized input payload
            response = stepfunctions.start_execution(
                stateMachineArn=STEP_FUNCTION_ARN,
                input=json.dumps(input_payload, default=decimal_default)
            )
            execution_arn = response['executionArn']
            logger.info(f"Started Step Function execution: {execution_arn} for batch chunk {batch_id}")

            # Update the batch chunk status to 'processing' with execution metadata
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
                    ':p': int(datetime.utcnow().timestamp())  # Current UTC timestamp as int
                }
            )
            logger.info(f"Batch chunk {batch_id} status updated to 'processing'.")

        except Exception as e:
            # Log any failure to start the Step Function or update DynamoDB
            logger.error(f"Failed to start Step Function or update batch chunk {batch_id}: {e}")

    # Return completion status after processing all records
    return {"status": "completed"}


def deserialize_dynamodb_value(value):
    """
    Helper function to convert DynamoDB JSON attribute values to native Python types.
    Supports String (S), Number (N), Boolean (BOOL), Null (NULL), List (L), and Map (M).
    Logs warning and returns None for unknown types.
    """
    if 'S' in value:
        return value['S']
    elif 'N' in value:
        # Convert number string to int or float depending on presence of decimal point
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
        # Recursively deserialize list elements
        return [deserialize_dynamodb_value(v) for v in value['L']]
    elif 'M' in value:
        # Recursively deserialize map values
        return {k: deserialize_dynamodb_value(v) for k, v in value['M'].items()}
    else:
        # Unknown or unsupported DynamoDB data type
        logger.warning(f"Unknown DynamoDB data type in value: {value}")
        return None
