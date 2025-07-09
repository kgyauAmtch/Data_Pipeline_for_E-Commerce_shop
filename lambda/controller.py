import boto3
import os
import json
import logging
from datetime import datetime
from decimal import Decimal
from botocore.exceptions import ClientError

# Configure logger to output INFO level logs
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS resources and clients
dynamodb = boto3.resource('dynamodb')
stepfunctions = boto3.client('stepfunctions')

# Load environment variables for DynamoDB table and Step Function ARN
BATCH_CHUNKS_TABLE = os.environ['BATCH_CHUNKS_TABLE']
STEP_FUNCTION_ARN = os.environ['STEP_FUNCTION_ARN']

# Reference to the DynamoDB table for batch chunks
batch_chunks_table = dynamodb.Table(BATCH_CHUNKS_TABLE)

class DecimalEncoder(json.JSONEncoder):
    """
    Custom JSON encoder to convert DynamoDB Decimal types to int or float.
    This is needed because DynamoDB uses Decimal for all numbers.
    """
    def default(self, obj):
        if isinstance(obj, Decimal):
            # Convert whole numbers to int, decimals to float
            if obj % 1 == 0:
                return int(obj)
            else:
                return float(obj)
        return super(DecimalEncoder, self).default(obj)

def lambda_handler(event, context):
    """
    AWS Lambda handler that controls batch processing by:
    1. Checking if a batch chunk is currently processing.
    2. If not, fetching the next pending batch chunk.
    3. Starting a Step Function execution for that batch chunk.
    4. Updating the batch chunk status to 'processing' in DynamoDB.
    """
    logger.info(f"Controller Lambda started at {datetime.utcnow().isoformat()}")

    try:
        from boto3.dynamodb.conditions import Key

        # Step 1: Query DynamoDB for any batch chunk currently in 'processing' status
        processing_chunks = batch_chunks_table.query(
            IndexName='status-created_at-index',  # GSI on status and created_at for efficient querying
            KeyConditionExpression=Key('status').eq('processing'),
            Limit=1  # We only need to know if any processing chunk exists
        ).get('Items', [])

        # If a processing chunk exists, log and exit early to avoid concurrent processing
        if processing_chunks:
            logger.info(f"Batch chunk already processing: {processing_chunks[0]['batch_id']}. Exiting.")
            return {"status": "already_processing", "batch_id": processing_chunks[0]['batch_id']}

        # Step 2: Query DynamoDB for the next pending batch chunk, ordered by creation time ascending
        pending_chunks = batch_chunks_table.query(
            IndexName='status-created_at-index',
            KeyConditionExpression=Key('status').eq('pending'),
            ScanIndexForward=True,  # Ascending order by created_at
            Limit=1
        ).get('Items', [])

        # If no pending batch chunk found, log and exit
        if not pending_chunks:
            logger.info("No pending batch chunks found. Nothing to process.")
            return {"status": "no_pending_batches"}

        # Extract batch chunk details from the first pending item
        batch_chunk = pending_chunks[0]
        batch_id = str(batch_chunk['batch_id'])
        group_key = str(batch_chunk['group_key'])
        file_paths = batch_chunk['file_paths']
        batch_start_timestamp = batch_chunk['batch_start_timestamp']

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
            "trigger_source": "controller_lambda"
        }

        # Step 3: Start the Step Function execution with the prepared input
        response = stepfunctions.start_execution(
            stateMachineArn=STEP_FUNCTION_ARN,
            input=json.dumps(input_payload, cls=DecimalEncoder)  # Use custom encoder for Decimal serialization
        )
        execution_arn = response['executionArn']
        logger.info(f"Started Step Function execution: {execution_arn}")

        # Step 4: Update the batch chunk status in DynamoDB to 'processing' with execution details
        logger.info(f"Updating batch chunk status to 'processing' for group_key: {group_key}, batch_id: {batch_id}")

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
                ':p': int(datetime.utcnow().timestamp())  # Store processing start time as UNIX timestamp
            }
        )

        logger.info(f"Batch chunk {batch_id} status updated to 'processing'.")

        # Return success response with execution details
        return {
            "status": "started_processing",
            "batch_id": batch_id,
            "execution_arn": execution_arn
        }

    except ClientError as e:
        # Log and re-raise AWS client errors
        logger.error(f"AWS ClientError: {e}")
        raise e
    except Exception as e:
        # Log and re-raise unexpected errors
        logger.error(f"Unexpected error: {e}")
        raise e
