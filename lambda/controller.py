import boto3
import os
import json
import logging
from datetime import datetime
from decimal import Decimal
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
stepfunctions = boto3.client('stepfunctions')

BATCH_CHUNKS_TABLE = os.environ['BATCH_CHUNKS_TABLE']
STEP_FUNCTION_ARN = os.environ['STEP_FUNCTION_ARN']

batch_chunks_table = dynamodb.Table(BATCH_CHUNKS_TABLE)

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            if obj % 1 == 0:
                return int(obj)
            else:
                return float(obj)
        return super(DecimalEncoder, self).default(obj)

def lambda_handler(event, context):
    logger.info(f"Controller Lambda started at {datetime.utcnow().isoformat()}")

    try:
        from boto3.dynamodb.conditions import Key

        # Step 1: Check if any batch chunk is currently processing
        processing_chunks = batch_chunks_table.query(
            IndexName='status-created_at-index',
            KeyConditionExpression=Key('status').eq('processing'),
            Limit=1
        ).get('Items', [])

        if processing_chunks:
            logger.info(f"Batch chunk already processing: {processing_chunks[0]['batch_id']}. Exiting.")
            return {"status": "already_processing", "batch_id": processing_chunks[0]['batch_id']}

        # Step 2: Fetch next pending batch chunk ordered by created_at
        pending_chunks = batch_chunks_table.query(
            IndexName='status-created_at-index',
            KeyConditionExpression=Key('status').eq('pending'),
            ScanIndexForward=True,
            Limit=1
        ).get('Items', [])

        if not pending_chunks:
            logger.info("No pending batch chunks found. Nothing to process.")
            return {"status": "no_pending_batches"}

        batch_chunk = pending_chunks[0]
        batch_id = str(batch_chunk['batch_id'])
        group_key = str(batch_chunk['group_key'])
        file_paths = batch_chunk['file_paths']
        batch_start_timestamp = batch_chunk['batch_start_timestamp']

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
            "trigger_source": "controller_lambda"
        }

        response = stepfunctions.start_execution(
            stateMachineArn=STEP_FUNCTION_ARN,
            input=json.dumps(input_payload, cls=DecimalEncoder)
        )
        execution_arn = response['executionArn']
        logger.info(f"Started Step Function execution: {execution_arn}")

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
                ':p': int(datetime.utcnow().timestamp())
            }
        )

        logger.info(f"Batch chunk {batch_id} status updated to 'processing'.")

        return {
            "status": "started_processing",
            "batch_id": batch_id,
            "execution_arn": execution_arn
        }

    except ClientError as e:
        logger.error(f"AWS ClientError: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise e
