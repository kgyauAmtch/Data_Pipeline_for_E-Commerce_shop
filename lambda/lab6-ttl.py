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

# Environment variables
INGESTION_TABLE = os.environ['INGESTION_TABLE']
BATCH_CHUNKS_TABLE = os.environ['BATCH_CHUNKS_TABLE']
STEP_FUNCTION_ARN = os.environ['STEP_FUNCTION_ARN']
S3_BUCKET = os.environ['S3_BUCKET_NAME']

ingestion_table = dynamodb.Table(INGESTION_TABLE)
batch_chunks_table = dynamodb.Table(BATCH_CHUNKS_TABLE)

MAX_FILES_PER_CHUNK = 1000  # Max files per batch chunk

def ensure_list(value):
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return value
    raise ValueError(f"Unexpected type for file paths: {type(value)}")

def decimal_default(obj):
    if isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def get_latest_products_path():
    try:
        response = ingestion_table.get_item(
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

def chunk_list(lst, chunk_size):
    """Yield successive chunk_size-sized chunks from lst."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def lambda_handler(event, context):
    now_ts = int(datetime.utcnow().timestamp())
    logger.info(f"TTL Lambda started at {datetime.utcnow().isoformat()} (epoch: {now_ts})")

    from boto3.dynamodb.conditions import Attr

    filter_expression = (
        Attr('processed_flag').eq(False) &
        Attr('debounce_ttl').lt(now_ts)
    )

    last_evaluated_key = None
    expired_items = []

    while True:
        scan_kwargs = {
            'FilterExpression': filter_expression,
            'ProjectionExpression': 'group_key, batch_start_timestamp, data_type_part, s3_path, arrival_timestamp, processed_flag, debounce_ttl'
        }
        if last_evaluated_key:
            scan_kwargs['ExclusiveStartKey'] = last_evaluated_key

        response = ingestion_table.scan(**scan_kwargs)
        items = response.get('Items', [])
        expired_items.extend(items)

        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break

    if not expired_items:
        logger.info("No expired unprocessed items found. Exiting.")
        return {"status": "no_expired_items"}

    # Group items by (group_key, batch_start_timestamp)
    groups = {}
    for item in expired_items:
        group_key = item['group_key']
        batch_start = item.get('batch_start_timestamp')
        if batch_start is None:
            logger.warning(f"Item missing batch_start_timestamp: {item}")
            continue
        groups.setdefault((group_key, batch_start), []).append(item)

    logger.info(f"Found {len(groups)} batches with expired debounce TTL.")

    batch_chunks_created = 0

    for (group_key, batch_start), parts in groups.items():
        if group_key == 'latest_products':
            logger.info("Skipping standalone products batch execution; products path included in other batches.")
            continue  # Skip standalone products batch chunk creation

        # Separate file paths by data_type_part prefix
        orders_paths = []
        order_items_paths = []

        for item in parts:
            data_type_part = item['data_type_part']
            s3_path = item['s3_path']

            if data_type_part.startswith('orders#'):
                orders_paths.append(s3_path)
            elif data_type_part.startswith('order_items#'):
                order_items_paths.append(s3_path)

        products_path = get_latest_products_path()

        # Prepare full S3 paths
        orders_paths = [f"s3://{S3_BUCKET}/{p}" for p in orders_paths]
        order_items_paths = [f"s3://{S3_BUCKET}/{p}" for p in order_items_paths]
        if products_path:
            products_path = f"s3://{S3_BUCKET}/{products_path}"

        # Combine all file paths into one list for chunking
        all_files = orders_paths + order_items_paths
        if products_path:
            all_files.append(products_path)

        # Chunk file paths into max MAX_FILES_PER_CHUNK
        for idx, chunk_files in enumerate(chunk_list(all_files, MAX_FILES_PER_CHUNK), start=1):
            batch_id = f"{datetime.utcnow().strftime('%Y-%m-%d')}-{group_key}-{batch_start}-{idx:03d}"

            batch_chunk_item = {
                'batch_id': batch_id,
                'group_key': group_key,
                'batch_start_timestamp': batch_start,
                'status': 'pending',
                'file_paths': chunk_files,
                'created_at': int(datetime.utcnow().timestamp())
            }

            try:
                batch_chunks_table.put_item(Item=batch_chunk_item)
                batch_chunks_created += 1
                logger.info(f"Created batch chunk {batch_id} with {len(chunk_files)} files.")

                # Start Step Function execution for this batch chunk
                input_payload = {
                    "batch_id": batch_id,
                    "group_key": group_key,
                    "batch_start_timestamp": batch_start,
                    "file_paths": chunk_files,
                    "status": "pending"
                }
                response = stepfunctions.start_execution(
                    stateMachineArn=STEP_FUNCTION_ARN,
                    input=json.dumps(input_payload, default=decimal_default)
                )
                logger.info(f"Started Step Function execution: {response['executionArn']} for batch chunk {batch_id}")

            except Exception as e:
                logger.error(f"Failed to write batch chunk {batch_id} to DynamoDB or start Step Function: {e}")

        # Mark original ingestion items as processed and clear debounce_ttl
        try:
            with ingestion_table.batch_writer() as batch:
                for item in parts:
                    updated_item = dict(item)
                    updated_item['processed_flag'] = True
                    updated_item['debounce_ttl'] = None
                    batch.put_item(Item=updated_item)
            logger.info(f"Marked {len(parts)} ingestion items as processed for group_key={group_key}, batch_start={batch_start}")
        except Exception as e:
            logger.error(f"Failed to update ingestion items for group_key={group_key}, batch_start={batch_start}: {e}")

    logger.info(f"TTL Lambda completed. Batch chunks created: {batch_chunks_created}")
    return {
        "status": "completed",
        "batch_chunks_created": batch_chunks_created,
        "total_expired_items": len(expired_items)
    }
