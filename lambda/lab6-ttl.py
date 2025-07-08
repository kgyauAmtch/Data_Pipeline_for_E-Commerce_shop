import boto3
import os
import json
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
stepfunctions = boto3.client('stepfunctions')

DDB_TABLE = os.environ['INGESTION_TABLE']
STEP_FUNCTION_ARN = os.environ['STEP_FUNCTION_ARN']
S3_BUCKET = os.environ['S3_BUCKET_NAME']

table = dynamodb.Table(DDB_TABLE)


def lambda_handler(event, context):
    now_ts = int(datetime.utcnow().timestamp())
    logger.info(f"TTL Lambda started at {datetime.utcnow().isoformat()} (epoch: {now_ts})")

    # Scan parameters
    filter_expression = (
        boto3.dynamodb.conditions.Attr('processed_flag').eq(False) &
        boto3.dynamodb.conditions.Attr('debounce_ttl').lt(now_ts)
    )

    # Pagination variables
    last_evaluated_key = None
    expired_items = []

    # Paginate through all matching items
    while True:
        scan_kwargs = {
            'FilterExpression': filter_expression,
            'ProjectionExpression': 'group_key, data_type_part, s3_path, arrival_timestamp, processed_flag, debounce_ttl'
        }
        if last_evaluated_key:
            scan_kwargs['ExclusiveStartKey'] = last_evaluated_key

        response = table.scan(**scan_kwargs)
        items = response.get('Items', [])
        expired_items.extend(items)

        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break

    if not expired_items:
        logger.info("No expired unprocessed items found. Exiting.")
        return {"status": "no_expired_items"}

    # Group items by group_key
    groups = {}
    for item in expired_items:
        group_key = item['group_key']
        groups.setdefault(group_key, []).append(item)

    logger.info(f"Found {len(groups)} groups with expired debounce TTL.")

    # Process each group
    groups_processed = 0
    for group_key, parts in groups.items():
        try:
            # Initialize lists for multiple paths
            orders_paths = []
            order_items_paths = []

            # Extract paths from current parts
            for item in parts:
                data_type_part = item['data_type_part']
                s3_path = item['s3_path']

                if data_type_part.startswith('orders#'):
                    orders_paths.append(s3_path)
                elif data_type_part.startswith('order_items#'):
                    order_items_paths.append(s3_path)

            # Always get latest products path from special record
            products_path = get_latest_products_path()

            # Prepend S3 bucket and s3:// prefix if paths exist
            orders_paths = [f"s3://{S3_BUCKET}/{p}" for p in orders_paths]
            order_items_paths = [f"s3://{S3_BUCKET}/{p}" for p in order_items_paths]
            if products_path:
                products_path = f"s3://{S3_BUCKET}/{products_path}"

            # Construct the input payload with arrays
            input_payload = {
                "group_key": group_key,
                "trigger_source": "ttl_debounce_lambda",
                "products_path": products_path or ""  # Always include, empty string if None
            }

            if orders_paths:
                input_payload["orders_paths"] = orders_paths
            if order_items_paths:
                input_payload["order_items_paths"] = order_items_paths

            logger.info(f"Starting Step Function for group_key={group_key} with input: {json.dumps(input_payload)}")

            response = stepfunctions.start_execution(
                stateMachineArn=STEP_FUNCTION_ARN,
                input=json.dumps(input_payload)
            )
            logger.info(f"Started Step Function execution: {response['executionArn']}")

            # Mark all parts as processed and clear debounce_ttl
            with table.batch_writer() as batch:
                for item in parts:
                    updated_item = dict(item)
                    updated_item['processed_flag'] = True
                    updated_item['debounce_ttl'] = None
                    batch.put_item(Item=updated_item)

            groups_processed += 1
            logger.info(f"Marked {len(parts)} parts as processed for group_key={group_key}")

        except Exception as e:
            logger.error(f"Error processing group_key={group_key}: {e}")

    logger.info(f"TTL Lambda completed. Groups processed: {groups_processed}")
    return {
        "status": "completed",
        "groups_processed": groups_processed,
        "total_expired_items": len(expired_items)
    }


def get_latest_products_path():
    """
    Retrieve the latest products file path from DynamoDB.
    Assumes a special record with group_key='latest_products' and data_type_part='products'.
    Modify this if you store it differently.
    """
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
