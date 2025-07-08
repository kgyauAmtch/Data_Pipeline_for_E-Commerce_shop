import boto3
import os
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
BATCH_CHUNKS_TABLE = os.environ['BATCH_CHUNKS_TABLE']
batch_chunks_table = dynamodb.Table(BATCH_CHUNKS_TABLE)

def lambda_handler(event, context):
    """
    Expected input event:
    {
        "batch_id": "2025-07-08-orders-159357-001",
        "status": "done"  # or "failed"
    }
    """
    group_key = event.get('group_key')
    batch_id = event.get('batch_id')
    status = event.get('status')

    if not all([group_key, batch_id]) or status not in ('done', 'failed'):
        error_msg = "Missing 'group_key', 'batch_id', or invalid 'status'."
        logger.error(error_msg)
        raise ValueError(error_msg)

    try:
        update_expression = "SET #st = :s, completed_at = :c"
        expression_attribute_names = {'#st': 'status'}
        expression_attribute_values = {
            ':s': status,
            ':c': int(datetime.utcnow().timestamp())
        }

        batch_chunks_table.update_item(
            Key={
                'group_key': group_key,
                'batch_id': batch_id
            },
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values
        )

        logger.info(f"Batch chunk {batch_id} status updated to '{status}'.")
        return {"group_key": group_key, "batch_id": batch_id, "status": status, "updated_at": datetime.utcnow().isoformat()}

    except Exception as e:
        logger.error(f"Failed to update batch chunk status: {e}")
        raise e