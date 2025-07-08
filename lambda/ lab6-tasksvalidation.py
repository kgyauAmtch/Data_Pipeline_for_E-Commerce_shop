import json
import boto3
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket = os.environ['S3_BUCKET_NAME']
    key = os.environ.get('PROCESSING_RESULT_S3_KEY', 'validation_output/result.json')

    try:
        logger.info(f"Reading processing result from s3://{bucket}/{key}")
        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj['Body'].read().decode('utf-8')
        data = json.loads(content)
        logger.info(f"Processing result: {data}")
        return data

    except Exception as e:
        logger.error(f"Error reading processing result from S3: {e}")
        raise e
