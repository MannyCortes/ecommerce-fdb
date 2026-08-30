import asynchio
import json
import redis
import redis.asyncio as redisasync
import os
import aioboto3 
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

load_dotenv()
session = aioboto3.Session()
redis_url = os.getenv("REDIS_URL")
redis_client = redisasync.from_url(os.getenv("REDIS_URL"))
bucket_name = os.getenv("S3_DLQ_BUCKET_NAME")
secret_key = os.getenv("S3_DLQ_AWS_ACESS_KEY_ID"
aws_key_id = os.getenv("S3_AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("S3_AWS_SECRET_ACCESS_KEY")



async def daemon():
    SERVER_CODES = {
    "Throttling", "RequestLimitExceeded", "InternalFailure",
    "ProvisionedThroughputExceededException", "ServiceUnavailable",
    "TransactionConflictException"
    }
    CLIENT_ERRORS = {
    "EndpointConnectionError", "ReadTimeoutError", "ConnectTimeoutError"
    }
    while True:
        try:
            #move from task queue to processing queue
            #daemon listens forever, checks for new file in queue if no file it holds the connection and freezes cpu
            queue = await redis_client.blmove("dead_letter_queue", "DLQ_proccessing", "LEFT", "LEFT", 0)
            message = json.loads(queue)
            #establish a connection to redis and process the file
            from_ticket_key = message['key']
            from_bucket_name = message['bucket_name']
            async with session.client('s3', aws_access_key_id=aws_key_id, aws_secret_access_key=aws_secret_key) as s3_bucket:
                copy_source = {"Bucket": from_bucket_name, "Key": from_ticket_key}
                await s3_bucket.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=from_ticket_key)
                await s3_client.delete_object(Bucket=from_bucket_name, Key=from_ticket_key)
            await redis_client.lrem("DLQ_processing", 1, queue)
        except BotoCoreError as e:
            if type(e).__name__ in CLIENT_ERRORS:
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in server_codes
        except redis.RedisError as e:


