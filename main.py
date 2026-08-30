import logging
import sys
import redis.asyncio as redis
from dotenv import load_dotenv
import os
import uuid
import aiboto3
from fastapi import FastAPI, UploadFile, File, Response, status
import json

load_dotenv()
app = FastAPI()
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
aws_key_id = os.getenv("S3_AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("S3_AWS_SECRET_ACCESS_KEY")
logger = logging.getLogger(__name__)
session = aioboto3.Session()
redis_client = redis.from_url(os.getenv("REDIS_URL"))
max_upload_size = 30 * 1024 * 1024

@app.post("/upload")
#fast api waits for file to be buffered and loaded in server before running
#file i loaded in a buffer and a temp file, temp file deleted after context
async def main(file: UploadFile = File(...)):
    if file.size > max_upload_size:
         return Response(content="File must be 30MB or less", status_code=status.HTTP_400_BAD_REQUEST)
    if file.content_type != "text/csv":
        return Response(content="Invalid file type. Please upload a CSV file.", status_code=status.HTTP_400_BAD_REQUEST)
    #ab is used for asynchronus raw byte streams without blocking event loop
    #with = context manager gaurentees the file is closed
    ticket_key = f"raw_uploads/{uuid.uuid4()}.csv"
    try:
        #async opens a connection or process and closes when block is exited
        async with session.client('s3', aws_access_key_id=aws_key_id, aws_secret_access_key=aws_secret_key) as s3:
                #await yeilds back control of event loop so other tasks can run while waiting for the write to complete
                #8Mb chunks at a time
                #TCP Backpressure
                await s3.upload_fileobj(file.file, os.getenv("S3_BUCKET_NAME"), key=ticket_key, ExtraArgs={'ChecksumAlgorithm': 'SHA256'})
                #After the with loop is  complete redis allows us to  use server In memory data to store data
                #Producer consumer patterns Fast API finishes streaming to AWS Redis staples a JSON ticket
        file_location = f"s3://{os.getenv('S3_BUCKET_NAME')}/{ticket_key}"
        #f"s3://my-bucket/{filename}"
        message_payload = json.dumps({
        "task": "transform_csv",
        "original_filename": file.filename,
        "file_location": file_location,
        "key": ticket_key
        })
        #lpsuh = lpush into  redis queue, FIFO/First In First Out 
        await redis_client.lpush("task_queue", message_payload)
        logging.info(f"File {ticket_key} uploaded to S3 and task queued in Redis.")
        #redis automatically handles return responses
        #redis is asynchronou it also helps seperate fast api from pandas
        return Response(content=f"File {ticket_key} uploaded to S3 and task queued in Redis.", status_code=status.HTTP_200_OK)
    except redis.RedisError as e:
        logging.error(f"Redis connection error: {e}")
        async with session.client('s3', aws_access_key_id=aws_key_id, aws_secret_access_key=aws_secret_key) as s3_bucket:
            await s3_bucket.delete_object(os.getenv("S3_BUCKET_NAME"), ticket_key)
            return Response(content=f"Redis connection error", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR) 
    except Exception as e:
        logging.error(f"Error Uploading file:{ticket_key} - {e}")
        return Response(content=f"Error Uploading file:{ticket_key} - {e}", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
if __name__ == "__main__":
    main()