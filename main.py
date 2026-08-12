import logging
import sys
import redis.asyncio as redis
from redis.exceptions import ConnectionError, TimeoutError, ResponseError
from dotenv import load_dotenv
from boto3.dynamodb.conditions import Key
import aiboto3 
import os
from fastapi import FastAPI, UploadFile, File, Response, status
from process import to_pd_df
import json

load_dotenv()
app = FastAPI()
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
aws_key_id = os.getenv("S3_AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("S3_AWS_SECRET_ACCESS_KEY")
logger = logging.getLogger(__name__)
session = aiboto3.Session()
redis_client = redis.from_url(os.getenv("REDIS_URL"))
chunk_size = 5 * 1024 * 1024
@app.post("/upload")
async def main(file: UploadFile = File(...)):
    if file.content_type != "text/csv":
        return Response(content="Invalid file type. Please upload a CSV file.", status_code=status.HTTP_400_BAD_REQUEST)
    #ab is used for asynchronus raw byte streams without blocking event loop
    #with = context manager gaurentees the file is closed
    try:
        #async opens a connection or process and closes when block is exited
        async with session.client('s3', aws_access_key_id=aws_key_id, aws_secret_access_key=aws_secret_key) as s3:
                #await yeilds back control of event loop so other tasks can run while waiting for the write to complete
                #8Mb chunks at a time
                await s3.upload_fileobj(file.file, os.getenv("S3_BUCKET_NAME"), aws_secret_access_key=aws_secret_key, ExtraArgs={'ChecksumAlgorithm': 'SHA256'})
                #After the with loop is  complete redis allows us to  use server In memory data to store data
                #Producer consumer patterns Fast API finishes streaming to AWS Redis staples a JSON ticket
        file_location = f"s3://my-bucket/{file.filename}"
        message_payload = json.dumps({
        "task": "transform_csv",
        "file_location": f"s3://my-bucket/{file.filename}"
        })
    except Exception as e:
        #delete file from bucket
        await s3.delete_object(os.getenv("S3_BUCKET_NAME", file_location))
        logging.error(f"Error Uploading file:{file.filename} - {e}")
        return Response(content=f"Error Uploading file:{file.filename} - {e}", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    try:
        #lpsuh = lpush into  redis queue, FIFO/First In First Out 
        await redis_client.lpush("task_queue", message_payload)
        logging.info(f"File {file.filename} uploaded to S3 and task queued in Redis.")
        #redis automatically handles return responses
        #redis is asynchronou it also helps seperate fast api from pandas
    except ConnectionError:
        logging.error(f"Redis connection error: {e}")
        return Response(content=f"Redis connection error: {e}", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    except TimeoutError:
        logging.error(f"Redis timeout error: {e}")
        return Response(content=f"Redis timeout error: {e}", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    except Exception as e:
        #delete file from bucket
        logging.error(f"Error Uploading file:{file.filename} - {e}")
        return Response(content=f"Error Uploading file:{file.filename} - {e}", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
if __name__ == "__main__":
    main()