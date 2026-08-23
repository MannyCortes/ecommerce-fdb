import asyncio

import pandas as pd
import logging
import gc 
import numpy as np
import datetime as dt
import asyncio
import os
import json
import redis
import aiboto3 
import redis.asyncio as redisasync
from dotenv import load_dotenv


load_dotenv()
redis_url = os.getenv("REDIS_URL")
session = aiboto3.Session()
redis_client = redis.from_url(os.getenv("REDIS_URL"))
aws_key_id = os.getenv("S3_AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("S3_AWS_SECRET_ACCESS_KEY")
logger = logging.getLogger(__name__)
def verify_file(filename):

#dameon process that runs in the background and checks for new files in the queue
async def daemon(file_location: str,  ):
    #compensating Transaction
    while True:
        #move from task queue to processing queue
        #daemon listens forever, checks for new file in queue if no file it holds the connection and freezes cpu
        queue = await redis_client.blmove("task_queue", "processing_queue", 0)
        #if not processing_queue: !!!!NOT COMPLETE
        try: 
            #job is now a python dict 
            message = json.loads(queue)
            #establish a connection to redis and process the file
            async with session.client('s3', aws_access_key_id=aws_key_id, aws_secret_access_key=aws_secret_key) as s3:
                file = await s3.get_object(file.file, os.getenv("S3_BUCKET_NAME"), aws_secret_access_key=aws_secret_key, ExtraArgs={'ChecksumAlgorithm': 'SHA256'})
            ##Check the file prior to processing, falure leads to dead letter queue
            #After checking the file i then turned into a dataframe then downcasted.
        
            to_pd_df(message['file_location'])
            await ack_job(queue)
        except Exception as e:
            logging.error(f"Error processing {message['file_location']}: {e}")
            #acknowledge the job to remove it from the processing queue
            await redis_client.lpush("dead_letter_queue", queue)
            #remove it from processing queue 
            await redis_client.lrem("processing_queue", 0, queue)

   #time redis_client.zadd('jobs:processing:times', {job['id']: dt.datetime.now().timestamp()})
   #from which queue, where from, what specific file

async def ack_job(queue):
    redis_client.lrem("processing_queue", 1, queue)

def to_pd_df(temp_file):
    try:
    #chunk and convert to str for memory optimization
        csv_df = pd.read_csv(temp_file, chunk_size=10000, dtype=)
        return csv_df
    #returns a streamable varaible
    except Exception as e:
        logging.error(f"Error batching file{e}")

#regex allows us to validate data before attempting to downcast
def regex(df):
    # enforce int and uint 
    headers = [header for header in df.columns]
    #create our master mask
    is_valid_mask = np.ones(len(df), dtype=bool)
    for col, pattern in regex_schema.items():
        if col in headers:
            valid_col = df[col].str.match(pattern, na=False)
        is_valid_mask = is_valid_mask & valid_col 
    del df 
    #delete our df so we dont load 2 dfs into memory at the same time
    clean_data = df[is_valid_mask].copy()
    bad_data = df[~is_valid_mask].copy()
    if len(bad_data) > 0:
        pd.to_csv("quarintied_data.csv", date_format=dt.datetime.now(), index=False)
    del bad_data 
    gc.collect()


def downcast(df):
    headers = [header for header in df.columns]
    for col in headers:
        if 

