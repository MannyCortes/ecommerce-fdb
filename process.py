import asyncio
import io
import pandas as pd
import logging
import numpy as np
import datetime as dt
import asyncio
import os
import json
import aiboto3 
import redis.asyncio as redisasync
from dotenv import load_dotenv


load_dotenv()
redis_url = os.getenv("REDIS_URL")
session = boto3.Session()
bucket_name = os.getenv("S3_BUCKET_NAME")
redis_client = redisasync.from_url(os.getenv("REDIS_URL"))
aws_key_id = os.getenv("S3_AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("S3_AWS_SECRET_ACCESS_KEY")
logger = logging.getLogger(__name__)
#dameon process that runs in the background and checks for new files in the queue
async def daemon():
    while True:
        #move from task queue to processing queue
        #daemon listens forever, checks for new file in queue if no file it holds the connection and freezes cpu
        queue = await redis_client.blmove("task_queue", "processing_queue", "LEFT", "LEFT", 0)
        #keeps the connection open, sends header and 
        try: 
            #Json raw string -> python dict
            message = json.loads(queue) 
            #establish a connection to redis and process the file
            file_location = message['file_location']
            ticket_key = message['ticket_key']
            #creates a new thread, pandas yields GIL control, runs its seperate thread outside the event loop and not jamming the daemon
            # the function itself is asynchronous but the event is asynchronous
            df = await asyncio.to_thread(to_pd_df(file_location, ticket_key, queue))   
            if df[0] != True:
                raise Exception(df[1])
            else:
                await ack_job(queue)
        except Exception as e:
            logging.error(f"Error processing {file_location}: {e}")
            message_payload = json.dumps({
                "file_location": file_location,
                "key": ticket_key
                "bucket_name": bucket_name
                })
            await redis_client.lpush("dead_letter_queue", message_payload)
            #remove it from processing queue 
            await redis_client.lrem("processing_queue", 0, queue)

   #time redis_client.zadd('jobs:processing:times', {job['id']: dt.datetime.now().timestamp()})
   #from which queue, where from, what specific file

async def ack_job(queue):
    await redis_client.lrem("processing_queue", 1, queue)
         
#synchronou
def to_pd_df(file_location, ticket_key)->tuple[bool, pd.DataFrame]:
    #begin session, synchrnous get object, read into a pandas df return
    with session.client('s3', aws_access_key_id=aws_key_id, aws_secret_access_key=aws_secret_key) as s3:
        try:
            temp_file = s3.get_object(Bucket=os.getenv("S3_BUCKET_NAME"), Key=ticket_key, ExtraArgs={'ChecksumAlgorithm': 'SHA256'})
            #stream body has to be turned into raw bytes and then put in a ram buffer
            #stream body allows us to read and control the data flow over the network
            stream = temp_file['Body'] 
            #.read() triggers the network download and transfers the streaming body into memory
            raw_bytes = stream.read()
            #io turns the raw bytes into a file-like object that pandas can read from
            file = io.BytesIO(raw_bytes) 
            # If Pandas did not surrnder GIL itd be bouncing back and forth between 
            # the original thread/event loop, since pandas is C both threads can run in parrallel with not interference   
            df = pd.read_csv(file, chunk_size=10000, dtype=str)  
            for chunk in df:
                chunk = regex(chunk, file_location)
                chunk = downcast(chunk, file_location)   
            return (True, df) 
        except pd.errors as e:
            logging.error(f"Error reading {file_location} into pandas. Error: {e}")
            return (False, e)
        except Exception as e:
            logging.error(f"Error found at {file_location}, file redirected to DLQ Error: {e}")
            return(False, e)

#regex allows us to validate data before attempting to downcast
def regex(df_chunk, file_loc)->pd.DataFrame:
    #creates a new df in memory, gc collects the original
    try: 
        df_chunk = df_chunk.where(pd.notnull(df_chunk), None)
        headers = [header for header in df_chunk.columns]
        #create our master mask
        is_valid_mask = np.ones(len(df_chunk), dtype=bool)
        for col, pattern in regex_schema.items():
            if col in headers:
                #valid_col returns an array 
                valid_col = df_chunk[col].str.match(pattern, na=False)
            is_valid_mask = is_valid_mask & valid_col 
        #delete our df so we dont load 2 dfs into memory at the same time
        clean_data = df_chunk[is_valid_mask].copy()
        bad_data = df_chunk[~is_valid_mask].copy()
        if len(bad_data) > 0:
            pd.to_csv(f"{file_loc}.csv", date_format=dt.datetime.now(), index=False)

        numeric_cols = #[list of names of numeric columns

        for col in numeric_cols:
            if col in clean_data.columns:
                df_chunk[col] = pd.to_numeric(clean_data[col])
        if len(clean_data) > 0:
            return clean_data
    except pd.errors as e:
        logging.error(f"Pandas Error: {e}, File: {file_loc}")
        return (False, e)
    except Exception as e:
        logging.error(f"Error: {e} processing File: {file_loc}")
        return  (False, e)

def downcast(df_chunk, file_location):
    try:
        columns = [header for header in df_chunk.columns]
        for col in columns:
            d_type = str(df_chunk[col].dtype).lower()
            #if unique/total row is around 50% using 'category' creates a look up table,
            if d_type == "object":
                unique_str = df_chunk[col].nunique()
                total_rows = len(df_chunk[col])
                if unique_str/total_rows < 0.5:
                    df_chunk[col] = df_chunk[col].astype('category')
            elif "int" in d_type:
                df_min = df_chunk[col].min()
                if df_min >= 0:
                    df_chunk[col] = pd.to_numeric(df_chunk[col], downcast='unsigned')
                else: df_chunk[col] = pd.to_numeric(df_chunk[col], downcast='integer')
            elif "float" in d_type:
                df_chunk[col]= pd.to_numeric(df_chunk[col], downcast='float')
        return df_chunk
    except Exception as e:
        logging.error(f"Error: {e} optimizing memory {file_location}")