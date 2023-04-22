
import json
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging
from nyt_helpers import transform_nyt_json

logger = logging.getLogger("airflow.task")

def read_nyt_data(bucket_name, file_key, s3):
    try:
        str_data = s3.read_key(key=file_key, bucket_name=bucket_name)
        json_data = json.loads(str_data)
        return json_data
    except Exception as e:
        logger.error("Failed to retrieve data from S3 or transform it: {}".format(str(e)))
        raise

def store_processed_data(bucket_name, df, s3):
    folder = 'processed/'
    processed_file_key = f"{folder}toparticles_{datetime.now().strftime('%Y%m%d')}.csv"
    try:
        csv_buffer = df.to_csv(index=False).encode()
        s3.load_bytes(csv_buffer, key=processed_file_key, bucket_name=bucket_name, replace=True)
        logger.info(f"Processed data stored successfully in S3: {bucket_name}/{processed_file_key}" )
        return processed_file_key
    except Exception as e:
        logging.error("Failed to store processed data in S3: {}".format(str(e)))
        raise 


def nyt_transform_raw(bucket_name, **context):
    file_key = context['task_instance'].xcom_pull(task_ids='store_nyt_raw')
    s3 = S3Hook(aws_conn_id='aws_default')
    json_data = read_nyt_data(bucket_name, file_key, s3)
    df = transform_nyt_json(json_data)
    return store_processed_data(bucket_name, df, s3)
    
    


