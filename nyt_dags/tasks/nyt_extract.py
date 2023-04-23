
import os
import json
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import requests
import logging
import pytz
from botocore.exceptions import ClientError
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable


logger = logging.getLogger("airflow.task")


def store_nyt_raw(bucket_name:str,period=1, **context) -> str:
    """
    Fetches the most popular articles from the New York Times API for the specified period,
    and stores the raw JSON data in an S3 bucket.

    :param bucket_name: The name of the S3 bucket where the data will be stored.
    :param period: The number of days to fetch data for (default: 1).
    :param context: Additional keyword arguments for the Airflow context.
    :return: The S3 file key where the raw data is stored.
    """
    logger.info(f'****** Running in {Variable.get("AIRFLOW_ENVIRONMENT")}')
    dublin_timezone = pytz.timezone('Europe/Dublin')
    current_time_dublin = datetime.now(dublin_timezone).strftime('%Y-%m-%d %H:%M:%S %Z%z')
    current_system_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z%z')
    logger.info(f"Local Time in Dublin: {current_time_dublin}")
    logger.info(f"System Time: {current_system_time}")
    
    api_key = Variable.get('NYT_API_KEY')
    print(f"Fetching values for the past {period} days")
    url = f"https://api.nytimes.com/svc/mostpopular/v2/viewed/{period}.json"
    params = {"api-key": api_key}
    response = requests.get(url, params=params)
    if response.status_code != 200:
        logger.error("Error: %d",  response.status_code)
        raise ValueError('API returned an error')
    
    folder = 'raw'
    file_key = f"{folder}/nyt_raw_{datetime.now().strftime('%Y%m%d')}.json"
    try:
        s3_conn = S3Hook(aws_conn_id="aws_default")
        s3_conn.load_string(
            json.dumps(response.json()),
            bucket_name=bucket_name,
            key=file_key,
            replace=True
        )
        return file_key
    except ClientError as e:
        logger.error(f"Failed to store data in S3: {str(e)}")
        raise
