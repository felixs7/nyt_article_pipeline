
import os
import json
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import requests
from botocore.exceptions import ClientError
from airflow.providers.amazon.aws.hooks.s3 import S3Hook



def get_api_creds(**context) -> str:
    print("Fetching creds")
    parent_dir = Path(__file__).resolve().parent.parent
    env_path = parent_dir / ".env"
    load_dotenv(dotenv_path=env_path)
    return os.environ.get('API_KEY')


def store_nyt_raw(bucket_name:str,period=1, **context) -> str:
    api_key = get_api_creds()
    print(f"Fetching values for the past {period} days")
    url = f"https://api.nytimes.com/svc/mostpopular/v2/viewed/{period}.json"
    params = {"api-key": api_key}
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print("Error: ", response.status_code)
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
        raise Exception("Failed to store data in S3: {}".format(str(e)))