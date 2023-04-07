
import json
import pandas as pd
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def transform_nyt_json(data:dict) -> pd.DataFrame:
        fields_of_interest = ["id",
                              "published_date",
                              "section", "adx_keywords",
                              "byline", 
                              "type",
                              "title",
                              "abstract"
        ]
        filtered_results = [
            {key: result[key] for key in fields_of_interest} for result in data["results"]
        ]
        return pd.DataFrame(filtered_results)




def nyt_transform_raw(bucket_name, **context):
    file_key = context['task_instance'].xcom_pull(task_ids='store_nyt_raw')
    s3 = S3Hook(aws_conn_id='aws_default')
    try:
        str_data = s3.read_key(key=file_key, bucket_name=bucket_name)
        json_data = json.loads(str_data)
    except Exception as e:
        raise Exception("Failed to retrieve data from S3: {}".format(str(e)))

    df = transform_nyt_json(json_data)
    folder = 'processed/'
    processed_file_key = f"{folder}toparticles_{datetime.now().strftime('%Y%m%d')}.csv"
    try:
        csv_buffer = df.to_csv(index=False).encode()
        s3.load_bytes(csv_buffer, key=processed_file_key, bucket_name=bucket_name, replace=True)
    except Exception as e:
        raise Exception("Failed to store processed data in S3: {}".format(str(e)))
