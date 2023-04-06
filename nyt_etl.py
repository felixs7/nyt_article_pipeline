import pandas as pd
import os
import json
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import requests
from nyt_helpers import transform_nyt_json



def get_api_creds(**context) -> str:
    parent_dir = Path(__file__).resolve().parent.parent
    env_path = parent_dir / ".env"
    load_dotenv(dotenv_path=env_path)
    return os.environ.get('API_KEY')

def read_nyt_api(api_key, period=1, **context)-> dict:
    print(f"Fetching values for the past {period} days")
    url = f"https://api.nytimes.com/svc/mostpopular/v2/viewed/{period}.json"
    params = {"api-key": api_key}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return json.dumps(response.json())
    else:
        print("Error: ", response.status_code)
        raise Exception

def transform_raw_nyt(data, **context):
    # should be passed in via airflow
    data = json.loads(data)
    df = transform_nyt_json(data)
    df['runtime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return json.dumps(df.to_dict(orient='records'))

def write_data_to_s3(df, **context):
    df = pd.DataFrame.from_dict(json.loads(df))
    print('Writing df \n',df)
    if context:
        start_date = context['start_date']
        df.to_csv(f"s3://nyt-project-bucket-felix/{start_date}_top_articles.csv")
    else:
        start_date = datetime.now().strftime('%Y-%m-%d')
        df.to_csv(f"./data/{start_date}_top_articles.csv")
    
    return



if __name__ == "__main__":
    # for running the ETL process without Airflow
    print("Running as python script")
    api_key = get_api_creds()
    response_json = read_nyt_api(api_key, 1)
    df = transform_raw_nyt(response_json)
    write_data_to_s3(df)