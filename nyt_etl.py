import pandas as pd
import os
import json
from datetime import datetime
import s3fs
from dotenv import load_dotenv
from pathlib import Path
import requests
from nyt_helpers import transform_nyt_json



def run_nyt_etl():
    parent_dir = Path(__file__).resolve().parent.parent
    env_path = parent_dir / ".env"
    load_dotenv(dotenv_path=env_path)
    print(os.path.exists(env_path))
    # should be passed in via airflow
    period = 1
    url = f"https://api.nytimes.com/svc/mostpopular/v2/viewed/{period}.json"
    api_key = os.environ.get('API_KEY')
    params = {"api-key": api_key}


    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        df = transform_nyt_json(data)
        df['runtime'] = datetime.now()
        print(df)
        df.to_csv("s3://nyt-project-bucket-felix/top_articles.csv")
    else:
        print("Error: ", response.status_code)



