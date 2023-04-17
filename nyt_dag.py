
import os
from datetime import timedelta
from airflow import DAG
import yaml
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from nyt_extract import store_nyt_raw 
from nyt_transform import nyt_transform_raw

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml"), "r") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

env = os.environ.get('AIRFLOW_ENVIRONMENT', 'qa')
allowed_values = config["allowed_env_values"]
if env not in allowed_values:
    raise ValueError(f"Invalid value for AIRFLOW_ENVIRONMENT. Allowed values are {allowed_values}")
bucket_name = config[f"bucket_name_{env}"]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 13),
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'max_active_runs':1,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'dagrun_timeout': timedelta(hours=0.2),
}

dag = DAG(
    'nyt_dag',
    default_args=default_args,
    description="NYT ETL process",
    schedule_interval='0 3 * * *', # Runs every day at 03:00 UTC
)



store_nyt_raw = PythonOperator(
    task_id='store_nyt_raw',
    python_callable=store_nyt_raw,
    op_kwargs={'api_key': '{{ ti.xcom_pull(task_ids="get_api_creds") }}',
               'bucket_name': bucket_name, 
               },
    dag=dag,
)

nyt_transform_raw = PythonOperator(
    task_id='nyt_transform_raw',
    python_callable=nyt_transform_raw,
    op_kwargs={'bucket_name': bucket_name},
    dag=dag,
)


store_nyt_raw >> nyt_transform_raw 

if __name__ == "__main__":
    dag.test()
