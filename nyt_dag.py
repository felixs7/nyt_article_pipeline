
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from nyt_extract import store_nyt_raw 
from nyt_transform import nyt_transform_raw




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
}

dag = DAG(
    'nyt_dag',
    default_args=default_args,
    description="NYT ETL process",
    schedule_interval='0 0 * * *'  # Runs every day at midnight
)



store_nyt_raw = PythonOperator(
    task_id='store_nyt_raw',
    python_callable=store_nyt_raw,
    op_kwargs={'api_key': '{{ ti.xcom_pull(task_ids="get_api_creds") }}',
               'bucket_name': 'nyt-project-bucket-qa', # edit later
               },
    dag=dag,
)

nyt_transform_raw = PythonOperator(
    task_id='nyt_transform_raw',
    python_callable=nyt_transform_raw,
    op_kwargs={'bucket_name': 'nyt-project-bucket-qa'},
    dag=dag,
)


store_nyt_raw >> nyt_transform_raw 

if __name__ == "__main__":
    dag.test()