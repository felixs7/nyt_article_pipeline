
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from nyt_etl import get_api_creds, read_nyt_api, transform_raw_nyt, write_data_to_s3



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'nyt_dag',
    default_args=default_args,
    description="NYT ETL process",
    schedule_interval='0 0 * * *'  # Runs every day at midnight
)



get_api_creds = PythonOperator(
    task_id='get_api_creds',
    python_callable=get_api_creds,
    dag=dag,
)

read_nyt_api = PythonOperator(
    task_id='read_nyt_api',
    python_callable=read_nyt_api,
    op_kwargs={'api_key': '{{ ti.xcom_pull(task_ids="get_api_creds") }}'},
    dag=dag,
)

transform_raw_nyt = PythonOperator(
    task_id='transform_raw_nyt',
    python_callable=transform_raw_nyt,
    op_kwargs={'data': '{{ ti.xcom_pull(task_ids="read_nyt_api") }}'},
    dag=dag,
)
write_to_s3 = PythonOperator(
    task_id='write_to_s3',
    python_callable=write_data_to_s3,
    op_kwargs={'df': '{{ ti.xcom_pull(task_ids="transform_raw_nyt") }}',
               'start_date': '{{ ds }}'},
    dag=dag,
)

get_api_creds >> read_nyt_api >> transform_raw_nyt >> write_to_s3

