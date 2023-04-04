
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from nyt_etl import run_nyt_etl



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
    description="NYT ETL process"
)


run_etl = PythonOperator(
    task_id='complete_nyt_etl',
    python_callable=run_nyt_etl,
    dag=dag, 
)


run_etl