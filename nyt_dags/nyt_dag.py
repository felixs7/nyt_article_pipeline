
import os
from datetime import timedelta, datetime
from airflow import DAG
import yaml
from tasks.nyt_extract import store_nyt_raw 
from tasks.nyt_transform import nyt_transform_raw
from tasks.s3_to_redshift import s3_to_redshift
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable


def load_config() -> dict:
    """Load configuration from config.yaml file"""
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml"), "r") as f:
        try:
            config = yaml.load(f, Loader=yaml.FullLoader)
        except yaml.YAMLError as e:
            raise ValueError(f"Error loading configuration file: {str(e)}")
    return config

def set_airflow_vars(config: dict, env: str):
    """Set Airflow variables based on the environment."""
    Variable.set('redshift_landing_schema', config['redshift_landing_schema'])
    Variable.set('bucket_name', config[f'bucket_name_{env}'])
    Variable.set('redshift_db_name', config[f'redshift_db_name_{env}'])
    Variable.set('full_redshift_dump', config[f'full_redshift_dump'])
    return 

config = load_config()
allowed_values = config["allowed_env_values"]
env = Variable.get('AIRFLOW_ENVIRONMENT', 'qa') # config['env']
dbt_path ='/home/ubuntu/airflow/dbt_project/nyt_dbt_project'
if env not in allowed_values:
    raise ValueError(f"Invalid value for AIRFLOW_ENVIRONMENT. Allowed values are {allowed_values}")

set_airflow_vars(config, env)


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
    description=f"NYT ETL process ({env} environment)",
    schedule_interval='0 3 * * *', # Runs every day at 03:00 UTC
)

store_nyt_raw = PythonOperator(
    task_id='store_nyt_raw',
    retries = 3,
    retry_delay = timedelta(minutes=0.2),
    python_callable=store_nyt_raw,
    op_kwargs={'api_key': '{{ ti.xcom_pull(task_ids="get_api_creds") }}',
               'bucket_name': Variable.get('bucket_name'), 
               },
    dag=dag,
)
nyt_transform_raw = PythonOperator(
    task_id='nyt_transform_raw',
    python_callable=nyt_transform_raw,
    op_kwargs={'bucket_name': Variable.get('bucket_name')},
    dag=dag,
)
transfer_s3_to_redshift = PythonOperator(
    task_id='transfer_s3_to_redshift',
    python_callable=s3_to_redshift,
    op_kwargs={'redshift_db_name': Variable.get("redshift_db_name"),
        'redshift_landing_schema': Variable.get("redshift_landing_schema"),
        's3_bucket': Variable.get('bucket_name'),
        'file_key_s3': '{{ ti.xcom_pull(task_ids="nyt_transform_raw") }}',
    },
    dag=dag,
)


run_dbt_reporting_model = BashOperator(
    task_id='run_dbt_reporting_model',
    bash_command=f'cd {dbt_path} && dbt run --target {env}',
    dag=dag,
)

store_nyt_raw >> nyt_transform_raw >> transfer_s3_to_redshift >> run_dbt_reporting_model

if __name__ == "__main__":
    dag.test()
