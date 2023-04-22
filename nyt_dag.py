
import os
from datetime import timedelta
from airflow import DAG
import yaml
from datetime import datetime
from nyt_extract import store_nyt_raw 
from nyt_transform import nyt_transform_raw
from s3_to_redshift import s3_to_redshift
from airflow.operators.python import PythonOperator

from airflow.models import Variable

#with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml"), "r") as f:
#    config = yaml.load(f, Loader=yaml.FullLoader)

def load_config() -> dict:
    """Load configuration from config.yaml file"""
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml"), "r") as f:
        try:
            config = yaml.load(f, Loader=yaml.FullLoader)
        except yaml.YAMLError as e:
            raise ValueError(f"Error loading configuration file: {str(e)}")
    return config

def set_airflow_vars(config: dict, env: str):
    
    Variable.set('AIRFLOW_ENVIRONMENT', config['env'])
    Variable.set('redshift_landing_schema', config['redshift_landing_schema'])
    Variable.set('bucket_name', config[f'bucket_name_{env}'])
    Variable.set('redshift_db_name', config[f'redshift_db_name_{env}'])
    print(Variable.get('redshift_db_name'))
    Variable.set('full_redshift_dump', config[f'full_redshift_dump'])
    return 

config = load_config()
allowed_values = config["allowed_env_values"]
env = config['env']
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

print(Variable.get('redshift_db_name'))
store_nyt_raw = PythonOperator(
    task_id='store_nyt_raw',
    python_callable=store_nyt_raw,
    op_kwargs={'api_key': '{{ ti.xcom_pull(task_ids="get_api_creds") }}',
               'bucket_name': Variable.get('bucket_name'), 
               },
    dag=dag,
)
print(Variable.get('redshift_db_name'))
nyt_transform_raw = PythonOperator(
    task_id='nyt_transform_raw',
    python_callable=nyt_transform_raw,
    op_kwargs={'bucket_name': Variable.get('bucket_name')},
    dag=dag,
)
print(Variable.get('redshift_db_name'))
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



store_nyt_raw >> nyt_transform_raw >> transfer_s3_to_redshift

if __name__ == "__main__":
    dag.test()
