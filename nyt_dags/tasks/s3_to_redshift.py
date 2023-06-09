import redshift_connector
import logging
from airflow.hooks.base import BaseHook
from airflow.models import Variable

logger = logging.getLogger("airflow.task")

def get_copy_query(s3_bucket, redshift_landing_schema, full_file_key):
    return f"""
        COPY {redshift_landing_schema}.NYT_TOP_ARTICLES 
        FROM 's3://{s3_bucket}/{full_file_key}' 
        credentials 'aws_iam_role=arn:aws:iam::872959048383:role/my-rredshift-role-felix'
        csv
        IGNOREHEADER 1
        FILLRECORD
        EMPTYASNULL
        BLANKSASNULL
        dateformat 'YYYY-MM-DD';
    """


def s3_to_redshift(redshift_db_name,redshift_landing_schema, s3_bucket, file_key_s3) -> str:
    ''' Runs a Redshift copy command to load S3 dump into Redshift '''
    logger.info("Starting s3_to_redshift transfer")
    full_file_key = 'processed/toparticles_' if Variable.get('full_redshift_dump') == 'true' else file_key_s3
    
    rs_conn = BaseHook.get_connection('fs_redshift_connection')
    with redshift_connector.connect(host=rs_conn.host, port=rs_conn.port,
        database=redshift_db_name, user=rs_conn.login, password=rs_conn.password
    ) as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            query = get_copy_query(s3_bucket,redshift_landing_schema, full_file_key)
            cursor.execute(query)
    logger.info(f"Lifted s3 object {full_file_key} from {s3_bucket} into Redhsift Schema "
                 f"{redshift_landing_schema}.")
    return
