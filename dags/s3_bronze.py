from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.utils.dates import days_ago

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator


# Specify default arguments for Airflow
default_args = {
    'owner': 'szymon',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'aws_conn_id': 's3_conn'
}

source_bucketname = 'bernasiakkbucket'
sink_bucketname = 'sink-bbucket'

current_date = datetime.now().strftime("%d%m%Y")
source_filename = f'user_purchase1000_{current_date}.csv'
sink_filename = f'bronze/{source_filename}'

# Define the DAG
with DAG(
    dag_id='s3_bronze',
    description='Bronze layer: Copy and transform files from source to sink bucket',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    tags=['s3', 'pyspark']
) as dag:
    
    s3_sensor = S3KeySensor(
        task_id='s3_file_check',
        poke_interval=5,
        timeout=180,
        soft_fail=False,
        bucket_key=source_filename,
        bucket_name=source_bucketname,
    )
    
    copy_task = S3CopyObjectOperator(
        task_id='copy_s3_object',
        source_bucket_name=source_bucketname,
        source_bucket_key=source_filename,
        dest_bucket_name=sink_bucketname,
        dest_bucket_key=sink_filename,
    )
    
    trigger_s3_silver = TriggerDagRunOperator(
        task_id='trigger_s3_silver',
        trigger_dag_id='s3_silver',
    )

    s3_sensor >> copy_task >> trigger_s3_silver