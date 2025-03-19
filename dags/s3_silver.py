from datetime import datetime, timedelta
import os
import tempfile

from airflow.models import DAG
from airflow.utils.dates import days_ago

from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import boto3

from airflow.sensors.external_task_sensor import ExternalTaskSensor

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def download_from_s3(key: str, bucket_name: str, local_path: str, desired_filename: str) -> str:
    connection = BaseHook.get_connection('s3_conn')
    
    s3 = boto3.client(
        's3',
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password)
    
    full_local_path = os.path.join(local_path, desired_filename)
    s3.download_file(bucket_name, key, full_local_path)
    
    return full_local_path

def transform_file(input_path: str) -> str:
    spark = SparkSession.builder \
                    .appName("LocalPySparkTest") \
                    .master("local[*]") \
                    .getOrCreate()
    
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    df = df.withColumns({
        "total_amount": F.round(df["quantity"]*df["unit_price"], 2),
        "day_of_week": F.dayofweek(df["invoice_date"])
    })
    
    temp_dir = tempfile.gettempdir()
    output_path = os.path.join(temp_dir, "transformed")
    
    df.coalesce(1)\
        .write\
        .mode("overwrite")\
        .option("header", "true")\
        .csv(output_path)


def upload_to_s3(file_path: str, bucket_name: str, s3_key: str):
    connection = BaseHook.get_connection('s3_conn')
    s3 = boto3.client(
        's3',
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password)
    
    for root, dirs, files in os.walk(file_path):
        for file in files:
            s3.upload_file(os.path.join(root,file),bucket_name,f"{s3_key}/{file}")

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

sink_bucketname = 'sink-bbucket'
current_date = datetime.now().strftime("%d%m%Y")
filename = f'user_purchase1000_{current_date}.csv'
sink_filename = f'bronze/{filename}'
s3_transformed_dir = f'silver/{current_date}'

# Define the DAG
with DAG(
    dag_id='s3_silver',
    description='Silver layer: transform file and upload to silver folder',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    tags=['s3', 'pyspark']
) as dag:
        
    task_download_from_s3 = PythonOperator(
        task_id='download_from_s3',
        python_callable=download_from_s3,
        op_kwargs={
            'key': sink_filename,
            'bucket_name': sink_bucketname,
            'local_path': tempfile.gettempdir(),
            'desired_filename': filename
        }
    )
    
    transform_task = PythonOperator(
        task_id="transform_file_task",
        python_callable=transform_file,
        op_kwargs={
            'input_path': os.path.join(tempfile.gettempdir(), filename)
        }
    )
    
    upload_task = PythonOperator(
        task_id="upload_transformed_file",
        python_callable=upload_to_s3,
        op_kwargs={
            'file_path': os.path.join(tempfile.gettempdir(), "transformed"),
            'bucket_name': sink_bucketname,
            's3_key': s3_transformed_dir
        }
    )

    task_download_from_s3 >> transform_task >> upload_task