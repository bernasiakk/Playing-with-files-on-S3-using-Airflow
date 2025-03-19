from datetime import datetime, timedelta
import os
import tempfile

from airflow.models import DAG
from airflow.utils.dates import days_ago

from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import boto3

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# define variables
sink_bucketname = 'sink-bbucket'
current_date = datetime.now().strftime("%d%m%Y")
filename = f'user_purchase1000_{current_date}.csv'
sink_filename = f'silver/{filename}'

# set up functions
def download_from_s3(key, bucket_name, local_path, desired_filename):
    connection = BaseHook.get_connection('s3_conn')
    
    s3 = boto3.client(
        's3',
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password)
    
    full_local_path = os.path.join(local_path, desired_filename)
    s3.download_file(bucket_name, key, full_local_path)
    
    return full_local_path

def transform_sales(input_path, output_path):
    spark = SparkSession.builder \
                .appName("LocalPySparkTest") \
                .master("local[*]") \
                .getOrCreate()
    
    df = spark.read\
					.format("csv")\
					.option("header","true")\
					.option("inferSchema", "true")\
					.load(input_path)
    
    df = df.drop(
        '_c0',
        'invoice_date',
        'country',
        'day_of_week',
        )\
        .select(
            F.expr("uuid()").alias('id'),
            '*'
        )
    
    df.coalesce(1)\
        .write\
        .mode("overwrite")\
        .option("header", "true")\
        .csv(output_path)

def transform_invoices(input_path, output_path):
    spark = SparkSession.builder \
                .appName("LocalPySparkTest") \
                .master("local[*]") \
                .getOrCreate()
    
    df = spark.read\
					.format("csv")\
					.option("header","true")\
					.option("inferSchema", "true")\
					.load(input_path)
      
    df = df.select(
        'invoice_number',
        'invoice_date',
        'day_of_week'
    ).dropDuplicates()

    df.coalesce(1)\
        .write\
        .mode("overwrite")\
        .option("header", "true")\
        .csv(output_path)

def transform_customers(input_path, output_path):
    spark = SparkSession.builder \
                .appName("LocalPySparkTest") \
                .master("local[*]") \
                .getOrCreate()
    
    df = spark.read\
					.format("csv")\
					.option("header","true")\
					.option("inferSchema", "true")\
					.load(input_path)
    
    df = df.select(
        'customer_id',
        'country'
    ).dropDuplicates()
    
    df.coalesce(1)\
        .write\
        .mode("overwrite")\
        .option("header", "true")\
        .csv(output_path)

def upload_to_s3(file_path, bucket_name):
    connection = BaseHook.get_connection('s3_conn')
    s3 = boto3.client(
        's3',
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password)
    
    for root, dirs, files in os.walk(file_path):
        for file in files:
            if file.startswith("part") and file.endswith(".csv"):
                s3.upload_file(os.path.join(root, file), bucket_name, f"gold/{os.path.basename(file_path)}_{current_date}.csv")

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

# Define the DAG
with DAG(
    dag_id='s3_gold',
    description='Gold layer: split silver file into a relational schema and upload to gold folder',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    tags=['s3', 'pyspark']
) as dag:
    
    task_download_from_s3 = PythonOperator(
        task_id="download_from_s3",
        python_callable=download_from_s3,
        op_kwargs={
            'key': sink_filename,
            'bucket_name': sink_bucketname,
            'local_path': tempfile.gettempdir(),
            'desired_filename': filename
        }
    )
    
    transform_sales_task = PythonOperator(
        task_id="transform_sales",
        python_callable=transform_sales,
        op_kwargs={
            'input_path': os.path.join(tempfile.gettempdir(), filename),
            'output_path': os.path.join(tempfile.gettempdir(), 'sales')
        }
    )
    
    transform_invoices_task = PythonOperator(
        task_id="transform_invoices",
        python_callable=transform_invoices,
        op_kwargs={
            'input_path': os.path.join(tempfile.gettempdir(), filename),
            'output_path': os.path.join(tempfile.gettempdir(), 'invoices')
        }
    )
    
    transform_customers_task = PythonOperator(
        task_id="transform_customers",
        python_callable=transform_customers,
        op_kwargs={
            'input_path': os.path.join(tempfile.gettempdir(), filename),
            'output_path': os.path.join(tempfile.gettempdir(), 'customers')
        }
    )
    
    upload_sales_task = PythonOperator(
        task_id="upload_sales",
        python_callable=upload_to_s3,
        op_kwargs={
            'file_path': os.path.join(tempfile.gettempdir(), "sales"),
            'bucket_name': sink_bucketname,
        }
    )
    
    upload_invoices_task = PythonOperator(
        task_id="upload_invoices",
        python_callable=upload_to_s3,
        op_kwargs={
            'file_path': os.path.join(tempfile.gettempdir(), "invoices"),
            'bucket_name': sink_bucketname,
        }
    )
    
    upload_customers_task = PythonOperator(
        task_id="upload_customers",
        python_callable=upload_to_s3,
        op_kwargs={
            'file_path': os.path.join(tempfile.gettempdir(), "customers"),
            'bucket_name': sink_bucketname,
        }
    )



    task_download_from_s3 >> [
        transform_sales_task >> upload_sales_task,
        transform_invoices_task >> upload_invoices_task,
        transform_customers_task >> upload_customers_task
    ]