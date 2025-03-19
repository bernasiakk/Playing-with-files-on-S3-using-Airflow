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


# def read_parquet_from_s3(s3_path: str):
#     """
#     Reads a Parquet file from S3 into a PySpark DataFrame.
    
#     :param s3_path: S3 path to the Parquet file (e.g., "s3a://my-bucket/path/to/file.parquet")
#     :return: PySpark DataFrame
#     """
#     # Get AWS credentials from Airflow connection
#     connection = BaseHook.get_connection('s3_conn')
#     aws_access_key = connection.login
#     aws_secret_key = connection.password
    
#     # Initialize Spark session with S3 support
#     spark = SparkSession.builder \
#         .appName("ReadS3Parquet") \
#         .config("spark.hadoop.fs.s3.access.key", aws_access_key) \
#         .config("spark.hadoop.fs.s3.secret.key", aws_secret_key) \
#         .config("spark.hadoop.fs.s3.path.style.access", "true") \
#         .getOrCreate()
    
#     # Read the Parquet file
#     df = spark.read.parquet(s3_path)
    
#     return df

def download_from_s3(key: str, bucket_name: str, local_path: str, desired_filename: str) -> str:
    connection = BaseHook.get_connection('s3_conn')
    
    s3 = boto3.client(
        's3',
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password)
    
    full_local_path = os.path.join(local_path, desired_filename)
    s3.download_file(bucket_name, key, full_local_path)
    
    return full_local_path

# def read_file_as_df(input_path: str) -> str:
#     spark = SparkSession.builder \
#                     .appName("LocalPySparkTest") \
#                     .master("local[*]") \
#                     .getOrCreate()
    
#     df = spark.read.parquet(input_path)

#     return df

def transform_file(input_path: str) -> str:
    # spark = SparkSession.builder \
    #                 .appName("LocalPySparkTest") \
    #                 .master("local[*]") \
    #                 .getOrCreate()
    
    # df = spark.read.csv(input_path, header=True, inferSchema=True)

    # df = df.withColumns({
    #     "total_amount": F.round(df["quantity"]*df["unit_price"], 2),
    #     "day_of_week": F.dayofweek(df["invoice_date"])
    # })
    
    # temp_dir = tempfile.gettempdir()
    # output_path = os.path.join(temp_dir, "transformed")
    
    # df.write.mode('overwrite').parquet(output_path)
    from pyspark.sql import functions as F
    
    spark = SparkSession.builder \
                .appName("LocalPySparkTest") \
                .master("local[*]") \
                .getOrCreate()
    
    df = spark.read\
					.format("csv")\
					.option("header","true")\
					.option("inferSchema", "true")\
					.load(input_path)
    
    df_sales = df.drop(
        '_c0',
        'invoice_date',
        'country',
        'day_of_week',
        )\
        .select(
            F.expr("uuid()").alias('id'),
            '*'
        )
    
    df_invoices = df.select(
        'invoice_number',
        'invoice_date',
        'day_of_week'
    ).dropDuplicates()
    
    df_customers = df.select(
        'customer_id',
        'country'
    ).dropDuplicates()
    
    return df_sales, df_invoices, df_customers



def upload_to_s3(file_path: str, bucket_name: str, s3_key: str):
    connection = BaseHook.get_connection('s3_conn')
    s3 = boto3.client(
        's3',
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password)
    
    for root, dirs, files in os.walk(file_path):
        for file in files:
            if file.startswith("part") and file.endswith(".csv"):
                s3.upload_file(os.path.join(root, file), bucket_name, f"{s3_key}")

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
sink_filename = f'silver/{filename}'
s3_transformed_dirs = {
                       'TODO': 'TODO'}

# Define the DAG
with DAG(
    dag_id='s3_gold',
    description='Gold layer: split silver file into a relational schema and upload to gold folder',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    tags=['s3', 'pyspark']
) as dag:
    
    # task_read_parquet_from_s3 = PythonOperator(
    #     task_id="read_parquet_from_s3",
    #     python_callable=read_parquet_from_s3,
    #     op_kwargs={
    #         's3_path': 's3://sink-bbucket/silver/17032025/'
    #     }
    # )
    
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
    
    # task_read_file_as_df = PythonOperator(
    #     task_id="read_file_as_df",
    #     python_callable=read_file_as_df,
    #     op_kwargs={
    #         'input_path': os.path.join(tempfile.gettempdir(), filename)
    #     }
    # )
    
    task_transform = PythonOperator(
        task_id="transform_file",
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
            's3_key': 's3_transformed_dir'
        }
    )

    # wait_for_s3_bronze_DAG >> 
    # task_read_s3_file >> transform_task >> upload_task
    # task_read_parquet_from_s3
    task_download_from_s3 >> task_transform