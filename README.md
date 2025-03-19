# S3 Data Pipeline with Airflow

This project demonstrates a data pipeline using Apache Airflow to process user purchase data from Amazon S3.

## Architecture

The pipeline follows this architecture:

<img src="./images/architecture_new.png" alt="Pipeline Architecture" width="300"/>
<br><br><br>
Files written into the `gold` layer follow this schema:

<img src="./images/relational_db.png" alt="Schema Diagram" width="600"/>

## Setup

### Prerequisites

- [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
- [GitHub Account](https://github.com/)
- [Docker](https://docs.docker.com/engine/install/) and [Docker Compose](https://docs.docker.com/compose/install/) (4GB+ RAM recommended)
- AWS account with S3 and IAM access (free trial is fine; no worries if you used up free trial, costs will be minimal)

### 1. Airflow Environment (Docker)

This setup is based on [beginner_de_project](https://github.com/josephmachado/beginner_de_project) by josephmachado.

1. Clone the repository:

    ```bash
    git clone https://github.com/bernasiakk/Playing-with-files-on-S3-using-Airflow.git
    cd Playing-with-files-on-S3-using-Airflow
    ```

2. Start Airflow:

    ```bash
    make up
    sleep 30 # Allow time for Airflow to initialize
    make ci # Run checks and tests
    ```

### 2. AWS Setup

#### a. Create S3 Buckets

1. Navigate to S3 in the AWS Console.
2. Create two buckets: one for the source data and another for the "bronze" and "silver" layers.
3. Note the bucket names for later configuration.

#### b. Create an IAM User

1. Navigate to IAM in the AWS Console.
2. Create a new user with "Programmatic access."
3. Attach the `AmazonS3FullAccess` policy.
4. Save the Access Key ID and Secret Access Key.

### 3. Airflow Setup

#### a. Configure Airflow Connection

1. Open the Airflow UI (http://localhost:8080, login: `airflow`/`airflow`).
2. Navigate to `Admin` -> `Connections`.
3. Add a new connection:
    - **Connection ID:** `s3_conn`
    - **Connection Type:** `Amazon Web Services`
    - **Login:** `<Your Access Key ID>`
    - **Password:** `<Your Secret Access Key>`
4. Save the connection.

### 4. Code Setup

Update `source_bucketname` and `sink_bucketname` in `s3_bronze.py`, `s3_silver.py`, and `s3_gold.py` with your S3 bucket names.

## Usage

1. Upload `user_purchase1000_{ddmmyyyy}.csv` to your source S3 bucket.
2. Open the Airflow UI and navigate to `DAGs` -> `s3_bronze`.
3. Trigger the DAG (`Run DAG`).
4. All other dags (`s3_silver` and `s3_gold`) will trigger automatically (via trigger tasks)

**Note:** The DAG will wait for the data file to be available in the source bucket before proceeding. You can test this behavior by triggering the DAG first and uploading the file afterward.

## Future Improvements (TODO)

1. Store `source_bucketname` and `sink_bucketname` as Airflow variables.
2. Implement data quality checks:
   - Ensure all items from a given `invoice_number` are processed on the same day:
     ```python
     grouped_df = df.groupby('invoice_number')\
         .agg(F.count_distinct(F.col('invoice_date')).alias('unique_dates'))\
         .filter(F.col('unique_dates') != 1)
     ```
   - Verify that each `stock_code` corresponds to a single `detail`:
     ```python
     grouped_df = df.groupby('stock_code')\
         .agg(F.count_distinct(F.col('detail')).alias('unique_details'))\
         .filter(F.col('unique_details') != 1)
     ```

