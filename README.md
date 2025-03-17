# Playing with S3 files using Airflow

This project is a simple data pipeline using Airflow to interact with Amazon S3.

## Architecture
The pipeline is designed to perform the following tasks:
1. Download user purchase data from S3
2. Put the data into a new S3 bucket (/bronze)
3. Transform data
4. Put the data into S3 bucket (/silver)  
<br>

<img src="./images/architecture.png" alt="drawing" width="600"/>

## Setup

### Set up Airflow Environment (using Docker)
This section setup is taken from **[beginner_de_project](https://github.com/josephmachado/beginner_de_project)** repository by josephmachado.

To run locally, you need:

1. [git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
2. [Github account](https://github.com/)
3. [Docker](https://docs.docker.com/engine/install/) with at least 4GB of RAM and [Docker Compose](https://docs.docker.com/compose/install/) v1.27.0 or later

Clone the repo and run the following commands to start the data pipeline:

```bash
git clone https://github.com/bernasiakk/Playing-with-files-on-S3-using-Airflow.git
cd beginner_de_project 
make up
sleep 30 # wait for Airflow to start
make ci # run checks and tests
```

---

### Setting Up an S3 Bucket and IAM User with S3 Admin Access

#### 1. Create an S3 Bucket

1. Open the AWS Management Console.
2. Navigate to **S3**.
3. Click **Create bucket**.
4. Enter a unique **Bucket name**.
5. Choose a **Region**.
6. Configure other settings as needed.
7. Click **Create bucket**.

---

#### 2. Create an IAM User with S3 Admin Access

1. Open the AWS Management Console.
2. Navigate to **IAM** (Identity and Access Management).
3. Go to **Users** → Click **Add users**.
4. Enter a **User name**.
5. Select **Access key - Programmatic access**.
6. Click **Next** → Select **Attach policies directly**.
7. Search for and attach the **AmazonS3FullAccess** policy.
8. Click **Next** → **Create user**.
9. Save the **Access Key ID** and **Secret Access Key** (you will need them in next step).

---

#### 3. Add S3 Access Key in Airflow UI

1. Open your **Airflow UI**.
2. Navigate to **Admin** → **Connections**.
3. Click **+ Add Connection**.
4. Set the fields as follows:
   - **Connection Id**: `s3_conn`
   - **Connection Type**: `Amazon Web Services`
   - **Login**: `<Your Access Key ID>`
   - **Password**: `<Your Secret Access Key>`
5. Click **Save**.

Your DAGs will now use this connection to interact with S3.

---

#### Adjust bucket names in `s3_bronze.py`
Change `source_bucketname` and `sink_bucketname` to your bucket names.

## Usage

Go to [http:localhost:8080](http:localhost:8080) to see the Airflow UI. Username and password are both `airflow`.

To run your code:
1. First, upload file `user_purchase1000_{ddmmyyyy}.csv` to your S3 bucket.
2. Navigate to Airflow >> DAGs >> `s3_bronze` >> `Run DAG`.

You can play with the order of tasks 1 and 2. (i.e., you can run task 2 first to notice that it actually 'waits' for `user_purchase.csv` to be placed in `bucket-A`. Only after `user_purchase.csv` is placed in `bucket-A`, task 1 will start to download it from `bucket-A` and put it in `bucket-B/bronze`.)