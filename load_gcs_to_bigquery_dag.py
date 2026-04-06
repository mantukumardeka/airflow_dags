from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

import csv
from faker import Faker
import random
import string
from google.cloud import storage
import os

# ================= CONFIG =================
PROJECT_ID = "dbt-proejct2026"
BUCKET_NAME = "random_landing_data"
GCS_FILE = "employee_data.csv"

BIGQUERY_DATASET = "handson2"
BIGQUERY_TABLE = "example"

# ✅ Local Mac path (FIXED)
file_path = os.path.expanduser('~/airflow/logs/employee_data.csv')

fake = Faker()
password_characters = string.ascii_letters + string.digits


# ================= STEP 1 =================
def generate_csv():
    print("Generating CSV file...")

    # ensure directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, mode='w', newline='') as file:
        fieldnames = [
            'first_name', 'last_name', 'job_title', 'department',
            'email', 'address', 'phone_number', 'salary', 'password'
        ]

        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        for _ in range(100):
            writer.writerow({
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "job_title": fake.job(),
                "department": fake.job(),
                "email": fake.email(),
                "address": fake.city(),
                "phone_number": fake.phone_number(),
                "salary": fake.random_number(digits=5),
                "password": ''.join(random.choice(password_characters) for _ in range(8))
            })

    print(f"✅ File created at: {file_path}")


# ================= STEP 2 =================
def upload_to_gcs():
    print("Uploading file to GCS...")

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(GCS_FILE)

    blob.upload_from_filename(file_path)

    print(f"✅ Uploaded to bucket: {BUCKET_NAME}")


# ================= DAG =================
with DAG(
    dag_id="gcp_full_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gcp", "etl", "bigquery"],
) as dag:

    generate_task = PythonOperator(
        task_id="generate_csv",
        python_callable=generate_csv,
    )

    upload_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
    )

    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=[GCS_FILE],
        destination_project_dataset_table=f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        gcp_conn_id="google_cloud_default",  # ✅ REQUIRED
    )

    # DAG flow
    generate_task >> upload_task >> load_to_bigquery