from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
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

file_path = "/opt/airflow/dags/employee_data.csv"   # ✅ persistent path

fake = Faker()
password_characters = string.ascii_letters + string.digits + 'm'


# ================= STEP 1 =================
def generate_csv():
    print("Generating file...")

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

    print("File created:", file_path)


# ================= STEP 2 =================
def upload_to_gcs():
    print("Uploading to GCS...")

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(GCS_FILE)

    blob.upload_from_filename(file_path)

    print("Upload complete!")


# ================= DAG =================
with DAG(
    dag_id="gcp_full_etl_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gcp", "bigquery"],
) as dag:

    generate_task = PythonOperator(
        task_id="generate_csv",
        python_callable=generate_csv,
    )

    upload_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
    )

    # ================= STEP 3 (NEW) =================
    load_to_bq = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=[GCS_FILE],
        destination_project_dataset_table=f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",  # overwrite
        autodetect=True,
    )

    # Pipeline flow
    generate_task >> upload_task >> load_to_bq