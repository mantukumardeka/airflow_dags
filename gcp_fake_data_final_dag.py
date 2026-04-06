from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

import csv
from faker import Faker
import random
import string
from google.cloud import storage
import os

# ✅ Config
bucket_name = 'random_landing_data'

# ✅ Correct path for LOCAL (Mac)
file_path = os.path.expanduser('~/airflow/logs/employee_data.csv')

fake = Faker()
password_characters = string.ascii_letters + string.digits


# ✅ Step 1: Generate CSV
def generate_csv():
    print("Starting file generation...")

    # Ensure directory exists
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


# ✅ Step 2: Upload to GCS
def upload_to_gcs():
    print("Starting upload to GCS...")

    # Uses GOOGLE_APPLICATION_CREDENTIALS
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob('employee_data.csv')

    blob.upload_from_filename(file_path)

    print(f"✅ File uploaded to GCS bucket: {bucket_name}")


# ✅ DAG definition
with DAG(
    dag_id="gcp_fake_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gcp", "etl"],
) as dag:

    generate_task = PythonOperator(
        task_id="generate_csv",
        python_callable=generate_csv,
    )

    upload_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
    )

    generate_task >> upload_task