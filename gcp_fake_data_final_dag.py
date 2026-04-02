from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

import csv
from faker import Faker
import random
import string
from google.cloud import storage
import os

# Config
bucket_name = 'random_landing_data'
file_path = '/opt/airflow/logs/employee_data.csv'   # ✅ IMPORTANT FIX

fake = Faker()
password_characters = string.ascii_letters + string.digits + 'm'


# ✅ Step 1: Generate CSV (your logic preserved)
def generate_csv():
    print("Starting file generation...")

    num_employees = 100

    with open(file_path, mode='w', newline='') as file:
        fieldnames = [
            'first_name', 'last_name', 'job_title', 'department',
            'email', 'address', 'phone_number', 'salary', 'password'
        ]

        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        for _ in range(num_employees):
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

    print("File created successfully!")
    print("Files in logs folder:", os.listdir("/opt/airflow/logs"))


# ✅ Step 2: Upload to GCS (your function reused)
def upload_to_gcs():
    print("Starting upload to GCS...")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob('employee_data.csv')

    blob.upload_from_filename(file_path)

    print(f'File uploaded to GCS bucket: {bucket_name}')


# ✅ DAG definition
with DAG(
    dag_id="gcp_fake_data_pipeline",
    start_date=datetime(2023, 1, 1),
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