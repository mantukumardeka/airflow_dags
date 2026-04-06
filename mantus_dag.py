from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from faker import Faker
import random
from google.cloud import storage
import csv
import io

# =========================
# CONFIG
# =========================
PROJECT_ID = "dbt-proejct2026"
BUCKET_NAME = "random_landing_data"
GCS_PATH = "newfile.csv"

BIGQUERY_DATASET = "handson1"
BIGQUERY_TABLE = "sales_data"
TRANSFORMED_TABLE = "sales_data_transform"

# =========================
# PRODUCT LIST (REALISTIC)
# =========================
PRODUCTS = [
    "Mobile", "Laptop", "Tablet", "Smartwatch", "Headphones",
    "Camera", "Printer", "Monitor", "Keyboard", "Mouse"
]

# =========================
# SCHEMA
# =========================
schema_fields = [
    {"name": "order_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "customer_name", "type": "STRING"},
    {"name": "order_amount", "type": "FLOAT"},
    {"name": "order_date", "type": "DATE"},
    {"name": "product", "type": "STRING"},
]

# =========================
# GENERATE + UPLOAD (50 ROWS DAILY)
# =========================
def generate_and_upload_sales_data(bucket_name, gcs_path, num_orders=50):
    fake = Faker()

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow(["order_id", "customer_name", "order_amount", "order_date", "product"])

    for i in range(1, num_orders + 1):
        writer.writerow([
            i,
            fake.name(),
            round(random.uniform(100.0, 5000.0), 2),
            datetime.today().date(),  # ✅ today's date
            random.choice(PRODUCTS)   # ✅ meaningful products
        ])

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    blob.upload_from_string(output.getvalue(), content_type='text/csv')

    print(f"✅ Uploaded to gs://{bucket_name}/{gcs_path}")


# =========================
# DAG
# =========================
with DAG(
    dag_id="mantus_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",   # ✅ runs daily
    catchup=False,
    tags=["gcp", "daily", "etl"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Task 1: Generate Data
    generate_sales_data = PythonOperator(
        task_id="generate_sales_data",
        python_callable=generate_and_upload_sales_data,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "gcs_path": GCS_PATH,
        },
    )

    # Task 2: Load to BigQuery (APPEND)
    load_to_bigquery = BigQueryInsertJobOperator(
        task_id="load_to_bigquery",
        gcp_conn_id="google_cloud_default",
        configuration={
            "load": {
                "sourceUris": [f"gs://{BUCKET_NAME}/{GCS_PATH}"],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": BIGQUERY_TABLE,
                },
                "sourceFormat": "CSV",
                "writeDisposition": "WRITE_APPEND",  # ✅ keep adding daily data
                "skipLeadingRows": 1,
                "schema": {"fields": schema_fields},
            }
        },
    )

    # Task 3: Transform
    transform_data = BigQueryInsertJobOperator(
        task_id="transform_data",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.{TRANSFORMED_TABLE}` AS
                SELECT 
                    *,
                    CASE
                        WHEN order_amount < 500 THEN 'Low'
                        WHEN order_amount BETWEEN 500 AND 2000 THEN 'Medium'
                        ELSE 'High'
                    END AS sales_category,
                    CURRENT_TIMESTAMP() AS load_time
                FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}`;
                """,
                "useLegacySql": False,
            }
        },
    )

    start >> generate_sales_data >> load_to_bigquery >> transform_data >> end