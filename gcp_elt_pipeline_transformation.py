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
GCS_PATH = "sales_data.csv"

BIGQUERY_DATASET = "handson2"
BIGQUERY_TABLE = "example"
TRANSFORMED_TABLE = "example_transform"

# =========================
# SCHEMA
# =========================
schema_fields = [
    {"name": "order_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "customer_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "order_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "order_date", "type": "DATE", "mode": "NULLABLE"},
    {"name": "product", "type": "STRING", "mode": "NULLABLE"},
]

# =========================
# SAFE FUNCTION
# =========================
def generate_and_upload_sales_data(bucket_name, gcs_path, num_orders=200):
    fake = Faker()

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow(["order_id", "customer_name", "order_amount", "order_date", "product"])

    for i in range(1, num_orders + 1):
        writer.writerow([
            i,
            fake.name(),
            round(random.uniform(10.0, 1000.0), 2),
            fake.date_between(start_date='-30d', end_date='today'),
            fake.word()
        ])

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    blob.upload_from_string(output.getvalue(), content_type='text/csv')

    print(f"Uploaded to gs://{bucket_name}/{gcs_path}")


# =========================
# DAG
# =========================
with DAG(
    dag_id="sales_orders_to_bigquery_with_transformation",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gcp"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    generate_sales_data = PythonOperator(
        task_id="generate_sales_data",
        python_callable=generate_and_upload_sales_data,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "gcs_path": GCS_PATH,
        },
    )

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
                "writeDisposition": "WRITE_TRUNCATE",
                "skipLeadingRows": 1,
                "schema": {"fields": schema_fields},
            }
        },
    )

    # ✅ COMBINED TRANSFORMATION (NO EXTRA TASK → NO CRASH)
    transform_and_filter = BigQueryInsertJobOperator(
        task_id="transform_and_filter",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.{TRANSFORMED_TABLE}` AS
                SELECT 
                    order_id,
                    customer_name,
                    order_amount,
                    CASE
                        WHEN order_amount < 100 THEN 'Small'
                        WHEN order_amount BETWEEN 100 AND 500 THEN 'Medium'
                        ELSE 'Large'
                    END AS order_category,
                    order_date,
                    product,
                    CURRENT_TIMESTAMP() AS load_timestamp
                FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}`;

                CREATE OR REPLACE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.large_orders` AS
                SELECT *
                FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}`
                WHERE order_amount >= 500;
                """,
                "useLegacySql": False,
            }
        },
    )

    start >> generate_sales_data >> load_to_bigquery >> transform_and_filter >> end