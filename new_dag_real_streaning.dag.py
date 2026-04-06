from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime
from faker import Faker
import random
from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound
import csv
import io

# =========================
# CONFIG
# =========================
PROJECT_ID = "dbt-proejct2026"
BUCKET_NAME = "random_landing_data"

DATASET = "handson1"
TABLE = "sales_data"
TRANSFORMED = "sales_data_transform"
AGG = "sales_product_summary"

PRODUCTS = ["Mobile","Laptop","Tablet","Smartwatch","Headphones"]

# =========================
# GENERATE DATA
# =========================
def generate_streaming_data(**context):
    fake = Faker()
    bq = bigquery.Client()

    try:
        query = f"""
            SELECT COALESCE(MAX(order_id),0) as max_id
            FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        """
        result = bq.query(query).result()
        max_id = list(result)[0].max_id
    except NotFound:
        max_id = 0

    start_id = max_id + 1
    num_orders = 10

    # ✅ unique file per run
    file_name = f"stream_{context['ts_nodash']}.csv"

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow(["order_id","customer_name","order_amount","order_date","product"])

    for i in range(start_id, start_id + num_orders):
        writer.writerow([
            i,
            fake.name(),
            round(random.uniform(100,5000),2),
            datetime.today().date(),
            random.choice(PRODUCTS)
        ])

    storage.Client().bucket(BUCKET_NAME).blob(file_name)\
        .upload_from_string(output.getvalue(), content_type="text/csv")

    print(f"✅ Uploaded {num_orders} rows starting from {start_id}")

# =========================
# DAG
# =========================
with DAG(
    dag_id="mantus_realtime_dag",
    start_date=datetime(2024,1,1),
    schedule="*/5 * * * *",   # every 5 mins
    catchup=False,
    tags=["gcp","etl"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # =========================
    # TASK 1: GENERATE
    # =========================
    generate = PythonOperator(
        task_id="generate_stream",
        python_callable=generate_streaming_data,
    )

    # =========================
    # TASK 2: LOAD (FIXED)
    # =========================
    load = BigQueryInsertJobOperator(
        task_id="load",
        configuration={
            "load": {
                # ✅ load ONLY current run file
                "sourceUris": [
                    f"gs://{BUCKET_NAME}/stream_{{{{ ts_nodash }}}}.csv"
                ],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET,
                    "tableId": TABLE,
                },
                "sourceFormat": "CSV",
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
                "skipLeadingRows": 1,
                "schema": {
                    "fields": [
                        {"name": "order_id", "type": "INTEGER"},
                        {"name": "customer_name", "type": "STRING"},
                        {"name": "order_amount", "type": "FLOAT"},
                        {"name": "order_date", "type": "DATE"},
                        {"name": "product", "type": "STRING"},
                    ]
                }
            }
        },
    )

    # =========================
    # TASK 3: TRANSFORM
    # =========================
    transform = BigQueryInsertJobOperator(
        task_id="transform",
        configuration={
            "query": {
                "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.{TRANSFORMED}` AS
                SELECT 
                    order_id,
                    customer_name,
                    order_amount,
                    order_date,
                    product,
                    CASE
                        WHEN order_amount < 500 THEN 'Low'
                        WHEN order_amount < 2000 THEN 'Medium'
                        ELSE 'High'
                    END AS sales_category,
                    CURRENT_TIMESTAMP() AS load_time
                FROM `{PROJECT_ID}.{DATASET}.{TABLE}`;
                """,
                "useLegacySql": False,
            }
        },
    )

    # =========================
    # TASK 4: AGGREGATE
    # =========================
    aggregate = BigQueryInsertJobOperator(
        task_id="aggregate",
        configuration={
            "query": {
                "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.{AGG}` AS
                SELECT
                    product,
                    COUNT(*) AS total_orders,
                    SUM(order_amount) AS total_sales,
                    AVG(order_amount) AS avg_order_value,
                    MAX(load_time) AS last_updated
                FROM `{PROJECT_ID}.{DATASET}.{TRANSFORMED}`
                GROUP BY product
                ORDER BY total_sales DESC;
                """,
                "useLegacySql": False,
            }
        },
    )

    # =========================
    # FLOW
    # =========================
    start >> generate >> load >> transform >> aggregate >> end