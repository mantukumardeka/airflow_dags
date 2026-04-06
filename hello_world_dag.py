from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator  # keep for now

default_args = {
    'owner': 'airflow',
}

def say_hello():
    print("Hello World from Airflow!")

with DAG(
    dag_id='hello_world',
    default_args=default_args,
    description='A simple Hello World Airflow DAG',
    schedule=None,  # 🔥 change this (Airflow 3)
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['hello-world'],
) as dag:

    start = EmptyOperator(task_id='start')

    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=say_hello,
    )

    end = EmptyOperator(task_id='end')

    start >> hello_task >> end