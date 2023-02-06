import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Mats",
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=1),
    "start_date": datetime.datetime(2023, 1, 2),
}

def print_hello():
    print("Hello guys")

with DAG(
    "mats_dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:

    print_hello = PythonOperator(
        task_id="print_hello", # This controls what your task name is in the airflow UI 
        python_callable=print_hello # This is the function that airflow will run 
    )