import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Kristoffer",
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=2),
    "start_date": datetime.datetime(2023, 2, 1),
}

def my_first_task():
    print('hello world')

with DAG(
    "kristoffers_dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:
    hello_world = PythonOperator(
        task_id="hello world", # This controls what your task name is in the airflow UI 
        python_callable=my_first_task # This is the function that airflow will run 
    )