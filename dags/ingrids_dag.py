import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "IngridMorch",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(2022, 8, 21),
}


def run(**kwargs):
    # This function will be called by airflow, specified by python_callable in teh PythonOperator
    print(kwargs)

def hello_world():
    print('Ingrid says hello world')

with DAG(
    "hello_world",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:

    run_python_task = PythonOperator(
        task_id="hello_world", # This controls what your task name is in the airflow UI 
        python_callable=hello_world # This is the function that airflow will run 
    )