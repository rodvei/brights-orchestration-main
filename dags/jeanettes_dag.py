import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Jeanette",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=1),
    "start_date": datetime.datetime(2023, 2, 5),
}

def run():
    print('test run function')

with DAG(
    "jeanette_dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:

    run_python_task = PythonOperator(
        task_id="run_some_python_task", # This controls what your task name is in the airflow UI 
        python_callable=run # This is the function that airflow will run 
    )