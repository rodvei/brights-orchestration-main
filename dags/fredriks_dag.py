import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "freddie",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(2022, 8, 21),
}


def bonjour_le_monde():
    # This function will be called by airflow, specified by python_callable in teh PythonOperator
    print('Bonjour le monde!')

with DAG(
    "example_dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:

    run_python_task = PythonOperator(
        task_id="bonjour_le_monde", # This controls what your task name is in the airflow UI 
        python_callable=bonjour_le_monde # This is the function that airflow will run 
    )