import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Ozzyz",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(2022, 8, 21),
}


def run(**kwargs):
    # This function will be called by airflow, specified by python_callable in teh PythonOperator
    # kwargs['ds']
    print(kwargs)

with DAG(
    "example_dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:

    run_python_task = PythonOperator(
        task_id="run_some_python_task", # This controls what your task name is in the airflow UI 
        python_callable=run # This is the function that airflow will run 
    )