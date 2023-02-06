import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "GlenRoger",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(2022, 8, 21),
}


def run(**kwargs):
    # This function will be called by airflow, specified by python_callable in teh PythonOperator
    print(kwargs)

def howdy_world(**kwargs):
    print("Howdy World!")
    print(kwargs.keys())

with DAG(
    dag_id ="gr_test_dag",
    description="This is gr's first test dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:

    run_python_task = PythonOperator(
        task_id="gr_howdy_world", # This controls what your task name is in the airflow UI 
        python_callable=howdy_world # This is the function that airflow will run 
    )