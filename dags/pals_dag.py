import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Pål",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=1),
    "start_date": datetime.datetime(2023, 2, 1),
}

def first_task():
    print("hei drime velkommen")

def second_task():
    print("dette blir en flott dag!")

with DAG(
    "pals_first_dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:

    run_python_task = PythonOperator(
        task_id="si_velkommen", # This controls what your task name is in the airflow UI 
        python_callable=first_task # This is the function that airflow will run 
    )
    run_second_python_task = PythonOperator(
        task_id="fin_dag", 
        python_callable=second_task 
    )

run_python_task >> run_second_python_task