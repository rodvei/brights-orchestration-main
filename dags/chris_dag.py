

import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Chris",
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=2),
    "start_date": datetime.datetime(2023, 2, 1),
}

def my_first_task():
    print('Ikkje så alt for lenge til lunsj')

def my_second_task():
    print('Har glemt å pakke lunch :(')

with DAG(
    "chris_dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:
    lunch_msg_1 = PythonOperator(
        task_id="lunch_msg_1", # This controls what your task name is in the airflow UI 
        python_callable=my_first_task # This is the function that airflow will run 
    )
    lunch_msg_2 = PythonOperator(
        task_id="lunch_msg_2", # This controls what your task name is in the airflow UI 
        python_callable=my_second_task # This is the function that airflow will run 
    )

lunch_msg_1>>lunch_msg_2