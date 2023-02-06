import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Sara',
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=2),
    'start_date': datetime.datetime(2023, 2, 6)
}

def my_first_task():
    print('Hello World')

def my_second_task():
    print('Second task')

with DAG(
    'sara_dag',
    default_args = default_args,
    schedule_interval = '@daily'
) as dag:
    hello_world = PythonOperator(
        task_id = 'hello_world',
        python_callable = my_first_task
    )
    second_task = PythonOperator(
        task_id = 'second_task',
        python_callable = my_second_task
    )

hello_world>>second_task