import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Kristoffer",
    "retries": 5,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(2023, 2, 1),
}


def run(**kwargs):
    # This function will be called by airflow, specified by python_callable in teh PythonOperator
    # print(kwargs)
    print('This is Kristoffer first dag!!')

with DAG(
    dag_id="kristoffer_dag_1",
    description="This is our first test dag",
    default_args=default_args,
    schedule_interval="@daily", #None, @hourly, @weekly, @monthly, @yearly,...
) as dag:

    run_python_task = PythonOperator(
        task_id="test_kristoffers_first_task", # This controls what your task name is in the airflow UI 
        python_callable=run # This is the function that airflow will run 
    )
