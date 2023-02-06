import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Mats",
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=1),
    "start_date": datetime.datetime(2023, 1, 2),
}

def cry():
    print("Jeg ville drikke øl, men ble trist og syk istedenfor. Bu")

with DAG(
    "mats_dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:

    run_crying = PythonOperator(
        task_id="øl_grining", # This controls what your task name is in the airflow UI 
        python_callable=cry # This is the function that airflow will run 
    )