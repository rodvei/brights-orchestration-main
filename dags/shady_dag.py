import datetime

from google.cloud import storage

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="shady_dag",
    start_date=datetime.datetime(2023, 2, 1),
    schedule_interval="@weekly"
) as dag:
    
    def list_buckets():
        storage_client = storage.Client()

        print(storage_client.list_buckets())

    t = BashOperator(
        task_id="sys_info",
        bash_command="echo hello world!"
    )
    
    t2 = PythonOperator(
        task_id="list_buckets",
        python_callable=list_buckets
    )
    
    t >> t2
