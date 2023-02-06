import requests
import http.client, urllib.parse
import datetime
import json
from airflow import DAG
from airflow.operators.python import PythonOperator

apikey = "acf08f2517abc3baa8e1608b66fcb52e"

default_args = {
    "owner": "Ozzyz",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(2023, 1, 1),
}
print("hello")

def api_call():
    params = {
        'access_key': apikey,
        'categories': '-general,-sports',
        'limit': 10,
        'countries': 'gb'
    }

    response = requests.get("http://api.mediastack.com/v1/news", params=params).json()
    
    print("hello world")
    
with DAG(
    "fehmmi-dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:

    run_python_task = PythonOperator(
        task_id="api_call", # This controls what your task name is in the airflow UI 
        python_callable=api_call # This is the function that airflow will run 
    )


    