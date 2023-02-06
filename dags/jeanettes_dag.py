import datetime as dt
import requests
import csv
import os
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Jeanette",
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=1),
    "start_date": dt.datetime(2023, 2, 5),
}

def run():
    bucket_name = 'brights_bucket_1'
    today = dt.datetime.today()
    date = f"{today.day}.{today.month}.{today.year}"
    day = today.day
    month = today.month
    blob_name = f'date_fact{month}/{day}.csv'
    url = f"http://numbersapi.com/{month}/{day}/date"
    res = requests.get(url)
    type(res)
    type(res.text)
    data = []
    header = ['timepoint', 'cloudcover']
    data.append({'date': date, 'text': res.text})
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(os.path.join('jeanette_folder', blob_name))
    header = ['date, text']
    with blob.open("w") as f:
        writer = csv.DictWriter(f, fieldnames=header, lineterminator="\n")
        writer.writeheader()
        writer.writerows(data)


with DAG(
    "jeanette_dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:

    run_python_task = PythonOperator(
        task_id="run_some_python_task", # This controls what your task name is in the airflow UI 
        python_callable=run # This is the function that airflow will run 
    )