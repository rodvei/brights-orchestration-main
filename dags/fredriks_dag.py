import datetime
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from datetime import date
from google.cloud import storage


default_args = {
    "owner": "freddie",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(2022, 8, 21),
}


def get_date_fact(**kwargs):
    date_string = kwargs['ds']
    bucket_name = 'brights_bucket_1'

    month = date_string[5:7]
    day = date_string[8:10]
    number = f'{month}/{day}'
    blob_name = f'{number}_todays_fact.txt'

    url = f"http://numbersapi.com/{number}/date"
    res = requests.get(url)
    res_data = res.text
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(os.path.join('freddies_date_facts', blob_name))
    with blob.open("w") as f:
        f.write(res_data)

    blobs = storage_client.list_blobs(bucket_name)
    print('get all blobs names:')
    for blob in blobs:
        print(blob.name)
    return res_data

res_data = get_date_fact()

def print_file():
    print(res_data)


with DAG(
    "freddies_dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:

    run_python_task = PythonOperator(
        task_id="bonjour_le_monde", # This controls what your task name is in the airflow UI 
        python_callable=get_date_fact # This is the function that airflow will run 
    )

get_date_fact >> print_file
