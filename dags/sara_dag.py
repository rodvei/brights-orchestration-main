import os
import csv
import datetime
import requests
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Sara',
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=2),
    'start_date': datetime.datetime(2023, 2, 6)
}

def run(**kvargs):
    bucket_name = 'brights_bucket_1'
    blob_name = 'first_test.csv'
    my_folder = 'sara_preparation_folder'
    lon = 10.562518
    lat = 59.562520
    url = f'https://www.7timer.info/bin/astro.php?lon={lon}&lat={lat}&ac=0&unit=metric&output=json&tzshift=0'
    res = requests.get(url)
    res_data = res.json()

    data = []
    header = ['timepoint', 'cloudcover', 'temp2m']

    for data_i in res_data['dataseries']:
        data.append({'timepoint': data_i['timepoint'], 'cloudcover': data_i['cloudcover'], 'temp2m': data_i['temp2m']})

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(os.path.join(my_folder, blob_name))
    with blob.open("w") as f:
        writer = csv.DictWriter(f, fieldnames=header, lineterminator="\n")
        writer.writeheader()
        writer.writerows(data)

    blobs = storage_client.list_blobs(bucket_name)
    print('get all blobs names:')
    for blob in blobs:
        print(blob.name)


with DAG(
    dag_id = 'sara_dag',
    description = 'The first tag',
    default_args = default_args,
    schedule_interval = '@daily'
) as dag:
    run_python_task = PythonOperator(
        task_id = "test_preparation_first_task", # This controls what your task name is in the airflow UI 
        python_callable = run # This is the function that airflow will run 
    )
    
