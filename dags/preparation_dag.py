import os
import csv
import datetime
import requests
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Kristoffer",
    "retries": 5,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(2023, 2, 4),
}

#brights_bucket_1/preparation_test_folder


def run(**kwargs):
    # API Inspiration: 
    # https://rapidapi.com/collection/list-of-free-apis
    # https://mixedanalytics.com/blog/list-actually-free-open-no-auth-needed-apis/
    bucket_name = 'brights_bucket_1'
    blob_name = 'test1.csv'
    lon = 10.757933
    lat = 59.911491
    url = f"https://www.7timer.info/bin/astro.php?lon={lon}&lat={lat}&ac=0&unit=metric&output=json&tzshift=0"
    res = requests.get(url)
    res_data = res.json()
    data_date = res_data['init'][:-2]
    data = []
    header = ['timepoint', 'cloudcover']
    for data_i in res_data['dataseries']:
        data.append({'timepoint': data_i['timepoint'], 'cloudcover': data_i['cloudcover']})
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    #blob = bucket.blob(os.path.join('preparation_test_folder', blob_name))
    blob = bucket.blob(fr'preparation_test_folder/{blob_name}')
    with blob.open("w") as f:
        writer = csv.DictWriter(f, fieldnames=header, lineterminator="\n")
        writer.writeheader()
        writer.writerows(data)
    
    blobs = storage_client.list_blobs(bucket_name)
    print('get all blobs names:')
    for blob in blobs:
        print(blob.name)



with DAG(
    dag_id="preparation_dag",
    description="This is our first test dag",
    default_args=default_args,
    schedule_interval="@daily", #None, @hourly, @weekly, @monthly, @yearly,...
) as dag:

    run_python_task = PythonOperator(
        task_id="test_preparation_first_task", # This controls what your task name is in the airflow UI 
        python_callable=run # This is the function that airflow will run 
    )
