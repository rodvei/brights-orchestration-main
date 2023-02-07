import os
import csv
import datetime
import requests
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Marian",
    "retries": 5,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(2023, 2, 6),
}

def run(**kwargs):
    # API Inspiration: 
    # https://rapidapi.com/collection/list-of-free-apis
    # https://mixedanalytics.com/blog/list-actually-free-open-no-auth-needed-apis/
    bucket_name = 'marians_bucket_2'
    blob_name = 'marian_blob.csv'

    url = f"https://v2.jokeapi.dev/joke/Any?safe-mode"
    res = requests.get(url)
    data = res.json()
    data_new=[]
    for k,v in data.items():
        data_dict={}
        if data['type']=='twopart':
            data_dict['category']=data['category']
            data_dict['joke'] = data['setup'] +' '+data['delivery']
        elif data['type']=='single':
            data_dict['category']=data['category']
            data_dict['joke'] = data['joke']
        print(data_dict)

    header = ['category', 'joke']
    data_new.append(data_dict)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    #blob = bucket.blob(os.path.join('preparation_test_folder', blob_name))
    blob = bucket.blob(fr'preparation_test_folder/{blob_name}')
    with blob.open("w") as f:
        writer = csv.DictWriter(f, fieldnames=header, lineterminator="\n")
        writer.writeheader()
        writer.writerows(data_new)
    
    blobs = storage_client.list_blobs(bucket_name)
    print('get all blobs names:')
    for blob in blobs:
        print(blob.name)

with DAG(
dag_id="the_joke_dag",
description="A joke a day dag",
default_args=default_args,
schedule_interval="@daily", #None, @hourly, @weekly, @monthly, @yearly,...
) as dag:

    run_python_task = PythonOperator(
    task_id="test_preparation_first_task", # This controls what your task name is in the airflow UI 
    python_callable=run # This is the function that airflow will run 
)