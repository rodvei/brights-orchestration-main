import datetime
import requests
import json
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage

default_args = {
    "owner": "Kenneth",
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=2),
    "start_date": datetime.datetime(2023, 1, 1),
}

def get_data():
    # Set up API
    # SATS - Center Storo, Nydalen, Bjørvika and Ullevål
    url = "https://sats-prod-euw-centersapi.azurewebsites.net/current-center-load?brand=Sats&c=157&c=178&c=208&c=221"

    # Get data
    response = requests.request("GET", url=url)

    # Raise status
    response.raise_for_status()

    # Transform to JSON
    json_response = response.json()

    # Set up gcp storage
    bucket_name = 'brights_bucket_1'
    blob_name = "sats_capasity_nearby.json"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(os.path.join('kenneth-blob-folder', blob_name))

    # Save JSON-file in gcp
    with blob.open("w") as fp:
        json.dump(json_response, fp)

    # Inspect blob
    blobs = storage_client.list_blobs(bucket_name)
    print('get all blobs names:')
    for blob in blobs:
        print(blob.name)

with DAG(
    dag_id="kenneth-dag",
    description="Get data about nearby sats capasity",
    default_args=default_args,
    schedule_interval="@hourly", #None, @hourly, @weekly, @monthly, @yearly,...
) as dag:

    data_extraction = PythonOperator(
        task_id="data_extraction", # This controls what your task name is in the airflow UI 
        python_callable=get_data # This is the function that airflow will run 
    )


