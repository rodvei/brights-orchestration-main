import datetime
import requests
import json
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

GCS_BUCKET_NAME = "brights_bucket_1"
GCS_BLOB_STAGING_PATH = r"kenneth-blob-folder/sats_capasity_2023-02-06_15:00:06.json"
BQ_PROJECT = "brights-orchestration"
BQ_DATASET_NAME = "capacity_data"
BQ_TABLE_NAME = "capacity_table"

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
    dt_now = datetime.datetime.now()
    current_time = dt_now.strftime("%H")
    current_date = dt_now.date()

    # Use kwargs["ds"] to retrieve date
    # Add hour 

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(GCS_BLOB_STAGING_PATH)

    # Save JSON-file in gcp
    with blob.open("w") as fp:
        json.dump(json_response, fp)

with DAG(
    dag_id="kenneth-dag",
    description="Get data about nearby sats capasity",
    default_args=default_args,
    schedule_interval="0 14-17 * * 1-3", #None, @hourly, @weekly, @monthly, @yearly,...
) as dag:

    data_extraction = PythonOperator(
        task_id="data_extraction", # This controls what your task name is in the airflow UI 
        python_callable=get_data # This is the function that airflow will run 
    )

    data_load = GCSToBigQueryOperator(
        task_id="load_data_to_bq",
        bucket=GCS_BUCKET_NAME,
        source_objects=[GCS_BLOB_STAGING_PATH],
        destination_project_dataset_table=f"{BQ_PROJECT}:{BQ_DATASET_NAME}.{BQ_TABLE_NAME}",
        source_format="JSON",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        dag=dag
    )

    data_extraction>>data_load