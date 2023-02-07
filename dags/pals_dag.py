import datetime
import csv
import requests
import os
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

BUCKET_NAME = 'brights_bucket_1'
BLOB_STAGING_PATH = r'pals_test_folder/planets.csv'
BQ_PROJECT = 'brights-orchestration'
BQ_DATASET_NAME = 'planet_dag'
BQ_TABLE_NAME = 'planets'

default_args = {
    "owner": "Pål",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=1),
    "start_date": datetime.datetime(2023, 2, 4),
}

def get_planets(date, **kwargs):
    bucket_name = 'brights_bucket_1'
    blob_name = f'{date}_planet_list.csv'
    planets = ['mercury','venus','earth','mars','jupiter','saturn','uranus','neptune']
    planet_list = []
    for planet in planets:
        url = f'https://api.api-ninjas.com/v1/planets?name={planet}'
        payload = requests.get(url, headers={'X-API-Key': 'IfBN/09mgUEHE4M+UdDYkw==VojvtoSTfSBFpOug'})
        payload_data = payload.json()   
        planet_list.append(payload_data[0])  
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(os.path.join(f'pals_test_folder', BLOB_STAGING_PATH))

    headers = planet_list[0].keys()
    with blob.open("w") as f:
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()
        writer.writerows(planet_list)

def run(**kwargs):
    run_date = kwargs['ds']
    get_planets(run_date)

with DAG(
    "pals_first_dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:
    run_planet_task = PythonOperator(
        task_id = "get_planet_list",
        python_callable = run
    )
    task_csv_load = GCSToBigQueryOperator(
        task_id="upload_planet_list", # This controls what your task name is in the airflow UI 
        bucket=BUCKET_NAME, # This is the function that airflow will run 
        source_objects=[BLOB_STAGING_PATH],
        destination_project_dataset_table=f"{BQ_PROJECT}:{BQ_DATASET_NAME}.{BQ_TABLE_NAME}",
        schema_fields=[
            {'name': 'name', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'mass', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'radius', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'period', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'semi_major_axis', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'temperature', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'distance_light_year', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'host_star_mass', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'host_star_temperature', 'type': 'FLOAT', 'mode': 'REQUIRED'}],
        write_disposition='WRITE_TRUNCATE'
    )