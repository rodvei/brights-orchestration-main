import os
import csv
import datetime
import requests
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

BUCKET_NAME = 'brights_bucket_1'
BLOB_STAGING_PATH = r'preparation_test_folder/games.csv'
BQ_PROJECT = 'brights-orchestration'
BQ_DATASET_NAME = 'brights_datasets'
BQ_TABLE_NAME = 'mats_table'

default_args = {
    "owner": "Mats",
    "retries": 5,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(2023, 2, 4),
}

def get_api_data():
    url = "https://free-to-play-games-database.p.rapidapi.com/api/filter"
    querystring = {"tag":"3d.mmorpg.fantasy.pvp","platform":"pc"}
    headers = {
	"X-RapidAPI-Key": "5159d08578msha1641dfe82c9f26p1bd992jsn45e198531a33",
	"X-RapidAPI-Host": "free-to-play-games-database.p.rapidapi.com"
    }
    response = requests.request("GET", url, headers=headers, params=querystring).json()
    return response

def transform_api_data(api_data):
    for d in api_data:
        del d['thumbnail']
        del d['game_url']
        del d['short_description']
        del d['freetogame_profile_url']
    return api_data

def save_to_csv(processed_data):
    bucket_name = 'brights_bucket_1'
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(BLOB_STAGING_PATH)
    with blob.open("w") as f:
        writer = csv.DictWriter(f, fieldnames=processed_data[0].keys(), lineterminator="\n")
        writer.writeheader()
        writer.writerows(processed_data)

def run():
    # dag_date = kwargs['ds']
    api_data = get_api_data()
    processed_data = transform_api_data(api_data)
    save_to_csv(processed_data)

with DAG(
    dag_id="mats_api",
    description="Gets data on free-to-play online games",
    default_args=default_args,
    schedule_interval="@daily", 
) as dag:

    run_all = PythonOperator(
        task_id="f2p_run_task", 
        python_callable=run 
    )

    task_csv_load = GCSToBigQueryOperator(
        task_id="load_csv_gcs_to_bq", 
        bucket=BUCKET_NAME, 
        source_objects=[BLOB_STAGING_PATH],
        destination_project_dataset_table=f"{BQ_PROJECT}:{BQ_DATASET_NAME}.{BQ_TABLE_NAME}",
        schema_fields=[
            {'name': 'id', 'type': 'INT64', 'mode': 'REQUIRED'},
            {'name': 'title', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'genre', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'platform', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'publisher', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'developer', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'release_date', 'type': 'STRING', 'mode': 'NULLABLE'}],
        write_disposition='WRITE_TRUNCATE'
    )

    run_all>>task_csv_load
