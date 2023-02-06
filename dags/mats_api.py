import os
import csv
import datetime
import requests
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Mats",
    "retries": 5,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(2023, 2, 4),
}

def run(**kwargs):
    bucket_name = 'brights_bucket_1'
    blob_name = 'mats_test.csv'
    url = "https://free-to-play-games-database.p.rapidapi.com/api/filter"
    querystring = {"tag":"3d.mmorpg.fantasy.pvp","platform":"pc"}
    headers = {
	"X-RapidAPI-Key": "5159d08578msha1641dfe82c9f26p1bd992jsn45e198531a33",
	"X-RapidAPI-Host": "free-to-play-games-database.p.rapidapi.com"
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    response_data = response.json()
    for d in response_data:
        del d['thumbnail']
        del d['game_url']
        del d['short_description']
        del d['freetogame_profile_url']
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(os.path.join('mats_preparation_test_folder', blob_name))
    with blob.open("w") as f:
        writer = csv.DictWriter(f, fieldnames=response_data[0].keys(), lineterminator="\n")
        writer.writeheader()
        writer.writerows(response_data)

with DAG(
    dag_id="mats_api",
    description="Gets data on free-to-play online games",
    default_args=default_args,
    schedule_interval="@daily", 
) as dag:

    run_python_task = PythonOperator(
        task_id="f2p_first_task", # This controls what your task name is in the airflow UI 
        python_callable=run # This is the function that airflow will run 
    )
