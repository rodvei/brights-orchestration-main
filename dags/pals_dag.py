import datetime
import csv
import requests
import os
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Pål",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=1),
    "start_date": datetime.datetime(2023, 2, 4),
}

def get_planets(date):
    bucket_name = 'brights_bucket_1'
    blob_name = f'{date}_planet_list.csv'
    planets = ['mercury','venus','earth','mars','jupiter','saturn','uranus','neptune']
    planet_list = []
    for planet in planets:
        url = f'https://api.api-ninjas.com/v1/planets?name={planet}'
        payload = requests.get(url, headers={'X-API-Key': 'IfBN/09mgUEHE4M+UdDYkw==VojvtoSTfSBFpOug'})
        payload_data = payload.json()   
        planet_list.append(payload_data)
        for rows in payload_data:
            planet_list.append(rows)    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(os.path.join(f'preparation_test_folder/pal', blob_name))

    with blob.open("w") as f:
        writer = csv.DictWriter(f, fieldnames=planet_list[0][0], lineterminator="\n")
        writer.writeheader()
        for i in range(len(planet_list)):
            writer.writerows(planet_list[i])

def run(**kwargs):
    run_date = kwargs['ds']
    get_planets(run_date)

with DAG(
    "pals_first_dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:
    run_planet_task = PythonOperator(
        task_id = "planet_list",
        python_callable = run
    )
