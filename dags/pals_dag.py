import datetime
import csv
import requests
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Pål",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=1),
    "start_date": datetime.datetime(2023, 2, 4),
}

def get_planets():
    bucket_name = 'brights_bucket_1'
    blob_name = 'uranus_planet.csv'
    url = f'https://api.api-ninjas.com/v1/planets?name=uranus'
    payload = requests.get(url, payload = requests.get(url, headers={'X-API-Key': 'IfBN/09mgUEHE4M+UdDYkw==VojvtoSTfSBFpOug'}))
    payload_data = payload.json()
    planet_list = []
    for rows in payload_data:
        planet_list.append(rows)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(os.path.join(f'preparation_test_folder/pal', blob_name))
    with blob.open("w") as f:
        writer = csv.DictWriter(f, lineterminator="\n")
        writer.writerows(planet_list)

    print(planet_list)

with DAG(
    "pals_first_dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:
    run_planet_task = PythonOperator(
        task_id = "show_uranus",
        python_callable = get_planets
    )
