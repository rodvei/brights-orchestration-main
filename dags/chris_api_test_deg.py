import os
import csv
import datetime
import requests
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Chris",
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=1),
    "start_date": datetime.datetime(2023, 2, 4),
}


def get_data_from_api():
    url = "https://currency-exchange.p.rapidapi.com/exchange"
    querystring = {"from":"USD","to":"EURO","q":"1.0"}
    headers =   {
                "X-RapidAPI-Key": "4581d01d22msh3d0c48969bbb406p1e12c7jsn2baa9a7a5f4e",
                "X-RapidAPI-Host": "currency-exchange.p.rapidapi.com"
                }
    response = requests.request("GET", url, headers=headers, params=querystring)
    if response.ok:
         return response.json()

def store_data(current_exchange_rate):
    time_stamp = str(datetime.datetime.now())
    bucket_name = 'brights_bucket_1'
    blob_name = 'exchange_rate' + " " + time_stamp
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(fr'exchange_rate_folder_chris/{blob_name}')
    header = ['date_time', 'euro_per_USD']
    with blob.open("w") as f:
        writer = csv.DictWriter(f, fieldnames=header, lineterminator="\n")
        writer.writeheader()
        writer.writerow({'date_time': time_stamp, 'euro_per_USD': current_exchange_rate})

def save_exchange_rate():
    data = get_data_from_api()
    store_data(data)


with DAG(
    dag_id="USD_to_EURO_exchange_rate",
    description="Gets the number og Euro 1 USD can buy",
    default_args=default_args,
    schedule_interval="*/5 * * * *", #None, @hourly, @weekly, @monthly, @yearly,...
) as dag:

    run_python_task = PythonOperator(
        task_id="save_exchange_rate", # This controls what your task name is in the airflow UI 
        python_callable=save_exchange_rate # This is the function that airflow will run 
    )
