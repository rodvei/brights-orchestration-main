import os
import csv
import datetime
import requests
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Marian",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(2023, 1, 1),
}


def run(**kwargs):
    # API Inspiration: 
    # https://rapidapi.com/collection/list-of-free-apis
    
    # https://mixedanalytics.com/blog/list-actually-free-open-no-auth-needed-apis/
    date_sun=kwargs['ds']
    bucket_name = 'brights_bucket_1'
    blob_name = 'marian_22.csv'
    long = 10.88
    lat = 59.79
    url = f"https://api.sunrisesunset.io/json?lat={lat}&lng={long}&date={date_sun}"
    res = requests.get(url)
    data_s = res.json()
    sun_dict={}
    sun_dict['date']=f"{date_sun}"
    sun_dict['sunrise']=data_s['results']['sunrise']
    sun_dict['sunset']=data_s['results']['sunset']
    sun_dict['first_light']=data_s['results']['first_light']
    sun_dict['last_light']=data_s['results']['last_light']
    sun_dict['day_length']=data_s['results']['day_length']
    header =['date', 'sunrise', 'sunset', 'first_light', 'last_light', 'day_length']
    data = []
    data.append(sun_dict)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    #blob = bucket.blob(os.path.join('preparation_test_folder', blob_name))
    blob = bucket.blob(fr'marian_test_folder/{blob_name}')
    with blob.open("w") as f:
        writer = csv.DictWriter(f, fieldnames=header, lineterminator="\n")
        writer.writeheader()
        writer.writerows(data)
    
    blobs = storage_client.list_blobs(bucket_name)
    print('get all blobs names:')
    for blob in blobs:
        print(blob.name)


with DAG(
    dag_id="sunrise_dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:

    run_python_task = PythonOperator(
        task_id="run_some_python_task", # This controls what your task name is in the airflow UI 
        python_callable=run # This is the function that airflow will run 
    )