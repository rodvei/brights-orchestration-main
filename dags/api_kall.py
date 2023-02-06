import os
import csv
import datetime
import requests
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Ingrid",
    "retries": 5,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(2023, 2, 4),
}

#brights_bucket_1/preparation_test_folder


def run(**kwargs):
    # API Inspiration: 
    # https://rapidapi.com/collection/list-of-free-apis
    # https://mixedanalytics.com/blog/list-actually-free-open-no-auth-needed-apis/

    bucket_name = 'brights_bucket_1'
    dag_date = kwargs['ds']
    dag_time = datetime.datetime.now()
    blob_name = f'dadjoke_plus_kanyequote_DATE_{dag_date}TIME_{dag_time}.csv'

    url_1 = "https://v2.jokeapi.dev/joke/Any?safe-mode"
    res_1 = requests.get(url_1)
    res_1_data = res_1.json()

    url_2 = "https://api.kanye.rest/"
    res_1 = requests.get(url_2)
    res_2_data = res_1.json()

    print(res_1_data)
    print(res_2_data)

    joke_list = []
    header = ['timepoint', 'joke']

    joke_list.append(dag_time)

    if res_1_data['type'] == 'twopart':
        joke_list.append(f"Dad: {res_1_data['setup']} {res_1_data['delivery']}. Kanye: {res_2_data}")
    elif res_1_data['type'] == 'onepart':
        joke_list.append(f"Dad: {res_1_data['joke']}. Kanye: {res_2_data}")

    print(joke_list)

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(os.path.join('ingrids_folder', blob_name))

    with blob.open("w") as f:
        writer = csv.DictWriter(f, fieldnames=header, lineterminator="\n")
        writer.writeheader()
        writer.writerows(joke_list)
    
    blobs = storage_client.list_blobs(bucket_name)
    print('get all blobs names:')

    for blob in blobs:
        print(blob.name)

with DAG(
    dag_id="random_joke_dag_1",
    description="This is our first test dag",
    default_args=default_args,
    schedule_interval="@daily", #None, @hourly, @weekly, @monthly, @yearly,...
) as dag:

    run_python_task = PythonOperator(
        task_id="random_joke_task", # This controls what your task name is in the airflow UI 
        python_callable=run # This is the function that airflow will run 
    )
