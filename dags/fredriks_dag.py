import datetime
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator, GCSToBigQueryOperator
import os
from datetime import date
from google.cloud import storage
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


default_args = {
    "owner": "freddie",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(2022, 8, 21),
}


def get_date_fact(**kwargs):
    date_string = kwargs['ds']
    bucket_name = 'brights_bucket_1'

    month = date_string[5:7]
    day = date_string[8:10]
    number = f'{month}/{day}'
    blob_name = f'{number}_todays_fact.txt'

    url = f"http://numbersapi.com/{number}/date"
    res = requests.get(url)
    res_data = res.text
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(os.path.join('freddies_date_facts', blob_name))
    with blob.open("w") as f:
        f.write(res_data)

    blobs = storage_client.list_blobs(bucket_name)
    print('get all blobs names:')
    for blob in blobs:
        print(blob.name)
    return res_data



def print_file(**kwargs):
    date_string = kwargs['ds']

    month = date_string[5:7]
    day = date_string[8:10]
    number = f'{month}/{day}'

    url = f"http://numbersapi.com/{number}/date"
    res = requests.get(url)
    res_data = res.text
    print(res_data)

with DAG(
    "freddies_dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:


    factern = GCSToBigQueryOperator(
        task_id='get_date_fact',
        bucket='brights_bucket_1',
        source_objects=['todays_fact.txt'],
        destination_project_dataset_table='brigths-orchestration.preperation_dag.freddies_pool_table',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED'
    )

    # factern = PythonOperator(
    #     task_id="bonjour_le_monde", # This controls what your task name is in the airflow UI 
    #     python_callable=get_date_fact # This is the function that airflow will run 
    # )
    # printern = PythonOperator(
    #     task_id="printern", # This controls what your task name is in the airflow UI 
    #     python_callable=print_file # This is the function that airflow will run 
    # )

    # factern >> printern
