import datetime
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from datetime import date
from google.cloud import storage
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import json
import csv

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
    blob_name = 'todays_fact.csv'

    url = f"http://numbersapi.com/{number}/date"
    res = requests.get(url)
    res_data = res.text
    res_dict = [{'fact': res_data}]

    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(os.path.join('freddies_date_facts', blob_name))


    with blob.open("w") as file:
        header = ['fact']
        writer = csv.DictWriter(file, fieldnames=header, extrasaction='ignore', lineterminator="\n")
        writer.writeheader()
        for x in res_dict:    
            writer.writerow(x)

    blobs = storage_client.list_blobs(bucket_name)
    print('get all blobs names:')
    for blob in blobs:
        print(blob.name)






# def print_file(**kwargs):
#     date_string = kwargs['ds']

#     month = date_string[5:7]
#     day = date_string[8:10]
#     number = f'{month}/{day}'

#     url = f"http://numbersapi.com/{number}/date"
#     res = requests.get(url)
#     res_data = res.text
#     print(res_data)

with DAG(
    "freddies_dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:

    factern = PythonOperator(
        task_id="bonjour_le_monde", # This controls what your task name is in the airflow UI 
        python_callable=get_date_fact # This is the function that airflow will run 
    )

    GCS_factern = GCSToBigQueryOperator(
        task_id='get_date_fact',
        bucket='brights_bucket_1',
        source_objects=['freddies_date_facts/todays_fact.csv'],
        destination_project_dataset_table='brights-orchestration.brights_datasets.fredrik_table',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        source_format='CSV',
        schema_fields=[{'name':'fact', 'type': 'STRING', 'mode': 'NULLABLE'}]
    )


    # printern = PythonOperator(
    #     task_id="printern", # This controls what your task name is in the airflow UI 
    #     python_callable=print_file # This is the function that airflow will run 
    # )

    factern >> GCS_factern