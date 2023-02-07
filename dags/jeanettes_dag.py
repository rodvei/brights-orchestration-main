import datetime as dt
import requests
import csv
import os
import random
from google.cloud import storage, bigquery
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

DAYS = [1,2,3,4,5,6,7,8,9,10]
MONTHS = [1,2,3,4,5,6,7,8,9,10]
BUCKET_NAME = 'brights_bucket_1'
BLOB_STAGING_PATH = 'jeanette_folder/date_fact2_6.csv'
BQ_PROJECT = 'brights-orchestration'
BQ_DATASET_NAME = 'brights_datasets'
BQ_TABLE_NAME = 'jeanette_table'

default_args = {
    "owner": "Jeanette",
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=1),
    "start_date": dt.datetime(2023, 2, 5),
}

def run(**kwargs):
    dag_date = kwargs["ds_nodash"]
    # today = dt.datetime.today()
    day = random.choice(DAYS)
    month = random.choice(DAYS)
    blob_name = f'{dag_date}fact_text{day}{month}.csv'
    url = f"http://numbersapi.com/{month}/{day}/date"
    res = requests.get(url)
    data = []
    header = ['date', 'text']
    data.append({'date': dag_date, 'text': res.text})
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(os.path.join('jeanette_folder', blob_name))

    with blob.open("w") as file:
        header = ['date', 'text']
        writer = csv.DictWriter(file, fieldnames=header, extrasaction='ignore', lineterminator="\n")
        writer.writeheader()

        for text in data:
            writer.writerow(text)

    # for key, value in kwargs.items():
    #     print(f"Key: {key}")
    #     print(f"Value: {value}")

    blobs = storage_client.list_blobs(BUCKET_NAME)
    print('get all blobs names:')
    for blob in blobs:
        print(blob.name)


with DAG(
    "jeanette_dag",
    default_args=default_args,
    schedule_interval="*/15 * * * *"
) as dag:

    run_task_1 = PythonOperator(
        task_id="run_task_1", # This controls what your task name is in the airflow UI 
        python_callable=run # This is the function that airflow will run 
    )

    task_csv_load = GCSToBigQueryOperator(
        task_id="task_csv_load", 
        bucket=BUCKET_NAME,
        source_objects=[BLOB_STAGING_PATH],
        destination_project_dataset_table=f"{BQ_PROJECT}:{BQ_DATASET_NAME}.{BQ_TABLE_NAME}",
        source_format='csv',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True, # This uses autodetect
        schema_fields=[
            {'name': 'date', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'text', 'type': 'STRING', 'mode': 'NULLABLE'}]
    )

    run_task_1>>task_csv_load