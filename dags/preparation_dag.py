import os
import csv
import datetime
import requests
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

BUCKET_NAME = 'brights_bucket_1'
BLOB_STAGING_PATH = r'preparation_test_folder/test1.csv'
BQ_PROJECT = 'brights-orchestration'
BQ_DATASET_NAME = 'preperation_dag'
BQ_TABLE_NAME = 'weather_data'


default_args = {
    "owner": "Kristoffer",
    "retries": 5,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(2023, 2, 4),
}

#brights_bucket_1/preparation_test_folder



def get_data_from_api(**kwargs):
    # API Inspiration: 
    # https://rapidapi.com/collection/list-of-free-apis
    # https://mixedanalytics.com/blog/list-actually-free-open-no-auth-needed-apis/
    dag_date = kwargs['ds']
    lon = 10.757933
    lat = 59.911491
    url = f"https://www.7timer.info/bin/astro.php?lon={lon}&lat={lat}&ac=0&unit=metric&output=json&tzshift=0"
    res = requests.get(url)
    res_data = res.json()
    data = []
    header = ['timepoint', 'cloudcover']
    for data_i in res_data['dataseries']:
        instance = {}
        instance['timepoint'] = f'{dag_date}-{data_i["timepoint"]}'
        instance['cloudcover'] = data_i['cloudcover']
        data.append(instance)
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(BLOB_STAGING_PATH)
    with blob.open("w") as f:
        writer = csv.DictWriter(f, fieldnames=header, lineterminator="\n")
        writer.writeheader()
        writer.writerows(data)
    
    # blobs = storage_client.list_blobs(BUCKET_NAME)
    # print('get all blobs names:')
    # for blob in blobs:
    #     print(blob.name)



with DAG(
    dag_id="preparation_dag",
    description="This is our first test dag",
    default_args=default_args,
    schedule_interval="@daily", #None, @hourly, @weekly, @monthly, @yearly,...
) as dag:

    task_get_data_from_api = PythonOperator(
        task_id="get_data_from_api", # This controls what your task name is in the airflow UI 
        python_callable=get_data_from_api # This is the function that airflow will run 
    )

    task_csv_load = GCSToBigQueryOperator(
        task_id="load_csv_gcs_to_bq", # This controls what your task name is in the airflow UI 
        bucket=BUCKET_NAME, # This is the function that airflow will run 
        source_objects=[BLOB_STAGING_PATH],
        destination_project_dataset_table=f"{BQ_PROJECT}:{BQ_DATASET_NAME}.{BQ_TABLE_NAME}",
        schema_fields=[
            {'name': 'time_point', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'cloudcover', 'type': 'INT64', 'mode': 'NULLABLE'}],
        write_disposition='WRITE_TRUNCATE'
    )

    task_get_data_from_api>>task_csv_load