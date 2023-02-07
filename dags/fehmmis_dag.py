import requests
import http.client, urllib.parse
import datetime
import json
import csv
import os
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator

apikey = "acf08f2517abc3baa8e1608b66fcb52e"

default_args = {
    "owner": "Fehmmi",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=1),
    "start_date": datetime.datetime(2023, 1, 1),
}

bucket_name = 'brights_bucket_1'
blob_name_csv = 'GB_news.csv'
blob_name_json = 'GB_news.json'
BLOB_STAGING_OBJECT = r'fehmmi/api_mediastack_transform_json/2023-02-06_GB_news.json'
BQ_PROJECT = 'brights-orchestration'
BQ_DATASET_NAME = 'brights_datasets'
BQ_TABLE_NAME = 'fehmmi_table'

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name) 


def api_call():
    params = {
        'access_key': apikey,
        'categories': '-general,-sports',
        'limit': 10,
        'countries': 'gb'
    }

    response = requests.get("http://api.mediastack.com/v1/news", params=params).json()
    
    return response

def transform_data(**kwargs):
    """getting out relevant information from json and then writing a csv file
    """
    api_connection = api_call()

    date_api_call = kwargs['ds']

    my_dict = []
    header = ['author', 'source','category', 'url']

    for data in api_connection['data']:
        author = data['author']
        source = data['source']
        category = data['category']
        url = data['url']
        my_dict.append({'author': author, 'source': source, 'category': category, 'url': url})
    # print(my_dict)   

    blob = bucket.blob(fr'fehmmi/api_mediastack_csv/{date_api_call}_{blob_name_csv}') #storing data in google cloud storage using blob
    
    with blob.open("w") as f:
        writer = csv.DictWriter(f, fieldnames=header, lineterminator="\n")
        writer.writeheader()
        writer.writerows(my_dict)

def transform_json(**kwargs):
    """accessing csv file from gcs by using get_blob and transforming the csv file to a json
    """
    date_api_call = kwargs['ds']
    json_storage = []


    blob = bucket.blob(fr'fehmmi/api_mediastack_transform_json/{date_api_call}_{blob_name_json}') #storing data in google cloud storage using blob
    
    
    #opening the csv file
    get_blob = bucket.get_blob(fr'fehmmi/api_mediastack_csv/{date_api_call}_{blob_name_csv}') # accessing the blob from google cloud storage so i can use it in second task
    with get_blob.open("r") as f:
        csv_reader = csv.DictReader(f)

        for rows in csv_reader:
            json_storage.append(rows)

    with blob.open("w") as outfile:
        json.dump(json_storage, outfile)    

    
with DAG(
    "fehmmi-dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:

    task1_api_mediastack = PythonOperator(
        task_id="api_mediastack.com", # This controls what your task name is in the airflow UI 
        python_callable=transform_data # This is the function that airflow will run 
    )

    task2_convert_to_json = PythonOperator(
        task_id="convert_to_json",
        python_callable = transform_json
    )

    task1_api_mediastack >> task2_convert_to_json 


    