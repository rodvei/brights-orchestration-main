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
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(2023, 1, 1),
}

def api_call_date(**kwargs):
    date_api = kwargs['ds']
    return date_api


def api_call():
    params = {
        'access_key': apikey,
        'categories': '-general,-sports',
        'limit': 10,
        'countries': 'gb'
    }

    response = requests.get("http://api.mediastack.com/v1/news", params=params).json()
    
    return response

def transform_data():
    """getting out relevant information from json and then writing a csv file
    """
    api_connection = api_call()

    bucket_name = 'brights_bucket_1'
    blob_name = 'GB_news.csv'

    my_dict = []
    header = ['data', 'author', 'source','category']

    for data in api_connection['data']:
        author = data['author']
        source = data['source']
        category = data['category']
        url = data['url']
        my_dict.append({'author': author, 'source': source, 'category': category, 'url': url})
    # print(my_dict)   
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name) 

    blob = bucket.blob(os.path.join('preparation_test_folder/fehmmi', blob_name))
    with blob.open("w") as f:
        writer = csv.DictWriter(f, fieldnames=header, lineterminator="\n")
        writer.writeheader()
        writer.writerows(data)

    blobs = storage_client.lost_blobs(bucket_name)
    print('get all blobs names:')
    for blob in blobs:
        print(blob.name)
   
    

    
with DAG(
    "fehmmi-dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:

    run_python_task = PythonOperator(
        task_id="api_call", # This controls what your task name is in the airflow UI 
        python_callable=transform_data # This is the function that airflow will run 
    )


    