import os
import csv
import json
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

def dump_to_blob(API_res, filename, **kwargs):
    dag_date = kwargs['ds']
    dag_time = datetime.datetime.now()

    bucket_name = 'brights_bucket_1'
    blob_name = f'{filename}_DATE_{dag_date}TIME_{dag_time}.json'
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(os.path.join('ingrids_folder', blob_name))

    with blob.open("w") as outfile:
        json.dump(API_res, outfile)
    
    blobs = storage_client.list_blobs(bucket_name)
    print('get all blobs names:')

    for blob in blobs:
        print(blob.name)

def load_from_blob(blob_name, **kwargs):
    blob_name = blob_name
    bucket_name = 'brights_bucket_1'
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(os.path.join('ingrids_folder', blob_name))

    with blob.open("r") as infile:
        blob_file = infile.read()

    return blob_file

def extract_api(**kwargs):
    # API Inspiration: 
    # https://rapidapi.com/collection/list-of-free-apis
    # https://mixedanalytics.com/blog/list-actually-free-open-no-auth-needed-apis/

    url_joke = "https://v2.jokeapi.dev/joke/Any?safe-mode"
    joke_response = requests.get(url_joke)
    joke_data = joke_response.json()

    url_quote = "https://api.kanye.rest/"
    quote_response = requests.get(url_quote)
    quote_data = quote_response.json()

    API_res_dict = {}
    API_res_dict['joke_API'] = joke_data
    API_res_dict['quote_API'] = quote_data

    dump_to_blob(API_res_dict, 'API_results', **kwargs)
    

def transform_to_csv(**kwargs):
    dag_date = kwargs['ds']
    dag_time = datetime.datetime.now()
    blob_name = f'API_results_DATE_{dag_date}TIME_{dag_time}.json'

    API_dict = load_from_blob(blob_name)
    joke_data = API_dict['joke_API']
    quote_data = API_dict['quote_API']

    joke_quote_dict = {}

    if joke_data['type'] == 'twopart':
        joke_quote_dict['dad_kanye_exchange'] = f"Dad: {joke_data['setup']} {joke_data['delivery']}. Kanye: {quote_data}"
    elif joke_data['type'] == 'onepart':
        joke_quote_dict['dad_kanye_exchange'] = f"Dad: {joke_data['joke']}. Kanye: {quote_data}"

    dump_to_blob(joke_quote_dict, 'dad_kanye_exchange', **kwargs)


def load_to_csv(joke_quote_list, **kwargs):
    dag_date = kwargs['ds']
    dag_time = datetime.datetime.now()
    blob_name = f'dad_kanye_exchange_DATE_{dag_date}TIME_{dag_time}.csv'
    dad_kanye_exchange_dict = load_from_blob(blob_name)
    
    headers = ['joke']
    joke_quote_list = [dad_kanye_exchange_dict]

    storage_client = storage.Client()
    bucket_name = 'brights_bucket_1'
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(os.path.join('ingrids_folder', blob_name))

    with blob.open("w") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=headers, lineterminator="\n")
        writer.writeheader()
        writer.writerows(joke_quote_list)
    
    blobs = storage_client.list_blobs(bucket_name)
    print('get all blobs names:')

    for blob in blobs:
        print(blob.name)

with DAG(
    dag_id="dad_kanye_exchange",
    description="Dad tells a joke and Kanye answers with a statement that makes sense to him",
    default_args=default_args,
    schedule_interval="@daily", #None, @hourly, @weekly, @monthly, @yearly,...
) as dag:

    run_python_task = PythonOperator(
        task_id="extract_task", # This controls what your task name is in the airflow UI 
        python_callable=extract_api # This is the function that airflow will run 
    )

    run_python_task = PythonOperator(
        task_id="transform_task", # This controls what your task name is in the airflow UI 
        python_callable=transform_to_csv # This is the function that airflow will run 
    )

    run_python_task = PythonOperator(
        task_id="load_task", # This controls what your task name is in the airflow UI 
        python_callable=load_to_csv # This is the function that airflow will run 
    )

    extract_api>>transform_to_csv>>load_to_csv
