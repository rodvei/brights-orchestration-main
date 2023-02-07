import datetime as dt
import requests
import csv
import os
import random
from google.cloud import storage, bigquery
from airflow import DAG
from airflow.operators.python import PythonOperator

DAYS = [1,2,3,4,5,6,7,8,9,10]
MONTHS = [1,2,3,4,5,6,7,8,9,10]

default_args = {
    "owner": "Jeanette",
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=1),
    "start_date": dt.datetime(2023, 2, 5),
}

def run(**kwargs):
    dag_date = kwargs["ds_nodash"]
    bucket_name = 'brights_bucket_1'
    today = dt.datetime.today()
    day = random.choice(DAYS)
    month = random.choice(DAYS)
    blob_name = f'{dag_date}fact_text{day}{month}.csv'
    url = f"http://numbersapi.com/{month}/{day}/date"
    res = requests.get(url)
    data = []
    header = ['date', 'text']
    data.append({'date': dag_date, 'text': res.text})
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
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

    blobs = storage_client.list_blobs(bucket_name)
    print('get all blobs names:')
    for blob in blobs:
        print(blob.name)

def test_second_run(**kwargs):
    dag_date = kwargs["ds_nodash"]
    print(f"This is second run, date_dat: {dag_date}")
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    # table_id = "dag_project.your_dataset.your_table_name"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("date", "STRING"),
            bigquery.SchemaField("text", "STRING"),
        ],
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )
    uri = "gs://cloud-samples-data/bigquery/us-states/us-states.csv"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print(f"Loaded {destination_table.num_rows} rows.")




with DAG(
    "jeanette_dag",
    default_args=default_args,
    schedule_interval="*/15 * * * *",
) as dag:

    run_task_1 = PythonOperator(
        task_id="run_task_1", # This controls what your task name is in the airflow UI 
        python_callable=run # This is the function that airflow will run 
    )
    run_task_2 = PythonOperator(
        task_id="run_task_2", # This controls what your task name is in the airflow UI 
        python_callable=test_second_run # This is the function that airflow will run 
    )

    run_task_1>>run_task_2