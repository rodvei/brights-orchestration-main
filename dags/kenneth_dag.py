import datetime
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Kenneth",
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=2),
    "start_date": datetime.datetime(2023, 1, 1),
}

def get_data():
    # Set up API
    url = "https://api.norsk-tipping.no/LotteryGameInfo/v2/api/results/vikinglotto?"

    parameters = {
        "fromDate": "2023-01-01",
        "toDate": "2023-02-02"
    }

    # Get data
    response = requests.get(url, params=parameters)

    # Raise status
    response.raise_for_status()

    # Print data
    print(response.text)

with DAG(
    dag_id="kenneth-dag",
    description="Get data from API",
    default_args=default_args,
    schedule_interval="@weekly", #None, @hourly, @weekly, @monthly, @yearly,...
) as dag:

    data_extraction = PythonOperator(
        task_id="data_extraction", # This controls what your task name is in the airflow UI 
        python_callable=get_data # This is the function that airflow will run 
    )


