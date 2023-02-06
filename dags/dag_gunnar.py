import datetime
import requests
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator


def get_aviation_sigmets(atsu_id:str = "MKCE", date:datetime.datetime=datetime.date.today()):
    date = date.isoformat()
    URL = fr"https://api.weather.gov/aviation/sigmets/{atsu_id}/{date}"

    res = requests.get(URL)

    if res:
        data = res.json()
        return data

with DAG(
    "dag_gunnar",
    default_args = {
        "email_on_failure": True,
        "email_on_retry" : True,
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=1)
    },
    description = "A simple dag that spams an email address",
    schedule = datetime.timedelta(hours=1),
    start_date = datetime.datetime(2023, 2, 5)

) as dag:

    send_email_task = EmailOperator(
        task_id="send_email_spam",
        to="fehmmialiti@gmail.com",
        subject="You just won 1 million USD!",
        html_content = f"<h1> {get_aviation_sigmets()} </h1>",
        retries = 2
    )

    def print_success():
        print("SUCCESS!")
    
    success_print = PythonOperator(
        task_id = "log_success",
        python_callable = print_success,
        depends_on_past = True
    )

    send_email_task >> success_print

