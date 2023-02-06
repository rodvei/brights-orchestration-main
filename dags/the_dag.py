import datetime
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator, BashOperator

default_args = {
    "owner":"duggurd",
    "retries":"1",
    "retry_delay":datetime.timedelta(minutes=1),
    "start_date":datetime.datetime.now()
}

with DAG(
    dag_id = "date_swapi",
    description = "This DAG gets todays date, then queries the SWAPI",
    default_args = default_args,
    schedule_interval = "@monthly"
) as dag:

    t1 = BashOperator(
        task_id="print_date",
        bash_command="date"
    )

    dag.doc_md = """
    # the_dag Doc
    this is a useless dag, go touch some grass instead of reading this documentation
    you need it, trust me.
    """

    t1.doc_md = """
    # print_date Doc
    This task prints todays date then does nothing because 
    it is a useless task and essentiall should not exists to begin with.
    """

    def query_swapi():
        print(requests.get("swapi.dev/api/planets/1/").json())

    t2 = PythonOperator(
        task_id="query_swapi",
        python_callable=query_swapi
    )

    t1 >> t2