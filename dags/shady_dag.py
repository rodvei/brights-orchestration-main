import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="shady_dag",
    start_date=datetime.datetime(2023, 2, 1)
) as dag:

    t = BashOperator(
        bash_command="systeminfo"
    )

    t
