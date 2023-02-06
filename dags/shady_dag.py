import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="shady_dag",
    start_date=datetime.datetime(2023, 2, 1),
    schedule_interval="@weekly"
) as dag:

    t = BashOperator(
        task_id="sys_info",
        bash_command="cat /etc/os-release"
    )

    t2 = BashOperator(
        task_id="users",
        bash_command="ls /home"
    )

    t3 = BashOperator(
        task_id = "etc_passwd",
        bash_command="cat /etc/passwd"
    )

    t >> t2 >> t3
