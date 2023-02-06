#!/usr/bin/env python
# coding: utf-8

# # Imports

# In[7]:


import datetime
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator
#from airflow.providers.papermill.operators.papermill import PapermillOperator


# # define dag

# In[8]:


default_args = {
    "owner": "Amar",
    "retries": 5,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(2023, 2, 4),
}


# # Functions

# In[13]:


def joke():
    print("Two hunters are out in the woods when one of them collapses. He doesn't seem to be breathing and his eyes are glazed. The other guy whips out his phone and calls the emergency services. He gasps, My friend is dead! What can I do? The operator says, Calm down. I can help. First, let's make sure he's dead. There is a silence; then a gun shot is heard. Back on the phone, the guy says, OK, now what?")


# # DAG

# In[15]:


with DAG(
    dag_id="joke_dag",
    description="amar1",
    default_args=default_args,
    schedule="@daily", #None, @hourly, @weekly, @monthly, @yearly,...
) as dag:
    run_python_task = PythonOperator(
        task_id="amar_joke", # This controls what your task name is in the airflow UI 
        python_callable=joke # This is the function that airflow will run 
    )


# notebook_task = PapermillOperator(
#         task_id="run_example_notebook",
#         input_nb="include/example_notebook.ipynb",
#         output_nb="include/out-{{ execution_date }}.ipynb",
#         parameters={"execution_date": "{{ execution_date }}"},
#     )
