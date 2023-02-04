# brights-orchestration
Created by Åsmund Brekke
Lets' learn how to orchestrate tasks using Airflow!


## Webserver/UI
The Airflow webserver is available [here](https://3014f0a0fcb64835a1878072f62dbc45-dot-europe-west1.composer.googleusercontent.com/).
Ask Kristoffer for access!

## Introduction

DAGs are defined as Python files in the [dags](./dags/) directory. Have a look at the [example_dag](./dags/example_dag.py) to see how they are defined.
Although the example dag uses a Python Operator, there are also [many](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/index.html) other operators that allow you to do everything from sending emails to glue together [tons of different services](https://airflow.apache.org/docs/#providers-packagesdocsapache-airflow-providersindexhtml)


## Exercises

Try to create your own DAGs and use them to run some python code! For instance, querying an API and storing the result in [Google Cloud Storage](https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-code-sample).
