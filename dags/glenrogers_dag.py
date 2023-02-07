
import requests
import json
import os
import datetime
from datetime import datetime, timedelta
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

BUCKET_NAME = 'brights_bucket_1'
BLOB_STAGING_PATH = r'glenroger_test_folder/'
BLOB_NAME = ""
BQ_PROJECT = 'brights-orchestration'
BQ_DATASET_NAME = 'brights_datasets'
BQ_TABLE_NAME = 'glen_roger_table'


default_args = {
    "owner": "GlenRoger",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2022, 8, 21),
    # "retry_delay": datetime.timedelta(minutes=5),
    # "start_date": datetime.datetime(2022, 8, 21),
}


def run(**kwargs):
    # This function will be called by airflow, specified by python_callable in teh PythonOperator
    print(kwargs)

def get_released_games(**kwargs):

    release_date = kwargs['ds']  #Inneholder dateon vi skal kjøre spørringen

    #Apparently we get strings from kwargs, need to convert to proper
    #date-format.
    release_date = datetime.strptime(release_date, "%Y-%m-%d")
    release_date = datetime.date(release_date)

    release_date = release_date - timedelta(days=1)

    url = f"https://api.rawg.io/api/games?key=fd484827b3dd46a2b26ae6fce116905a&dates={release_date},{release_date}"

    bucket_name = 'brights_bucket_1'
    BLOB_NAME = f'games_released_{release_date}.csv'
    blob_name = f'games_released_{release_date}.csv'

    resp = requests.get(url)
    response = resp.json()

    dict_to_save = {}
    dict_to_save['games'] = []

    save_str_to_file = ""

    game_dict_to_save = {}
    # game_dict_to_save['release_date'] = str(release_date)
    game_dict_to_save['games'] = []

    if resp.ok:

        str_top_games = f"Some games release at {release_date}:"
        save_str_to_file = str_top_games +"\n"
        save_str_to_file += "#"*(len(str_top_games))  +"\n"


        for i in range(0, len(response['results'])):

            add_space = ""
            if i + 1 < 10:
                add_space = " "
            save_str_to_file += f"\t{add_space}{i+1}: {response['results'][i]['name']} / ("

            # game_dict_to_save['games'].append(response['results'][i]['name'])
            game_dict_to_save['games'].append(f"{response['results'][i]['name']};{response['results'][i]['id']}")

            if response['results'][i]['platforms'] != None:
                for j in response['results'][i]['platforms']:
                    save_str_to_file += j['platform']['name'] +", "

                save_str_to_file = save_str_to_file[:-2]+")\n"
                
        dict_to_save['games'].append(game_dict_to_save)


        #Save on all the formats!!
        # with open(path+filename_without_prefix+".json", 'w') as json_out:
        #     json_out.write(json.dumps(dict_to_save))        

        # with open(path+filename_without_prefix+".txt", "w") as txt_out:
        #     txt_out.write(save_str_to_file)
            


        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(os.path.join(fr'glenroger_test_folder/{blob_name}'))


        with blob.open('w') as f:  # You will need 'wb' mode in Python 2.x
            
            f.write('release_date;game;game_id\n')
            for i in dict_to_save['games'][0]['games']:
                line_to_write = f"{release_date};{i}\n"
                f.write(line_to_write)            
        
        blobs = storage_client.list_blobs(bucket_name)            

    else:
        # save_str_to_file(f"Could not get response from {url}")
        print(f"Could not get response from {url}")  


with DAG(
    dag_id ="gr_test_dag",
    description="This is gr's first test dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:

    run_python_task = PythonOperator(
        task_id="gr_howdy_world", # This controls what your task name is in the airflow UI 
        python_callable=get_released_games # This is the function that airflow will run 
    )



    task_csv_load = GCSToBigQueryOperator(
        task_id="load_games_csv_to_gsc", # This controls what your task name is in the airflow UI 
        bucket=BUCKET_NAME, # This is the function that airflow will run 
        source_objects=[BLOB_STAGING_PATH+BLOB_NAME],
        create_disposition='CREATE_IF_NEEDED',
        destination_project_dataset_table=f"{BQ_PROJECT}:{BQ_DATASET_NAME}.{BQ_TABLE_NAME}",
        schema_fields=[
            {'date': 'release_date', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'game', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'game_id': 'game_id', 'type': 'STRING', 'mode': 'REQUIRED'}],
        write_disposition='WRITE_TRUNCATE'
    )

run_python_task>>task_csv_load


#til GCS,  

if __name__ == "__main__":
    print("Hell-o!")