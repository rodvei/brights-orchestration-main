import datetime
import requests
import json
import os
import csv
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "GlenRoger",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(2022, 8, 21),
}


def run(**kwargs):
    # This function will be called by airflow, specified by python_callable in teh PythonOperator
    print(kwargs)

def get_released_games(**kwargs):

    release_date = kwargs['ds']  #Inneholder dateon vi skal kjøre spørringen

    url = f"https://api.rawg.io/api/games?key=fd484827b3dd46a2b26ae6fce116905a&dates={release_date},{release_date}"

    bucket_name = 'brights_bucket_1'
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

            game_dict_to_save['games'].append(response['results'][i]['name'])

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
        blob = bucket.blob(os.path.join(fr'glenroger_test_folder/{blob_name}')


        with blob.open('w') as f:  # You will need 'wb' mode in Python 2.x
            
            f.write('release_date;game\n')
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
        python_callable=howdy_world # This is the function that airflow will run 
    )


if __name__ == "__main__":
    print("Hell-o!")