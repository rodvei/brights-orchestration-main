import datetime as dt
import requests
import csv
import os

bucket_name = 'date_fact'
today = dt.datetime.today()
file_name = f"{today.day}.{today.month}.{today.year}"
day = today.day
month = today.month
blob_name = f'date_fact{month}/{day}.csv'
url = f"http://numbersapi.com/{month}/{day}/date"
res = requests.get(url)
type(res)
type(res.text)
data = []
data.append({'date': file_name, 'text': res.text})
#storage_client = storage.Client()
#bucket = storage_client.bucket(bucket_name)
#blob = bucket.blob(os.path.join('preparation_test_folder', blob_name))
with open(f'{file_name}.csv', "w", newline='') as file:
    header = ['date', 'text']
    writer = csv.DictWriter(file, fieldnames=header, extrasaction='ignore')
    writer.writeheader()

    for text in data:
        writer.writerow(text)