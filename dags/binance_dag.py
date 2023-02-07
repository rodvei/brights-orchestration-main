from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from google.cloud import storage

import requests
import datetime
import csv

def date_to_ts(date_time:datetime.datetime):
    return int(date_time.timestamp()) * 1000

def storage_bucket():
    storage_client = storage.Client("brights-orchestration")
    return storage_client.bucket("brights_bucket_1")

def create_binance_blob(storage_bucket):
    return storage_bucket.blob("binance/")

def add_days_klines_to_blob(ds=None, **kwargs):
    bucket = storage_bucket()

    if not bucket.get_blob("binance/"):
        create_binance_blob(bucket)

    date = datetime.datetime.fromisoformat(ds)
    
    symbol = "BTCUSDT"
    interval = "1d"
    startTime = date_to_ts(date - datetime.timedelta(days=1))
    
    klines = get_klines(
        symbol,
        interval,
        startTime,
        limit=24
    )
    
    blob = bucket.blob(f"binance/{symbol}-{startTime}")

    headers = (
        "kline_open_time",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "kline_close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume"
    )

    with blob.open("w") as file:
        csv_writer = csv.writer(file, lineterminator="\n")
        csv_writer.writerow(headers)

        for kline in klines:
            csv_writer.writerow(kline[:-1])
    
    

def get_klines(
    symbol: str = "BTCUSDT", 
    interval:str = "1d", 
    startTime:float=date_to_ts(datetime.datetime.now() - datetime.timedelta(days=1)) , 
    endTime:float=date_to_ts(datetime.datetime.now()), 
    limit:int = 6
):
    URL = f"https://www.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&startTime={startTime}&endTime={endTime}&limit={limit}"
    
    resp = requests.get(URL)
    print(resp.url)
    if resp:
        return resp.json()
    else:
        raise requests.HTTPError(resp)


with DAG(
   dag_id="binance_daily",
   start_date=datetime.datetime(2023, 2, 7),
   schedule=datetime.timedelta(days=1)
) as dag:

    daily_klines = PythonOperator(
        task_id="daily_klines",
        python_callable=add_days_klines_to_blob,
    )

    create_bq_from_gcs = GCSToBigQueryOperator(
        task_id = "binance_klines_to_bq",
        bucket = "brights_bucket_1",
        source_objects = ["binance/*"],
        destination_project_dataset_table="brights_datasets.alexander_table",
        schema_fields = [
            {"name":"kline_open_time", "type":"INTEGER", "mode":"NULLABLE"},
            {"name":"open_price", "type":"NUMERIC", "mode":"NULLABLE"},
            {"name":"high_price", "type":"NUMERIC", "mode":"NULLABLE"},
            {"name":"low_price", "type":"NUMERIC", "mode":"NULLABLE"},
            {"name":"close_price", "type":"NUMERIC", "mode":"NULLABLE"},
            {"name":"volume", "type":"NUMERIC", "mode":"NULLABLE"},
            {"name":"kline_close_time", "type":"INTEGER", "mode":"NULLABLE"},
            {"name":"quote_asset_volume", "type":"NUMERIC", "mode":"NULLABLE"},
            {"name":"number_of_trades", "type":"NUMERIC", "mode":"NULLABLE"},
            {"name":"taker_buy_base_asset_volume", "type":"NUMERIC", "mode":"NULLABLE"},
            {"name":"taker_buy_quote_asset_volume", "type":"NUMERIC", "mode":"NULLABLE"}
        ], 
        write_disposition="WRITE_TRUNCATE",
    )
    
    daily_klines >> create_bq_from_gcs
