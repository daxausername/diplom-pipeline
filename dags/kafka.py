from datetime import datetime 
from airflow import DAG 
from airflow.operators.python import PythonOperator
import os, json, subprocess, csv
import pandas as pd

default_args = {
    'owner': 'dashaair', 
    'start_date': datetime(2024, 10, 3, 10, 00)

}


"""
можно сделать функцию что если файл zip уже есть то не обращаться в к kaggle 
"""


dataset = "ealaxi/paysim1"
filename = 'on.zip/PS_20174392719_1491204439457_log.csv'

def get_dataset_kaggle(dataset, filename):
    # тут нужен оброботчик ибо если файл уже есть нахрен его качать 
    if os.path.exists(filename):
        return from_csv_to_json()
    os.environ['KAGGLE_CONFIG_DIR'] = '.kaggle'
    import kaggle

    kaggle.api.dataset_download_files(dataset = dataset, unzip=True)
    return from_csv_to_json()
    

def from_csv_to_json():
    
    with open(filename, encoding = 'ISO-8859-1') as csv_file_fraud:
        # а как мне обработать именно 1000000 строк в csv файле а потом сместиться по строчкам и считать следующие 100000 в рамках исполнения dag(a)
        csv_reader = csv.DictReader(csv_file_fraud)
        data = []
        for i, row in enumerate(csv_reader):
            if i == 10:
                break
            data.append(row)


        
    json_output = json.dumps(data, indent = 4)
    return json_output





"""
нужна функция которая будет распаковывать мой  джейсон и слать батами по 100000 записей
еще есть вариант разделить эти данные по колонкам как будто они из разных мест и представляют собой данные из разных микросервисов и слать это дело на разные топики 
потом записывать инфу в слои и делать представление data mart - витринку как в kaggle и производить аналитику
кайф
"""


def stream_data():
    import json
    from kafka import KafkaProducer
    import time



    json_dataset = get_dataset_kaggle(dataset, filename)() # извлекаем данные из kaggle в json формате
    # res = format_data(res) получаем  данные форматирвоанные через функцию

    producer = KafkaProducer(bootstrap_servers = ['broker:29092'], max_block_ns = 5000)

    producer.send('users_created', json.dumps(json_dataset).encode('utf-8'))




with DAG('pipeline_froad', 
         default_args = default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable = stream_data
    )

stream_data()

