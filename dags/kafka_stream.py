from datetime import datetime, timedelta
from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import os, json, subprocess, csv
import pandas as pd
import json
from kafka import KafkaProducer
import time

#переносим данные из kafka + добавляем ключ id в эти данные и берем только определенные поля

def get_dataset_kaggle(dataset, filename):
    if os.path.exists(filename):
        print(f"{filename} уже есть, пропускаем скачивание")
        return
    
    os.environ['KAGGLE_CONFIG_DIR'] = '/home/dzharkova/diplom-pipeline/.kaggle'
    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset=dataset, path='.', unzip=True)
    print(f"Файл успешно распакован {dataset}")
    

def stream_data(dataset, filename, topic, batch_size=1000, max_records=100000, sleep_seconds=0.1):

    get_dataset_kaggle(dataset, filename) 

    producer = KafkaProducer(
        bootstrap_servers = ['kafka:9092'], 
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    with open(filename, encoding = 'ISO-8859-1') as csv_file_fraud:
        csv_reader = csv.DictReader(csv_file_fraud)
        count = 0
        for row in csv_reader:
            if count >= max_records:
                break
            try:
                producer.send(value=row)
                count += 1

                if count % batch_size == 0:
                    print(f"Отправлено {count} записей")
                    producer.flush()
                    time.sleep(sleep_seconds)

            except Exception as e:
                print(f"Ошибка при отправке строки: {e}")

        producer.flush()
        print(f"Всего отправлено записей: {count}")



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='kafka_transaction_stream',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    send_data_task = PythonOperator(
        task_id='send_data_to_kafka',
        python_callable=stream_data,
        op_kwargs={
            'dataset':  'ealaxi/paysim1',
            'filename': 'PS_20174392719_1491204439457_log.csv',
            'topic': 'transaction_created',
            'batch_size': 1000,
            'max_records': 100000,
            'sleep_seconds': 0.1

        }
    )