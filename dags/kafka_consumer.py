from kafka import KafkaConsumer
from clickhouse_driver import Client
import json
import logging
import clickhouse_connect

logger = logging.getLogger(__name__)

def consume_from_kafka_and_save_to_clickhouse(
    topic='transaction_created',
    bootstrap_servers='localhost:9092',
    clickhouse_host='localhost',
    database='default',
    table='transactions'
):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id='clickhouse-consumer-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    ch_client = Client(clickhouse_host)

    for message in consumer:
        value = message.value
        logger.info(f"Получено сообщение: {value}")

        try:
            ch_client.execute(
                f"INSERT INTO {database}.{table} VALUES",
                [value]
            )
        except Exception as e:
            logger.error(f"Ошибка при записи в ClickHouse: {e}")


# dags/kafka_consumer_dag.py

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.data_consumer import consume_from_kafka_and_save_to_clickhouse

with DAG(
    'kafka_to_clickhouse',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    consume_task = PythonOperator(
        task_id='consume_from_kafka_and_save_to_clickhouse',
        python_callable=consume_from_kafka_and_save_to_clickhouse
    )