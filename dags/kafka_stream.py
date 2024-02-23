import uuid
import api_data
import csv
import gzip
import time
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

default_args = {
    'owner': 'parliamentary_quota',
    'start_date': datetime(2023, 2, 20, 10, 00)
}

def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    # Caso não tenha, cadastre-se no Brasil.io e gere seu Token
    # Para mais instruções: https://blog.brasil.io/2020/10/10/como-acessar-os-dados-do-brasil-io/
    user_agent = "brunojaciel"
    auth_token = "972fcc73b93d75aa832018e4023ea663ef98c69d"

    api = api_data.BrasilIO(user_agent, auth_token)

    dataset_slug = "gastos-deputados"
    table_name = "cota_parlamentar"

    # Download do arquivo completo
    file_path = f"{dataset_slug}_{table_name}.csv.gz"
    api.download(dataset_slug, table_name, file_path)

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            # Leitura do arquivo em memória
            with gzip.open(file_path, mode="rt") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Generate UUID for each row
                    row['id'] = str(uuid.uuid4())

                    # Convert row to bytes and publish to Kafka
                    message = ','.join([f"{key}:{value}" for key, value in row.items() if key != 'id'])
                    message = f"id:{row['id']}, {message}".encode('utf-8')

                    producer.send('parliamentary_quota_data', message)
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            continue

with DAG('user_automation',
         default_args=default_args,
         schedule='@monthly',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data',
        python_callable=stream_data
    )
