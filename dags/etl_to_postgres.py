import api_data
import csv
import gzip
from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Bruno_Jaciel',
    'start_date': datetime(2023, 2, 20, 10, 00)
}

def get_data():
    postgres_hook = PostgresHook(postgres_conn_id='postgres-airflow', schema='questions-db')

    # Extract data

    # If you don't have one, register on Brasil.io and generate your Token
    # For further instructions: https://blog.brasil.io/2020/10/10/como-acessar-os-dados-do-brasil-io/
    user_agent = "brunojaciel"
    auth_token = "972fcc73b93d75aa832018e4023ea663ef98c69d"

    api = api_data.BrasilIO(user_agent, auth_token)

    dataset_slug = "gastos-deputados"
    table_name = "cota_parlamentar"

    data_list = []  # List to store filtered data

    # Download the full file
    file_path = f"{dataset_slug}_{table_name}.csv.gz"
    api.download(dataset_slug, table_name, file_path)

    # Reading the file into memory
    with gzip.open(file_path, mode="rt") as f:
        reader = csv.DictReader(f)
        for row in reader:

            # Transform data

            # Check if these items are empty strings
            numano = int(row['numano']) if row['numano'] else 0
            sgpartido = row['sgpartido'] if row['sgpartido'] else ''
            sguf = row['sguf'] if row['sguf'] else ''
            txtdescricao = row['txtdescricao'] if row['txtdescricao'] else ''
            txttrecho = row['txttrecho'] if row['txttrecho'] else ''

            # Handle empty strings in vlrliquido, vlrrestituicao, and nucarteiraparlamentar
            vlrliquido = float(row['vlrliquido']) if row['vlrliquido'] else 0.0
            vlrrestituicao = float(row['vlrrestituicao']) if row['vlrrestituicao'] else 0.0
            nucarteiraparlamentar = int(row['nucarteiraparlamentar']) if row['nucarteiraparlamentar'] else 0

            # Handle empty string in datemissao field
            datemissao = row['datemissao'] if row['datemissao'] else None

            filtered_row = {
                'numano': numano,
                'datemissao': datemissao,
                'sgpartido': sgpartido,
                'sguf': sguf,
                'txtdescricao': txtdescricao,
                'vlrliquido': vlrliquido,
                'vlrrestituicao': vlrrestituicao,
                'nucarteiraparlamentar': nucarteiraparlamentar,
                'txttrecho': txttrecho
            }
            data_list.append(filtered_row)

        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        # Load data

        try:
            for row in data_list:
                cur.execute(
                    f"INSERT INTO {table_name} (numano, datemissao, sgpartido, sguf, txtdescricao, "
                    "vlrliquido, vlrrestituicao, nucarteiraparlamentar, txttrecho) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (row['numano'], row['datemissao'], row['sgpartido'],
                     row['sguf'], row['txtdescricao'], row['vlrliquido'],
                     row['vlrrestituicao'], row['nucarteiraparlamentar'], row['txttrecho'])
                )

            conn.commit()

        except Exception as e:
            print(f"Error inserting rows: {e}")
            conn.rollback()

        finally:
            cur.close()
            conn.close()

with DAG('answer_questions',
        default_args=default_args,
        template_searchpath= '/opt/airflow/sql',
        schedule='@monthly',
        catchup=False) as dag:

    create_table_db = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres-airflow',
        sql='create_table_db.sql'
    )

    get_data_api = PythonOperator(
        task_id='get_data',
        python_callable=get_data
    )

    create_table_db >> get_data_api
