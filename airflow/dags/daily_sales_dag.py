from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
import psycopg2
import os

# config
DB_CONFIG = {
    "host": "host.docker.internal",
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "port": os.getenv("DB_PORT")
}
DATA_PATH = "/opt/airflow/data"
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# extract
def extract_raw_data(**context):
    print("[*] Memastikan data mentah siap di Postgres...")
    # di skenario dbt, biasanya data mentah sudah ada di db (elt)
    return "Data Ready"

# definisi dag
default_args = {
    'owner': 'ahmad',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG('daily_sales_dbt_dag', default_args=default_args, schedule_interval='0 6 * * *', catchup=False) as dag:
    t1 = PythonOperator(
        task_id='extract_raw_data',
        python_callable=extract_raw_data
    )
    
    t2 = BashOperator(
        task_id='dbt_transform',
        bash_command=f'dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}',
    )
    
    t3 = BashOperator(
        task_id='dbt_test',
        bash_command=f'dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}',
    )
    
    t1 >> t2 >> t3