from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'ahmad',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='process_sales_spark',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['spark', 'etl'],
) as dag:

    # Task: Submit Job ke Spark Cluster
    process_data = SparkSubmitOperator(
        task_id='submit_spark_job',
        conn_id='spark_default',         # ID koneksi yang kita buat tadi
        application='/opt/airflow/dags/spark_etl.py', # Lokasi script di dalam container Airflow
        verbose=True
    )

    process_data