from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Konfigurasi Dasar DAG
default_args = {
    'owner': 'ahmad',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definisi DAG
with DAG(
    dag_id='hello_world_airflow',
    default_args=default_args,
    description='DAG Pertama Ahmad',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 1: Print tanggal hari ini pakai Command Line Linux
    task_1 = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    # Task 2: Tidur 5 detik (Simulasi proses lama)
    task_2 = BashOperator(
        task_id='sleep',
        bash_command='sleep 5'
    )

    # Task 3: Print selesai
    task_3 = BashOperator(
        task_id='print_finish',
        bash_command='echo "Selesai Bos!"'
    )

    # Mengatur Urutan (Dependencies)
    # Task 1 jalan dulu, kalau sukses baru Task 2, lalu Task 3
    task_1 >> task_2 >> task_3