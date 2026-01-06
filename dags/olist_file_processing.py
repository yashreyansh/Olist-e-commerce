from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from gamma.Scripts.LoadToSilver import process_available_files
from datetime import datetime


with DAG(
    dag_id='silver_resilient_processor',
    start_date=datetime(2025, 1, 1),
    schedule_interval='*/10 * * * *',
    catchup=False
) as dag:

    process_data = PythonOperator(
        task_id='process_all_new_batches',
        python_callable=process_available_files
    )
