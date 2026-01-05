from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator


with DAG(
    dag_id='silver_resilient_processor',
    start_date=datetime(2025, 1, 1),
    schedule_interval='*/10 * * * *',
    catchup=False
) as dag:

    # Trips if ANY batch file exists
    wait_for_any_file = FileSensor(
        task_id='wait_for_data',
        filepath='batch_*.parquet', # Wildcard pattern
        fs_conn_id='fs_default',
        poke_interval=30,
        mode='reschedule'
    )

    process_data = PythonOperator(
        task_id='process_all_new_batches',
        python_callable=process_available_files
    )

    wait_for_any_file >> process_data