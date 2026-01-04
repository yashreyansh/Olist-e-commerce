from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
from gamma.Scripts.simulate_data import simulate_chunk, send_data
#from Project_Olist.Scripts.simulate_data import simulate_chunk, send_data


with DAG(
    dag_id="SimulateData",
    start_date=datetime(2025, 12, 24),
    schedule_interval="*/1 * * * *",  # every 1 minutes
    catchup=False,
) as dag:

    simulate = PythonOperator(
        task_id="simulate_chunk",
        python_callable=simulate_chunk,
    )
