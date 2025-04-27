from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import subprocess

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 27),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'weblog_etl',
    default_args=default_args,
    description='ETL pipeline for web logs using PySpark',
    schedule_interval='@daily',
    catchup=False
)

def run_spark_program():
    subprocess.run(['spark-submit', 'main_etl.py'], check=True)

etl_task = PythonOperator(
    task_id='run_log_analysis',
    python_callable=run_spark_program,
    dag=dag
)
