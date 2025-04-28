from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

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


etl_task = BashOperator(
    task_id='run_log_analysis',
    bash_command='./spark_job_submit.sh',  # Path to your .sh script
    dag=dag,
)
