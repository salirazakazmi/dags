
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define your DAG
default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 11, 3),
    'retries': 1,
}

dag = DAG(
    'readDB_DAG',
    default_args=default_args,
    schedule_interval=None,  # You can set a schedule interval here if you want periodic runs
    catchup=False,
)

# Task 1: Extract data from SQL Server and save it to a CSV file
def extract_data_to_csv():
    print("Task 1: Extract data from SQL Server and save it to a CSV file")

task_extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data_to_csv,
    dag=dag,
)
