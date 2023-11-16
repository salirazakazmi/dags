# This is readfile code, add for testing this line
# this test 

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import shutil

# Define your DAG
default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 11, 3),
    'retries': 1,
}

dag = DAG(
    'readfile_DAG',
    default_args=default_args,
    schedule_interval=None,  # You can set a schedule interval here if you want periodic runs
    # schedule_interval=timedelta(days=1, hours=15),  # Daily at 3:00 PM
    # schedule_interval=timedelta(hours=1),  # Daily, starting one hour after the previous run
    # schedule_interval=timedelta(minutes=15),  # Every 15 minutes
    # schedule_interval='0 0 * * 0',  # Every Sunday at 12:00 AM (midnight)
    catchup=False,
)

'''
* * * * *: This corresponds to the following fields:
Minute: * (every minute, 0-59)
Hour: * (every hour, 0-23)
Day of Month: * (every day of the month, 1-31)
Month: * (every month, 1-12 or month names)
Day of Week: * (every day of the week, 0-6 or day names)
'''

# Task 1: Read CSV file from a folder
def read_csv():
    # source_folder = r"D:\SampleData\sales"
    # Read your CSV file from the source folder and perform any necessary processing
    # Example: df = pd.read_csv(source_folder + 'your_file.csv')
    print(f"Task 1: Read CSV file from a folder")

task_read_csv = PythonOperator(
    task_id='read_csv',
    python_callable=read_csv,
    dag=dag,
)

# Task 2: Copy the CSV file into another folder
def copy_csv():
    source_folder = '/mnt/d/SampleData/sales'
    destination_folder = '/mnt/d/SampleData/sales/dest'
    file_to_copy = 'sales_data_sample.csv'

    source_file = source_folder + '/' + file_to_copy
    destination_file = destination_folder + '/' + file_to_copy

    try:
        shutil.copyfile(source_file, destination_file)
        print("Task 2: Copy the CSV file into another folder")
    except Exception as e:
        print(f"Error copying file: {str(e)}")

task_copy_csv = PythonOperator(
    task_id='copy_csv',
    python_callable=copy_csv,
    dag=dag,
)
# # Task 2: Copy the CSV file into another folder
# def copy_csv():
#     source_folder = r"D:\SampleData\sales"  # Use "r" before the string to treat it as a raw string
#     destination_folder = r"D:\SampleData\sales\dest"  # Use "r" before the string to treat it as a raw string
#     file_to_copy = 'sales_data_sample.csv'
#     # Copy the CSV file from the source folder to the destination folder
#     shutil.copyfile(source_folder + '\\' + file_to_copy, destination_folder + '\\' + file_to_copy)
#     print("Task 2: Copy the CSV file into another folder")

# task_copy_csv = PythonOperator(
#     task_id='copy_csv',
#     python_callable=copy_csv,
#     dag=dag,
# )

# Define the task dependencies
task_read_csv >> task_copy_csv
