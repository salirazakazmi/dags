# from airflow import DAG
# from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.decorators import task, dag
# from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
# import time 
# from datetime import datetime, timedelta
# import pyodbc
# import os

# # Define your DAG
# default_args = {
#     'owner': 'admin',
#     'start_date': datetime(2023, 11, 3),
#     'retries': 1,
# }

# dag = DAG(
#     'testsqlserver_DAG',
#     default_args=default_args,
#     schedule_interval=None,  # You can set a schedule interval here if you want periodic runs

#     catchup=False,
# )

# # Task 1: Read CSV file from a folder
# def testsqlserver():
#     server="DSL-AliY"
#     username="sa"
#     password="Hbl@1234"
#     database="EDW"
#     ddw_connection = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password+';TrustServerCertificate=yes;')
#     query1 = "select 1"
#     print(query1)
#     print(ddw_connection)
#     cursor = ddw_connection.cursor()
#     queryresult  = cursor.execute(query1)
#    ##database_names    = [db.ddw_databasename for db in databases_to_sync]
#     print(queryresult)

# task_read_csv = PythonOperator(
#     task_id='testsqlserver',
#     python_callable=testsqlserver,
#     dag=dag,
# )


# default_args = {
#     "start_date": datetime(2022, 1, 1)
# }


# testsqlserver