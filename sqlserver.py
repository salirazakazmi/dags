from airflow import DAG
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

# Declare Dag
@dag(schedule_interval=None, start_date=datetime(2023, 11, 3), catchup=False, tags=['sqlserver_gcp'])
# Define Dag Function
def sqlserver_test_DAG():
    # Define tasks
    @task()
    def sql_extract():
        try:
            hook = MsSqlHook(mssql_conn_id="sqlserver")
            sql = "SELECT Count(*) TotalRecords FROM LiveStreaming"
            
            df = hook.get_pandas_df(sql)
            total_records = df.iloc[0]['TotalRecords']
            
            # Write the "Total Records" value to a .txt file
            file_path = r"/mnt/d/SampleData/sales/dest/total_records.txt"

            with open(file_path, "w") as outfile:
                outfile.write(f"Total Records: {total_records}")

            print("Total Records written to file successfully.")
        except Exception as e:
            print("Data extract error: " + str(e))

    # Define the task dependencies
    extract_task = sql_extract()

gcp_extract_and_load = sqlserver_test_DAG()



# from airflow import DAG
# from datetime import datetime
# from airflow.decorators import dag, task
# from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

# # Declare Dag
# @dag(schedule_interval=None, start_date=datetime(2023, 11, 3), catchup=False, tags=['sqlserver_gcp'])
# # Define Dag Function
# def sqlserver_test_DAG():
#     # Define tasks
#     @task()
#     def sql_extract():
#         try:
#             hook = MsSqlHook(mssql_conn_id="sqlserver")
#             sql = "SELECT Count(*) TotalRecords FROM LiveStreaming"
            
#             df = hook.get_pandas_df(sql)
#             total_records = df.iloc[0]['TotalRecords']
            
#             # Push the "Total Records" value to XCom
#             ti.xcom_push(key="total_records", value=total_records)
            
#             # Write the "Total Records" value to a .txt file
#             file_path = r"/mnt/d/SampleData/sales/dest/total_records.txt"
#             with open(file_path, "w") as outfile:
#                 outfile.write(f"Total Records: {total_records}")
#         except Exception as e:
#             print("Data extract error: " + str(e))

#     sql_extract()

# gcp_extract_and_load = sqlserver_test_DAG()
