from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import os
import pandas as pd


DATA_FILE = '/opt/airflow/dags/data/customers.csv'  # csv file path in the DAG folder

def check_file():
    if os.path.exists(DATA_FILE):
        return 'process_data' 
    else:
        return 'skip_task'
    
    
    
def process_data():
    df = pd.read_csv(DATA_FILE)
    print("Data processed:")
    print(df)



with DAG(
    dag_id = 'dag_branching_example',
    start_date = datetime(2025, 9, 1),
    schedule_interval = '@daily',
    catchup = False
) as dag:
    
    branching = BranchPythonOperator(
        task_id = 'check_file_exists',
        python_callable = check_file
        
    )
    
    
    process = PythonOperator(
        task_id = 'process_data',
        python_callable = process_data
    )
    
    skip = DummyOperator(task_id = 'skip_task')
    
    branching >> [process, skip]
        
    