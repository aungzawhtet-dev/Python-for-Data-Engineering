from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd



DATA_FILES = ['/opt/airflow/dags/data/customers.csv', '/opt/airflow/dags/data/orders.csv']


def process_dateset(file):
    df = pd.read_csv(file)
    print(f"Processing {file} with {len(df)} records.")
    # Add your data processing logic here
    return f"Processed {file}"


with DAG(
    dag_id='dag_multiple_datasets',
    start_date=datetime(2025, 9, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    
    tasks = []
    for file in DATA_FILES:
        task = PythonOperator(
            task_id=f'process_{file.split("/")[-1].split(".")[0]}', # process_customers or process_orders
            python_callable=process_dateset,
            op_args=[file] #  just means: “when the task runs, pass the current file path into my function.”
        )
        tasks.append(task)
        
    
        
        
    """
    Difference between op_args and op_kwargs
    op_args=[...] → positional arguments (like normal function arguments).
    op_kwargs={...} → keyword arguments (like named parameters).
    """