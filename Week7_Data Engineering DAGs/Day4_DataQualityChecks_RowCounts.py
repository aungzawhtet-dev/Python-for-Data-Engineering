from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

DATA_FILE = '/opt/airflow/dags/data/customers.csv'


def check_row_count():
    df = pd.read_csv(DATA_FILE)
    if len(df) == 0:
        raise ValueError("Data quality check failed: No rows found!")
    print(f"Data quality check passed: {len(df)} rows found.")
    
    
with DAG(
    dag_id = 'dag_data_quality_checks',
    start_date = datetime(2025, 9, 1),
    schedule_interval = '@daily',
    catchup = False
) as dag:
    
    task = PythonOperator(
        task_id='check_row_count',
        python_callable=check_row_count
    )