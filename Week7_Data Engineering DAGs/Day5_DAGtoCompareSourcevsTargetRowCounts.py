from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd


SOURCE_FILE = '/opt/airflow/dags/data/customers_source.csv'
TARGET_FILE = '/opt/airflow/dags/data/customers_target.csv'


def compare_row_counts():
    source_df = pd.read_csv(SOURCE_FILE)
    target_df = pd.read_csv(TARGET_FILE)
    
    source_count = len(source_df)
    target_count = len(target_df)
    
    if source_count != target_count:
        raise ValueError(f"Row count mismatch! Source: {source_count}, Target: {target_count}")
    
    print(f"Row counts match!")
    
    

with DAG(
    dag_id = 'dag_compare_row_counts',
    start_date = datetime(2025, 9, 1),
    schedule_interval = '@daily',
    catchup = False
) as dag:
    
    task = PythonOperator(
        task_id='compare_row_counts',
        python_callable=compare_row_counts
    )