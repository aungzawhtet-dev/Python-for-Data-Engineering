from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random


def unstable_task():
    if random.random() < 0.7: # 70% chance to fail
        raise ValueError("Random failure! Retry will happen.")
    print("Task succeeded!")
    return "Task Success"


with DAG(
    dag_id = 'dag_retray_error_handling',
    start_date = datetime(2025, 9, 1),
    schedule_interval = '@daily',
    catchup = False
) as dag:
    
     task = PythonOperator(
        task_id='unstable_task',
        python_callable=unstable_task,
        retries=3,
        retry_delay=timedelta(seconds=10)  # wait 10 seconds before retrying
    )

    



