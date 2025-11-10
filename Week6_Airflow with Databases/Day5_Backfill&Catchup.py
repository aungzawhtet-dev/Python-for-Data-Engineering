from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# default_args = {
#     "owner": "airflow",
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }

def print_date(**kwargs):
    ds = kwargs['ds']
    print(f"Running for date is {ds}")

with DAG(
    dag_id="backfill_catchup_example",
    # default_args=default_args,
    start_date=datetime(2025, 9, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    run_task_task = PythonOperator(
        task_id="print_execution_date",
        python_callable=print_date,
    )


"""
Practice:
Run airflow dags backfill -s 2025-09-01 -e 2025-09-25 backfill_example
It will run historical dates.
Change catchup=False and see the difference.
 """