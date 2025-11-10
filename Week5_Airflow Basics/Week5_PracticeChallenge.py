from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator




# Python function
def greet():
    message = "Hello from Airflow! I'm running inside a DAG."
    print(message)  # This will appear in Airflow UI logs
    return message   # optional, can be pulled via XCom



# Default args

default_args = {
    "owner": "AZH",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}



# DAG Definition

with DAG(
    dag_id="week5_day6_practice_challenge",
    default_args=default_args,
    description="Practice DAG: print greeting and system date daily",
    start_date=datetime(2025, 9, 20),   # set in the past so it runs immediately
    schedule_interval="@daily",         # runs once per day
    catchup=False, # does not backfill
    tags=["practice", "basics"], # this code is part of the practice challenge
) as dag:

    # Task 1: PythonOperator
    task_greet = PythonOperator(
        task_id="say_hello",
        python_callable=greet
    )

    # Task 2: BashOperator
    task_print_date = BashOperator(
    task_id="print_date",
    bash_command="echo ' System date is:' $(date)",
    do_xcom_push=True # pushes stdout to XCom (optional)
    )

    # Task 3: BashOperator with more logic
    task_whoami = BashOperator(
    task_id="whoami_task",
    bash_command="echo ' Current user:' $(whoami)",
    do_xcom_push=True   # pushes stdout to XCom (optional)
)

   
    # Task Dependencies

    task_greet >> task_print_date >> task_whoami
