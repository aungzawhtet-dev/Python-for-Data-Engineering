from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
from clean_csv_module import clean_csv  # import the function (clean.py must be in the same folder as this DAG file)

# File paths relative to DAG
BASE_DIR = Path(__file__).parent # this code is used to get the parent directory of the current file
INPUT_FILE = BASE_DIR / "data/input/sample.csv" # this code is used to get the path of the input file
OUTPUT_FILE = BASE_DIR / "data/output/cleaned_sample.csv"

default_args = {
    "owner": "AZH",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="cleaning_csv_file_dag",  # updated DAG ID
    default_args=default_args,
    start_date=datetime(2025, 9, 18),
    schedule_interval="@daily",
    catchup=False,
    tags=["mini_project"],
) as dag:

    clean_task = PythonOperator(
        task_id="clean_csv_task",
        python_callable=clean_csv,
        op_kwargs={
            "input_path": str(INPUT_FILE),
            "output_path": str(OUTPUT_FILE),
        },
    )

    clean_task
