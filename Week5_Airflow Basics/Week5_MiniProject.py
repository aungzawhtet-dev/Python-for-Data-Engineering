from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path

# Define cleaning function
def clean_csv(input_path: str, output_path: str):
    input_path = Path(input_path)
    output_path = Path(output_path)
    
    # Ensure output folder exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df = pd.read_csv(input_path)
    df = df.dropna(subset=['name', 'email'])
    df = df[df['email'].str.contains(r"@")]
    
    median_age = df['age'].median()
    df['age'] = df['age'].fillna(median_age)
    
    df.to_csv(output_path, index=False)
    print(f"Cleaned CSV saved to {output_path}")

# File paths relative to DAG
BASE_DIR = Path(__file__).parent
INPUT_FILE = BASE_DIR / "data/input/sample.csv"
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
    dag_id="csv_cleaning_dag",
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
