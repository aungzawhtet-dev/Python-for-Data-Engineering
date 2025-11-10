"""
DAG Idea:
Task 1: Count rows in MongoDB.
Task 2: Count rows in Postgres.
Task 3: Compare counts -> success if match, fail otherwise.
Runs weekly (@weekly).
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient
import psycopg2
from airflow.hooks.base import BaseHook



def count_mongo(**context):
    # Fetch MongoDB connection from Airflow UI
    conn = BaseHook.get_connection("mongodb_conn_report_new")  # Replace with your MongoDB connection ID
    extra = conn.extra_dejson  # Get additional parameters from the "Extra" field
    # Construct the MongoDB URI for SRV
    mongo_uri = f"mongodb+srv://{conn.login}:{conn.password}@{conn.host}"

    # Remove the `srv` key from extra parameters if it exists
    extra.pop("srv", None)

    # Pass only valid parameters to MongoClient
    client = MongoClient(mongo_uri, **extra)
    db = client["ayapay"]
    collection = db["offerfactory"]
    count = collection.count_documents({})
    context['ti'].xcom_push(key="mongo_count", value=count)

def count_postgres(**context):    
    # Fetch PostgreSQL connection from Airflow UI
    conn = BaseHook.get_connection("postgres_ayapay")  # Replace with your PostgreSQL connection ID

    
    # Connect to PostgreSQL using psycopg2
    conn_pg = psycopg2.connect(
            dbname=conn.schema,
            user=conn.login,
            password=conn.password,
            host=conn.host,
            port=conn.port
    )
    cur = conn_pg.cursor()
    cur.execute("SELECT COUNT(*) FROM offerfactory;")
    count = cur.fetchone()[0]
    cur.close()
    conn_pg.close()
    context['ti'].xcom_push(key="pg_count", value=count)

def compare_counts(ti):
    mongo_count = ti.xcom_pull(task_ids="count_mongo", key="mongo_count")
    pg_count = ti.xcom_pull(task_ids="count_postgres", key="pg_count")
    if mongo_count == pg_count:
        print(f" Counts Match: Mongo={mongo_count}, Postgres={pg_count}")
    else:
        raise ValueError(f" Counts Mismatch: Mongo={mongo_count}, Postgres={pg_count}")

with DAG(
    dag_id="dag_mongo_vs_postgres_rowcount",
    start_date=datetime(2025, 9, 1),
    schedule_interval="@weekly",
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="count_mongo",
        python_callable=count_mongo
    )

    task2 = PythonOperator(
        task_id="count_postgres",
        python_callable=count_postgres
    )

    task3 = PythonOperator(
        task_id="compare_counts",
        python_callable=compare_counts
    )

    [task1, task2] >> task3
