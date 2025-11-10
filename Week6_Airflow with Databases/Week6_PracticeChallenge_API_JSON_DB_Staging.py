import requests
import json
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime


def fetch_api_to_staging():
    url = "https://jsonplaceholder.typicode.com/users"
    response = requests.get(url)
    data = response.json()

    conn = BaseHook.get_connection("postgres_conn")
    connection = psycopg2.connect(
        host=conn.host,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        port=conn.port
    )
    cursor = connection.cursor()

    # Ensure staging schema exists
    cursor.execute("CREATE SCHEMA IF NOT EXISTS staging")

    # Create staging table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS staging.api_users (
        raw_json JSONB
    )
    """)

    # Clear old data (so each run is fresh)
    cursor.execute("TRUNCATE TABLE staging.api_users")

    # Insert each record as raw JSON
    for user in data:
        cursor.execute(
            "INSERT INTO staging.api_users (raw_json) VALUES (%s)", # this is a single column table
            [json.dumps(user)] # convert dict to JSON
        )

    connection.commit()
    cursor.close()
    connection.close()


def transform_and_insert():
    conn = BaseHook.get_connection("postgres_conn")
    connection = psycopg2.connect(
        host=conn.host,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        port=conn.port
    )
    cursor = connection.cursor()

    # Ensure public schema exists (usually default, but just in case)
    cursor.execute("CREATE SCHEMA IF NOT EXISTS public")

    # Create final table in public schema
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS public.api_users (
        id INT PRIMARY KEY,
        name VARCHAR(100),
        username VARCHAR(50),
        email VARCHAR(100),
        city VARCHAR(50),
        zipcode VARCHAR(20)
    )
    """)

    # Transform from staging → public
    cursor.execute("""
    INSERT INTO public.api_users (id, name, username, email, city, zipcode)
    SELECT 
        (raw_json->>'id')::INT, # cast to INT
        raw_json->>'name',
        raw_json->>'username',
        raw_json->>'email',
        raw_json->'address'->>'city',
        raw_json->'address'->>'zipcode'
    FROM staging.api_users
    ON CONFLICT (id) DO NOTHING
    """)

    connection.commit()
    cursor.close()
    connection.close()


# DAG definition
with DAG(
    dag_id="with_staging_api_to_postgres",
    start_date=datetime(2025, 9, 1),
    schedule_interval="@once",
    catchup=False,
    tags=["api", "postgres", "staging"]
) as dag:

    task_fetch = PythonOperator(
        task_id="fetch_api_to_staging",
        python_callable=fetch_api_to_staging
    )

    task_insert = PythonOperator(
        task_id="transform_insert_public",
        python_callable=transform_and_insert
    )

    task_fetch >> task_insert
