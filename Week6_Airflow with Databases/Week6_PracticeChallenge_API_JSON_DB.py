import requests
import json
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime

DATA_PATH = "/opt/airflow/dags/data/api_users.json"



# 1️ Fetch data from API
def fetch_api(**kwargs):
    url = "https://jsonplaceholder.typicode.com/users"
    response = requests.get(url)
    data = response.json()

    # Save JSON locally
    with open(DATA_PATH, "w") as f:
        json.dump(data, f, indent=2)        
   
    
    # Push to XCom for next task
    kwargs['ti'].xcom_push(key="api_data", value=data)
    
    
    
# Insert data into Postgres
def insert_into_db(**kwargs):
    # Pull data from XCom
    data = kwargs['ti'].xcom_pull(key="api_data", task_ids="fetch_api")
    
    # Get Postgres connection
    conn = BaseHook.get_connection("postgres_conn")
    connection = psycopg2.connect(
        host=conn.host,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        port=conn.port
    )
    cursor = connection.cursor()
    
    # Create table if not exists
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS api_users (
        id INT PRIMARY KEY,
        name VARCHAR(100),
        username VARCHAR(50),
        email VARCHAR(100),
        city VARCHAR(50),
        zipcode VARCHAR(20)
    )
    """)
    
    # Insert data
    for user in data:
        cursor.execute("""
        INSERT INTO api_users (id, name, username, email, city, zipcode)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
        """, (
            user['id'], 
            user['name'], 
            user['username'], 
            user['email'], 
            user['address']['city'], 
            user['address']['zipcode']
        ))
    
    connection.commit()
    cursor.close()
    connection.close()


# DAG definition
with DAG(
    dag_id="api_to_postgres",
    start_date=datetime(2025, 9, 1),
    schedule_interval="@once",
    catchup=False,
) as dag:

    task_fetch = PythonOperator(
        task_id="fetch_api",
        python_callable=fetch_api
   
    )

    task_insert = PythonOperator(
        task_id="insert_into_db",
        python_callable=insert_into_db
      
    )

    task_fetch >> task_insert
