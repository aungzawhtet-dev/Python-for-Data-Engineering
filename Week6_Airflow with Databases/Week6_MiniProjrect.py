# Extract → Read a CSV file.
# Transform → Use Pandas to clean/transform.
# Load → Insert into Postgres with Airflow.

import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


# In Airflow, (**kwargs and **context) are basically the same — they both capture the context dictionary\
#that Airflow automatically passes into your callable.

# Extract the data from a CSV file
def extract_csv(**context): # **context is suitable here to access the task instance (ti)
    file_path = '/opt/airflow/dags/data/db_customers.csv'  # Update with the CSV file path
    df = pd.read_csv(file_path)
    context['ti'].xcom_push(key='raw_data', value=df.to_json())
    print("Data extracted from CSV file and pushed to XCom.")
    
# Transform the data using Pandas
def transform_data(ti,**kwargs): # ti is suitable here to access the task instance directly used with **kwargs
    raw_data = ti.xcom_pull(task_ids='extract_csv', key='raw_data')
    df = pd.read_json(raw_data)
    
    # Example transformation: uppercase last name + filter signup_date after 2024-03
    df['last_name'] = df['last_name'].str.upper()
    df = df[df['signup_date'] > '2024-03-01']
    
    ti.xcom_push(key='transformed_data', value=df.to_json())
    print("Data transformed and pushed to XCom.")
    
    
# Load the transformed data into Postgres
def load_to_postgres(**context): # **context is suitable here to access the task instance (ti)
    ti = context['ti']
    transform_data = ti.xcom_pull(key='transformed_data', task_ids='transform_data')
    df = pd.read_json(transform_data)
    
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    
    # Create table if not exists
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS public.customers_cleaned  ( 
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    signup_date DATE
    );
    """)
    
    # Insert data into the table
    cursor = conn.cursor()
    for _, row in df.iterrows():
        cursor.execute("""
        INSERT INTO public.customers_cleaned (customer_id, first_name, last_name, email, signup_date)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (customer_id) DO NOTHING;
        """, (row['id'], row['first_name'], row['last_name'], row['email'], row['signup_date']))
        
    conn.commit()
    cursor.close()
    conn.close()
    print("Transformed data loaded successfully into Postgres.")
    
    
# Define the DAG
with DAG(
    dag_id="csv_to_postgres_pipeline",
    start_date=datetime(2025, 9, 25),
    schedule_interval=None,   # Trigger manually
    catchup=False,
    tags=["mini_project", "etl"]
) as dag:

    extract_task = PythonOperator(
        task_id='extract_csv',
        python_callable=extract_csv
       
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
        
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
        
    )

    extract_task >> transform_task >> load_task
        
    
    
  