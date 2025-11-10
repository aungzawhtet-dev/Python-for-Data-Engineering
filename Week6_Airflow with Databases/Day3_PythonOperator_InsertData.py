import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.hooks.base import BaseHook


def insert_data():
    # Read dataset
    df = pd.read_csv("/opt/airflow/dags/data/db_customers.csv")
    
    #Get postgres connection
    conn = BaseHook.get_connection("postgres_conn")
    connection = psycopg2.connect(
        host = conn.host,
        dbname = conn.schema,
        user =conn.login,
        password = conn.password,
        port = conn.port,
    )
    
    
    # Why for _, row?
    """
    Since iterrows() gives (index, row),
    the first value is the index (like 0, 1, 2…)
    the second value is the row itself (a Series with your columns).
    """
    
    cursor =connection.cursor()
    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO db_customers (id,first_name, last_name, email, signup_date)
            VALUES (%s, %s, %s, %s,%s)
            """,
            (row['id'], row['first_name'], row['last_name'], row['email'], row['signup_date'])
        )
    connection.commit()
    cursor.close()
    connection.close()
    

# DAG must be defined at global/module level
with DAG(
    dag_id="insert_customers_data",
    start_date=datetime(2022, 1, 1),
    schedule_interval="@once",
    catchup=False,
    tags=["postgres", "insert"],
) as dag:

    insert_task = PythonOperator(
        task_id="insert_data",
        python_callable=insert_data,
    )