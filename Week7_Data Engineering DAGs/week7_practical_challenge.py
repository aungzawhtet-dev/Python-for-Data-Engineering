from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import pandas as pd

DATA_FILE = "/opt/airflow/dags/data/new_customers.csv"

def load_to_postgres():
    try:
        # Connect to Postgres
        conn = psycopg2.connect(
            dbname="db_test",
            user="postgres",
            password="Aung24188",
            host="host.docker.internal",   # service name from docker-compose
            port="5432"
        )
        cur = conn.cursor()
        
        # Read CSV
        df = pd.read_csv(DATA_FILE)
        
        # Create table if not exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS new_customers (
                id INT PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                email TEXT
            )
        """)
        
        # Insert rows
        for _, row in df.iterrows():
            cur.execute(
                """
                INSERT INTO new_customers (id, first_name, last_name, email)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
                """,
                (row['customer_id'], row['first_name'], row['last_name'], row['email'])
            )
        
        # Commit and close
        conn.commit()
        cur.close()
        conn.close()
        print("Data inserted successfully.")
        
    except Exception as e:
        print(f"Database connection failed: {e}")
        raise  # let Airflow retry

# DAG definition
with DAG(
    dag_id='dag_retry_db_connection',
    start_date=datetime(2025, 9, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args={'retries': 3, 'retry_delay': timedelta(seconds=10)}
) as dag:
    
    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )
