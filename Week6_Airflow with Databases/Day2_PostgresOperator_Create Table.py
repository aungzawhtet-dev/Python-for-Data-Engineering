from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id="create_customers_table",
    start_date=datetime(2022, 1, 1),
    schedule_interval="@once",   # run only once
    catchup=False,
    tags=["postgres", "example"],  # optional, helps group DAGs in UI
) as dag:

    # Task: Create table
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_conn",  # must exist in Airflow connections
        database="db_test",  #  optional: specify DB if not set in connection
        sql="""
            CREATE TABLE IF NOT EXISTS db_customers (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                email VARCHAR(100),
                signup_date DATE
            );
        """,
    )
