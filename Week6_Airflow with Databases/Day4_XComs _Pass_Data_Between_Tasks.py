from  airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def extract_data(**context):
    data = {"customer_id":101,"name":"Trust User"}
    context["task_instance"].xcom_push(key="customer_data", value="data")
    
    
def transform_data(**context):
    data = context["task_instance"].xcom_pull(key="customer_data")
    print("Received from XCom:", data)
    
    
with DAG(
    dag_id = "xcom_example",
    start_date = datetime(2024, 1, 1),
    schedule_interval = "@once",
    catchup = False,
) as dag:
    
    
    extract = PythonOperator(
        task_id = "extract",
        python_callable = extract_data,
       
        
    )
    
    transform = PythonOperator(
        task_id = "transform",
        python_callable = transform_data,
        
    )
    
    extract >> transform