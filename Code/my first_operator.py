from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime



import pandas as pd
import random 



with DAG(
    dag_id="my_first_dag",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    def extract():
        data = {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [25, 30, 35, 40, 45],
            "city": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
        }
        df = pd.DataFrame(data)
        df.to_csv("/tmp/extracted_data.csv", index=False)
        
    t1 = PythonOperator(
        task_id="extract_task",
        python_callable=extract
    )
    
    def transform():
        df = pd.read_csv("/tmp/extracted_data.csv")
        df["age"] = df["age"] + random.randint(1, 10)  # Simulate some transformation
        df.to_csv("/tmp/transformed_data.csv", index=False)

    t2 = PythonOperator(
        task_id="transform_task",
        python_callable=transform
    )

    
    t3 = BashOperator(
        task_id="load_task",
        bash_command="echo 'Loading data...' && cat /tmp/transformed_data.csv"
    )

    t1>>t2>>t3
    
    
