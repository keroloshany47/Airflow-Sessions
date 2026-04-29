from airflow.sdk import dag, task
from datetime import datetime
import pandas as pd
import random

@dag(
    dag_id="etl_dag",
    schedule="0 0 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def etl_dag_dec():

    @task()
    def extract_dec():
        data = {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [25, 30, 35, 40, 45],
            "city": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
        }
        df = pd.DataFrame(data)
        df.to_csv("/tmp/extracted_data.csv", index=False)

    @task()
    def transform_dec():
        df = pd.read_csv("/tmp/extracted_data.csv")
        df["age"] = df["age"] + random.randint(1, 10)
        df.to_csv("/tmp/transformed_data.csv", index=False)

    @task.bash
    def load_dec() -> str:
        return "echo 'Loading data...' && cat /tmp/transformed_data.csv"

    extract_dec() >> transform_dec() >> load_dec()

etl_dag_dec()
