# etl with out branching and grouping 
from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag
def etl():

    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id="postgres",
        sql="""
        TRUNCATE TABLE users;
        CREATE TABLE IF NOT EXISTS users (
            id VARCHAR(50) PRIMARY KEY,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available():
        import requests
        response = requests.get(
            "https://raw.githubusercontent.com/keroloshany47/dataset/refs/heads/main/fakeuser.json"
        )
        if response.status_code == 200:
            return PokeReturnValue(is_done=True, xcom_value=response.json())
        return PokeReturnValue(is_done=False)

    @task
    def extract_user_func(fake_user):
        users = []
        for user in fake_user:
            users.append({
                "id": user['id'],
                "firstname": user['personalInfo']['firstName'],
                "lastname": user['personalInfo']['lastName'],
                "email": user['personalInfo']['email']
            })
        return users

    @task
    def processing_user(user_info):
        import csv
        path = "/tmp/users.csv"
        with open(path, 'w', newline='') as f:
            fieldnames = ['id', 'firstname', 'lastname', 'email']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(user_info)
        return path   

    @task
    def store_user(file_path):
        hook = PostgresHook(postgres_conn_id="postgres")
        hook.copy_expert(
            sql="COPY users (id, firstname, lastname, email) FROM STDIN WITH CSV HEADER",
            filename=file_path
        )

   

    api_data = is_api_available()
    users = extract_user_func(api_data)
    file_path = processing_user(users)
    store_user(file_path)

    create_table >> api_data

etl()

#-----------------------------------------------------------------------------------------------------------------

#Etl with braching and grouping


from airflow.sdk import dag, task, task_group
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime


@dag(start_date=datetime(2024, 1, 1), schedule=None, catchup=False)
def small_etl():

    # ---------------- SQL ----------------
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
        TRUNCATE TABLE users;
        CREATE TABLE IF NOT EXISTS users (
            id VARCHAR(50) PRIMARY KEY,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # ---------------- SENSOR ----------------
    @task.sensor(poke_interval=20, timeout=300)
    def is_api_available():
        import requests

        r = requests.get(
            "https://raw.githubusercontent.com/keroloshany47/dataset/refs/heads/main/fakeuser.json"
        )

        if r.status_code == 200:
            return PokeReturnValue(True, r.json())

        return PokeReturnValue(False)

    # ---------------- EXTRACT ----------------
    @task
    def extract(data):
        return [
            {
                "id": u["id"],
                "firstname": u["personalInfo"]["firstName"],
                "lastname": u["personalInfo"]["lastName"],
                "email": u["personalInfo"]["email"],
            }
            for u in data
        ]

    # ---------------- BRANCH  ----------------
    @task.branch
    def branch_func(users):
        if users and len(users) > 0:
            return "process_group.transform"
        return "no_data"

    no_data = EmptyOperator(task_id="no_data")

    # ---------------- GROUPING ----------------
    @task_group(group_id="process_group")
    def process_group(users):

        @task
        def transform(users):
            return [
                {**u, "email": u["email"].lower()}
                for u in users
            ]

        @task
        def save_csv(users):
            import csv

            path = "/tmp/users.csv"

            with open(path, "w", newline="") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=["id", "firstname", "lastname", "email"]
                )
                writer.writeheader()
                writer.writerows(users)

            return path

        @task
        def load_db(file_path):
            hook = PostgresHook(postgres_conn_id="postgres")

            hook.copy_expert(
                sql="""
                COPY users(id, firstname, lastname, email)
                FROM STDIN WITH CSV HEADER
                """,
                filename=file_path
            )

        t = transform(users)
        file = save_csv(t)
        load_db(file)

    # ---------------- FLOW ----------------
    api = is_api_available()
    users = extract(api)
    branch = branch_func(users)
    create_table >> api >> users >> branch
    branch >> process_group(users)
    branch >> no_data

small_etl()
