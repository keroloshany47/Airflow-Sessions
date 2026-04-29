#Ex:1
from airflow.sdk import asset, dag, task
from typing import Any
import requests

USER_URI = "https://randomuser.me/api/"


@asset(schedule="@daily", uri=USER_URI)
def user() -> dict[str, Any]:
    return requests.get(USER_URI).json()


@dag(schedule=[user])
def process_user_dag():

    @task
    def process(**context):
        data = context["ti"].xcom_pull(
            dag_id="user",
            task_ids="user",
            include_prior_dates=True
        )

        if isinstance(data, list):
            data = data[0]

        location = data["results"][0]["location"]
        print("User location:", location)

    process()
process_user_dag()
#---------------------------------------------------------------------------------
#Ex:2
from airflow.sdk import asset, dag, task
from typing import Any
import requests

USER_URI = "https://randomuser.me/api/"


@asset(schedule="@daily", uri=USER_URI)
def user() -> dict[str, Any]:
    return requests.get(USER_URI).json()


@dag(schedule=[user])
def process_user_dag():

    @task
    def process(**context):
        data = context["ti"].xcom_pull(
            dag_id="user",
            task_ids="user",
            include_prior_dates=True
        )

        if isinstance(data, list):
            data = data[0]

        location = data["results"][0]["location"]
        print("User location:", location)

    process()


process_user_dag()

#------------------------------------------------------------------------------------------------
#Ex:3
from airflow.sdk import asset, Asset, Context
import requests

@asset(
    schedule="@daily",
    uri="https://randomuser.me/api/"
)
def my_first_asset(self) -> dict:
    r = requests.get(self.uri)
    return r.json()

@asset(schedule=my_first_asset)
def my_second_asset(my_first_asset: Asset, context: Context) -> dict:
    user_info = context["ti"].xcom_pull(
        dag_id=my_first_asset.name,
        task_ids=my_first_asset.name,
        key="return_value",
        include_prior_dates=True
    )

    if isinstance(user_info, list):
        user_info = user_info[0]

    return user_info["results"][0]["location"]

@asset(schedule=my_first_asset)  
def my_third_asset(my_first_asset: Asset, context: Context) -> dict:
    user_info = context["ti"].xcom_pull(
        dag_id=my_first_asset.name,
        task_ids=my_first_asset.name,
        key="return_value",
        include_prior_dates=True
    )

    if isinstance(user_info, list):
        user_info = user_info[0]

    return user_info["results"][0]["login"]
#------------------------------------------------------------------------------------------
#Ex:4
#multi assets 
# multi_asset.py
from airflow.sdk import asset , Asset, Context
import requests


#from asset import my_first_asset

user_location = Asset("user_location")
user_login    = Asset("user_login")

@asset.multi(
    schedule=my_first_asset,          
    outlets=[user_location, user_login]
)
def user_info(my_first_asset: Asset, context: Context):
    user_data = context["ti"].xcom_pull(
        dag_id=my_first_asset.name,
        task_ids=my_first_asset.name,
        key="return_value",
        include_prior_dates=True
    )

    if isinstance(user_data, list):
        user_data = user_data[0]

    return {
        "user_location": user_data["results"][0]["location"],
        "user_login":    user_data["results"][0]["login"]
    }
    