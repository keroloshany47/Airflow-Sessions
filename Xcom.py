# the old way 
from airflow.sdk import dag, task, Context

@dag
def xcom_dag():
    
    @task
    def t1(context: Context):
        val = 42
        context['ti'].xcom_push(key='my_key', value=val)

    @task
    def t2(context: Context):
        val = context['ti'].xcom_pull(task_ids='t1', key='my_key')
        print(val)

    t1() >> t2()

xcom_dag()
#------------------------------------------------------------------------------------------------
## new way single value 

from airflow.sdk import dag, task, Context
from typing import Dict, Any

@dag
def xcom_dag():
    @task                        
    def t1 ()-> int :            
        val =42 
        return val                

    @task 
    def t2 (val: int):            
        print(val)               
        

    val = t1()                    
    t2(val)
xcom_dag()

#------------------------------------------------------------------------------------------------
## multi values

from airflow.sdk import dag, task 
from typing import Dict, Any

@dag 
def xcom_dag():
    @task 
    def t1()-> Dict[str,Any]:
        my_val = 42
        my_sentence = "Hello world " 
        return {"my_val": my_val, "my_sentence": my_sentence}
        
    @task 
    def t2(val: Dict[str, Any]):
        print(val["my_val"])
        print(val["my_sentence"])
    
    
    val = t1()
    t2(val)
    
xcom_dag()
       