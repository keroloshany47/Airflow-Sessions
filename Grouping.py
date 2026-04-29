#Ex1
from airflow.sdk import dag ,task ,task_group

@dag
def group():


    @task
    def a():
        return 50


    @task_group(default_args={"retries": 3})
    def group1(val: int ):

        @task
        def b(my_val: int):
            print(my_val+23)

        @task_group(default_args={"retries": 5})
        def my_nested_group():
            @task
            def c():
                print("This is c")
            c()   

        b(val) >> my_nested_group()


    val = a() 
    group1(val)

group()    

#-------------------------------------------------------------------------------
#Ex2
from airflow.sdk import dag, task, task_group


@dag
def grouping_dag():

    @task
    def task1():
        return 50

    
    @task_group(default_args={"retries": 3})
    def task_group1(val):

        @task
        def task2(val: int):
            print(val + 24)

        @task
        def task3(val: int):
            print(val * 2)

        
        t2 = task2(val)
        t3 = task3(val)

    my_val = task1()
    task_group1(my_val)


grouping_dag()
#----------------------------------------------------------------------------------
#Ex_3
from airflow.sdk import dag, task, task_group


@dag
def grouping_dag():

    @task
    def task1():
        return 50

    
    @task_group(default_args={"retries": 3})
    def task_group1(val):

        @task
        def task2(val: int):
            print(val + 24)

        
        @task_group(default_args={"retries": 2})
        def inner_group(val):

            @task
            def task3(val: int):
                print(val * 2)

            @task
            def task4(val: int):
                print(val - 10)

            t3 = task3(val)
            t4 = task4(val) 

        t2 = task2(val)
        inner = inner_group(val)

        t2 >> inner   

    my_val = task1()
    task_group1(my_val)


grouping_dag()

