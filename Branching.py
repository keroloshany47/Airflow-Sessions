from airflow.sdk import dag, task

@dag
def branch():
    
    @task
    def task1():
        return 6
    
    @task.branch
    def task2(val: int):
        if val == 1:
            return "equal_1"
        return "different_than_1"
    
    @task
    def equal_1(val: int):
        print(f"Equal to {val}")
    
    @task
    def different_than_1(val: int):
        print(f"Different than 1: {val}")
    
    val = task1()
    task2(val) >> [equal_1(val), different_than_1(val)]
    
branch()