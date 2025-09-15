from airflow.sdk import dag, task

@dag
def branch_dag():
    
    @task
    def a():
        return 1

    @task.branch
    def b(val: int) -> str:
        if val < 1:
            return 'c'
        return 'd'
    
    @task
    def c():
        print("Val is less than 1")

    @task
    def d():
        print("Val is greater than or equal to 1")

    val = a()
    b(val) >> [c(), d()]

branch_dag()