from airflow.sdk import dag, task, task_group

@dag
def group():
    @task
    def task1() -> int:
        return 42

    @task_group
    def sub_group(val: int):
        
        @task
        def task2(val: int) -> int:
            return val + 12

        @task
        def task3(val: int) -> int:
            return val * 2
        
        task3(task2(val))

    val = task1()
    sub_group(val)

group()