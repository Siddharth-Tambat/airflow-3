from airflow.sdk import dag, task, Context
from typing import Dict, Any

@dag
def xcom_dag():

    @task
    def task1() -> Dict[str, Any]:
        pos = 'swimming'
        description = 'perform swimming'
        return {
            'position': pos,
            'description': description,
        }
    
    @task
    def task2(data: Dict[str, Any]) -> None:
        print(data)

    data = task1()
    task2(data)

xcom_dag()