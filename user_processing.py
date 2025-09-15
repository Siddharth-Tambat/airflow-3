from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import BranchPythonOperator
from datetime import datetime
import pendulum
import os

@dag(
        start_date=datetime(2025, 7, 1, 0, 0, tzinfo=pendulum.timezone("Asia/Kolkata")),
        schedule="@daily",
        catchup=False,
        tags=["termi", "test", "api-retry", "data-quality"],
        default_args={
            "owner": "terminator",
            "retries": 3,
            "retry_delay": 300  # 5 minutes
        }
)
def user_processing():
    
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS data_quality_check_users (
            id INT,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP,
            dq_failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            dq_reason TEXT
        );
        """
    )
    
    @task.sensor(poke_interval=30, timeout=300, retries=3, retry_delay=60)
    def is_api_available() -> PokeReturnValue:
        import requests, time
        for attempt in range(3):
            try:
                response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json", timeout=10)
                if response.status_code == 200:
                    fake_user = response.json()
                    return PokeReturnValue(is_done=True, xcom_value=fake_user)
            except requests.RequestException as e:
                print(f"API call failed: {e}, retrying in 5s...")
                time.sleep(5)
        return PokeReturnValue(is_done=False, xcom_value=None)
    
    @task
    def extract_user(fake_user):
        return {
            "id": fake_user["id"],
            "firstname": fake_user["personalInfo"]["firstName"],
            "lastname": fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"],
        }
        
    @task
    def process_user(user_info):
        import csv
        from datetime import datetime
        user_info["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        file_path = "/tmp/user_info.csv"
        with open(file_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)
        return file_path
    
    @task
    def data_quality_check(file_path: str):
        import great_expectations as ge
        import pandas as pd

        df = pd.read_csv(file_path)
        ge_df = ge.from_pandas(df)

        results = []
        # Expect ID not null
        results.append(ge_df.expect_column_values_to_not_be_null("id"))
        # Expect valid email format (basic regex check)
        results.append(ge_df.expect_column_values_to_match_regex("email", r"[^@]+@[^@]+\.[^@]+"))

        if all(r.success for r in results):
            return "store_user"
        else:
            # Save reject reason
            df["dq_reason"] = "Failed ID not null or Email format validation"
            reject_file = "/tmp/rejected_user_info.csv"
            df.to_csv(reject_file, index=False)
            return "store_failed_user"
    
    branch = BranchPythonOperator(
        task_id="branching",
        python_callable=lambda ti: ti.xcom_pull(task_ids="data_quality_check")
    )
    
    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id="postgres")
        hook.copy_expert(
            sql="COPY users FROM STDIN WITH CSV HEADER",
            filename="/tmp/user_info.csv"
        )
    
    @task
    def store_failed_user():
        hook = PostgresHook(postgres_conn_id="postgres")
        hook.copy_expert(
            sql="COPY data_quality_check_users FROM STDIN WITH CSV HEADER",
            filename="/tmp/rejected_user_info.csv"
        )
    
    # DAG Flow
    processed_file = process_user(extract_user(create_table >> is_api_available()))
    dq_result = data_quality_check(processed_file)
    dq_result >> branch
    branch >> [store_user(), store_failed_user()]

user_processing()
