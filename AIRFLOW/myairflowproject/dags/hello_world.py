from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="hello_world",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example"]
)
def hello_world_dag():

    @task
    def hello():
        print("Hello, World!")
        print("Welcome to Apache Airflow")

    hello()

hello_world_dag()