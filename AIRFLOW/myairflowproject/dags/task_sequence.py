from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def task1():
    print("=" * 50)
    print("Task 1 Started")
    print("Hello from Task 1")
    print("Task 1 Completed")
    print("=" * 50)


def task2():
    print("=" * 50)
    print("Task 2 Started")
    print("Processing data in Task 2")
    print("Task 2 Completed")
    print("=" * 50)


def task3():
    print("=" * 50)
    print("Task 3 Started")
    print("Task 3 Finished Successfully")
    print("=" * 50)


with DAG(
    dag_id="sample_python_operator_dag",
    description="Simple Airflow DAG Example",
    start_date=datetime(2025, 1, 1),
    schedule="* * * * *",   # Every minutes
    catchup=False,
    tags=["sample", "python"],
) as dag:

    task_1 = PythonOperator(
        task_id="task_1",
        python_callable=task1,
    )

    task_2 = PythonOperator(
        task_id="task_2",
        python_callable=task2,
    )

    task_3 = PythonOperator(
        task_id="task_3",
        python_callable=task3,
    )

    # Task dependency
    task_1 >> task_2 >> task_3