from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

default_args = {
    'owner': 'airflow'
}


def create_table():

    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    cur = conn.cursor()

    create_sql = """
    CREATE TABLE IF NOT EXISTS EMPLOYEE (
        ID NUMBER,
        NAME VARCHAR(50),
        SALARY NUMBER(10,2),
        HRA NUMBER(10,2),
        DEPARTMENTNAME VARCHAR(50)
    )
    """

    cur.execute(create_sql)

    conn.commit()
    cur.close()
    conn.close()


def insert_data():

    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    cur = conn.cursor()

    employee_data = [
        (101, 'RAM', 50000, 10000, 'IT'),
        (102, 'RAJA', 60000, 12000, 'HR'),
        (103, 'ARUN', 70000, 14000, 'FINANCE'),
        (104, 'KUMAR', 80000, 16000, 'SALES')
    ]

    insert_sql = """
    INSERT INTO EMPLOYEE
    (ID, NAME, SALARY, HRA, DEPARTMENTNAME)
    VALUES (%s, %s, %s, %s, %s)
    """

    cur.executemany(insert_sql, employee_data)

    conn.commit()
    cur.close()
    conn.close()


with DAG(
        dag_id='snowflake_employee_etl',
        start_date=datetime(2026, 6, 22),
        schedule=None,
        catchup=False,
        default_args=default_args
) as dag:

    create_table_task = PythonOperator(
        task_id='create_employee_table',
        python_callable=create_table
    )

    insert_data_task = PythonOperator(
        task_id='insert_employee_data',
        python_callable=insert_data
    )

    create_table_task >> insert_data_task




# =======================



from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


@dag(
    dag_id="snowflake_employee_etl",
    start_date=datetime(2026, 6, 22),
    schedule="@daily",
    catchup=False,
    tags=["snowflake", "etl"]
)
def snowflake_employee_etl():

    @task
    def create_table():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")

        create_sql = """
        CREATE TABLE IF NOT EXISTS EMPLOYEE (
            ID NUMBER,
            NAME VARCHAR(50),
            SALARY NUMBER(10,2),
            HRA NUMBER(10,2),
            DEPARTMENTNAME VARCHAR(50)
        )
        """

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(create_sql)

    @task
    def insert_data():

        employee_data = [
            (101, "RAM", 50000, 10000, "IT"),
            (102, "RAJA", 60000, 12000, "HR"),
            (103, "ARUN", 70000, 14000, "FINANCE"),
            (104, "KUMAR", 80000, 16000, "SALES")
        ]

        insert_sql = """
        INSERT INTO EMPLOYEE
        (ID, NAME, SALARY, HRA, DEPARTMENTNAME)
        VALUES (%s, %s, %s, %s, %s)
        """

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(insert_sql, employee_data)

    create_table() >> insert_data()


dag = snowflake_employee_etl()



# ========================



from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="snowflake_employee_etl",
    start_date=datetime(2026, 6, 22),
    schedule="@daily",
    catchup=False,
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="snowflake_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS EMPLOYEE(
            ID NUMBER,
            NAME VARCHAR(50),
            SALARY NUMBER(10,2),
            HRA NUMBER(10,2),
            DEPARTMENTNAME VARCHAR(50)
        );
        """
    )

    insert_data = SQLExecuteQueryOperator(
        task_id="insert_data",
        conn_id="snowflake_conn",
        sql="""
        INSERT INTO EMPLOYEE
        VALUES
        (101,'RAM',50000,10000,'IT'),
        (102,'RAJA',60000,12000,'HR'),
        (103,'ARUN',70000,14000,'FINANCE'),
        (104,'KUMAR',80000,16000,'SALES');
        """
    )

    create_table >> insert_data