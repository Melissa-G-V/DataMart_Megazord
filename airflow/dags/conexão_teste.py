from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def query_database():
    postgres_hook = PostgresHook(postgres_conn_id='raw_conection')
    sql_query = "SELECT * FROM clientes LIMIT 5;"
    result = postgres_hook.get_records(sql_query)
    print("Query Results:", result)

with DAG(
    dag_id='test_cnn_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    query_task = PythonOperator(
        task_id='query_task',
        python_callable=query_database,
    )

    start_task = DummyOperator(task_id='start_task')

    start_task >> query_task  
