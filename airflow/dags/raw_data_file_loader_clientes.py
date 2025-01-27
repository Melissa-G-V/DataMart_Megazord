from datetime import datetime, timedelta
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine

DATABASE_URL = "cockroachdb+psycopg2://megazorders:JBQROkforHRxPkyN2-3LeQ@mega-zordian-7326.j77.aws-us-east-1.cockroachlabs.cloud:26257/defaultdb"

def criar_tabela():

    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        sql = """
            CREATE TABLE raw_clientes (
                id INT PRIMARY KEY,
                nome VARCHAR(255),
                sobrenome VARCHAR(255),
                email VARCHAR(255),
                telefone VARCHAR(15),
            ) 
        """
        try:
            conn.execute(sql)
            print("Tabela criada.")
        except Exception as e:
            print(f"Erro: {e}")

def processamento_etl():

    print("Current Directory:", os.getcwd())
    concat_df = pd.DataFrame()

    for file_name in os.listdir('rawdata/clientes'):
        if file_name.endswith('.csv'):
            file_path = os.path.join('rawdata/clientes', file_name)
            print(f"Processing file: {file_name}")
            
            df = pd.read_csv(file_path)
            if 'data_transacao' in df.columns:
                df['data_transacao'] = pd.to_datetime(df['data_transacao'], format='ISO8601')
            concat_df = pd.concat([concat_df, df], ignore_index=True)

    if 'id_transacao' in concat_df.columns:
        concat_df = concat_df.drop_duplicates(subset='id_transacao')

    print("Final DataFrame:")
    print(concat_df.head())

    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        concat_df.to_sql('raw_clientes', conn, if_exists='append', index=False)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="raw_data_cliente_from_folder",
    default_args=default_args,
    description="RAW PARTITION ",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    criar_tabela_task = PythonOperator(
        task_id="criar_tabela",
        python_callable=criar_tabela,
    )

    salvar_dados_task = PythonOperator(
        task_id="processamento_etl",
        python_callable=processamento_etl,
    )

    criar_tabela_task >> salvar_dados_task
    #criar_tabela_task