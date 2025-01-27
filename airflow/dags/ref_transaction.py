from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine


DATABASE_RAW_URL = "cockroachdb+psycopg2://megazorders:JBQROkforHRxPkyN2-3LeQ@mega-zordian-7326.j77.aws-us-east-1.cockroachlabs.cloud:26257/defaultdb"
DATABASE_REF_URL = "cockroachdb+psycopg2://megazorders:JBQROkforHRxPkyN2-3LeQ@mega-zordian-7326.j77.aws-us-east-1.cockroachlabs.cloud:26257/Refined_stage"

def criar_tabela(**kwargs):
    """ Gerando uma tabela para salvar os dados"""
    DATABASE_REF_URL = "cockroachdb+psycopg2://megazorders:JBQROkforHRxPkyN2-3LeQ@mega-zordian-7326.j77.aws-us-east-1.cockroachlabs.cloud:26257/Refined_stage"
    engine = create_engine(DATABASE_REF_URL)
    
    create_table_query = """
        CREATE TABLE IF NOT EXISTS ref_transacoes (
            id_transacao INT PRIMARY KEY,
            id_cliente INT,
            id_produto INT,
            nome_cliente varchar(255),
            nome_produto varchar(255),
            sobrenome varchar(255),
            descricao varchar(255),
            ean int,
            preco decimal(10,2),
            quantidade INT,
            data_transacao TIMESTAMP
        );
    """
    try:
        with engine.connect() as connection:
            connection.execute(create_table_query)
            print("SUCCESS.")
    except Exception as e:
        print(f"Error: {e}")

def transferir_e_popular():
    raw_engine = create_engine(DATABASE_RAW_URL)
    with raw_engine.connect() as raw_conn:
        staging_transacoes = pd.read_sql("SELECT * FROM staging_transacoes", raw_conn) 
    master_engine = create_engine(DATABASE_REF_URL)
    with master_engine.connect() as master_conn:
        staging_transacoes.to_sql("temp_staging_transacoes", master_conn, if_exists="replace", index=False)
        join_query = """
INSERT INTO ref_transacoes (id_transacao, id_cliente,nome_cliente,sobrenome,nome_produto ,descricao, preco, ean, id_produto, quantidade, data_transacao)
        SELECT
            t.id_transacao,
            c.id AS id_cliente,
            c.nome as nome_cliente,
            c.sobrenome as sobrenome,
            p.nome as nome_produto,
            p.descricao,
            p.preco,
            p.ean,
            p.id AS id_produto,
            t.quantidade,
            t.data_transacao
        FROM
            temp_staging_transacoes t
        LEFT JOIN ref_clientes c ON t.id_cliente = c.id
        LEFT JOIN ref_produtos p ON t.id_produto = p.id
        WHERE
            c.id IS NOT NULL
            AND p.id IS NOT NULL;

        """
        master_conn.execute(join_query)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ref_transacoes_pipeline',
    default_args=default_args,
    description='transfere e popula a tabela ref',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    transferir_popular = PythonOperator(
        task_id='transferir_popular',
        python_callable=transferir_e_popular,
    )

    criar_tabela_task = PythonOperator(
        task_id='criar_tabela',
        python_callable=criar_tabela,
        provide_context=True,
    )
    
    criar_tabela_task >> transferir_popular