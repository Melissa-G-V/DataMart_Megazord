from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, timedelta

DATABASE_REF_URL = "cockroachdb+psycopg2://megazorders:JBQROkforHRxPkyN2-3LeQ@mega-zordian-7326.j77.aws-us-east-1.cockroachlabs.cloud:26257/Refined_stage"
DATABASE_MASTER_URL = "cockroachdb+psycopg2://megazorders:JBQROkforHRxPkyN2-3LeQ@mega-zordian-7326.j77.aws-us-east-1.cockroachlabs.cloud:26257/Master_stage"


def criar_tabelas_agregadas(**kwargs):
    """Creates aggregated tables and saves them in the master database."""
    refined_engine = create_engine(DATABASE_REF_URL)
    master_engine = create_engine(DATABASE_MASTER_URL)

    # Queries for aggregated data
    queries = {
        "receita_total_por_cliente": """
            SELECT 
                id_cliente, 
                nome_cliente, 
                sobrenome, 
                SUM(preco * quantidade) AS receita_total
            FROM ref_transacoes
            GROUP BY id_cliente, nome_cliente, sobrenome;
        """,

        "numero_transacoes_por_cliente": """
            SELECT 
                id_cliente, 
                nome_cliente, 
                sobrenome, 
                COUNT(id_transacao) AS numero_transacoes
            FROM ref_transacoes
            GROUP BY id_cliente, nome_cliente, sobrenome;
        """,

        "produto_mais_comprado_por_cliente": """
            SELECT 
                id_cliente, 
                nome_cliente, 
                sobrenome, 
                id_produto, 
                nome_produto, 
                MAX(quantidade) AS quantidade_maxima
            FROM (
                SELECT 
                    id_cliente, 
                    nome_cliente, 
                    sobrenome, 
                    id_produto, 
                    nome_produto, 
                    SUM(quantidade) AS quantidade
                FROM ref_transacoes
                GROUP BY id_cliente, nome_cliente, sobrenome, id_produto, nome_produto
            ) AS subquery
            GROUP BY id_cliente, nome_cliente, sobrenome, id_produto, nome_produto;
        """
    }

    try:
        for table_name, query in queries.items():
            with refined_engine.connect() as refined_conn:
                df = pd.read_sql(query, refined_conn)

            with master_engine.connect() as master_conn:
                df.to_sql(table_name, master_conn, if_exists="replace", index=False)

            print(f"Table {table_name} created and populated successfully in the master database.")
    except Exception as e:
        print(f"Error creating aggregated tables: {e}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'master_transformation',
    default_args=default_args,
    description='Transform and aggregate data into master database',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:


    criar_tabelas_agregadas_task = PythonOperator(
        task_id='criar_tabelas_agregadas',
        python_callable=criar_tabelas_agregadas,
        provide_context=True,
    )
    criar_tabelas_agregadas_task
