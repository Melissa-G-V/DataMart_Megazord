from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, timedelta

DATABASE_REF_URL = "cockroachdb+psycopg2://megazorders:JBQROkforHRxPkyN2-3LeQ@mega-zordian-7326.j77.aws-us-east-1.cockroachlabs.cloud:26257/Refined_stage"
DATABASE_MASTER_URL = "cockroachdb+psycopg2://megazorders:JBQROkforHRxPkyN2-3LeQ@mega-zordian-7326.j77.aws-us-east-1.cockroachlabs.cloud:26257/Master_stage"

QUERIES = {
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
    
    "top_5_produtos_mais_vendidos": """
        SELECT 
            id_produto, 
            nome_produto, 
            SUM(quantidade) AS total_vendido
        FROM ref_transacoes
        WHERE data_transacao >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY id_produto, nome_produto
        ORDER BY total_vendido DESC
        LIMIT 5;
    """,

    "numero_clientes_ativos": """
        SELECT 
            COUNT(DISTINCT id_cliente) AS clientes_ativos
        FROM ref_transacoes
        WHERE data_transacao >= CURRENT_DATE - INTERVAL '3 months';
    """
}

def processar_consulta(tabela: str, query: str, **kwargs):
    refined_engine = create_engine(DATABASE_REF_URL)
    master_engine = create_engine(DATABASE_MASTER_URL)

    try:
        with refined_engine.connect() as refined_conn:
            df = pd.read_sql(query, refined_conn)

        with master_engine.connect() as master_conn:
            df.to_sql(tabela, master_conn, if_exists="replace", index=False)

        print(f"Tabela {tabela} processada")
    except Exception as e:
        print(f"Erro {tabela}: {e}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'master_tabelas_views_aggregados',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_tasks=4 # PROCESSAMENTO EM PARALELO
) as dag:

    tarefas = []
    """Aqui Geramos Parelelização dos dados """
    for tabela, query in QUERIES.items():
        tarefa = PythonOperator(
            task_id=f'processar_{tabela}',
            python_callable=processar_consulta,
            op_kwargs={'tabela': tabela, 'query': query},
            provide_context=True,
        )
        tarefas.append(tarefa)
