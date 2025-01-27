from datetime import datetime, timedelta
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy import create_engine

DATA_FOLDER = os.path.join(os.sep, 'rawdata','transactions','/')

""" URL PARA O CONECTOR ONLINE DO COCKROACHDB """
DATABASE_URL = "cockroachdb+psycopg2://megazorders:JBQROkforHRxPkyN2-3LeQ@mega-zordian-7326.j77.aws-us-east-1.cockroachlabs.cloud:26257/defaultdb"


def print_pasta():
    """ Verificando locais"""
    print("Diretorio Atual:", os.getcwd())

def print_process(dataframe):
    """ Verificando dataframes """
    print(dataframe)

def process_and_load_files():
    """Acessa a pasta principal"""
    print_pasta()
    """Cria um dataframe vazio para armazenar a concaternação """
    concat_df = pd.DataFrame()

    """ Para cada nome de arquivo no listdir """
    for nm_arquivo in os.listdir('rawdata/transactions'):
        print(nm_arquivo)
        """Printa o nome do arquivo e depois verifica somente os .csv"""
        
        if nm_arquivo.endswith('.csv'):
            """ Pega o nome do arquivo mais diretorio """
            diretorio_arq = os.path.join('rawdata/transactions', nm_arquivo)
            """ Carrega no dataframe """
            df = pd.read_csv(diretorio_arq)
            """Dá um pd.concat com ignore"""
            concat_df = pd.concat([concat_df, df], ignore_index=True)
            concat_df['data_transacao'] = pd.to_datetime(concat_df['data_transacao'], format='ISO8601')

            """ Tirar duplicados das chaves principais """
            concat_df.drop_duplicates(subset='id_transacao')
            
            """Printa o dataframe final"""
            print_process(concat_df)
    
    """ Abre a engine de conexão com o cockroachdb devido não ter um conector nativo airflow"""
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        # concat_df.to_sql('transacoes', conn, if_exists='append', index=False)
        concat_df.to_sql('staging_transacoes', conn, if_exists='append', index=False)
        
""" APARTIR DOS DADOS ABAIXOS E CRIADO UMA TABELA DE STAGING AONDE TRATAMOS DE IDS QUE NÂO EXISTEM NA PRODUCT
E CLIENTE TABLES, JUSTAMENTE DEVIDO O PK E FK DAS TABELAS PARA NÂO OCORRER ERROS EM IMPORTAÇÂO E EXPORTAÇÂO
MANTENDO UMA TABELA ORIGINAL E UMA AJUSTADA AINDA RAW"""


"""Argumentos da dag"""
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

""" Deixando a configuração da DAG """
with DAG(
    'raw_data_transacao_loader',
    default_args=default_args,
    description='RAW data loading to postgres(cockroachdb)',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    salvar_dados_task = PythonOperator(
        task_id='carregando_arquivos_DB',
        python_callable=process_and_load_files,
    )

    criar_staging_table = PostgresOperator(
        task_id='criar_staging_table',
        postgres_conn_id='raw_conection',
        sql="""
            CREATE TABLE IF NOT EXISTS staging_transacoes (
                id_transacao INT,
                id_cliente INT,
                id_produto INT,
                quantidade INT,
                data_transacao TIMESTAMP
            );
        """,
    )

    criar_tabela_task = PostgresOperator(
        task_id='criar_tabela',
        postgres_conn_id='raw_conection',
        sql="""
        CREATE TABLE IF NOT EXISTS transacoes (
            id_transacao INT PRIMARY KEY,
            id_cliente INT REFERENCES clientes(id),
            id_produto INT REFERENCES produtos(id),
            quantidade INT,
            data_transacao TIMESTAMP
        );
        """,
    )
    
    setar_staging_to_raw = PostgresOperator(
        task_id='set_staging_to_raw',
        postgres_conn_id='raw_conection',
        sql="""
            INSERT INTO transacoes (id_transacao, id_cliente, id_produto, quantidade, data_transacao)
            SELECT id_transacao, id_cliente, id_produto, quantidade, data_transacao
            FROM staging_transacoes
            WHERE id_cliente IN (SELECT id FROM clientes)
            AND id_produto IN (SELECT id FROM produtos);
        );
        """,
    )
    
    
    
    
    
    criar_staging_table >> criar_tabela_task >> salvar_dados_task >> setar_staging_to_raw
