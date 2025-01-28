from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
import os
from sqlalchemy import create_engine
import re
import pandas as pd
import hashlib

def extrair_do_raw(**kwargs):
    """Extrair o dado do RAW para tranformação de refinamento"""
    DATABASE_RAW_URL = "cockroachdb+psycopg2://megazorders:JBQROkforHRxPkyN2-3LeQ@mega-zordian-7326.j77.aws-us-east-1.cockroachlabs.cloud:26257/defaultdb"
    engine = create_engine(DATABASE_RAW_URL)
    sql = "SELECT * FROM produtos;"
    try:
        with engine.connect() as conn:
            df_result = pd.read_sql(sql, conn)
        return df_result
    except Exception as e:
        print(f"Error extracting data: {e}")
        return None



# def hashmento(value1, value2,value3):
#     """ Gerando uma hash para fazer certeza que o dado não está duplicado,
#     devido poder ter pessoas diferentes com o mesmo email e telefone mas nomes diferentes."""
#     combined = f"{value1}{value2}{value3}"
#     return hashlib.sha256(combined.encode('utf-8')).hexdigest()

def remove_special_characters(value):
    """Remover caracteres especiais"""
    if isinstance(value, str):
        return re.sub(r'[^A-Za-z0-9\s]', '', value)
    return value 

def transform_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract')
    """ Usando os dados """  
    df = pd.DataFrame(data)
    df['nome'] = df['nome'].str.title().apply(remove_special_characters)
    df['descricao'] = df['descricao'].str.title().apply(remove_special_characters)

    """ Ajustando dados nullos"""
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = df[column].fillna('Não definido')
        else:
            df[column] = df[column].fillna(0)
            
    if 'price' in df.columns:
        mean = df['price'].mean()
        df['price'] = df['price'].fillna(mean)
    
    df = df.drop_duplicates(subset=['id'])

    """Realizando a conexão com o banco novamente"""
    DATABASE_REF_URL = "cockroachdb+psycopg2://megazorders:JBQROkforHRxPkyN2-3LeQ@mega-zordian-7326.j77.aws-us-east-1.cockroachlabs.cloud:26257/Refined_stage"

    engine = create_engine(DATABASE_REF_URL)
    
    with engine.connect() as conn:
        df.to_sql('ref_produtos', conn, if_exists='append', index=False)
        
def criar_index(**kwargs):
    """ Gerando um index"""
    DATABASE_REF_URL = "cockroachdb+psycopg2://megazorders:JBQROkforHRxPkyN2-3LeQ@mega-zordian-7326.j77.aws-us-east-1.cockroachlabs.cloud:26257/Refined_stage"
    engine = create_engine(DATABASE_REF_URL)
    
    create_query = """
    CREATE INDEX idx_id_transactions ON ref_transacoes (id_transacao, data_transacao);
    """
    try:
        with engine.connect() as connection:
            connection.execute(create_query)
            print("SUCCESS.")
    except Exception as e:
        print(f"Error: {e}")



def criar_tabela(**kwargs):
    """ Gerando uma tabela para salvar os dados"""
    DATABASE_REF_URL = "cockroachdb+psycopg2://megazorders:JBQROkforHRxPkyN2-3LeQ@mega-zordian-7326.j77.aws-us-east-1.cockroachlabs.cloud:26257/Refined_stage"
    engine = create_engine(DATABASE_REF_URL)
    
    create_table_query = """
        CREATE TABLE IF NOT EXISTS ref_produtos (
            id INT PRIMARY KEY,
            nome VARCHAR(255),
            descricao VARCHAR(255),
            preco DECIMAL(10, 2),
            EAN INT
        );
    """
    try:
        with engine.connect() as connection:
            connection.execute(create_table_query)
            print("SUCCESS.")
    except Exception as e:
        print(f"Error: {e}")



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

with DAG('ref_produto_pipeline', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=extrair_do_raw,
    )

    transforma_dado = PythonOperator(
        task_id='transforma_dado',
        python_callable=transform_data,
        provide_context=True,
    )

    
    criar_tabela_task = PythonOperator(
        task_id='criar_tabela',
        python_callable=criar_tabela,
        provide_context=True,
    )

    criar_index_task = PythonOperator(
        task_id='criar_index',
        python_callable=criar_index,
        provide_context=True,
    )


    extract >> criar_tabela_task >> transforma_dado  >> criar_index_task
