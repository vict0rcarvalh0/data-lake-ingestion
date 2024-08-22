import clickhouse_connect
import os
from dotenv import load_dotenv 

load_dotenv()

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST')
CLICKHOUSE_PORT = os.getenv('CLICKHOUSE_PORT')
CLICKHOUSE_USERNAME = os.getenv('CLICKHOUSE_USERNAME')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')

def get_client():
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USERNAME,
            password=CLICKHOUSE_PASSWORD
        )
        return client
    except Exception as e:
        print(f"Erro ao conectar ao ClickHouse: {str(e)}")
        raise ConnectionError("Não foi possível conectar ao ClickHouse") from e
    
def execute_sql_script(script_path):
    try:
        client = get_client()
        with open(script_path, 'r') as file:
            sql_script = file.read()
        client.command(sql_script)
        return client
    except FileNotFoundError as e:
        print(f"Arquivo SQL não encontrado: {str(e)}")
        raise FileNotFoundError(f"O arquivo {script_path} não foi encontrado") from e
    except Exception as e:
        print(f"Erro ao executar script SQL: {str(e)}")
        raise RuntimeError("Falha ao executar o script SQL") from e

def insert_dataframe(client, table_name, df):
    try:
        client.insert_df(table_name, df)
    except Exception as e:
        print(f"Erro ao inserir dataframe na tabela {table_name}: {str(e)}")
        raise RuntimeError(f"Falha ao inserir dados na tabela {table_name}") from e
