from flask import Flask, request, jsonify
from minio import Minio
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import clickhouse_connect
import requests
import tempfile
import os

app = Flask(__name__)

# Configuração do cliente MinIO
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Criar bucket se não existir
if not minio_client.bucket_exists("raw-data"):
    minio_client.make_bucket("raw-data")

# Função para executar o script SQL
def execute_sql_script():
    client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='admin')
    with open('sql/init_db.sql', 'r') as file:
        sql_script = file.read()
    client.command(sql_script)

def create_custom_temp_dir():
    temp_dir = "temporary"
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    return temp_dir

@app.route('/data', methods=['POST'])
def receive_data():
    data = request.get_json()
    if not data or 'date' not in data or 'data' not in data:
        return jsonify({"error": "Formato de dados inválido"}), 400

    try:
        datetime.fromtimestamp(data['date'])
        int(data['data'])
    except (ValueError, TypeError):
        return jsonify({"error": "Tipo de dados inválido"}), 400

    # Criar DataFrame e salvar como Parquet
    df = pd.DataFrame([data])
    filename = f"raw_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
    table = pa.Table.from_pandas(df)
    pq.write_table(table, filename)

    # Fazer upload para o MinIO
    minio_client.fput_object("raw-data", filename, filename)

    # Ler arquivo Parquet do MinIO
    minio_client.fget_object("raw-data", filename, f"downloaded_{filename}")
    df_parquet = pd.read_parquet(f"downloaded_{filename}")

    # Inserir dados no ClickHouse
    client = clickhouse_connect.get_client(host='localhost', port=8123)
    df_parquet['ingestion_date'] = datetime.now()
    df_parquet['line_data'] = df_parquet.apply(lambda row: row.to_json(), axis=1)
    df_parquet['tag'] = 'example_tag'

    client.insert_df('working_data', df_parquet[['ingestion_date', 'line_data', 'tag']])

    return jsonify({"message": "Dados recebidos, armazenados e processados com sucesso"}), 200

@app.route('/storage-pokemon-on-bucket/<name>', methods=['GET'])
def get_pokemon(name):
    # Requisição à PokeAPI
    response = requests.get(f'https://pokeapi.co/api/v2/pokemon/{name}')
    if response.status_code != 200:
        return jsonify({"error": "Pokémon não encontrado"}), 404

    pokemon_data = response.json()

    # Criar DataFrame e salvar como Parquet
    df = pd.DataFrame([{
        'name': pokemon_data['name'],
        'height': pokemon_data['height'],
        'weight': pokemon_data['weight'],
        'base_experience': pokemon_data['base_experience'],
        'abilities': ', '.join([ability['ability']['name'] for ability in pokemon_data['abilities']])
    }])

    filename = f"raw_pokemon_{name.lower()}_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
    temp_dir = create_custom_temp_dir()
    temp_file_path = os.path.join(temp_dir, filename)
    
    # Salvar o DataFrame como Parquet no diretório temporário
    table = pa.Table.from_pandas(df)
    pq.write_table(table, temp_file_path)

    # Fazer upload do arquivo JSON para o MinIO
    minio_client.fput_object("raw-data", filename, temp_file_path)

    return jsonify(pokemon_data), 200

@app.route('/get-pokemon-from-bucket/<filename>', methods=['GET'])
def get_pokemon_from_minio(filename):
    temp_dir = create_custom_temp_dir()
    temp_file_path = os.path.join(temp_dir, filename)

    # Baixar o arquivo Parquet do MinIO
    try:
        minio_client.fget_object("raw-data", filename, f"{temp_file_path}")
        df = pd.read_parquet(temp_file_path)
        print("df:", df)
    except Exception as e:
        return jsonify({"error": f"Erro ao acessar o arquivo Parquet: {str(e)}"}), 500

    # Converter o DataFrame para JSON e retornar a resposta
    pokemon_data = df.to_dict(orient='records')

    return jsonify(pokemon_data), 200

if __name__ == '__main__':
    execute_sql_script()
    app.run(host='0.0.0.0', port=5000)