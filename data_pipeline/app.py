from flask import Flask, request, jsonify
from minio import Minio
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import clickhouse_connect

app = Flask(__name__)

# Configuração do cliente MinIO
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# Criar bucket se não existir
if not minio_client.bucket_exists("raw-data"):
    minio_client.make_bucket("raw-data")

# Função para executar o script SQL
def execute_sql_script():
    client = clickhouse_connect.get_client(host='localhost', port=8123)
    with open('sql/init_db.sql', 'r') as file:
        sql_script = file.read()
    client.command(sql_script)

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

if __name__ == '__main__':
    execute_sql_script()
    app.run(host='0.0.0.0', port=5000)