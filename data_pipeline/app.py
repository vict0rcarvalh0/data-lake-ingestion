from flask import Flask, jsonify
from data_pipeline.minio_client import create_bucket_if_not_exists, upload_file, download_file
from data_pipeline.clickhouse_client import get_client, insert_dataframe, execute_sql_script
from data_pipeline.data_processing import process_data, prepare_dataframe_for_insert
from data_pipeline.data_ingestion import get_pokemon
from utils.files import read_parquet
from utils.folders import create_custom_temp_dir
import os

app = Flask(__name__)

create_bucket_if_not_exists("raw-data")

@app.route('/pokemon_pipeline/<name>', methods=['GET'])
def pokemon_pipeline(name):
    pokemon_data = get_pokemon(name)
    print("Dados do Pokémon coletados com sucesso!")

    filename, temp_file_path = process_data(pokemon_data, name)
    print("Dados do Pokémon processados com sucesso!")

    upload_file("raw-data", filename, temp_file_path)
    download_file("raw-data", filename, f"{temp_file_path}")

    df_parquet = read_parquet(temp_file_path)
    df_parquet = prepare_dataframe_for_insert(df_parquet, 'pokemon_data')
    print("DataFrame preparado com sucesso!")

    clickhouse_client = get_client()
    insert_dataframe(clickhouse_client, 'working_data', df_parquet)
    print("Dados inseridos com sucesso!")

    return jsonify({"message": "Dados recebidos, armazenados e processados com sucesso"}), 200

@app.route('/storage-pokemon-on-bucket/<name>', methods=['GET'])
def storage_pokemon_on_bucket(name):
    pokemon_data = get_pokemon(name)
    print("Dados do Pokémon coletados com sucesso!")

    filename, temp_file_path = process_data(pokemon_data, name)
    print("Dados do Pokémon processados com sucesso!")

    upload_file("raw-data", filename, temp_file_path)

    return jsonify(pokemon_data), 200

@app.route('/get-pokemon-from-bucket/<filename>', methods=['GET'])
def get_pokemon_from_bucket(filename):
    temp_dir = create_custom_temp_dir()
    temp_file_path = os.path.join(temp_dir, filename)
    print("Diretório temporário criado com sucesso!")

    download_file("raw-data", filename, f"{temp_file_path}")
    df = read_parquet(temp_file_path)

    pokemon_data = df.to_dict(orient='records')
    print("Dados do Pokémon recuperados com sucesso!")

    return jsonify(pokemon_data), 200

if __name__ == '__main__':
    execute_sql_script('sql/init_db.sql')
    app.run(host='0.0.0.0', port=5000)