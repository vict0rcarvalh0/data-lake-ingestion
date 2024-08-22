import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from utils.folders import create_custom_temp_dir
import os

def process_data(data, name):
    try:
        df = pd.DataFrame([{
            'name': data['name'],
            'height': data['height'],
            'weight': data['weight'],
            'base_experience': data['base_experience'],
            'abilities': ', '.join([ability['ability']['name'] for ability in data['abilities']])
        }])

        filename = f"raw_pokemon_{name.lower()}_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
        temp_dir = create_custom_temp_dir()
        temp_file_path = os.path.join(temp_dir, filename)
        
        table = pa.Table.from_pandas(df)
        pq.write_table(table, temp_file_path)
        
        return filename, temp_file_path
    
    except KeyError as e:
        print(f"Chave ausente nos dados do Pokémon: {e}")
        raise ValueError(f"Erro ao processar os dados do Pokémon {name}. Chave ausente: {e}") from e
    except (pa.ArrowInvalid, ValueError) as e:
        print(f"Erro ao converter o DataFrame para Parquet: {e}")
        raise RuntimeError(f"Erro ao salvar dados do Pokémon {name} no formato Parquet") from e
    except Exception as e:
        print(f"Erro inesperado ao processar os dados do Pokémon {name}: {e}")
        raise RuntimeError(f"Erro ao processar os dados do Pokémon {name}") from e

def prepare_dataframe_for_insert(df, tag):
    try:
        df['ingestion_date'] = datetime.now()
        df['line_data'] = df.apply(lambda row: row.to_json(), axis=1)
        df['tag'] = tag
        return df[['ingestion_date', 'line_data', 'tag']]
    
    except KeyError as e:
        print(f"Erro ao preparar DataFrame para inserção: {e}")
        raise ValueError("Erro ao preparar DataFrame para inserção de dados. Coluna ausente.") from e
    except Exception as e:
        print(f"Erro inesperado ao preparar DataFrame para inserção: {e}")
        raise RuntimeError("Erro ao preparar DataFrame para inserção") from e