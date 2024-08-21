import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from utils.folders import create_custom_temp_dir
import os

def process_data(data, name):
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
	
def prepare_dataframe_for_insert(df, tag):
	df['ingestion_date'] = datetime.now()
	df['line_data'] = df.apply(lambda row: row.to_json(), axis=1)
	df['tag'] = tag
	return df[['ingestion_date', 'line_data', 'tag']]