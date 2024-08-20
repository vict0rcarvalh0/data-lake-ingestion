import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

def process_data(data):
	df = pd.DataFrame([data])
	filename = f"raw_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
	table = pa.Table.from_pandas(df)
	pq.write_table(table, filename)
	
def prepare_dataframe_for_insert(df):
	df['ingestion_date'] = datetime.now()
	df['line_data'] = df.apply(Lambda row: row.to_json(), axis=1)
	df['tag'] = 'example_tag'
	return df[['ingestion_date', 'line_data', 'tag']]