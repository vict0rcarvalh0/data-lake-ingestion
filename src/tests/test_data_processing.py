import pytest
import pandas as pd
from unittest import mock
from datetime import datetime
from data_pipeline.data_processing import process_data, prepare_dataframe_for_working 
from utils.folders import create_custom_temp_dir

def test_process_data_success(mocker):
    mock_data = {
        'name': 'pikachu',
        'height': 4,
        'weight': 60,
        'base_experience': 112,
        'abilities': [
            {'ability': {'name': 'static'}},
            {'ability': {'name': 'lightning-rod'}}
        ]
    }

    mock_pa_write_table = mocker.patch('pyarrow.parquet.write_table')

    filename, path = process_data(mock_data, 'pikachu')

    assert filename.startswith('raw_pokemon_pikachu_')
    assert path == f'temporary/{filename}'

    mock_pa_write_table.assert_called_once()

def test_process_data_parquet_error(mocker):
    mock_data = {
        'name': 'pikachu',
        'height': 4,
        'weight': 60,
        'base_experience': 112,
        'abilities': [{'ability': {'name': 'static'}}]
    }

    mocker.patch('utils.folders.create_custom_temp_dir', return_value='/tmp')
    mocker.patch('pyarrow.parquet.write_table', side_effect=ValueError("Erro ao escrever Parquet"))

    with pytest.raises(RuntimeError, match="Erro ao salvar dados do Pokémon pikachu no formato Parquet"):
        process_data(mock_data, 'pikachu')

def test_prepare_dataframe_for_working_success():
    data = {
        'name': ['pikachu'],
        'height': [4],
        'weight': [60],
        'base_experience': [112]
    }
    df = pd.DataFrame(data)

    result_df = prepare_dataframe_for_working(df, 'test-tag')

    assert 'ingestion_date' in result_df.columns
    assert 'line_data' in result_df.columns
    assert 'tag' in result_df.columns
    assert result_df['tag'].iloc[0] == 'test-tag'
