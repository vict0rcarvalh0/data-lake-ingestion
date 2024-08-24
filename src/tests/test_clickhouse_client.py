import pytest
from unittest import mock
import clickhouse_connect
import os
from data_pipeline.clickhouse_client import get_client, execute_sql_script, insert_dataframe  

def test_get_client_success(mocker):
    mock_get_client = mocker.patch('clickhouse_connect.get_client')
    mock_get_client.return_value = mock.Mock()

    client = get_client()
    assert client is not None
    mock_get_client.assert_called_once_with(
        host=os.getenv('CLICKHOUSE_HOST'),
        port=os.getenv('CLICKHOUSE_PORT'),
        username=os.getenv('CLICKHOUSE_USERNAME'),
        password=os.getenv('CLICKHOUSE_PASSWORD')
    )

def test_get_client_failure(mocker):
    mock_get_client = mocker.patch('clickhouse_connect.get_client')
    mock_get_client.side_effect = Exception("Erro de conexão")

    with pytest.raises(ConnectionError):
        get_client()

def test_execute_sql_script_success(mocker):
    mock_get_client = mocker.patch('clickhouse_connect.get_client')
    mock_client = mock.Mock()
    mock_get_client.return_value = mock_client

    mock_open = mocker.patch('builtins.open', mock.mock_open(read_data="SELECT * FROM test"))
    mock_client.command = mock.Mock()

    execute_sql_script('path/to/script.sql')

    mock_open.assert_called_once_with('path/to/script.sql', 'r')
    mock_client.command.assert_called_once_with("SELECT * FROM test")

def test_execute_sql_script_file_not_found(mocker):
    mocker.patch('clickhouse_connect.get_client', return_value=mock.Mock())
    mock_open = mocker.patch('builtins.open', side_effect=FileNotFoundError("Arquivo não encontrado"))

    with pytest.raises(FileNotFoundError):
        execute_sql_script('path/to/invalid_script.sql')

def test_execute_sql_script_failure(mocker):
    mock_get_client = mocker.patch('clickhouse_connect.get_client')
    mock_client = mock.Mock()
    mock_get_client.return_value = mock_client

    mock_open = mocker.patch('builtins.open', mock.mock_open(read_data="SELECT * FROM test"))
    mock_client.command.side_effect = Exception("Erro na execução do SQL")

    with pytest.raises(RuntimeError):
        execute_sql_script('path/to/script.sql')

def test_insert_dataframe_success(mocker):
    mock_get_client = mocker.patch('clickhouse_connect.get_client')
    mock_client = mock.Mock()
    mock_get_client.return_value = mock_client

    df = mock.Mock()
    insert_dataframe(mock_client, 'test_table', df)

    mock_client.insert_df.assert_called_once_with('test_table', df)

def test_insert_dataframe_failure(mocker):
    mock_get_client = mocker.patch('clickhouse_connect.get_client')
    mock_client = mock.Mock()
    mock_get_client.return_value = mock_client

    df = mock.Mock()
    mock_client.insert_df.side_effect = Exception("Erro ao inserir dataframe")

    with pytest.raises(RuntimeError):
        insert_dataframe(mock_client, 'test_table', df)
