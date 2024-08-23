import pytest
import requests
from unittest import mock
from data_pipeline.data_ingestion import get_pokemon  

def test_get_pokemon_success(mocker):
    mock_response = mock.Mock()
    mock_response.json.return_value = {'name': 'pikachu', 'id': 25}
    mock_response.status_code = 200
    mock_get = mocker.patch('requests.get', return_value=mock_response)

    result = get_pokemon('pikachu')
    
    assert result == {'name': 'pikachu', 'id': 25}
    mock_get.assert_called_once_with('https://pokeapi.co/api/v2/pokemon/pikachu')

def test_get_pokemon_http_error(mocker):
    mock_get = mocker.patch('requests.get')
    mock_get.side_effect = requests.exceptions.HTTPError("HTTP Error")
    
    with pytest.raises(RuntimeError):
        get_pokemon('pikachu')

def test_get_pokemon_connection_error(mocker):
    mock_get = mocker.patch('requests.get')
    mock_get.side_effect = requests.exceptions.ConnectionError("Connection Error")
    
    with pytest.raises(RuntimeError):
        get_pokemon('pikachu')

def test_get_pokemon_timeout(mocker):
    mock_get = mocker.patch('requests.get')
    mock_get.side_effect = requests.exceptions.Timeout("Timeout Error")
    
    with pytest.raises(RuntimeError):
        get_pokemon('pikachu')

def test_get_pokemon_request_exception(mocker):
    mock_get = mocker.patch('requests.get')
    mock_get.side_effect = requests.exceptions.RequestException("Request Error")
    
    with pytest.raises(RuntimeError):
        get_pokemon('pikachu')
