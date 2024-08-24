import pytest
from unittest import mock
from minio import S3Error
from data_pipeline.minio_client import create_bucket_if_not_exists, upload_file, download_file  
from utils.folders import create_custom_temp_dir

def test_create_custom_temp_dir(mocker):
    mocker.patch('os.path.exists', return_value=False)
    mock_makedirs = mocker.patch('os.makedirs')

    temp_dir = create_custom_temp_dir()

    mock_makedirs.assert_called_once_with('temporary')
    assert temp_dir == 'temporary'

def test_create_custom_temp_dir_exists(mocker):
    mocker.patch('os.path.exists', return_value=True)
    mock_makedirs = mocker.patch('os.makedirs')

    temp_dir = create_custom_temp_dir()

    mock_makedirs.assert_not_called()
    assert temp_dir == 'temporary'

def test_create_bucket_if_not_exists_success(mocker):
    mock_bucket_exists = mocker.patch('data_pipeline.minio_client.minio_client.bucket_exists', return_value=False)
    mock_make_bucket = mocker.patch('data_pipeline.minio_client.minio_client.make_bucket')

    create_bucket_if_not_exists('test-bucket')

    mock_bucket_exists.assert_called_once_with('test-bucket')
    mock_make_bucket.assert_called_once_with('test-bucket')

def test_create_bucket_if_not_exists_already_exists(mocker):
    mock_bucket_exists = mocker.patch('data_pipeline.minio_client.minio_client.bucket_exists', return_value=True)
    mock_make_bucket = mocker.patch('data_pipeline.minio_client.minio_client.make_bucket')

    create_bucket_if_not_exists('test-bucket')

    mock_bucket_exists.assert_called_once_with('test-bucket')
    mock_make_bucket.assert_not_called()

def test_create_bucket_if_not_exists_s3error(mocker):
    mocker.patch('data_pipeline.minio_client.minio_client.bucket_exists', side_effect=S3Error('Mocked S3Error', 'error message', 'request', 'host', 'region', 'resource', 'request_id'))

    with pytest.raises(RuntimeError, match="Erro ao criar bucket 'test-bucket'"):
        create_bucket_if_not_exists('test-bucket')

def test_upload_file_success(mocker):
    mock_fput_object = mocker.patch('data_pipeline.minio_client.minio_client.fput_object')

    upload_file('test-bucket', 'test-file.txt', '/path/to/test-file.txt')

    mock_fput_object.assert_called_once_with('test-bucket', 'test-file.txt', '/path/to/test-file.txt')

def test_upload_file_s3error(mocker):
    mocker.patch('data_pipeline.minio_client.minio_client.fput_object', side_effect=S3Error('Mocked S3Error', 'error message', 'request', 'host', 'region', 'resource', 'request_id'))

    with pytest.raises(RuntimeError, match="Erro ao enviar arquivo 'test-file.txt' para o bucket 'test-bucket'"):
        upload_file('test-bucket', 'test-file.txt', '/path/to/test-file.txt')

def test_upload_file_not_found_error(mocker):
    mocker.patch('data_pipeline.minio_client.minio_client.fput_object', side_effect=FileNotFoundError("Mocked FileNotFoundError"))

    with pytest.raises(FileNotFoundError, match="Arquivo '/path/to/test-file.txt' não encontrado"):
        upload_file('test-bucket', 'test-file.txt', '/path/to/test-file.txt')

def test_download_file_success(mocker):
    mock_fget_object = mocker.patch('data_pipeline.minio_client.minio_client.fget_object')

    download_file('test-bucket', 'test-file.txt', '/path/to/download/test-file.txt')

    mock_fget_object.assert_called_once_with('test-bucket', 'test-file.txt', '/path/to/download/test-file.txt')

def test_download_file_s3error(mocker):
    mocker.patch('data_pipeline.minio_client.minio_client.fget_object', side_effect=S3Error('Mocked S3Error', 'error message', 'request', 'host', 'region', 'resource', 'request_id'))

    with pytest.raises(RuntimeError, match="Erro ao fazer download do arquivo 'test-file.txt'"):
        download_file('test-bucket', 'test-file.txt', '/path/to/download/test-file.txt')

def test_download_file_not_found_error(mocker):
    mocker.patch('data_pipeline.minio_client.minio_client.fget_object', side_effect=FileNotFoundError("Mocked FileNotFoundError"))

    with pytest.raises(FileNotFoundError, match="Diretório de destino '/path/to/download/test-file.txt' não encontrado"):
        download_file('test-bucket', 'test-file.txt', '/path/to/download/test-file.txt')