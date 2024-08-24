from minio import Minio, S3Error
import os
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def create_bucket_if_not_exists(bucket_name):
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' criado com sucesso!")

    except S3Error as e:
        print(f"Erro ao verificar ou criar bucket: {e}")
        raise RuntimeError(f"Erro ao criar bucket '{bucket_name}'") from e
    except Exception as e:
        print(f"Erro inesperado ao criar bucket '{bucket_name}': {e}")
        raise RuntimeError(f"Erro ao criar bucket '{bucket_name}'") from e

def upload_file(bucket_name, file_name, file_path):
    try:
        minio_client.fput_object(bucket_name, file_name, file_path)
        print(f"Arquivo '{file_name}' enviado com sucesso!")

    except S3Error as e:
        print(f"Erro ao enviar arquivo para o bucket '{bucket_name}': {e}")
        raise RuntimeError(f"Erro ao enviar arquivo '{file_name}' para o bucket '{bucket_name}'") from e
    except FileNotFoundError as e:
        print(f"Arquivo '{file_path}' não encontrado: {e}")
        raise FileNotFoundError(f"Arquivo '{file_path}' não encontrado") from e
    except Exception as e:
        print(f"Erro inesperado ao enviar arquivo '{file_name}' para o bucket '{bucket_name}': {e}")
        raise RuntimeError(f"Erro ao enviar arquivo '{file_name}' para o bucket '{bucket_name}'") from e

def download_file(bucket_name, file_name, download_path):
    try:
        minio_client.fget_object(bucket_name, file_name, download_path)
        print(f"Download do arquivo '{file_name}' feito com sucesso!")
        
    except S3Error as e:
        print(f"Erro ao fazer download do arquivo '{file_name}': {e}")
        raise RuntimeError(f"Erro ao fazer download do arquivo '{file_name}'") from e
    except FileNotFoundError as e:
        print(f"Diretório de destino '{download_path}' não encontrado: {e}")
        raise FileNotFoundError(f"Diretório de destino '{download_path}' não encontrado") from e
    except Exception as e:
        print(f"Erro inesperado ao fazer download do arquivo '{file_name}': {e}")
        raise RuntimeError(f"Erro ao fazer download do arquivo '{file_name}'") from e