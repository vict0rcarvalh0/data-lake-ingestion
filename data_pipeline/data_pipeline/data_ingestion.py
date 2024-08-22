import requests

def get_pokemon(name):
    try:
        response = requests.get(f'https://pokeapi.co/api/v2/pokemon/{name}')
        response.raise_for_status() 
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"Erro HTTP ao buscar Pokémon {name}: {str(http_err)}")
        raise RuntimeError(f"Erro HTTP ao buscar Pokémon {name}") from http_err
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Erro de conexão ao tentar acessar a API para Pokémon {name}: {str(conn_err)}")
        raise RuntimeError(f"Erro de conexão ao buscar Pokémon {name}") from conn_err
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout ao tentar acessar a API para Pokémon {name}: {str(timeout_err)}")
        raise RuntimeError(f"Timeout ao buscar Pokémon {name}") from timeout_err
    except requests.exceptions.RequestException as req_err:
        print(f"Erro ao fazer a requisição para Pokémon {name}: {str(req_err)}")
        raise RuntimeError(f"Erro ao buscar Pokémon {name}") from req_err