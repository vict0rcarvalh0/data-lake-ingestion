import requests

def get_pokemon(name):
    response = requests.get(f'https://pokeapi.co/api/v2/pokemon/{name}')

    return response.json()