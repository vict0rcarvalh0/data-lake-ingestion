import clickhouse_connect
import pandas as pd

def get_clickhouse_client():
    client = clickhouse_connect.get_client(
        host='localhost',
        port='8123',
        username='default',
        password='admin'
    )
    return client

def query_pokemon_data(pokemon_name):
    client = get_clickhouse_client()
    query = f"""
    SELECT 
        ingestion_date,
        pokemon_name,
        abilities,
        base_experience,
        tag
    FROM pokemon_data_view 
    WHERE pokemon_name = '{pokemon_name}'
    """
    result = client.query(query)
    
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    return df

def query_all_pokemon_counts():
    client = get_clickhouse_client()
    query = """
    SELECT 
        pokemon_name,
        COUNT(*) as count
    FROM pokemon_data_view 
    GROUP BY pokemon_name
    ORDER BY count DESC
    """
    result = client.query(query)
    
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    return df

def query_pokemon_abilities_count():
    client = get_clickhouse_client()
    query = """
    SELECT 
        abilities AS ability,
        COUNT(*) AS count
    FROM 
        pokemon_data_view
    GROUP BY 
        abilities
    ORDER BY 
        count DESC
    """
    result = client.query(query)
    df = pd.DataFrame(result.result_rows, columns=['ability', 'count'])
    return df

def query_pokemon_base_experience():
    client = get_clickhouse_client()
    query = """
    SELECT 
        base_experience,
        COUNT(*) AS count
    FROM 
        pokemon_data_view
    GROUP BY 
        base_experience
    ORDER BY 
        base_experience ASC
    """
    result = client.query(query)
    df = pd.DataFrame(result.result_rows, columns=['base_experience', 'count'])
    return df
