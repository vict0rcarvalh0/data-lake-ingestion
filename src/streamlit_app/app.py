import streamlit as st
from utils.db import query_pokemon_data, query_all_pokemon_counts, query_pokemon_base_experience, query_pokemon_abilities_count

st.title("Pokémon Data Dashboard")

pokemon_name = st.text_input("Digite o nome do Pokémon", value="pikachu")

if st.button("Buscar Dados"):
    df = query_pokemon_data(pokemon_name)

    if not df.empty:
        st.write(f"Dados para {pokemon_name}:")
        st.dataframe(df)
    else:
        st.write(f"Nenhum dado encontrado para {pokemon_name}.")



st.header("Contagem de Pokémon")

df_counts = query_all_pokemon_counts()

if not df_counts.empty:
    st.bar_chart(df_counts.set_index('pokemon_name'))
else:
    st.write("Nenhum dado de Pokémon disponível para exibir o gráfico.")



st.header("Frequência das Habilidades dos Pokémon")
df_abilities = query_pokemon_abilities_count()

if not df_abilities.empty:
    st.bar_chart(df_abilities.set_index('ability'))
else:
    st.write("Nenhuma habilidade encontrada.")



st.header("Distribuição da Experiência Base dos Pokémon")
df_experience = query_pokemon_base_experience()

if not df_experience.empty:
    st.bar_chart(df_experience.set_index('base_experience'))
else:
    st.write("Nenhuma experiência base encontrada.")