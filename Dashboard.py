import streamlit as st
import pandas as pd
import plotly.express as px

@st.cache_data
def load_data():
    # Carregar o DataFrame a partir do arquivo local
    df = pd.read_csv('indicadores', index_col=False)
    # Substituir espaços nos nomes das colunas por underlines
    df.columns = df.columns.str.replace(' ', '_')
    # Retornar o DataFrame
    return df
    
indicadores = load_data()

st.title('Dashboard de Indicadores Financeiros')

st.subheader('Tabela de Indicadores')
st.write(indicadores.style.format(subset=indicadores.select_dtypes(include='number').columns, formatter="{:.2f}"))

st.subheader('Gráfico de Indicadores')

variaveis_disponiveis = indicadores.columns.drop('Data')

variaveis_selecionadas = st.multiselect(
    'Selecione os indicadores:',
    options=variaveis_disponiveis
)

if variaveis_selecionadas:
    df_melted = indicadores.melt(
        id_vars=['Data'], 
        value_vars=variaveis_selecionadas,
        var_name='Indicador', 
        value_name='Valor'
    )
    fig = px.line(
        df_melted, 
        x='Data', 
        y='Valor', 
        color='Indicador',
        markers=True,
        title='Evolução dos Indicadores Selecionados'
    )
    fig.update_layout(xaxis_title='Data', yaxis_title='Valor')
    st.plotly_chart(fig)
else:
    st.warning('Por favor, selecione ao menos um indicador para visualizar o gráfico.')
