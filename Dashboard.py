import streamlit as st
import pandas as pd
import plotly.express as px

# Carregar o DataFrame 'indicadores'
# indicadores = pd.read_csv('seu_arquivo.csv')

# Exemplo de dataframe para demonstração
dados = {
    'Data': pd.date_range(start='2010-01-01', periods=10, freq='Y'),
    'Receita Líquida': [100, 120, 130, 150, 170, 160, 180, 200, 220, 210],
    'Lucro Líquido': [10, 12, 13, 15, 17, 16, 18, 20, 22, 21],
    'ROE': [5, 6, 6.5, 7, 7.5, 7.2, 7.8, 8, 8.5, 8.2]
}
indicadores = pd.DataFrame(dados)

# Verificar e ajustar o nome da coluna de data
if 'Data' not in indicadores.columns:
    if 'data' in indicadores.columns:
        indicadores.rename(columns={'data': 'Data'}, inplace=True)
    else:
        st.error('A coluna de data não foi encontrada no DataFrame.')
        st.stop()

# Garantir que a coluna 'Data' é do tipo datetime
indicadores['Data'] = pd.to_datetime(indicadores['Data'])

st.title('Dashboard de Indicadores Financeiros')

st.subheader('Tabela de Indicadores')
st.dataframe(indicadores)

st.subheader('Gráfico de Indicadores')

# Obter as variáveis disponíveis, excluindo a coluna 'Data'
variaveis_disponiveis = indicadores.columns.drop('Data')

# Seleção múltipla de indicadores
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
    st.plotly_chart(fig)
else:
    st.warning('Por favor, selecione ao menos um indicador para visualizar o gráfico.')
