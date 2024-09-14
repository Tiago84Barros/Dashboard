mport streamlit as st
import pandas as pd
import plotly.express as px

# Simulação de um dataframe de indicadores financeiros
dados = {
    
}
indicadores = pd.DataFrame(dados)

# Layout do dashboard
st.title('Dashboard de Indicadores Financeiros')

st.subheader('Tabela de Indicadores')
st.dataframe(indicadores)

st.subheader('Gráfico de Indicadores')
variaveis_selecionadas = st.multiselect('Selecione os indicadores:', indicadores.columns.drop('Data'))

if variaveis_selecionadas:
    df_melted = indicadores.melt(id_vars=['Data'], value_vars=variaveis_selecionadas)
    fig = px.line(df_melted, x='Data', y='value', color='variable', title='Evolução dos Indicadores')
    st.plotly_chart(fig)
else:
    st.write('Selecione pelo menos um indicador para visualizar o gráfico.')