import streamlit as st
import pandas as pd
import plotly.express as px
from sklearn.linear_model import LinearRegression
import numpy as np

# Definir o layout da página
st.set_page_config(page_title="Dashboard Financeiro", layout="wide")

# Sidebar com ícones de navegação
with st.sidebar:
    st.image("logo.png", width=150)
    st.markdown("# Início")
    st.markdown("## Transações")
    st.markdown("## Pagamentos")
    st.markdown("## Configurações")
    st.markdown("---")
    st.markdown("### Ajuda")
    st.markdown("### Sair")

@st.cache_data
def load_data():
    # Carregar o DataFrame a partir do arquivo local
    df = pd.read_csv('indicadores', index_col=False)
    # Substituir espaços nos nomes das colunas por underlines
    df.columns = df.columns.str.replace(' ', '_')
    # Retornar o DataFrame
    return df
    
indicadores = load_data()

# Função para calcular a regressão linear
def calcular_media_regressao(df, coluna):
    # Usar o tempo (ano) como variável independente e a coluna selecionada como variável dependente
    df['Ano'] = df['Data'].dt.year
    X = df[['Ano']]
    y = df[coluna].values.reshape(-1, 1)
    
    # Aplicar a regressão linear
    reg = LinearRegression().fit(X, y)
    
    # Prever o valor no último ano da tabela
    ultimo_ano = df['Ano'].max()
    previsao = reg.predict([[ultimo_ano]])
    
    return previsao[0][0]

# Calcular as métricas usando regressão linear em colunas específicas
balance = calcular_media_regressao(indicadores, 'Receita_Liquida')
income = calcular_media_regressao(indicadores, 'Lucro_Líquido')
savings = calcular_media_regressao(indicadores, 'Dividendos')
expenses = calcular_media_regressao(indicadores, 'Divida_Líquida')

# Barra superior (simulação)
col1, col2 = st.columns([4, 1])
with col1:
    st.text_input("Buscar")
with col2:
    st.button("Gerar Relatório")

# Mostrar Métricas Resumidas
st.markdown("## Visão Geral")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(label="Saldo (Balance)", value=f"R${balance:,.2f}", delta="2.5%")

with col2:
    st.metric(label="Renda (Income)", value=f"R${income:,.2f}", delta="0.5%")

with col3:
    st.metric(label="Poupança (Savings)", value=f"R${savings:,.2f}", delta="-1.5%")

with col4:
    st.metric(label="Despesas (Expenses)", value=f"R${expenses:,.2f}", delta="2.5%")

# Gráfico de Receita Líquida e Lucro Líquido
st.markdown("### Receita e Lucro Líquido")
df_melted = indicadores.melt(id_vars=['Data'], value_vars=['Receita_Liquida', 'Lucro_Líquido'],
                             var_name='Indicador', value_name='Valor')

fig = px.line(df_melted, x='Data', y='Valor', color='Indicador',
              title='Evolução da Receita e Lucro Líquido', markers=True)

fig.update_layout(xaxis_title='Data', yaxis_title='Valor')
st.plotly_chart(fig, use_container_width=True)

# Indicadores Categoriais (exemplo de blocos à direita)
col1, col2, col3 = st.columns(3)

with col1:
    st.progress(68, text="Compras")
with col2:
    st.progress(76, text="Trabalho")
with col3:
    st.progress(73, text="Plataforma")

# Tabela de Indicadores
st.markdown("### Tabela de Indicadores")
st.dataframe(indicadores)

# st.subheader('Tabela de Indicadores')
# st.dataframe(
#     indicadores.style.format(subset=indicadores.select_dtypes(include='number').columns, formatter="{:.2f}"),
#     use_container_width=True
# )

      
     

# st.subheader('Tabela de Indicadores')
# st.dataframe(indicadores.style.format(subset=indicadores.select_dtypes(include='number').columns, formatter="{:.2f}"))

# st.subheader('Gráfico de Indicadores')

# variaveis_disponiveis = indicadores.columns.drop('Data')

# variaveis_selecionadas = st.multiselect(
#     'Selecione os indicadores:',
#     options=variaveis_disponiveis
# )

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
