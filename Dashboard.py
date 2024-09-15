import streamlit as st
import pandas as pd
import plotly.express as px
from sklearn.linear_model import LinearRegression
import numpy as np

# Definir o layout da página
st.set_page_config(page_title="Dashboard Financeiro", layout="wide")

# # Estilo CSS para replicar o layout
# st.markdown("""
#     <style>
#     /* Fundo branco para a página */
#     .main {
#         background-color: #F5F5F5;
#         padding: 0px;
#     }
    
#     /* Estilo para a barra lateral */
#     .css-1544g2n {
#         background-color: #F5F5F5;
#     }
    
#     /* Ajuste do fundo dos blocos de métricas */
#     div[data-testid="metric-container"] {
#         background-color: white;
#         border: 1px solid #e6e6e6;
#         padding: 5% 5% 5% 10%;
#         border-radius: 10px;
#         box-shadow: 2px 2px 5px rgba(0, 0, 0, 0.1);
#     }
    
#     /* Cor do texto para as métricas */
#     div[data-testid="metric-container"] > label {
#         color: #8A2BE2;
#         font-size: 18px;
#     }

#     /* Cores das porcentagens positivas e negativas */
#     div[data-testid="metric-container"] > div > p {
#         color: green;
#         font-size: 18px;
#     }

#     /* Barra de progresso (cor personalizada) */
#     .stProgress > div > div > div > div {
#         background-color: #1E90FF;
#     }
#     </style>
#     """, unsafe_allow_html=True)

# Sidebar com ícones de navegação
with st.sidebar:
    #st.image("logo.png", width=150)
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
    # Converter a coluna 'Data' para datetime e extrair apenas o ano
    df['Data'] = pd.to_datetime(df['Data'], errors='coerce').dt.year  # Extrair somente o ano
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


