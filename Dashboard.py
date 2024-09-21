import streamlit as st
import pandas as pd
import requests
import plotly.express as px
import yfinance as yf
from sklearn.linear_model import LinearRegression
import numpy as np
import sqlite3
import os

# Função para obter a URL do logotipo a partir do repositório no GitHub ___________________________________________________________________________________________________________________________________________
def get_logo_url(ticker):
    ticker_clean = ticker.replace('.SA', '').upper()  # Remover o sufixo ".SA" e garantir que o ticker esteja em maiúsculas
    logo_url = f"https://raw.githubusercontent.com/thefintz/icones-b3/main/icones/{ticker_clean}.png"
    return logo_url
  
# Função para buscar informações da empresa usando yfinance
def get_company_info(ticker):
    try:
        # Adicionar ".SA" para tickers da B3 (bolsa brasileira) se não estiver presente
        if not ticker.endswith(".SA"):
            ticker += ".SA"
      
        # Usar yfinance para pegar informações básicas da empresa
        company = yf.Ticker(ticker)
        info = company.info
      
        return info['longName'], info.get('website')  # Retorna o nome da empresa e o site
    except:
        return None, None
        
# Definir o layout da página ___________________________________________________________________________________________________________________________________________________________--
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

# Sidebar com ícones de navegação __________________________________________________________________________________________________________________________________________________________
with st.sidebar:
    #st.image("logo.png", width=150)
    st.markdown("# Início")
    st.markdown("## Transações")
    st.markdown("## Pagamentos")
    st.markdown("## Configurações")
    st.markdown("---")
    st.markdown("### Ajuda")
    st.markdown("### Sair")

# carregando o banco de dados _____________________________________________________________________________________________________________________________________________________________

# URL do banco de dados no GitHub
# db_url = "https://github.com/Tiago84Barros/Dashboard/blob/main/indicadores_empresas.db"
db_url = "https://raw.githubusercontent.com/Tiago84Barros/Dashboard/main/indicadores_empresas.db"

@st.cache_data
def download_db_from_github(db_url, local_path='indicadores_empresas.db'):
    # Função para baixar o banco de dados do GitHub
    response = requests.get(db_url)
    
    # Verifica se o download foi bem-sucedido
    if response.status_code != 200:
        st.error("Erro ao baixar o banco de dados do GitHub.")
        return None
    
    with open(local_path, 'wb') as f:
        f.write(response.content)
    return local_path

@st.cache_data
def load_data(ticket=None, company_name=None):
    # Carregar o banco de dados SQLite baixado
    db_path = download_db_from_github(db_url)

    if db_path is None:
        st.error("Banco de dados não foi baixado.")
        return None
    
    # Verifique se o arquivo existe
    if not os.path.exists(db_path):
        st.error("Arquivo do banco de dados não encontrado.")
        return None

    try:
        conn = sqlite3.connect(db_path)
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return None
    
    #conn = sqlite3.connect(db_path)
    
    # Listar todas as tabelas no banco de dados
    query = "SELECT name FROM sqlite_master WHERE type='table'"
    tables = pd.read_sql_query(query, conn)['name'].tolist()
    
    # Filtrar tabelas que contenham o ticker ou o nome da empresa no nome da tabela
    if ticket:
        table_name = next((t for t in tables if ticket in t), None)
    elif company_name:
        table_name = next((t for t in tables if company_name in t), None)
    
    if table_name:
        # Carregar os dados da tabela encontrada
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()
        
        # Converter a coluna 'Data' para datetime e extrair apenas o ano
        df['Data'] = pd.to_datetime(df['Data'], errors='coerce').dt.year.astype(int)
        
        # Substituir espaços nos nomes das colunas por underlines
        df.columns = df.columns.str.replace(' ', '_')
        
        return df
    else:
        conn.close()
        return None

# Solicita ao usuário inserir um ticker 
col1, col2 = st.columns([4, 1])
with col1:
    ticket = st.text_input("Insira um ticker:").upper()

# Função para buscar e carregar dados de uma tabela específica
indicadores = load_data("GMAT3.SA")

# Adicionar placeholders ou layout vazio antes de o usuário inserir o ticket
if indicadores is None:
    st.write("Bem-vindo ao Dashboard de Indicadores de Empresas!")
    st.write("Insira um ticker no campo acima para começar a visualização dos dados.")

# Função para calcular o crescimento médio (CAGR) _______________________________________________________________________________________________________________________________________
def calculate_cagr(df, column):
    initial_value = df.iloc[0][column]
    final_value = df.iloc[-1][column]
    num_years = df['Data'].iloc[-1] - df['Data'].iloc[0]
    
    # Cálculo do CAGR
    if initial_value != 0:
        cagr = (final_value / initial_value) ** (1 / num_years) - 1
    else:
        cagr = np.nan  # Caso o valor inicial seja zero, não é possível calcular o CAGR
    
    return cagr

# Calcular o CAGR para cada indicador
cagrs = {}
for column in indicadores.columns:
    if column != 'Data':
        cagr = calculate_cagr(indicadores, column)
        cagrs[column] = cagr

# Função para formatar colunas monetárias e porcentagens _________________________________________________________________________________________________________________________________________
def format_dataframe(df):
    col_monetarias = ['Close', 'LPA', 'Receita_Líquida', 'Ativo_Circulante', 'Passivo_Circulante', 'Capital_de_Giro', 'Patrimonio_Líquido', 
                      'Lucro_Operacional', 'Lucro_Líquido', 'Dividendos', 'Divida_Líquida', 'Balanca_Comercial', 'Câmbio', 'PIB']
    col_porcentagem = ['Margem_Líquida', 'ROE', 'índice_endividamento', 'Selic', 'IPCA', 'ICC']
    
    # Formatando colunas monetárias manualmente (R$)
    for col in col_monetarias:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')  # Garantir que está em formato numérico
            df[col] = df[col].apply(lambda x: f"R${x:,.2f}" if pd.notnull(x) else x)
    
    # Formatando colunas de porcentagem manualmente (%)
    for col in col_porcentagem:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')  # Garantir que está em formato numérico
            df[col] = df[col].apply(lambda x: f"{x:.2f}%" if pd.notnull(x) else x)

     # Remover o sublinhado dos nomes das colunas
    df.columns = df.columns.str.replace('_', ' ')  # Substituir sublinhados por espaços

    # Garantir que 'Data' seja exibida como um número inteiro sem vírgulas
    df['Data'] = df['Data'].astype(str)  # Converte para string para garantir a exibição correta
     
    return df

# Aplicar formatação na tabela de indicadores
indicadores_formatado = format_dataframe(indicadores.copy())
    
# Barra superior (simulação) buscando a logo das empresas ____________________________________________________________________________________________________________________________________________
col1, col2 = st.columns([4, 1])
with col1:
    ticket = st.text_input("GMAT3.SA").upper()

# Verificar se o botão foi pressionado
if ticket:
    # Buscar informações da empresa e verificar se existe
    company_name, company_website = get_company_info(ticket)
    
    if company_name:
        st.subheader(f"Visão Geral - {company_name}")
        # Buscar o logotipo usando a URL do repositório
        logo_url = get_logo_url(ticket)
        
        # Exibir o logotipo no canto direito
        col1, col2 = st.columns([4, 1])
        with col1:
            st.write(f"Informações financeiras de {company_name}")
        with col2:
            # Exibir o logotipo diretamente usando o Streamlit
            st.image(logo_url, width=80)  # Carregando a imagem diretamente da URL
    else:
        st.error("Empresa não encontrada.")
  
# Mostrar Métricas Resumidas ____________________________________________________________________________________________________________________________________________________________________________
st.markdown("## Visão Geral (CAGR)")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(label="CAGR Receita Líquida", value=f"{cagrs['Receita_Liquida']:.2%}")

with col2:
    st.metric(label="CAGR Lucro Líquido", value=f"{cagrs['Lucro_Líquido']:.2%}")

with col3:
    st.metric(label="CAGR Dividendos", value=f"{cagrs['Dividendos']:.2%}")

with col4:
    st.metric(label="CAGR Dívida Líquida", value=f"{cagrs['Divida_Líquida']:.2%}")


# Seletor para escolher quais variáveis visualizar no gráfico _______________________________________________________________________________________________________________________________________
st.markdown("### Selecione os Indicadores para Visualizar no Gráfico")
variaveis_disponiveis = [col for col in indicadores.columns if col != 'Data']
variaveis_selecionadas = st.multiselect("Escolha os Indicadores:", variaveis_disponiveis, default=['Receita_Liquida', 'Lucro_Líquido'])

# Gráfico de indicadores selecionados
if variaveis_selecionadas:
    df_melted = indicadores.melt(id_vars=['Data'], value_vars=variaveis_selecionadas,
                                 var_name='Indicador', value_name='Valor')

    # Configurando layout escuro e exibindo valores nos eixos
    fig = px.line(df_melted, x='Data', y='Valor', color='Indicador',
                  title='Evolução dos Indicadores Selecionados', markers=True)


    fig.update_layout(xaxis_title='Ano', yaxis_title='Valor', yaxis=dict(showgrid=True, gridcolor='#444444'))
    fig.update_layout(xaxis_title='Ano', yaxis_title='Valor',
                      plot_bgcolor='#1f1f1f',  # Fundo escuro
                      paper_bgcolor='#1f1f1f',
                      font=dict(color='#ffffff'),  # Cor do texto
                      title_font=dict(color='#ffffff', size=24),
                      legend_title_text='Indicadores',
                      xaxis=dict(showgrid=True, gridcolor='#444444'),
                      yaxis=dict(showgrid=True, gridcolor='#444444'))

    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("Por favor, selecione pelo menos um indicador para exibir no gráfico.")

# Tabela de Indicadores  ___________________________________________________________________________________________________________________________________________________________________________

st.markdown("### Tabela de Indicadores")
st.dataframe(indicadores_formatado)
