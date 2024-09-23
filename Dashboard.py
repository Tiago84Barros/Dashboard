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

st.markdown("""
    <style>
    /* Fundo da página */
    .main {
        background-color: var(--background-color); /* Usando variável para cor de fundo */
        padding: 0px;
        color: var(--text-color); /* Cor de texto dependente do tema */
    }
    
    /* Estilo para a barra lateral */
    .css-1544g2n {
        background-color: var(--background-color);
        color: var(--text-color);
    }
    
    /* Ajuste do fundo dos blocos de métricas */
    div[data-testid="metric-container"] {
        background-color: var(--block-background-color); /* Usando variáveis para mais flexibilidade */
        border: 1px solid var(--block-border-color);
        padding: 5% 5% 5% 10%;
        border-radius: 10px;
        box-shadow: 2px 2px 5px rgba(0, 0, 0, 0.1);
    }
    
    /* Cor do texto para as métricas */
    div[data-testid="metric-container"] > label {
        color: var(--metric-text-color); /* Mantendo a cor roxa ou outra cor */
        font-size: 18px;
    }

    /* Cores das porcentagens positivas e negativas */
    div[data-testid="metric-container"] > div > p {
        color: var(--positive-color); /* Cor para números positivos (usando verde como padrão) */
        font-size: 18px;
    }

    /* Barra de progresso */
    .stProgress > div > div > div > div {
        background-color: var(--progress-bar-color);
    }

    /* Ajuste de cor para widgets */
    .stSelectbox, .stSlider, .stButton, .stCheckbox {
        color: var(--text-color);
    }

    /* Cor do texto em caixas de texto */
    .css-2trqyj {
        color: var(--text-color);
    }
    
    /* Ajuste para os botões */
    button {
        background-color: var(--button-background-color);
        color: var(--button-text-color);
        border-radius: 5px;
        padding: 5px 10px;
        border: none;
    }

    /* Ajuste para hover nos botões */
    button:hover {
        background-color: var(--button-hover-background-color);
        color: var(--button-hover-text-color);
    }

    /* Ajuste do fundo do app */
    .stApp {
        background-color: var(--background-color);
        color: var(--text-color);
    }
    </style>
""", unsafe_allow_html=True)

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

# carregando o banco de dados _______________________________________________________________________________________________________________________________________________________________________________

# URL do banco de dados no GitHub
db_url = "https://raw.githubusercontent.com/Tiago84Barros/Dashboard/main/indicadores_empresas.db"

# Função para baixar o banco de dados do GitHub
@st.cache_data
def download_db_from_github(db_url, local_path='indicadores_empresas.db'):
    try:
        response = requests.get(db_url, allow_redirects=True)        
        if response.status_code == 200:
            with open(local_path, 'wb') as f:
                f.write(response.content)
            return local_path
        else:
            return None
    except requests.exceptions.RequestException as e:
        st.error(f"Erro ao tentar se conectar ao GitHub: {e}")
        return None

# Função para carregar os dados do banco de dados
@st.cache_data
def load_data_from_db(ticket=None, company_name=None):
    db_path = download_db_from_github(db_url)
    
    if db_path is None or not os.path.exists(db_path):
        return None

    try:
        conn = sqlite3.connect(db_path)

      # Se fornecido, o ticket será usado na busca
        if ticket:
            query_tabelas = f"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%{ticket}%'"
        elif company_name:
            # Caso contrário, busca por nome da empresa
            query_tabelas = f"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%{company_name}%'"
        else:
            st.error("É necessário fornecer um ticket ou nome da empresa.")
            return None

        # Lendo os nomes das tabelas que contêm o ticket ou nome da empresa
        tabelas = pd.read_sql_query(query_tabelas, conn)

        # Verificando se encontrou alguma tabela
        if not tabelas.empty:
            nome_tabela = tabelas.iloc[0, 0]  # Pegando o primeiro nome de tabela que contenha o ticket ou empresa
           
            # Escapando o nome da tabela com aspas duplas para evitar erros de sintaxe
            nome_tabela_escapado = f'"{nome_tabela}"'

            # Carregando os dados da tabela
            query_dados = f"SELECT * FROM {nome_tabela_escapado}"
            df = pd.read_sql_query(query_dados, conn)

            return df
        else:
            st.error(f"Nenhuma tabela encontrada para o ticket '{ticket}' ou nome da empresa '{company_name}'")
            return None
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return None
    finally:
        if conn:
            conn.close()
            
# Inserindo o ticker para a busca ___________________________________________________________________________________________________________________________________________________________________________
col1, col2 = st.columns([4, 1])
with col1:
    ticket = st.text_input("Digite o ticker (ex: GMAT3)", key="ticker_input").upper()
    # Atualizar ticker no estado da sessão ao pressionar Enter
    if ticket_input:
        ticker = ticket_input.upper() + ".SA"
        st.session_state.ticker = ticker

indicadores = load_data_from_db(ticket)

# Função para calcular o crescimento médio (CAGR) _______________________________________________________________________________________________________________________________________________________________
def calculate_cagr(df, column):

   try:
        # Verificando se a coluna 'Data' existe e está no formato correto
        if 'Data' not in df.columns:
            raise ValueError("A coluna 'Data' não foi encontrada no DataFrame.")
        
        df['Data'] = pd.to_datetime(df['Data'], errors='coerce')

        # Verificar se houve falha na conversão de datas
        if df['Data'].isnull().any():
            raise ValueError("A coluna 'Data' contém valores inválidos que não puderam ser convertidos para data.")

        # Valores inicial e final da coluna de interesse
        initial_value = df.iloc[0][column]
        final_value = df.iloc[-1][column]

        # Calculando o número de anos (diferenca de tempo em anos)
        num_years = (df['Data'].iloc[-1] - df['Data'].iloc[0]).days / 365.25

        # Verificando possíveis erros nos valores
        if initial_value == 0:
            raise ValueError(f"Valor inicial do indicador '{column}' é zero. Não é possível calcular CAGR.")
        
        if num_years <= 0 or pd.isna(num_years):
            raise ValueError(f"O número de anos calculado é inválido: {num_years}. Verifique as datas fornecidas.")

        # Cálculo do CAGR
        cagr = (final_value / initial_value) ** (1 / num_years) - 1
        return cagr

   except Exception as e:
        st.error(f"Erro ao calcular o CAGR: {e}")
        return np.nan  # Retorna NaN em caso de erro

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
    
# Verificar se o botão foi pressionado _____________________________________________________________________________________________________________________________________________________________________
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
    st.metric(label="CAGR Receita Líquida", value=f"{cagrs['Receita_Líquida']:.2%}")

with col2:
    st.metric(label="CAGR Lucro Líquido", value=f"{cagrs['Lucro_Líquido']:.2%}")

with col3:
    st.metric(label="CAGR Dividendos", value=f"{cagrs['Dividendos']:.2%}")

with col4:
    st.metric(label="CAGR Dívida Líquida", value=f"{cagrs['Divida_Líquida']:.2%}")


# Seletor para escolher quais variáveis visualizar no gráfico _______________________________________________________________________________________________________________________________________
st.markdown("### Selecione os Indicadores para Visualizar no Gráfico")
variaveis_disponiveis = [col for col in indicadores.columns if col != 'Data']
variaveis_selecionadas = st.multiselect("Escolha os Indicadores:", variaveis_disponiveis, default=['Receita_Líquida', 'Lucro_Líquido', 'Divida_Líquida'])

#Copiar código
# Ensure 'indicadores' is correctly loaded
if variaveis_selecionadas:

     # Função para verificar e atualizar o tema do Streamlit
    def update_theme():
        # # Adiciona um seletor de tema para que o usuário escolha
        # selected_theme = st.selectbox("Escolha o tema do gráfico:", ["light", "dark"])
    
        # # Atualiza o tema na sessão com base na seleção do usuário
        # if 'theme' not in st.session_state or st.session_state['theme'] != selected_theme:
        #     st.session_state['theme'] = selected_theme
        #     st.write(f"Tema selecionado: {selected_theme}")

           
        # Configurações de cores com base no tema armazenado na sessão
        current_theme = st.session_state.get('theme', 'light')
        st.write(f"Tema atual utilizado para o gráfico: {current_theme}")
        # Se `current_theme` estiver `None`, define um tema padrão (light) para garantir que o código não quebre
        if current_theme is None:
            current_theme = "light"  # Definindo o tema padrão como 'light'
    
        # Atualiza apenas se o tema atual for diferente do armazenado
        if 'theme' not in st.session_state:
            st.session_state['theme'] = current_theme
            st.write(f"Tema inicial detectado: {current_theme}")
        elif st.session_state['theme'] != current_theme:
            st.session_state['theme'] = current_theme
            st.write(f"Tema atualizado para: {current_theme}")
    
      # Chamar a função para verificar o tema ao iniciar o aplicativo
    update_theme()
 
    def plot_graph(df_melted):
                
        # Configurações de cores com base no tema armazenado na sessão
        current_theme = st.session_state['theme']
        st.write(f"Tema atual utilizado para o gráfico: {current_theme}")

        # Configurações de cores com base no tema
        if st.session_state['theme'] == "dark":
            theme_colors = {
                "bg_color": "#1f1f1f",
                "text_color": "#ffffff",
                "grid_color": "#444444"
            }
        else:
            theme_colors = {
                "bg_color": "#ffffff",
                "text_color": "#000000",
                "grid_color": "#dddddd"
            }
        
        # Criando o gráfico com cores adaptativas
        fig = px.line(df_melted, x='Data', y='Valor', color='Indicador', markers=True,
                      title='Evolução dos Indicadores Selecionados')
        
        fig.update_layout(
            xaxis_title='Ano',
            yaxis_title='Valor',
            plot_bgcolor=theme_colors['bg_color'],
            paper_bgcolor=theme_colors['bg_color'],
            font=dict(color=theme_colors['text_color']),
            title_font=dict(color=theme_colors['text_color'], size=24),
            legend_title_text='Indicadores',
            xaxis=dict(showgrid=True, gridcolor=theme_colors['grid_color']),
            yaxis=dict(showgrid=True, gridcolor=theme_colors['grid_color'])
        )
        
        # Renderizando o gráfico
        st.plotly_chart(fig, use_container_width=True)
    
    # Criar o DataFrame derretido (df_melted já existente)
    df_melted = indicadores.melt(id_vars=['Data'], value_vars=variaveis_selecionadas,
                                 var_name='Indicador', value_name='Valor')
       
    # Chama a função para exibir o gráfico
    plot_graph(df_melted)
    
   
else:
    st.warning("Por favor, selecione pelo menos um indicador para exibir no gráfico.")
# Tabela de Indicadores  ___________________________________________________________________________________________________________________________________________________________________________

st.markdown("### Tabela de Indicadores")
st.dataframe(indicadores_formatado)
