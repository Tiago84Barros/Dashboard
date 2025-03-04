import streamlit as st
import pandas as pd
import requests
import plotly.express as px
import matplotlib.pyplot as plt
import yfinance as yf
from sklearn.linear_model import LinearRegression
import numpy as np
import sqlite3
import openai
import os

# Função para obter a URL do logotipo a partir do repositório no GitHub ___________________________________________________________________________________________________________________________________________

def get_logo_url(ticker):
    ticker_clean = ticker.replace('.SA', '').upper()  # Remover o sufixo ".SA" e garantir que o ticker esteja em maiúsculas
    logo_url = f"https://raw.githubusercontent.com/thefintz/icones-b3/main/icones/{ticker_clean}.png"
    return logo_url
  
# Função para buscar informações da empresa usando yfinance _______________________________________________________________________________________________________________________________________________________
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
      
# Definir o layout da página ______________________________________________________________________________________________________________________________________________________________________________________

# Definir o layout da página como o primeiro comando
st.set_page_config(page_title="Dashboard Financeiro", layout="wide")

# Adicionar o estilo CSS para personalizar a aparência da página
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


# Carregando o banco de dados _______________________________________________________________________________________________________________________________________________________________________________

# URL do banco de dados no GitHub
db_url = "https://raw.githubusercontent.com/Tiago84Barros/Dashboard/main/metadados.db"

# Função para baixar o banco de dados do GitHub
@st.cache_data(ttl=3600)  # Atualiza o cache a cada 1 hora
def download_db_from_github(db_url, local_path='metadados.db'):
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

# Função para carregar os SETORES do banco de dados _______________________________________________________________________________________________________________________________________________________________
@st.cache_data
def load_setores_from_db():
    db_path = download_db_from_github(db_url)
    
    if db_path is None or not os.path.exists(db_path):
        return None

    try:
        conn = sqlite3.connect(db_path)

        # Buscar dados da tabela 'setores'
        query_setores = "SELECT * FROM setores"
        df_setores = pd.read_sql_query(query_setores, conn)
        return df_setores
    except Exception as e:
        st.error(f"Erro ao carregar a tabela 'setores': {e}")
        return None
    finally:
        if conn:
            conn.close()

# Carregar os setores
setores = load_setores_from_db()

# Função para carregar os dados das DEMONSTRAÇÕES FINANCEIRAS do banco de dados _______________________________________________________________________________________________________________________________________________________________
@st.cache_data
def load_data_from_db(ticker):
    db_path = download_db_from_github(db_url)
    
    if db_path is None or not os.path.exists(db_path):
        return None
    
    try:
        conn = sqlite3.connect(db_path)
    
        # Buscar dados na tabela 'Demonstracoes_Financeiras' sem o sufixo '.SA'
        query_dados = f"SELECT * FROM Demonstracoes_Financeiras WHERE Ticker = '{ticker}' OR Ticker = '{ticker.replace('.SA', '')}'"
        df = pd.read_sql_query(query_dados, conn)
    
        return df
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return None
    finally:
        if conn:
            conn.close()

# Função que carrega o banco de dados dos MÚLTIPLOS __________________________________________________________________________________________________________________________________________________
@st.cache_data
def load_multiplos_from_db(ticker):
    db_path = download_db_from_github(db_url)
    
    if db_path is None or not os.path.exists(db_path):
        return None

    try:
        conn = sqlite3.connect(db_path)

        # Buscar todos os dados históricos da tabela 'multiplos' para o ticker
        query_multiplos = f"""
        SELECT * FROM multiplos
        WHERE Ticker = '{ticker}' OR Ticker = '{ticker.replace('.SA', '')}' 
        ORDER BY Data ASC
        """
        df_multiplos = pd.read_sql_query(query_multiplos, conn)
        return df_multiplos
    except Exception as e:
        st.error(f"Erro ao carregar a tabela 'multiplos': {e}")
        return None
    finally:
        if conn:
            conn.close()

# Função para carregar DADOS MACROECONÔMICOS históricos do banco de dados ____________________________________________________________________________________________________________________
@st.cache_data
def load_macro_summary():
    db_path = download_db_from_github(db_url)
    
    if db_path is None or not os.path.exists(db_path):
        return "Não há dados macroeconômicos disponíveis no banco de dados."

    try:
        conn = sqlite3.connect(db_path)

        # Buscar todos os dados da tabela 'macroeconomia'
        query_macro = "SELECT * FROM info_economica ORDER BY Data ASC"
        df_macro = pd.read_sql_query(query_macro, conn)
        return df_macro
        
    except Exception as e:
        return f"Erro ao carregar os dados macroeconômicos: {e}"
    finally:
        if conn:
            conn.close()

# Sidebar com ícones de navegação __________________________________________________________________________________________________________________________________________________________

with st.sidebar:
    #st.image("logo.png", width=150)
    st.markdown("# Análises")
    #st.markdown("## Básica")
    #st.markdown("## Avançada")
    #st.markdown("## Trading")
    
    # USE O RADIO PARA ESCOLHER ENTRE AS SEÇÕES:
    pagina = st.radio("Escolha a seção:", ["Básica", "Avançada", "Trading"])

if pagina == "Básica":

        # Adicionar o título ao cabeçalho
        st.markdown("""
            <h1 style='text-align: center; font-size: 36px; color: #333;'>Análise Básica de Ações</h1>
        """, unsafe_allow_html=True)
    
        # CSS para posicionar o botão de atualização no canto superior direito __________________________________________________________________________________________________________________________________________________________________________________________
        st.markdown("""
            <style>
            .button-container {
                display: flex;
                justify-content: flex-end;
                position: absolute;
                top: 10px;
                right: 10px;
                z-index: 1;
            }
            .button-container button {
                background-color: #4CAF50; /* Cor verde */
                color: white;
                padding: 10px 20px;
                border: none;
                border-radius: 4px;
                cursor: pointer;
            }
            .button-container button:hover {
                background-color: #45a049; /* Tom de verde mais escuro ao passar o mouse */
            }
            </style>
        """, unsafe_allow_html=True)
        
        # Adicionando o botão dentro de um container HTML no canto superior direito
        st.markdown("""
            <div class="button-container">
                <form action="#">
                    <button type="submit">Atualizar dados</button>
                </form>
            </div>
        """, unsafe_allow_html=True)
        
        # Verifica se o botão foi clicado e atualiza os dados
        if st.button('Atualizar dados'):
            st.cache_data.clear()  # Limpa o cache
            st.experimental_rerun()  # Recarrega a aplicação
        
             
        # Adicionar estilo CSS para criação dos blocos de descrição das empresas, com o logo à direita e as informações à esquerda, e altura fixa ___________________________________________________________________________________________________________________________________________________________________________________________
        st.markdown("""
            <style>
            .sector-box {
                border: 1px solid #ddd;
                padding: 15px;
                border-radius: 10px;
                margin-bottom: 10px;
                display: flex;
                justify-content: space-between;
                align-items: center;
                height: 140px;  /* Definindo uma altura fixa para os blocos */
                cursor: pointer;  /* Torna o quadrado clicável */
                transition: background-color 0.3s ease;  /* Animação de transição ao passar o mouse */
            }
            .sector-box:hover {
                background-color: #f0f0f0;  /* Muda a cor de fundo ao passar o mouse */
            }
            .sector-info {
                font-size: 14px;
                color: #333;
                text-align: left;
                flex: 1;  /* O texto ocupa a maior parte à esquerda */
                overflow: hidden;  /* Esconder o texto que ultrapassar a área */
                text-overflow: ellipsis;  /* Adicionar reticências caso o texto seja muito longo */
            }
            .sector-info strong {
                font-size: 16px;
                color: #000;
            }
            .sector-logo {
                width: 50px;
                height: auto;
                margin-left: 15px;  /* Adiciona espaço entre o texto e o logo */
            }
            </style>
        """, unsafe_allow_html=True)

        
        # Inserir campo para o usuário digitar o ticker _______________________________________________________________________________________________________________________________
        col1, col2 = st.columns([4, 1])
        with col1:
            # Se houver um ticker definido via clique ou input, usá-lo como valor no campo de busca
            if 'ticker' in st.session_state:
                ticker_input = st.text_input("DIGITE O TICKER:", value=st.session_state.ticker.split(".SA")[0], key="ticker_input").upper()
            else:
                ticker_input = st.text_input("Digite o ticker:", key="ticker_input").upper()
        
            # Verificar se o campo de busca está vazio e remover o ticker do session_state
            if ticker_input == "":
                if 'ticker' in st.session_state:
                    del st.session_state['ticker']  # Remove o ticker do estado
                ticker = None  # Garantir que o sistema retorne à lista de setores
            else:
                # Se houver input, atualizar o estado
                ticker = ticker_input + ".SA" if ticker_input else None
                if ticker_input:
                    st.session_state.ticker = ticker
        
        # Se nenhum ticker for inserido, exibir lista de tickers disponíveis por setor
        if not ticker:
            st.markdown("### Selecione um Ticker")
        
            if setores is not None and not setores.empty:
                # Agrupar tickers por setor
                setores_agrupados = setores.groupby('SETOR')
        
                for setor, dados_setor in setores_agrupados:
                    st.markdown(f"#### {setor}")
        
                    col1, col2, col3 = st.columns(3)
                    for i, row in dados_setor.iterrows():
                        logo_url = get_logo_url(row['ticker'])  # Obter a URL do logotipo da empresa
                        with [col1, col2, col3][i % 3]:
                            # Tornar o quadrado clicável para atualizar o campo de busca com o ticker
                            if st.button(f"{row['nome_empresa']}", key=row['ticker']):
                                st.session_state.ticker = row['ticker']  # Salva o ticker no estado
        
                            # Exibir o layout do quadrado
                            st.markdown(f"""
                            <div class='sector-box'>
                                <div class='sector-info'>
                                    <strong>{row['nome_empresa']}</strong><br>
                                    Ticker: {row['ticker']}<br>
                                    Subsetor: {row['SUBSETOR']}<br>
                                    Segmento: {row['SEGMENTO']}
                                </div>
                                <img src='{logo_url}' class='sector-logo' alt='Logo da empresa'>
                            </div>
                            """, unsafe_allow_html=True)
            else:
                st.warning("Nenhuma informação de setores encontrada.")
        else:
            # Se houver um ticker, continuar com a exibição normal das informações do ticker
            ticker = st.session_state.ticker
            indicadores = load_data_from_db(ticker)
            indicadores = indicadores.drop(columns=['Ticker'])

          # Inserindo espaçamento entre os elementos
        placeholder = st.empty()
        placeholder.markdown("<div style='height: 46px;'></div>", unsafe_allow_html=True)
           
        # -----------------------------------------------------------------------------
        # Função para calcular a taxa de crescimento via regressão logarítmica
        # -----------------------------------------------------------------------------
        def calculate_growth_rate(df, column):
            """
            Calcula a taxa de crescimento anual (percentual) de 'column'
            usando regressão linear no log dos valores ao longo do tempo.
            """
            try:
                # Garantir que existe a coluna 'Data'
                if 'Data' not in df.columns:
                    raise ValueError("A coluna 'Data' não foi encontrada no DataFrame.")
        
                # Converter datas
                df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
                if df['Data'].isnull().any():
                    raise ValueError("A coluna 'Data' contém valores inválidos que não puderam ser convertidos para data.")
        
                # Verificar se a coluna está completamente nula
                if df[column].isnull().all():
                    raise ValueError(f"A coluna '{column}' está vazia ou contém apenas valores nulos.")
                
                # Ordenar pela data
                df = df.sort_values(by='Data')
        
                # Filtrar somente valores válidos (não nulos e positivos, pois faremos log)
                mask_valid = df[column].notnull() & (df[column] > 0)
                df_valid = df.loc[mask_valid]
                
                # Verificar se temos pelo menos 2 pontos para a regressão
                if df_valid.shape[0] < 2:
                    raise ValueError(f"Dados insuficientes na coluna '{column}' para regressão logarítmica.")
                
                # Tempo (X) em anos, tomando como referência a primeira data válida
                X = (df_valid['Data'] - df_valid['Data'].iloc[0]).dt.days / 365.25
                if X.nunique() == 0:
                    raise ValueError("Não há variação de tempo suficiente para calcular a taxa de crescimento.")
                
                # Log dos valores (y)
                y_log = np.log(df_valid[column].values)
                
                # Ajuste da regressão linear: slope, intercept
                slope, intercept = np.polyfit(X, y_log, deg=1)
                
                # slope => taxa de crescimento contínua; convertemos para taxa nominal anual
                growth_rate = np.exp(slope) - 1
                
                return growth_rate
            
            except Exception as e:
                # Se algo der errado, retorna NaN
                # st.error(f"Erro ao calcular a taxa de crescimento para '{column}': {e}")
                return np.nan
        
        
        # -----------------------------------------------------------------------------
        # Calcular a taxa de crescimento para cada indicador
        # -----------------------------------------------------------------------------
        growth_rates = {}
        
        for column in indicadores.columns:
            if column != 'Data':  # Ignorar a coluna de datas
                col_data = indicadores[column]
        
                # Caso a coluna seja só nulos ou zeros, descartar
                if col_data.isnull().all() or (col_data.fillna(0) == 0).all():
                    growth_rates[column] = None
                else:
                    rate = calculate_growth_rate(indicadores, column)
                    growth_rates[column] = rate
     
        # Da algumas informações referentes a empresa no momento da escolha do ticker _____________________________________________________________________________________________________________________________________________________________________
        
        if ticker:
            # Mostrar o valor do ticker inserido
            st.write(f"Ticker inserido: {ticker}")
        
            def get_price(ticker):
               try:
                    # Usar yfinance para obter o preço da ação
                    stock = yf.Ticker(ticker)
                    stock_info = stock.history(period="1d")  # Obter dados do último dia
            
                    # Verificar se existe o preço de fechamento ('Close')
                    if not stock_info.empty:
                        current_price = stock_info['Close'].iloc[0]
                        return current_price
                    else:
                        return None
        
               except Exception as e:
                    st.error(f"Erro ao obter o preço da ação: {e}")
                    return None
                    
            # Buscar informações da empresa e verificar se existe
            company_name, company_website = get_company_info(ticker)
        
            # Obter o preço atual da ação
            current_price = get_price(ticker)
            
            if company_name:
                if current_price is not None:
                    st.subheader(f"{company_name} - Preço Atual: R$ {current_price:.2f}")
                else:
                    st.error("Não foi possível obter o preço da ação.")
                # Buscar o logotipo usando a URL do repositório
                logo_url = get_logo_url(ticker)
                
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
        
        # -----------------------------------------------------------------------------
        # Adicionar estilo CSS para os quadrados de exibição
        # -----------------------------------------------------------------------------
        st.markdown("""
            <style>
            .growth-box {
                border: 2px solid #ddd;
                padding: 20px;
                border-radius: 10px;
                margin-bottom: 10px;
                display: flex;
                justify-content: center;
                align-items: center;
                height: 100px;
                width: 100%;
                text-align: center;
                font-size: 20px;
                font-weight: bold;
                color: #333;
                background-color: #f9f9f9;
            }
            </style>
        """, unsafe_allow_html=True)

        
        def format_growth_rate(value):
            """Formata a taxa de crescimento em percentual, ou '-' caso inválida."""
            if isinstance(value, (int, float)) and not pd.isna(value) and not np.isinf(value):
                return f"{value:.2%}"
            else:
                return "-"
                 
                      
        # Exibir os valores da regressão linear em quadrados
        st.markdown("### Visão Geral (Taxa de Crescimento Médio Anual)")
        col1, col2, col3 = st.columns(3)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(
                f"<div class='growth-box'>Receita Líquida: {format_growth_rate(growth_rates['Receita_Liquida'])}</div>",
                unsafe_allow_html=True
            )
        with col2:
            st.markdown(
                f"<div class='growth-box'>Lucro Líquido: {format_growth_rate(growth_rates['Lucro_Liquido'])}</div>",
                unsafe_allow_html=True
            )
        with col3:
            st.markdown(
                f"<div class='growth-box'>Patrimônio Líquido: {format_growth_rate(growth_rates['Patrimonio_Liquido'])}</div>",
                unsafe_allow_html=True
            )
         
         # Inserindo espaçamento entre os elementos
        placeholder = st.empty()
        placeholder.markdown("<div style='height: 46px;'></div>", unsafe_allow_html=True)
    
        # Cria o gráfico em BARRA e o seletor para escolher quais variáveis mostrar das DFPs __________________________________________________________________________________________________________________________________________________
        
        # Seletor para escolher quais variáveis visualizar no gráfico
        st.markdown("### Selecione os Balanços para Visualizar no Gráfico")
        
        # Criar mapeamento de nomes de colunas para nomes amigáveis
        col_name_mapping = {col: col.replace('_', ' ').title() for col in indicadores.columns if col != 'Data'}
        # Ajustar manualmente os nomes para incluir os acentos corretos
        correcoes = {
            'Receita Liquida': 'Receita Líquida',
            'Lucro Liquido': 'Lucro Líquido',
            'Patrimonio Liquido': 'Patrimônio Líquido',
            'Caixa Liquido': 'Caixa Líquido',
            'Passivo Exigivel': 'Passivo Exigível',
            'Divida Liquida': 'Dívida Líquida'
        }
        
        # Atualizar o mapeamento com as correções
        col_name_mapping = {k: correcoes.get(v, v) for k, v in col_name_mapping.items()}
        
        
        display_name_to_col = {v: k for k, v in col_name_mapping.items()}
        
        # Lista de nomes amigáveis para exibição
        variaveis_disponiveis_display = list(col_name_mapping.values())
        
        # Nomes padrão (amigáveis) para seleção
        default_cols = ['Receita Líquida', 'Lucro Líquido', 'Dívida Líquida']  # Ajuste conforme necessário
        default_display = [nome for nome in variaveis_disponiveis_display if nome in default_cols]
        
        variaveis_selecionadas_display = st.multiselect(
            "Escolha os Indicadores:",
            variaveis_disponiveis_display,
            default=default_display
        )
        
        
        # Garantir que 'indicadores' está carregado corretamente
        if variaveis_selecionadas_display:
        
            # Converter nomes amigáveis selecionados para nomes originais
            variaveis_selecionadas = [display_name_to_col[nome] for nome in variaveis_selecionadas_display]
        
            # Função para verificar o tema do Streamlit
            def update_theme():
                theme_colors = {}
                if st.config.get_option('theme.base') == 'dark':
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
                return theme_colors
        
            # Função para exibir o gráfico de barras
            def plot_graph(df_melted):
                theme_colors = update_theme()  # Atualiza as cores com base no tema
        
                # Criar o gráfico de barras com cores adaptativas
                fig = px.bar(
                    df_melted,
                    x='Data',
                    y='Valor',
                    color='Indicador',
                    barmode='group',
                    title='Evolução dos Balanços Selecionados'
                )
        
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
        
                # Renderizar o gráfico no Streamlit
                st.plotly_chart(fig, use_container_width=True)
        
            # Criar o DataFrame "melted" para formatar os dados
            df_melted = indicadores.melt(
                id_vars=['Data'],
                value_vars=variaveis_selecionadas,
                var_name='Indicador',
                value_name='Valor'
            )
        
            # Mapear os nomes das colunas para os nomes amigáveis no DataFrame
            df_melted['Indicador'] = df_melted['Indicador'].map(col_name_mapping)
        
            # Chama a função para exibir o gráfico
            plot_graph(df_melted)
        
        else:
            st.warning("Por favor, selecione pelo menos um indicador para exibir no gráfico.")

         # Inserindo espaçamento entre os elementos
        placeholder = st.empty()
        placeholder.markdown("<div style='height: 46px;'></div>", unsafe_allow_html=True)
            
        
        # Exibir a tabela de indicadores no final ____________________________________________________________________________________________________________________________________________________
        #st.markdown("### Tabela de Indicadores")
        #st.dataframe(indicadores)  # Mostra a tabela interativa no dashboard
        
        # Função para carregar os dados da tabela "multiplos_TRI" do banco de dados  ________________________________________________________________________________________________________________________________________________
        @st.cache_data
        def load_multiplos_limitado_from_db(ticker):
            db_path = download_db_from_github(db_url)
            
            if db_path is None or not os.path.exists(db_path):
                return None
        
            try:
                conn = sqlite3.connect(db_path)
        
                # Buscar dados na tabela 'multiplos' para o ticker
                query_multiplos = f"""
                SELECT * FROM multiplos_TRI 
                WHERE Ticker = '{ticker}' OR Ticker = '{ticker.replace('.SA', '')}' 
                ORDER BY Data DESC LIMIT 1
                """
                df_multiplos = pd.read_sql_query(query_multiplos, conn)
                return df_multiplos
            except Exception as e:
                st.error(f"Erro ao carregar a tabela 'multiplos': {e}")
                return None
            finally:
                if conn:
                    conn.close()
        
        # Carregar dados da tabela 'multiplos'
        multiplos = load_multiplos_limitado_from_db(ticker)
        #st.markdown("### Tabela de Múltiplos")
        #st.dataframe(multiplos)  # Mostra a tabela interativa no dashboard

           
        # Adicionar estilo CSS para os quadrados que apresentarão os múltiplos _________________________________________________________________________________________________________________
         # Verificando colunas esperadas
        
        st.markdown("""
            <style>
            /* Estilo dos quadrados de métricas */
            .metric-box {
                background-color: white;
                padding: 20px;
                margin: 10px;
                border-radius: 10px;
                box-shadow: 2px 2px 5px rgba(0, 0, 0, 0.1);
                border: 1px solid #f0f0f0;
                text-align: center;
                width: 100%; /* Garante que o tamanho dos quadrados seja consistente */
            }
            
            /* Estilo para o valor das métricas */
            .metric-value {
                font-size: 24px;
                font-weight: bold;
            }
            
            /* Estilo para o rótulo das métricas */
            .metric-label {
                #font-size: 14px;
                #color: #6c757d;
                color: #FFA500; /* Cor alaranjada */
                font-weight: bold; /* Texto em negrito */
            }
        
            /* Ajustes para a responsividade */
            .stColumns > div {
                display: flex;
                align-items: center;
                justify-content: center;
            }
            
            </style>
        """, unsafe_allow_html=True)
        
        if multiplos is not None and not multiplos.empty:
              
            # Exibir múltiplos em "quadrados"
            st.markdown("### Indicadores Financeiros")
            
            col1, col2, col3, col4 = st.columns(4)
        
            # Coluna 1 - Margem Líquida
            with col1:
                margem_liquida = multiplos['Margem_Liquida'].fillna(0).values[0]
                st.markdown(f"""
                <div class='metric-box'>
                    <div class='metric-value'>{margem_liquida:.2f}%</div>
                    <div class='metric-label' title='Mede a eficiência da empresa em converter receita em lucro após todas as despesas (Lucro Líquido/Receita Líquida).'>Margem Líquida</div>
                  </div>
                </div>
                """, unsafe_allow_html=True)
        
            # Coluna 2 - Margem Operacional
            with col2:
                margem_Operacional = multiplos['Margem_Operacional'].fillna(0).values[0]
                st.markdown(f"""
                <div class='metric-box'>
                    <div class='metric-value'>{margem_Operacional:.2f}%</div>
                    <div class='metric-label' title='Mede a eficiência operacional da empresa antes das despesas financeiras e impostos (EBIT/Receita Líquida).'>Margem Operacional</div>
                  </div>
                </div>
                """, unsafe_allow_html=True)
        
            # Coluna 3 - ROE
            with col3:
                roe = multiplos['ROE'].fillna(0).values[0]
                st.markdown(f"""
                <div class='metric-box'>
                    <div class='metric-value'>{roe:.2f}%</div>
                    <div class='metric-label' title='ROE (Retorno sobre o Patrimônio): Indica a eficiência da empresa em gerar lucro com o capital dos acionistas (Lucro Líquido/Patrimônio Líquido).'>ROE</div>
                </div>
                """, unsafe_allow_html=True)
        
            # Coluna 4 - ROIC
            with col4:
                roic = multiplos['ROIC'].fillna(0).values[0]
                st.markdown(f"""
                <div class='metric-box'>
                    <div class='metric-value'>{roic:.2f}%</div>
                    <div class='metric-label' title='ROIC (Retorno sobre Capital Investido): Mede a eficiência da empresa em gerar retorno sobre o capital total investido (EBIT/(Ativo Total - Passivo Circulante).'>ROIC</div>
                </div>
                """, unsafe_allow_html=True)
        
             # Segunda linha de colunas
            col5, col6, col7, col8 = st.columns(4)
        
            # Coluna 5 - Dividend Yield
            with col5:
                dy_value = multiplos['DY'].fillna(0).values[0]
                if current_price == 0 or pd.isna(dy_value): # Verifica divisão por zero ou NaN
                    dividend_yield = "-"
                else:
                    dividend_yield = f"{(100 * (dy_value/current_price)):.2f}%"
                st.markdown(f"""
                <div class='metric-box'>
                    <div class='metric-value'>{dividend_yield}</div>
                    <div class='metric-label' title='Mede o retorno percentual dos dividendos pagos pela empresa em relação ao preço da ação (Dividendos/Preço da ação).'>Dividend Yield</div>
                </div>
                """, unsafe_allow_html=True)
        
            # Coluna 6 - P/VP
            with col6:
                pvp_value = multiplos['P/VP'].fillna(0).values[0]
                if pvp_value == 0 or pd.isna(pvp_value) or np.isinf(pvp_value):  # Verifica divisão por zero ou NaN
                    pvp = "-"
                else:
                    pvp = f"{(current_price/pvp_value):.2f}"
                st.markdown(f"""
                <div class='metric-box'>
                    <div class='metric-value'>{pvp}</div>
                    <div class='metric-label' title='P/VP (Preço sobre Valor Patrimonial): Avalia se a ação está sendo negociada acima ou abaixo do valor contábil da empresa.'>P/VP</div>
                </div>
                """, unsafe_allow_html=True)
        
            # Coluna 07 - Payout
            with col7:
                payout_value = multiplos['Payout'].fillna(0).values[0]
                if pd.isna(payout_value):  # Verifica NaN
                    payout = "-"
                else:
                    payout = f"{(payout_value * 100):.2f}%"
                st.markdown(f"""
                <div class='metric-box'>
                    <div class='metric-value'>{payout}</div>
                    <div class='metric-label' title='Indica a porcentagem do lucro líquido que é distribuída aos acionistas na forma de dividendos (Dividendos/Lucro Líquido).'>Payout</div>
                </div>
                """, unsafe_allow_html=True)
        
            # Coluna 08 - P/L
            with col8:
                pl_value = multiplos['P/L'].fillna(0).values[0]
                if pl_value == 0 or pd.isna(pl_value) or np.isinf(pl_value):  # Verifica divisão por zero ou NaN
                    pl = "-"
                else:
                    pl = f"{(current_price/pl_value):.2f}"
                st.markdown(f"""
                <div class='metric-box'>
                    <div class='metric-value'>{pl}</div>
                    <div class='metric-label' title='P/L (Preço sobre Lucro): Indica quantos anos levaria para o investidor recuperar seu investimento com os lucros da empresa.'>P/L</div>
                </div>
                """, unsafe_allow_html=True)
        
        
            # Terceira linha de colunas
            col9, col10, col11, col12 = st.columns(4)
            # Coluna 09 - Endividamento Total
            with col9:
                endividamento_total = multiplos['Endividamento_Total'].fillna(0).values[0]
                st.markdown(f"""
                <div class='metric-box'>
                    <div class='metric-value'>{endividamento_total:.2f}</div>
                     <div class='metric-label' title='Mede o nível de dívida da empresa em relação ao seus ativos (Passivo Total/Ativo Total).'>Endividamento Total</div>
                </div>
                """, unsafe_allow_html=True)
        
             # Coluna 10 - Alavancagem Financeira sobre o Patrimônio Líquido
            with col10:
                alavancagem_financeira = multiplos['Alavancagem_Financeira'].fillna(0).values[0]
                st.markdown(f"""
                <div class='metric-box'>
                    <div class='metric-value'>{alavancagem_financeira:.2f}</div>
                    <div class='metric-label' title='Mede o quanto a empresa utiliza de capital de terceiros em relação ao patrimônio líquido (Divida Líquida/Patrimônio Líquido).'>Alavancagem Financeira</div>
                </div>
                """, unsafe_allow_html=True)
        
             
            # Coluna 11: Líquidez Corrente
            with col11:
                Liquidez_Corrente = multiplos['Liquidez_Corrente'].fillna(0).values[0]
                st.markdown(f"""
                <div class='metric-box'>
                    <div class='metric-value'>{Liquidez_Corrente:.2f}</div>
                    <div class='metric-label' title='Mede a capacidade da empresa em honrar suas dívidas de curto prazo com seus ativos circulantes (Ativo Circulante/Passivo Circulante).'>Liquidez Corrente</div>
                </div>
                """, unsafe_allow_html=True)

        # Inserindo espaçamento entre os elementos
        placeholder = st.empty()
        placeholder.markdown("<div style='height: 46px;'></div>", unsafe_allow_html=True)
        
        
        # Cria o gráfico em BARRA e o seletor para escolher quais variáveis mostrar dos Múltiplos __________________________________________________________________________________________________________________________________________________
        
        # 1 - Chamar a tabela multiplos do banco de dados com todas as informações 

        # Carregar dados históricos
        multiplos = load_multiplos_from_db(ticker)
        
        # Converter 'Data' para datetime, se necessário
        multiplos['Data'] = pd.to_datetime(multiplos['Data'], errors='coerce')
    
   
        # 2 - Seletor para escolher quais variáveis visualizar no gráfico
        st.markdown("### Selecione os Indicadores para Visualizar no Gráfico")
        
        # Excluir a coluna 'Data' do mapeamento
        exclude_columns = ['Data', 'Ticker', 'N Acoes']
        
        # Mapeamentos personalizados (se necessário)
        custom_mappings = {
            'Margem_Liquida': 'Margem Líquida',
            'Margem_Operacional': 'Margem Operacional',
            'DY': 'Dividend Yield',
            'P_VP': 'P/VP',
            'P_L': 'P/L',
            # Adicione outros mapeamentos personalizados conforme necessário
        }
        
        # Criar mapeamentos de nomes de colunas
        def create_column_name_mappings(df, exclude_columns=None, custom_mappings=None):
            if exclude_columns is None:
                exclude_columns = []
            if custom_mappings is None:
                custom_mappings = {}
            col_name_mapping = {}
            for col in df.columns:
                if col not in exclude_columns:
                    # Usa o mapeamento personalizado se existir, caso contrário, formata o nome padrão
                    friendly_name = custom_mappings.get(col, col.replace('_', ' ').title())
                    col_name_mapping[col] = friendly_name
            display_name_to_col = {v: k for k, v in col_name_mapping.items()}
            display_names = list(col_name_mapping.values())
            return col_name_mapping, display_name_to_col, display_names
        
        col_name_mapping, display_name_to_col, variaveis_disponiveis_display = create_column_name_mappings(
            multiplos,
            exclude_columns=exclude_columns,
            custom_mappings=custom_mappings
        )
        
        # Nomes padrão (amigáveis) para seleção
        default_cols = ['Margem Líquida', 'Margem Operacional']  # Ajuste conforme necessário
        default_display = [nome for nome in variaveis_disponiveis_display if nome in default_cols]
        
        variaveis_selecionadas_display = st.multiselect(
            "Escolha os Indicadores:",
            variaveis_disponiveis_display,
            default=default_display,
            key='multiplos_multiselect'
        )
        
        # Garantir que 'multiplos' está carregado corretamente
        if variaveis_selecionadas_display:
        
            # Converter nomes amigáveis selecionados para nomes originais
            variaveis_selecionadas = [display_name_to_col[nome] for nome in variaveis_selecionadas_display]
        
            # Função para verificar o tema do Streamlit
            def update_theme():
                theme_colors = {}
                if st.config.get_option('theme.base') == 'dark':  # Verifica o tema configurado no Streamlit
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
                return theme_colors
        
            # Função para exibir o gráfico de barras
            def plot_graph(df_melted):
                theme_colors = update_theme()  # Atualiza as cores com base no tema
                
                # Criar o gráfico de barras com cores adaptativas
                fig = px.bar(
                    df_melted,
                    x='Data',
                    y='Valor',
                    color='Indicador',
                    barmode='group',  # Barras agrupadas por indicador
                    title='Evolução dos Indicadores Selecionados'
                )
                
                fig.update_layout(
                    xaxis_title='Ano',
                    yaxis_title='Valor',
                    plot_bgcolor=theme_colors['bg_color'],  # Aplicando cor de fundo
                    paper_bgcolor=theme_colors['bg_color'],  # Aplicando cor de fundo do papel
                    font=dict(color=theme_colors['text_color']),  # Aplicando cor da fonte
                    title_font=dict(color=theme_colors['text_color'], size=24),  # Cor do título
                    legend_title_text='Indicadores',
                    xaxis=dict(showgrid=True, gridcolor=theme_colors['grid_color']),  # Cor da grade do eixo X
                    yaxis=dict(showgrid=True, gridcolor=theme_colors['grid_color'])  # Cor da grade do eixo Y
                )
                
                # Renderizar o gráfico no Streamlit
                st.plotly_chart(fig, use_container_width=True)
        
            # Criar o DataFrame "melted" para formatar os dados
            df_melted = multiplos.melt(
                id_vars=['Data'],
                value_vars=variaveis_selecionadas,
                var_name='Indicador',
                value_name='Valor'
            )
        
            # Mapear os nomes das colunas para os nomes amigáveis no DataFrame
            df_melted['Indicador'] = df_melted['Indicador'].map(col_name_mapping)
        
            # Chama a função para exibir o gráfico
            plot_graph(df_melted)
        
        else:
            st.warning("Por favor, selecione pelo menos um indicador para exibir no gráfico.")

if pagina == "Avançada": #_______________________________________________________________# Análise Avançada #____________________________________________________________________________________________________________

    # ===============================================
    #                FUNÇÕES AUXILIARES
    # ===============================================

       
    # Função para remover outliers usando o método IQR __________________________________________________________________________________________________________________
    def remover_outliers_iqr(df, colunas):
        df_filtrado = df.copy()
        for col in colunas:
            if col in df.columns:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                limite_inferior = Q1 - 1.5 * IQR
                limite_superior = Q3 + 1.5 * IQR
                df_filtrado = df_filtrado[(df_filtrado[col] >= limite_inferior) & (df_filtrado[col] <= limite_superior)]
        return df_filtrado

    # Função que realiza a normalização dos dados (comparabilidade dos múltiplos, reduzindo distorções causadas por concentração de valores em um extremo)______________
    def z_score_normalize(series, melhor_alto=True):
        series = series.replace([np.inf, -np.inf], np.nan)
        valid = series.dropna()
        if valid.empty:
            return pd.Series([0.0] * len(series), index=series.index)
        mean_val = valid.mean()
        std_val = valid.std()
        if std_val == 0:
            return pd.Series([0.0] * len(series), index=series.index)
        normalized = (series - mean_val) / std_val
        return normalized.fillna(0.0) if melhor_alto else -normalized.fillna(0.0)
    
    
    def slope_regressao_log(df, col): # Finalidade de encontrar a taxa de crescimento de variáveis (mais robusto que o CAGR) ______________________________________________________________
        """
        Faz regressão linear de ln(col) vs Ano, retornando o slope (beta).
        Filtra valores <= 0, pois ln(<=0) não é definido.
        Retorna 0.0 se não houver dados suficientes.
        """
        df_valid = df.dropna(subset=['Ano', col]).copy()
        # Apenas valores positivos
        df_valid = df_valid[df_valid[col] > 0]
        if len(df_valid) < 2:
            return 0.0
        
        # ln(col)
        df_valid['ln_col'] = np.log(df_valid[col])
        
        X = df_valid[['Ano']].values
        y = df_valid['ln_col'].values
        model = LinearRegression()
        model.fit(X, y)
        slope = model.coef_[0]
        return slope
    
    def slope_to_growth_percent(slope): # transforma o valor absoluto do valor encontrado na regressão para porcentagem ____________________________________________________________________
        """
        Converte slope da regressão log em taxa de crescimento aproximada (%).
        Ex.: se slope=0.07, growth ~ e^0.07 - 1 ~ 7.25%
        """
        return np.exp(slope) - 1


    def calcular_media_e_std(df, col):
        """
        Retorna a média e o desvio padrão da coluna `col` do DataFrame `df`.
        Remove valores nulos e infinitos antes do cálculo e exibe informações
        de depuração via Streamlit.
        """   
        
        # 1️⃣ Verificando se a coluna existe
        if col not in df.columns:
            st.error(f"⚠️ A coluna `{col}` não existe no DataFrame!")
            return (0.0, 0.0)
    
        # 5️⃣ Removendo valores NaN
        df_valid = df.dropna(subset=[col])
      
        # 6️⃣ Convertendo a coluna para numérico, tratando erros
        df_valid[col] = pd.to_numeric(df_valid[col], errors='coerce')
    
        # 7️⃣ Verificando quantos valores se tornaram NaN após conversão
        nan_count = df_valid[col].isna().sum()
    
        # 8️⃣ Removendo valores NaN novamente
        df_valid = df_valid.dropna(subset=[col])
    
        # 9️⃣ Removendo valores infinitos
        df_valid = df_valid[np.isfinite(df_valid[col])]
    
        # 🔟 Caso o DataFrame fique vazio após os tratamentos
        if df_valid.empty:
            return (0.0, 0.0)
    
        # 🔥 11️⃣ Calcular e exibir estatísticas finais
        media = df_valid[col].mean()
        std = df_valid[col].std()
          
        return (media, std)

      
    def winsorize(series, lower_quantile=0.05, upper_quantile=0.95): # Retira valores que distoam muito dos valores médios e podem comprometer os cálculos causando distorções ____________
        """
        Trunca outliers abaixo do 5º percentil e acima do 95º percentil.
        """
        s = series.dropna()
        if s.empty:
            return series
        l_val = s.quantile(lower_quantile)
        u_val = s.quantile(upper_quantile)
        return series.clip(l_val, u_val)
     
    # ===============================================
    # FUNÇÃO PRINCIPAL: Calcular Métricas Históricas
    # ===============================================
    def calcular_metricas_historicas_simplificadas(df_mult, df_dre):
        """
        Calcula métricas essenciais para um conjunto pequeno de variáveis.
        - Múltiplos: Margem_Liquida, Margem_Operacional, ROE, ROIC, P/VP, Endividamento_Total, Alavancagem_Financeira, Liquidez_Corrente
        - DRE: Receita Líquida, Lucro Líquido, Patrimônio Líquido, Dívida Líquida, Caixa Líquido (com slope log)
        
        Retorna um dicionário que representa a 'linha' de métricas da empresa.
        """
        # Converter Data -> Ano
        df_mult['Ano'] = pd.to_datetime(df_mult['Data'], errors='coerce').dt.year
        df_dre['Ano']  = pd.to_datetime(df_dre['Data'], errors='coerce').dt.year
        
        # Ordenar por Ano
        df_mult.sort_values('Ano', inplace=True)
        df_dre.sort_values('Ano', inplace=True)
        
        # Dicionário final
        metrics = {}
        
        # =============== MÚLTIPLOS ===============
        for col in ['Margem_Liquida', 'Margem_Operacional', 'ROE', 'ROIC', 'P/VP', 'Endividamento_Total', 'Alavancagem_Financeira', 'Liquidez_Corrente']:
            mean, std = calcular_media_e_std(df_mult, col)
            metrics[f'{col}_mean'] = mean
            metrics[f'{col}_std'] = std
        
        # =============== DEMONSTRAÇÕES ===============
        for col in ['Receita_Liquida', 'Lucro_Liquido', 'Patrimonio_Liquido', 'Divida_Liquida', 'Caixa_Liquido']:
            slope = slope_regressao_log(df_dre, col)
            metrics[f'{col}_slope_log'] = slope
            metrics[f'{col}_growth_approx'] = slope_to_growth_percent(slope)
        
        # Penalização por alta volatilidade (desvio padrão relativo à média)
        for col in ['Margem_Liquida', 'ROE', 'ROIC', 'Endividamento_Total', 'Liquidez_Corrente']:
            if metrics[f'{col}_mean'] != 0:
                coef_var = metrics[f'{col}_std'] / abs(metrics[f'{col}_mean'])
                metrics[f'{col}_volatility_penalty'] = min(1.0, coef_var)  # Penalização limitada a 100%
            else:
                metrics[f'{col}_volatility_penalty'] = 1.0  # Penalização máxima se a média for zero
        
         # 📌 NOVA Penalização por Histórico Longo → Agora mais severa
        num_anos = df_dre['Ano'].nunique()
    
        def calcular_historico_bonus(anos):
            """ Penaliza empresas novas mais severamente """
            return anos / (10 + anos)  # Ajustável, pode ser 15+ se quiser penalizar ainda mais
    
        # Aplicando penalização aprimorada
        metrics['historico_bonus'] = calcular_historico_bonus(num_anos)
        
        return metrics
       
    # espaçamento entre os elementos
    st.markdown("""
        <h1 style='text-align: center; font-size: 36px; color: #333;'>Análise Avançada de Ações</h1>
    """, unsafe_allow_html=True)

    # ===============================================
    #         CARREGAR E FILTRAR OS DADOS
    # ===============================================

    # Passo 1: Selecionar o Setor _________________________________________________________________________________________________________________________________________________________
    setores_unicos = setores['SETOR'].dropna().unique()
    setor_selecionado = st.selectbox("Selecione o Setor:", sorted(setores_unicos))
    
    if setor_selecionado:
        # Filtrar subsetores do setor selecionado __________________________________________________________________________________________________________________________________________
        subsetores_filtrados = setores[setores['SETOR'] == setor_selecionado]['SUBSETOR'].dropna().unique()
        subsetor_selecionado = st.selectbox("Selecione o Subsetor:", sorted(subsetores_filtrados))
    
        if subsetor_selecionado:
            # Filtrar segmentos do subsetor selecionado ____________________________________________________________________________________________________________________________________
            segmentos_filtrados = setores[(setores['SETOR'] == setor_selecionado) & (setores['SUBSETOR'] == subsetor_selecionado)]['SEGMENTO'].dropna().unique()
            segmento_selecionado = st.selectbox("Selecione o Segmento:", sorted(segmentos_filtrados))
    
            if segmento_selecionado:
                # Filtrar as empresas do (Setor, Subsetor, Segmento) escolhido
                empresas_filtradas = setores[
                    (setores['SETOR'] == setor_selecionado) &
                    (setores['SUBSETOR'] == subsetor_selecionado) &
                    (setores['SEGMENTO'] == segmento_selecionado)
                ]
           
                if empresas_filtradas.empty:
                    st.warning("Não há empresas nesse segmento.")

                st.markdown(f"### Empresas no Segmento {segmento_selecionado}")
                st.markdown("---")

                # Lista p/ armazenar dados agregados
                resultados = []
                                              
                for i, row in empresas_filtradas.iterrows():
                    ticker = row['ticker']
                    nome_emp = row['nome_empresa']

                     # Carregar histórico das tabelas ________________________________________________________________________________________________
                    multiplos = load_multiplos_from_db(ticker + ".SA")
                    df_dre    = load_data_from_db(ticker + ".SA")
                
                    if multiplos is None or multiplos.empty:
                        continue
                    if df_dre is None or df_dre.empty:
                        continue

                    # **Remover outliers antes de calcular métricas** __________________________________________________________________________________
                    colunas_para_filtrar = ['Receita_Liquida', 'Lucro_Liquido', 'EBIT', 'ROE', 'ROIC', 'Margem_Liquida', 
                                            'Divida_Total', 'Passivo_Circulante', 'Liquidez_Corrente', 
                                            'Crescimento_Receita', 'Crescimento_Lucro']

                    multiplos_corrigido = remover_outliers_iqr(multiplos, colunas_para_filtrar)
                    df_dre_corrigido = remover_outliers_iqr(df_dre, colunas_para_filtrar)
              
                    # Calcular métricas simplificadas ______________________________________________________________________________________________________
                    metrics_dict = calcular_metricas_historicas_simplificadas(multiplos_corrigido, df_dre_corrigido)
                                                          
                    data_emp = {
                        'ticker': ticker,
                        'nome_empresa': nome_emp,
                        'Setor': row['SETOR'],
                        'Subsetor': row['SUBSETOR'],
                        'Segmento': row['SEGMENTO']
                    }
                    data_emp.update(metrics_dict)
                    resultados.append(data_emp)   
                            
                if not resultados:
                    st.info("Não há dados para as empresas deste segmento.")
                                   
                df_empresas = pd.DataFrame(resultados)  # Coloca as informações agrupadas no dataframe df_empresas
                                                         
                # Carregar dados macroeconômicos do banco de dados
                dados_macro = load_macro_summary()
                 
                # ================================================
                #  DEFINIÇÃO DE INDICADORES E PESOS PARA SCORE
                # ================================================___________________________________________________________________________________________________________________________
                # Definir indicadores para score
                indicadores_score_ajustados = {
                    'Margem_Liquida_mean': {'peso': 0.15, 'melhor_alto': True},
                    'Margem_Operacional_mean': {'peso': 0.20, 'melhor_alto': True},
                    'ROE_mean': {'peso': 0.20, 'melhor_alto': True},
                    'ROIC_mean': {'peso': 0.20, 'melhor_alto': True},
                    'P/VP_mean': {'peso': 0.10, 'melhor_alto': False},
                    'Endividamento_Total_mean': {'peso': 0.15, 'melhor_alto': False},
                    'Alavancagem_Financeira_mean': {'peso': 0.15, 'melhor_alto': False},
                    'Liquidez_Corrente_mean': {'peso': 0.15, 'melhor_alto': True},
                    'Receita_Liquida_slope_log': {'peso': 0.15, 'melhor_alto': True},
                    'Lucro_Liquido_slope_log': {'peso': 0.20, 'melhor_alto': True},
                    'Patrimonio_Liquido_slope_log': {'peso': 0.15, 'melhor_alto': True},
                    'Divida_Liquida_slope_log': {'peso': 0.15, 'melhor_alto': False},
                    'Caixa_Liquido_slope_log': {'peso': 0.15, 'melhor_alto': True},
                }
               
                def calcular_score(df_empresas, indicadores_score_ajustados):
                    if df_empresas.empty:
                        st.warning("O DataFrame está vazio. Não há dados para calcular o score.")
                        return df_empresas
                
                    # Inicializar Score_Ajustado
                    df_empresas['Score_Ajustado'] = 0.0
                
                    for col, config in indicadores_score_ajustados.items():
                        if col not in df_empresas.columns:
                            st.warning(f"A coluna '{col}' não existe em df_empresas e será ignorada.")
                            df_empresas[col] = 0.0  # Criar coluna com valor 0
                            df_empresas[col + '_norm'] = 0.0  # Criar versão normalizada
                            continue
                
                        # Aplicar Winsorize para suavizar outliers
                        df_empresas[col] = winsorize(df_empresas[col])
                
                        # 📌 Aplicando Penalização por Volatilidade
                        volatility_col = col.replace("_mean", "_volatility_penalty")
                        if volatility_col in df_empresas.columns:
                            df_empresas[col] *= (1 - df_empresas[volatility_col])  # Penaliza o indicador pela volatilidade
                
                        # 📌 Aplicando Penalização por Tempo de Mercado
                        if 'historico_bonus' in df_empresas.columns:
                            df_empresas[col] *= df_empresas['historico_bonus']  # Penaliza o indicador pelo tempo de mercado
                
                        # Criar coluna normalizada
                        df_empresas[col + '_norm'] = z_score_normalize(df_empresas[col], config['melhor_alto'])
                
                        # Se a normalização falhar, criar a coluna `_norm`
                        if col + '_norm' not in df_empresas.columns:
                            st.error(f"Erro ao criar '{col}_norm'. Criando com valor padrão.")
                            df_empresas[col + '_norm'] = 0.0
                
                        # Somar ao Score Ajustado
                        df_empresas['Score_Ajustado'] += df_empresas[col + '_norm'] * config['peso']
                
                    # Criar ranking dentro do segmento
                    df_empresas['Rank_Ajustado'] = df_empresas['Score_Ajustado'].rank(method='dense', ascending=False)
                
                    return df_empresas

                    
                df_empresas = calcular_score(df_empresas, indicadores_score_ajustados) # CÁLCULO DO SCRORE DAS EMPRESAS

                # Ordenar resultado pelo Score Ajustado
                df_empresas.sort_values(['Segmento', 'Score_Ajustado'], ascending=[True, False], inplace=True)
                
                # Exibir Ranking de Empresas em quadrados estilizados lado a lado
                st.markdown("### Ranking de Empresas (Score Ajustado)")
                
                colunas_layout = st.columns(3)
                for idx, row in enumerate(df_empresas.itertuples()):
                    col = colunas_layout[idx % len(colunas_layout)]
                    with col:
                        logo_url = get_logo_url(row.ticker)
                        st.markdown(
                            f"""
                            <div style="
                                border: 2px solid #ddd;
                                border-radius: 10px;
                                padding: 15px;
                                margin: 10px;
                                background-color: #f9f9f9;
                                box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
                                text-align: center;
                            ">
                                <img src="{logo_url}" style="width: 50px; height: 50px; margin-bottom: 10px;">
                                <h4 style="color: #333;">{row.nome_empresa} ({row.ticker})</h4>
                                <p style="font-size: 18px; color: green; font-weight: bold;">Score: {row.Score_Ajustado:.2f}</p>
                                <p style="font-size: 16px;">Rank: {int(row.Rank_Ajustado)}</p>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )
                 
                
                # (Opcional) exibir df_empresas em modo tabela
                #st.dataframe(df_empresas)

                
                # Esse score inicial considera poucas variáveis (Margem, ROE, P/L, etc.) 
                # e a tendência de crescimento (slope log) de Receita e Lucro. 
                # Caso deseje adicionar mais variáveis (ex.: Patrimônio, Caixa, etc.), 
                # basta inserir nos dicionários e na função de cálculo.
            
                         
                 # Inserindo espaçamento entre os elementos
                st.markdown("---") # Espaçamento entre diferentes tipos de análise
                st.markdown("<div style='margin: 30px;'></div>", unsafe_allow_html=True)

                st.markdown("### Comparação de Indicadores (Múltiplos) entre Empresas do Segmento") #______Gráfico dos Múltiplos_____________________________________________________________________________________________
                
                # Lista de indicadores disponíveis
                indicadores_disponiveis = ["Margem Líquida", "Margem Operacional", "ROE", "ROIC", "P/L", "Liquidez Corrente", "Alavancagem Financeira", "Endividamento Total"]
                
                # Mapeamento de nomes amigáveis para nomes de colunas no banco
                nomes_to_col = {
                    "Margem Líquida": "Margem_Liquida",
                    "Margem Operacional": "Margem_Operacional",
                    "ROE": "ROE",
                    "ROIC": "ROIC",
                    "P/L": "P/L",
                    "Liquidez Corrente": "Liquidez_Corrente",
                    "Alavancagem Financeira": "Alavancagem_Financeira",
                    "Endividamento Total": "Endividamento_Total"
                    
                }

                 # Selecionar as empresas a exibir
                lista_empresas = empresas_filtradas['nome_empresa'].tolist()
                empresas_selecionadas = st.multiselect("Selecione as empresas a serem exibidas no gráfico:", lista_empresas, default=lista_empresas)
                
                # Selecionar o indicador a ser exibido
                indicador_selecionado = st.selectbox("Selecione o Indicador para Comparar:", indicadores_disponiveis, index=0)
                col_indicador = nomes_to_col[indicador_selecionado]
                
                                  
                # Opção para normalizar os dados
                normalizar = st.checkbox("Normalizar os Indicadores (Escala de 0 a 1)", value=False)
                
                # Construir o DataFrame com o histórico completo de cada empresa selecionada
                df_historico = []
                for i, row in empresas_filtradas.iterrows():
                    nome_emp = row['nome_empresa']
                    if nome_emp in empresas_selecionadas:
                        ticker = row['ticker']
                        multiplos_data = load_multiplos_from_db(ticker + ".SA")
                        if multiplos_data is not None and not multiplos_data.empty and col_indicador in multiplos_data.columns:
                            # Processar os dados da empresa
                            df_emp = multiplos_data[['Data', col_indicador]].copy()
                            df_emp['Ano'] = pd.to_datetime(df_emp['Data'], errors='coerce').dt.year  # Extrair apenas o ano
                            df_emp['Empresa'] = nome_emp
                            df_historico.append(df_emp)
                        else:
                            st.info(f"Empresa {nome_emp} não possui dados para o indicador {indicador_selecionado}.")
                
                if len(df_historico) == 0:
                    st.warning("Não há dados históricos disponíveis para as empresas selecionadas ou para o indicador escolhido.")
                else:
                    # Concatenar os DataFrames em um único DataFrame
                    df_historico = pd.concat(df_historico, ignore_index=True)
                
                    # Remover entradas com anos nulos
                    df_historico = df_historico.dropna(subset=['Ano'])
                
                    # Normalizar os dados se a opção estiver marcada
                    if normalizar:
                        max_valor = df_historico[col_indicador].max()
                        min_valor = df_historico[col_indicador].min()
                        df_historico[col_indicador] = (df_historico[col_indicador] - min_valor) / (max_valor - min_valor)
                
                    # Garantir que todos os anos presentes no conjunto de dados sejam exibidos no gráfico
                    anos_disponiveis = sorted(df_historico['Ano'].unique())
                    df_historico['Ano'] = df_historico['Ano'].astype(str)  # Converter para string para lidar com gaps no eixo
                
                    # Criar o gráfico de barras
                    fig = px.bar(
                        df_historico,
                        x='Ano',
                        y=col_indicador,
                        color='Empresa',
                        barmode='group',
                        title=f"Evolução Histórica de {indicador_selecionado} por Empresa"
                    )
                
                    # Ajustar layout do gráfico
                    fig.update_layout(
                        xaxis_title="Ano",
                        yaxis_title=f"{indicador_selecionado} {'(Normalizado)' if normalizar else ''}",
                        xaxis=dict(type='category', categoryorder='category ascending', tickvals=anos_disponiveis),
                        legend_title="Empresa"
                    )
                
                    # Exibir o gráfico no Streamlit
                    st.plotly_chart(fig, use_container_width=True)
                
                    st.markdown("---") # Espaçamento entre diferentes tipos de análise
                    st.markdown("<div style='margin: 30px;'></div>", unsafe_allow_html=True)
                
                # Seção: Gráfico Comparativo de Demonstrações Financeiras ________________Gráfico das Demonstrações Financeiras__________________________________________________________________
                # Título da seção
                st.markdown("### Comparação de Demonstrações Financeiras entre Empresas")
                
                # Função para carregar dados de demonstrações financeiras de todas as empresas selecionadas
                def load_dre_comparativo(empresas, indicadores_dre):
                    df_comparativo = []
                    for _, row in empresas.iterrows():
                        nome_emp = row['nome_empresa']
                        ticker = row['ticker']
                
                        # Carregar dados da tabela demonstracoes_financeiras
                        dre_data = load_data_from_db(ticker + ".SA")  # Função para carregar os dados
                        if dre_data is not None and not dre_data.empty:
                            dre_data['Empresa'] = nome_emp
                            dre_data['Ano'] = pd.to_datetime(dre_data['Data'], errors='coerce').dt.year  # Extrair apenas o ano
                            df_comparativo.append(dre_data)
                
                    if df_comparativo:
                        return pd.concat(df_comparativo, ignore_index=True)
                    return None
                
                # Carregar os dados para as empresas selecionadas
                dre_data_comparativo = load_dre_comparativo(
                    empresas_filtradas[empresas_filtradas['nome_empresa'].isin(empresas_selecionadas)],
                    indicadores_dre=["Receita_Liquida", "EBIT", "Lucro_Liquido", "Patrimonio_Liquido", "Divida_Liquida", "Caixa_Liquido"]
                )
                
                if dre_data_comparativo is not None:
                    # Criar mapeamento de nomes de colunas para nomes amigáveis
                    col_name_mapping = {
                        "Receita_Liquida": "Receita Líquida",
                        "EBIT": "EBIT",
                        "Lucro_Liquido": "Lucro Líquido",
                        "Patrimonio_Liquido": "Patrimônio Líquido",
                        "Divida_Liquida": "Dívida Líquida",
                        "Caixa_Liquido": "Caixa Líquido",
                  
                    }
                    display_name_to_col = {v: k for k, v in col_name_mapping.items()}
                    variaveis_disponiveis_display = list(col_name_mapping.values())
                
                    # Selecionar um único indicador para visualizar
                    indicador_selecionado_display = st.selectbox(
                        "Escolha o Indicador:",
                        variaveis_disponiveis_display,
                        index=0
                    )
                
                    # Converter o nome amigável selecionado para o nome original
                    indicador_selecionado = display_name_to_col[indicador_selecionado_display]
                
                    # Filtrar os dados apenas para o indicador selecionado
                    df_filtrado = dre_data_comparativo[['Ano', indicador_selecionado, 'Empresa']].copy()
                    df_filtrado = df_filtrado.rename(columns={indicador_selecionado: "Valor"})  # Renomear para padronização
                
                    # Garantir que todos os anos estejam presentes no eixo X
                    anos_disponiveis = sorted(df_filtrado['Ano'].unique())
                    df_filtrado['Ano'] = df_filtrado['Ano'].astype(str)  # Converter para string para lidar com gaps no eixo
                
                    # Criar o gráfico de barras agrupadas
                    fig = px.bar(
                        df_filtrado,
                        x="Ano",
                        y="Valor",
                        color="Empresa",
                        barmode="group",
                        title=f"Comparação de {indicador_selecionado_display} entre Empresas"
                    )
                
                    # Ajustar layout do gráfico
                    fig.update_layout(
                        xaxis_title="Ano",
                        yaxis_title=indicador_selecionado_display,
                        legend_title="Empresa",
                        xaxis=dict(type='category', categoryorder='category ascending', tickvals=anos_disponiveis)
                    )
                
                    # Exibir o gráfico no Streamlit
                    st.plotly_chart(fig, use_container_width=True)

                
                else:
                    st.warning("Não há dados disponíveis para as empresas selecionadas nas Demonstrações Financeiras.")

              
               # Resumo da melhor empresa em comparação com a média do mercado ============================================================================================================
                        
                def gerar_resumo_melhor_empresa(df_empresas):
                    """
                    Gera um resumo da melhor empresa ranqueada em relação à média do mercado.
                    """
                
                    if df_empresas.empty:
                        st.warning("O DataFrame de empresas está vazio. Não há dados para gerar o resumo.")
                        return
                
                    # Identificar a melhor empresa (Rank_Ajustado == 1)
                    melhor_empresa = df_empresas[df_empresas["Rank_Ajustado"] == 1]
                
                    if melhor_empresa.empty:
                        st.warning("Nenhuma empresa está ranqueada como a melhor. Verifique os dados.")
                        return
                
                    melhor_empresa = melhor_empresa.iloc[0]  # Pegamos a primeira entrada (caso haja empates)
                
                    # Calcular a média do mercado para comparação
                    colunas_metricas = [
                        "Margem_Liquida_mean", "ROE_mean", "ROIC_mean", 
                        "P/VP_mean", "Endividamento_Total_mean", "Liquidez_Corrente_mean",
                        "Receita_Liquida_slope_log", "Lucro_Liquido_slope_log"
                    ]
                    
                    df_mercado = df_empresas[colunas_metricas].mean()
                
                    # Título
                    st.subheader(f"📊 Resumo de Desempenho: {melhor_empresa['nome_empresa']} ({melhor_empresa['ticker']})")
                
                    # Criar DataFrame para exibir no Streamlit
                    dados = []
                    for col in colunas_metricas:
                        valor_empresa = melhor_empresa[col]
                        media_mercado = df_mercado[col]
                        diff = (valor_empresa - media_mercado) / media_mercado * 100 if media_mercado != 0 else 0
                        cor_valor = "🔴" if diff < 0 else "🟢"  # Emoticons para diferenciar resultados negativos e positivos
                        titulo = col.replace("_mean", "").replace("_slope_log", "").replace("_", " ")
                
                        dados.append([titulo, f"{valor_empresa:.2f}", f"{media_mercado:.2f}", f"{diff:.1f}% {cor_valor}"])
                
                    # Criando DataFrame formatado para exibição
                    df_resultado = pd.DataFrame(dados, columns=["Indicador", "Empresa", "Mercado", "Diferença"])
                
                    # Exibir tabela formatada no Streamlit
                    st.write(df_resultado.style.set_table_styles(
                        [{'selector': 'th', 'props': [('background-color', '#f4f4f4'), ('text-align', 'left')]}]
                    ))

                                                
                    # Criando um gráfico comparativo =========================================================================================================================================
                    df_comparacao = pd.DataFrame({
                        "Indicador": colunas_metricas,
                        "Melhor Empresa": [melhor_empresa[col] for col in colunas_metricas],
                        "Média do Mercado": [df_mercado[col] for col in colunas_metricas]
                    })
                
                    fig = px.bar(
                        df_comparacao.melt(id_vars="Indicador", var_name="Categoria", value_name="Valor"),
                        x="Indicador",
                        y="Valor",
                        color="Categoria",
                        barmode="group",
                        title=f"📊 Comparação: {melhor_empresa['nome_empresa']} vs. Média do Mercado"
                    )
                
                    st.plotly_chart(fig, use_container_width=True)          

                gerar_resumo_melhor_empresa(df_empresas)

               # ========================== CRIAÇÃO DO BENCHMARK (LÍDER X CONCORRENTES) =========================================================================================

                # 📌 Função para formatar valores em Reais (R$)
                def formatar_real(valor):
                    formatted = f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                    return "R$ " + formatted
                
                
                # 📌 Função para simular aportes mensais e calcular a evolução do patrimônio
                def calcular_patrimonio_com_aportes(precos, investimento_inicial=1000, aporte_mensal=1000):
                    """
                    Simula aportes mensais e calcula a evolução do patrimônio ao longo do tempo.
                
                    Retorna:
                    - `df_patrimonio`: DataFrame com o patrimônio final de cada empresa.
                    - `df_patrimonio_evolucao`: DataFrame com a evolução do patrimônio ao longo do tempo.
                    """
                
                    patrimonio_final = {}
                    patrimonio_evolucao = pd.DataFrame(index=precos.index)  # Criando DataFrame para evolução
                
                    for ticker in precos.columns:
                        df_precos = precos[[ticker]].dropna()
                                              
                        if df_precos.empty or len(df_precos) < 12:  # Filtra empresas com histórico insuficiente
                            print(f"⚠️ Empresa {ticker} removida da análise (dados insuficientes).")
                            continue
                        
                        df_precos['Mes'] = df_precos.index.to_period('M')  # Agrupa por mês
                        df_mensal = df_precos.groupby('Mes').first()  # Pegando o primeiro preço de cada mês
                        
                        total_acoes = 0
                        total_investido = 0
                        patrimonio = []
                
                        for data, preco in df_mensal.iterrows():
                            if np.isnan(preco[ticker]):  # Se não houver dado, pula o mês
                                continue
                            
                            # Primeiro aporte
                            if total_investido == 0:
                                total_acoes += investimento_inicial / preco[ticker]
                                total_investido += investimento_inicial
                            else:
                                total_acoes += aporte_mensal / preco[ticker]
                                total_investido += aporte_mensal
                            
                            patrimonio.append(total_acoes * preco[ticker])
                
                            # ✅ Atualizando o DataFrame de evolução do patrimônio
                            patrimonio_evolucao.loc[data.start_time, ticker] = total_acoes * preco[ticker]
                
                        ultimo_preco = df_precos[ticker].dropna().iloc[-1] if not df_precos[ticker].dropna().empty else None
                
                        if ultimo_preco is not None:
                            patrimonio_final[ticker] = total_acoes * ultimo_preco
                
                    return (
                        pd.DataFrame.from_dict(patrimonio_final, orient='index', columns=['Patrimonio Final']),
                        patrimonio_evolucao.ffill()  # Preenche valores NaN para manter a evolução contínua
                    )
                    
                def calcular_patrimonio_selic_macro(dados_macro, investimento_inicial=1000, aporte_mensal=1000):
                    """
                    Calcula a evolução do patrimônio investido no Tesouro Selic, considerando aportes mensais e a taxa histórica da Selic.
                    
                    - `dados_macro`: DataFrame contendo a taxa Selic ao longo do tempo.
                    - `investimento_inicial`: Valor inicial investido (padrão: R$1.000).
                    - `aporte_mensal`: Valor a ser investido a cada mês (padrão: R$1.000).
                
                    Retorna um DataFrame com a evolução do patrimônio no Tesouro Selic.
                    """
                
                    # Verificar se `dados_macro` está carregado corretamente
                    if dados_macro is None or dados_macro.empty:
                        raise ValueError("O DataFrame `dados_macro` está vazio ou não foi carregado corretamente.")
                
                    # Garantir que o índice de `dados_macro` esteja no formato datetime
                    if not isinstance(dados_macro.index, pd.DatetimeIndex):
                        try:
                           # 🔹 Se o índice numérico foi criado erroneamente, redefina-o
                            dados_macro = dados_macro.reset_index(drop=True)
                            
                            # 🔹 Agora, garanta que a coluna de datas está nomeada corretamente
                            if 'Data' in dados_macro.columns:
                                # Converter a coluna de Data para datetime
                                dados_macro["Data"] = pd.to_datetime(dados_macro["Data"], errors="coerce")
                            
                                # Definir a coluna de Data como índice correto
                                dados_macro.set_index("Data", inplace=True)
                            else:
                                print("⚠️ A coluna 'Data' não foi encontrada no DataFrame.")
                                                  
                        except Exception as e:
                            raise ValueError(f"Erro ao converter índice de `dados_macro` para datetime: {e}")
                
                    # Remover possíveis linhas com índice inválido
                    dados_macro = dados_macro.dropna(subset=["Selic"])  # Remove linhas onde a Selic está vazia
                
                    # Criar DataFrame para evolução do patrimônio
                    patrimonio_selic = pd.DataFrame(index=pd.date_range(start=dados_macro.index.min(), 
                                                                         end=dados_macro.index.max(), 
                                                                         freq="M"))
                                  
                    patrimonio_selic["Tesouro Selic"] = 0  # Inicializa a coluna
                
                    total_aplicado = investimento_inicial
                    saldo = investimento_inicial  # Saldo inicial
                
                    for data in patrimonio_selic.index:
                        # Selecionar a taxa Selic correspondente ao ano
                        ano_referencia = data.year
                        taxa_selic_ano = dados_macro.loc[dados_macro.index.year == ano_referencia, "selic"].values
                        if len(taxa_selic_ano) > 0:
                            taxa_selic_ano = taxa_selic_ano[0] / 100
                        else:
                            taxa_selic_ano = 0.1  # Taxa padrão de 10% ao ano se não houver dado
                
                        # Converter taxa Selic anual para mensal composta
                        taxa_selic_mensal = (1 + taxa_selic_ano) ** (1/12) - 1
                
                        # Aplicar rentabilidade do mês e adicionar novo aporte
                        saldo *= (1 + taxa_selic_mensal)
                        saldo += aporte_mensal
                        total_aplicado += aporte_mensal
                
                        # Armazenar o valor do patrimônio no Tesouro Selic
                        patrimonio_selic.loc[data, "Tesouro Selic"] = saldo
                
                    return patrimonio_selic

                    
                df_patrimonio_selic = calcular_patrimonio_selic_macro(dados_macro)
                                
                
                # 📌 Baixando preços ajustados das empresas
                def baixar_precos(tickers, start="2020-01-01"):
                    try:
                        precos = yf.download(tickers, start=start)['Close']
                        precos.columns = precos.columns.str.replace(".SA", "", regex=False)
                        return precos
                    except Exception as e:
                        st.error(f"Erro ao baixar preços: {e}")
                        return None
                
                
                # 📌 Analisando empresas por segmento
                if 'df_empresas' in locals() and not df_empresas.empty:
                    df_lideres = df_empresas[df_empresas["Rank_Ajustado"] == 1]
                
                    for segmento in df_lideres["Segmento"].unique():
                        st.subheader(f"📊 Comparação no Segmento: {segmento}")
                
                        lider = df_lideres[df_lideres["Segmento"] == segmento].iloc[0]
                        concorrentes = df_empresas[(df_empresas["Segmento"] == segmento) & (df_empresas["Rank_Ajustado"] != 1)]
                
                        if concorrentes.empty:
                            st.warning(f"⚠️ Não há concorrentes disponíveis para `{lider['nome_empresa']}` no segmento {segmento}.")
                            continue
                
                        tickers = [lider["ticker"]] + concorrentes["ticker"].tolist()
                        tickers = [ticker + ".SA" for ticker in tickers]
                
                        precos = baixar_precos(tickers)
                                                        
                        if precos is None or precos.empty:
                            continue
                
                
                        # 📌 Cálculo do patrimônio acumulado e evolução ao longo do tempo
                        df_patrimonio, df_patrimonio_evolucao = calcular_patrimonio_com_aportes(precos)
                                                       
                        df_patrimonio = pd.concat([df_patrimonio, df_patrimonio_selic.iloc[-1].to_frame().T], axis=0)
                        df_patrimonio = df_patrimonio.sort_values(by="Patrimonio Final", ascending=False)
                
                        # 📌 PLOTAGEM DO GRÁFICO DE EVOLUÇÃO DO PATRIMÔNIO ================================================================================================================
                        st.subheader("📈 Evolução do Patrimônio com Aportes Mensais")
                
                        fig, ax = plt.subplots(figsize=(12, 6))
                        
                        # 🔹 Convertendo índice para datetime e ordenando
                        df_patrimonio_evolucao.index = pd.to_datetime(df_patrimonio_evolucao.index, errors='coerce')
                        df_patrimonio_evolucao = df_patrimonio_evolucao.sort_index()
                        
                        # 🔹 Verificando se ainda há dados antes de prosseguir
                        if df_patrimonio_evolucao.empty:
                            st.warning("⚠️ Dados insuficientes para plotar a evolução do patrimônio.")
                        else:
                            # 🔹 Loop para plotar a evolução de cada empresa
                            for ticker in df_patrimonio_evolucao.columns:
                                if ticker == lider["ticker"]:  # Destacar empresa líder
                                    df_patrimonio_evolucao[ticker].plot(ax=ax, linewidth=2, color="red", label=f"{lider['nome_empresa']} (Líder)")
                                else:
                                    df_patrimonio_evolucao[ticker].plot(ax=ax, linewidth=1, linestyle="--", alpha=0.6, label=ticker)
                            
                            # 🔹 Ajuste do eixo X
                            min_date, max_date = df_patrimonio_evolucao.index.min(), df_patrimonio_evolucao.index.max()
                            if not pd.isna(min_date) and not pd.isna(max_date):
                                ax.set_xlim(min_date, max_date)
                                ax.set_xticks(pd.date_range(start=min_date, end=max_date, freq='6M'))  # Marcações semestrais
                                ax.tick_params(axis='x', rotation=30)
                            
                            ax.set_title(f"Evolução do Patrimônio Acumulado no Segmento: {segmento}")
                            ax.set_xlabel("Data")
                            ax.set_ylabel("Patrimônio (R$)")
                            ax.legend()
                            st.pyplot(fig)
                                        
                        # 📌 EXIBIÇÃO DOS QUADRADOS (BLOCOS COM OS RESULTADOS)
                        st.subheader("📊 Patrimônio Final para R$1.000/Mês Investidos desde 2020")
                
                        df_patrimonio = df_patrimonio.reset_index(drop=False)  # Tickers como coluna
                
                        num_columns = 3  # Número de colunas no layout
                        columns = st.columns(num_columns)
                
                        # Exibir os blocos organizados corretamente com os tickers visíveis e ícones das empresas
                        for i, row in df_patrimonio.iterrows():
                            ticker = row['index']
                            patrimonio = row['Patrimonio Final']
                            icone_url = get_logo_url(ticker)  # Obtendo o ícone da empresa
                            
                            # Se for a empresa líder, destacá-la apenas com a borda dourada
                            is_lider = (ticker == lider["ticker"])
                            border_color = "#DAA520" if is_lider else "#d3d3d3"  # Borda dourada para líder, cinza para demais
                            
                            with columns[i % num_columns]:  # Distribuindo os blocos nas colunas
                                st.markdown(f"""
                                    <div style="
                                        background-color: #ffffff;
                                        border: 3px solid {border_color};
                                        border-radius: 10px;
                                        padding: 15px;
                                        margin: 10px;
                                        text-align: center;
                                        box-shadow: 2px 2px 5px rgba(0, 0, 0, 0.1);
                                        flex: 1;
                                    ">
                                        <img src="{icone_url}" alt="{ticker}" style="width: 50px; height: auto; margin-bottom: 5px;">
                                        <h3 style="margin: 0; color: #4a4a4a;">{ticker}</h3>
                                        <p style="font-size: 18px; margin: 5px 0; font-weight: bold; color: #2ecc71;">
                                            {formatar_real(patrimonio)}
                                        </p>
                                    </div>
                                """, unsafe_allow_html=True)
