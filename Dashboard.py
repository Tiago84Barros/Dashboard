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

# Chama o banco de dados dos Múltiplos __________________________________________________________________________________________________________________________________________________
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
        
             
        # Adicionar estilo CSS para os blocos, com o logo à direita e as informações à esquerda, e altura fixa ___________________________________________________________________________________________________________________________________________________________________________________________
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
        
        # Inserir campo para o usuário digitar o ticker _______________________________________________________________________________________________________________________________-
        col1, col2 = st.columns([4, 1])
        with col1:
            # Se houver um ticker definido via clique ou input, usá-lo como valor no campo de busca
            if 'ticker' in st.session_state:
                ticker_input = st.text_input("DIGITE O TICKER:", value=st.session_state.ticker.split(".SA")[0], key="ticker_input").upper()
            else:
                ticker_input = st.text_input("Digite o ticker (ex: GMAT3)", key="ticker_input").upper()
        
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
        
                # Verificar se a coluna está vazia ou possui apenas valores nulos
                if df[column].isnull().all():
                    raise ValueError(f"A coluna '{column}' está vazia ou contém apenas valores nulos.")
        
                # Valores inicial e final da coluna de interesse
                initial_value = df[column].iloc[0]
                final_value = df[column].iloc[-1]
        
                # Verificando possíveis erros nos valores
                if initial_value == 0:
                    raise ValueError(f"Valor inicial do indicador '{column}' é zero. Não é possível calcular CAGR.")
                
                if final_value == 0:
                    raise ValueError(f"Valor final do indicador '{column}' é zero. Não é possível calcular CAGR.")
        
                # Calculando o número de anos (diferença de tempo em anos)
                num_years = (df['Data'].iloc[-1] - df['Data'].iloc[0]).days / 365.25
        
                if num_years <= 0 or pd.isna(num_years):
                    raise ValueError(f"O número de anos calculado é inválido: {num_years}. Verifique as datas fornecidas.")
        
                # Cálculo do CAGR
                cagr = (final_value / initial_value) ** (1 / num_years) - 1
            
                return cagr
        
            except Exception as e:
                #st.error(f"Erro ao calcular o CAGR para '{column}': {e}")
                return np.nan  # Retorna NaN em caso de erro
        
        
        # Calcular o CAGR para cada indicador
        cagrs = {}
        
        for column in indicadores.columns:
            if column != 'Data':  # Ignorar a coluna de datas
                # Checar se há dados suficientes para cálculo
                col_data = indicadores[column]
                if col_data.isnull().all() or (col_data == 0).all():
                    cagrs[column] = None  # Ignorar colunas inválidas
                else:
                    cagr = calculate_cagr(indicadores, column)
                    cagrs[column] = cagr
            
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
        
        # Adicionar estilo CSS para criar quadrados para o CAGR
        st.markdown("""
            <style>
            .cagr-box {
                border: 1px solid #ddd;
                padding: 20px;
                border-radius: 10px;
                margin-bottom: 10px;
                display: flex;
                justify-content: center;
                align-items: center;
                height: 100px;  /* Definindo a altura do bloco */
                width: 100%;  /* Largura completa */
                text-align: center;
                font-size: 20px;
                font-weight: bold;
                color: #333;
                background-color: #f9f9f9;
            }
            </style>
        """, unsafe_allow_html=True)
        
        def format_cagr(value):
            if isinstance(value, (int, float)) and not pd.isna(value) and not np.isinf(value):
                return f"{value:.2%}"
            else:
                return "-"
        
        # Exibir os valores do CAGR em quadrados
        st.markdown("### Visão Geral (CAGR)")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown(f"<div class='cagr-box'>Receita Líquida: {format_cagr(cagrs['Receita_Liquida'])}</div>", unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"<div class='cagr-box'>Lucro Líquido: {format_cagr(cagrs['Lucro_Liquido'])}</div>", unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"<div class='cagr-box'>Patrimônio Líquido: {format_cagr(cagrs['Patrimonio_Liquido'])}</div>", unsafe_allow_html=True)
         
        
        # Cria o gráfico em BARRA e o seletor para escolher quais variáveis mostrar das DFPs __________________________________________________________________________________________________________________________________________________
        
        # Seletor para escolher quais variáveis visualizar no gráfico
        st.markdown("### Selecione os Indicadores para Visualizar no Gráfico")
        
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
        default_cols = ['Receita Líquida', 'Lucro Líquido']  # Ajuste conforme necessário
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
                    title='Evolução dos Indicadores Selecionados'
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
            
        
        # Exibir a tabela de indicadores no final ____________________________________________________________________________________________________________________________________________________
        # st.markdown("### Tabela de Indicadores")
        # st.dataframe(indicadores)  # Mostra a tabela interativa no dashboard
        
        # Função para carregar os dados da tabela "multiplos" do banco de dados  ________________________________________________________________________________________________________________________________________________
        @st.cache_data
        def load_multiplos_limitado_from_db(ticker):
            db_path = download_db_from_github(db_url)
            
            if db_path is None or not os.path.exists(db_path):
                return None
        
            try:
                conn = sqlite3.connect(db_path)
        
                # Buscar dados na tabela 'multiplos' para o ticker
                query_multiplos = f"""
                SELECT * FROM multiplos 
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
        # st.markdown("### Tabela de Múltiplos")
        # st.dataframe(multiplos)  # Mostra a tabela interativa no dashboard
        
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
                    <div class='metric-label' title='Mede a eficiência da empresa em converter receita em lucro após todas as despesas.'>Margem Líquida</div>
                  </div>
                </div>
                """, unsafe_allow_html=True)
        
            # Coluna 2 - Margem Operacional
            with col2:
                margem_Operacional = multiplos['Margem_Operacional'].fillna(0).values[0]
                st.markdown(f"""
                <div class='metric-box'>
                    <div class='metric-value'>{margem_Operacional:.2f}%</div>
                    <div class='metric-label' title='Mede a eficiência operacional da empresa antes das despesas financeiras e impostos.'>Margem Operacional</div>
                  </div>
                </div>
                """, unsafe_allow_html=True)
        
            # Coluna 3 - ROE
            with col3:
                roe = multiplos['ROE'].fillna(0).values[0]
                st.markdown(f"""
                <div class='metric-box'>
                    <div class='metric-value'>{roe:.2f}%</div>
                    <div class='metric-label' title='ROE (Retorno sobre o Patrimônio): Indica a eficiência da empresa em gerar lucro com o capital dos acionistas.'>ROE</div>
                </div>
                """, unsafe_allow_html=True)
        
            # Coluna 4 - ROIC
            with col4:
                roic = multiplos['ROIC'].fillna(0).values[0]
                st.markdown(f"""
                <div class='metric-box'>
                    <div class='metric-value'>{roic:.2f}%</div>
                    <div class='metric-label' title='ROIC (Retorno sobre Capital Investido): Mede a eficiência da empresa em gerar retorno sobre o capital total investido.'>ROIC</div>
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
                    dividend_yield = f"{(100 * dy_value):.2f}%"
                st.markdown(f"""
                <div class='metric-box'>
                    <div class='metric-value'>{dividend_yield}</div>
                    <div class='metric-label' title='Mede o retorno percentual dos dividendos pagos pela empresa em relação ao preço da ação.'>Dividend Yield</div>
                </div>
                """, unsafe_allow_html=True)
        
            # Coluna 6 - P/VP
            with col6:
                pvp_value = multiplos['P/VP'].fillna(0).values[0]
                if pvp_value == 0 or pd.isna(pvp_value) or np.isinf(pvp_value):  # Verifica divisão por zero ou NaN
                    pvp = "-"
                else:
                    pvp = f"{(pvp_value):.2f}"
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
                    <div class='metric-label' title='Indica a porcentagem do lucro líquido que é distribuída aos acionistas na forma de dividendos.'>Payout</div>
                </div>
                """, unsafe_allow_html=True)
        
            # Coluna 08 - P/L
            with col8:
                pl_value = multiplos['P/L'].fillna(0).values[0]
                if pl_value == 0 or pd.isna(pl_value) or np.isinf(pl_value):  # Verifica divisão por zero ou NaN
                    pl = "-"
                else:
                    pl = f"{(pl_value):.2f}"
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
                     <div class='metric-label' title='Mede o nível de dívida da empresa em relação ao seu patrimônio e ativos.'>Endividamento Total</div>
                </div>
                """, unsafe_allow_html=True)
        
             # Coluna 10 - Alavancagem Financeira sobre o Patrimônio Líquido
            with col10:
                alavancagem_financeira = multiplos['Alavancagem_Financeira'].fillna(0).values[0]
                st.markdown(f"""
                <div class='metric-box'>
                    <div class='metric-value'>{alavancagem_financeira:.2f}</div>
                    <div class='metric-label' title='Mede o quanto a empresa utiliza de capital de terceiros em relação ao patrimônio líquido.'>Alavancagem Financeira</div>
                </div>
                """, unsafe_allow_html=True)
        
             
            # Coluna 11: Líquidez Corrente
            with col11:
                Liquidez_Corrente = multiplos['Liquidez_Corrente'].fillna(0).values[0]
                st.markdown(f"""
                <div class='metric-box'>
                    <div class='metric-value'>{Liquidez_Corrente:.2f}</div>
                    <div class='metric-label' title='Mede a capacidade da empresa em honrar suas dívidas de curto prazo com seus ativos circulantes.'>Liquidez Corrente</div>
                </div>
                """, unsafe_allow_html=True)
        
        
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
    
        st.markdown("""
            <h1 style='text-align: center; font-size: 36px; color: #333;'>Análise Avançada de Ações</h1>
        """, unsafe_allow_html=True)

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

                    # Determinar o último ano disponível no banco de dados de demonstrações financeiras _______________________________________________________________________________________
                    def get_latest_year(df):
                        if "Data" in df.columns:
                            return pd.to_datetime(df["Data"], errors="coerce").dt.year.max()
                        return None
                    
                    # Atualizar o filtro de empresas
                    filtered_empresas = []
                    latest_year = None  # Variável para armazenar o último ano disponível
                    
                    # Filtrar empresas com informações do último ano apresentado
                    for i, row in empresas_filtradas.iterrows():
                        ticker = row['ticker']
                        
                        # Carregar os dados das demonstrações financeiras da empresa
                        dfp = load_data_from_db(ticker + ".SA")
                        if dfp is None or dfp.empty:
                            continue
                        
                        # Determinar o último ano disponível na demonstração financeira
                        if latest_year is None:
                            latest_year = get_latest_year(dfp)
                        
                        # Verificar se a empresa possui informações do último ano
                        dfp["Ano"] = pd.to_datetime(dfp["Data"], errors="coerce").dt.year
                        if latest_year in dfp["Ano"].values:
                            filtered_empresas.append(row)
                    
                    # Criar um DataFrame com as empresas filtradas
                    empresas_filtradas = pd.DataFrame(filtered_empresas)
                    
                    # Verificar se há empresas restantes após o filtro
                    if empresas_filtradas.empty:
                        st.warning("Nenhuma empresa apresenta informações para o último ano disponível.")
         
        
                    st.markdown(f"### Empresas no Segmento {segmento_selecionado}")

                    st.markdown("---") # Espaçamento entre diferentes tipos de análise
                    st.markdown("<div style='margin: 30px;'></div>", unsafe_allow_html=True)
                    
                    # Criar um dataframe para armazenar o score das empresas _________________________________________________________________________________________________________________
                    # Inicializar a lista de resultados
                    resultados = []
                    
                    # Iterar pelas empresas
                    for i, row in empresas_filtradas.iterrows():
                        ticker = row['ticker']
                        nome_emp = row['nome_empresa']
                    
                        # Carregar dados de múltiplos
                        multiplos = load_multiplos_from_db(ticker + ".SA")
                        if multiplos is None or multiplos.empty:
                            continue
                    
                        # Carregar dados das demonstrações financeiras
                        dfp = load_data_from_db(ticker + ".SA")
                                      
                        if dfp is None or dfp.empty:
                            continue
                    
                        # Filtrar apenas dados recentes (ex.: do último ano)
                        multiplos_recent = multiplos[multiplos['Data'] == multiplos['Data'].max()]
                        dfp_recent = dfp[dfp['Data'] == dfp['Data'].max()]
                    
                        if multiplos_recent.empty or dfp_recent.empty:
                            continue
                    
                        # Seleção de métricas das demonstrações financeiras
                        receita_liquida = dfp_recent['Receita_Liquida'].iloc[0]
                        lucro_liquido = dfp_recent['Lucro_Liquido'].iloc[0]
                        patrimonio_liquido = dfp_recent['Patrimonio_Liquido'].iloc[0]
                        caixa_liquido = dfp_recent['Caixa_Liquido'].iloc[0]
                        divida_liquida = dfp_recent['Divida_Liquida'].iloc[0]
                    
                        # Seleção de métricas de múltiplos
                        margem_liquida = multiplos_recent['Margem_Liquida'].iloc[0]
                        roe = multiplos_recent['ROE'].iloc[0]
                        endividamento = multiplos_recent['Endividamento_Total'].iloc[0]
                        pl = multiplos_recent['P/L'].iloc[0]
                    
                        # Normalizar métricas em relação ao setor (se disponível)
                        setor_empresas = empresas_filtradas[empresas_filtradas['SETOR'] == row['SETOR']]
                        setor_dfp = dfp[dfp['Ticker'].isin(setor_empresas['ticker'])]
                        setor_multiplos = multiplos[multiplos['Ticker'].isin(setor_empresas['ticker'])]
                    
                        # Métricas normalizadas (usar média do setor como referência)
                        def normalize_sector(value, sector_values):
                            if pd.isna(value) or sector_values.empty:
                                return 0.5  # Valor padrão se dados faltarem
                            sector_mean = sector_values.mean()
                            sector_std = sector_values.std()
                            return (value - sector_mean) / sector_std if sector_std > 0 else 0.5
                    
                        score_receita = normalize_sector(receita_liquida, setor_dfp['Receita_Liquida'])
                        score_lucro = normalize_sector(lucro_liquido, setor_dfp['Lucro_Liquido'])
                        score_patrimonio = normalize_sector(patrimonio_liquido, setor_dfp['Patrimonio_Liquido'])
                        score_caixa = normalize_sector(caixa_liquido, setor_dfp['Caixa_Liquido'])
                        score_divida = normalize_sector(divida_liquida, setor_dfp['Divida_Liquida'])
                        score_margem = normalize_sector(margem_liquida, setor_multiplos['Margem_Liquida'])
                        score_roe = normalize_sector(roe, setor_multiplos['ROE'])
                        score_endividamento = normalize_sector(endividamento, setor_multiplos['Endividamento_Total'])
                        score_pl = normalize_sector(pl, setor_multiplos['P/L'])
                    
                        # Combinar os scores com média ponderada
                        final_score = np.mean([
                            score_receita, score_lucro, score_patrimonio, score_caixa, score_divida,
                            score_margem, score_roe, score_endividamento, score_pl
                        ])
                    
                        resultados.append({
                            'nome_empresa': nome_emp,
                            'ticker': ticker,
                            'score': final_score
                        })
                    
                    # Converter resultados em dataframe e ordenar
                    if resultados:
                        df_resultados = pd.DataFrame(resultados).sort_values(by='score', ascending=False)
                    
                        # Exibir as empresas em pequenos blocos
                        st.markdown("### Ranking de Empresas")
                        colunas = st.columns(3)  # Ajuste o número de colunas desejado
                    
                        # Iterar pelos resultados em ordem sequencial
                        for idx, row in enumerate(df_resultados.itertuples()):
                            col = colunas[idx % len(colunas)]  # Seleciona a coluna da esquerda para a direita
                    
                            with col:
                                logo_url = get_logo_url(row.ticker)  # Função para obter o logotipo
                    
                                # Criar layout usando colunas internas para logotipo e informações
                                col_logo, col_texto = st.columns([1, 3])
                    
                                with col_logo:
                                    st.image(logo_url, width=50)  # Exibir o logotipo com largura de 50px
                    
                                with col_texto:
                                    st.markdown(f"""
                                        <div style="font-size: 16px; font-weight: bold; color: #333; margin-bottom: 5px;">
                                            {row.nome_empresa} ({row.ticker})
                                        </div>
                                        <div style="font-size: 14px; color: #555;">
                                            Score: <span style="color: green; font-weight: bold;">{row.score:.2f}</span>
                                        </div>
                                    """, unsafe_allow_html=True)
                    
                    else:
                        st.info("Não há dados disponíveis para empresas neste segmento.")

                   # Este trecho de código deve estar dentro do bloco onde o usuário já selecionou o setor, subsetor, segmento.
                    # Ou seja, após você ter definido o DataFrame 'empresas_filtradas'.

                    st.markdown("---") # Espaçamento entre diferentes tipos de análises
                    st.markdown("<div style='margin: 30px;'></div>", unsafe_allow_html=True)
                    
                    st.markdown("### Comparação de Indicadores (Múltiplos) entre Empresas do Segmento") #___________________________________________________________________________________________________
                    
                  # Lista de indicadores disponíveis
                    indicadores_disponiveis = ["Margem Líquida", "ROE", "P/L", "EV_EBITDA"]
                    
                    # Mapeamento de nomes amigáveis para nomes de colunas no banco
                    nomes_to_col = {
                        "Margem Líquida": "Margem_Liquida",
                        "ROE": "ROE",
                        "P/L": "P/L",
                        "EV_EBITDA": "EV_EBITDA"
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

                       # Seção: Gráfico Comparativo de Demonstrações Financeiras _____________________________________________________________________________________________________________
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
                            indicadores_dre=["Receita_Liquida", "Lucro_Liquido", "Patrimonio_Liquido", "Caixa_Liquido"]
                        )
                        
                        if dre_data_comparativo is not None:
                            # Criar mapeamento de nomes de colunas para nomes amigáveis
                            col_name_mapping = {
                                "Receita_Liquida": "Receita Líquida",
                                "Lucro_Liquido": "Lucro Líquido",
                                "Patrimonio_Liquido": "Patrimônio Líquido",
                                "Caixa_Liquido": "Caixa Líquido",
                                "Fluxo_Caixa": "Fluxo de Caixa"
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
