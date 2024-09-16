import streamlit as st
import pandas as pd
import plotly.express as px
from sklearn.linear_model import LinearRegression
import numpy as np

# Função para carregar logotipos locais ___________________________________________________________________________________________________________________________________________
def get_local_logo(ticker):
    logo_path = f"logos/{ticker}.png"  # Substitua pela pasta onde seus logotipos estão armazenados
    if os.path.exists(logo_path):
        return logo_path
    return None

# Função para buscar informações da empresa usando yfinance
def get_company_info(ticker):
    try:
        # Adicionar ".SA" para tickers da B3 (bolsa brasileira) se não estiver presente
        if not ticker.endswith(".SA"):
            ticker += ".SA"
        
        # Usar yfinance para pegar informações básicas da empresa
        company = yf.Ticker(ticker)
        info = company.info
        return info['longName'], info.get('website')  # Retorna o nome da empresa e o site (para o logo)
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
@st.cache_data
def load_data():
    # Carregar o DataFrame a partir do arquivo local
    df = pd.read_csv('indicadores', index_col=False)
    # Converter a coluna 'Data' para datetime e extrair apenas o ano
    df['Data'] = pd.to_datetime(df['Data'], errors='coerce').dt.year  # Extrair somente o ano
     # Remover a coluna 'Ano' se existir no DataFrame
    if 'Ano' in df.columns:
        df = df.drop(columns=['Ano'])
    # Garantir que 'Data' seja convertida para inteiros (sem vírgulas)
    df['Data'] = df['Data'].astype(int)
    # Substituir espaços nos nomes das colunas por underlines
    df.columns = df.columns.str.replace(' ', '_')
    # Retornar o DataFrame

     # Remover a coluna 'Ano' se existir no DataFrame
    if 'Ano' in df.columns:
        df = df.drop(columns=['Ano'])
    return df
    
indicadores = load_data()

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
    col_monetarias = ['Close', 'LPA', 'Receita_Liquida', 'Ativo_Circulante', 'Passivo_Circulante', 'Capital_de_Giro', 'patrimonio_liquido', 
                      'lucro_operacional', 'Lucro_Líquido', 'Dividendos', 'Divida_Líquida', 'balanca_comercial', 'cambio']
    col_porcentagem = ['Margem_Líquida', 'ROE', 'indice_endividamento', 'selic', 'ipca', 'icc']
    
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
    
    return df

# Aplicar formatação na tabela de indicadores
indicadores_formatado = format_dataframe(indicadores.copy())

# Barra superior (simulação) buscando a logo das empresas ____________________________________________________________________________________________________________________________________________
col1, col2 = st.columns([4, 1])
with col1:
    ticket = st.text_input("Buscar por Ticket")

# Verificar se o ticket foi inserido
if ticket:
    company_name, logo_url = get_company_info(ticket)
    if company_name and logo_url:
        st.subheader(f"Visão Geral (CARG) - {company_name}")
        
        # Exibir o logotipo no canto direito
        col1, col2 = st.columns([4, 1])
        with col1:
            st.write(f"Informações financeiras de {company_name}")
        with col2:
            st.image(logo_url, width=150)  # Mostrar o logotipo
    else:
        st.error(f"Empresa não encontrada. Verifique o ticket inserido.")

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


    fig.update_traces(line=dict(width=3), marker=dict(size=8), text=df_melted['Valor'].round(2), mode='lines+markers+text',
                      textposition='top center')
    fig.update_layout(xaxis_title='Ano', yaxis_title='Valor',
                      plot_bgcolor='#1f1f1f',  # Fundo escuro
                      paper_bgcolor='#1f1f1f',
                      font=dict(color='#ffffff'),  # Cor do texto
                      title_font=dict(color='#8A2BE2'),
                      legend_title_text='Indicadores',
                      xaxis=dict(showgrid=True, gridcolor='#444444'),
                      yaxis=dict(showgrid=True, gridcolor='#444444'))

    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("Por favor, selecione pelo menos um indicador para exibir no gráfico.")

# Indicadores Categoriais (exemplo de blocos à direita) ____________________________________________________________________________________________________________________________________________
col1, col2, col3 = st.columns(3)

with col1:
    st.progress(68, text="Compras")
with col2:
    st.progress(76, text="Trabalho")
with col3:
    st.progress(73, text="Plataforma")

# Tabela de Indicadores
st.markdown("### Tabela de Indicadores")
st.dataframe(indicadores_formatado)


