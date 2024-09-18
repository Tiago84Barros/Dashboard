import pandas as pd
import wget
import os
import zipfile
import requests
import plotly.graph_objects as go
import yfinance as yf
from datetime import datetime
from bcb import sgs


#Insere o local no qual os arquivos dos históricos das informçaões das empresas
#de capital aberto, disponibilizadas pela CVM, ficarão armazenados.

diretorio_inicial = '/content/drive/MyDrive/Colab Notebooks/Dados'
if not os.path.exists(f"{diretorio_inicial}/dados_cvm"):
    os.makedirs(f"{diretorio_inicial}/dados_cvm")
os.chdir(f"{diretorio_inicial}/dados_cvm")

# Função para verificar se o arquivo de um determinado ano já foi extraído

def arquivo_extraido(ano):
    # Verificar se existe uma pasta ou arquivo do ano correspondente no diretório
    arquivos_ano = [f for f in os.listdir('/content/drive/MyDrive/Colab Notebooks/Dados/dados_cvm') if str(ano) in f]
    return len(arquivos_ano) > 0

# A partir do site da CVM, o código abaixo pega todo o histórico das Demonstrações Financeiras Padronizadas e 
# insere na pasta /content/drive/MyDrive/Colab_Notebooks/Dados/dados_cvm. Estes arquivos estão no formato ZIP e 
# serão descompactados e colocados na pasta /content/drive/MyDrive/Colab_Notebooks/Dados/CVM/

Ultimo_ano = 2024 # Ano do último lançamento de dados
url_base = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"
for ano in range(2010,Ultimo_ano):
  if not arquivo_extraido(ano):
        arquivo_zip = wget.download(url_base + f"dfp_cia_aberta_{ano}.zip")
        with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
            zip_ref.extractall('/content/drive/MyDrive/Colab Notebooks/Dados/CVM')

# Da pasta /content/drive/MyDrive/Colab_Notebooks/Dados/CVM captura apenas o arquivo referente ao histórico de todas das DREs, BPA e BPP, 
# concatenando todos em um só arquivo chamado 'con_cia_aberta_DRE_2010_2024', 'con_cia_aberta_BPA_2010_2024' e 'con_cia_aberta_BPP_2010_2024' 
# que ficará dentro da pasta /content/drive/MyDrive/Colab_Notebooks/Dados/dados_concat

arquivo = pd.DataFrame()
ano_atual = datetime.now().year
for ano in range(2010,Ultimo_ano):
    arquivo = pd.concat([arquivo, pd.read_csv(f'/content/drive/MyDrive/Colab Notebooks/Dados/CVM/dfp_cia_aberta_DRE_con_{ano}.csv', sep = ';', decimal = ',', encoding = 'ISO-8859-1')])
if not os.path.exists('/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat'):
    os.makedirs(f'/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat')
elif Ultimo_ano > ano_atual:
  os.remove(f'/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat/con_cia_aberta_DRE_2010_{Ultimo_ano - 1}')  # Excluir o arquivo antigo
arquivo.to_csv(f'/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat/con_cia_aberta_DRE_2010_{Ultimo_ano}', index = False)

# O mesmo comando do código acima, porém para o BPA

arquivo2 = pd.DataFrame()

for ano in range(2010,Ultimo_ano):
  arquivo2 = pd.concat([arquivo2, pd.read_csv(f'/content/drive/MyDrive/Colab Notebooks/Dados/CVM/dfp_cia_aberta_BPA_con_{ano}.csv', sep = ';', decimal = ',', encoding = 'ISO-8859-1')])
if not os.path.exists('/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat'):
    os.makedirs(f'/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat')
    arquivo2.to_csv(f'/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat/con_cia_aberta_BPA_2010_{Ultimo_ano}', index = False)
elif Ultimo_ano > ano_atual:
  os.remove(f'/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat/con_cia_aberta_BPA_2010_{Ultimo_ano-1}')  # Excluir o arquivo antigo
  arquivo2.to_csv(f'/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat/con_cia_aberta_BPA_2010_{Ultimo_ano}', index = False)

# O mesmo do comando anterior, agora para o BPP

arquivo3 = pd.DataFrame()
for ano in range(2010,2024):
  arquivo3 = pd.concat([arquivo3, pd.read_csv(f'/content/drive/MyDrive/Colab Notebooks/Dados/CVM/dfp_cia_aberta_BPP_con_{ano}.csv', sep = ';', decimal = ',', encoding = 'ISO-8859-1')])
if not os.path.exists('/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat'):
    os.makedirs(f'/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat')
elif Ultimo_ano > ano_atual:
  os.remove(f'/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat/con_cia_aberta_BPP_2010_{Ultimo_ano-1}')  # Excluir o arquivo antigo
arquivo3.to_csv(f'/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat/con_cia_aberta_BPP_2010_{Ultimo_ano}', index = False)

# O mesmo do comando anterior, agora para o DFC

arquivo4 = pd.DataFrame()
for ano in range(2010,2024):
  arquivo4 = pd.concat([arquivo4, pd.read_csv(f'/content/drive/MyDrive/Colab Notebooks/Dados/CVM/dfp_cia_aberta_DFC_MD_con_{ano}.csv', sep = ';', decimal = ',', encoding = 'ISO-8859-1')])
if not os.path.exists('/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat'):
    os.makedirs(f'/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat')
elif Ultimo_ano > ano_atual:
  os.remove(f'/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat/con_cia_aberta_DFC_2010_{Ultimo_ano-1}')  # Excluir o arquivo antigo
arquivo4.to_csv(f'/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat/con_cia_aberta_DFC_2010_{Ultimo_ano}', index = False)

# Nesse ponto, os arquivos criados serão puxados para as variáveis dre, bpa, bpp em um formato de DataFrame, utilizando a biblioteca 'Pandas'


dre = pd.read_csv(f'/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat/con_cia_aberta_DRE_2010_{Ultimo_ano}')
bpa = pd.read_csv(f'/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat/con_cia_aberta_BPA_2010_{Ultimo_ano}')
bpp = pd.read_csv(f'/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat/con_cia_aberta_BPP_2010_{Ultimo_ano}')
dfc = pd.read_csv(f'/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat/con_cia_aberta_DFC_2010_{Ultimo_ano}')

# Começa aqui o processo de filtragem das informações contidas em dre, bpa e bpp. Sendo deixado no arquivo apenas o último balanço presente no DataFrame

dre = dre[dre['ORDEM_EXERC'] == "ÚLTIMO"]
bpa = bpa[bpa['ORDEM_EXERC'] == "ÚLTIMO"]
bpp = bpp[bpp['ORDEM_EXERC'] == "ÚLTIMO"]
dfc = dfc[dfc['ORDEM_EXERC'] == "ÚLTIMO"]

# Outra filtragem. Desssa vez, no arquivo chamado 'empresas', puxa apenas as colunas 'DENOM_CIA' = nome das empresas e 
# 'CD_CVM' = código referente a essa empresa, eliminando também nomes duplicados, já que o mesmo nome da empresa aparece varias vezes na DRE

empresas = dre[['DENOM_CIA', 'CD_CVM']].drop_duplicates().set_index('CD_CVM')

# Filtra apenas a empresa de código '25186' que se trata da .... Exibindo em DataTable apenas algumas colunas relevantes da DRE dessa empresa

CD_CVM = 18376
empresa  = dre[dre['CD_CVM'] == CD_CVM]
empresa2 = bpa[bpa['CD_CVM'] == CD_CVM]
empresa3 = bpp[bpp['CD_CVM'] == CD_CVM]
empresa4 = dfc[dfc['CD_CVM'] == CD_CVM]

# A partir do código específico da receita líquida e lucro por ação da empresa, presente na DRE (empresa), '3.01' e '3.99.01.01' 
# respectivamente, que pode ser visualizado pela filtragem anterior, esses valores são inseridos na variável 'conta_receita' e 'conta_LPA'

conta_receita = empresa[empresa['CD_CONTA'] == '3.01']
conta_receita.index = pd.to_datetime(conta_receita['DT_REFER'])

conta_lucro_operacional = empresa[empresa['CD_CONTA'] == '3.05']
conta_lucro_operacional.index = pd.to_datetime(conta_lucro_operacional['DT_REFER'])

conta_lucro_liquido = empresa[empresa['CD_CONTA'] == '3.11']
conta_lucro_liquido.index = pd.to_datetime(conta_lucro_liquido['DT_REFER'])

conta_LPA = empresa[empresa['CD_CONTA'] == '3.99.01.01']
conta_LPA.index = pd.to_datetime(conta_LPA['DT_REFER'])

# Utilizando o Balanço Patrimonial dos Ativos presentes em BPA (empresa2) e a partir do código específico do Ativo Circulante, 
# '1.01' que pode ser visualizado pela filtragem anterior, esses valores são inseridos na variável 'conta_receita' e 'conta_lucro_liquido

conta_ativo_circulante = empresa2[empresa2['CD_CONTA'] == '1.01']
conta_ativo_circulante.index = pd.to_datetime(conta_ativo_circulante['DT_REFER'])

conta_caixa_e_equivalentes = empresa2[empresa2['CD_CONTA'] == '1.01.01']
conta_caixa_e_equivalentes.index = pd.to_datetime(conta_caixa_e_equivalentes['DT_REFER'])

# Utilizando o Balanço Patrimonial dos Passivos presentes em BPP (empresa3) e a partir do código específico do Passivo Circulante, 
# '2.01', dividendos, '2.01.05.02.01' e patrimônio líquido, 2.03, que podem ser visualizados pela filtragem anterior, esses valores são 
# inseridos na variável 'conta_ativo_circulante', 'conta_dividendos' e 'conta_patrimonio_liquido' respectivamente.


conta_passivo_circulante = empresa3[empresa3['CD_CONTA'] == '2.01']
conta_passivo_circulante.index = pd.to_datetime(conta_passivo_circulante['DT_REFER'])

conta_passivo_nao_circulante = empresa3[empresa3['CD_CONTA'] == '2.02']
conta_passivo_nao_circulante.index = pd.to_datetime(conta_passivo_nao_circulante['DT_REFER'])

conta_patrimonio_liquido = empresa3[empresa3['CD_CONTA'] == '2.03']
conta_patrimonio_liquido.index = pd.to_datetime(conta_patrimonio_liquido['DT_REFER'])

conta_dividendos = empresa3[empresa3['CD_CONTA'] == '2.01.05.02.01']
conta_dividendos.index = pd.to_datetime(conta_dividendos['DT_REFER'])

#Através de yfinance pegamos o preço histórico da empresa desde o início de 2010 até os dias de hoje e utilizamos um filtro para inserir apenas o valor do preço de fechamento na variável prices

prices = yf.download('TRPL3.SA', start = '2010-01-01', end = '2023-12-31')['Close']

# Utiliza a função "resample" para obter os preços apenas do último dia do ano

df_resample = prices.resample('A').last()
precos = df_resample

# Definindo o período de interesse

start_date = datetime.strptime('01/01/2010', "%d/%m/%Y").date() # Changed the format code to match the date string format
end_date =  datetime.strptime('31/12/2023', "%d/%m/%Y").date() # Changed the format code to match the date string format

# Descrição do código e dos indicadores que serão capturados através da API do Banco Central

ind_economicos = {
     432: 'selic',                  # SELIC
     433: 'ipca',                   # IPCA
       1: 'cambio',                 # Câmbio
   22707: 'balanca_comercial',      # Balança Comercial
      14: 'icc',                    # Índice de Confiança do Consumidor
    4380: 'pib',                    # PIB
    4502: 'divida_publica'  # Dívida Pública (DBGG)
}

# Dicionário para armazenar DataFrames
dados = {}

# Obtendo dados do Banco central

for codigo, nome in ind_economicos.items():
      df = sgs.get({nome: codigo}, start = start_date, end = end_date)
      dados[nome] = df

# Função para calcular o acumulado anual do IPCA

def calcular_acumulado_anual(df, coluna):
    df_resample = df.resample('A').apply(lambda x: (1 + x / 100).prod() - 1)
    df_resample.rename(columns={coluna: f'{coluna}'}, inplace=True)
    return df_resample

# Função para identificar o último valor inserido no ano das variáveis SELIC, Câmbio e Dívida Pública


def ultimo_dia_util(df, coluna):
    df_resample = df.resample('A').last()
    df_resample.rename(columns={coluna: f'{coluna}'}, inplace=True)
    return df_resample

# Função para calcular o PIB anual (somando os valores trimestrais)

def calcular_pib_anual(df):
    df_resample = df.resample('A').sum()
    df_resample.rename(columns={'pib': 'PIB'}, inplace=True)
    return df_resample

# Função para somar os dados mensais da balança comercial para obter o valor anual

def somar_anual(df, coluna):
    df_resample = df.resample('A').sum()
    df_resample.rename(columns={coluna: f'{coluna}'}, inplace=True)
    return df_resample

# Calculando o acumulado anual para indicadores relevantes e obtendo o último dia útil para SELIC e Câmbio

dados_anuais_acumulados = pd.DataFrame()
for nome, df in dados.items():
   if nome == 'pib': # Calcular o PIB anual somando os valores trimestrais
        df_pib_anual = calcular_pib_anual(df)
        dados_anuais_acumulados = pd.concat([dados_anuais_acumulados, df_pib_anual], axis=1)
   elif nome in ['selic', 'cambio', 'divida_publica']: # Pega apenas o último dia de cada ano
        df_ultimo = ultimo_dia_util(df, nome)
        dados_anuais_acumulados = pd.concat([dados_anuais_acumulados, df_ultimo], axis=1)
   elif nome == 'balanca_comercial': # Calcular a Balança Comercial mensal para anual
        df_anual = somar_anual(df, nome)
        dados_anuais_acumulados = pd.concat([dados_anuais_acumulados, df_anual], axis=1)
   else: # Utiliza informações acumuladas do IPCA para determinar o valor anual do índice
        df_acumulado = calcular_acumulado_anual(df, nome)
        dados_anuais_acumulados = pd.concat([dados_anuais_acumulados, df_acumulado], axis=1)

# Utilizamos DataFrame para transformar a variável 'prices' para um dataframe e e utilizamos a função join para juntar os dados da variável 'prices' a valores de 'VL_CONTA' presente na variável 'conta' e posteriormente colocando todas essas informações na variável 'indicadores'

indicadores = pd.DataFrame()  # Cria um DataFrame vazio, sem colunas e sem linhas
indicadores = pd.DataFrame(precos).join(conta_LPA['VL_CONTA'], how='outer')

# Transformando o nome de 'VL_CONTA' para 'LPA' na variável indicadores

indicadores.rename({'VL_CONTA':'LPA'}, axis = 1, inplace = True)

# Reune todos os indicadores coletados em uma única tabela chamada 'indicadores'

indicadores['Receita_Liquida'] = conta_receita['VL_CONTA']
indicadores['Ativo_Circulante'] = conta_ativo_circulante['VL_CONTA']
indicadores['Passivo_Circulante'] = conta_passivo_circulante['VL_CONTA']
indicadores['Capital_de_Giro'] = conta_ativo_circulante['VL_CONTA'] - conta_passivo_circulante['VL_CONTA']
indicadores['patrimonio_liquido'] = conta_patrimonio_liquido['VL_CONTA']
indicadores['lucro operacional'] = conta_lucro_operacional['VL_CONTA']
indicadores['Lucro_Líquido'] = conta_lucro_liquido['VL_CONTA']
indicadores['Dividendos'] = conta_dividendos['VL_CONTA']
indicadores['Divida_Líquida'] = conta_passivo_nao_circulante['VL_CONTA'] + conta_passivo_circulante['VL_CONTA'] - conta_caixa_e_equivalentes['VL_CONTA']
indicadores['indice_endividamento'] = (conta_passivo_nao_circulante['VL_CONTA'] + conta_passivo_circulante['VL_CONTA']) / conta_patrimonio_liquido['VL_CONTA']
indicadores['Margem_Líquida'] = (conta_lucro_liquido['VL_CONTA'] / conta_receita['VL_CONTA'])*100
indicadores['ROE'] = (conta_lucro_operacional['VL_CONTA']/conta_patrimonio_liquido['VL_CONTA'])*100
indicadores['PL'] = indicadores['Close'] / indicadores['LPA']
indicadores['selic']  = dados_anuais_acumulados['selic']
indicadores['cambio'] = dados_anuais_acumulados['cambio']
indicadores['ipca'] = dados_anuais_acumulados['ipca']
indicadores['icc'] = dados_anuais_acumulados['icc']
indicadores['pib'] = dados_anuais_acumulados['PIB']
indicadores['balanca_comercial'] = dados_anuais_acumulados['balanca_comercial']

# Encontra Celulas vazias e elimina a linha inteira na qual essa célula está inserida

indicadores.dropna(inplace = True)
if not os.path.exists('/content/drive/MyDrive/Colab Notebooks/Dados/indicadores_anuais'):
    os.makedirs(f'/content/drive/MyDrive/Colab Notebooks/Dados/indicadores_anuais')
indicadores['Data'] = indicadores.index # insere a coluna data no DataFrame indicadores
# Obter a lista de colunas
cols = indicadores.columns.tolist()

# Mover a coluna 'Data' para a primeira posição
cols.insert(0, cols.pop(cols.index('Data')))

# Reordenar o DataFrame
indicadores = indicadores[cols]

# Transforma a coluna Data no tipo DataTime
indicadores['Data'] = pd.to_datetime(indicadores['Data'])

indicadores.to_csv(f'/content/drive/MyDrive/Colab Notebooks/Dados/indicadores_anuais/indicadores', index = False)

