import pandas as pd
import yfinance as yf
from datetime import datetime
from bcb import sgs


# Nesse ponto, os arquivos criados serão puxados para as variáveis dre, bpa, bpp em um formato de DataFrame, utilizando a biblioteca 'Pandas'
Ultimo_ano = '2024'
dre = pd.read_csv(f'/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat/con_cia_aberta_DRE_2010_{Ultimo_ano}')
bpa = pd.read_csv(f'/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat/con_cia_aberta_BPA_2010_{Ultimo_ano}')
bpp = pd.read_csv(f'/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat/con_cia_aberta_BPP_2010_{Ultimo_ano}')
dfc = pd.read_csv(f'/content/drive/MyDrive/Colab Notebooks/Dados/dados_concat/con_cia_aberta_DFC_2010_{Ultimo_ano}')

# Começa aqui o processo de filtragem das informações contidas em dre, bpa, bpp e dfc. Sendo deixado no arquivo apenas o último balanço presente no DataFrame

dre = dre[dre['ORDEM_EXERC'] == "ÚLTIMO"]
bpa = bpa[bpa['ORDEM_EXERC'] == "ÚLTIMO"]
bpp = bpp[bpp['ORDEM_EXERC'] == "ÚLTIMO"]
dfc = dfc[dfc['ORDEM_EXERC'] == "ÚLTIMO"]

# Outra filtragem. Desssa vez, no arquivo chamado 'empresas', puxa apenas as colunas 'DENOM_CIA' = nome das empresas e 
# 'CD_CVM' = código referente a essa empresa, eliminando também nomes duplicados, já que o mesmo nome da empresa aparece varias vezes na DRE

empresas = dre[['DENOM_CIA', 'CD_CVM']].drop_duplicates().set_index('CD_CVM')

# Função para obter o ticker ON com base no código CVM
def obter_ticker(codigo_cvm):
    # Este exemplo considera que o ticker base é o código CVM com número final '3' para ações ON
    ticker_base = str(codigo_cvm)[:4]
    return f"{ticker_base}3
    
# Função para verificar se a empresa tem dados anteriores a 2023

def tem_dados_anteriores_a_2023(df):
  if 'DT_REFER' in df.columns:
      df['DT_REFER'] = pd.to_datetime(df['DT_REFER'])
      anos_disponiveis = df['DT_REFER'].dt.year.unique()
      return any(ano < 2023 for ano in anos_disponiveis)
  return False
  
# Criar um loop para processar cada empresa e criar as subpastas
for codigo_cvm, empresa_row in empresas.iterrows():
    # Obter o ticker ON para a empresa
    ticker = obter_ticker(codigo_cvm)

    # Obter o nome da empresa
    nome_empresa = empresa_row['DENOM_CIA'].replace(' ', '_')  # Substituir espaços por underscores

     # Criar a pasta principal da empresa nomeada como nome_da_empresa_ticker
    pasta_empresa = f'/content/drive/MyDrive/Colab Notebooks/Dados/demo_financeiras/{nome_empresa}_{ticker}'
    os.makedirs(pasta_empresa, exist_ok=True)

    # Criar subpastas DRE, BPA, BPP e DFC dentro da pasta principal da empresa
    pasta_dre = f'{pasta_empresa}/{nome_empresa}_{ticker}_DRE'
    pasta_bpa = f'{pasta_empresa}/{nome_empresa}_{ticker}_BPA'
    pasta_bpp = f'{pasta_empresa}/{nome_empresa}_{ticker}_BPP'
    pasta_dfc = f'{pasta_empresa}/{nome_empresa}_{ticker}_DFC'

    os.makedirs(pasta_dre, exist_ok=True)
    os.makedirs(pasta_bpa, exist_ok=True)
    os.makedirs(pasta_bpp, exist_ok=True)
    os.makedirs(pasta_dfc, exist_ok=True)

# Filtra apenas a empresa de código xxxxx que se trata da empresa yyyyyy exibindo em DataTable apenas algumas colunas relevantes da DRE dessa empresa

    empresa  = dre[dre['CD_CVM'] == CD_CVM]
    empresa2 = bpa[bpa['CD_CVM'] == CD_CVM]
    empresa3 = bpp[bpp['CD_CVM'] == CD_CVM]
    empresa4 = dfc[dfc['CD_CVM'] == CD_CVM]

    # Verificar se a empresa tem dados anteriores a 2023
    if (tem_dados_anteriores_a_2023(empresa_dre) or 
        tem_dados_anteriores_a_2023(empresa_bpa) or 
        tem_dados_anteriores_a_2023(empresa_bpp)):
        
        # Salvar os DataFrames filtrados nas subpastas correspondentes
        empresa_dre.to_csv(f'{pasta_dre}/DRE_{ticker}.csv', index=False)
        empresa_bpa.to_csv(f'{pasta_bpa}/BPA_{ticker}.csv', index=False)
        empresa_bpp.to_csv(f'{pasta_bpp}/BPP_{ticker}.csv', index=False)
        empresa_dfc.to_csv(f'{pasta_dfc}/DFC_{ticker}.csv', index=False)

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
    
    prices = yf.download(ticker, start = '2010-01-01', end = '2023-12-31')['Close']
    
    # Utiliza a função "resample" para obter os preços apenas do último dia do ano
    
    df_resample = prices.resample('A').last()
    precos = df_resample
    
    # Criando o DataFrame "indicadores" que será usado pelo Dashboard
    
    indicadores = pd.DataFrame() # Cria um DataFrame vazio, sem colunas e sem linhas
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
    
    # Encontra células vazias e elimina a linha inteira na qual essa célula está inserida
    
    indicadores.dropna(inplace = True)
    indicadores['Data'] = indicadores.index # insere a coluna data no DataFrame indicadores
    
    # Obter a lista de colunas
    cols = indicadores.columns.tolist()
    
    # Mover a coluna 'Data' para a primeira posição
    cols.insert(0, cols.pop(cols.index('Data')))
    
    # Reordenar o DataFrame
    indicadores = indicadores[cols]
    
    # Transforma a coluna Data no tipo DataTime
    indicadores['Data'] = pd.to_datetime(indicadores['Data'])
    
    indicadores.to_csv(f'/content/drive/MyDrive/Colab Notebooks/Dados/indicadores_anuais/indicadores_nome_da_empresa_ticker', index = False)
    
