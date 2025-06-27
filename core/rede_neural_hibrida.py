import pandas as pd
import numpy as np
import requests
import json
from core.db_loader import load_demonstracoes_completas_from_db

def montar_features_para_o_dia(precos: pd.DataFrame, ticker: str, data_dia: pd.Timestamp) -> list:
    dados = precos[precos.index <= data_dia].copy()
    if len(dados) < 50:
        return []
    
    dados['Return'] = dados[ticker].pct_change()
    dados['Volatility'] = dados['Return'].rolling(window=10).std()
    dados['SMA_10'] = dados[ticker].rolling(window=10).mean()
    dados['SMA_50'] = dados[ticker].rolling(window=50).mean()

    if data_dia not in dados.index:
        return []

    linha_dia = dados.loc[data_dia]

    fundamentos = load_demonstracoes_completas_from_db(ticker)
    fundamentos['Data'] = pd.to_datetime(fundamentos['Data'])
    fundamentos.set_index('Data', inplace=True)
    fundamentos.sort_index(inplace=True)
    fundamentos = fundamentos.ffill().loc[:data_dia]
    if fundamentos.empty:
        return []

    ult_fundamentais = fundamentos.iloc[-1]

    features = [
        linha_dia['Return'],
        linha_dia['Volatility'],
        linha_dia['SMA_10'],
        linha_dia['SMA_50'],
    ] + list(ult_fundamentais.values)

    return features

def prever_movimento_acao(precos: pd.DataFrame, ticker: str, data_dia: pd.Timestamp) -> bool:
    try:
