import pandas as pd
import numpy as np
import requests
import json
from core.db_loader import load_demonstracoes_completas_from_db

def montar_features_para_o_dia(precos: pd.DataFrame, ticker: str, data_dia: pd.Timestamp) -> list:
    try:
        dados = precos.copy()
        if data_dia not in dados.index or len(dados) < 50:
            print(f"[DEBUG] Dados insuficientes ou data não encontrada para {ticker} em {data_dia}")
            return []

        dados['Return'] = dados[ticker].pct_change()
        dados['Volatility'] = dados['Return'].rolling(window=10).std()
        dados['SMA_10'] = dados[ticker].rolling(window=10).mean()
        dados['SMA_50'] = dados[ticker].rolling(window=50).mean()

        linha_dia = dados.loc[data_dia]
        if linha_dia.isnull().any():
            print(f"[DEBUG] Indicadores técnicos incompletos para {ticker} em {data_dia}")
            return []

        # Carregar dados fundamentalistas mais recentes
        fundamentos = load_demonstracoes_completas_from_db(ticker)
        fundamentos['Data'] = pd.to_datetime(fundamentos['Data'], errors='coerce')
        fundamentos.set_index('Data', inplace=True)
        fundamentos = fundamentos.sort_index().ffill().loc[:data_dia]

        if fundamentos.empty:
            print(f"[DEBUG] Sem fundamentos disponíveis para {ticker} em {data_dia}")
            return []

        ult_fundamentais = fundamentos.iloc[-1].select_dtypes(include=[np.number])
        if ult_fundamentais.isnull().any():
            print(f"[DEBUG] Fundamentos incompletos para {ticker} em {data_dia}")
            return []

        features = [
            linha_dia['Return'],
            linha_dia['Volatility'],
            linha_dia['SMA_10'],
            linha_dia['SMA_50'],
        ] + list(ult_fundamentais.values)

        return features
    except Exception as e:
        print(f"[ERROR] montar_features_para_o_dia: {e}")
        return []

def prever_movimento_acao(precos: pd.DataFrame, ticker: str, data_dia: pd.Timestamp) -> bool:
    try:
        features = montar_features_para_o_dia(precos, ticker, data_dia)
        if not features:
            return False

        payload = {
            "ticker": ticker,
            "features": features
        }

        response = requests.post(
            "https://api-rna-177898037259.us-central1.run.app/predict",
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload),
            timeout=10
        )
        response.raise_for_status()
        result = response.json()

        return result.get("buy_signal", False)

    except Exception as e:
        print(f"[ERROR] prever_movimento_acao para {ticker} em {data_dia}: {e}")
        return False

def melhor_dia_compra_no_mes(precos: pd.DataFrame, ticker: str, ano: int, mes: int) -> pd.Timestamp | None:
    try:
        dias_do_mes = pd.date_range(start=f"{ano}-{mes:02d}-01", end=f"{ano}-{mes:02d}-28", freq='B')
        dias_validos = [d for d in dias_do_mes if d in precos.index]

        for dia in dias_validos:
            if prever_movimento_acao(precos, ticker, dia):
                print(f"[DEBUG] {ticker} - Buy signal encontrado em {dia.date()}")
                return dia

        print(f"[DEBUG] {ticker} - Nenhum sinal de compra em {mes}/{ano}")
        return None
    except Exception as e:
        print(f"[ERROR] melhor_dia_compra_no_mes: {e}")
        return None
