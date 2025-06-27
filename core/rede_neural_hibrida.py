# core/rede_neural_hibrida.py

import pandas as pd
import numpy as np
import streamlit as st
import pickle
import os
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from core.db_loader import load_data_from_db, load_demonstracoes_completas_from_db
from core.yf_data import baixar_precos

MODELOS_DIR = "modelos_rna"
os.makedirs(MODELOS_DIR, exist_ok=True)

def _get_model_path(ticker):
    return os.path.join(MODELOS_DIR, f"modelo_{ticker}.pkl")

def _get_scaler_path(ticker):
    return os.path.join(MODELOS_DIR, f"scaler_{ticker}.pkl")

def _preparar_dados(ticker, data_limite):
    precos = baixar_precos([f"{ticker}.SA"], start="2010-01-01")
    
    precos = precos[[ticker]].dropna()
    precos = precos[precos.index < data_limite]
   
    precos['Return'] = precos[ticker].pct_change()
    precos['Volatility'] = precos['Return'].rolling(window=10).std()
    precos['SMA_10'] = precos[ticker].rolling(window=10).mean()
    precos['SMA_50'] = precos[ticker].rolling(window=50).mean()

    fundamentos = load_demonstracoes_completas_from_db(f"{ticker}.SA")
    if fundamentos is None or fundamentos.empty:
        return None, None, None
    fundamentos['Data'] = pd.to_datetime(fundamentos['Data'])
    fundamentos.set_index('Data', inplace=True)
    fundamentos.sort_index(inplace=True)
    fundamentos = fundamentos.resample('B').ffill().reindex(precos.index).dropna()

    df = precos.join(fundamentos, how='inner').dropna()
    st.dataframe(df)
    df['Return_20d'] = df[ticker].shift(-20) / df[ticker] - 1
    df['Tendencia_Label'] = df['Return_20d'].apply(lambda ret: 1 if ret >= 0.05 else (0 if ret <= -0.05 else np.nan))
    df.dropna(subset=['Tendencia_Label'], inplace=True)

    features = ['Close', 'Return', 'Volatility', 'SMA_10', 'SMA_50'] + list(fundamentos.columns)
    X = df[features]
    y = df['Tendencia_Label']

    scaler = MinMaxScaler()
    X_scaled = scaler.fit_transform(X)

    return X_scaled, y.values, scaler

def treinar_modelo_para_ticker(ticker: str, data_limite: pd.Timestamp):
    X, y, scaler = _preparar_dados(ticker, data_limite)
    if X is None:
        return False

    model = Sequential([
        Dense(200, activation='tanh', input_shape=(X.shape[1],)),
        Dense(200, activation='tanh'),
        Dense(1, activation='sigmoid')
    ])
    model.compile(optimizer='nadam', loss='binary_crossentropy', metrics=['accuracy'])
    model.fit(X, y, epochs=900, batch_size=32, verbose=0)

    with open(_get_model_path(ticker), "wb") as f:
        pickle.dump(model.get_weights(), f)
    with open(_get_scaler_path(ticker), "wb") as f:
        pickle.dump(scaler, f)

    return True

def prever_movimento_acao(precos: pd.DataFrame, ticker: str, data_dia: pd.Timestamp) -> bool:
 
    def _get_model_path(ticker):
        return os.path.join("modelos_rna", f"modelo_{ticker}.pkl")

    def _get_scaler_path(ticker):
        return os.path.join("modelos_rna", f"scaler_{ticker}.pkl")

    # Se o modelo ou o scaler não existem, tenta treinar primeiro
    if not os.path.exists(_get_model_path(ticker)) or not os.path.exists(_get_scaler_path(ticker)):
        sucesso = treinar_modelo_para_ticker(ticker, data_dia)
        
    # Carrega os pesos e o scaler
    with open(_get_model_path(ticker), "rb") as f:
        pesos = pickle.load(f)
    with open(_get_scaler_path(ticker), "rb") as f:
        scaler = pickle.load(f)

    # Baixa os preços históricos
    df = baixar_precos([f"{ticker}.SA"], start="2010-01-01")

    df = df[[ticker]].dropna()
    df = df[df.index <= data_dia]
   
    # Corrige o nome da coluna para 'Close'
    df = df.rename(columns={ticker: 'Close'})

    # Calcula os indicadores técnicos
    df['Return'] = df['Close'].pct_change()
    df['Volatility'] = df['Return'].rolling(window=10).std()
    df['SMA_10'] = df['Close'].rolling(window=10).mean()
    df['SMA_50'] = df['Close'].rolling(window=50).mean()

    # Carrega os fundamentos
    fundamentos = load_demonstracoes_completas_from_db(f"{ticker}.SA")

    fundamentos['Data'] = pd.to_datetime(fundamentos['Data'])
    fundamentos.set_index('Data', inplace=True)
    fundamentos.sort_index(inplace=True)
    fundamentos = fundamentos.resample('B').ffill().reindex(df.index).dropna()

    # Faz o join com os fundamentos
    df = df.join(fundamentos, how='inner').dropna()
   
    # Prepara os features
    features = ['Close', 'Return', 'Volatility', 'SMA_10', 'SMA_50'] + list(fundamentos.columns)
    X = df[features]

    # Normaliza
    X_scaled = scaler.transform(X)

    # Monta a arquitetura do modelo para poder carregar os pesos
    model = Sequential([
        Dense(200, activation='tanh', input_shape=(X_scaled.shape[1],)),
        Dense(200, activation='tanh'),
        Dense(1, activation='sigmoid')
    ])
    model.set_weights(pesos)

    # Faz a previsão
    pred = model.predict(X_scaled[-1].reshape(1, -1), verbose=0)[0, 0]

    # Retorna True se a probabilidade de alta for acima de 60%
    return pred > 0.6

# Nova função para prever melhor dia de compra no mês

def melhor_dia_compra_no_mes(precos: pd.DataFrame, ticker: str, ano: int, mes: int) -> pd.Timestamp | None:
    dias_do_mes = pd.date_range(f"{ano}-{mes:02d}-01", periods=31, freq='D')
    dias_do_mes = [d for d in dias_do_mes if d.month == mes and d in precos.index]
    for dia in dias_do_mes:
        if prever_movimento_acao(precos, ticker, dia):
            return dia
    return None
