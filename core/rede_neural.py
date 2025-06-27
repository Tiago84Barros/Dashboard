from sklearn.neural_network import MLPRegressor
from sklearn.preprocessing import MinMaxScaler
import pandas as pd
import numpy as np


def prever_movimento_acao(dados: pd.DataFrame) -> bool:
    """
    Treina uma rede neural com dados históricos (menos o último mês)
    e retorna True se a previsão para o mês seguinte for de alta.

    Parâmetros:
        dados: DataFrame com as colunas ['Date', 'Close']

    Retorno:
        bool: True se previsão indicar alta, False se queda ou estabilidade
    """
    if len(dados) < 13:
        return True  # fallback: assume compra se dados insuficientes

    dados = dados[['Date', 'Close']].copy()
    dados['Date'] = pd.to_datetime(dados['Date'])
    dados = dados.sort_values('Date')

    # Retira o último ponto (mês a ser previsto)
    dados_treino = dados.iloc[:-1]
    y_real = dados.iloc[-1]['Close']

    # Feature simples baseada no tempo
    dados_treino['Indice'] = range(len(dados_treino))
    X = dados_treino[['Indice']]
    y = dados_treino['Close']

    scaler = MinMaxScaler()
    X_scaled = scaler.fit_transform(X)

    model = MLPRegressor(hidden_layer_sizes=(50,), max_iter=1000, random_state=42)
    model.fit(X_scaled, y)

    # Previsão para o próximo mês
    X_next = scaler.transform([[len(dados)]])
    y_pred = model.predict(X_next)[0]

    return y_pred > y_real
