"""helpers.py
~~~~~~~~~~~~~
Funções utilitárias compartilhadas entre módulos do Dashboard Financeiro.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Tuple

import pandas as pd
import yfinance as yf

# ---------------------------------------------------------------------------
# Obtenção de setor ----------------------------------------------------------
# ---------------------------------------------------------------------------


def obter_setor_da_empresa(ticker: str, setores_df: pd.DataFrame) -> str:
    """Retorna o setor de *ticker* ou 'Setor Desconhecido' se não encontrado."""
    setor = setores_df.loc[setores_df["ticker"] == ticker, "SETOR"]
    return setor.iloc[0] if not setor.empty else "Setor Desconhecido"


# ---------------------------------------------------------------------------
# Determinação de líderes ----------------------------------------------------
# ---------------------------------------------------------------------------


def determinar_lideres(
    df_scores: pd.DataFrame,
    metricas: Iterable[str] | None = None,
) -> pd.DataFrame:
    """
    Retorna um DataFrame com as empresas líderes (maior valor) por **ano**
    para cada métrica da lista *metricas*.

    Parâmetros
    ----------
    df_scores : DataFrame
        Deve conter uma coluna 'Ano' e as colunas numéricas das métricas.
    metricas : lista[str] ou None
        Métricas a comparar. Se None, usa ['Score_Ajustado'].

    Exemplo
    -------
    >>> lideres = determinar_lideres(df, ["Score_Ajustado", "ROE"])
    """
    if metricas is None:
        metricas = ["Score_Ajustado"]

    frames = []
    for met in metricas:
        if met not in df_scores.columns:
            continue
        idx = df_scores.groupby("Ano")[met].idxmax()
        frame = df_scores.loc[idx].copy()
        frame["Metrica_Lideranca"] = met
        frames.append(frame)

    if not frames:
        return pd.DataFrame()

    # Concatena e remove duplicatas (caso a mesma empresa lidere várias métricas no mesmo ano)
    df_all = pd.concat(frames).sort_values(["Ano", "Metrica_Lideranca"])
    return df_all.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Formatação de moeda --------------------------------------------------------
# ---------------------------------------------------------------------------


def formatar_real(valor: float | int | None) -> str:
    """Formata número como 'R$ 1.234,56' ou avisa se indisponível."""
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return "Valor indisponível"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


# ---------------------------------------------------------------------------
# URL do logotipo ------------------------------------------------------------
# ---------------------------------------------------------------------------


def get_logo_url(ticker: str) -> str:
    """
    Retorna a URL PNG do logotipo da empresa.

    Os ícones são servidos a partir do repositório público *thefintz/icones-b3*,
    onde cada arquivo segue o padrão `<TICKER>.png`.
    """
    tk = ticker.replace(".SA", "").upper()
    return f"https://raw.githubusercontent.com/thefintz/icones-b3/main/icones/{tk}.png"


# ---------------------------------------------------------------------------
# Informações da empresa via yfinance ---------------------------------------
# ---------------------------------------------------------------------------


def get_company_info(ticker: str) -> Tuple[str | None, str | None]:
    """
    Retorna *(nome completo, website)* da empresa usando **yfinance**.

    Se houver erro de conexão ou o ticker não existir, devolve (None, None).
    """
    try:
        if not ticker.endswith(".SA"):
            ticker = ticker + ".SA"
        company = yf.Ticker(ticker)
        info = company.info
        name = info.get("longName") or info.get("shortName")
        site = info.get("website")
        return name, site
    except Exception:
        return None, None


# ---------------------------------------------------------------------------
# __all__ --------------------------------------------------------------------
# ---------------------------------------------------------------------------

__all__: List[str] = [
    "obter_setor_da_empresa",
    "determinar_lideres",
    "formatar_real",
    "get_logo_url",
    "get_company_info",
]
