"""helpers.py
~~~~~~~~~~~~~
Funções utilitárias compartilhadas entre módulos do Dashboard Financeiro.
"""

from __future__ import annotations
import pandas as pd
import yfinance as yf

# ---------------------------------------------------------------------------
# Obtenção de setor ----------------------------------------------------------
# ---------------------------------------------------------------------------

def obter_setor_da_empresa(ticker: str, setores_df: pd.DataFrame) -> str:
    """Retorna o setor de *ticker* ou 'Setor Desconhecido' se não encontrado."""
    setor = setores_df.loc[setores_df['ticker'] == ticker, 'SETOR']
    return setor.iloc[0] if not setor.empty else 'Setor Desconhecido'

# ---------------------------------------------------------------------------
# Determinação de líderes ---------------------------------------------------
# ---------------------------------------------------------------------------

def determinar_lideres(df_scores: pd.DataFrame) -> pd.DataFrame:
    """Seleciona, por ano, a empresa com maior Score_Ajustado."""
    idx = df_scores.groupby('Ano')['Score_Ajustado'].idxmax()
    return df_scores.loc[idx]

# ---------------------------------------------------------------------------
# Formatação de moeda --------------------------------------------------------
# ---------------------------------------------------------------------------

def formatar_real(valor: float | int | None) -> str:
    """Formata número como 'R$ 1.234,56' ou avisa se indisponível."""
    if valor is None or pd.isna(valor):
        return 'Valor indisponível'
    return f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

# ---------------------------------------------------------------------------
# URL do logotipo ------------------------------------------------------------
# ---------------------------------------------------------------------------

def get_logo_url(ticker: str) -> str:
    """Retorna URL PNG do logotipo a partir do repositório *icones-b3* no GitHub."""
    tk = ticker.replace('.SA', '').upper()
    return f"https://raw.githubusercontent.com/thefintz/icones-b3/main/icones/{tk}.png"

# ---------------------------------------------------------------------------
# Informações da empresa via yfinance ---------------------------------------
# ---------------------------------------------------------------------------

def get_company_info(ticker: str) -> tuple[str|None, str|None]:
    """
    Retorna (nome longo, website) da empresa via yfinance; adiciona sufixo .SA.
    """
    try:
        if not ticker.endswith('.SA'):
            ticker = ticker + '.SA'
        company = yf.Ticker(ticker)
        info = company.info
        name = info.get('longName') or info.get('shortName')
        site = info.get('website')
        return name, site
    except Exception:
        return None, None

__all__ = [
    'obter_setor_da_empresa',
    'determinar_lideres',
    'formatar_real',
    'get_logo_url',
    'get_company_info',
]
