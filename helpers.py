"""helpers.py
~~~~~~~~~~~~~
Funções utilitárias simples compartilhadas entre os módulos.

Funções públicas
----------------
- get_logo_url(ticker)
- obter_setor_da_empresa(ticker, setores_df)
- determinar_lideres(df_scores)
- formatar_real(valor)
"""

from __future__ import annotations

import pandas as pd

# ---------------------------------------------------------------------------
# URL do logotipo ------------------------------------------------------------
# ---------------------------------------------------------------------------

def get_logo_url(ticker: str) -> str:
    """
    Retorna a URL do logotipo PNG de um ticker B3
    baseado no repositório "thefintz/icones-b3".
    """
    tk = ticker.replace('.SA', '').upper()
    return f"https://raw.githubusercontent.com/thefintz/icones-b3/main/icones/{tk}.png"

# ---------------------------------------------------------------------------
# Setor da empresa -----------------------------------------------------------
# ---------------------------------------------------------------------------

def obter_setor_da_empresa(ticker: str, setores_df: pd.DataFrame) -> str:
    """Retorna o setor de *ticker* ou 'Setor Desconhecido' se não achar."""
    setor = setores_df.loc[setores_df['ticker'] == ticker, 'SETOR']
    return setor.iloc[0] if not setor.empty else 'Setor Desconhecido'

# ---------------------------------------------------------------------------
# Líder anual ----------------------------------------------------------------
# ---------------------------------------------------------------------------

def determinar_lideres(df_scores: pd.DataFrame) -> pd.DataFrame:
    """Seleciona, por ano, a empresa com maior *Score_Ajustado*."""
    return df_scores.loc[df_scores.groupby('Ano')['Score_Ajustado'].idxmax()]

# ---------------------------------------------------------------------------
# Formatação de moeda --------------------------------------------------------
# ---------------------------------------------------------------------------

def formatar_real(valor: float | int | None) -> str:
    """Formata número como "R$ 1.234,56" ou avisa se indisponível."""
    if valor is None or pd.isna(valor):
        return 'Valor indisponível'
    return f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

# ---------------------------------------------------------------------------
__all__ = [
    'get_logo_url',
    'obter_setor_da_empresa',
    'determinar_lideres',
    'formatar_real',
]
