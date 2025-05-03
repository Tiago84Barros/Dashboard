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

import pandas as pd

# ---------------------------------------------------------------------------
# URL do logotipo ------------------------------------------------------------
# ---------------------------------------------------------------------------

def get_logo_url(ticker):
    """
    Retorna a URL do logotipo PNG de um ticker B3
    baseado no repositório "thefintz/icones-b3".
    """
    tk = ticker.replace('.SA', '').upper()
    return f"https://raw.githubusercontent.com/thefintz/icones-b3/main/icones/{tk}.png"

# ---------------------------------------------------------------------------
# Setor da empresa -----------------------------------------------------------
# ---------------------------------------------------------------------------

def obter_setor_da_empresa(ticker, setores_df):
    """Retorna o setor do ticker ou 'Setor Desconhecido' se não achar."""
    setor = setores_df.loc[setores_df['ticker'] == ticker, 'SETOR']
    return setor.iloc[0] if not setor.empty else 'Setor Desconhecido'

# ---------------------------------------------------------------------------
# Líder anual ----------------------------------------------------------------
# ---------------------------------------------------------------------------

def determinar_lideres(df_scores):
    """Seleciona, por ano, a empresa com maior Score_Ajustado."""
    idx = df_scores.groupby('Ano')['Score_Ajustado'].idxmax()
    return df_scores.loc[idx]

# ---------------------------------------------------------------------------
# Formatação de moeda --------------------------------------------------------
# ---------------------------------------------------------------------------

def formatar_real(valor):
    """Formata número como 'R$ 1.234,56' ou indica indisponível."""
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return 'Valor indisponível'
    try:
        return f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
    except Exception:
        return 'Valor indisponível'

__all__ = [
    'get_logo_url',
    'obter_setor_da_empresa',
    'determinar_lideres',
    'formatar_real',
]
