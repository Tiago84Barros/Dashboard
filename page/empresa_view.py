"""
page/empresa_view.py
~~~~~~~~~~~~~~~~~~~
Visão de empresa (detalhes por ticker) e carregamento de DRE/DFs.

Correções:
1) Remove o erro "multiple values for argument 'engine'" ao NÃO passar engine na assinatura.
2) Remove o erro "UnhashableParamError: Cannot hash argument 'engine'" ao não cachear Engine.

Estratégia correta Streamlit:
- Engine: @st.cache_resource
- Dados: @st.cache_data (somente parâmetros hashable)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd
import streamlit as st
from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.db_supabase import get_engine

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------

_DEFAULT_TABLE = "demonstracoes_financeiras"  # ajuste se necessário


@st.cache_resource(show_spinner=False)
def _engine() -> Engine:
    """Engine singleton cacheado (não entra em @cache_data)."""
    return get_engine()


def _norm_sa(ticker: str) -> str:
    t = (ticker or "").strip().upper()
    if t.endswith(".SA"):
        t = t[:-3]
    return t


@dataclass(frozen=True)
class EmpresaViewConfig:
    table: str = _DEFAULT_TABLE
    max_rows: int = 20_000


def _sql_demonstracoes(table: str) -> str:
    """
    Ajuste este SELECT para o seu schema real, se necessário.
    O COALESCE tenta cobrir variações de nomes de colunas.
    """
    return f"""
        SELECT
            ticker,
            COALESCE(periodo, dt_ref, data_referencia, ano_trimestre, ano) AS periodo,
            COALESCE(demonstracao, tipo_demonstracao, demonstrativo, relatorio) AS demonstracao,
            COALESCE(conta, descricao, item, linha) AS conta,
            valor
        FROM {table}
        WHERE ticker = :ticker
        ORDER BY
            COALESCE(periodo, dt_ref, data_referencia, ano_trimestre, ano) DESC
    """


def _safe_to_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


# ---------------------------------------------------------------------
# DATA LOADING (CACHE SAFE)
# ---------------------------------------------------------------------

@st.cache_data(show_spinner=False, ttl=60 * 60)
def load_demonstracoes_financeiras(
    ticker: str,
    *,
    table: str = _DEFAULT_TABLE,
    max_rows: int = 20_000,
) -> pd.DataFrame:
    """
    Carrega demonstrações financeiras brutas do banco.

    OBS:
    - Não recebe 'engine' como argumento (para evitar UnhashableParamError).
    - Usa _engine() internamente (cache_resource).
    """
    t = _norm_sa(ticker)
    eng = _engine()

    sql = _sql_demonstracoes(table)
    df = pd.read_sql(text(sql), con=eng, params={"ticker": t})

    if df is None or df.empty:
        return pd.DataFrame()

    if "valor" in df.columns:
        df["valor"] = _safe_to_numeric(df["valor"])

    for col in ("ticker", "periodo", "demonstracao", "conta"):
        if col in df.columns:
            df[col] = df[col].astype(str)

    if len(df) > max_rows:
        df = df.head(max_rows)

    return df


@st.cache_data(show_spinner=False, ttl=60 * 60)
def load_dre(ticker: str, *, table: str = _DEFAULT_TABLE) -> pd.DataFrame:
    """
    Retorna DRE pivotada (contas x períodos).
    """
    df = load_demonstracoes_financeiras(
        _norm_sa(ticker),
        table=table,
    )

    if df.empty:
        return pd.DataFrame()

    # filtra DRE (tolerante)
    dre_mask = df["demonstracao"].str.upper().str.contains("DRE", na=False)
    dre = df.loc[dre_mask].copy()
    if dre.empty:
        dre = df.copy()

    pivot = (
        dre.pivot_table(
            index="conta",
            columns="periodo",
            values="valor",
            aggfunc="sum",
        )
        .sort_index()
    )

    # mais recente primeiro
    pivot = pivot.reindex(sorted(pivot.columns, reverse=True), axis=1)

    return pivot


# ---------------------------------------------------------------------
# RENDER
# ---------------------------------------------------------------------

def render_empresa_view(ticker: str, *, config: Optional[EmpresaViewConfig] = None) -> None:
    cfg = config or EmpresaViewConfig()
    t = _norm_sa(ticker)

    st.subheader(f"Empresa: {t}")

    with st.spinner("Carregando demonstrações financeiras..."):
        dre = load_dre(t, table=cfg.table)

    if dre.empty:
        st.warning(
            "Não encontrei demonstrações financeiras para esse ticker no banco.\n\n"
            f"Verifique se a tabela `{cfg.table}` está populada e se o ticker está normalizado."
        )
        return

    st.markdown("### DRE (por período)")
    st.dataframe(dre, use_container_width=True)
