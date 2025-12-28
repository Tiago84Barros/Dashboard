"""
page/empresa_view.py
~~~~~~~~~~~~~~~~~~~
Renderização de detalhes por empresa (ticker) na seção Básica.

Objetivo:
- Ler demonstrações financeiras do Supabase (schema cvm).
- Ser robusto a variações de schema/colunas (auto-detecção).
- Evitar erros de cache do Streamlit com SQLAlchemy Engine.

Tabelas disponíveis (conforme seu Supabase):
- cvm.demonstracoes_financeiras
- cvm.demonstracoes_financeiras_dfp
- cvm.demonstracoes_financeiras_tri
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

DEFAULT_TABLE = "cvm.demonstracoes_financeiras_dfp"  # anual
# DEFAULT_TABLE = "cvm.demonstracoes_financeiras_tri"  # trimestral


@st.cache_resource(show_spinner=False)
def _engine() -> Engine:
    return get_engine()


def _norm_sa(ticker: str) -> str:
    t = (ticker or "").strip().upper()
    if t.endswith(".SA"):
        t = t[:-3]
    return t


@dataclass(frozen=True)
class EmpresaViewConfig:
    table: str = DEFAULT_TABLE
    max_rows: int = 20_000


# ---------------------------------------------------------------------
# SCHEMA INTROSPECTION
# ---------------------------------------------------------------------

def _split_schema_table(fullname: str) -> tuple[str, str]:
    if "." in fullname:
        schema, table = fullname.split(".", 1)
        return schema, table
    return "public", fullname


@st.cache_data(show_spinner=False, ttl=24 * 60 * 60)
def get_table_columns(full_table_name: str) -> list[str]:
    """
    Busca colunas reais da tabela no Supabase.
    Cache de 24h (schema não muda toda hora).
    """
    schema, table = _split_schema_table(full_table_name)
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name   = :table
        ORDER BY ordinal_position
    """
    df = pd.read_sql(text(sql), con=_engine(), params={"schema": schema, "table": table})
    return df["column_name"].tolist() if not df.empty else []


def _first_existing(cols: list[str], candidates: list[str]) -> Optional[str]:
    for c in candidates:
        if c in cols:
            return c
    return None


# ---------------------------------------------------------------------
# DATA LOAD
# ---------------------------------------------------------------------

@st.cache_data(show_spinner=False, ttl=60 * 60)
def load_raw_table(
    ticker: str,
    *,
    table: str,
    max_rows: int,
) -> pd.DataFrame:
    """
    Carrega registros do ticker na tabela informada, sem assumir colunas além de 'ticker' quando existir.
    Faz ORDER BY pelo melhor campo de data/período que existir.
    """
    t = _norm_sa(ticker)
    cols = get_table_columns(table)
    if not cols:
        return pd.DataFrame()

    # campos prováveis de data/período
    order_col = _first_existing(
        cols,
        ["data", "dt_ref", "data_referencia", "periodo", "ano_trimestre", "ano", "ref"],
    )

    # se não existir coluna ticker, não há como filtrar por ticker (evita erro)
    if "ticker" not in cols:
        return pd.DataFrame()

    sql = f"SELECT * FROM {table} WHERE ticker = :ticker"
    if order_col:
        sql += f" ORDER BY {order_col} DESC"
    if max_rows:
        sql += f" LIMIT {int(max_rows)}"

    df = pd.read_sql(text(sql), con=_engine(), params={"ticker": t})
    return df if df is not None else pd.DataFrame()


def _build_dre_from_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    Constrói DRE pivot a partir de um formato LONGO:
      conta/descricao + valor + periodo/data + demonstracao(tipo)
    """
    cols = df.columns.tolist()

    periodo = _first_existing(cols, ["periodo", "data", "dt_ref", "data_referencia", "ano_trimestre", "ano"])
    conta = _first_existing(cols, ["conta", "descricao", "item", "linha"])
    valor = _first_existing(cols, ["valor", "value", "vlr"])
    demo = _first_existing(cols, ["demonstracao", "tipo_demonstracao", "demonstrativo", "relatorio", "tipo"])

    if not (periodo and conta and valor):
        return pd.DataFrame()

    work = df.copy()
    work[valor] = pd.to_numeric(work[valor], errors="coerce")

    # filtra DRE se houver coluna de demonstrativo/tipo
    if demo:
        mask = work[demo].astype(str).str.upper().str.contains("DRE", na=False)
        if mask.any():
            work = work.loc[mask]

    if work.empty:
        return pd.DataFrame()

    pivot = work.pivot_table(index=conta, columns=periodo, values=valor, aggfunc="sum")

    # ordenar colunas (mais recente primeiro) se possível
    try:
        pivot = pivot.reindex(sorted(pivot.columns, reverse=True), axis=1)
    except Exception:
        pass

    return pivot


def _build_dre_from_wide(df: pd.DataFrame) -> pd.DataFrame:
    """
    Constrói visão wide (index=data/período; colunas=métricas) sem depender de nomes exatos.
    Mantém apenas colunas numéricas mais relevantes (exclui chaves/metadata).
    """
    if df.empty:
        return pd.DataFrame()

    cols = df.columns.tolist()
    idx = _first_existing(cols, ["data", "periodo", "dt_ref", "data_referencia", "ano_trimestre", "ano"])
    work = df.copy()

    if idx and idx in work.columns:
        # tenta converter datas
        work[idx] = pd.to_datetime(work[idx], errors="ignore")
        work = work.sort_values(idx)
        work = work.set_index(idx)

    # remove colunas claramente não-métricas
    drop_like = {"id", "created_at", "updated_at", "fetched_at", "cnpj", "nome", "razao", "ticker"}
    keep = [c for c in work.columns if c not in drop_like]

    # tenta manter numéricas
    out = work[keep].copy()
    for c in out.columns:
        out[c] = pd.to_numeric(out[c], errors="ignore")

    # se sobrar muita coisa, mantém só as numéricas
    num = out.select_dtypes(include="number")
    if not num.empty:
        out = num

    return out


@st.cache_data(show_spinner=False, ttl=60 * 60)
def load_dre(ticker: str, *, table: str, max_rows: int) -> pd.DataFrame:
    """
    Decide automaticamente se a tabela é LONGA ou WIDE e monta a visão.
    """
    raw = load_raw_table(ticker, table=table, max_rows=max_rows)
    if raw.empty:
        return pd.DataFrame()

    # Heurística: se existir (conta|descricao) + (valor) => LONG
    cols = raw.columns.tolist()
    has_conta = any(c in cols for c in ["conta", "descricao", "item", "linha"])
    has_valor = any(c in cols for c in ["valor", "value", "vlr"])

    if has_conta and has_valor:
        dre = _build_dre_from_long(raw)
        if not dre.empty:
            return dre

    # fallback: wide
    return _build_dre_from_wide(raw)


# ---------------------------------------------------------------------
# RENDER
# ---------------------------------------------------------------------

def render_empresa_view(ticker: str, *, config: Optional[EmpresaViewConfig] = None) -> None:
    cfg = config or EmpresaViewConfig()
    t = _norm_sa(ticker)

    st.subheader(f"Empresa: {t}")
    st.caption(f"Tabela fonte: `{cfg.table}`")

    # diagnóstico rápido das colunas
    cols = get_table_columns(cfg.table)
    if not cols:
        st.error(
            f"Não consegui ler o schema da tabela `{cfg.table}`. "
            "Verifique permissões e se ela existe no Supabase."
        )
        return

    with st.expander("Diagnóstico: colunas detectadas"):
        st.write(cols)

    try:
        with st.spinner("Carregando demonstrações..."):
            dre = load_dre(t, table=cfg.table, max_rows=cfg.max_rows)

        if dre.empty:
            st.warning(
                "Não encontrei dados para esse ticker na tabela selecionada.\n\n"
                "Confirme se o ticker existe na tabela e se o seu pipeline CVM populou DFP/TRI."
            )
            return

        st.markdown("### Demonstrações (visão gerada automaticamente)")
        st.dataframe(dre, use_container_width=True)

    except Exception as e:
        st.error("Falha inesperada ao renderizar os detalhes da empresa.")
        st.exception(e)
