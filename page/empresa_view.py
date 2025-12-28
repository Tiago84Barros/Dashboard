"""
page/empresa_view.py
~~~~~~~~~~~~~~~~~~~
Renderização da visão de empresa (detalhes por ticker) e carregamento de DRE/DFs.

Correção principal:
- load_demonstracoes_financeiras(..., *, engine=...) -> engine é keyword-only
  evitando: "got multiple values for argument 'engine'".

Dependências esperadas:
- core.db_supabase.get_engine  (SQLAlchemy Engine)
- Streamlit

Atenção:
- Ajuste o nome da tabela em _DEFAULT_TABLE se necessário.
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

# AJUSTE AQUI se a tabela no seu Supabase tiver outro nome.
_DEFAULT_TABLE = "demonstracoes_financeiras"

# Colunas mínimas esperadas na tabela (o módulo tenta se adaptar, mas estas são as mais comuns)
# ticker | periodo (ou dt_ref/ano_trimestre) | conta (ou descricao) | valor | demonstracao (DRE/BP/DFC)
# Se suas colunas forem diferentes, adapte o SELECT em _sql_demonstracoes().


# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------


def _norm_sa(ticker: str) -> str:
    """Normaliza ticker para o padrão sem sufixo .SA e em maiúsculas."""
    t = (ticker or "").strip().upper()
    if t.endswith(".SA"):
        t = t[:-3]
    return t


@st.cache_resource(show_spinner=False)
def _engine() -> Engine:
    """Engine singleton cacheado (ideal para Streamlit Cloud)."""
    return get_engine()


@dataclass(frozen=True)
class EmpresaViewConfig:
    table: str = _DEFAULT_TABLE
    max_rows: int = 20_000  # segurança para não explodir memória em deploy


def _sql_demonstracoes(table: str) -> str:
    """
    SQL flexível: seleciona campos típicos.
    Caso seu schema seja diferente, ajuste aqui.

    Estratégia:
    - tentar cobrir nomes de coluna comuns com aliases padronizados:
      periodo, demonstracao, conta, valor
    """
    # Você pode ajustar os nomes conforme seu banco:
    # Exemplo se você tiver "dt_ref" e "tipo_demonstracao":
    # SELECT ticker, dt_ref as periodo, tipo_demonstracao as demonstracao, conta, valor ...

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
    """Converte com tolerância valores que podem vir como texto."""
    return pd.to_numeric(s, errors="coerce")


# ---------------------------------------------------------------------
# CORE LOADING (AQUI ESTÁ A CORREÇÃO DO ENGINE)
# ---------------------------------------------------------------------


@st.cache_data(show_spinner=False, ttl=60 * 60)  # 1h
def load_demonstracoes_financeiras(
    ticker: str,
    *,
    engine: Engine,
    table: str = _DEFAULT_TABLE,
    max_rows: int = 20_000,
) -> pd.DataFrame:
    """
    Carrega demonstrações financeiras brutas do banco.

    Correção: engine é keyword-only (*, engine=...)
    """
    t = _norm_sa(ticker)

    sql = _sql_demonstracoes(table)
    df = pd.read_sql(text(sql), con=engine, params={"ticker": t})

    if df is None or df.empty:
        return pd.DataFrame()

    # sanitização
    if "valor" in df.columns:
        df["valor"] = _safe_to_numeric(df["valor"])
    for col in ("ticker", "periodo", "demonstracao", "conta"):
        if col in df.columns:
            df[col] = df[col].astype(str)

    # proteção
    if len(df) > max_rows:
        df = df.head(max_rows)

    return df


@st.cache_data(show_spinner=False, ttl=60 * 60)
def load_dre(
    ticker: str,
    *,
    table: str = _DEFAULT_TABLE,
) -> pd.DataFrame:
    """
    Retorna DRE pivotada (contas x períodos), pronta para exibir.

    Importante: engine é passado apenas como keyword (engine=_engine()).
    """
    df = load_demonstracoes_financeiras(
        _norm_sa(ticker),
        engine=_engine(),
        table=table,
    )

    if df.empty:
        return pd.DataFrame()

    # filtra DRE (tolerante a nulos e variações)
    dre_mask = df["demonstracao"].str.upper().str.contains("DRE", na=False)
    dre = df.loc[dre_mask].copy()

    if dre.empty:
        # se não houver a flag "DRE", retorna tudo (melhor do que quebrar)
        dre = df.copy()

    # pivot: linhas=conta, colunas=periodo, valores=valor
    pivot = (
        dre.pivot_table(
            index="conta",
            columns="periodo",
            values="valor",
            aggfunc="sum",
        )
        .sort_index()
    )

    # ordena períodos da direita para a esquerda (mais recente primeiro), se possível
    pivot = pivot.reindex(sorted(pivot.columns, reverse=True), axis=1)

    return pivot


# ---------------------------------------------------------------------
# RENDER
# ---------------------------------------------------------------------


def render_empresa_view(ticker: str, *, config: Optional[EmpresaViewConfig] = None) -> None:
    """
    Render da página de detalhes da empresa.
    Chamado por page/basic.py (exibir_detalhes_empresa).
    """
    cfg = config or EmpresaViewConfig()

    t = _norm_sa(ticker)
    st.subheader(f"Empresa: {t}")

    # Carrega DRE
    with st.spinner("Carregando demonstrações financeiras..."):
        dre = load_dre(t, table=cfg.table)

    if dre.empty:
        st.warning(
            "Não encontrei demonstrações financeiras para esse ticker no banco.\n\n"
            f"Verifique se a tabela `{cfg.table}` está populada e se o ticker está normalizado."
        )
        return

    # Exibição (DRE)
    st.markdown("### DRE (pivot por período)")
    st.dataframe(dre, use_container_width=True)

    # Alguns indicadores rápidos (opcional, sem assumir muito do seu schema)
    st.markdown("### Destaques (se disponíveis)")
    try:
        # heurística por nome de conta
        contas = [c.upper() for c in dre.index.astype(str).tolist()]
        col_mais_recente = dre.columns[0] if len(dre.columns) else None

        def _get_by_contains(keys: list[str]) -> Optional[float]:
            if col_mais_recente is None:
                return None
            for i, nome in enumerate(contas):
                if any(k in nome for k in keys):
                    v = dre.iloc[i][col_mais_recente]
                    if pd.notna(v):
                        return float(v)
            return None

        receita = _get_by_contains(["RECEITA", "VENDA", "FATUR"])
        ebitda = _get_by_contains(["EBITDA"])
        lucro = _get_by_contains(["LUCRO", "RESULTADO"])

        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Receita (últ. período)", f"{receita:,.0f}" if receita is not None else "—")
        with c2:
            st.metric("EBITDA (últ. período)", f"{ebitda:,.0f}" if ebitda is not None else "—")
        with c3:
            st.metric("Lucro (últ. período)", f"{lucro:,.0f}" if lucro is not None else "—")

    except Exception as e:
        # não quebra a página por conta de indicadores auxiliares
        st.info("Indicadores rápidos indisponíveis para este formato de dados.")
        st.caption(f"Detalhe técnico: {e}")
