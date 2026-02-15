from __future__ import annotations
"""
page/patch6_teste.py
--------------------
Página de teste rápida do Patch 6, sem passar por criacao_portfolio.py.

Inclui:
1) Selecionar tickers
2) Contar docs existentes no Supabase (public.docs_corporativos)
3) Ingerir docs (A->B fallback): CVM/IPE e/ou RI (public.ri_map)
"""

from typing import List, Dict
import streamlit as st
from sqlalchemy import text

from core.db_loader import get_supabase_engine
from pickup.ingest_docs_fallback import ingest_strategy_for_tickers


def _norm(t: str) -> str:
    return (t or "").upper().replace(".SA", "").strip()


def _count_docs_by_tickers(tickers: List[str]) -> Dict[str, int]:
    tks = [_norm(x) for x in tickers if str(x).strip()]
    tks = list(dict.fromkeys(tks))
    if not tks:
        return {}
    engine = get_supabase_engine()
    sql = text("""
        select upper(ticker) as ticker, count(*)::int as qtd
        from public.docs_corporativos
        where upper(ticker) = any(:tks)
        group by upper(ticker)
    """)
    out = {tk: 0 for tk in tks}
    with engine.begin() as conn:
        rows = conn.execute(sql, {"tks": tks}).fetchall()
        for tk, qtd in rows:
            out[str(tk).upper()] = int(qtd or 0)
    return out


def render() -> None:
    st.title("Patch 6 — Teste rápido (A/B/C)")
    st.caption("Depuração: ingestão e contagem sem rodar criação de portfólio/score/backtest.")

    tickers_str = st.text_input("Tickers (separados por vírgula)", value="BBAS3,PETR3,VALE3")
    tickers = [_norm(x) for x in tickers_str.split(",") if x.strip()]

    st.subheader("2) Verificar docs já existentes")
    if st.button("Contar docs no Supabase"):
        counts = _count_docs_by_tickers(tickers)
        total = sum(counts.values())
        st.success(f"Docs carregados no Supabase: {total}")
        with st.expander("Ver contagem por ticker", expanded=True):
            for tk, q in counts.items():
                st.write(f"{tk}: {q}")

    st.subheader("3) Ingerir docs (A->B)")
    col1, col2, col3 = st.columns(3)
    with col1:
        anos = st.number_input("Anos (janela)", min_value=0, max_value=15, value=2, step=1)
    with col2:
        max_docs = st.number_input("Máx docs por ticker", min_value=1, max_value=200, value=25, step=1)
    with col3:
        strategy = st.selectbox("Estratégia", ["A", "B", "A->B", "A->B->C"], index=2)

    st.caption("A: CVM/IPE • B: RI via public.ri_map • C: secundário (placeholder)")
    if st.button("Ingerir agora"):
        with st.spinner("Ingerindo..."):
            res = ingest_strategy_for_tickers(
                tickers,
                anos=int(anos),
                max_docs_por_ticker=int(max_docs),
                sleep_s=0.2,
                strategy=str(strategy),
                ri_map_table="public.ri_map",
                enable_c=False,
            )
        st.json(res)

    st.subheader("4) Tabela RI map (manual)")
    st.code("""create table if not exists public.ri_map (
  id bigserial primary key,
  ticker text not null unique,
  ri_url text not null,
  created_at timestamptz default now()
);

insert into public.ri_map (ticker, ri_url)
values ('BBAS3', 'https://ri.bb.com.br/')
on conflict (ticker) do update set ri_url=excluded.ri_url;
""", language="sql")
