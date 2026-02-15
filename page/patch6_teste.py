from __future__ import annotations

"""
page/patch6_teste.py
--------------------
Página de TESTE do Patch 6 (RAG) sem executar criação de portfólio, score ou backtest.

O que esta página faz:
1) Seleciona tickers
2) Conta docs já existentes em public.docs_corporativos (Supabase)
3) Ingestão ENET (CVM) usando Código CVM via public.cvm_to_ticker
4) (Opcional) Tenta rodar o Patch 6, se existir um runner no seu projeto
   - caso não exista, apenas mostra os textos coletados para validação do RAG.

Requisitos esperados no Supabase:
- public.cvm_to_ticker com colunas: "Ticker" (text) e "CVM" (int)
- public.docs_corporativos (ticker, data, fonte, tipo, titulo, url, raw_text, doc_hash)
- public.docs_corporativos_chunks (opcional)

Importante:
- Este arquivo define render() para ser carregado pelo dashboard.py
"""

import importlib
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import streamlit as st
from sqlalchemy import text

from core.db_loader import get_supabase_engine

# Ingest ENET (Opção A)
from pickup.ingest_docs_cvm_enet import ingest_enet_for_tickers


# ─────────────────────────────────────────────────────────────────────
# Helpers SQL (evita depender de pickup.docs_rag)
# ─────────────────────────────────────────────────────────────────────
def _norm_ticker(t: str) -> str:
    return (t or "").upper().replace(".SA", "").strip()


def _parse_tickers(raw: str) -> List[str]:
    raw = (raw or "").strip()
    if not raw:
        return []
    parts = [p.strip() for p in raw.replace(";", ",").replace("\n", ",").split(",")]
    tks = [_norm_ticker(p) for p in parts if p]
    # unique preserving order
    out: List[str] = []
    seen = set()
    for t in tks:
        if t and t not in seen:
            out.append(t)
            seen.add(t)
    return out


def count_docs_by_tickers(tickers: Sequence[str]) -> Tuple[int, Dict[str, int]]:
    tks = [_norm_ticker(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys(tks))
    if not tks:
        return 0, {}
    eng = get_supabase_engine()
    sql_total = text("""
        select count(*) as n
        from public.docs_corporativos
        where upper(ticker) = any(:tks)
    """)
    sql_by = text("""
        select upper(ticker) as ticker, count(*) as n
        from public.docs_corporativos
        where upper(ticker) = any(:tks)
        group by upper(ticker)
        order by 2 desc
    """)
    with eng.begin() as conn:
        total = int(conn.execute(sql_total, {"tks": tks}).scalar() or 0)
        rows = conn.execute(sql_by, {"tks": tks}).fetchall()
    by = {str(r[0]).upper(): int(r[1]) for r in rows}
    return total, by


def get_docs_by_tickers(
    tickers: Sequence[str],
    *,
    max_docs_por_ticker: int = 8,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Retorna docs (metadados + raw_text) para cada ticker.
    Usado para validar o insumo do RAG sem depender de outro módulo.
    """
    tks = [_norm_ticker(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys(tks))
    if not tks:
        return {}

    max_n = max(1, int(max_docs_por_ticker))
    eng = get_supabase_engine()

    sql = text("""
        select ticker, data, fonte, tipo, titulo, url, raw_text, id
        from public.docs_corporativos
        where upper(ticker) = any(:tks)
        order by ticker asc, data desc nulls last, id desc
    """)

    out: Dict[str, List[Dict[str, Any]]] = {t: [] for t in tks}
    with eng.begin() as conn:
        rows = conn.execute(sql, {"tks": tks}).fetchall()

    for r in rows:
        tk = _norm_ticker(str(r[0]))
        if tk not in out:
            continue
        if len(out[tk]) >= max_n:
            continue
        out[tk].append({
            "ticker": tk,
            "data": (r[1].isoformat() if hasattr(r[1], "isoformat") and r[1] else (str(r[1]) if r[1] else None)),
            "fonte": r[2],
            "tipo": r[3],
            "titulo": r[4],
            "url": r[5],
            "raw_text": r[6] or "",
            "id": int(r[7]) if r[7] is not None else None,
        })
    return out


def _try_find_patch6_runner() -> Optional[Callable[..., Any]]:
    """
    Procura um runner existente no projeto (nomes comuns).
    Se não achar, retorna None (a página ainda funciona para ingest/validação).
    """
    candidates = [
        ("pickup.patch6", ["run_patch6", "rodar_patch6", "run_patch6_for_tickers", "patch6_run"]),
        ("core.patch6",   ["run_patch6", "rodar_patch6", "run_patch6_for_tickers", "patch6_run"]),
        ("patch6",        ["run_patch6", "rodar_patch6", "run_patch6_for_tickers", "patch6_run"]),
    ]
    for mod_name, fn_names in candidates:
        try:
            mod = importlib.import_module(mod_name)
        except Exception:
            continue
        for fn in fn_names:
            f = getattr(mod, fn, None)
            if callable(f):
                return f
    return None


# ─────────────────────────────────────────────────────────────────────
# UI
# ─────────────────────────────────────────────────────────────────────
def render() -> None:
    st.title("🧪 Patch 6 — Modo Teste (ENET / Código CVM)")

    st.caption("Roda o Patch 6 sem executar criação de portfólio, score ou backtest.")

    default_tickers = "BBAS3, PETR3, VALE3"

    colA, colB = st.columns([2, 1], gap="large")
    with colA:
        tickers_raw = st.text_input("Tickers (separados por vírgula)", value=default_tickers)
    with colB:
        max_docs_ui = st.number_input("Máx docs por ticker (RAG)", min_value=1, max_value=50, value=8, step=1)

    tickers = _parse_tickers(tickers_raw)

    st.markdown("### Parâmetros do teste")
    c1, c2, c3 = st.columns([1, 1, 1], gap="large")
    with c1:
        anos = st.number_input("Anos (janela)", min_value=0, max_value=10, value=2, step=1)
    with c2:
        max_docs_ingest = st.number_input("Máx docs para ingerir por ticker", min_value=1, max_value=200, value=20, step=5)
    with c3:
        baixar_extrair = st.toggle("Baixar e extrair texto (PDF/HTML)", value=True)

    st.markdown("### 1) Validar seleção")
    if not tickers:
        st.warning("Informe ao menos 1 ticker para continuar.")
        st.stop()
    st.write("Tickers selecionados:", ", ".join(tickers))

    st.divider()

    st.markdown("### 2) Verificar docs já existentes (Supabase)")
    colx, coly = st.columns([1, 2], gap="large")
    with colx:
        if st.button("Contar docs no Supabase", use_container_width=True):
            total, by = count_docs_by_tickers(tickers)
            st.session_state["patch6_docs_total"] = total
            st.session_state["patch6_docs_by"] = by

    total = st.session_state.get("patch6_docs_total", None)
    by = st.session_state.get("patch6_docs_by", None)
    if isinstance(total, int) and isinstance(by, dict):
        st.success(f"Docs carregados do Supabase: {total}")
        with st.expander("Ver contagem por ticker", expanded=True):
            for tk in tickers:
                st.write(f"**{tk}**: {int(by.get(tk, 0))} docs")

    st.divider()

    st.markdown("### 3) Ingerir docs (CVM ENET) para os tickers")
    st.caption("Requer: tabela public.cvm_to_ticker preenchida (Ticker → CVM).")

    cat_default = [
        "Fato Relevante",
        "Comunicado ao Mercado",
        "Aviso aos Acionistas",
        "Assembleia",
        "Edital",
        "Release",
        "Apresentação",
    ]
    categorias = st.multiselect("Categorias (filtro estratégico)", options=cat_default, default=cat_default)

    if st.button("⬇️ Ingerir ENET (CVM) agora", use_container_width=True):
        with st.spinner("Consultando ENET e ingerindo documentos..."):
            out = ingest_enet_for_tickers(
                tickers=tickers,
                anos=int(anos),
                max_docs_por_ticker=int(max_docs_ingest),
                baixar_e_extrair=bool(baixar_extrair),
                categorias=categorias,
            )
        st.subheader("Resultado da ingestão")
        st.json(out)

        # Reconta automaticamente
        try:
            total2, by2 = count_docs_by_tickers(tickers)
            st.session_state["patch6_docs_total"] = total2
            st.session_state["patch6_docs_by"] = by2
            st.info(f"Após ingestão → total docs: {total2}")
        except Exception as e:
            st.warning(f"Não consegui recontar docs após ingestão: {e}")

    st.divider()

    st.markdown("### 4) Rodar Patch 6 com RAG do Supabase (opcional)")
    st.caption("Se não existir runner do Patch6 no seu projeto, esta etapa apenas mostra o material para o RAG.")

    docs = get_docs_by_tickers(tickers, max_docs_por_ticker=int(max_docs_ui))

    with st.expander("Ver amostra de docs carregados para o RAG", expanded=False):
        for tk, lst in docs.items():
            st.markdown(f"#### {tk} — {len(lst)} docs")
            for i, d in enumerate(lst, start=1):
                title = (d.get("titulo") or "").strip() or "(sem título)"
                st.write(f"{i}. **{title}** — {d.get('data')}")
                st.caption(d.get("url") or "")
                raw = (d.get("raw_text") or "")
                st.code(raw[:600] + ("..." if len(raw) > 600 else ""))

    runner = _try_find_patch6_runner()
    if runner is None:
        st.warning("Runner do Patch 6 não encontrado no projeto. A ingestão e validação de RAG está OK; para rodar, conecte seu runner aqui.")
        return

    st.success(f"Runner do Patch 6 encontrado: `{runner.__module__}.{runner.__name__}()`")

    if st.button("🧠 Rodar Patch 6 (usar RAG Supabase)", use_container_width=True):
        payload = {
            "tickers": tickers,
            "docs_by_ticker": docs,
        }
        with st.spinner("Rodando Patch 6..."):
            try:
                result = runner(**payload)
            except TypeError:
                result = runner(tickers, docs)

        st.subheader("Saída do Patch 6")
        if isinstance(result, (dict, list)):
            st.json(result)
        else:
            st.write(result)
