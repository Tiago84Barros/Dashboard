# dashboard/page/patch6_teste.py
# Análise de Portfólio — Patch 6 (CVM/IPE + RAG + LLM)
from __future__ import annotations

import datetime as _dt
import importlib
import inspect
import json
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from sqlalchemy import text

# ---------------------------------------------------------------------
# Helpers básicos
# ---------------------------------------------------------------------
def _norm_tk(t: str) -> str:
    return (t or "").strip().upper().replace(".SA", "").strip()

def _safe_call(fn: Callable[..., Any], **kwargs) -> Any:
    """Chama fn apenas com kwargs compatíveis com a assinatura."""
    try:
        sig = inspect.signature(fn)
    except Exception:
        return fn(**kwargs)

    accepted: Dict[str, Any] = {}
    for k, v in kwargs.items():
        if k in sig.parameters:
            accepted[k] = v

    # aliases frequentes
    if "window_months" in kwargs and "window_months" not in accepted:
        if "months" in sig.parameters:
            accepted["months"] = kwargs["window_months"]

    if "max_docs_per_ticker" in kwargs and "max_docs_per_ticker" not in accepted:
        if "max_docs" in sig.parameters:
            accepted["max_docs"] = kwargs["max_docs_per_ticker"]
        elif "limit_per_ticker" in sig.parameters:
            accepted["limit_per_ticker"] = kwargs["max_docs_per_ticker"]

    return fn(**accepted)

# ---------------------------------------------------------------------
# Supabase
# ---------------------------------------------------------------------
def _get_engine():
    from core.db_loader import get_supabase_engine
    return get_supabase_engine()

def _read_sql_df(sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    eng = _get_engine()
    with eng.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params or {})

def _ensure_patch6_tables():
    eng = _get_engine()
    ddl = """
    create table if not exists public.patch6_reports (
      id bigserial primary key,
      plan_hash text not null,
      asof_quarter text not null,
      ticker text not null,
      perspectiva text,
      fator double precision,
      variaveis_uteis int,
      report_json jsonb,
      created_at timestamptz not null default now(),
      unique(plan_hash, asof_quarter, ticker)
    );
    create index if not exists patch6_reports_plan_hash_idx
      on public.patch6_reports(plan_hash);
    create index if not exists patch6_reports_asof_idx
      on public.patch6_reports(asof_quarter);
    """
    with eng.begin() as conn:
        conn.execute(text(ddl))

def _get_latest_plan() -> Optional[Dict[str, Any]]:
    df = _read_sql_df(
        """
        select plan_hash, created_at, ultimo_ano, margem_superior, payload
        from public.portfolio_plans
        order by created_at desc
        limit 1
        """
    )
    if df is None or df.empty:
        return None
    r = df.iloc[0].to_dict()
    payload = r.get("payload")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = None
    r["payload"] = payload
    return r

def _count_variaveis_uteis(tickers: List[str]) -> Dict[str, int]:
    """Conta quantos 'variáveis úteis' (chunks) existem por ticker."""
    tks = [_norm_tk(t) for t in (tickers or []) if str(t).strip()]
    if not tks:
        return {}
    df = _read_sql_df(
        """
        select ticker, count(*)::int as cnt
        from public.docs_corporativos_chunks
        where ticker = any(:tks)
        group by ticker
        """,
        {"tks": tks},
    )
    out = {t: 0 for t in tks}
    for _, row in df.iterrows():
        out[str(row["ticker"])] = int(row["cnt"])
    return out

def _get_chunks_for_llm(ticker: str, limit: int) -> List[str]:
    df = _read_sql_df(
        """
        select chunk_text
        from public.docs_corporativos_chunks
        where ticker = :tk
        order by id desc
        limit :lim
        """,
        {"tk": _norm_tk(ticker), "lim": int(limit)},
    )
    if df is None or df.empty:
        return []
    return [str(x or "").strip() for x in df["chunk_text"].tolist() if str(x or "").strip()]

# ---------------------------------------------------------------------
# Ingest + Chunking (unificado)
# ---------------------------------------------------------------------
def _try_find_ingest_runner() -> Optional[Callable[..., Any]]:
    candidates = [
        ("pickup.ingest_docs_cvm_ipe", ["ingest_ipe_for_tickers"]),
        ("core.ingest_docs_cvm_ipe", ["ingest_ipe_for_tickers"]),
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

def _try_find_chunker():
    candidates = [
        ("core.patch6_store", ["process_missing_chunks_for_ticker"]),
    ]
    for mod_name, fns in candidates:
        try:
            mod = importlib.import_module(mod_name)
        except Exception:
            continue
        for fn in fns:
            f = getattr(mod, fn, None)
            if callable(f):
                return f
    return None

# ---------------------------------------------------------------------
# LLM
# ---------------------------------------------------------------------
def _build_prompt(ticker: str, context: str, extra_vars: Dict[str, Any]) -> str:
    extras = json.dumps(extra_vars or {}, ensure_ascii=False)
    return f"""
Você é um analista fundamentalista focado em direcionalidade estratégica (capex, expansão, guidance, investimentos futuros,
desalavancagem, alocação de capital e prioridades do management).

Julgue a empresa **{ticker}** com base APENAS no contexto fornecido (variáveis úteis) e nas variáveis quantitativas do portfólio.

VARIÁVEIS QUANTITATIVAS (do portfólio):
{extras}

ENTREGA (responda em JSON válido):
{{
  "ticker": "{ticker}",
  "perspectiva_compra": "forte|moderada|fraca",
  "resumo": "2-4 frases, direto",
  "pontos_chave": ["..."],
  "riscos_ou_alertas": ["..."],
  "porque": "1 parágrafo objetivo",
  "evidencias": [{{"fonte":"CVM/IPE","trecho":"<=240 chars","observacao":"por que isso importa"}}]
}}

REGRAS:
- Não invente números. Se não houver, diga "não informado".
- Use apenas o contexto fornecido.
- Evidências devem vir do contexto.

[VARIÁVEIS ÚTEIS]
{context}
""".strip()

def _run_llm(ticker: str, vars_limit: int, extra_vars: Dict[str, Any]) -> Dict[str, Any]:
    parts = _get_chunks_for_llm(ticker, vars_limit)
    if not parts:
        return {"ok": False, "error": "Sem variáveis úteis suficientes."}

    context = "\n\n---\n\n".join([p[:1800] for p in parts[::-1]])

    prompt = _build_prompt(_norm_tk(ticker), context, extra_vars)

    schema_hint = r"""
{
  "ticker": "STRING",
  "perspectiva_compra": "forte|moderada|fraca",
  "resumo": "STRING",
  "pontos_chave": ["STRING"],
  "riscos_ou_alertas": ["STRING"],
  "porque": "STRING",
  "evidencias": [{"fonte":"STRING","trecho":"STRING","observacao":"STRING"}]
}
""".strip()

    from core.ai_models.llm_client.factory import get_llm_client
    llm = get_llm_client()

    system = """Você é um analista buy-side, cético e orientado a evidência.
- NÃO invente fatos, números, datas.
- Use APENAS o contexto fornecido.
- Responda OBRIGATORIAMENTE em JSON válido.
""".strip()

    out = llm.generate_json(system=system, user=prompt, schema_hint=schema_hint, context=None)
    return {"ok": True, "result": out, "meta": {"vars_used": len(parts)}}

# ---------------------------------------------------------------------
# CSS institucional
# ---------------------------------------------------------------------
CSS = """
<style>
.p6-card{background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);border-radius:16px;padding:16px;margin:12px 0;}
.p6-head{display:flex;justify-content:space-between;align-items:flex-start;gap:12px;}
.p6-title{font-weight:900;font-size:18px;color:#fff;}
.p6-pill{font-weight:800;font-size:12px;padding:6px 10px;border-radius:999px;}
.p6-strong{background:rgba(34,197,94,0.16);border:1px solid rgba(34,197,94,0.35);color:#bbf7d0;}
.p6-mid{background:rgba(250,204,21,0.14);border:1px solid rgba(250,204,21,0.35);color:#fef08a;}
.p6-weak{background:rgba(239,68,68,0.14);border:1px solid rgba(239,68,68,0.35);color:#fecaca;}
.p6-meta{font-size:12px;color:rgba(255,255,255,0.70);margin-top:4px;}
.p6-section{margin-top:10px;}
.p6-section h5{margin:8px 0 6px 0;font-size:13px;color:rgba(255,255,255,0.85);}
.p6-list{margin:0;padding-left:18px;color:rgba(255,255,255,0.80);font-size:13px;}
.p6-text{color:rgba(255,255,255,0.82);font-size:13px;line-height:1.35;}
</style>
"""

# ---------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------
def render() -> None:
    st.title("📌 Análise de Portfólio")
    st.caption("CVM/IPE → variáveis úteis → LLM → modulação sugerida de aporte (trimestral)")

    st.markdown(CSS, unsafe_allow_html=True)

    plan = _get_latest_plan()
    if not plan:
        st.warning("Execute primeiro a página **Criação de Portfólio** para salvar o portfólio. Depois volte aqui.")
        st.stop()

    payload = plan.get("payload") or {}
    ativos = payload.get("ativos") or []
    tickers = [_norm_tk(a.get("ticker")) for a in ativos if a.get("ticker")]

    # resumo do plano
    c1, c2, c3 = st.columns([1, 1, 1], gap="large")
    with c1:
        st.metric("Plano", plan.get("plan_hash", "")[:10])
    with c2:
        st.metric("Ano-líder", str(payload.get("ultimo_ano", "")))
    with c3:
        st.metric("Margem vs Selic (%)", f"{float(payload.get('margem_superior', 0.0)):.2f}")

    st.markdown("### 🧾 Ativos do Portfólio")
    df_port = pd.DataFrame([
        {"Ticker": _norm_tk(a.get("ticker")), "Peso": float(a.get("peso", 0.0) or 0.0), "Segmento": a.get("segmento"), "Ano líder": a.get("ano_lider"), "Motivo": a.get("motivo_select")}
        for a in ativos
    ])
    if not df_port.empty:
        df_port["Peso"] = df_port["Peso"].map(lambda x: round(float(x) * 100, 2))
        st.dataframe(df_port, use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("### A) 📥 Atualizar base estratégica (CVM/IPE)")

    colA, colB, colC, colD = st.columns([1, 1, 1, 1], gap="large")
    with colA:
        window_months = st.number_input("Janela (meses)", min_value=1, max_value=24, value=12, step=1)
    with colB:
        max_docs_ingest = st.number_input("Máx docs por ticker", min_value=5, max_value=200, value=60, step=5)
    with colC:
        max_pdfs = st.number_input("Máx PDFs por ticker", min_value=0, max_value=50, value=12, step=1)
    with colD:
        max_runtime_s = st.number_input("Tempo máx (s)", min_value=10, max_value=240, value=30, step=5)

    cE, cF = st.columns([1, 1], gap="large")
    with cE:
        strategic_only = st.toggle("Somente estratégicos", value=True)
    with cF:
        download_pdfs = st.toggle("Baixar PDFs e extrair texto", value=True)

    ingest_runner = _try_find_ingest_runner()
    chunker = _try_find_chunker()

    if st.button("⬇️ Buscar informações (CVM/IPE)", use_container_width=True):
        if ingest_runner is None:
            st.error("Ingest runner não encontrado (pickup.ingest_docs_cvm_ipe.ingest_ipe_for_tickers).")
        else:
            with st.spinner("Ingerindo documentos..."):
                ingest_out = _safe_call(
                    ingest_runner,
                    tickers=tickers,
                    window_months=int(window_months),
                    max_docs_per_ticker=int(max_docs_ingest),
                    strategic_only=bool(strategic_only),
                    download_pdfs=bool(download_pdfs),
                    max_pdfs_per_ticker=int(max_pdfs),
                    max_runtime_s=float(max_runtime_s),
                    verbose=False,
                )
            st.session_state["p6_ingest_out"] = ingest_out

            if chunker is not None:
                with st.spinner("Gerando variáveis úteis..."):
                    from core.patch6_store import process_missing_chunks_for_ticker
                    res_all = {tk: process_missing_chunks_for_ticker(_norm_tk(tk), limit_docs=80, only_with_text=True) for tk in tickers}
                st.session_state["p6_chunk_out"] = res_all

            st.success("Atualização concluída.")

    # mostrar contagem de variáveis úteis
    vars_by = _count_variaveis_uteis(tickers)
    df_vars = pd.DataFrame([{"Ticker": t, "Variáveis úteis": int(vars_by.get(t, 0))} for t in tickers])
    st.caption("Variáveis úteis = trechos processados para a IA (sem exibir logs técnicos)")
    st.dataframe(df_vars, use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("### B) 🧠 Relatórios por empresa (LLM)")

    col1, col2 = st.columns([1, 1], gap="large")
    with col1:
        vars_limit = st.number_input("Variáveis úteis por empresa", min_value=5, max_value=120, value=25, step=5)
    with col2:
        beta = st.slider("Intensidade do ajuste", min_value=0.0, max_value=0.20, value=0.10, step=0.01,
                         help="0.10 = +10% forte / -10% fraca (modulação leve e disciplinada)")

    # fatores
    fator_map = {"forte": 1.0 + float(beta), "moderada": 1.0, "fraca": max(0.0, 1.0 - float(beta))}
    st.caption(f"Fatores atuais: forte={fator_map['forte']:.2f}, moderada=1.00, fraca={fator_map['fraca']:.2f}")

    if st.button("🚀 Gerar relatórios (IA)", use_container_width=True):
        _ensure_patch6_tables()
        placeholder = st.container()
        progress = st.progress(0)
        reports: Dict[str, Any] = {}
        overlay: Dict[str, float] = {}

        # asof quarter
        today = _dt.date.today()
        q = (today.month - 1) // 3 + 1
        asof_quarter = f"{today.year}Q{q}"

        # extra vars (do portfólio)
        base_weight = { _norm_tk(a.get("ticker")): float(a.get("peso", 0.0) or 0.0) for a in ativos }

        eng = _get_engine()
        for i, tk in enumerate(tickers, start=1):
            extra_vars = {"peso_base": base_weight.get(tk, 0.0)}
            out = _run_llm(tk, int(vars_limit), extra_vars)
            if out.get("ok"):
                r = out["result"]
                perspectiva = str(r.get("perspectiva_compra", "moderada") or "moderada").lower().strip()
                if perspectiva not in fator_map:
                    perspectiva = "moderada"
                fator = float(fator_map[perspectiva])
                overlay[tk] = fator
                reports[tk] = {"perspectiva": perspectiva, "report": r, "vars_used": int(out.get("meta", {}).get("vars_used", 0)), "fator": fator}

                # persist
                _ensure_patch6_tables()
                with eng.begin() as conn:
                    conn.execute(
                        text("""
                        insert into public.patch6_reports(plan_hash, asof_quarter, ticker, perspectiva, fator, variaveis_uteis, report_json)
                        values (:plan_hash, :asof, :tk, :persp, :fator, :vars, :js::jsonb)
                        on conflict(plan_hash, asof_quarter, ticker) do update
                        set perspectiva = excluded.perspectiva,
                            fator = excluded.fator,
                            variaveis_uteis = excluded.variaveis_uteis,
                            report_json = excluded.report_json,
                            created_at = now()
                        """),
                        {
                            "plan_hash": plan.get("plan_hash"),
                            "asof": asof_quarter,
                            "tk": tk,
                            "persp": perspectiva,
                            "fator": fator,
                            "vars": int(reports[tk]["vars_used"]),
                            "js": json.dumps(r, ensure_ascii=False),
                        },
                    )

                # render card incremental
                pill_cls = "p6-mid"
                if perspectiva == "forte":
                    pill_cls = "p6-strong"
                elif perspectiva == "fraca":
                    pill_cls = "p6-weak"

                with placeholder:
                    st.markdown(f"""
<div class="p6-card">
  <div class="p6-head">
    <div>
      <div class="p6-title">{tk}</div>
      <div class="p6-meta">Variáveis úteis usadas: {reports[tk]['vars_used']} • Fator: {fator:.2f}</div>
    </div>
    <div class="p6-pill {pill_cls}">{perspectiva.upper()}</div>
  </div>
  <div class="p6-section">
    <div class="p6-text">{str(r.get('resumo','') or '').strip()}</div>
  </div>
  <div class="p6-section">
    <h5>Pontos-chave</h5>
    <ul class="p6-list">{''.join([f'<li>{x}</li>' for x in (r.get('pontos_chave') or [])[:6]])}</ul>
  </div>
  <div class="p6-section">
    <h5>Riscos / alertas</h5>
    <ul class="p6-list">{''.join([f'<li>{x}</li>' for x in (r.get('riscos_ou_alertas') or [])[:6]])}</ul>
  </div>
</div>
""", unsafe_allow_html=True)

            else:
                reports[tk] = {"perspectiva": "moderada", "report": {"ticker": tk, "perspectiva_compra": "moderada", "resumo": out.get("error", "Sem dados"), "pontos_chave": [], "riscos_ou_alertas": [], "porque": ""}, "vars_used": 0, "fator": 1.0}
                overlay[tk] = 1.0

            progress.progress(int(i / max(1, len(tickers)) * 100))

        st.session_state["patch6_reports"] = reports
        st.session_state["patch6_overlay"] = overlay
        st.session_state["patch6_asof_quarter"] = asof_quarter
        st.success("Relatórios concluídos.")

    # Se já existirem resultados, mostrar recomendação de aporte
    reports = st.session_state.get("patch6_reports")
    overlay = st.session_state.get("patch6_overlay")
    if reports and overlay:
        st.divider()
        st.markdown("### C) 💰 Sugestão de modulação do aporte")

        base = { _norm_tk(a.get("ticker")): float(a.get("peso", 0.0) or 0.0) for a in ativos }
        # aplica fator e renormaliza
        tmp = { tk: (base.get(tk, 0.0) * float(overlay.get(tk, 1.0))) for tk in tickers }
        total = sum(tmp.values())
        pct = { tk: (tmp[tk] / total if total > 0 else 0.0) for tk in tickers }

        df_out = pd.DataFrame([
            {
                "Ticker": tk,
                "Peso base (%)": round(base.get(tk, 0.0) * 100, 2),
                "Perspectiva": (reports.get(tk, {}) or {}).get("perspectiva", "moderada"),
                "Fator": round(float(overlay.get(tk, 1.0)), 2),
                "Aporte sugerido (%)": round(pct.get(tk, 0.0) * 100, 2),
            }
            for tk in tickers
        ]).sort_values("Aporte sugerido (%)", ascending=False)

        st.dataframe(df_out, use_container_width=True, hide_index=True)

        # salva para consumo pela Criação de Portfólio (opcional)
        st.session_state["patch6_overlay_pct"] = pct

if __name__ == "__main__":
    render()
