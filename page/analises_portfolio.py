# page/analises_portfolio.py
"""Análises de Portfólio (Patch6)

Página Streamlit responsável por:
1) Ler snapshot do portfólio (gerado em criacao_portfolio.py)
2) Atualizar evidências (CVM/IPE) via ingest + chunking
3) Rodar análise qualitativa via LLM + RAG (Top‑K inteligente) para TODOS os tickers
4) Renderizar relatório profissional (cards CSS) + relatórios por empresa

Requisitos de robustez:
- Não depender exclusivamente de st.session_state para achar o snapshot.
- Não travar o app quando algum helper não existir: usar fallbacks.
- Sempre expor função render() (contrato do dashboard.py).
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

# --- Imports do projeto (com fallbacks defensivos) ---

# DB engine
_get_engine = None
for _cand in (
    ("core.db_loader", "get_engine"),
    ("core.db", "get_engine"),
):
    try:
        _mod = __import__(_cand[0], fromlist=[_cand[1]])
        _get_engine = getattr(_mod, _cand[1])
        break
    except Exception:
        pass

# Patch6 report
try:
    from core.patch6_report import render_patch6_report
except Exception:
    render_patch6_report = None  # type: ignore

# RAG retriever
try:
    from core.rag_retriever import get_topk_chunks_inteligente
except Exception:
    get_topk_chunks_inteligente = None  # type: ignore

try:
    from core.rag_retriever import fetch_topk_chunks
except Exception:
    fetch_topk_chunks = None  # type: ignore

# Patch6 store
save_patch6_run = None
list_patch6_history = None
try:
    from core.patch6_runs_store import save_patch6_run as _save_patch6_run
    from core.patch6_runs_store import list_patch6_history as _list_patch6_history

    save_patch6_run = _save_patch6_run
    list_patch6_history = _list_patch6_history
except Exception:
    pass

# LLM
try:
    import core.ai_models.llm_client.factory as llm_factory
except Exception:
    llm_factory = None  # type: ignore

# Ingest runner
ingest_runner = None
try:
    from core.patch6_ingest import ingest_runner as _ingest_runner

    ingest_runner = _ingest_runner
except Exception:
    try:
        from core.patch6_ingest_runner import ingest_runner as _ingest_runner2

        ingest_runner = _ingest_runner2
    except Exception:
        pass

# Logos (opcional)
get_logo_url = None
try:
    from core.ticker_logos import get_logo_url as _get_logo_url

    get_logo_url = _get_logo_url
except Exception:
    pass

st.set_page_config(page_title="Análises de Portfólio", layout="wide")


def _css() -> None:
    st.markdown(
        """
<style>
  .p6-wrap {max-width: 1200px; margin: 0 auto;}

  .cf-header{display:flex;justify-content:space-between;align-items:flex-start;gap:16px;
    background:linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.02));
    border:1px solid rgba(255,255,255,0.08);border-radius:18px;padding:18px 18px 14px 18px;}
  .cf-title{margin:0;font-size:42px;line-height:1.1;letter-spacing:-0.5px;}
  .cf-subtitle{margin:8px 0 0 0;opacity:0.85;font-size:14px;}
  .cf-pill{display:inline-flex;align-items:center;gap:8px;
    padding:10px 12px;border-radius:999px;border:1px solid rgba(255,255,255,0.12);
    background:rgba(255,255,255,0.04);font-size:13px;opacity:0.9;}

  .cf-grid{display:grid;grid-template-columns:repeat(4, minmax(0, 1fr));gap:14px;margin-top:14px;}
  @media (max-width: 1100px){.cf-grid{grid-template-columns:repeat(2, minmax(0, 1fr));}}
  @media (max-width: 640px){.cf-grid{grid-template-columns:1fr;}}

  .cf-card{border-radius:18px;border:1px solid rgba(255,255,255,0.10);
    background:rgba(255,255,255,0.04);padding:14px 14px 12px 14px;}
  .cf-card-label{font-size:13px;opacity:0.8;margin-bottom:6px;}
  .cf-card-value{font-size:30px;font-weight:700;line-height:1.1;margin-bottom:6px;}
  .cf-card-extra{font-size:12px;opacity:0.75;line-height:1.35;}

  .p6-badge{display:inline-flex;align-items:center;gap:6px;padding:6px 10px;border-radius:999px;
    font-size:12px;border:1px solid rgba(255,255,255,0.14);background:rgba(255,255,255,0.04);}
  .p6-badge-strong{border-color: rgba(55, 220, 150, 0.35); background: rgba(55, 220, 150, 0.10);}
  .p6-badge-neutral{border-color: rgba(120, 170, 255, 0.35); background: rgba(120, 170, 255, 0.10);}
  .p6-badge-caution{border-color: rgba(255, 170, 60, 0.35); background: rgba(255, 170, 60, 0.10);}
  .p6-badge-error{border-color: rgba(255, 70, 90, 0.35); background: rgba(255, 70, 90, 0.10);}

  .p6-ticker-row{display:flex;flex-wrap:wrap;gap:10px;margin-top:10px;}
  .p6-ticker-pill{display:flex;align-items:center;gap:10px; padding:10px 12px;border-radius:16px;
    border:1px solid rgba(255,255,255,0.10);background:rgba(255,255,255,0.03);}
  .p6-ticker-pill img{width:26px;height:26px;border-radius:8px;object-fit:contain;background:#fff;}
  .p6-ticker-pill .t{font-weight:700;letter-spacing:0.2px;}

  .p6-card{border-radius:18px;border:1px solid rgba(255,255,255,0.10);
    background:rgba(255,255,255,0.03);padding:14px;}
  .p6-card-head{display:flex;justify-content:space-between;align-items:flex-start;gap:10px;margin-bottom:10px;}
  .p6-card-title{font-size:18px;font-weight:800;margin:0;}
  .p6-card-meta{font-size:12px;opacity:0.75;line-height:1.35;}
  .p6-card-body{font-size:14px;opacity:0.92;line-height:1.45;white-space:pre-wrap;}
  .p6-hr{height:1px;background:rgba(255,255,255,0.08);margin:18px 0;}
</style>
        """,
        unsafe_allow_html=True,
    )


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip().replace("%", "")
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _format_pct(v: Optional[float]) -> str:
    if v is None:
        return "—"
    return f"{v:.2f}%"


def _get_snapshot_from_state() -> Optional[Dict[str, Any]]:
    snap = st.session_state.get("snapshot_portfolio")
    if isinstance(snap, dict):
        return snap
    return None


def _load_latest_snapshot_from_db() -> Optional[Dict[str, Any]]:
    """Tenta carregar o último snapshot diretamente do Supabase.

    A estrutura exata varia entre versões do dashboard; então fazemos heurística.
    """
    if _get_engine is None:
        return None
    engine = _get_engine()
    if engine is None:
        return None

    from sqlalchemy import text

    candidates: List[Tuple[str, List[str]]] = [
        ("snapshot_portfolio", ["snapshot", "payload", "data", "json", "portfolio"]),
        ("snapshot_portfolios", ["snapshot", "payload", "data", "json", "portfolio"]),
        ("snapshots_portfolio", ["snapshot", "payload", "data", "json", "portfolio"]),
    ]

    for table, json_cols in candidates:
        try:
            row = engine.execute(
                text(f"select * from {table} order by 1 desc limit 1")
            ).mappings().first()
            if not row:
                continue
            rowd = dict(row)

            # tenta achar uma coluna JSON
            cols_lower = {k.lower(): k for k in rowd.keys()}
            json_col = None
            for jc in json_cols:
                if jc in cols_lower:
                    json_col = cols_lower[jc]
                    break
            if json_col and rowd.get(json_col) is not None:
                val = rowd[json_col]
                if isinstance(val, dict):
                    return val
                if isinstance(val, str):
                    try:
                        return json.loads(val)
                    except Exception:
                        pass
            return rowd
        except Exception:
            continue

    return None


def _extract_snapshot_metrics(snapshot: Dict[str, Any], tickers: List[str]) -> Dict[str, Any]:
    selic = None
    for k in (
        "selic",
        "selic_usada",
        "selic_escolhida",
        "selic_anual",
        "selic_input",
        "benchmark_selic",
        "taxa_selic",
    ):
        selic = _safe_float(snapshot.get(k))
        if selic is not None:
            break

    margin = None
    for k in (
        "margem_minima",
        "margem_min",
        "margem_minima_sobre",
        "margem_minima_sobre_benchmark",
        "margem_sobre_selic",
        "percentual_acima_benchmark",
        "perc_acima_benchmark",
        "pct_acima_benchmark",
    ):
        margin = _safe_float(snapshot.get(k))
        if margin is not None:
            break

    items = snapshot.get("items") or snapshot.get("acoes") or snapshot.get("portfolio")
    if isinstance(items, list):
        if selic is None:
            for it in items:
                if isinstance(it, dict):
                    selic = _safe_float(it.get("selic"))
                    if selic is not None:
                        break
        if margin is None:
            for it in items:
                if isinstance(it, dict):
                    margin = _safe_float(it.get("margem"))
                    if margin is not None:
                        break

    seg_count = 0
    if _get_engine is not None and tickers:
        try:
            from sqlalchemy import text

            engine = _get_engine()
            if engine is not None:
                q = text(
                    """
                    select ticker, coalesce(segmento, subsetor, setor) as seg
                    from public.setores
                    where ticker = any(:tickers)
                    """
                )
                rows = engine.execute(q, {"tickers": tickers}).fetchall()
                segs = {r[1] for r in rows if r and r[1]}
                seg_count = len(segs)
        except Exception:
            seg_count = 0

    return {"selic": selic, "margin": margin, "segments": seg_count}


def _call_llm(client: Any, prompt: str) -> str:
    if client is None:
        raise RuntimeError("Cliente LLM indisponível")

    for m in ("chat", "complete", "invoke"):
        if hasattr(client, m):
            fn = getattr(client, m)
            out = fn(prompt)
            if isinstance(out, dict):
                return (
                    out.get("text")
                    or out.get("content")
                    or out.get("output")
                    or json.dumps(out, ensure_ascii=False)
                )
            return str(out)
    if callable(client):
        return str(client(prompt))
    raise AttributeError("Cliente LLM não expõe chat/complete/invoke")


def _build_prompt(ticker: str, period_ref: str, context: str) -> str:
    return (
        "Você é um analista sell-side (research) focado em alocação de capital e geração de valor ao acionista minoritário.\n\n"
        f"Ativo: {ticker}\n"
        f"Recorte temporal: {period_ref} (janela qualitativa ~12 meses)\n\n"
        "Contexto (trechos de comunicados/relatórios/notas):\n"
        + context
        + "\n\n"
        "Tarefa:\n"
        "1) Identifique sinais sobre: CAPEX/expansão, estrutura de capital/dívida, política de dividendos/recompra, M&A/desinvestimentos, guidance/direcionamento.\n"
        "2) Escreva uma tese (síntese) em 4-6 linhas, focada no caminho que a empresa está traçando.\n"
        "3) Classifique a direção em uma destas: Construtiva / Equilibrada / Cautelosa.\n"
        "4) Liste 3 gatilhos (o que observar) + 2 riscos principais.\n\n"
        "Formato de saída (JSON):\n"
        "{\n"
        '  "direcao": "Construtiva|Equilibrada|Cautelosa",\n'
        '  "tese": "...",\n'
        '  "gatilhos": ["...","...","..."],\n'
        '  "riscos": ["...","..."],\n'
        '  "resumo": "1 parágrafo curto"\n'
        "}\n"
    )


def _parse_llm_json(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    if not raw:
        return {"erro": "resposta vazia"}
    try:
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(raw[start : end + 1])
    except Exception:
        pass
    return {"raw": raw}


def _badge_class(direcao: str) -> str:
    d = (direcao or "").lower()
    if "construt" in d:
        return "p6-badge p6-badge-strong"
    if "equilibr" in d or "neutr" in d:
        return "p6-badge p6-badge-neutral"
    if "caut" in d:
        return "p6-badge p6-badge-caution"
    if "erro" in d:
        return "p6-badge p6-badge-error"
    return "p6-badge"


def _render_ticker_pills(tickers: List[str]) -> None:
    if not tickers:
        return
    html = ["<div class='p6-ticker-row'>"]
    for t in tickers:
        url = None
        if get_logo_url is not None:
            try:
                url = get_logo_url(t)
            except Exception:
                url = None
        if url:
            html.append(
                f"<div class='p6-ticker-pill'><img src='{url}'/><div class='t'>{t}</div></div>"
            )
        else:
            html.append(f"<div class='p6-ticker-pill'><div class='t'>{t}</div></div>")
    html.append("</div>")
    st.markdown("\n".join(html), unsafe_allow_html=True)


def _render_saved_data_cards(metrics: Dict[str, Any], tickers: List[str]) -> None:
    selic = metrics.get("selic")
    margin = metrics.get("margin")
    segs = int(metrics.get("segments") or 0)
    qtd = len(tickers)

    st.markdown(
        """
        <div class="cf-grid">
            <div class="cf-card">
                <div class="cf-card-label">SELIC usada</div>
                <div class="cf-card-value">{selic}</div>
                <div class="cf-card-extra">Taxa base informada na Criação de Portfólio (benchmark).</div>
            </div>
            <div class="cf-card">
                <div class="cf-card-label">Ações no portfólio</div>
                <div class="cf-card-value">{qtd}</div>
                <div class="cf-card-extra">Quantidade de ativos selecionados no snapshot.</div>
            </div>
            <div class="cf-card">
                <div class="cf-card-label">% acima do benchmark</div>
                <div class="cf-card-value">{margin}</div>
                <div class="cf-card-extra">Margem mínima projetada vs índice base (definida na seleção).</div>
            </div>
            <div class="cf-card">
                <div class="cf-card-label">Segmentos</div>
                <div class="cf-card-value">{segs}</div>
                <div class="cf-card-extra">Diversificação setorial estimada pelos segmentos (B3).</div>
            </div>
        </div>
        """.format(
            selic=(f"{selic:.2f}%" if isinstance(selic, (int, float)) else "—"),
            qtd=qtd,
            margin=_format_pct(margin),
            segs=segs,
        ),
        unsafe_allow_html=True,
    )


def render() -> None:
    _css()

    st.markdown('<div class="p6-wrap">', unsafe_allow_html=True)
    st.markdown(
        """
        <div class="cf-header">
            <div>
                <h1 class="cf-title">🧠 Análises de Portfólio (Patch6)</h1>
                <p class="cf-subtitle">Consolidação qualitativa baseada em evidências (RAG) + tese por empresa (LLM).</p>
            </div>
            <div>
                <span class="cf-pill">Janela padrão: <strong>12 meses</strong></span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    snapshot = _get_snapshot_from_state()
    if snapshot is None:
        snapshot = _load_latest_snapshot_from_db()
        if snapshot is not None:
            st.session_state["snapshot_portfolio"] = snapshot

    if snapshot is None:
        st.markdown("<div class='p6-hr'></div>", unsafe_allow_html=True)
        st.markdown(
            """
            <div class="p6-card">
              <div class="p6-card-head">
                <h3 class="p6-card-title">Nenhum portfólio encontrado</h3>
              </div>
              <div class="p6-card-body">Execute <strong>Criação de Portfólio</strong> para gerar os dados (snapshot).</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown("</div>", unsafe_allow_html=True)
        return

    # tickers
    tickers: List[str] = []
    items = snapshot.get("items") or snapshot.get("acoes") or snapshot.get("portfolio")
    if isinstance(items, list):
        for it in items:
            if isinstance(it, dict):
                t = it.get("ticker") or it.get("ativo")
                if t:
                    tickers.append(str(t).upper())
    if not tickers:
        tks = snapshot.get("tickers")
        if isinstance(tks, list):
            tickers = [str(t).upper() for t in tks]

    tickers = sorted(list(dict.fromkeys([t for t in tickers if t])))
    metrics = _extract_snapshot_metrics(snapshot, tickers)

    st.markdown("<div class='p6-hr'></div>", unsafe_allow_html=True)
    st.subheader("📦 Dados salvos")
    _render_saved_data_cards(metrics, tickers)
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    st.markdown("**Ativos selecionados**")
    _render_ticker_pills(tickers)

    # --- Atualizar evidências ---
    st.markdown("<div class='p6-hr'></div>", unsafe_allow_html=True)
    st.subheader("📦 Atualizar evidências")
    st.caption("Atualiza documentos e recortes (RAG) no Supabase para a janela padrão de 12 meses.")

    colA, colB = st.columns([1, 2])
    max_docs = colA.number_input("Máx docs/ticker", min_value=10, max_value=200, value=80, step=10)
    max_pdfs = colA.number_input("Máx PDFs/ticker", min_value=5, max_value=60, value=20, step=5)
    run_ingest = colB.button("Atualizar documentos", use_container_width=True)

    if run_ingest:
        if ingest_runner is None:
            st.error("Ingest runner não encontrado no projeto (core.patch6_ingest*).")
        else:
            with st.spinner("Atualizando evidências (ingest + chunks)..."):
                try:
                    out = ingest_runner(
                        tickers=tickers,
                        window_months=12,
                        max_docs=int(max_docs),
                        max_pdfs=int(max_pdfs),
                        verbose=False,
                    )
                    st.success("Evidências atualizadas.")
                    if isinstance(out, dict) and out.get("errors"):
                        st.warning(f"Alguns tickers retornaram erro: {out.get('errors')}")
                except Exception as e:
                    st.error(f"Falha no ingest: {e}")

    # --- Relatório profissional ---
    st.markdown("<div class='p6-hr'></div>", unsafe_allow_html=True)
    st.subheader("📘 Relatório de Análise de Portfólio")
    period_ref = st.text_input("period_ref (ex.: 2024Q4)", value="2024Q4")

    if render_patch6_report is not None:
        try:
            render_patch6_report(tickers=tickers, period_ref=period_ref, window_months=12)
        except Exception as e:
            st.error(f"Falha ao renderizar relatório: {e}")
    else:
        st.info("Módulo core.patch6_report não disponível.")

    # --- LLM ---
    st.markdown("<div class='p6-hr'></div>", unsafe_allow_html=True)
    st.subheader("🤖 Análise por LLM")

    topk = st.slider("Top‑K (recortes de contexto)", min_value=3, max_value=20, value=6, step=1)
    use_topk_inteligente = st.checkbox(
        "Usar Top‑K inteligente (intenção futura)",
        value=True,
        help="Seleciona recortes mais prováveis de conter sinais sobre CAPEX, dívida, dividendos, M&A e guidance.",
    )

    run_llm = st.button("Rodar LLM agora", use_container_width=True)

    if run_llm:
        if llm_factory is None:
            st.error("LLM factory não disponível (core.ai_models.llm_client.factory).")
        else:
            client = llm_factory.get_llm_client()
            progress = st.progress(0)
            status_box = st.empty()
            results: Dict[str, Dict[str, Any]] = {}

            for i, t in enumerate(tickers, start=1):
                status_box.info(f"Processando {t} ({i}/{len(tickers)})…")
                try:
                    if use_topk_inteligente and get_topk_chunks_inteligente is not None:
                        chunks = get_topk_chunks_inteligente(
                            ticker=t,
                            window_months=12,
                            top_k=int(topk),
                            period_ref=period_ref,
                            debug=False,
                        )
                    elif fetch_topk_chunks is not None:
                        chunks = fetch_topk_chunks(
                            ticker=t,
                            window_months=12,
                            top_k=int(topk),
                            period_ref=period_ref,
                        )
                    else:
                        chunks = []

                    parts: List[str] = []
                    if isinstance(chunks, list):
                        for c in chunks:
                            if isinstance(c, dict):
                                parts.append(
                                    str(
                                        c.get("text")
                                        or c.get("chunk")
                                        or c.get("content")
                                        or ""
                                    )
                                )
                            else:
                                parts.append(str(c))
                        context = "\n\n".join([p for p in parts if p])
                    else:
                        context = str(chunks)

                    prompt = _build_prompt(t, period_ref, context[:12000])
                    raw = _call_llm(client, prompt)
                    parsed = _parse_llm_json(raw)
                    results[t] = parsed

                    if save_patch6_run is not None:
                        payload = {
                            "ticker": t,
                            "period_ref": period_ref,
                            "window_months": 12,
                            "top_k": int(topk),
                            "direcao": parsed.get("direcao"),
                            "tese": parsed.get("tese") or parsed.get("resumo"),
                            "resumo": parsed.get("resumo") or parsed.get("tese"),
                            "gatilhos": parsed.get("gatilhos"),
                            "riscos": parsed.get("riscos"),
                            "raw": raw,
                            "meta": {
                                "docs_usados": None,
                                "recortes_usados": len(chunks)
                                if isinstance(chunks, list)
                                else None,
                                "topk_inteligente": bool(use_topk_inteligente),
                            },
                        }
                        try:
                            save_patch6_run(payload)
                        except Exception:
                            pass

                except Exception as e:
                    results[t] = {"erro": str(e)}

                progress.progress(int(100 * i / max(1, len(tickers))))

            status_box.success("Concluído.")

            st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
            for t in tickers:
                r = results.get(t) or {}
                direcao = r.get("direcao") or ("Erro" if r.get("erro") else "—")
                badge = _badge_class(str(direcao))

                meta = []
                if isinstance(r.get("gatilhos"), list) and r.get("gatilhos"):
                    meta.append(f"Gatilhos: {', '.join(list(r.get('gatilhos'))[:3])}")
                if isinstance(r.get("riscos"), list) and r.get("riscos"):
                    meta.append(f"Riscos: {', '.join(list(r.get('riscos'))[:2])}")

                body = r.get("tese") or r.get("resumo") or r.get("raw")
                if r.get("erro"):
                    body = f"Erro ao consultar LLM: {r.get('erro')}"

                st.markdown(
                    f"""
                    <div class="p6-card" style="margin-bottom:12px;">
                        <div class="p6-card-head">
                            <div>
                                <h3 class="p6-card-title">{t}</h3>
                                <div class="p6-card-meta">{('<br/>'.join(meta) if meta else '—')}</div>
                            </div>
                            <div class="{badge}">{direcao}</div>
                        </div>
                        <div class="p6-card-body">{(body or '—')}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            time.sleep(0.2)
            st.rerun()

    if list_patch6_history is not None:
        st.markdown("<div class='p6-hr'></div>", unsafe_allow_html=True)
        st.subheader("📜 Histórico (patch6_runs)")
        try:
            df_hist = list_patch6_history(limit=50)
            st.dataframe(df_hist, use_container_width=True)
        except Exception:
            pass

    st.markdown("</div>", unsafe_allow_html=True)
