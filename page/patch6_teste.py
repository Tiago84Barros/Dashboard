
from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import streamlit as st

from core.portfolio_snapshot_store import get_latest_snapshot, list_snapshots, get_snapshot
from core.patch6_runs_store import save_patch6_run, list_patch6_history

# Reuso do pipeline Patch 6 já existente
from core.patch6_store import process_missing_chunks_for_ticker  # type: ignore
from pickup.ingest_docs_cvm_ipe import ingest_ipe_for_tickers  # type: ignore

# LLM client (já no seu projeto)
from core.ai_models.llm_client.factory import get_llm_client


def _quarter_ref(dt: datetime) -> str:
    q = (dt.month - 1) // 3 + 1
    return f"{dt.year}Q{q}"


def _build_prompt(ticker: str, context: str, manual_text: str) -> str:
    manual_block = f"\n\n[TEXTO MANUAL]\n{manual_text.strip()}\n" if manual_text and manual_text.strip() else ""
    return f"""
Você é um analista fundamentalista focado em direcionalidade estratégica (capex, expansão, guidance, investimentos futuros,
desalavancagem, alocação de capital e prioridades do management).

Seu trabalho é julgar a empresa **{ticker}** com base nos documentos coletados (CVM/IPE) e no texto manual (se houver).

ENTREGA (responda em JSON):
{{
  "ticker": "{ticker}",
  "perspectiva_compra": "forte|moderada|fraca",
  "resumo": "2-4 frases, direto",
  "pontos_chave": ["...","...","..."],
  "riscos_ou_alertas": ["...","..."],
  "sinais_de_investimento_futuro": ["capex","expansão","projetos","guidance","M&A","desalavancagem", "..."],
  "porque": "1 parágrafo objetivo (por que forte/moderada/fraca)",
  "evidencias": [
    {{"fonte":"CVM/IPE","trecho":"<=240 chars","observacao":"por que isso importa"}}
  ]
}}

REGRAS:
- Não invente números. Se não houver, diga explicitamente "não informado".
- Foque em intenção estratégica e direcionamento, não em DFP/ITR.
- Evidências devem vir do contexto fornecido (RAG + texto manual).

[CONTEXTO - RAG]
{context}
{manual_block}
""".strip()


def _get_chunks_for_rag(ticker: str, top_k: int) -> List[Dict[str, Any]]:
    # Import local para não acoplar esta página a uma implementação única
    from page.patch6_teste import get_chunks_for_rag as _old_get_chunks  # type: ignore
    return _old_get_chunks(ticker=ticker, top_k=int(top_k))


def _run_llm(ticker: str, top_k: int, manual_text: str) -> Dict[str, Any]:
    chunks = _get_chunks_for_rag(ticker=ticker, top_k=int(top_k))
    if not chunks:
        return {"ok": False, "error": f"Sem chunks para {ticker}. Rode ingest+chunking antes."}

    parts: List[str] = []
    for c in chunks[::-1]:
        txt = str(c.get("chunk_text", "") or "").strip()
        if txt:
            parts.append(txt[:1800])
    context = "\n\n---\n\n".join(parts)

    prompt = _build_prompt(ticker=ticker, context=context, manual_text=manual_text)

    schema_hint = r"""
{
  "ticker": "STRING",
  "perspectiva_compra": "forte|moderada|fraca",
  "resumo": "STRING",
  "pontos_chave": ["STRING"],
  "riscos_ou_alertas": ["STRING"],
  "sinais_de_investimento_futuro": ["STRING"],
  "porque": "STRING",
  "evidencias": [{"fonte":"STRING","trecho":"STRING","observacao":"STRING"}]
}
""".strip()

    llm = get_llm_client()
    system = """
Você é um analista buy-side, cético e orientado a evidência.
- NÃO invente fatos, números, datas.
- Use APENAS o contexto fornecido.
- Responda OBRIGATORIAMENTE em JSON válido.
""".strip()

    out = llm.generate_json(system=system, user=prompt, schema_hint=schema_hint, context=None)
    return {"ok": True, "result": out, "meta": {"top_k": int(top_k), "chunks_used": len(chunks)}}


def _suggest_weights(items: List[Dict[str, Any]], labels: Dict[str, str], cap: float = 0.30) -> Dict[str, float]:
    """
    Sugestão simples e controlada:
      forte: +20% relativo
      moderada: 0%
      fraca: -20% relativo
    Renormaliza e aplica cap.
    """
    base = {i["ticker"]: float(i.get("peso") or 0.0) for i in items if (i.get("ticker") and i.get("peso") is not None)}
    if not base:
        return {}

    adj = {}
    for tk, w in base.items():
        lab = (labels.get(tk) or "moderada").lower().strip()
        if lab == "forte":
            adj[tk] = w * 1.20
        elif lab == "fraca":
            adj[tk] = w * 0.80
        else:
            adj[tk] = w * 1.00

    s = sum(adj.values())
    if s <= 0:
        return base
    adj = {k: v / s for k, v in adj.items()}

    # cap iterativo
    for _ in range(12):
        over = {k: v for k, v in adj.items() if v > cap}
        if not over:
            break
        excess = sum(v - cap for v in over.values())
        for k in over:
            adj[k] = cap
        under = [k for k, v in adj.items() if v < cap - 1e-12]
        if not under or excess <= 0:
            break
        under_sum = sum(adj[k] for k in under)
        if under_sum <= 0:
            add = excess / len(under)
            for k in under:
                adj[k] += add
        else:
            for k in under:
                adj[k] += excess * (adj[k] / under_sum)

    s = sum(adj.values())
    if s > 0:
        adj = {k: v / s for k, v in adj.items()}
    return adj


def render() -> None:
    st.title("📊 Análises de Portfólio")
    st.caption("Página institucional do Patch 6 (CVM/IPE + RAG + LLM). Não altera backtests da seção Avançada.")

    # Carrega snapshot automaticamente (gating)
    snap = get_latest_snapshot()
    if not snap:
        st.info("Nenhum portfólio salvo. Execute primeiro a página **Criação de Portfólio**.")
        return

    with st.sidebar:
        st.markdown("### 📌 Snapshot")
        snaps_df = list_snapshots(limit=25, status="active")
        # fallback caso tabela ainda não exista
        if snaps_df is not None and not snaps_df.empty:
            opts = [f"{r['created_at']} | {r['id']}" for _, r in snaps_df.iterrows()]
            idx = 0
            chosen = st.selectbox("Escolher snapshot", options=opts, index=idx)
            chosen_id = chosen.split("|")[-1].strip()
            snap = get_snapshot(chosen_id) or snap

        st.markdown("### ⚙️ Execução Patch 6")
        window_months = st.number_input("Janela (meses)", min_value=1, max_value=24, value=12, step=1)
        max_docs_ingest = st.number_input("Máx docs por ticker (ingest)", min_value=5, max_value=200, value=60, step=5)
        strategic_only = st.toggle("Somente estratégicos (heurística)", value=True)
        download_pdfs = st.toggle("Baixar PDFs e extrair texto (sem OCR)", value=True)
        max_pdfs = st.number_input("Máx PDFs por ticker", min_value=0, max_value=50, value=12, step=1)
        only_with_text = st.toggle("Chunk apenas com texto", value=True)

        st.markdown("### 🧠 LLM")
        top_k = st.number_input("Top-K chunks", min_value=5, max_value=120, value=25, step=5)
        manual_text = st.text_area("Texto manual (opcional)", value="", height=120)

    # Snapshot header
    st.subheader("📌 Portfólio carregado")
    st.write(f"Snapshot: `{snap['id']}` | Criado em: {snap.get('created_at')}")
    st.write(f"Margem vs Selic: {snap.get('margem_superior')}% | Selic ref: {snap.get('selic_ref')} | Tipo empresa: {snap.get('tipo_empresa')}")

    items = snap.get("items") or []
    tickers = [str(i.get("ticker")).strip().upper() for i in items if i.get("ticker")]
    tickers = list(dict.fromkeys([t for t in tickers if t]))

    # Tabela simples de composição
    with st.expander("Ver composição do portfólio", expanded=False):
        import pandas as pd
        df = pd.DataFrame([{"ticker": i.get("ticker"), "segmento": i.get("segmento"), "peso_%": (float(i.get("peso") or 0.0) * 100.0)} for i in items])
        st.dataframe(df, use_container_width=True, hide_index=True)

    st.divider()
    colA, colB = st.columns([1, 1], gap="large")

    # A) Ingest + chunking controlado (sem logs)
    with colA:
        st.markdown("### 📥 Atualizar evidências (CVM/IPE)")
        if st.button("Atualizar documentos + chunks", use_container_width=True):
            t0 = time.monotonic()
            with st.spinner("Ingerindo documentos..."):
                try:
                    ingest_ipe_for_tickers(
                        tickers=tickers,
                        window_months=int(window_months),
                        max_docs_per_ticker=int(max_docs_ingest),
                        strategic_only=bool(strategic_only),
                        download_pdfs=bool(download_pdfs),
                        max_pdfs_per_ticker=int(max_pdfs),
                        max_runtime_s=90.0,
                        verbose=False,
                    )
                    st.success("Ingest concluído.")
                except Exception as e:
                    st.error(f"Falha no ingest: {type(e).__name__}: {e}")
                    return

            with st.spinner("Gerando chunks faltantes..."):
                ok_cnt, fail_cnt = 0, 0
                for tk in tickers:
                    try:
                        process_missing_chunks_for_ticker(
                            ticker=tk,
                            limit_docs=50,
                            only_with_text=bool(only_with_text),
                            max_runtime_s=60.0,
                        )
                        ok_cnt += 1
                    except Exception:
                        fail_cnt += 1

            elapsed = time.monotonic() - t0
            st.info(f"Atualização concluída. OK: {ok_cnt} | Falhas: {fail_cnt} | Tempo: {elapsed:.1f}s")

    # B) Rodar LLM para todos (ou por ticker)
    with colB:
        st.markdown("### 🧠 Análise qualitativa (LLM + RAG)")
        mode = st.radio("Modo", ["Rodar para todos", "Rodar por ticker"], horizontal=True)
        chosen_tk = None
        if mode == "Rodar por ticker":
            chosen_tk = st.selectbox("Ticker", options=tickers, index=0)

        if st.button("Rodar análise agora", use_container_width=True):
            to_run = tickers if mode == "Rodar para todos" else [str(chosen_tk)]
            results: Dict[str, Dict[str, Any]] = st.session_state.get("p6_results", {})

            with st.spinner("Rodando LLM..."):
                for tk in to_run:
                    out = _run_llm(tk, int(top_k), manual_text)
                    if out.get("ok"):
                        res = out["result"]
                        results[tk] = res
                        # persistência trimestral (se tabela existir)
                        try:
                            save_patch6_run(snapshot_id=snap["id"], ticker=tk, period_ref=_quarter_ref(datetime.now()), result=res)
                        except Exception:
                            pass
                    else:
                        results[tk] = {"ticker": tk, "perspectiva_compra": "moderada", "resumo": out.get("error", "Falha"), "pontos_chave": [], "riscos_ou_alertas": [], "sinais_de_investimento_futuro": [], "porque": "", "evidencias": []}

            st.session_state["p6_results"] = results
            st.success("Análises atualizadas.")

    # Consolidado
    results2: Dict[str, Dict[str, Any]] = st.session_state.get("p6_results", {})
    if results2:
        labels = {tk: (results2.get(tk, {}) or {}).get("perspectiva_compra", "moderada") for tk in tickers}
        forte = [tk for tk, lb in labels.items() if str(lb).lower().strip() == "forte"]
        fraca = [tk for tk, lb in labels.items() if str(lb).lower().strip() == "fraca"]
        moder = [tk for tk in tickers if tk not in forte and tk not in fraca]

        st.divider()
        st.subheader("📌 Visão consolidada (Patch 6)")
        c1, c2, c3 = st.columns(3)
        c1.metric("Forte", len(forte))
        c2.metric("Moderada", len(moder))
        c3.metric("Fraca", len(fraca))

        st.caption("Classificação baseada em evidências CVM/IPE + RAG. Use como modulação de aporte ou rebalance trimestral controlado.")

        # Peso atual vs sugerido
        suggested = _suggest_weights(items=items, labels=labels, cap=0.30)
        if suggested:
            import pandas as pd
            st.markdown("### ⚖️ Peso atual vs peso sugerido (controle)")
            rows = []
            base_map = {i["ticker"]: float(i.get("peso") or 0.0) for i in items}
            for tk in tickers:
                rows.append({
                    "ticker": tk,
                    "perspectiva": labels.get(tk, "moderada"),
                    "peso_atual_%": base_map.get(tk, 0.0) * 100.0,
                    "peso_sugerido_%": suggested.get(tk, 0.0) * 100.0,
                    "delta_pp": (suggested.get(tk, 0.0) - base_map.get(tk, 0.0)) * 100.0,
                })
            dfw = pd.DataFrame(rows).sort_values(["perspectiva", "peso_sugerido_%"], ascending=[True, False])
            st.dataframe(dfw, use_container_width=True, hide_index=True)

        # Cards por empresa
        st.markdown("### 🧾 Relatórios por empresa")
        for tk in tickers:
            r = results2.get(tk) or {}
            with st.container(border=True):
                st.markdown(f"**{tk}** — perspectiva: **{(r.get('perspectiva_compra') or 'moderada').upper()}**")
                st.write(r.get("resumo") or "")
                cols = st.columns(2)
                with cols[0]:
                    pts = r.get("pontos_chave") or []
                    if pts:
                        st.markdown("**Pontos-chave**")
                        for p in pts[:6]:
                            st.write(f"- {p}")
                with cols[1]:
                    risks = r.get("riscos_ou_alertas") or []
                    if risks:
                        st.markdown("**Riscos/Alertas**")
                        for p in risks[:6]:
                            st.write(f"- {p}")

                evs = r.get("evidencias") or []
                if evs:
                    with st.expander("Evidências (trechos)", expanded=False):
                        for ev in evs[:10]:
                            trecho = (ev or {}).get("trecho", "")
                            obs = (ev or {}).get("observacao", "")
                            st.write(f"- {trecho}")
                            if obs:
                                st.caption(obs)

                # Histórico trimestral
                with st.expander("Histórico trimestral (se disponível)", expanded=False):
                    try:
                        h = list_patch6_history(tk, limit=8)
                        if h is None or h.empty:
                            st.caption("Sem histórico salvo para este ticker.")
                        else:
                            st.dataframe(h, use_container_width=True, hide_index=True)
                    except Exception:
                        st.caption("Histórico indisponível (tabela patch6_runs não criada).")

    else:
        st.info("Rode a análise LLM para ver a visão consolidada e os relatórios.")
