# -*- coding: utf-8 -*-
"""
page/analises_portfolio.py

Patch 6 — Página padrão com LOGS completos (Ingest + Chunking) por ticker.

Por que isso existe:
- "chunks = 0" geralmente NÃO é erro do chunking, é falta de documentos no Supabase.
- O botão anterior estava rodando apenas chunking, então tickers com docs=0 "passavam rápido"
  e não mostravam motivo.

Agora:
- Para cada ticker: roda Ingest (CVM/IPE) -> mostra relatório -> roda Chunking -> mostra resultado
- Se docs continuar 0 após ingest, você verá isso explicitamente e o relatório do ingest
"""

from __future__ import annotations

import os
import json
import html
import time
import math
import traceback
import importlib
import inspect
from typing import Any, Dict, List, Optional, Callable, Tuple

import streamlit as st
import pandas as pd

from core.helpers import get_logo_url

from core.portfolio_snapshot_store import get_latest_snapshot
from core.docs_corporativos_store import (
    count_docs,
    count_chunks,
)
from core.patch6_store import process_missing_chunks_for_ticker
from core.patch6_runs_store import save_patch6_run, list_patch6_history
from core.patch6_writer import build_result_json

import core.ai_models.llm_client.factory as llm_factory


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def _now_ms() -> int:
    return int(time.time() * 1000)

def _fmt_s(ms: int) -> str:
    return f"{ms/1000:.1f}s"

def _safe_upper(x: Any) -> str:
    return str(x or "").strip().upper()

def _fmt_pct(x: Any, default: str = "—") -> str:
    try:
        if x is None:
            return default
        v = float(x)
        # aceita 0.12 ou 12.0
        if abs(v) <= 1.5:
            v *= 100.0
        return f"{v:.2f}%"
    except Exception:
        return default

def _fmt_pp(x: Any, default: str = "—") -> str:
    try:
        if x is None:
            return default
        v = float(x)
        if abs(v) <= 1.5:
            v *= 100.0
        return f"{v:.2f} p.p."
    except Exception:
        return default


def _calc_budget_topk(num_chunks: int, peso: float, cap_max: int) -> dict:
    """Define budget adaptativo de Top-K.

    Retorna dict com:
      - budget_raw: antes do cap
      - budget_used: após cap
      - base: base por faixa de chunks
      - peso_mult: multiplicador por peso
    """
    try:
        n = int(num_chunks or 0)
    except Exception:
        n = 0

    # Base por tamanho do corpus
    if n < 120:
        base = 10
    elif n < 500:
        base = 20
    elif n < 1500:
        base = 35
    else:
        base = 50

    try:
        p = float(peso or 0.0)
    except Exception:
        p = 0.0

    if p >= 0.15:
        peso_mult = 1.2
    elif p >= 0.05:
        peso_mult = 1.1
    else:
        peso_mult = 1.0

    budget_raw = int(math.ceil(base * peso_mult))
    budget_used = int(max(3, min(int(cap_max or budget_raw), budget_raw)))
    return {"base": base, "peso_mult": peso_mult, "budget_raw": budget_raw, "budget_used": budget_used}




def _classify_corpus_health(num_docs: int, num_chunks: int) -> Dict[str, Any]:
    """Score simples de saúde do corpus usando métricas já disponíveis sem alterar o core."""
    try:
        docs = int(num_docs or 0)
    except Exception:
        docs = 0
    try:
        chunks = int(num_chunks or 0)
    except Exception:
        chunks = 0

    ratio = (chunks / docs) if docs > 0 else 0.0

    # score por volume
    score_docs = min(docs / 20.0, 1.0) * 30.0
    score_chunks = min(chunks / 500.0, 1.0) * 50.0

    # score por granularidade do chunking
    if docs == 0:
        score_ratio = 0.0
    elif ratio < 1.2:
        score_ratio = 0.0
    elif ratio < 3:
        score_ratio = 8.0
    elif ratio < 8:
        score_ratio = 14.0
    else:
        score_ratio = 20.0

    score = round(score_docs + score_chunks + score_ratio, 1)

    if docs == 0:
        diag = "Sem documentos"
    elif chunks == 0:
        diag = "Sem chunks"
    elif ratio <= 1.2:
        diag = "Anomalia de chunking"
    elif score < 40:
        diag = "Corpus fraco"
    elif score < 70:
        diag = "Corpus razoável"
    else:
        diag = "Corpus robusto"

    observacao = ""
    if docs == 0:
        observacao = "Ticker sem base documental no banco."
    elif chunks == 0:
        observacao = "Há documentos, mas ainda não foram fragmentados em chunks."
    elif ratio <= 1.2:
        observacao = "Quantidade de chunks muito próxima da quantidade de documentos. Isso sugere 1 chunk por documento ou extração curta demais."
    elif ratio < 3:
        observacao = "Granularidade ainda baixa. Vale revisar chunk_size/chunk_overlap em documentos extensos."
    elif chunks > 3000:
        observacao = "Corpus muito volumoso. Convém monitorar redundância e diversidade do retrieval."
    else:
        observacao = "Cobertura operacional aceitável para o estágio atual."

    return {
        "score": score,
        "diag": diag,
        "ratio": round(ratio, 2),
        "observacao": observacao,
    }


def _make_ingest_results_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows or [])
    if df.empty:
        return df

    rename_map = {
        "ticker": "Ticker",
        "status": "Status",
        "docs_before": "Docs antes",
        "chunks_before": "Chunks antes",
        "docs_after_ingest": "Docs após ingestão",
        "chunks_after_ingest": "Chunks após ingestão",
        "chunks_inseridos": "Novos chunks",
        "chunks_after": "Total de chunks",
        "tempo": "Tempo",
        "motivo": "Observação",
    }
    df = df.rename(columns=rename_map)

    order = [
        "Ticker",
        "Status",
        "Docs antes",
        "Chunks antes",
        "Docs após ingestão",
        "Chunks após ingestão",
        "Novos chunks",
        "Total de chunks",
        "Tempo",
        "Observação",
    ]
    df = df[[c for c in order if c in df.columns]]

    if "Status" in df.columns:
        df["Status"] = df["Status"].replace({
            "OK": "OK",
            "SEM_DOCS": "Sem documentos",
            "SEM_CHUNKS": "Sem chunks",
            "FALHA_CHUNK": "Falha no chunking",
        })

    return df


def _make_corpus_health_df(tickers: List[str], weight_map: Dict[str, float]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for tk in tickers:
        docs = count_docs(tk)
        chunks = count_chunks(tk)
        health = _classify_corpus_health(docs, chunks)
        rows.append({
            "Ticker": tk,
            "Peso no portfólio": round(float(weight_map.get(tk, 0.0) or 0.0) * 100.0, 2),
            "Documentos": int(docs or 0),
            "Chunks": int(chunks or 0),
            "Chunks por documento": health["ratio"],
            "Score do corpus": health["score"],
            "Diagnóstico": health["diag"],
            "Leitura operacional": health["observacao"],
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df["Peso no portfólio"] = df["Peso no portfólio"].map(lambda x: f"{x:.2f}%")
        df = df.sort_values(by=["Score do corpus", "Chunks"], ascending=[False, False]).reset_index(drop=True)
    return df


def _summarize_health_df(df: pd.DataFrame) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"forte": 0, "razoavel": 0, "fraco": 0, "anomalia": 0}

    diags = df["Diagnóstico"].astype(str).tolist()
    return {
        "forte": sum(1 for x in diags if x == "Corpus robusto"),
        "razoavel": sum(1 for x in diags if x == "Corpus razoável"),
        "fraco": sum(1 for x in diags if x in {"Corpus fraco", "Sem documentos", "Sem chunks"}),
        "anomalia": sum(1 for x in diags if x == "Anomalia de chunking"),
    }


def _render_saved_data_header_html(selic: Any, n_acoes: int, margem: Any, n_segmentos: int) -> str:
    return f'''
    <div class="p6-saved">
        <div class="p6-saved-title">📌 Dados salvos</div>
        <div class="p6-saved-meta">
            <span class="p6-pill">Selic usada: <b>{_fmt_pct(selic)}</b></span>
            <span class="p6-pill">Ações: <b>{n_acoes}</b></span>
            <span class="p6-pill">Acima do benchmark: <b>{_fmt_pp(margem)}</b></span>
            <span class="p6-pill">Segmentos: <b>{n_segmentos}</b></span>
        </div>
    </div>
    '''

def _render_ticker_chips_html(tickers: List[str]) -> str:
    # Chips com logo + ticker (mesmo padrão visual da seção Básica)
    parts: List[str] = ['<div class="p6-chips">']
    for t in tickers:
        url = get_logo_url(t)
        parts.append(
            f'''<span class="p6-chip">
                    <img src="{url}" alt="{t}" onerror="this.style.display='none';"/>
                    <span class="tck">{t}</span>
                </span>'''
        )
    parts.append("</div>")
    return "".join(parts)

def _import_first(*module_paths: str):
    errors = []
    for p in module_paths:
        try:
            return importlib.import_module(p)
        except Exception as e:
            errors.append((p, e))
    msg = "Falha ao importar módulos. Tentativas:\n" + "\n".join([f"- {p}: {repr(e)}" for p, e in errors])
    raise ImportError(msg)

def _import_ingest():
    """
    Carrega ingest diretamente do arquivo físico,
    ignorando problemas de PYTHONPATH no Streamlit Cloud.
    """
    import importlib.util
    from pathlib import Path

    # sobe de page/ para raiz do projeto
    base_dir = Path(__file__).resolve().parents[1]
    ingest_path = base_dir / "pickup" / "ingest_docs_cvm_ipe.py"

    if not ingest_path.exists():
        raise ImportError(f"Arquivo não encontrado: {ingest_path}")

    spec = importlib.util.spec_from_file_location(
        "ingest_docs_cvm_ipe",
        str(ingest_path)
    )

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    fn = getattr(module, "ingest_ipe_for_tickers", None)

    if not callable(fn):
        raise ImportError(
            "Função ingest_ipe_for_tickers não encontrada em ingest_docs_cvm_ipe.py"
        )

    return fn
    raise ImportError("Não encontrei função de ingest no módulo pickup.ingest_docs_cvm_ipe (ou fallbacks).")

def _safe_call(fn: Callable[..., Any], **kwargs):
    """
    Chama função adaptando para assinaturas diferentes.
    """
    try:
        sig = inspect.signature(fn)
        accepted = {k: v for k, v in kwargs.items() if k in sig.parameters}

        # alias comuns
        # ticker
        if "ticker" in kwargs and "ticker" not in accepted:
            for alt in ("tk", "symbol", "ticker_str"):
                if alt in sig.parameters:
                    accepted[alt] = kwargs["ticker"]
                    break



        # tickers (lista)
        if "tickers" in kwargs and "tickers" not in accepted:
            for alt in ("symbols", "ticker_list"):
                if alt in sig.parameters:
                    accepted[alt] = kwargs["tickers"]
                    break

        # months window
        if "window_months" in kwargs and "window_months" not in accepted:
            for alt in ("months", "months_window", "janela_meses"):
                if alt in sig.parameters:
                    accepted[alt] = kwargs["window_months"]
                    break

        # max docs
        if "max_docs" in kwargs and "max_docs" not in accepted:
            for alt in ("limit_docs", "max_docs_per_ticker", "limite_docs"):
                if alt in sig.parameters:
                    accepted[alt] = kwargs["max_docs"]
                    break

        # max runtime
        if "max_runtime_s" in kwargs and "max_runtime_s" not in accepted:
            for alt in ("timeout_s", "runtime_s", "time_budget_s"):
                if alt in sig.parameters:
                    accepted[alt] = kwargs["max_runtime_s"]
                    break

        # max pdfs
        if "max_pdfs" in kwargs and "max_pdfs" not in accepted:
            for alt in ("max_pdfs_per_ticker", "limite_pdfs"):
                if alt in sig.parameters:
                    accepted[alt] = kwargs["max_pdfs"]
                    break

        return fn(**accepted)
    except Exception:
        # fallback: tenta direto
        return fn(**kwargs)



import re

def _clip(s: str, max_chars: int) -> str:
    s = (s or "").strip()
    if len(s) <= max_chars:
        return s
    return s[:max_chars].rstrip() + " …"

def _build_context_limited(chunks: List[str], per_chunk_chars: int = 1200, total_chars: int = 12000) -> str:
    parts: List[str] = []
    used = 0
    for i, ch in enumerate(chunks or [], start=1):
        piece = _clip(str(ch), per_chunk_chars)
        block = f"[CHUNK {i}]\n{piece}\n"
        if used + len(block) > total_chars:
            break
        parts.append(block)
        used += len(block)
    return "\n".join(parts)

def _dedupe_keep_order(texts: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for t in texts or []:
        key = (t or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out

def _subtract_chunks(base: List[str], already_seen: List[str]) -> List[str]:
    seen = {(x or "").strip() for x in already_seen or [] if (x or "").strip()}
    out: List[str] = []
    for t in base or []:
        key = (t or "").strip()
        if not key or key in seen:
            continue
        out.append(key)
    return out

def _build_temporal_context(sections: Dict[str, List[Any]], per_chunk_chars: int = 1100, total_chars: int = 20000) -> str:
    labels = [
        ("janela_0_12m", "JANELA RECENTE (0-12 meses)"),
        ("janela_12_24m", "JANELA INTERMEDIÁRIA (12-24 meses)"),
        ("janela_24_36m", "JANELA ANTIGA (24-36 meses)"),
    ]
    parts: List[str] = []
    used = 0
    idx_global = 1

    for key, title in labels:
        chunks = sections.get(key) or []
        if not chunks:
            continue

        header = f"\n### {title}\n"
        if used + len(header) > total_chars:
            break
        parts.append(header)
        used += len(header)

        for ch in chunks:
            if isinstance(ch, dict):
                text_value = str(ch.get("text") or "").strip()
                data_doc = str(ch.get("data_doc") or "").strip()
                tipo_doc = str(ch.get("tipo_doc") or "").strip()
                theme = str(ch.get("theme") or "").strip()
                meta = " | ".join([x for x in [data_doc, tipo_doc, theme] if x])
                meta_line = f"[META] {meta}\n" if meta else ""
            else:
                text_value = str(ch or "").strip()
                meta_line = ""

            piece = _clip(text_value, per_chunk_chars)
            block = f"[CHUNK {idx_global}]\n{meta_line}{piece}\n"
            if used + len(block) > total_chars:
                break
            parts.append(block)
            used += len(block)
            idx_global += 1

    return "\n".join(parts).strip()

def _parse_json_loose(text: str) -> Dict[str, Any]:
    t = (text or "").strip()

    # remove fences ```json
    t = re.sub(r"^```(?:json)?\s*", "", t, flags=re.IGNORECASE).strip()
    t = re.sub(r"\s*```$", "", t).strip()

    # direct parse
    try:
        return json.loads(t)
    except Exception:
        pass

    # extract first {...}
    m = re.search(r"\{.*\}", t, flags=re.DOTALL)
    if not m:
        raise ValueError("LLM não retornou JSON")
    return json.loads(m.group(0))


def render() -> None:
    st.title("🧠 Análises de Portfólio")

    # CSS institucional (header + cards + chips + cards LLM)
    st.markdown(
        """
        <style>
          .p6-header{
            border:1px solid rgba(255,255,255,.10);
            border-radius:18px;
            padding:16px 18px;
            background:rgba(255,255,255,.03);
            margin:10px 0 12px 0;
            box-shadow:0 10px 24px rgba(0,0,0,.22);
          }
          .p6-header .p6-title{font-size:28px;font-weight:900;letter-spacing:.2px;margin:0}
          .p6-header .p6-sub{opacity:.78;margin-top:6px;font-size:14px}
          .p6-pill-mini{
            display:inline-block;
            padding:5px 10px;
            border-radius:999px;
            border:1px solid rgba(255,255,255,.12);
            background:rgba(255,255,255,.04);
            font-size:12px;
            margin-top:10px;
          }

          .p6-mcard{
            border:1px solid rgba(255,255,255,.10);
            border-radius:16px;
            padding:14px 14px 12px 14px;
            background:rgba(255,255,255,.03);
            box-shadow:0 10px 24px rgba(0,0,0,.18);
            height:100%;
          }
          .p6-mlabel{opacity:.78;font-size:12px;margin-bottom:6px}
          .p6-mvalue{font-size:22px;font-weight:900;letter-spacing:.2px}
          .p6-mextra{opacity:.70;font-size:12px;margin-top:6px;line-height:1.25}

          .p6-chips{display:flex;flex-wrap:wrap;gap:8px;margin:10px 0 14px 0}
          .p6-chip{
            display:inline-flex;
            align-items:center;
            gap:12px;
            padding:10px 16px;
            border-radius:14px;
            border:1px solid rgba(255,255,255,0.10);
            background: rgba(255,255,255,0.04);
            box-shadow: 0 8px 20px rgba(0,0,0,0.25);
          }
          .p6-chip img{
            width:75px;
            height:75px;
            object-fit:contain;
            border-radius:10px;
            background:#ffffff;
            padding:5px;
          }
          .p6-chip .tck{
            font-weight:800;
            font-size:14px;
            letter-spacing:0.3px;
          }

          /* cards por empresa (LLM) */
          .p6-card{border:1px solid rgba(255,255,255,.10);border-radius:16px;padding:16px 16px 12px 16px;
                   background:rgba(255,255,255,.03);margin:12px 0;box-shadow:0 10px 24px rgba(0,0,0,.18);}
          .p6-head{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:10px}
          .p6-title-sm{font-size:18px;font-weight:900;letter-spacing:.2px}
          .p6-badges{display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end}
          .p6-pill{font-size:12px;padding:4px 10px;border-radius:999px;border:1px solid rgba(255,255,255,.10);opacity:.95}
          .p6-pill-forte{background:rgba(34,197,94,.15);border-color:rgba(34,197,94,.35)}
          .p6-pill-moderada{background:rgba(234,179,8,.15);border-color:rgba(234,179,8,.35)}
          .p6-pill-fraca{background:rgba(239,68,68,.15);border-color:rgba(239,68,68,.35)}
          .p6-pill-info{background:rgba(59,130,246,.12);border-color:rgba(59,130,246,.30)}
          .p6-grid{display:grid;grid-template-columns:1fr;gap:10px}
          .p6-k{font-weight:800}
          .p6-muted{opacity:.78}
          .p6-list{margin:6px 0 0 18px}
          .p6-hr{height:1px;background:rgba(255,255,255,.08);border:none;margin:12px 0}
        </style>
        """,
        unsafe_allow_html=True,
    )


    snapshot = get_latest_snapshot()
    if not snapshot:
        st.warning("Nenhum snapshot ativo encontrado. Execute primeiro a Criação de Portfólio.")
        st.stop()

    
    snapshot_id = str(snapshot.get("id") or "")

    # Dados salvos (sem expor o hash)
    selic_used = snapshot.get("selic") or snapshot.get("selic_ref") or snapshot.get("selic_aa")
    # margem acima do benchmark (p.p.) — compatível com versões antigas e novas
    margem_bench = (
        snapshot.get("margem_sobre_benchmark")
        or snapshot.get("margem_minima")
        or snapshot.get("margem_min")
        or snapshot.get("margem")
        or snapshot.get("margem_superior")
    )

    # Compatibilidade: algumas versões retornam tickers direto em snapshot["tickers"],
    # enquanto a versão atual usa snapshot["items"] (portfolio_snapshot_items).
    raw_list = snapshot.get("items") or snapshot.get("tickers") or []

    # Normaliza para lista de dicts com chave "ticker"
    if raw_list and isinstance(raw_list, list) and raw_list and isinstance(raw_list[0], str):
        raw_list = [{"ticker": t} for t in raw_list]

    items = raw_list if isinstance(raw_list, list) else []
    tickers = [_safe_upper(it.get("ticker")) for it in items if _safe_upper(it.get("ticker"))]
    tickers = sorted(list(dict.fromkeys(tickers)))

    # Mapa de pesos por ticker (quando disponível no snapshot_items)
    weight_map: Dict[str, float] = {}
    for it in items:
        tk = _safe_upper(it.get("ticker"))
        if not tk:
            continue
        try:
            weight_map[tk] = float(it.get("peso") or it.get("weight") or it.get("allocation") or 0.0)
        except Exception:
            weight_map[tk] = 0.0



    # Métricas do portfólio (para o cabeçalho "Dados salvos")
    # Segmentos cobertos:
    # - Preferência: info por item (quando disponível)
    # - Fallback: lista pré-calculada salva no snapshot (filters_json["segmentos"]) — evita mudar schema
    seg_values: list[str] = []
    for it in items:
        seg = it.get("segmento") or it.get("setor") or it.get("sector") or it.get("segment")
        if seg:
            seg_values.append(str(seg).strip())

    if not seg_values:
        fj = snapshot.get("filters_json") or {}
        segs_saved = fj.get("segmentos") or fj.get("segments")
        if isinstance(segs_saved, (list, tuple, set)):
            seg_values = [str(x).strip() for x in segs_saved if str(x).strip()]
        elif isinstance(segs_saved, str) and segs_saved.strip():
            raw = segs_saved.replace(";", ",")
            seg_values = [s.strip() for s in raw.split(",") if s.strip()]

    n_segmentos = len(set([s for s in seg_values if s]))

    # Header institucional + cards de resumo do snapshot
    st.markdown(
    f"""
    <div class="p6-header">
      <div class="p6-title">📌 Dados salvos</div>
      <div class="p6-sub">
        Snapshot do portfólio • parâmetros utilizados na seleção (Criação de Portfólio).
      </div>
      <span class="p6-pill-mini">Snapshot ativo</span>
    </div>
    """,
    unsafe_allow_html=True,
)

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(
    f"""
    <div class="p6-mcard">
      <div class="p6-mlabel">Selic usada (benchmark)</div>
      <div class="p6-mvalue">{_fmt_pct(selic_used)}</div>
      <div class="p6-mextra">Taxa de referência salva no snapshot.</div>
    </div>
    """,
    unsafe_allow_html=True,
)
    c2.markdown(
    f"""
    <div class="p6-mcard">
      <div class="p6-mlabel">Ações selecionadas</div>
      <div class="p6-mvalue">{len(tickers)}</div>
      <div class="p6-mextra">Quantidade de ativos no portfólio.</div>
    </div>
    """,
    unsafe_allow_html=True,
)
    c3.markdown(
    f"""
    <div class="p6-mcard">
      <div class="p6-mlabel">Acima do benchmark</div>
      <div class="p6-mvalue">{_fmt_pp(margem_bench)}</div>
      <div class="p6-mextra">Margem mínima configurada na criação.</div>
    </div>
    """,
    unsafe_allow_html=True,
)
    c4.markdown(
    f"""
    <div class="p6-mcard">
      <div class="p6-mlabel">Segmentos cobertos</div>
      <div class="p6-mvalue">{n_segmentos}</div>
      <div class="p6-mextra">Diversificação por segmento/setor.</div>
    </div>
    """,
    unsafe_allow_html=True,
)
    st.markdown(_render_ticker_chips_html(tickers), unsafe_allow_html=True)

    st.divider()

    # ------------------------------------------------------------------
    # Estado (sanidade)
    # ------------------------------------------------------------------

    # Fonte única de verdade para profundidade temporal.
    # Lemos do session_state para que a janela de evidências fique sempre
    # sincronizada com o modo de análise selecionado na seção qualitativa.
    analysis_mode = st.session_state.get("analysis_mode", "Padrão (24 meses)")
    analysis_window_months = 24 if analysis_mode == "Padrão (24 meses)" else 36
    analysis_period_ref = "24M" if analysis_window_months == 24 else "36M"

    # ------------------------------------------------------------------
    # Ingest + Chunking com logs por ticker
    # ------------------------------------------------------------------
    st.subheader("📦 Atualizar evidências")

    col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
    with col1:
        st.markdown(
            f"""
            <div class="p6-mcard">
              <div class="p6-mlabel">Janela de evidências</div>
              <div class="p6-mvalue">{analysis_window_months} meses</div>
              <div class="p6-mextra">Sincronizada automaticamente com o modo de análise qualitativa.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col2:
        max_docs = st.number_input("Máx docs/ticker", min_value=5, max_value=300, value=80, step=5)
    with col3:
        max_pdfs = st.number_input("Máx PDFs/ticker", min_value=0, max_value=80, value=20, step=1)
    with col4:
        max_runtime_s = st.number_input("Tempo máx total (s)", min_value=5, max_value=180, value=60, step=5)

    st.caption(f"Atualizar documentos sempre tentará expandir o corpus até o limite configurado, usando automaticamente {analysis_window_months} meses de histórico.")

    show_traceback = False

    # Diagnóstico sob demanda (não altera pipeline; apenas inspeciona presença de docs/chunks)
    diag_btn = st.button("🩺 Diagnóstico (docs/chunks)", help="Mostra rapidamente se cada ticker tem docs e chunks no Supabase.")
    health_btn = st.button("🧬 Saúde do corpus", help="Avalia a qualidade operacional do corpus por ticker e destaca possíveis gargalos de chunking.")

    if diag_btn:
        diag_rows: List[Dict[str, Any]] = []
        total_docs = 0
        total_chunks = 0
        missing_docs = 0
        missing_chunks = 0

        for tk in tickers:
            d = count_docs(tk)
            c = count_chunks(tk)
            total_docs += int(d or 0)
            total_chunks += int(c or 0)
            if (d or 0) == 0:
                missing_docs += 1
            if (c or 0) == 0:
                missing_chunks += 1

            diag_rows.append({
                "ticker": tk,
                "docs": d,
                "chunks": c,
                "status": ("OK" if (d or 0) > 0 and (c or 0) > 0 else
                           "SEM_DOCS" if (d or 0) == 0 else
                           "SEM_CHUNKS"),
            })

        st.markdown(
            f"**Resumo diagnóstico:** docs={total_docs} | chunks={total_chunks} | tickers sem docs={missing_docs} | tickers sem chunks={missing_chunks}"
        )
        st.dataframe(pd.DataFrame(diag_rows).rename(columns={"ticker":"Ticker","docs":"Documentos","chunks":"Chunks","status":"Status"}), use_container_width=True)

        if missing_docs > 0:
            st.warning("Há tickers sem documentos. Use **Atualizar documentos** para tentar ingerir CVM/IPE.")
        elif missing_chunks > 0:
            st.warning("Há tickers com docs mas sem chunks. Use **Atualizar documentos** (ele também processa chunks faltantes).")
        else:
            st.success("Todos os tickers têm docs e chunks. Pode rodar o LLM com evidências completas.")


    if health_btn:
        health_df = _make_corpus_health_df(tickers, weight_map)
        summary = _summarize_health_df(health_df)

        st.markdown(
            f"**Resumo saúde do corpus:** robusto={summary['forte']} | razoável={summary['razoavel']} | fraco={summary['fraco']} | anomalias de chunking={summary['anomalia']}"
        )

        if not health_df.empty:
            st.dataframe(health_df, use_container_width=True)

            anom_df = health_df[health_df["Diagnóstico"] == "Anomalia de chunking"]
            if not anom_df.empty:
                st.warning(
                    "Foram detectados tickers com possível anomalia de chunking. "
                    "Quando a razão chunks/documento fica muito próxima de 1, o sistema pode estar gerando apenas 1 chunk por documento "
                    "ou extraindo texto curto demais do PDF."
                )

                if "PETR3" in anom_df["Ticker"].tolist():
                    petr_row = anom_df[anom_df["Ticker"] == "PETR3"].iloc[0].to_dict()
                    st.error(
                        f"PETR3 exige revisão. Hoje ele aparece com {petr_row['Documentos']} documentos e {petr_row['Chunks']} chunks "
                        f"(razão {petr_row['Chunks por documento']}). Isso não é normal para um emissor desse porte e sugere gargalo de chunking ou extração textual."
                    )
                    st.caption(
                        "Hipóteses mais prováveis: chunk_size alto demais, chunk_overlap baixo ou extração de texto muito curta/ruim em PDFs da Petrobras."
                    )

    btn = st.button("Atualizar documentos", type="primary")

    log_panel = st.empty()
    table_panel = st.empty()
    err_panel = st.empty()

    def _extract_ingest_summary(report: Optional[Dict[str, Any]], ticker: str) -> Dict[str, Any]:
        if not isinstance(report, dict):
            return {}
        stats = report.get("stats") if isinstance(report.get("stats"), dict) else {}
        per_ticker = stats.get(ticker) if isinstance(stats.get(ticker), dict) else {}
        merged: Dict[str, Any] = {}
        merged.update({k: v for k, v in report.items() if k != "stats"})
        merged.update(per_ticker)
        return merged

    if btn:
        # carrega ingest uma vez
        try:
            ingest_fn = _import_ingest()
        except Exception as e:
            st.error("Não consegui importar o módulo de ingest do CVM/IPE no deploy.")
            st.code(str(e))
            st.stop()

        t0 = _now_ms()
        results: List[Dict[str, Any]] = []
        errors: Dict[str, str] = {}

        progress = st.progress(0, text="Iniciando...")

        for i, tk in enumerate(tickers, start=1):
            start = _now_ms()
            before_docs = count_docs(tk)
            before_chunks = count_chunks(tk)

            progress.progress(int((i - 1) / max(1, len(tickers)) * 100), text=f"Processando {i}/{len(tickers)} — {tk}")

            with log_panel.container():
                st.info(f"🔎 {tk} — início | docs={before_docs} | chunks={before_chunks}")

            ingest_report: Optional[Dict[str, Any]] = None
            ingest_ran = False

            # ---- Ingest (sempre tenta expandir o corpus até o limite configurado)
            try:
                ingest_ran = True
                r = _safe_call(
                    ingest_fn,
                    tickers=[tk],
                    window_months=int(analysis_window_months),
                    max_docs_per_ticker=int(max_docs),
                    max_runtime_s=float(max_runtime_s),
                    max_pdfs_per_ticker=int(max_pdfs),
                )
                # normaliza relatório
                if isinstance(r, dict):
                    ingest_report = r
                else:
                    ingest_report = {"result": str(r)}
            except Exception as e:
                tb = traceback.format_exc()
                msg = f"Ingest {type(e).__name__}: {e}"
                errors[f"{tk}::ingest"] = tb if show_traceback else msg
                ingest_report = {"error": msg}
                with log_panel.container():
                    st.error(f"❌ {tk} — ingest falhou | {msg}")

            mid_docs = count_docs(tk)
            mid_chunks = count_chunks(tk)

            ingest_summary = _extract_ingest_summary(ingest_report, tk)

            with log_panel.container():
                st.write(f"📥 {tk} — ingest concluído | docs agora={mid_docs} | chunks={mid_chunks}")
                if ingest_summary:
                    st.caption("Relatório ingest (resumo):")
                    st.json({
                        k: ingest_summary.get(k)
                        for k in [
                            "existing_before",
                            "matched",
                            "dataset_candidates",
                            "considered",
                            "inserted",
                            "skipped",
                            "updated_text",
                            "pdf_fetched",
                            "pdf_text_ok",
                            "requested_max_docs",
                            "requested_max_pdfs",
                            "selection_truncated",
                            "pdf_limit_hit",
                            "stopped_reason",
                            "error",
                            "reason",
                            "result",
                        ]
                        if ingest_summary.get(k) is not None
                    })

            # Se ainda não tem docs, explique claramente e pule chunking
            if mid_docs == 0:
                results.append({
                    "ticker": tk,
                    "status": "SEM_DOCS",
                    "docs_before": before_docs,
                    "chunks_before": before_chunks,
                    "docs_after_ingest": mid_docs,
                    "chunks_after_ingest": mid_chunks,
                    "chunks_inseridos": 0,
                    "chunks_after": mid_chunks,
                    "tempo": _fmt_s(_now_ms() - start),
                    "motivo": (ingest_report.get("reason") if isinstance(ingest_report, dict) else "") or "Sem documentos retornados para a janela/fonte atual.",
                })
                with log_panel.container():
                    st.warning(
                        f"⚠️ {tk} — sem docs após ingest. "
                        f"Isso explica a execução rápida e ausência de chunks. "
                        f"Verifique janela (meses), filtros do ingest e disponibilidade de documentos no CVM/IPE."
                    )
                table_panel.dataframe(_make_ingest_results_df(results), use_container_width=True)
                continue

            # ---- Chunking
            try:
                chunk_report = process_missing_chunks_for_ticker(
                    tk,
                    limit_docs=int(max_docs),
                    chunk_size=1500,
                )
                inserted = int(chunk_report.get("chunks_inserted", 0))
                after_docs = count_docs(tk)
                after_chunks = count_chunks(tk)

                results.append({
                    "ticker": tk,
                    "status": "OK",
                    "docs_before": before_docs,
                    "chunks_before": before_chunks,
                    "docs_after_ingest": mid_docs,
                    "chunks_after_ingest": mid_chunks,
                    "chunks_inseridos": int(inserted),
                    "chunks_after": after_chunks,
                    "tempo": _fmt_s(_now_ms() - start),
                    "motivo": "",
                })

                with log_panel.container():
                    st.success(f"✅ {tk} — chunking ok | +{inserted} chunks | chunks={after_chunks} | {_fmt_s(_now_ms()-start)}")

            except Exception as e:
                tb = traceback.format_exc()
                msg = f"Chunking {type(e).__name__}: {e}"
                errors[f"{tk}::chunking"] = tb if show_traceback else msg

                results.append({
                    "ticker": tk,
                    "status": "FALHA_CHUNK",
                    "docs_before": before_docs,
                    "chunks_before": before_chunks,
                    "docs_after_ingest": mid_docs,
                    "chunks_after_ingest": mid_chunks,
                    "chunks_inseridos": 0,
                    "chunks_after": None,
                    "tempo": _fmt_s(_now_ms() - start),
                    "motivo": msg,
                })

                with log_panel.container():
                    st.error(f"❌ {tk} — chunking falhou | {msg} | {_fmt_s(_now_ms()-start)}")

            table_panel.dataframe(_make_ingest_results_df(results), use_container_width=True)

        progress.progress(100, text="Concluído")
        st.success(f"Fim. Tempo total: {_fmt_s(_now_ms() - t0)}")

        if errors:
            with err_panel.container():
                st.subheader("🧾 Logs de erro (por etapa)")
                for key, tb in errors.items():
                    with st.expander(key):
                        st.code(tb)

    st.divider()

    # ------------------------------------------------------------------
    # LLM
    # ------------------------------------------------------------------
    
    # ------------------------------------------------------------------
    # LLM (RAG + julgamento qualitativo)
    # ------------------------------------------------------------------
    st.subheader("🤖 Análise qualitativa")
    if not tickers:
        st.info("Sem tickers no snapshot.")
        return
    def _pill_class(p: str) -> str:
        p = (p or "").strip().lower()
        if p == "forte":
            return "p6-pill p6-pill-forte"
        if p == "moderada":
            return "p6-pill p6-pill-moderada"
        return "p6-pill p6-pill-fraca"

    def _as_list(x: Any) -> List[str]:
        if x is None:
            return []
        if isinstance(x, list):
            return [str(i) for i in x if str(i).strip()]
        if isinstance(x, str):
            s = x.strip()
            return [s] if s else []
        return [str(x)]

    def _render_card(ticker: str, result: Dict[str, Any], top_k_used: int, period_ref: str) -> None:
      def esc(x: Any) -> str:
          return html.escape("" if x is None else str(x).strip())

      persp_raw = (result.get("perspectiva_compra", "") or "").strip()
      resumo_raw = (result.get("resumo", "") or "").strip()
      consider_raw = (
          result.get("consideracoes_llm")
          or result.get("consideracoes")
          or result.get("observacoes")
          or result.get("rationale")
          or ""
      )
      confianca_raw = result.get("confianca", result.get("confidence", ""))

      evolucao_raw = result.get("evolucao_estrategica", "") or ""
      consistencia_raw = result.get("consistencia_do_discurso", "") or result.get("consistencia_entre_periodos", "") or ""
      execucao_raw = result.get("execucao_vs_promessa", "") or ""
      mudancas_raw = result.get("mudancas_estrategicas", "") or result.get("mudancas_capex_divida_dividendos_ma", "") or ""
      tese_final_raw = result.get("tese_final", "") or ""

      pontos = _as_list(result.get("pontos_chave") or result.get("pontos-chave") or result.get("pontos"))
      riscos = _as_list(result.get("riscos"))
      sinais = _as_list(result.get("sinais_recorrentes"))
      evid = _as_list(result.get("evidencias") or result.get("evidence") or result.get("citacoes"))

      docs_usados = result.get("docs_usados") or result.get("docs_used") or result.get("documentos") or None
      evid_usadas = result.get("evid_usadas") or result.get("chunks_used") or result.get("evidencias_usadas") or None

      ticker_e = esc(ticker)
      persp_e = esc(persp_raw)
      resumo_e = esc(resumo_raw)
      consider_e = esc(consider_raw)
      confianca_e = esc(confianca_raw)
      period_ref_e = esc(period_ref)
      evolucao_e = esc(evolucao_raw)
      consistencia_e = esc(consistencia_raw)
      execucao_e = esc(execucao_raw)
      mudancas_e = esc(mudancas_raw)
      tese_final_e = esc(tese_final_raw)

      st.markdown(
          f"""
          <div class="p6-card">
            <div class="p6-head">
              <div class="p6-title-sm">{ticker_e}</div>
              <div class="p6-badges">
                <span class="{_pill_class(persp_raw)}">{(persp_e or "—").upper()}</span>
                <span class="p6-pill p6-pill-info">Top-K: {int(top_k_used)}</span>
                <span class="p6-pill p6-pill-info">Janela: {period_ref_e}</span>
                {f'<span class="p6-pill p6-pill-info">Docs: {int(docs_usados)}</span>' if docs_usados is not None else ""}
                {f'<span class="p6-pill p6-pill-info">Evidências: {int(evid_usadas)}</span>' if evid_usadas is not None else ""}
              </div>
            </div>

            <div class="p6-grid">
              <div><span class="p6-k">Resumo:</span> <span class="p6-muted">{resumo_e or "—"}</span></div>
              {f'<div><span class="p6-k">Evolução estratégica:</span> <span class="p6-muted">{evolucao_e}</span></div>' if evolucao_raw else ''}
              {f'<div><span class="p6-k">Consistência do discurso:</span> <span class="p6-muted">{consistencia_e}</span></div>' if consistencia_raw else ''}
              {f'<div><span class="p6-k">Execução vs promessa:</span> <span class="p6-muted">{execucao_e}</span></div>' if execucao_raw else ''}
              {f'<div><span class="p6-k">Mudanças estratégicas:</span> <span class="p6-muted">{mudancas_e}</span></div>' if mudancas_raw else ''}
              {f'<div><span class="p6-k">Tese final:</span> <span class="p6-muted">{tese_final_e}</span></div>' if tese_final_raw else ''}
              {f'<div><span class="p6-k">Considerações da LLM:</span> <span class="p6-muted">{consider_e}</span></div>' if consider_raw else ''}
              {f'<div><span class="p6-k">Confiança:</span> <span class="p6-muted">{confianca_e}</span></div>' if confianca_raw else ''}
            </div>

            <hr class="p6-hr"/>

            <div class="p6-grid">
              <div>
                <span class="p6-k">Pontos-chave</span>
                <ul class="p6-list">
                  {''.join([f'<li>{html.escape(str(p))}</li>' for p in pontos]) if pontos else '<li class="p6-muted">—</li>'}
                </ul>
              </div>

              <div>
                <span class="p6-k">Riscos</span>
                <ul class="p6-list">
                  {''.join([f'<li>{html.escape(str(r))}</li>' for r in riscos]) if riscos else '<li class="p6-muted">—</li>'}
                </ul>
              </div>

              <div>
                <span class="p6-k">Sinais recorrentes</span>
                <ul class="p6-list">
                  {''.join([f'<li>{html.escape(str(s))}</li>' for s in sinais]) if sinais else '<li class="p6-muted">—</li>'}
                </ul>
              </div>
            </div>
          </div>
          """,
          unsafe_allow_html=True,
      )

      if evid:
          with st.expander(f"📌 Evidências (trechos) — {ticker}", expanded=False):
              for i, e in enumerate(evid[:15], start=1):
                  st.markdown(f"**{i}.** {html.escape(str(e))}")

    # Defaults fixos (sem UI)
    run_llm_all = True
    use_topk_inteligente = True
    debug_topk = False

    # Controles da análise qualitativa
    top_k = st.slider(
        "Máx evidências por ticker (cap)",
        min_value=20,
        max_value=120,
        value=80,
        step=5,
        help="O budget adaptativo define quantas evidências usar por ticker. Este controle atua apenas como limite máximo para evitar excesso de contexto."
    )
    st.caption("O sistema usa budget adaptativo por ticker. Este valor é apenas o teto máximo de evidências permitidas por empresa.")

    analysis_mode = st.radio(
        "Modo de análise",
        options=["Padrão (24 meses)", "Aprofundada (36 meses)"],
        index=(0 if analysis_mode == "Padrão (24 meses)" else 1),
        horizontal=True,
        key="analysis_mode",
        help="A análise padrão observa os últimos 24 meses. A aprofundada amplia a leitura para 36 meses, favorecendo avaliação de trajetória, consistência do discurso e execução ao longo do tempo."
    )
    analysis_window_months = 24 if analysis_mode == "Padrão (24 meses)" else 36
    analysis_period_ref = "24M" if analysis_window_months == 24 else "36M"
    st.caption(f"Janela temporal ativa da análise qualitativa: {analysis_window_months} meses. Esta mesma janela é usada na atualização de evidências.")

    st.markdown("## 📘 Relatório consolidado do portfólio")
    st.caption("Montado a partir do que está salvo em patch6_runs. Ao rodar a LLM, este relatório é atualizado automaticamente.")
    
    report_box = st.empty()
    
    def _render_saved_report():
        with report_box.container():
            try:
                from core.patch6_report import render_patch6_report
                render_patch6_report(
                    tickers=tickers,
                    period_ref=analysis_period_ref,
                    llm_factory=llm_factory,
                    show_company_details=True,
                )
            except Exception as e:
                st.error("Relatório indisponível.")
                st.exception(e)
    
    _render_saved_report()


    # Wrappers
  

    def _call_llm(client: Any, prompt: str) -> str:
        """
        Compatível com:
        - OpenAI SDK novo: client.responses.create(...)
        - OpenAI SDK legado: client.chat.completions.create(...)
        - Clientes custom: .complete/.chat/.invoke ou callable
        - Unwrap defensivo (dict/client/_client)
        """
        if client is None:
            raise AttributeError("Cliente LLM é None.")
    
        # unwrap defensivo
        if isinstance(client, dict) and "client" in client:
            client = client["client"]
        if hasattr(client, "client"):
            try:
                client = client.client
            except Exception:
                pass
        if hasattr(client, "_client"):
            try:
                client = client._client
            except Exception:
                pass
    
        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    
        # 1) OpenAI SDK novo (Responses API)
        if hasattr(client, "responses") and hasattr(client.responses, "create") and callable(client.responses.create):
            resp = client.responses.create(model=model, input=prompt)
            txt = getattr(resp, "output_text", None)
            if txt:
                return txt
            # fallback defensivo
            try:
                return resp.output[0].content[0].text
            except Exception:
                return str(resp)
    
        # 2) OpenAI SDK legado (Chat Completions)
        if hasattr(client, "chat") and hasattr(client.chat, "completions") and hasattr(client.chat.completions, "create"):
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
            )
            return resp.choices[0].message.content
    
        # 3) Clientes custom / wrappers
        if hasattr(client, "complete") and callable(getattr(client, "complete")):
            return client.complete(prompt)
        if hasattr(client, "chat") and callable(getattr(client, "chat")):
            return client.chat(prompt)
        if hasattr(client, "invoke") and callable(getattr(client, "invoke")):
            out = client.invoke(prompt)
            # alguns wrappers retornam objeto com .content
            return getattr(out, "content", out)
        if callable(client):
            out = client(prompt)
            return getattr(out, "content", out)
    
        raise AttributeError("Cliente LLM não expõe métodos suportados (responses/chat/complete/invoke).")
        
    def _get_chunks_for_ticker(t: str, top_k_used: int, months_window: int, return_debug: bool = False):
        try:
            if use_topk_inteligente:
                from core.rag_retriever import get_topk_chunks_inteligente, summarize_retrieval_mix  # type: ignore
    
                rag_out = get_topk_chunks_inteligente(
                    ticker=t,
                    top_k=int(top_k_used),
                    months_window=int(months_window),
                    debug=True,
                )
    
                hits = rag_out or []
                normalized_hits: List[Dict[str, Any]] = []
                for item in hits:
                    if isinstance(item, str):
                        txt = item.strip()
                        if txt:
                            normalized_hits.append({"text": txt, "data_doc": "", "tipo_doc": "", "theme": "", "doc_id": ""})
                    else:
                        txt = getattr(item, "chunk_text", None)
                        if txt:
                            dt = getattr(item, "data_doc", None)
                            normalized_hits.append(
                                {
                                    "text": str(txt).strip(),
                                    "data_doc": (dt.date().isoformat() if hasattr(dt, "date") else str(dt or "")),
                                    "tipo_doc": str(getattr(item, "tipo_doc", "") or ""),
                                    "theme": str(getattr(item, "strategic_theme", "") or ""),
                                    "doc_id": str(getattr(item, "doc_id", "") or ""),
                                }
                            )
    
                mix = summarize_retrieval_mix(rag_out) if hits and not isinstance(hits[0], str) else {}
                if return_debug:
                    return normalized_hits, "topk_inteligente", mix
    
                return [h["text"] for h in normalized_hits if h.get("text")], "topk_inteligente"
    
        except Exception:
            pass
    
        from core.docs_corporativos_store import fetch_topk_chunks
        chunks = fetch_topk_chunks(t, int(top_k_used))
        if return_debug:
            return [{"text": str(c).strip(), "data_doc": "", "tipo_doc": "", "theme": "", "doc_id": ""} for c in (chunks or [])], "fetch_topk_chunks", {}
        return chunks or [], "fetch_topk_chunks"

    def _get_temporal_chunks_for_ticker(t: str, top_k_used: int):
        recent, fonte_recent, mix_recent = _get_chunks_for_ticker(
            t,
            top_k_used=max(6, min(top_k_used, 18)),
            months_window=12,
            return_debug=True,
        )

        if int(analysis_window_months) <= 24:
            cumulative_24, fonte_24, mix_24 = _get_chunks_for_ticker(
                t,
                top_k_used=max(10, min(top_k_used, 28)),
                months_window=24,
                return_debug=True,
            )

            recent_texts = [x.get("text", "") for x in recent]
            previous = [x for x in cumulative_24 if x.get("text", "") not in set(recent_texts)]

            sections = {
                "janela_0_12m": recent[:18],
                "janela_12_24m": previous[:18],
                "janela_24_36m": [],
            }

            audit = {
                "mix_recent": mix_recent,
                "mix_total": mix_24,
                "docs_retrieved": len({x.get("doc_id") for x in cumulative_24 if x.get("doc_id")}),
                "years": sorted({
                    str(x.get("data_doc", ""))[:4]
                    for x in cumulative_24
                    if str(x.get("data_doc", ""))[:4].isdigit()
                }),
            }
            return sections, fonte_24 or fonte_recent, audit

        cumulative_24, fonte_24, mix_24 = _get_chunks_for_ticker(
            t,
            top_k_used=max(10, min(top_k_used, 24)),
            months_window=24,
            return_debug=True,
        )
        cumulative_36, fonte_36, mix_36 = _get_chunks_for_ticker(
            t,
            top_k_used=max(12, min(top_k_used, 36)),
            months_window=36,
            return_debug=True,
        )

        recent_texts = {x.get("text", "") for x in recent}
        cumulative_24_texts = {x.get("text", "") for x in cumulative_24}

        middle = [x for x in cumulative_24 if x.get("text", "") not in recent_texts]
        older = [x for x in cumulative_36 if x.get("text", "") not in cumulative_24_texts]

        sections = {
            "janela_0_12m": recent[:18],
            "janela_12_24m": middle[:18],
            "janela_24_36m": older[:18],
        }

        audit = {
            "mix_recent": mix_recent,
            "mix_total": mix_36 or mix_24,
            "docs_retrieved": len({x.get("doc_id") for x in cumulative_36 if x.get("doc_id")}),
            "years": sorted({
                str(x.get("data_doc", ""))[:4]
                for x in cumulative_36
                if str(x.get("data_doc", ""))[:4].isdigit()
            }),
        }
        return sections, fonte_36 or fonte_24 or fonte_recent, audit

    def _build_prompt(contexto: str) -> str:
        return f"""
Você é um analista fundamentalista institucional especializado em trajetória estratégica, execução, alocação de capital e consistência de discurso corporativo.

Use SOMENTE o CONTEXTO abaixo, organizado por janelas temporais e com metadados de data/tipo documental/tema.

Objetivo:
1. comparar as janelas temporais;
2. identificar mudanças reais de estratégia;
3. classificar a execução como forte, moderada, fraca ou inconsistente;
4. separar riscos estruturais de ruídos pontuais;
5. listar evidências documentais concretas;
6. produzir um JSON rico, auditável e específico.

Regras:
- não faça resumo genérico;
- não invente fatos;
- quando houver pouca base, diga isso explicitamente;
- use no mínimo 5 evidências se o contexto trouxer material suficiente;
- sempre que possível, atribua ano ou janela temporal às evidências;
- prefira evidências sobre dívida, capex, dividendos, recompra, guidance, execução, governança e M&A.

Responda APENAS em JSON válido neste formato:

{{
  "perspectiva_compra": "forte|moderada|fraca",
  "leitura_direcionalidade": "construtiva|equilibrada|cautelosa|negativa",
  "resumo": "síntese em 4 a 6 linhas",
  "evolucao_estrategica": {{
    "historico": "...",
    "fase_atual": "...",
    "tendencia": "..."
  }},
  "consistencia_discurso": {{
    "analise": "...",
    "grau_consistencia": "alto|medio|baixo",
    "contradicoes": ["..."]
  }},
  "execucao_vs_promessa": {{
    "analise": "...",
    "avaliacao_execucao": "forte|moderada|fraca|inconsistente",
    "entregas_confirmadas": ["..."],
    "entregas_pendentes_ou_incertas": ["..."]
  }},
  "mudancas_estrategicas": ["..."],
  "pontos_chave": ["..."],
  "catalisadores": ["..."],
  "riscos": ["..."],
  "o_que_monitorar": ["..."],
  "consideracoes_llm": "limitações do contexto, qualidade dos documentos e lacunas",
  "evidencias": [
    {{
      "topico": "2024 ou janela",
      "trecho": "trecho literal curto",
      "interpretacao": "por que isso importa"
    }}
  ],
  "tese_final": "conclusão final integrada"
}}

CONTEXTO TEMPORAL:
{contexto}
"""
    if st.button("🔄 Atualizar relatório com LLM agora"):
        client = llm_factory.get_llm_client()

        tickers_run = tickers
        total = len(tickers_run)

        st.info("Iniciando leitura qualitativa… ao final, o relatório principal acima será recarregado com os resultados atualizados.")
        prog = st.progress(0)
        status_box = st.empty()

        fortes = moderadas = fracas = erros = 0
        status_rows: List[Dict[str, Any]] = []

        for i, t in enumerate(tickers_run, start=1):
            status_box.markdown(f"✅ Processando **{t}** ({i}/{total})…")
            t0 = time.time()

            try:
                num_chunks = count_chunks(t)
                peso = weight_map.get(t, 0.0)
                budget_info = _calc_budget_topk(num_chunks=num_chunks, peso=peso, cap_max=int(top_k))
                topk_run = int(budget_info['budget_used'])

                temporal_sections, fonte_chunks, retrieval_audit = _get_temporal_chunks_for_ticker(t, topk_run)
                total_temporal_evidence = sum(len(v or []) for v in temporal_sections.values())
                if total_temporal_evidence == 0:
                    erros += 1
                    status_rows.append({"ticker": t, "status": "SEM_CHUNKS", "erro": "Sem chunks no Supabase"})
                    prog.progress(int(i / total * 100))
                    continue

                topk_retry_used = None

                # Quality Gate: se evidência muito baixa, tenta ampliar budget (respeitando o cap da UI)
                if total_temporal_evidence < 10 and int(top_k) > int(topk_run):
                    topk_retry = min(int(top_k), int(topk_run) + 6)
                    try:
                        temporal_sections2, fonte2, retrieval_audit2 = _get_temporal_chunks_for_ticker(t, topk_retry)
                        total2 = sum(len(v or []) for v in temporal_sections2.values())
                        if total2 > total_temporal_evidence:
                            temporal_sections, fonte_chunks, retrieval_audit = temporal_sections2, fonte2, retrieval_audit2
                            total_temporal_evidence = total2
                            topk_run = int(topk_retry)
                            topk_retry_used = int(topk_run)
                    except Exception:
                        pass

                contexto = _build_temporal_context(temporal_sections, per_chunk_chars=1100, total_chars=20000)
                context_preview = contexto[:4000]
                try:
                    raw = _call_llm(client, _build_prompt(contexto))
                except Exception as e_call:
                    msg = str(e_call).lower()
                    if "context window" in msg or "exceed" in msg:
                        flat_chunks = []
                        for _bucket in temporal_sections.values():
                            flat_chunks.extend(_bucket or [])
                        contexto = _build_context_limited(flat_chunks, per_chunk_chars=800, total_chars=8000)
                        raw = _call_llm(client, _build_prompt(contexto))
                    else:
                        raise

                try:
                    result = _parse_json_loose(raw)
                except Exception:
                    erros += 1
                    status_rows.append({"ticker": t, "status": "JSON_INVALIDO", "erro": "LLM não retornou JSON"})
                    if debug_topk:
                        with st.expander(f"⚠️ Resposta bruta (debug) — {t}", expanded=False):
                            st.code(raw, language="json")
                    prog.progress(int(i / total * 100))
                    continue

                # metadados de contexto
                result.setdefault("evid_usadas", int(total_temporal_evidence))
                result.setdefault("docs_usados", None)
                result.setdefault("_meta", {})
                result["_meta"]["context_preview"] = context_preview
                result.setdefault("metodo_chunks", fonte_chunks)
                # metadados institucionais (auditoria)
                result.setdefault("_meta", {})
                result["_meta"].update({
                    "peso": float(peso or 0.0),
                    "num_chunks": int(num_chunks or 0),
                    "budget_base": int(budget_info.get("base", 0)),
                    "budget_peso_mult": float(budget_info.get("peso_mult", 1.0)),
                    "budget_raw": int(budget_info.get("budget_raw", topk_run)),
                    "top_k_used": int(topk_run),
                    "top_k_retry_used": (int(topk_retry_used) if topk_retry_used is not None else None),
                    "top_k_cap_ui": int(top_k),
                    "window_months": int(analysis_window_months),
                    "analysis_mode": analysis_mode,
                    "docs_retrieved": int((retrieval_audit or {}).get("docs_retrieved", 0)),
                    "context_years": list((retrieval_audit or {}).get("years", []) or []),
                    "retrieval_mix": (retrieval_audit or {}).get("mix_total", {}),
                    "context_preview": context_preview,
                    "temporal_buckets": {
                        "0_12m": int(len(temporal_sections.get("janela_0_12m") or [])),
                        "12_24m": int(len(temporal_sections.get("janela_12_24m") or [])),
                        "24_36m": int(len(temporal_sections.get("janela_24_36m") or [])),
                    },
                })

                # normaliza/enriquece o JSON antes de salvar
                result = build_result_json(result)

                # salva
                save_patch6_run(
                    snapshot_id=str(snapshot_id),
                    ticker=t,
                    period_ref=analysis_period_ref,
                    result=result,
                )

                # conta perspectiva
                p = str(result.get("perspectiva_compra", "")).strip().lower()
                if p == "forte":
                    fortes += 1
                elif p == "moderada":
                    moderadas += 1
                elif p == "fraca":
                    fracas += 1
                else:
                    erros += 1

                # Não renderiza cards intermediários abaixo do botão.
                # O relatório principal acima será refeito ao final com os cards enriquecidos do patch6_report.

                status_rows.append(
                    {
                        "ticker": t,
                        "status": "OK",
                        "metodo_chunks": fonte_chunks,
                        "top_k_used": int(topk_run),
                        "top_k_retry_used": (int(topk_retry_used) if topk_retry_used is not None else None),
                        "evidencias_totais": int(total_temporal_evidence),
                        "score_qualitativo": result.get("score_qualitativo"),
                        "confianca_analise": result.get("confianca_analise"),
                        "tempo_s": round(time.time() - t0, 1),
                    }
                )

            except Exception as e:
                erros += 1
                status_rows.append({"ticker": t, "status": "ERRO_LLM", "erro": str(e)})
                if debug_topk:
                    with st.expander(f"❌ Erro (traceback) — {t}", expanded=False):
                        st.code(traceback.format_exc())
                else:
                    st.warning(f"❌ {t} — falha ao rodar LLM: {e}")

            prog.progress(int(i / total * 100))

        status_box.markdown("✅ Concluído.")
        st.subheader("📌 Parecer resumido do portfólio")
        st.write(f"Forte: **{fortes}** | Moderada: **{moderadas}** | Fraca: **{fracas}** | Erros/sem dados: **{erros}**")

        if erros > 0:
            st.warning(f"Execução concluída com {erros} ocorrência(s).")
            if debug_topk:
                st.subheader("🧾 Status por ticker")
                st.dataframe(status_rows, use_container_width=True)
        else:
            st.caption("Execução concluída sem erros. O relatório consolidado acima foi atualizado.")

        # Atualiza o relatório no MESMO lugar (sem duplicar seção)
        report_box.empty()
        _render_saved_report()
