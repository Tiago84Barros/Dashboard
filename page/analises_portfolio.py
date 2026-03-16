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
import html
import traceback
import importlib
import inspect
from typing import Any, Dict, List, Optional, Callable, Tuple

import streamlit as st

from core.helpers import get_logo_url

from core.portfolio_snapshot_store import get_latest_snapshot
from core.docs_corporativos_store import (
    count_docs,
    count_chunks,
    process_missing_chunks_for_ticker,
)
from core.patch6_runs_store import save_patch6_run, list_patch6_history

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



def _fetch_chunks_by_age_window(
    ticker: str,
    *,
    months_recent: int,
    months_older_than: int = 0,
    k: int = 12,
    per_doc_cap: int = 3,
) -> List[str]:
    """
    Busca chunks por janela temporal relativa à data atual, preservando diversidade por documento.
    Ex.:
      - months_recent=12, months_older_than=0  => últimos 12 meses
      - months_recent=24, months_older_than=12 => entre 12 e 24 meses
    """
    tk = _safe_upper(ticker)
    if not tk or int(k) <= 0:
        return []

    engine = get_supabase_engine()
    candidate_limit = max(int(k) * 8, 80)

    sql = """
        with ranked as (
            select
                c.doc_id,
                c.chunk_index,
                c.chunk_text,
                d.data as doc_data,
                row_number() over (
                    partition by c.doc_id
                    order by c.chunk_index asc
                ) as rn_doc
            from public.docs_corporativos_chunks c
            join public.docs_corporativos d
              on d.id = c.doc_id
            where c.ticker = :tk
              and d.data >= (current_date - (:recent || ' months')::interval)
              and (
                    :older = 0
                    or d.data < (current_date - (:older || ' months')::interval)
                  )
            order by d.data desc nulls last, c.doc_id desc, c.chunk_index asc
            limit :candidate_limit
        )
        select chunk_text
        from ranked
        where rn_doc <= :per_doc_cap
        order by doc_data desc nulls last, doc_id desc, chunk_index asc
        limit :k
    """
    with engine.connect() as conn:
        df = pd.read_sql_query(
            text(sql),
            conn,
            params={
                "tk": tk,
                "recent": int(months_recent),
                "older": int(months_older_than),
                "candidate_limit": int(candidate_limit),
                "per_doc_cap": int(per_doc_cap),
                "k": int(k),
            },
        )
    return df["chunk_text"].tolist() if not df.empty else []


def _dedupe_preserve_order(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for item in items or []:
        key = str(item).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _split_budget_across_windows(total_k: int, analysis_window_months: int) -> Dict[str, int]:
    total_k = max(6, int(total_k or 6))
    if int(analysis_window_months) >= 36:
        recent = max(6, int(round(total_k * 0.40)))
        middle = max(4, int(round(total_k * 0.35)))
        older = max(3, total_k - recent - middle)
        # garante soma
        while recent + middle + older > total_k:
            if older > 3:
                older -= 1
            elif middle > 4:
                middle -= 1
            else:
                recent -= 1
        while recent + middle + older < total_k:
            recent += 1
        return {"0_12m": recent, "12_24m": middle, "24_36m": older}
    else:
        recent = max(6, int(round(total_k * 0.55)))
        middle = max(4, total_k - recent)
        while recent + middle > total_k:
            if middle > 4:
                middle -= 1
            else:
                recent -= 1
        while recent + middle < total_k:
            recent += 1
        return {"0_12m": recent, "12_24m": middle, "24_36m": 0}


def _get_temporal_chunks_for_ticker(ticker: str, top_k_used: int, analysis_window_months: int) -> Tuple[Dict[str, List[str]], Dict[str, Any]]:
    allocation = _split_budget_across_windows(total_k=top_k_used, analysis_window_months=analysis_window_months)

    recent_chunks = _fetch_chunks_by_age_window(
        ticker,
        months_recent=12,
        months_older_than=0,
        k=allocation["0_12m"],
        per_doc_cap=3,
    )
    middle_chunks = _fetch_chunks_by_age_window(
        ticker,
        months_recent=24,
        months_older_than=12,
        k=allocation["12_24m"],
        per_doc_cap=3,
    ) if analysis_window_months >= 24 else []
    older_chunks = _fetch_chunks_by_age_window(
        ticker,
        months_recent=36,
        months_older_than=24,
        k=allocation["24_36m"],
        per_doc_cap=3,
    ) if analysis_window_months >= 36 and allocation["24_36m"] > 0 else []

    windows = {
        "0_12m": _dedupe_preserve_order(recent_chunks),
        "12_24m": _dedupe_preserve_order(middle_chunks),
        "24_36m": _dedupe_preserve_order(older_chunks),
    }
    total_found = sum(len(v) for v in windows.values())
    stats = {
        "allocation": allocation,
        "found": {k: len(v) for k, v in windows.items()},
        "total_found": total_found,
    }
    return windows, stats


def _build_temporal_context(windows: Dict[str, List[str]], per_chunk_chars: int = 900, total_chars: int = 22000) -> str:
    sections: List[str] = []
    used = 0
    labels = {
        "0_12m": "JANELA RECENTE (0-12 meses)",
        "12_24m": "JANELA INTERMEDIÁRIA (12-24 meses)",
        "24_36m": "JANELA HISTÓRICA (24-36 meses)",
    }
    for key in ["0_12m", "12_24m", "24_36m"]:
        chunks = windows.get(key) or []
        if not chunks:
            continue
        header = f"\n\n### {labels[key]}\n"
        if used + len(header) > total_chars:
            break
        parts = [header]
        used += len(header)
        for i, ch in enumerate(chunks, start=1):
            piece = _clip(str(ch), per_chunk_chars)
            block = f"[{key} | CHUNK {i}]\n{piece}\n"
            if used + len(block) > total_chars:
                break
            parts.append(block)
            used += len(block)
        sections.append("".join(parts))
        if used >= total_chars:
            break
    return "".join(sections).strip()

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
            width:40px;
            height:40px;
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
    


    # ------------------------------------------------------------------
    # Ingest + Chunking com logs por ticker
    # ------------------------------------------------------------------
    st.subheader("📦 Atualizar evidências")

    col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
    with col1:
        window_months = st.number_input("Janela (meses)", min_value=1, max_value=60, value=12, step=1)
    with col2:
        max_docs = st.number_input("Máx docs/ticker", min_value=5, max_value=300, value=80, step=5)
    with col3:
        max_pdfs = st.number_input("Máx PDFs/ticker", min_value=0, max_value=80, value=20, step=1)
    with col4:
        max_runtime_s = st.number_input("Tempo máx total (s)", min_value=5, max_value=180, value=60, step=5)

    only_missing_docs = True
    show_traceback = False

    # Diagnóstico sob demanda (não altera pipeline; apenas inspeciona presença de docs/chunks)
    diag_btn = st.button("🩺 Diagnóstico (docs/chunks)", help="Mostra rapidamente se cada ticker tem docs e chunks no Supabase.")

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
        st.dataframe(diag_rows, use_container_width=True)

        if missing_docs > 0:
            st.warning("Há tickers sem documentos. Use **Atualizar documentos** para tentar ingerir CVM/IPE.")
        elif missing_chunks > 0:
            st.warning("Há tickers com docs mas sem chunks. Use **Atualizar documentos** (ele também processa chunks faltantes).")
        else:
            st.success("Todos os tickers têm docs e chunks. Pode rodar o LLM com evidências completas.")

    btn = st.button("Atualizar documentos", type="primary")

    log_panel = st.empty()
    table_panel = st.empty()
    err_panel = st.empty()

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

            # ---- Ingest
            try:
                if (not only_missing_docs) or (before_docs == 0):
                    ingest_ran = True
                    r = _safe_call(
                        ingest_fn,
                        tickers=[tk],
                        window_months=int(window_months),
                        max_docs_per_ticker=int(max_docs),
                        max_runtime_s=float(max_runtime_s),
                        max_pdfs_per_ticker=int(max_pdfs),
                    )
                    # normaliza relatório
                    if isinstance(r, dict):
                        ingest_report = r
                    else:
                        ingest_report = {"result": str(r)}
                else:
                    ingest_report = {"skipped": True, "reason": "docs já existem"}
            except Exception as e:
                tb = traceback.format_exc()
                msg = f"Ingest {type(e).__name__}: {e}"
                errors[f"{tk}::ingest"] = tb if show_traceback else msg
                ingest_report = {"error": msg}
                with log_panel.container():
                    st.error(f"❌ {tk} — ingest falhou | {msg}")

            mid_docs = count_docs(tk)
            mid_chunks = count_chunks(tk)

            with log_panel.container():
                if ingest_ran:
                    st.write(f"📥 {tk} — ingest concluído | docs agora={mid_docs} | chunks={mid_chunks}")
                    if ingest_report:
                        st.caption("Relatório ingest (resumo):")
                        st.json({k: ingest_report[k] for k in ingest_report.keys() if k in {"matched","inserted","skipped","pdf_fetched","pdf_text_ok","error","result","skipped","reason"}})
                else:
                    st.write(f"📥 {tk} — ingest não executado (docs já existiam) | docs={mid_docs}")

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
                table_panel.dataframe(results, use_container_width=True)
                continue

            # ---- Chunking
            try:
                inserted = process_missing_chunks_for_ticker(tk, limit_docs=int(max_docs), max_chars=1500)
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

            table_panel.dataframe(results, use_container_width=True)

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
      # Sanitização defensiva: impede que HTML vindo da LLM quebre o layout
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
  
      pontos = _as_list(result.get("pontos_chave") or result.get("pontos-chave") or result.get("pontos"))
      riscos = _as_list(result.get("riscos"))
      evid = _as_list(result.get("evidencias") or result.get("evidence") or result.get("citacoes"))
      sinais = _as_list(result.get("sinais_recorrentes"))
      evolucao_raw = (result.get("evolucao_do_discurso") or "").strip()
      consist_raw = (result.get("consistencia_entre_periodos") or "").strip()
      exec_raw = (result.get("execucao_vs_promessa") or "").strip()
      mud_raw = (result.get("mudancas_estrategicas") or "").strip()
  
      docs_usados = result.get("docs_usados") or result.get("docs_used") or result.get("documentos") or None
      evid_usadas = result.get("evid_usadas") or result.get("chunks_used") or result.get("evidencias_usadas") or None
  
      # Escapa campos críticos (texto vindo da LLM)
      ticker_e = esc(ticker)
      persp_e = esc(persp_raw)
      resumo_e = esc(resumo_raw)
      consider_e = esc(consider_raw)
      confianca_e = esc(confianca_raw)
      period_ref_e = esc(period_ref)
  
      st.markdown(
          f"""
          <div class="p6-card">
            <div class="p6-head">
              <div class="p6-title-sm">{ticker_e}</div>
              <div class="p6-badges">
                <span class="{_pill_class(persp_raw)}">{(persp_e or "—").upper()}</span>
                <span class="p6-pill p6-pill-info">Top-K: {int(top_k_used)}</span>
                <span class="p6-pill p6-pill-info">period_ref: {period_ref_e}</span>
                {f'<span class="p6-pill p6-pill-info">Docs: {int(docs_usados)}</span>' if docs_usados is not None else ""}
                {f'<span class="p6-pill p6-pill-info">Evidências: {int(evid_usadas)}</span>' if evid_usadas is not None else ""}
              </div>
            </div>
  
            <div class="p6-grid">
              <div><span class="p6-k">Resumo:</span> <span class="p6-muted">{resumo_e or "—"}</span></div>
              {f'<div><span class="p6-k">Evolução do discurso:</span> <span class="p6-muted">{html.escape(str(evolucao_raw))}</span></div>' if evolucao_raw else ''}
              {f'<div><span class="p6-k">Consistência entre períodos:</span> <span class="p6-muted">{html.escape(str(consist_raw))}</span></div>' if consist_raw else ''}
              {f'<div><span class="p6-k">Execução vs promessa:</span> <span class="p6-muted">{html.escape(str(exec_raw))}</span></div>' if exec_raw else ''}
              {f'<div><span class="p6-k">Mudanças estratégicas:</span> <span class="p6-muted">{html.escape(str(mud_raw))}</span></div>' if mud_raw else ''}
              {f'<div><span class="p6-k">Considerações da LLM:</span> <span class="p6-muted">{consider_e}</span></div>' if (consider_raw and str(consider_raw).strip()) else ''}
              {f'<div><span class="p6-k">Confiança:</span> <span class="p6-muted">{confianca_e}</span></div>' if (confianca_raw and str(confianca_raw).strip()) else ''}
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
  
      # Evidências: render em texto puro (sem HTML)
      if evid:
          with st.expander(f"📌 Evidências (trechos) — {ticker}", expanded=False):
              for i, e in enumerate(evid[:12], start=1):
                  st.markdown(f"**{i}.** {html.escape(str(e))}")
      
    # Defaults fixos (sem UI)
    run_llm_all = True
    use_topk_inteligente = True
    debug_topk = False
    window_months = 12  # fixo internamente
    
    # Único controle exposto
    top_k = st.slider(
        "Máx evidências por ticker (cap)",
        min_value=20,
        max_value=120,
        value=80,
        step=5,
        help="O budget adaptativo define quantas evidências usar por ticker. Este controle atua apenas como limite máximo para evitar excesso de contexto."
    )
    st.caption("O sistema usa budget adaptativo por ticker. Este valor é apenas o teto máximo de evidências permitidas por empresa.")
    
    period_ref = st.text_input(
        "period_ref (ex.: 2024Q4)",
        value="2024Q4"
    )

    st.markdown("## 📘 Relatório consolidado do portfólio")
    st.caption("Montado a partir do que está salvo em patch6_runs. Ao rodar a LLM, este relatório é atualizado automaticamente.")
    
    report_box = st.empty()
    
    def _render_saved_report():
        with report_box.container():
            try:
                from core.patch6_report import render_patch6_report
                render_patch6_report(
                    tickers=tickers,
                    period_ref=period_ref,
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
      
    def _get_chunks_for_ticker(t: str, top_k_used: int) -> Tuple[List[str], str]:
        # preferir Top-K inteligente; fallback para fetch_topk_chunks
        try:
            if use_topk_inteligente:
                from core.rag_retriever import get_topk_chunks_inteligente  # type: ignore
                chunks, meta = get_topk_chunks_inteligente(
                    ticker=t,
                    top_k=int(top_k_used),
                    window_months=int(window_months),
                    debug=bool(debug_topk),
                )
                return chunks or [], "topk_inteligente"
        except Exception:
            # cai no fetch simples
            pass

        from core.docs_corporativos_store import fetch_topk_chunks
        chunks = fetch_topk_chunks(t, int(top_k_used))
        return chunks or [], "fetch_topk_chunks"

    def _build_prompt(contexto: str, analysis_mode: str, analysis_window_months: int) -> str:
        return f"""
Você é um analista fundamentalista institucional, focado em trajetória estratégica, alocação de capital e coerência entre discurso e execução.

Use SOMENTE o CONTEXTO abaixo, que já está organizado por janelas temporais.
Sua tarefa NÃO é resumir genericamente a empresa. Sua tarefa é comparar os períodos e identificar evolução real.

Analise obrigatoriamente:
1. Evolução do discurso ao longo das janelas temporais
2. Consistência entre o que a empresa dizia antes e o que diz mais recentemente
3. Execução vs promessa (o que parece ter sido entregue e o que continua recorrente apenas no discurso)
4. Mudanças de foco em capex, dívida/desalavancagem, dividendos/recompra e M&A/desinvestimentos
5. Sinais estratégicos recorrentes ao longo do tempo
6. Principais riscos observáveis a partir do histórico

Se houver pouca informação em alguma janela, diga isso explicitamente.
Priorize evidências concretas e comparações temporais.

Devolva APENAS JSON válido no formato:

{{
  "perspectiva_compra": "forte|moderada|fraca",
  "resumo": "4-6 linhas com leitura histórica da empresa, mostrando evolução temporal e tese final",
  "evolucao_do_discurso": "Comparação objetiva entre janelas temporais",
  "consistencia_entre_periodos": "Avaliação da consistência do discurso ao longo do tempo",
  "execucao_vs_promessa": "O que parece ter sido executado versus o que permaneceu apenas como sinalização",
  "mudancas_estrategicas": "Mudanças observadas em capex, dívida, dividendos, M&A ou foco operacional",
  "sinais_recorrentes": ["..."],
  "pontos_chave": ["..."],
  "riscos": ["..."],
  "consideracoes_llm": "Ressalvas, lacunas temporais ou limitações do corpus",
  "confianca": "alta|media|baixa",
  "evidencias": ["trechos curtos e literais do contexto, distribuídos entre janelas quando possível"]
}}

Modo de análise ativo: {analysis_mode}
Janela total de análise: {analysis_window_months} meses

CONTEXTO TEMPORAL:
{contexto}
"""

    if st.button("🔄 Atualizar relatório com LLM agora"):
        client = llm_factory.get_llm_client()

        tickers_run = tickers
        total = len(tickers_run)

        st.info("Iniciando leitura qualitativa… os cards aparecem à medida que cada ticker finalizar.")
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

                temporal_windows, temporal_stats = _get_temporal_chunks_for_ticker(
                    t,
                    top_k_used=topk_run,
                    analysis_window_months=analysis_window_months,
                )
                chunks = _dedupe_preserve_order(
                    (temporal_windows.get("0_12m") or [])
                    + (temporal_windows.get("12_24m") or [])
                    + (temporal_windows.get("24_36m") or [])
                )
                fonte_chunks = "temporal_windows"

                if not chunks:
                    # fallback para o método anterior
                    chunks, fonte_chunks = _get_chunks_for_ticker(t, topk_run)

                if not chunks:
                    erros += 1
                    status_rows.append({"ticker": t, "status": "SEM_CHUNKS", "erro": "Sem chunks no Supabase"})
                    prog.progress(int(i / total * 100))
                    continue

                topk_retry_used = None

                # Quality Gate: se evidência muito baixa, tenta ampliar budget (respeitando o cap da UI)
                if len(chunks) < 10 and int(top_k) > int(topk_run):
                    topk_retry = min(int(top_k), int(topk_run) + 8)
                    try:
                        temporal_windows_2, temporal_stats_2 = _get_temporal_chunks_for_ticker(
                            t,
                            top_k_used=topk_retry,
                            analysis_window_months=analysis_window_months,
                        )
                        chunks2 = _dedupe_preserve_order(
                            (temporal_windows_2.get("0_12m") or [])
                            + (temporal_windows_2.get("12_24m") or [])
                            + (temporal_windows_2.get("24_36m") or [])
                        )
                        if chunks2 and len(chunks2) > len(chunks):
                            temporal_windows, temporal_stats = temporal_windows_2, temporal_stats_2
                            chunks = chunks2
                            fonte_chunks = "temporal_windows_retry"
                            topk_run = int(topk_retry)
                            topk_retry_used = int(topk_run)
                    except Exception:
                        pass

                if fonte_chunks.startswith("temporal_windows"):
                    contexto = _build_temporal_context(temporal_windows, per_chunk_chars=900, total_chars=22000)
                else:
                    contexto = _build_context_limited(chunks, per_chunk_chars=1200, total_chars=18000)

                try:
                    raw = _call_llm(client, _build_prompt(contexto, analysis_mode=analysis_mode, analysis_window_months=analysis_window_months))
                except Exception as e_call:
                    msg = str(e_call).lower()
                    if "context window" in msg or "exceed" in msg:
                        if fonte_chunks.startswith("temporal_windows"):
                            contexto = _build_temporal_context(temporal_windows, per_chunk_chars=700, total_chars=14000)
                        else:
                            contexto = _build_context_limited(chunks, per_chunk_chars=800, total_chars=10000)
                        raw = _call_llm(client, _build_prompt(contexto, analysis_mode=analysis_mode, analysis_window_months=analysis_window_months))
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
                result.setdefault("evid_usadas", len(chunks))
                if not str(result.get("resumo") or "").strip():
                    evo = str(result.get("evolucao_do_discurso") or "").strip()
                    tese = str(result.get("execucao_vs_promessa") or "").strip()
                    mud = str(result.get("mudancas_estrategicas") or "").strip()
                    resumo_auto = " | ".join([x for x in [evo, tese, mud] if x])[:900]
                    result["resumo"] = resumo_auto or "Leitura qualitativa gerada sem resumo explícito."
                result.setdefault("docs_usados", None)
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
                    "window_months": int(window_months),
                })

                # salva
                save_patch6_run(
                    snapshot_id=str(snapshot_id),
                    ticker=t,
                    period_ref=period_ref,
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

                # mostra card (IMEDIATO)
                #_render_card(ticker=t, result=result, top_k_used=int(top_k), period_ref=period_ref)

                status_rows.append(
                    {
                        "ticker": t,
                        "status": "OK",
                        "metodo_chunks": fonte_chunks,
                        "top_k_used": int(topk_run),
                    "top_k_retry_used": (int(topk_retry_used) if topk_retry_used is not None else None),
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

        mostrar_tabela_status = debug_topk or (erros > 0)
        if mostrar_tabela_status:
            st.subheader("🧾 Status por ticker")
            st.dataframe(status_rows, use_container_width=True)
        else:
            st.caption("Execução concluída sem erros.")

        # Atualiza o relatório no MESMO lugar (sem duplicar seção)
        report_box.empty()
        _render_saved_report()
