# core/patch6_report.py
"""Relatório profissional do Patch6.

Este módulo é chamado pela página analises_portfolio.py.

Objetivo:
- Consolidar "qualidade" (heurística), "perspectiva 12m" e "cobertura" (quantidade de empresas com análise).
- Exibir resumo executivo e cards por empresa.

Importante: este arquivo deve ser robusto a variações de schema e não quebrar a página.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st


def _get_engine():
    for mod_name, fn_name in (
        ("core.db_loader", "get_engine"),
        ("core.db", "get_engine"),
    ):
        try:
            mod = __import__(mod_name, fromlist=[fn_name])
            return getattr(mod, fn_name)()
        except Exception:
            pass
    return None


def _css() -> None:
    st.markdown(
        """
<style>
  .p6r-wrap{max-width:1200px;margin:0 auto;}
  .p6r-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin:14px 0 10px 0;}
  @media (max-width:1100px){.p6r-grid{grid-template-columns:repeat(2,minmax(0,1fr));}}
  @media (max-width:640px){.p6r-grid{grid-template-columns:1fr;}}

  .p6r-card{border-radius:18px;border:1px solid rgba(255,255,255,0.10);
    background:rgba(255,255,255,0.04);padding:14px 14px 12px 14px;}
  .p6r-label{font-size:13px;opacity:0.80;margin-bottom:6px;}
  .p6r-value{font-size:30px;font-weight:800;line-height:1.1;margin-bottom:6px;}
  .p6r-extra{font-size:12px;opacity:0.75;line-height:1.35;}

  .p6r-row{display:flex;flex-wrap:wrap;gap:10px;margin:10px 0 0 0;}
  .p6r-pill{display:inline-flex;align-items:center;gap:8px;padding:6px 10px;border-radius:999px;
    font-size:12px;border:1px solid rgba(255,255,255,0.14);background:rgba(255,255,255,0.04);}

  .p6r-company{border-radius:18px;border:1px solid rgba(255,255,255,0.10);
    background:rgba(255,255,255,0.03);padding:14px;margin:0 0 12px 0;}
  .p6r-company-head{display:flex;justify-content:space-between;align-items:flex-start;gap:12px;margin-bottom:10px;}
  .p6r-company-title{font-size:18px;font-weight:900;margin:0;}
  .p6r-company-meta{font-size:12px;opacity:0.75;line-height:1.35;}
  .p6r-company-body{font-size:14px;opacity:0.92;line-height:1.45;white-space:pre-wrap;}

  .p6r-badge{display:inline-flex;align-items:center;gap:6px;padding:6px 10px;border-radius:999px;
    font-size:12px;border:1px solid rgba(255,255,255,0.14);background:rgba(255,255,255,0.04);}
  .p6r-badge-strong{border-color: rgba(55, 220, 150, 0.35); background: rgba(55, 220, 150, 0.10);}
  .p6r-badge-neutral{border-color: rgba(120, 170, 255, 0.35); background: rgba(120, 170, 255, 0.10);}
  .p6r-badge-caution{border-color: rgba(255, 170, 60, 0.35); background: rgba(255, 170, 60, 0.10);}
</style>
        """,
        unsafe_allow_html=True,
    )


def _badge_class(direcao: str) -> str:
    d = (direcao or "").lower()
    if "construt" in d:
        return "p6r-badge p6r-badge-strong"
    if "equilibr" in d or "neutr" in d:
        return "p6r-badge p6r-badge-neutral"
    return "p6r-badge p6r-badge-caution"


def _try_json(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return v
    return v


def _fetch_latest_runs(tickers: List[str], period_ref: str) -> pd.DataFrame:
    engine = _get_engine()
    if engine is None:
        return pd.DataFrame()

    from sqlalchemy import text

    # Tabelas candidatas
    tables = [
        "patch6_runs",
        "public.patch6_runs",
    ]

    for t in tables:
        try:
            q = text(
                f"""
                select *
                from {t}
                where ticker = any(:tickers)
                  and (:period_ref is null or period_ref = :period_ref)
                order by created_at desc nulls last, id desc nulls last
                """
            )
            df = pd.read_sql(q, engine, params={"tickers": tickers, "period_ref": period_ref})
            if not df.empty:
                return df
        except Exception:
            continue

    return pd.DataFrame()


def _pick_latest_per_ticker(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if "ticker" not in df.columns:
        return df
    # mantém a primeira ocorrência por ticker (já ordenado desc)
    return df.drop_duplicates(subset=["ticker"], keep="first").reset_index(drop=True)


def _infer_quality(latest: pd.DataFrame, tickers_total: int) -> Tuple[str, str]:
    """Qualidade heurística baseada em cobertura + completude do conteúdo."""

    cov = len(latest)
    cov_ratio = cov / max(1, tickers_total)

    # mede completude
    filled = 0
    for col in ("tese", "resumo", "direcao"):
        if col in latest.columns:
            filled += int((latest[col].fillna("") != "").sum())

    # heurística simples
    if cov_ratio >= 0.9 and filled >= cov * 2:
        return "Alta", "Cobertura alta e conteúdo consistente (tese/direção presentes na maioria dos ativos)."
    if cov_ratio >= 0.6:
        return "Média", "Cobertura razoável, porém há lacunas em alguns ativos (ex.: falta de tese/direção)."
    return "Baixa", "Cobertura limitada: poucos ativos com análise recente ou com campos incompletos."


def _perspectiva_distribution(latest: pd.DataFrame) -> Dict[str, int]:
    out = {"Construtivas": 0, "Equilibradas": 0, "Cautelosas": 0}
    if latest.empty:
        return out
    if "direcao" not in latest.columns:
        return out
    for v in latest["direcao"].fillna(""):
        s = str(v).lower()
        if "construt" in s:
            out["Construtivas"] += 1
        elif "equilibr" in s or "neutr" in s:
            out["Equilibradas"] += 1
        elif s:
            out["Cautelosas"] += 1
    return out


def _coverage(latest: pd.DataFrame, tickers_total: int) -> str:
    return f"{len(latest)}/{tickers_total}"


def _fetch_docs_and_recortes(tickers: List[str]) -> Dict[str, Tuple[Optional[int], Optional[int]]]:
    """Retorna (docs, recortes) por ticker, se as tabelas existirem."""

    engine = _get_engine()
    if engine is None:
        return {t: (None, None) for t in tickers}

    from sqlalchemy import text

    # candidatos
    docs_tables = [
        ("patch6_docs", "public.patch6_docs"),
        ("rag_docs", "public.rag_docs"),
    ]
    chunks_tables = [
        ("patch6_chunks", "public.patch6_chunks"),
        ("rag_chunks", "public.rag_chunks"),
    ]

    docs_map: Dict[str, Optional[int]] = {t: None for t in tickers}
    recortes_map: Dict[str, Optional[int]] = {t: None for t in tickers}

    # docs
    for a, b in docs_tables:
        for table in (a, b):
            try:
                q = text(
                    f"""
                    select ticker, count(*) as n
                    from {table}
                    where ticker = any(:tickers)
                    group by ticker
                    """
                )
                rows = engine.execute(q, {"tickers": tickers}).fetchall()
                for r in rows:
                    docs_map[str(r[0]).upper()] = int(r[1])
                raise StopIteration
            except StopIteration:
                break
            except Exception:
                continue

    # recortes/chunks
    for a, b in chunks_tables:
        for table in (a, b):
            try:
                q = text(
                    f"""
                    select ticker, count(*) as n
                    from {table}
                    where ticker = any(:tickers)
                    group by ticker
                    """
                )
                rows = engine.execute(q, {"tickers": tickers}).fetchall()
                for r in rows:
                    recortes_map[str(r[0]).upper()] = int(r[1])
                raise StopIteration
            except StopIteration:
                break
            except Exception:
                continue

    return {t: (docs_map.get(t), recortes_map.get(t)) for t in tickers}


def render_patch6_report(*, tickers: List[str], period_ref: str, window_months: int = 12) -> None:
    _css()

    tickers = [str(t).upper() for t in tickers if t]
    if not tickers:
        st.info("Sem tickers no snapshot.")
        return

    df = _fetch_latest_runs(tickers, period_ref)
    latest = _pick_latest_per_ticker(df)

    quality_label, quality_expl = _infer_quality(latest, len(tickers))
    persp = _perspectiva_distribution(latest)
    coverage = _coverage(latest, len(tickers))

    st.markdown('<div class="p6r-wrap">', unsafe_allow_html=True)

    st.markdown(
        f"""
        <div class="p6r-grid">
          <div class="p6r-card">
            <div class="p6r-label">Qualidade (heurística)</div>
            <div class="p6r-value">{quality_label}</div>
            <div class="p6r-extra">{quality_expl}</div>
          </div>
          <div class="p6r-card">
            <div class="p6r-label">Perspectiva 12m</div>
            <div class="p6r-value">{('Neutra' if persp['Construtivas']==persp['Cautelosas'] else ('Construtiva' if persp['Construtivas']>persp['Cautelosas'] else 'Cautelosa'))}</div>
            <div class="p6r-extra">Direcionalidade agregada do conjunto (construtivo vs cauteloso).</div>
          </div>
          <div class="p6r-card">
            <div class="p6r-label">Cobertura</div>
            <div class="p6r-value">{coverage}</div>
            <div class="p6r-extra">Ativos com análise recente no período {period_ref}.</div>
          </div>
          <div class="p6r-card">
            <div class="p6r-label">Distribuição</div>
            <div class="p6r-value">{persp['Construtivas']} • {persp['Equilibradas']} • {persp['Cautelosas']}</div>
            <div class="p6r-extra">Construtivas • Equilibradas • Cautelosas</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Resumo executivo
    st.markdown("### 🧾 Resumo executivo")
    if latest.empty:
        st.info("Nenhuma análise encontrada ainda para o período. Rode a LLM para gerar os relatórios.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    # resumo: concatena 1 linha por ticker
    lines = []
    for _, r in latest.iterrows():
        t = str(r.get("ticker", "")).upper()
        direcao = str(r.get("direcao", "") or "").strip() or "—"
        tese = str(r.get("tese", "") or r.get("resumo", "") or "").strip()
        tese = tese.replace("\n", " ")
        if len(tese) > 180:
            tese = tese[:177] + "…"
        lines.append(f"- **{t}** ({direcao}): {tese if tese else '—'}")

    st.markdown("\n".join(lines))

    # Relatórios por empresa
    st.markdown("### 🧩 Relatórios por empresa")

    docs_recortes = _fetch_docs_and_recortes(tickers)

    for _, r in latest.iterrows():
        t = str(r.get("ticker", "")).upper()
        direcao = str(r.get("direcao", "") or "").strip() or "—"
        badge = _badge_class(direcao)

        # métricas
        docs, recortes = docs_recortes.get(t, (None, None))
        meta_parts = []
        if docs is not None:
            meta_parts.append(f"Documentos: {docs}")
        if recortes is not None:
            meta_parts.append(f"Recortes (RAG): {recortes}")
        if "created_at" in latest.columns:
            dt = r.get("created_at")
            if pd.notna(dt):
                meta_parts.append(f"Atualizado: {str(dt)[:19]}")

        meta = " • ".join(meta_parts) if meta_parts else "—"

        body = str(r.get("tese", "") or r.get("resumo", "") or r.get("raw", "") or "").strip()
        if not body:
            body = "Sem tese/resumo persistidos para este ativo."

        st.markdown(
            f"""
            <div class="p6r-company">
              <div class="p6r-company-head">
                <div>
                  <h3 class="p6r-company-title">{t}</h3>
                  <div class="p6r-company-meta">{meta}</div>
                </div>
                <div class="{badge}">{direcao}</div>
              </div>
              <div class="p6r-company-body">{body}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("</div>", unsafe_allow_html=True)
