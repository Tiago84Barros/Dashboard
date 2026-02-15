from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple
import hashlib
import json
from datetime import date

import pandas as pd
from sqlalchemy import text

from core.db_loader import get_supabase_engine


# ============================================================
# Helpers
# ============================================================

def _norm_ticker(t: str) -> str:
    return (t or "").upper().replace(".SA", "").strip()

def _sha256(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()

def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str)

def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if pd.isna(v):
            return None
        return v
    except Exception:
        return None

def _to_int(x: Any) -> int:
    try:
        return int(x)
    except Exception:
        return 0


# ============================================================
# SQL schema (referência)
# ============================================================
# Você já vai criar no Supabase via SQL:
# - public.patch6_assessments
# - public.patch6_initiatives
# conforme te passei anteriormente.
#
# Este módulo assume que:
# - docs estão em public.docs_corporativos (doc_hash, ticker, data, fonte, tipo, titulo, url, raw_text, created_at)
# - chunks não são necessários para essa etapa (Patch6 usa docs completos / top N).
#
# ============================================================


# ============================================================
# Docs query (para montar docs_hash)
# ============================================================

def fetch_recent_docs_for_ticker(
    ticker: str,
    *,
    limit: int = 12,
    tipos_prioritarios: Optional[Sequence[str]] = None,
    fontes_prioritarias: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Busca documentos estratégicos recentes no Supabase (docs_corporativos).
    Retorna lista de dicts: {id, ticker, data, fonte, tipo, titulo, url, raw_text, doc_hash}
    """
    tk = _norm_ticker(ticker)
    if not tk:
        return []

    engine = get_supabase_engine()

    # filtros opcionais (prioridade, não exclusão): a seleção final é por ORDER BY
    # Estratégia:
    # - ordena por data desc, created_at desc
    # - se tipos_prioritarios ou fontes_prioritarias existirem, traz eles primeiro via CASE WHEN

    tipos = [str(x).strip().lower() for x in (tipos_prioritarios or []) if str(x).strip()]
    fontes = [str(x).strip().lower() for x in (fontes_prioritarias or []) if str(x).strip()]

    order_priority = ""
    params: Dict[str, Any] = {"ticker": tk, "limit": int(limit)}

    if tipos:
        params["tipos"] = tipos
        order_priority += """
        CASE WHEN lower(tipo) = ANY(:tipos) THEN 0 ELSE 1 END,
        """
    if fontes:
        params["fontes"] = fontes
        order_priority += """
        CASE WHEN lower(fonte) = ANY(:fontes) THEN 0 ELSE 1 END,
        """

    sql = f"""
    SELECT
      id, ticker, data, fonte, tipo, titulo, url, raw_text, doc_hash, created_at
    FROM public.docs_corporativos
    WHERE ticker = :ticker
    ORDER BY
      {order_priority}
      data DESC NULLS LAST,
      created_at DESC
    LIMIT :limit
    """

    with engine.connect() as conn:
        df = pd.read_sql_query(text(sql), conn, params=params)

    if df is None or df.empty:
        return []

    # limpa e normaliza
    out: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        out.append({
            "id": _to_int(r.get("id")),
            "ticker": _norm_ticker(str(r.get("ticker", ""))),
            "data": (pd.to_datetime(r.get("data"), errors="coerce").date().isoformat()
                     if pd.notna(r.get("data")) else None),
            "fonte": str(r.get("fonte", "") or "").strip(),
            "tipo": str(r.get("tipo", "") or "").strip(),
            "titulo": str(r.get("titulo", "") or "").strip(),
            "url": str(r.get("url", "") or "").strip(),
            "raw_text": str(r.get("raw_text", "") or "").strip(),
            "doc_hash": str(r.get("doc_hash", "") or "").strip(),
            "created_at": str(r.get("created_at", "") or ""),
        })
    return out


def build_docs_hash(docs: Sequence[Dict[str, Any]]) -> str:
    """
    Hash estável baseado no conjunto de documentos usados.
    Recomendação: usar doc_hash + id + data, em ordem, para ficar determinístico.
    """
    items: List[str] = []
    for d in (docs or []):
        items.append(
            "|".join([
                str(d.get("id") or ""),
                str(d.get("doc_hash") or ""),
                str(d.get("data") or ""),
                str(d.get("tipo") or ""),
                str(d.get("fonte") or ""),
            ])
        )
    payload = "||".join(items)
    return _sha256(payload)


def build_run_key(
    *,
    ticker: str,
    docs_hash: str,
    provider: str,
    model: str,
    patch_version: str = "patch6_v1",
    knobs: Optional[Dict[str, Any]] = None,
) -> str:
    """
    run_key = hash de tudo que influencia o output final.
    Se não mudar docs/model/knobs, você NÃO precisa chamar LLM de novo.
    """
    tk = _norm_ticker(ticker)
    base = {
        "ticker": tk,
        "docs_hash": docs_hash,
        "provider": (provider or "").strip().lower(),
        "model": (model or "").strip(),
        "patch_version": (patch_version or "").strip(),
        "knobs": knobs or {},
    }
    return _sha256(_json_dumps(base))


def last_doc_date(docs: Sequence[Dict[str, Any]]) -> Optional[str]:
    dts: List[date] = []
    for d in (docs or []):
        s = d.get("data")
        if isinstance(s, str) and s.strip():
            dt = pd.to_datetime(s, errors="coerce")
            if pd.notna(dt):
                dts.append(dt.date())
    if not dts:
        return None
    return max(dts).isoformat()


# ============================================================
# Cache read (assessment)
# ============================================================

def get_assessment_by_run_key(run_key: str) -> Optional[Dict[str, Any]]:
    """
    Retorna assessment consolidado e iniciativas (se existirem) a partir do run_key.
    """
    rk = (run_key or "").strip()
    if not rk:
        return None

    engine = get_supabase_engine()

    sql_a = """
    SELECT *
    FROM public.patch6_assessments
    WHERE run_key = :rk
    LIMIT 1
    """

    with engine.connect() as conn:
        df = pd.read_sql_query(text(sql_a), conn, params={"rk": rk})

    if df is None or df.empty:
        return None

    a = df.iloc[0].to_dict()

    # iniciativas
    sql_i = """
    SELECT *
    FROM public.patch6_initiatives
    WHERE assessment_id = :aid
    ORDER BY iniciativa_index ASC
    """
    aid = _to_int(a.get("id"))
    with engine.connect() as conn:
        dfi = pd.read_sql_query(text(sql_i), conn, params={"aid": aid})

    iniciativas = []
    if dfi is not None and not dfi.empty:
        iniciativas = [r.to_dict() for _, r in dfi.iterrows()]

    return {"assessment": a, "initiatives": iniciativas}


# ============================================================
# Persist (upsert)
# ============================================================

def upsert_assessment_and_initiatives(
    *,
    run_key: str,
    ticker: str,
    provider: str,
    model: str,
    docs_hash: str,
    docs_count: int,
    last_doc_date_iso: Optional[str],

    score_regua_0_100: Optional[float],
    ajuste_ia_pp: Optional[float],
    score_final_0_100: Optional[float],
    risco_execucao: str,

    rating_compra: str,
    motivo_compra: str,
    fator_peso_aporte: Optional[float],

    resumo_1_paragrafo: str,
    pontos_a_favor: Optional[List[str]] = None,
    pontos_contra: Optional[List[str]] = None,
    perguntas_criticas: Optional[List[str]] = None,

    llm_json: Optional[Dict[str, Any]] = None,
    iniciativas: Optional[List[Dict[str, Any]]] = None,
) -> int:
    """
    Upsert por run_key:
      - se já existir, atualiza campos
      - apaga iniciativas antigas e reinsere (simples, robusto)

    Retorna assessment_id.
    """
    rk = (run_key or "").strip()
    tk = _norm_ticker(ticker)
    if not rk or not tk:
        raise ValueError("run_key e ticker são obrigatórios")

    engine = get_supabase_engine()

    sql_upsert = text("""
    INSERT INTO public.patch6_assessments (
      ticker, run_key, provider, model,
      docs_count, docs_hash, last_doc_date,

      score_regua_0_100, ajuste_ia_pp, score_final_0_100,
      risco_execucao, rating_compra, motivo_compra, fator_peso_aporte,

      resumo_1_paragrafo, pontos_a_favor, pontos_contra, perguntas_criticas,
      llm_json
    )
    VALUES (
      :ticker, :run_key, :provider, :model,
      :docs_count, :docs_hash, :last_doc_date,

      :score_regua_0_100, :ajuste_ia_pp, :score_final_0_100,
      :risco_execucao, :rating_compra, :motivo_compra, :fator_peso_aporte,

      :resumo_1_paragrafo, :pontos_a_favor, :pontos_contra, :perguntas_criticas,
      :llm_json
    )
    ON CONFLICT (run_key) DO UPDATE SET
      updated_at = now(),
      docs_count = EXCLUDED.docs_count,
      docs_hash = EXCLUDED.docs_hash,
      last_doc_date = EXCLUDED.last_doc_date,

      score_regua_0_100 = EXCLUDED.score_regua_0_100,
      ajuste_ia_pp = EXCLUDED.ajuste_ia_pp,
      score_final_0_100 = EXCLUDED.score_final_0_100,

      risco_execucao = EXCLUDED.risco_execucao,
      rating_compra = EXCLUDED.rating_compra,
      motivo_compra = EXCLUDED.motivo_compra,
      fator_peso_aporte = EXCLUDED.fator_peso_aporte,

      resumo_1_paragrafo = EXCLUDED.resumo_1_paragrafo,
      pontos_a_favor = EXCLUDED.pontos_a_favor,
      pontos_contra = EXCLUDED.pontos_contra,
      perguntas_criticas = EXCLUDED.perguntas_criticas,
      llm_json = EXCLUDED.llm_json
    RETURNING id
    """)

    # jsonb
    def _as_jsonb_list(xs: Optional[List[str]]) -> Optional[str]:
        if not xs:
            return None
        return _json_dumps(xs)

    params = {
        "ticker": tk,
        "run_key": rk,
        "provider": (provider or "").strip().lower(),
        "model": (model or "").strip(),
        "docs_count": _to_int(docs_count),
        "docs_hash": (docs_hash or "").strip(),
        "last_doc_date": last_doc_date_iso,

        "score_regua_0_100": _to_float(score_regua_0_100),
        "ajuste_ia_pp": _to_float(ajuste_ia_pp),
        "score_final_0_100": _to_float(score_final_0_100),

        "risco_execucao": (risco_execucao or "").strip(),
        "rating_compra": (rating_compra or "").strip(),
        "motivo_compra": (motivo_compra or "").strip(),
        "fator_peso_aporte": _to_float(fator_peso_aporte),

        "resumo_1_paragrafo": (resumo_1_paragrafo or "").strip(),
        "pontos_a_favor": _as_jsonb_list(pontos_a_favor),
        "pontos_contra": _as_jsonb_list(pontos_contra),
        "perguntas_criticas": _as_jsonb_list(perguntas_criticas),
        "llm_json": _json_dumps(llm_json) if llm_json else None,
    }

    with engine.begin() as conn:
        res = conn.execute(sql_upsert, params)
        row = res.first()
        if row is None:
            raise RuntimeError("Falha ao inserir/atualizar patch6_assessments")
        assessment_id = int(row[0])

        # simplificação robusta: delete+insert iniciativas
        conn.execute(text("DELETE FROM public.patch6_initiatives WHERE assessment_id = :aid"), {"aid": assessment_id})

        itens = iniciativas or []
        if itens:
            sql_ins_i = text("""
            INSERT INTO public.patch6_initiatives (
              assessment_id, ticker, iniciativa_index,
              tipo, descricao_curta, horizonte, impacto_esperado, sinal,
              dependencias, evidencia
            )
            VALUES (
              :assessment_id, :ticker, :idx,
              :tipo, :descricao, :horizonte, :impacto, :sinal,
              :dependencias, :evidencia
            )
            """)
            for idx, it in enumerate(itens):
                dep = it.get("dependencias", None)
                ev = it.get("evidencia", None)
                conn.execute(sql_ins_i, {
                    "assessment_id": assessment_id,
                    "ticker": tk,
                    "idx": int(idx),
                    "tipo": str(it.get("tipo", "") or "").strip(),
                    "descricao": str(it.get("descricao_curta", "") or "").strip(),
                    "horizonte": str(it.get("horizonte", "") or "").strip(),
                    "impacto": str(it.get("impacto_esperado", "") or "").strip(),
                    "sinal": str(it.get("sinal", "") or "").strip(),
                    "dependencias": _json_dumps(dep) if dep is not None else None,
                    "evidencia": _json_dumps(ev) if ev is not None else None,
                })

    return assessment_id


# ============================================================
# Convenience: preparar pacote docs + chaves
# ============================================================

def prepare_patch6_docs_and_keys(
    ticker: str,
    *,
    limit_docs: int = 12,
    tipos_prioritarios: Optional[Sequence[str]] = None,
    fontes_prioritarias: Optional[Sequence[str]] = None,
    provider: str = "openai",
    model: str = "gpt-4.1-mini",
    patch_version: str = "patch6_v1",
    knobs: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Função utilitária para o Patch6:
      - busca docs
      - calcula docs_hash, last_doc_date
      - calcula run_key
    """
    tk = _norm_ticker(ticker)
    docs = fetch_recent_docs_for_ticker(
        tk,
        limit=int(limit_docs),
        tipos_prioritarios=tipos_prioritarios,
        fontes_prioritarias=fontes_prioritarias,
    )
    dh = build_docs_hash(docs)
    rk = build_run_key(
        ticker=tk,
        docs_hash=dh,
        provider=provider,
        model=model,
        patch_version=patch_version,
        knobs=knobs or {},
    )
    return {
        "ticker": tk,
        "docs": docs,
        "docs_count": len(docs),
        "docs_hash": dh,
        "last_doc_date": last_doc_date(docs),
        "run_key": rk,
    }
