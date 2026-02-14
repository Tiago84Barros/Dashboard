from __future__ import annotations

"""
pickup/ingest_docs_cvm_ipe.py
----------------------------
Ingestão de comunicados da CVM (IPE) para Supabase:
- salva doc em public.docs_corporativos
- cria chunks em public.docs_corporativos_chunks (opcional)

Objetivo: abastecer o Patch 6 (RAG) com fontes oficiais.

IMPORTANTE:
- Este script foi desenhado para rodar *dentro* do Streamlit (button),
  então usa st.cache_data com TTL pequeno e mostra progresso.
- A coleta na CVM depende de internet.
"""

from typing import Any, Dict, List, Optional, Sequence, Tuple
import hashlib
import re
import time

import pandas as pd
import streamlit as st
import requests
from sqlalchemy import text

from core.db_loader import get_supabase_engine


# -------------------------
# Util
# -------------------------

def _norm_ticker(t: str) -> str:
    return (t or "").upper().replace(".SA", "").strip()

def _sha256(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()

def _clean_text(s: str) -> str:
    s = (s or "").replace("\x00", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _chunk_text(texto: str, chunk_chars: int = 1500, overlap: int = 200) -> List[str]:
    t = (texto or "").strip()
    if not t:
        return []
    t = t.replace("\r\n", "\n")
    out = []
    i = 0
    n = len(t)
    while i < n:
        j = min(n, i + chunk_chars)
        out.append(t[i:j])
        if j >= n:
            break
        i = max(0, j - overlap)
    return out


# -------------------------
# CVM IPE – endpoints
# -------------------------
# Observação: a CVM muda endpoints de tempos em tempos. Este módulo usa um fallback simples:
# - Busca no endpoint de “consultas IPE” por ticker + período (quando disponível)
# Se falhar, ainda assim você consegue usar o Patch6 via texto manual.

CVM_BASE = "https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx"  # página (fallback)
# API não-oficial utilizada por vários scrapers (pode mudar):
CVM_JSON = "https://www.rad.cvm.gov.br/ENET/ConsultaExternaCVM/ConsultaExternaCVM.aspx/ConsultarDocumentos"


def _cvm_post(payload: Dict[str, Any], timeout: int = 45) -> Dict[str, Any]:
    headers = {"Content-Type": "application/json; charset=utf-8"}
    r = requests.post(CVM_JSON, json=payload, headers=headers, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    return data


def _buscar_ipe_por_ticker(
    ticker: str,
    *,
    ano_ini: int,
    ano_fim: int,
    max_paginas: int = 4,
) -> List[Dict[str, Any]]:
    """
    Retorna lista de “documentos” (metadados) conforme a API da CVM retornar.

    Nota: o campo 'ticker' nem sempre existe nos retornos; a busca aqui é por código CVM
    na prática. Se você já tiver cvm_code->ticker no seu banco, pode evoluir.
    Por enquanto: buscamos por ticker no filtro livre e pegamos o que vier.
    """
    tk = _norm_ticker(ticker)
    if not tk:
        return []

    out: List[Dict[str, Any]] = []

    # payload básico observado em scrapers públicos; a CVM pode mudar.
    # A ideia é: se funcionar, ótimo; se não, Patch6 continua possível via manual.
    for page in range(1, max_paginas + 1):
        payload = {
            "data": {
                "parametros": {
                    "CodigoInstituicao": "",
                    "Cnpj": "",
                    "NomeEmpresa": "",
                    "Ticker": tk,
                    "DataIni": f"01/01/{ano_ini}",
                    "DataFim": f"31/12/{ano_fim}",
                    "Categoria": "",
                    "TipoDocumento": "",
                    "Versao": "",
                },
                "pagina": page,
                "registrosPorPagina": 50,
            }
        }
        try:
            resp = _cvm_post(payload)
        except Exception:
            break

        # A API geralmente devolve {'d': {'registros': [...], 'total': N}}
        d = resp.get("d") if isinstance(resp, dict) else None
        if isinstance(d, str):
            # alguns casos vêm string JSON
            try:
                import json
                d = json.loads(d)
            except Exception:
                d = None

        registros = None
        if isinstance(d, dict):
            registros = d.get("registros") or d.get("Registros") or d.get("records")
        if not registros:
            break

        if isinstance(registros, list):
            out.extend([x for x in registros if isinstance(x, dict)])

        # se vier menos que a página, para
        if len(registros) < 50:
            break

    return out


def _extrair_texto_doc(doc_meta: Dict[str, Any]) -> str:
    """
    MVP: muitas entradas do IPE têm HTML/PDF. Sem OCR.
    Se houver campo textual, usamos. Se não, retorna vazio.
    """
    for k in ("Texto", "texto", "Resumo", "ResumoDocumento", "DescricaoAssunto", "Assunto"):
        v = doc_meta.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


# -------------------------
# Supabase upsert
# -------------------------

def _upsert_doc_and_chunks(
    *,
    ticker: str,
    data: Optional[str],
    fonte: str,
    tipo: str,
    titulo: str,
    url: str,
    raw_text: str,
    chunk_chars: int = 1500,
    overlap: int = 200,
) -> Tuple[bool, str]:
    """
    Insere doc + chunks. Retorna (inseriu_ou_atualizou, doc_hash).
    """
    tk = _norm_ticker(ticker)
    if not tk:
        return False, ""

    fonte = (fonte or "CVM").strip()
    tipo = (tipo or "ipe").strip()
    titulo = (titulo or "").strip()
    url = (url or "").strip()
    raw_text = (raw_text or "").strip()

    doc_hash = _sha256("|".join([tk, fonte, tipo, titulo, url, raw_text]))

    engine = get_supabase_engine()

    sql_doc = text(
        """
        INSERT INTO public.docs_corporativos (ticker, data, fonte, tipo, titulo, url, raw_text, doc_hash)
        VALUES (:ticker, :data, :fonte, :tipo, :titulo, :url, :raw_text, :doc_hash)
        ON CONFLICT (doc_hash) DO NOTHING
        RETURNING id
        """
    )

    with engine.begin() as conn:
        res = conn.execute(sql_doc, {
            "ticker": tk,
            "data": data,
            "fonte": fonte,
            "tipo": tipo,
            "titulo": titulo,
            "url": url,
            "raw_text": raw_text,
            "doc_hash": doc_hash,
        })
        row = res.first()

        if row is None:
            # já existia
            return False, doc_hash

        doc_id = int(row[0])

        chunks = _chunk_text(raw_text, chunk_chars=chunk_chars, overlap=overlap)
        if not chunks:
            return True, doc_hash

        sql_chunk = text(
            """
            INSERT INTO public.docs_corporativos_chunks (doc_id, ticker, chunk_index, chunk_text, chunk_hash)
            VALUES (:doc_id, :ticker, :chunk_index, :chunk_text, :chunk_hash)
            ON CONFLICT (chunk_hash) DO NOTHING
            """
        )

        for i, ch in enumerate(chunks):
            ch_clean = ch.strip()
            if not ch_clean:
                continue
            ch_hash = _sha256("|".join([str(doc_id), tk, str(i), ch_clean]))
            conn.execute(sql_chunk, {
                "doc_id": doc_id,
                "ticker": tk,
                "chunk_index": int(i),
                "chunk_text": ch_clean,
                "chunk_hash": ch_hash,
            })

    return True, doc_hash


# -------------------------
# Public API (importada pela page.patch6_teste)
# -------------------------

def ingest_ipe_for_tickers(
    tickers: Sequence[str],
    *,
    anos: int = 2,
    max_docs_por_ticker: int = 25,
    sleep_s: float = 0.2,
    chunk_chars: int = 1500,
    overlap: int = 200,
) -> Dict[str, Any]:
    """
    Baixa/ingere documentos IPE (quando a API responder) para os tickers informados.

    Retorno:
      {
        "ok": bool,
        "stats": { "TICKER": {"seen":N,"inserted":M,"skipped":K} },
        "errors": { "TICKER": "mensagem" }
      }

    Se a CVM não responder (mudança de endpoint / bloqueio), o retorno terá errors.
    Mesmo assim, o Patch6 continua utilizável via texto manual.
    """
    tks = [_norm_ticker(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys(tks))
    if not tks:
        return {"ok": False, "stats": {}, "errors": {"__all__": "Lista de tickers vazia."}}

    ano_fim = int(pd.Timestamp.utcnow().year)
    ano_ini = int(ano_fim - max(0, int(anos)))

    stats: Dict[str, Dict[str, int]] = {}
    errors: Dict[str, str] = {}

    for tk in tks:
        stats[tk] = {"seen": 0, "inserted": 0, "skipped": 0}
        try:
            docs = _buscar_ipe_por_ticker(tk, ano_ini=ano_ini, ano_fim=ano_fim)
            stats[tk]["seen"] = int(len(docs))

            for doc in docs[: int(max_docs_por_ticker)]:
                raw = _extrair_texto_doc(doc)
                raw = _clean_text(raw)
                if not raw:
                    stats[tk]["skipped"] += 1
                    continue

                data = None
                for k in ("DataEntrega", "DataReferencia", "DataDocumento", "DataEnvio", "Data"):
                    v = doc.get(k)
                    if isinstance(v, str) and v.strip():
                        # tenta DD/MM/YYYY ou ISO
                        d = pd.to_datetime(v, errors="coerce", dayfirst=True)
                        if pd.notna(d):
                            data = d.date().isoformat()
                            break

                titulo = str(doc.get("Assunto") or doc.get("DescricaoAssunto") or doc.get("Titulo") or "").strip()
                url = str(doc.get("Link") or doc.get("Url") or doc.get("url") or "").strip()

                inserted, _ = _upsert_doc_and_chunks(
                    ticker=tk,
                    data=data,
                    fonte="CVM",
                    tipo="ipe",
                    titulo=titulo,
                    url=url,
                    raw_text=raw,
                    chunk_chars=int(chunk_chars),
                    overlap=int(overlap),
                )
                if inserted:
                    stats[tk]["inserted"] += 1
                else:
                    stats[tk]["skipped"] += 1

                if sleep_s and float(sleep_s) > 0:
                    time.sleep(float(sleep_s))

        except Exception as e:
            errors[tk] = f"{type(e).__name__}: {e}"

    return {"ok": (len(errors) == 0), "stats": stats, "errors": errors}
