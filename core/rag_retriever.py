from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import math
import re
from collections import defaultdict

from core.db_loader import get_supabase_engine
from core.rag_multitopic import retrieve_multitopic_chunks
import core.ai_models.llm_client.factory as llm_factory


# ────────────────────────────────────────────────────────────────────────────────
# Tuning constants — adjust here to calibrate retrieval behaviour
# ────────────────────────────────────────────────────────────────────────────────

# Weight applied to the recency component in the final hybrid score.
# Increase to bias more strongly toward recent chunks.
# Range guidance: 1.5 (soft recency bias) to 4.0 (strong recency bias).
RECENCY_ALPHA: float = 2.5

# Hard temporal window in months. Chunks older than this are excluded.
# Must match the analytical concept used in the Patch6 pipeline (36 months).
TEMPORAL_WINDOW_MONTHS: int = 36

# Coverage thresholds — used to classify recent coverage quality per ticker.
# "recent" is defined as within the first 12 months of the window.
COVERAGE_HIGH_THRESHOLD: int = 5   # ≥ N recent chunks → "alta"
COVERAGE_MED_THRESHOLD: int = 2    # ≥ N recent chunks → "média"; below → "baixa"


@dataclass
class RagHit:
    doc_id: Optional[int]
    ticker: str
    chunk_text: str
    dist: Optional[float] = None
    data_doc: Optional[str] = None
    tipo_doc: str = ""
    strategic_theme: str = ""
    score: float = 0.0
    recency_score: float = 0.0   # v7: 0.0–1.0, filled by compute_recency_score()


def _extract_year(data_doc: Any) -> str:
    s = str(data_doc or "")[:4]
    return s if s.isdigit() else ""


# ────────────────────────────────────────────────────────────────────────────────
# v7 — Recency scoring
# ────────────────────────────────────────────────────────────────────────────────

def compute_recency_score(
    data_doc: Any,
    reference_date: Optional[datetime] = None,
    max_window_months: int = TEMPORAL_WINDOW_MONTHS,
) -> float:
    """Score de recência temporal: 1.0 (hoje) → 0.0 (borda da janela) → inelegível.

    Usa decaimento linear em dois estágios para maior discriminação:
      - 0–12m:  1.0 → 0.65  (planalto recente: alta relevância)
      - 12–24m: 0.65 → 0.30 (zona média: relevância moderada)
      - 24–36m: 0.30 → 0.0  (zona histórica: relevância baixa)
      - >36m:   retorna -1.0 (hard ineligibility marker)

    Args:
        data_doc:       String com data no formato YYYY-MM-DD (ou YYYY-MM ou YYYY).
        reference_date: Data de referência (default = utcnow()).
        max_window_months: Limite da janela em meses.

    Returns:
        float em [-1.0, 1.0]. Valores negativos indicam fora da janela.
    """
    s = str(data_doc or "")[:10].strip()
    if len(s) < 4 or not s[:4].isdigit():
        return 0.0   # data desconhecida → tratada como neutra, não inelegível

    try:
        year = int(s[:4])
        month = int(s[5:7]) if len(s) >= 7 and s[4] in ("-", "/") else 6
        ref = reference_date or datetime.utcnow()
        months_old = (ref.year - year) * 12 + (ref.month - month)

        if months_old < 0:
            # Documento com data futura — tratar como mais recente possível
            return 1.0
        if months_old > max_window_months:
            return -1.0   # fora da janela — marcador de inelegibilidade

        # Decaimento em dois estágios
        half = max_window_months / 2   # default: 18 meses
        if months_old <= 12:
            return 1.0 - 0.35 * (months_old / 12.0)    # 1.0 → 0.65
        if months_old <= 24:
            return 0.65 - 0.35 * ((months_old - 12.0) / 12.0)   # 0.65 → 0.30
        # 24..max_window_months
        return 0.30 - 0.30 * ((months_old - 24.0) / max(1, max_window_months - 24.0))  # 0.30 → 0.0
    except Exception:
        return 0.0   # parse failure → neutro


def _bucket_for_months(data_doc: Any) -> str:
    s = str(data_doc or "")[:10]
    if len(s) < 7:
        return "sem_data"
    try:
        year = int(s[:4])
        month = int(s[5:7])
        # aproximação robusta sem depender de timezone do container
        # bucket relativo pelos últimos 36 meses; anos mais recentes tendem a ficar em buckets menores
        from datetime import datetime
        now = datetime.utcnow()
        months_delta = (now.year - year) * 12 + (now.month - month)
        if months_delta <= 12:
            return "0_12m"
        if months_delta <= 24:
            return "12_24m"
        if months_delta <= 36:
            return "24_36m"
        return "fora_janela"
    except Exception:
        return "sem_data"


def _tipo_bucket(tipo_doc: str) -> str:
    blob = (tipo_doc or "").lower()
    if "fato relevante" in blob or "comunicado" in blob:
        return "evento"
    if any(k in blob for k in ["itr", "dfp", "fre", "formulário", "formulario"]):
        return "financeiro"
    if any(k in blob for k in ["assembleia", "governança", "governanca", "estatuto", "conselho"]):
        return "governanca"
    return "outros"


def _materiality_bonus(txt: str) -> float:
    score = 0.0
    if re.search(r"R\$\s?[\d\.,]+", txt):
        score += 1.4
    if re.search(r"\b\d+[\.,]?\d*%", txt):
        score += 1.0
    if re.search(r"\b\d+[\.,]?\d*\s*(milh|milhão|milhoes|milhões|bilh|bilhão|bilhoes|bilhões)\b", txt, flags=re.I):
        score += 1.2
    if re.search(r"\b(capex|guidance|payout|dividendos?|jcp|alavanc|dívida|divida|margem|ebitda|covenant|reestrutura|aquisi|cisão|fusão|incorporação|desinvest)\b", txt, flags=re.I):
        score += 1.4
    return score


def _strategic_bonus(txt: str, theme: str = "", tipo_doc: str = "") -> float:
    blob = f"{txt} {theme} {tipo_doc}".lower()
    kws = [
        "guidance", "capex", "dividend", "jcp", "payout", "dívida", "divida", "alavanc", "desalavanc",
        "reestrutura", "aquisi", "fusão", "cisão", "incorp", "joint venture", "governan", "conting",
        "covenant", "emissão", "debênt", "debent", "margem", "eficiência", "eficiencia", "produção", "producao",
        "retorno ao acionista", "alocação de capital", "alocacao de capital", "desinvest", "meta", "execução", "execucao"
    ]
    return 0.35 * sum(1 for k in kws if k in blob)


def _generic_penalty(txt: str) -> float:
    blob = txt.lower()
    bad = [
        "a companhia segue", "busca continuamente", "estratégia de longo prazo", "estrategia de longo prazo",
        "permanece comprometida", "segue focada", "reforça seu compromisso", "fortalecer sua atuação",
        "criação de valor no longo prazo", "geração de valor no longo prazo", "geracao de valor no longo prazo"
    ]
    return 1.6 if any(k in blob for k in bad) else 0.0


def _score_text_quality(
    texto: str,
    theme: str = "",
    tipo_doc: str = "",
    dist: Optional[float] = None,
    data_doc: Optional[str] = None,     # v7: date string for recency component
    recency_alpha: float = RECENCY_ALPHA,
) -> float:
    """Score híbrido = qualidade_textual + alpha * recência.

    score_final = (semantic_distance_bonus + materiality + strategic - generic + length + type_bonus)
                + recency_alpha * recency_score

    Chunks mais recentes recebem bônus de até +recency_alpha.
    Chunks com data inválida ou fora da janela recebem penalização severa.

    Ajuste de calibração:
      - RECENCY_ALPHA = 2.5  → recência equivale a distância semântica boa (3.8 - dist)
      - Para bias mais forte: aumente para 3.5–4.0
      - Para bias mais suave: reduza para 1.5
    """
    txt = (texto or "").strip()
    if not txt:
        return -999.0

    score = 0.0

    # ── Componente semântico (distância vetorial)
    if dist is not None and not (isinstance(dist, float) and math.isnan(dist)):
        score += max(0.0, 3.8 - float(dist))

    # ── Componente de qualidade textual
    score += _materiality_bonus(txt)
    score += _strategic_bonus(txt, theme=theme, tipo_doc=tipo_doc)
    score -= _generic_penalty(txt)

    n = len(txt)
    if 220 <= n <= 1500:
        score += 0.9
    elif 120 <= n < 220:
        score += 0.3
    elif n < 100:
        score -= 1.0

    tipo_blob = (tipo_doc or "").lower()
    if "fato relevante" in tipo_blob:
        score += 1.2
    elif "comunicado" in tipo_blob:
        score += 0.8
    elif any(k in tipo_blob for k in ["itr", "dfp", "fre"]):
        score += 0.6

    # ── v7: componente de recência temporal ──────────────────────────────────
    # score_final = score_qualidade + RECENCY_ALPHA * recency_score
    # Chunks dentro da janela recebem bônus 0..RECENCY_ALPHA.
    # Chunks fora da janela (recency < 0) recebem penalidade severa.
    if data_doc is not None:
        rec = compute_recency_score(data_doc)
        if rec < 0:
            # Fora da janela analítica — torna o chunk inelegível
            return -999.0
        score += recency_alpha * rec

    return score


def _take_best(
    pool: List[RagHit],
    selected: List[RagHit],
    used: set,
    target: int,
    per_doc: Dict[int, int],
    per_year: Dict[str, int],
    per_tipo: Dict[str, int],
    max_per_doc: int,
) -> None:
    for cand in pool:
        if len(selected) >= target:
            return
        # v7: hard-exclude chunks outside temporal window (score = -999.0)
        if cand.score <= -900.0:
            continue
        key = (cand.doc_id, cand.chunk_text)
        if key in used:
            continue
        if cand.doc_id is not None and per_doc[int(cand.doc_id)] >= max_per_doc:
            continue
        year = _extract_year(cand.data_doc) or "sem_ano"
        tipo = _tipo_bucket(cand.tipo_doc)
        selected.append(cand)
        used.add(key)
        if cand.doc_id is not None:
            per_doc[int(cand.doc_id)] += 1
        per_year[year] += 1
        per_tipo[tipo] += 1


def _select_diverse_hits(hits: List[RagHit], top_k: int) -> List[RagHit]:
    top_k = max(12, int(top_k))
    year_buckets: Dict[str, List[RagHit]] = defaultdict(list)
    tipo_buckets: Dict[str, List[RagHit]] = defaultdict(list)
    by_all: List[RagHit] = list(hits)

    for h in hits:
        year_buckets[_bucket_for_months(h.data_doc)].append(h)
        tipo_buckets[_tipo_bucket(h.tipo_doc)].append(h)

    for bucket in year_buckets.values():
        bucket.sort(key=lambda h: (-h.score, str(h.data_doc or ""), -(int(h.doc_id) if h.doc_id else 0), h.chunk_text[:48]))
    for bucket in tipo_buckets.values():
        bucket.sort(key=lambda h: (-h.score, str(h.data_doc or ""), -(int(h.doc_id) if h.doc_id else 0), h.chunk_text[:48]))

    selected: List[RagHit] = []
    used = set()
    per_doc: Dict[int, int] = defaultdict(int)
    per_year: Dict[str, int] = defaultdict(int)
    per_tipo: Dict[str, int] = defaultdict(int)

    # quota temporal agressiva, mas segura
    temporal_targets = {
        "0_12m": 3,
        "12_24m": 3,
        "24_36m": 2,
    }
    if top_k >= 14:
        temporal_targets["24_36m"] = 3
    for bucket_name, target in temporal_targets.items():
        _take_best(year_buckets.get(bucket_name, []), selected, used, len(selected) + target, per_doc, per_year, per_tipo, max_per_doc=2)

    # quota por tipo documental
    tipo_targets = {
        "financeiro": 3,
        "evento": 3,
        "governanca": 1,
    }
    for bucket_name, target in tipo_targets.items():
        _take_best(tipo_buckets.get(bucket_name, []), selected, used, len(selected) + target, per_doc, per_year, per_tipo, max_per_doc=2)

    # completar com melhores scores, permitindo mais densidade mas mantendo diversidade
    _take_best(by_all, selected, used, top_k, per_doc, per_year, per_tipo, max_per_doc=3)

    selected.sort(key=lambda h: (-h.score, str(h.data_doc or ""), -(int(h.doc_id) if h.doc_id else 0), h.chunk_text[:48]))
    return selected[:top_k]


def assess_recent_coverage(
    hits: List[RagHit],
    recent_months: int = 12,
    reference_date: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Avalia qualidade da cobertura recente dos chunks selecionados.

    Retorna um dict com:
      - quality:  "alta" | "média" | "baixa"
      - recent_count: nº de chunks nos últimos `recent_months`
      - total_count: total de chunks elegíveis
      - warning: mensagem de alerta ou "" se cobertura suficiente
      - time_buckets: distribuição por bucket temporal

    Usado para calibrar a confiança da LLM e alertar quando a empresa tem pouca
    cobertura documental recente (ex: último relatório em 2022).
    """
    ref = reference_date or datetime.utcnow()
    recent_count = 0
    total_count = 0
    buckets: Dict[str, int] = defaultdict(int)

    for h in hits:
        if h.score <= -900.0:
            continue   # inelegível — não conta
        total_count += 1
        bucket = _bucket_for_months(h.data_doc)
        buckets[bucket] += 1
        s = str(h.data_doc or "")[:10]
        if len(s) >= 7:
            try:
                year = int(s[:4])
                month = int(s[5:7])
                months_old = (ref.year - year) * 12 + (ref.month - month)
                if months_old <= recent_months:
                    recent_count += 1
            except Exception:
                pass

    if recent_count >= COVERAGE_HIGH_THRESHOLD:
        quality = "alta"
        warning = ""
    elif recent_count >= COVERAGE_MED_THRESHOLD:
        quality = "média"
        warning = (
            f"Cobertura recente ({recent_months}m) moderada: {recent_count} chunks. "
            "Análise pode ter lacunas de atualidade."
        )
    else:
        quality = "baixa"
        warning = (
            f"Cobertura recente ({recent_months}m) insuficiente: apenas {recent_count} chunk(s). "
            "LLM deve reduzir convicção sobre perspectiva atual da empresa."
        )

    return {
        "quality": quality,
        "recent_count": recent_count,
        "total_count": total_count,
        "warning": warning,
        "time_buckets": dict(buckets),
    }


def get_topk_chunks_inteligente(
    ticker: str,
    top_k: int = 24,
    months_window: int = 36,
    debug: bool = False,
    period_ref: Optional[str] = None,
):
    engine = get_supabase_engine()
    llm_client = llm_factory.get_llm_client()
    topics = None
    top_k = max(14, int(top_k))
    recall_total = max(top_k * 5, 120)
    per_topic_k = max(18, math.ceil(recall_total / 10))

    with engine.connect() as conn:
        rows, _stats = retrieve_multitopic_chunks(
            conn=conn,
            llm_client=llm_client,
            ticker=str(ticker).strip().upper(),
            period_ref=period_ref,
            months_back=int(months_window),
            top_k_total=recall_total,
            per_topic_k=per_topic_k,
            topics=topics,
        )

    hits: List[RagHit] = []
    for row in rows:
        txt = str(row.get("chunk_text") or "").strip()
        if not txt:
            continue
        hit = RagHit(
            doc_id=row.get("doc_id"),
            ticker=str(row.get("ticker") or ticker).strip().upper(),
            chunk_text=txt,
            dist=row.get("dist"),
            data_doc=str(row.get("data_doc") or ""),
            tipo_doc=str(row.get("tipo_doc") or ""),
            strategic_theme=str(row.get("topic") or ""),
        )
        hit.recency_score = compute_recency_score(hit.data_doc)
        hit.score = _score_text_quality(
            hit.chunk_text,
            theme=hit.strategic_theme,
            tipo_doc=hit.tipo_doc,
            dist=hit.dist,
            data_doc=hit.data_doc,   # v7: enables hybrid score + hard exclusion
        )
        hits.append(hit)

    hits.sort(key=lambda h: (-h.score, str(h.data_doc or ""), -(int(h.doc_id) if h.doc_id else 0), h.chunk_text[:48]))
    final_hits = _select_diverse_hits(hits, top_k=top_k)
    return final_hits


def summarize_retrieval_mix(hits: List[Any]) -> Dict[str, Any]:
    years: Dict[str, int] = defaultdict(int)
    tipos: Dict[str, int] = defaultdict(int)
    temas: Dict[str, int] = defaultdict(int)
    buckets: Dict[str, int] = defaultdict(int)
    docs = set()
    for h in hits or []:
        year = _extract_year(getattr(h, "data_doc", "") or "") or "sem_ano"
        years[year] += 1
        tipo = str(getattr(h, "tipo_doc", "") or "sem_tipo").strip() or "sem_tipo"
        tipos[tipo] += 1
        tema = str(getattr(h, "strategic_theme", "") or "geral").strip() or "geral"
        temas[tema] += 1
        buckets[_bucket_for_months(getattr(h, "data_doc", "") or "")] += 1
        doc_id = getattr(h, "doc_id", None)
        if doc_id is not None:
            docs.add(doc_id)
    return {
        "total_hits": len(hits or []),
        "docs": len(docs),
        "years": dict(sorted(years.items(), reverse=True)),
        "time_buckets": dict(sorted(buckets.items())),
        "tipos": dict(sorted(tipos.items(), key=lambda kv: (-kv[1], kv[0]))),
        "temas": dict(sorted(temas.items(), key=lambda kv: (-kv[1], kv[0]))),
    }
