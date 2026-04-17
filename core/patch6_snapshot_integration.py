"""core/patch6_snapshot_integration.py

Camada de integração entre portfolio_snapshot_analysis (patches 1-5)
e o pipeline Patch6 (relatório qualitativo + macro).

Responsabilidades:
  - Carregar snapshot por snapshot_id e indexar por ticker
  - Gerar contexto textual quantitativo por empresa (para o LLM)
  - Gerar resumo quantitativo consolidado do portfólio (para o LLM)
  - Calcular multiplicador de alocação baseado no snapshot

Não faz:
  - Chamadas LLM
  - Renderização Streamlit
  - Acesso a patch6_runs
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────

_FORCE_LABELS: Dict[str, str] = {
    "FORTE":    "Força quantitativa alta",
    "MODERADA": "Força quantitativa moderada",
    "FRACA":    "Força quantitativa fraca",
}

_FACTOR_DISPLAY = [
    ("score_qualidade",    "qualidade"),
    ("score_valuation",    "valuation"),
    ("score_dividendos",   "dividendos"),
    ("score_crescimento",  "crescimento"),
    ("score_consistencia", "consistência"),
]

_PENAL_DISPLAY = [
    ("penal_crowding",  "crowding setorial"),
    ("penal_lideranca", "liderança recorrente"),
    ("penal_plato",     "saturação de platô"),
]


def _safe_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    return []


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        f = float(value)
        return None if f != f else f   # NaN guard
    except Exception:
        return None


def _fmt_score_decomp(row: Dict[str, Any]) -> str:
    parts = []
    for key, label in _FACTOR_DISPLAY:
        val = _safe_float(row.get(key))
        if val is not None:
            parts.append(f"{label}: {val:.1f}")
    return ", ".join(parts) if parts else "—"


def _fmt_penalties(row: Dict[str, Any]) -> str:
    parts = []
    for key, label in _PENAL_DISPLAY:
        val = _safe_float(row.get(key))
        if val is not None and val > 0.01:
            parts.append(f"{label}: -{val:.1f}pt")
    return ", ".join(parts) if parts else "nenhuma penalização relevante"


# ────────────────────────────────────────────────────────────────────────────────
# Data loading
# ────────────────────────────────────────────────────────────────────────────────

def load_snapshot_for_patch6(snapshot_id: str) -> Dict[str, Dict[str, Any]]:
    """Carrega portfolio_snapshot_analysis e indexa por ticker (upper-case).

    Retorna dict vazio se snapshot_id vazio ou DB inacessível.
    Seguro para chamar — exceções são suprimidas com degradação limpa.
    """
    if not snapshot_id:
        return {}
    try:
        from core.portfolio_snapshot_analysis_store import load_snapshot_analysis
        df = load_snapshot_analysis(snapshot_id)
        if df is None or df.empty:
            return {}
        result: Dict[str, Dict[str, Any]] = {}
        for row in df.to_dict(orient="records"):
            tk = str(row.get("ticker") or "").strip().upper()
            if tk:
                result[tk] = row
        return result
    except Exception:
        return {}


# ────────────────────────────────────────────────────────────────────────────────
# Per-company quantitative context text
# ────────────────────────────────────────────────────────────────────────────────

def build_snapshot_quant_context(ticker: str, row: Dict[str, Any]) -> str:
    """Bloco textual institucional com contexto quantitativo de uma empresa.

    Formato esperado: 8-12 linhas, linguagem de research, sem elogios.
    Retorna string vazia se row vazio.
    """
    if not row:
        return ""

    rank_geral   = row.get("rank_geral")
    rank_seg     = row.get("rank_segmento")
    segmento     = row.get("segmento") or row.get("setor") or "—"
    classe       = (row.get("classe_forca") or "").strip().upper()
    score_final  = _safe_float(row.get("score_final"))
    penal_total  = _safe_float(row.get("penal_total"))
    drivers_pos  = _safe_list(row.get("drivers_positivos"))
    drivers_neg  = _safe_list(row.get("drivers_negativos"))
    motivos      = _safe_list(row.get("motivos_selecao"))

    force_label  = _FORCE_LABELS.get(classe, f"Classificação: {classe or '—'}")
    rank_str     = f"rank geral {rank_geral}" if rank_geral else "rank não registrado"
    seg_str      = f"rank {rank_seg} no segmento {segmento}" if rank_seg else f"segmento {segmento}"
    score_str    = f"{score_final:.2f}" if score_final is not None else "—"
    decomp_str   = _fmt_score_decomp(row)
    penal_str    = _fmt_penalties(row)
    pt_str       = f"-{penal_total:.1f}pt" if penal_total else "sem penalização total registrada"
    pos_str      = "; ".join(drivers_pos[:4]) if drivers_pos else "não especificados"
    neg_str      = "; ".join(drivers_neg[:4]) if drivers_neg else "não especificados"
    motivos_str  = "; ".join(motivos[:3])  if motivos      else "não especificados"

    # Fundamentos no snapshot
    fund_parts = []
    for key, label in [
        ("roe",            "ROE"),
        ("roic",           "ROIC"),
        ("dividend_yield", "DY"),
        ("p_vp",           "P/VP"),
        ("margem_liquida", "Mg.Líq"),
    ]:
        val = _safe_float(row.get(key))
        if val is not None:
            unit = "x" if key == "p_vp" else "%"
            fund_parts.append(f"{label} {val:.1f}{unit}")
    fund_str = ", ".join(fund_parts) if fund_parts else "—"

    lines = [
        f"[CONTEXTO QUANTITATIVO — {ticker}]",
        f"Força: {force_label} | {rank_str} | {seg_str} | score final: {score_str}.",
        f"Decomposição do score: {decomp_str}.",
        f"Fundamentos no snapshot: {fund_str}.",
        f"Drivers positivos: {pos_str}.",
        f"Drivers negativos: {neg_str}.",
        f"Penalizações: {penal_str}. Penalização total: {pt_str}.",
        f"Motivos de seleção na carteira: {motivos_str}.",
    ]
    return "\n".join(lines)


# ────────────────────────────────────────────────────────────────────────────────
# Portfolio-level quantitative summary
# ────────────────────────────────────────────────────────────────────────────────

def build_portfolio_snapshot_quant_summary(snapshot_map: Dict[str, Dict[str, Any]]) -> str:
    """Resumo quantitativo consolidado do portfólio para o LLM.

    Responde:
    - perfil de força (FORTE/MODERADA/FRACA)
    - fator dominante (valuation / qualidade / dividendos / crescimento / consistência)
    - concentração por segmento
    - penalizações agregadas e destaques
    - fragilidades quantitativas agregadas

    Retorna string vazia se sem dados.
    """
    rows = list(snapshot_map.values())
    if not rows:
        return ""

    n = len(rows)

    # Force class distribution
    classes: Dict[str, int] = {}
    for r in rows:
        c = (r.get("classe_forca") or "?").strip().upper()
        classes[c] = classes.get(c, 0) + 1
    class_str = ", ".join(
        f"{k}: {v}/{n}" for k, v in sorted(classes.items())
    )

    # Segment concentration
    segs: Dict[str, int] = {}
    for r in rows:
        s = (r.get("segmento") or r.get("setor") or "Indefinido").strip()
        segs[s] = segs.get(s, 0) + 1
    seg_str = ", ".join(
        f"{k}: {v}" for k, v in sorted(segs.items(), key=lambda x: -x[1])[:5]
    )

    # Factor averages
    def avg(key: str) -> Optional[float]:
        vals = [_safe_float(r.get(key)) for r in rows]
        vals = [v for v in vals if v is not None]
        return round(sum(vals) / len(vals), 2) if vals else None

    factor_scores: Dict[str, float] = {}
    factor_parts: List[str] = []
    for key, label in _FACTOR_DISPLAY:
        val = avg(key)
        if val is not None:
            factor_scores[label] = val
            factor_parts.append(f"{label}: {val:.1f}")
    factor_str  = ", ".join(factor_parts) if factor_parts else "—"
    dominant    = max(factor_scores, key=factor_scores.get) if factor_scores else "—"

    # Penalty aggregates
    pt_avg = avg("penal_total")
    pc_avg = avg("penal_crowding")
    pl_avg = avg("penal_lideranca")

    penalized = sorted(
        [(str(r.get("ticker", "?")), _safe_float(r.get("penal_total")) or 0.0)
         for r in rows if (_safe_float(r.get("penal_total")) or 0.0) > 2.0],
        key=lambda x: -x[1],
    )
    penal_highlight = (
        ", ".join(f"{t} (-{v:.1f}pt)" for t, v in penalized[:4])
        or "nenhuma penalização relevante"
    )

    # Top-ranked tickers
    ranked = sorted(
        rows,
        key=lambda r: (r.get("rank_geral") or 9999, r.get("ticker") or ""),
    )
    top3 = [str(r.get("ticker", "?")) for r in ranked[:3]]

    # Quant fragility flags
    fragile_flags: List[str] = []
    n_fraca = classes.get("FRACA", 0)
    if n_fraca > 0:
        fragile_flags.append(f"{n_fraca} ativo(s) classificados como FRACA")
    if pt_avg and pt_avg > 5.0:
        fragile_flags.append(f"penalização média alta ({pt_avg:.1f}pt)")
    if pc_avg and pc_avg > 3.0:
        fragile_flags.append(f"crowding setorial relevante ({pc_avg:.1f}pt médio)")

    frag_str = "; ".join(fragile_flags) or "nenhuma fragilidade quantitativa agregada crítica"

    sc_final = avg("score_final")

    lines = [
        f"[RESUMO QUANTITATIVO DO PORTFÓLIO — {n} ativos]",
        f"Distribuição de força: {class_str}.",
        f"Score médio final: {sc_final:.2f}." if sc_final is not None else "",
        f"Fatores (médias por componente): {factor_str}.",
        (
            f"Fator dominante: {dominant} "
            f"— a carteira está estruturalmente apoiada neste vetor quantitativo."
        ),
        f"Concentração por segmento: {seg_str}.",
        (
            f"Penalização média total: {pt_avg:.2f}pt | "
            f"por crowding: {pc_avg:.2f}pt | por liderança: {pl_avg:.2f}pt."
        ) if pt_avg is not None else "",
        f"Ativos com maior penalização: {penal_highlight}.",
        f"Melhores rankeados no snapshot: {', '.join(top3)}.",
        f"Fragilidades quantitativas agregadas: {frag_str}.",
    ]
    return "\n".join(l for l in lines if l)


# ────────────────────────────────────────────────────────────────────────────────
# Allocation multiplier
# ────────────────────────────────────────────────────────────────────────────────

def compute_quant_allocation_multiplier(row: Optional[Dict[str, Any]]) -> float:
    """Multiplicador de alocação derivado do snapshot quantitativo.

    Range: 0.70 (fraco + penalizado) até 1.30 (forte + limpo).
    Retorna 1.0 se row=None (sem dados → sem impacto).

    Critérios:
      - classe_forca:  FORTE +0.15, FRACA -0.15
      - penal_total:   cada ponto reduz 0.012, limitado a -0.18
      - score_final:   >70 → +0.05; <40 → -0.05
    """
    if not row:
        return 1.0
    try:
        classe      = (row.get("classe_forca") or "").strip().upper()
        penal_total = _safe_float(row.get("penal_total")) or 0.0
        score_final = _safe_float(row.get("score_final")) or 0.0

        base       = {"FORTE": 1.15, "MODERADA": 1.00, "FRACA": 0.85}.get(classe, 1.00)
        penal_adj  = max(-0.18, -penal_total * 0.012)
        score_adj  = 0.05 if score_final > 70 else (-0.05 if score_final < 40 else 0.0)

        return max(0.70, min(1.30, base + penal_adj + score_adj))
    except Exception:
        return 1.0


# ────────────────────────────────────────────────────────────────────────────────
# Convergence assessment (used in LLM context)
# ────────────────────────────────────────────────────────────────────────────────

def assess_quant_quali_convergence(
    ticker: str,
    snapshot_row: Dict[str, Any],
    perspectiva: str,
    execution_trend: str,
    narrative_shift: str,
    forward_direction: str,
) -> str:
    """Avalia convergência/conflito entre camada quantitativa e qualitativa.

    Retorna string de diagnóstico para o LLM usar como contexto estruturado.
    """
    if not snapshot_row:
        return f"{ticker}: dados quantitativos do snapshot não disponíveis."

    classe     = (snapshot_row.get("classe_forca") or "").strip().upper()
    penal_tot  = _safe_float(snapshot_row.get("penal_total")) or 0.0
    persp      = (perspectiva or "").strip().lower()
    exec_t     = (execution_trend or "").strip().lower()
    narr_shift = (narrative_shift or "").strip().lower()
    fwd        = (forward_direction or "").strip().lower()

    signals: List[str] = []

    # Convergence signals
    if classe == "FORTE" and persp == "forte":
        signals.append("convergência plena: força quantitativa e perspectiva qualitativa alinhadas positivamente")
    elif classe == "FRACA" and persp in ("fraca",):
        signals.append("convergência negativa: fraqueza quantitativa confirmada pelo qualitativo")

    # Conflict signals
    if classe == "FORTE" and persp in ("fraca",):
        signals.append("CONFLITO: ativo forte no snapshot, mas perspectiva qualitativa fraca — deterioração recente não refletida no snapshot")
    elif classe == "FORTE" and exec_t == "deteriorando":
        signals.append("ALERTA: força quantitativa com deterioração de execução recente — convicção reduzida")
    elif classe == "FRACA" and persp == "forte":
        signals.append("CONFLITO: fraqueza quantitativa, mas perspectiva qualitativa positiva — analisar se melhora é estrutural ou pontual")

    # Narrative shift signals
    if narr_shift == "significativo":
        signals.append("mudança narrativa significativa detectada — baseline quantitativo pode estar desatualizado")

    # Forward direction alignment
    if fwd == "melhorando" and classe in ("FORTE", "MODERADA"):
        signals.append("sinal prospectivo positivo alinhado com base quantitativa")
    elif fwd == "deteriorando" and classe == "FORTE":
        signals.append("sinal prospectivo negativo em ativo quantitativamente forte — ponto de atenção")

    # Penalty signal
    if penal_tot > 8.0:
        signals.append(f"penalização elevada (-{penal_tot:.1f}pt) reduz atratividade marginal apesar da classificação formal")

    if not signals:
        signals.append("sem sinais claros de convergência ou conflito entre camadas")

    return f"{ticker} — " + "; ".join(signals) + "."
