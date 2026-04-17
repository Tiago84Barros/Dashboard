"""core/patch6_report.py

Renderização Streamlit do Patch6 (relatório estilo casa de análise).

Responsabilidades DESTA camada:
  - Cards, badges, tabelas, markdown, CSS
  - Orquestrar patch6_analysis → patch6_service → render

NÃO faz:
  - Acesso ao banco de dados
  - Cálculos de score / alocação
  - Chamadas LLM (delegado a patch6_service)
"""

from __future__ import annotations

import html
from typing import Any, Dict, List, Optional

import streamlit as st

from core.patch6_analysis import (
    build_portfolio_analysis,
    pick_text,
    safe_float,
    safe_int,
    strip_html,
)
from core.patch6_schema import (
    AllocationRow,
    CompanyAnalysis,
    PortfolioAnalysis,
    PortfolioStats,
)
from core.patch6_service import run_portfolio_llm_report, safe_call_llm

try:
    from core.helpers import get_logo_url
except Exception:
    def get_logo_url(ticker: str) -> str:
        return ""


# ────────────────────────────────────────────────────────────────────────────────
# Formatting helpers
# ────────────────────────────────────────────────────────────────────────────────

def _esc(value: Any) -> str:
    return html.escape(strip_html(value))


def _fmt_confidence(value: float) -> str:
    if value <= 0:
        return "—"
    pct = round(max(0.0, min(1.0, value)) * 100)
    return f"{pct}%"


def _fmt_score(value: int) -> str:
    if value <= 0:
        return "—"
    return f"{max(0, min(100, value))}/100"


def _badge(texto: str, tone: str = "neutral") -> str:
    """Badge com semântica de cor padronizada:
      good    → verde  (#22c55e) — favorável / aumentar / forte
      warn    → âmbar  (#f59e0b) — neutro / manter / observar
      bad     → vermelho (#ef4444) — reduzir / revisar / risco
      neutral → cinza  (#64748b) — informativo
    """
    tone_map = {
        "good":    "#22c55e",
        "warn":    "#f59e0b",
        "bad":     "#ef4444",
        "neutral": "#64748b",
    }
    color = tone_map.get(tone, "#64748b")
    texto_safe = _esc(texto)
    return (
        f"<span style='display:inline-block;padding:2px 10px;border-radius:999px;"
        f"border:1px solid {color};color:{color};font-weight:600;font-size:12px'>{texto_safe}</span>"
    )


def _tone_from_perspectiva(p: str) -> str:
    p = (p or "").strip().lower()
    if p == "forte":
        return "good"
    if p == "moderada":
        return "warn"
    if p == "fraca":
        return "bad"
    return "neutral"


def _tone_from_decision(label: str) -> str:
    """Ton semântico para decision_label."""
    return {
        "aumentar": "good",
        "manter":   "neutral",
        "revisar":  "warn",
        "reduzir":  "bad",
    }.get((label or "").strip().lower(), "neutral")


def _tone_from_trend(value: str) -> str:
    """Ton semântico para campos de tendência (portfolio_trend)."""
    v = (value or "").strip().lower()
    if v in ("favorável", "melhorando", "alta"):
        return "good"
    if v in ("estável", "neutro", "moderada"):
        return "neutral"
    if v in ("atenção", "cauteloso"):
        return "warn"
    if v in ("deteriorando", "baixa"):
        return "bad"
    return "neutral"


def _box_html(text: str) -> str:
    return (
        "<div style=\"border:1px solid rgba(255,255,255,0.08);"
        "background:rgba(255,255,255,0.03);border-radius:14px;"
        "padding:14px 16px;box-shadow:0 10px 24px rgba(0,0,0,0.18);"
        "margin-top:8px;line-height:1.5;\">"
        + _esc(text).replace("\n", "<br/>")
        + "</div>"
    )

_SPECIAL_PORTFOLIO_TITLES = {
    "Base analítica": "neutral",
    "Diagnóstico executivo": "good",
    "Identidade da carteira": "neutral",
    "Cenário macro atual": "warn",
    "Leitura macro": "neutral",
    "Riscos internacionais relevantes": "bad",
    "Dependências de cenário macro": "warn",
    "Vulnerabilidades da carteira sob o regime atual": "bad",
    "O que a carteira está apostando implicitamente": "good",
    "Análise de concentração econômica": "warn",
    "Racional de ajuste de alocação": "good",
    "Forças principais": "good",
    "Fragilidades principais": "bad",
    "Riscos invisíveis": "bad",
    "Papel estratégico dos ativos": "neutral",
    "Alocação sugerida (visão estratégica)": "good",
    "Desalinhamentos": "warn",
    "Plano de ação": "good",
    "Insight final": "neutral",
}

def _render_spotlight_section(title: str, body_html: str, tone: str = "neutral") -> None:
    tone_map = {
        "good": ("rgba(34,197,94,.16)", "rgba(34,197,94,.34)", "#86efac"),
        "warn": ("rgba(245,158,11,.16)", "rgba(245,158,11,.34)", "#fcd34d"),
        "bad": ("rgba(239,68,68,.16)", "rgba(239,68,68,.34)", "#fca5a5"),
        "neutral": ("rgba(59,130,246,.15)", "rgba(59,130,246,.30)", "#93c5fd"),
    }
    bg, border, title_color = tone_map.get(tone, tone_map["neutral"])
    st.markdown(
        f"""
        <div style="margin:14px 0 10px 0; border:1px solid {border}; border-radius:18px;
                    background:linear-gradient(180deg,{bg}, rgba(255,255,255,.03));
                    box-shadow:0 10px 24px rgba(0,0,0,.18); overflow:hidden;">
          <div style="padding:12px 16px 10px 16px; border-bottom:1px solid rgba(255,255,255,.08);">
            <div style="font-size:20px; font-weight:900; color:{title_color}; letter-spacing:.2px;">{_esc(title)}</div>
          </div>
          <div style="padding:14px 16px 16px 16px;">{body_html}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _logo_html(ticker: str, size: int = 34) -> str:
    try:
        url = get_logo_url(ticker)
    except Exception:
        url = ""
    if not url:
        return (
            f"<div style='width:{size}px;height:{size}px;border-radius:10px;"
            "display:flex;align-items:center;justify-content:center;"
            "background:rgba(255,255,255,.08);font-size:18px;'>🏢</div>"
        )
    return (
        f"<img src='{html.escape(url)}' alt='{_esc(ticker)}' "
        f"style='width:{size}px;height:{size}px;object-fit:contain;border-radius:10px;background:#fff;padding:3px'/>"
    )


def _company_row_html(ticker: str, subtitle: str = "", value_badge: str = "") -> str:
    left = (
        "<div style='display:flex;align-items:center;gap:12px;'>"
        + _logo_html(ticker, 38)
        + "<div>"
        + f"<div style='font-size:18px;font-weight:900;letter-spacing:.2px'>{_esc(ticker)}</div>"
        + (f"<div style='font-size:13px;opacity:.78;margin-top:2px'>{_esc(subtitle)}</div>" if subtitle else "")
        + "</div></div>"
    )
    right = (
        f"<div style='font-size:13px;font-weight:800;padding:6px 10px;border-radius:999px;"
        "border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.05)'>"
        f"{_esc(value_badge)}</div>" if value_badge else ""
    )
    return f"<div style='display:flex;align-items:center;justify-content:space-between;gap:12px'>{left}{right}</div>"

def _render_text_spotlight(title: str, text_value: str) -> None:
    tone = _SPECIAL_PORTFOLIO_TITLES.get(title, "neutral")
    body = f"<div style='font-size:15px;line-height:1.65;opacity:.96'>{_esc(text_value).replace(chr(10), '<br/>')}</div>"
    _render_spotlight_section(title, body, tone)

def _render_list_spotlight(title: str, values: List[str], limit: Optional[int] = None) -> None:
    clean = [strip_html(v) for v in values if strip_html(v)]
    if limit is not None:
        clean = clean[:limit]
    if not clean:
        return
    tone = _SPECIAL_PORTFOLIO_TITLES.get(title, "neutral")
    items = ''.join([f"<div style='font-size:15px;line-height:1.65;margin:7px 0;'>• {_esc(item)}</div>" for item in clean])
    _render_spotlight_section(title, items, tone)



# ────────────────────────────────────────────────────────────────────────────────
# Score / confidence explanation helpers
# ────────────────────────────────────────────────────────────────────────────────

def _explicar_score(company: CompanyAnalysis) -> str:
    score = company.score_qualitativo
    riscos = len(company.riscos)
    evidencias = len(company.evidencias)
    execucao = strip_html(company.execucao.get("avaliacao_execucao", "")) or "não classificada" \
        if isinstance(company.execucao, dict) else "não classificada"

    if score >= 75:
        faixa = "🟢 Forte"
    elif score >= 55:
        faixa = "🟡 Moderada"
    elif score >= 40:
        faixa = "🟠 Atenção"
    else:
        faixa = "🔴 Fraca"

    return f"{_fmt_score(score)} • {faixa} | Execução: {execucao} | {riscos} riscos | {evidencias} evidências"


def _explicar_confianca(company: CompanyAnalysis) -> str:
    conf = company.confianca
    pct = _fmt_confidence(conf)
    evidencias = len(company.evidencias)
    anos = len(company.strategy_detector.get("coverage_years", [])) \
        if isinstance(company.strategy_detector.get("coverage_years"), list) else 0

    if conf >= 0.75:
        faixa = "🟢 Alta"
    elif conf >= 0.55:
        faixa = "🟡 Média"
    else:
        faixa = "🔴 Baixa"

    return f"{pct} • {faixa} | {evidencias} evidências | {anos} ano(s) analisado(s)"


# ────────────────────────────────────────────────────────────────────────────────
# Rendering primitives
# ────────────────────────────────────────────────────────────────────────────────

def _render_metric_cards(items: List[tuple], columns_per_row: int = 3) -> None:
    clean_items = [(str(label), str(value)) for label, value in items if str(label).strip()]
    if not clean_items:
        return
    for i in range(0, len(clean_items), columns_per_row):
        row_items = clean_items[i:i + columns_per_row]
        cols = st.columns(len(row_items))
        for col, (label, value) in zip(cols, row_items):
            col.markdown(
                f"""
                <div class="p6-card p6-card-metric">
                    <div class="p6-card-label">{_esc(label)}</div>
                    <div class="p6-card-value">{_esc(value)}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

def _render_detail_card(title: str, body_html: str, tone: str = "neutral") -> None:
    tone_map = {
        "good": ("rgba(34,197,94,.08)", "rgba(34,197,94,.22)", "#86efac"),
        "warn": ("rgba(245,158,11,.08)", "rgba(245,158,11,.22)", "#fcd34d"),
        "bad": ("rgba(239,68,68,.08)", "rgba(239,68,68,.22)", "#fca5a5"),
        "neutral": ("rgba(255,255,255,.03)", "rgba(255,255,255,.08)", "#f8fafc"),
    }
    bg, border, title_color = tone_map.get(tone, tone_map["neutral"])
    st.markdown(
        f"""
        <div class="p6-detail-card" style="background:linear-gradient(180deg, {bg}, rgba(255,255,255,.02));border-color:{border};">
            <div class="p6-detail-card-title" style="color:{title_color};">{_esc(title)}</div>
            <div class="p6-detail-card-body">{body_html}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_detail_text_card(title: str, text_value: str, tone: str = "neutral") -> None:
    clean = strip_html(text_value)
    if not clean:
        return
    body = f"<div style='font-size:15px;line-height:1.65'>{_esc(clean).replace(chr(10), '<br/>')}</div>"
    _render_detail_card(title, body, tone)


def _render_detail_list_card(title: str, values: List[str], limit: Optional[int] = None, tone: str = "neutral") -> None:
    clean = [strip_html(v) for v in values if strip_html(v)]
    if limit is not None:
        clean = clean[:limit]
    if not clean:
        return
    body = ''.join([f"<div class='p6-detail-bullet'>• {_esc(item)}</div>" for item in clean])
    _render_detail_card(title, body, tone)


def _render_detail_key_value_card(title: str, data: Dict[str, Any], label_map: List[tuple], tone: str = "neutral") -> None:
    if not data:
        return
    blocks: List[str] = []
    for key, label in label_map:
        value = data.get(key)
        if isinstance(value, str) and strip_html(value):
            blocks.append(
                f"<div class='p6-kv-row'><div class='p6-kv-label'>{_esc(label)}</div><div class='p6-kv-value'>{_esc(strip_html(value))}</div></div>"
            )
        elif isinstance(value, list):
            clean = [strip_html(v) for v in value if strip_html(v)]
            if clean:
                items = ''.join([f"<div class='p6-detail-bullet'>• {_esc(v)}</div>" for v in clean])
                blocks.append(
                    f"<div class='p6-kv-row'><div class='p6-kv-label'>{_esc(label)}</div><div class='p6-kv-value'>{items}</div></div>"
                )
    if not blocks:
        return
    _render_detail_card(title, ''.join(blocks), tone)


def _render_detail_metric_strip(items: List[tuple]) -> None:
    clean = [(str(a), str(b)) for a, b in items if str(a).strip() and str(b).strip()]
    if not clean:
        return
    cols = st.columns(len(clean))
    for col, (label, value) in zip(cols, clean):
        col.markdown(
            f"""
            <div class="p6-mini-card">
                <div class="p6-mini-label">{_esc(label)}</div>
                <div class="p6-mini-value">{_esc(value)}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _render_section_text(title: str, text_value: str) -> None:
    if not strip_html(text_value):
        return
    if title in _SPECIAL_PORTFOLIO_TITLES:
        _render_text_spotlight(title, text_value)
        return
    st.markdown(f"**{title}**")
    st.markdown(_box_html(text_value), unsafe_allow_html=True)


def _render_section_list(title: str, values: List[str], limit: Optional[int] = None) -> None:
    clean = [strip_html(v) for v in values if strip_html(v)]
    if limit is not None:
        clean = clean[:limit]
    if not clean:
        return
    if title in _SPECIAL_PORTFOLIO_TITLES:
        _render_list_spotlight(title, clean, limit=None)
        return
    st.markdown(f"**{title}**")
    for item in clean:
        st.markdown(
            f"<div style='font-size:15px;line-height:1.6;margin:4px 0;'>• {_esc(item)}</div>",
            unsafe_allow_html=True,
        )


def _render_key_value_section(title: str, data: Dict[str, Any], label_map: List[tuple]) -> None:
    if not data:
        return
    blocks: List[str] = []
    for key, label in label_map:
        value = data.get(key)
        if isinstance(value, str) and strip_html(value):
            blocks.append(f"**{label}:** {_esc(value)}")
        elif isinstance(value, list):
            clean = [strip_html(v) for v in value if strip_html(v)]
            if clean:
                blocks.append(f"**{label}:** " + " • ".join(_esc(v) for v in clean))
    if not blocks:
        return
    st.markdown(f"**{title}**")
    st.markdown(
        "<div style='border:1px solid rgba(255,255,255,0.08);background:rgba(255,255,255,0.03);"
        "border-radius:14px;padding:14px 16px;box-shadow:0 10px 24px rgba(0,0,0,0.18);"
        "margin-top:8px;line-height:1.5;'>"
        + "<br/><br/>".join(blocks)
        + "</div>",
        unsafe_allow_html=True,
    )


def _render_evidence_section(evidences: List[Any], limit: int = 6) -> None:
    normalized: List[Dict[str, str]] = []
    for item in evidences[:limit]:
        if isinstance(item, dict):
            normalized.append(
                {
                    "topico": strip_html(item.get("topico") or item.get("ano") or ""),
                    "trecho": strip_html(item.get("trecho") or item.get("citacao") or ""),
                    "interpretacao": strip_html(item.get("interpretacao") or item.get("leitura") or ""),
                }
            )
        elif isinstance(item, str) and item.strip():
            normalized.append({"topico": "", "trecho": strip_html(item), "interpretacao": ""})

    normalized = [n for n in normalized if n["trecho"] or n["interpretacao"]]
    if not normalized:
        return

    st.markdown("**Evidências**")
    for item in normalized:
        head = item["topico"] or "Evidência"
        body_parts = []
        if item["trecho"]:
            body_parts.append(f"**Trecho:** {_esc(item['trecho'])}")
        if item["interpretacao"]:
            body_parts.append(f"**Leitura:** {_esc(item['interpretacao'])}")
        st.markdown(
            f"""
            <div style="border:1px solid rgba(255,255,255,0.08);background:rgba(255,255,255,0.025);
                        border-radius:12px;padding:12px 14px;margin:8px 0;line-height:1.45;">
                <div style="font-size:12px;opacity:0.7;margin-bottom:6px;">{_esc(head)}</div>
                {'<br/>'.join(body_parts)}
            </div>
            """,
            unsafe_allow_html=True,
        )


def _render_strategy_detector(detector: Dict[str, Any]) -> None:
    if not detector:
        return

    summary = strip_html(detector.get("summary"))
    years = detector.get("coverage_years") if isinstance(detector.get("coverage_years"), list) else []
    changes = detector.get("detected_changes") if isinstance(detector.get("detected_changes"), list) else []
    timeline = detector.get("yearly_timeline") if isinstance(detector.get("yearly_timeline"), list) else []
    n_events = safe_int(detector.get("n_events"), 0)

    if not (summary or years or changes or timeline or n_events):
        return

    st.markdown("**Detector de Mudança Estratégica**")
    _render_metric_cards(
        [
            ("Cobertura temporal", ", ".join([str(y) for y in years]) if years else "—"),
            ("Eventos detectados", str(n_events) if n_events > 0 else "—"),
        ],
        columns_per_row=2,
    )
    if summary:
        st.markdown(_box_html(summary), unsafe_allow_html=True)
    if changes:
        _render_section_list("Mudanças detectadas", [strip_html(v) for v in changes], limit=10)
    if timeline:
        st.markdown("**Linha do Tempo Estratégica**")
        for item in timeline[:6]:
            if not isinstance(item, dict):
                continue
            year = strip_html(item.get("year") or "—")
            summary_line = strip_html(item.get("summary") or "")
            evidences = item.get("evidences") if isinstance(item.get("evidences"), list) else []
            extra = ""
            if evidences:
                extra = (
                    "<br/><span style='opacity:.75;font-size:12px;'>"
                    + _esc(" | ".join([strip_html(x) for x in evidences[:2] if strip_html(x)]))
                    + "</span>"
                )
            st.markdown(
                f"""
                <div style="border:1px solid rgba(255,255,255,0.08);background:rgba(255,255,255,0.025);
                            border-radius:12px;padding:12px 14px;margin:8px 0;line-height:1.45;">
                    <div style="font-size:13px;opacity:0.80;margin-bottom:6px;font-weight:700;
                                letter-spacing:.2px;">{_esc(year)}</div>
                    <div style="font-size:16px;line-height:1.55;font-weight:700;">
                        {_esc(summary_line or 'Sem resumo temporal consolidado.')}
                    </div>
                    {extra}
                </div>
                """,
                unsafe_allow_html=True,
            )




def _fmt_macro_value(value: Any, suffix: str = "", decimals: int = 2) -> str:
    try:
        if value is None or value == "":
            return "—"
        num = float(value)
        return f"{num:.{decimals}f}{suffix}"
    except Exception:
        return "—"


def _extract_macro_panel_data(macro: Dict[str, Any]) -> List[tuple]:
    summary = macro.get("macro_summary", {}) if isinstance(macro, dict) else {}
    anual = macro.get("anual", {}) if isinstance(macro, dict) else {}
    return [
        ("Selic atual", _fmt_macro_value(summary.get("selic_current"), "%")),
        ("Dólar atual", _fmt_macro_value(summary.get("cambio_current"))),
        ("IPCA 12m", _fmt_macro_value(summary.get("ipca_12m_current"), "%")),
        ("IPCA anual", _fmt_macro_value(anual.get("ipca"), "%")),
    ]


def _render_macro_panel() -> None:
    macro = st.session_state.get("macro_context_run") or st.session_state.get("macro_context") or {}
    if not isinstance(macro, dict) or not macro:
        return

    cards = _extract_macro_panel_data(macro)
    if not any(value != "—" for _, value in cards):
        return

    st.markdown("## 🌎 Cenário Macro")
    _render_metric_cards(cards, columns_per_row=4)

    interpretation = macro.get("macro_interpretation") if isinstance(macro.get("macro_interpretation"), list) else []
    clean_interp = [strip_html(x) for x in interpretation if strip_html(x)]
    if clean_interp:
        _render_section_list("Leitura macro", clean_interp, limit=6)

def _render_score_explanations(company: CompanyAnalysis) -> None:
    score = company.score_qualitativo
    conf = company.confianca
    execucao = strip_html(company.execucao.get("avaliacao_execucao", "")) or "não classificada" \
        if isinstance(company.execucao, dict) else "não classificada"
    riscos = company.riscos
    evidencias = company.evidencias
    anos = company.strategy_detector.get("coverage_years", []) \
        if isinstance(company.strategy_detector.get("coverage_years"), list) else []

    if score >= 75:
        score_txt = (
            f"Qualidade alta: a leitura qualitativa está mais favorável. A execução foi classificada como '{execucao}', "
            f"com {len(evidencias)} evidência(s) documentais e {len(riscos)} risco(s) explícito(s) no recorte."
        )
    elif score >= 55:
        score_txt = (
            f"Qualidade moderada: há sinais positivos, mas com pontos de atenção. A execução foi classificada como '{execucao}', "
            f"com {len(riscos)} risco(s) e {len(evidencias)} evidência(s) sustentando a análise."
        )
    elif score >= 40:
        score_txt = (
            f"Qualidade de atenção: a tese ainda mostra fragilidades. A execução aparece como '{execucao}', "
            f"com {len(riscos)} risco(s) relevantes frente a {len(evidencias)} evidência(s) disponíveis."
        )
    else:
        score_txt = (
            f"Qualidade fraca: a leitura qualitativa está pressionada por riscos e/ou baixa consistência. "
            f"A execução foi classificada como '{execucao}'."
        )

    if conf >= 0.75:
        conf_txt = (
            f"Confiança alta: a leitura se apoia em base documental mais robusta, com {len(evidencias)} evidência(s) "
            f"e cobertura temporal de {len(anos)} ano(s)."
        )
    elif conf >= 0.55:
        conf_txt = (
            f"Confiança média: há base documental útil, mas ainda incompleta. O resultado usa {len(evidencias)} evidência(s) "
            f"e cobertura temporal de {len(anos)} ano(s)."
        )
    else:
        conf_txt = (
            f"Confiança baixa: a leitura depende de base documental mais limitada. Neste caso, há {len(evidencias)} evidência(s) "
            f"e cobertura temporal de {len(anos)} ano(s), o que recomenda cautela adicional."
        )

    st.caption("Como interpretar os scores")
    st.markdown(_box_html(score_txt + "\n\n" + conf_txt), unsafe_allow_html=True)
    if company.score_source == "heuristic":
        st.markdown(
            _badge("Score heurístico — LLM não retornou valor; score estimado por estrutura do JSON", "warn"),
            unsafe_allow_html=True,
        )


# ────────────────────────────────────────────────────────────────────────────────
# Portfolio-level sections
# ────────────────────────────────────────────────────────────────────────────────

def _render_allocation_section(allocation_rows: List[AllocationRow]) -> None:
    st.markdown("## 💼 Alocação Sugerida")
    cols_per_row = 4
    for i in range(0, len(allocation_rows), cols_per_row):
        row_items = allocation_rows[i:i + cols_per_row]
        cols = st.columns(len(row_items))
        for col, item in zip(cols, row_items):
            tone = _tone_from_perspectiva(item.perspectiva or "")
            tone_color = {
                "good": "#22c55e",
                "warn": "#f59e0b",
                "bad": "#ef4444",
                "neutral": "#94a3b8",
            }.get(tone, "#94a3b8")
            col.markdown(
                f"""
                <div class="p6-card p6-card-allocation">
                  <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:12px;">
                    <div style="display:flex;align-items:center;gap:10px;">
                      {_logo_html(item.ticker, 34)}
                      <div style="font-size:18px;font-weight:900;letter-spacing:.2px;">{_esc(item.ticker)}</div>
                    </div>
                    <div>{_badge((item.perspectiva or '—').upper(), tone)}</div>
                  </div>
                  <div class="p6-card-value" style="font-size:28px;color:{tone_color};">{item.allocation_pct:.2f}%</div>
                  <div style="display:flex;flex-wrap:wrap;gap:6px;margin-top:10px;">
                    {_badge(f'Score {_fmt_score(item.score)}', 'neutral')}
                    {_badge(f'Confiança {_fmt_confidence(item.confianca)}', 'neutral')}
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def _render_structured_portfolio_report(report: Dict[str, Any], mode_label: str) -> None:
    with st.expander("🧠 Relatório Estratégico do Portfólio", expanded=False):
        st.caption(f"Modo utilizado: {mode_label}")
        _render_section_text("Base analítica", report.get("analytical_basis", ""))
        _render_section_text("Diagnóstico executivo", report.get("executive_summary", ""))

        # v6 — perfil quantitativo do portfólio (LLM)
        _render_section_text("Perfil quantitativo do portfólio", report.get("quantitative_profile", ""))

        # v6 — convergências e conflitos quanti/quali/macro (LLM)
        _render_section_list(
            "✅ Convergências (quanti + quali + macro)",
            report.get("quanti_quali_macro_convergences", []),
            limit=8,
        )
        _render_section_list(
            "⚡ Conflitos detectados (quanti ↔ quali ↔ macro)",
            report.get("quanti_quali_macro_conflicts", []),
            limit=8,
        )

        _render_section_text("Identidade da carteira", report.get("portfolio_identity", ""))
        _render_section_text("Cenário macro atual", report.get("current_market_context", ""))
        _render_section_text("Leitura macro", report.get("macro_reading", ""))
        _render_section_list("Desalinhamentos", report.get("misalignments", []), limit=8)
        _render_section_list("Plano de ação", report.get("action_plan", []), limit=10)
        _render_section_text("Insight final", report.get("final_insight", ""))

    asset_roles = report.get("asset_roles", []) or []
    if asset_roles:
        with st.expander("🧩 Papel estratégico dos ativos", expanded=False):
            for item in asset_roles[:12]:
                if not isinstance(item, dict):
                    continue
                ticker = strip_html(item.get("ticker") or "—")
                role = strip_html(item.get("role") or "")
                rationale = strip_html(item.get("rationale") or "")
                st.markdown(
                    f"""
                    <div class="p6-card" style="margin:8px 0;min-height:auto;">
                        {_company_row_html(ticker, role)}
                        <div style="font-size:15px;line-height:1.6;margin-top:10px;">{_esc(rationale or '—')}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    suggested_allocations = report.get("suggested_allocations", []) or []
    if suggested_allocations:
        with st.expander("🎯 Alocação sugerida — síntese quanti + quali + macro", expanded=False):
            st.caption(
                "A alocação reflete a síntese integrada de três camadas: "
                "base quantitativa (patches 1–5), leitura qualitativa documental e contexto macro."
            )
            for item in suggested_allocations[:15]:
                if not isinstance(item, dict):
                    continue
                ticker        = strip_html(item.get("ticker") or "—")
                suggested_range = strip_html(item.get("suggested_range") or "")
                rationale     = strip_html(item.get("rationale") or "")
                quant_basis   = strip_html(item.get("quant_basis") or "")
                quali_basis   = strip_html(item.get("quali_basis") or "")
                macro_basis   = strip_html(item.get("macro_basis") or "")

                basis_parts = []
                if quant_basis:
                    basis_parts.append(f"<span style='color:#60a5fa'>📐 Quant:</span> {_esc(quant_basis)}")
                if quali_basis:
                    basis_parts.append(f"<span style='color:#a3e635'>📄 Quali:</span> {_esc(quali_basis)}")
                if macro_basis:
                    basis_parts.append(f"<span style='color:#fb923c'>🌎 Macro:</span> {_esc(macro_basis)}")
                basis_html = "<br/>".join(basis_parts) if basis_parts else ""

                st.markdown(
                    f"""
                    <div class="p6-card" style="margin:8px 0;min-height:auto;">
                        {_company_row_html(ticker, "", suggested_range or '—')}
                        {f'<div style="font-size:12px;line-height:1.7;margin:8px 0 4px;opacity:.85">{basis_html}</div>'
                         if basis_html else ''}
                        <div style="font-size:14px;line-height:1.6;margin-top:6px;opacity:.95">{_esc(rationale or '—')}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )


# ────────────────────────────────────────────────────────────────────────────────
# Company detail section
# ────────────────────────────────────────────────────────────────────────────────

def _render_company_expander(company: CompanyAnalysis) -> None:
    tk = (company.ticker or "").strip() or "—"
    p = (company.perspectiva_compra or "").strip().lower()

    sintese_src = (company.tese or company.leitura or "").strip()
    if sintese_src:
        primeiro = sintese_src.split(".")[0].strip()
        sintese = (primeiro + ".") if primeiro else sintese_src[:220]
        if len(sintese) > 220:
            sintese = sintese[:217] + "…"
    else:
        sintese = "—"

    risco_src = (company.riscos[0] if company.riscos else company.fragilidade_regime_atual or "").strip()
    if risco_src and len(risco_src) > 140:
        risco_src = risco_src[:140] + "…"

    action_src = (company.recommended_action or "").strip()
    if action_src and len(action_src) > 80:
        action_src = action_src[:80] + "…"

    attn_badge = ""
    if company.attention_level in ("alta", "média"):
        attn_tone = "bad" if company.attention_level == "alta" else "warn"
        attn_badge = _badge(f"⚡ {company.attention_level.upper()}", attn_tone)

    decision_badge = ""
    dl = (getattr(company, "decision_label", "") or "").strip().lower()
    if dl not in ("—", ""):
        decision_badge = _badge(f"→ {dl.upper()}", _tone_from_decision(dl))

    ec_class = {
        "forte":    "p6-ec p6-ec-forte",
        "moderada": "p6-ec p6-ec-moderada",
        "fraca":    "p6-ec p6-ec-fraca",
    }.get(p, "p6-ec p6-ec-unknown")

    badges_html = " ".join([x for x in [_badge((p or "—").upper(), _tone_from_perspectiva(p)), decision_badge, attn_badge] if x])

    extra_pills = []
    if risco_src:
        extra_pills.append(f'<span class="p6-risk-pill">⚠ {_esc(risco_src)}</span>')
    if action_src:
        extra_pills.append(f'<span class="p6-action-pill">→ {_esc(action_src)}</span>')
    extra_pills_html = "".join(extra_pills)

    card_html = (
        f'<div class="{ec_class}">'
        f'  <div class="p6-ec-head">'
        f'    <div style="display:flex;align-items:center;gap:12px;">{_logo_html(tk, 38)}<div class="p6-ec-ticker">{_esc(tk)}</div></div>'
        f'    <div class="p6-ec-badges">{badges_html}</div>'
        f'  </div>'
        f'  <div class="p6-ec-sintese">{_esc(sintese)}</div>'
        f'  <div style="display:flex;flex-wrap:wrap;gap:6px;align-items:center">{extra_pills_html}</div>'
        f'</div>'
    )
    st.markdown(card_html, unsafe_allow_html=True)

    with st.expander(f"Ver análise completa — {tk}", expanded=False):
        st.caption(
            f"Período analisado: {company.period_ref} • Atualizado em: {company.created_at}"
            + (f" • Confiança: {_fmt_confidence(company.confianca)}" if company.confianca > 0 else "")
            + (f" • Score: {_fmt_score(company.score_qualitativo)}" if company.score_qualitativo > 0 else "")
        )

        _render_detail_metric_strip([
            ("Perspectiva", (p or "—").upper()),
            ("Decisão", (dl or "—").upper()),
            ("Confiança", _fmt_confidence(company.confianca)),
            ("Score", _fmt_score(company.score_qualitativo)),
        ])

        # ── v6 quantitative snapshot section ─────────────────────────────────
        _qc = (company.quant_classe or "").strip().upper()
        _qt = (company.quant_context_text or "").strip()
        _qv = (company.quant_convergence or "").strip()
        if _qt:
            _qc_tone = {"FORTE": "good", "MODERADA": "warn", "FRACA": "bad"}.get(_qc, "neutral")
            _qc_colors = {"good": "#22c55e", "bad": "#ef4444", "warn": "#f59e0b", "neutral": "#64748b"}
            _qc_border = _qc_colors.get(_qc_tone, "#64748b")
            _qrank_str = (
                f"Rank {company.quant_rank_geral}" if company.quant_rank_geral
                else "Rank —"
            )
            _qscore_str = (
                f"Score quant. {company.quant_score_final:.1f}"
                if company.quant_score_final else ""
            )
            _qheader = f"📐 Base Quantitativa (Patches 1–5) — {_qc or '—'}" + (
                f" | {_qrank_str}" if _qrank_str else ""
            ) + (f" | {_qscore_str}" if _qscore_str else "")
            st.markdown(
                f"<div style='border:1px solid {_qc_border}33;border-left:4px solid {_qc_border};"
                f"background:{_qc_border}0a;border-radius:12px;padding:12px 16px;"
                f"margin:6px 0 6px;font-size:13px;line-height:1.55'>"
                f"<strong>{_esc(_qheader)}</strong><br/>"
                f"<pre style='font-family:inherit;font-size:12px;margin:6px 0 0;"
                f"white-space:pre-wrap;opacity:.88'>{_esc(_qt)}</pre>"
                f"</div>",
                unsafe_allow_html=True,
            )
        if _qv:
            _conflict = "CONFLITO" in _qv.upper() or "ALERTA" in _qv.upper()
            _conv_tone_color = "#ef4444" if _conflict else "#64748b"
            st.markdown(
                f"<div style='border:1px solid {_conv_tone_color}22;border-left:3px solid {_conv_tone_color};"
                f"background:{_conv_tone_color}08;border-radius:10px;padding:8px 14px;"
                f"margin:4px 0 8px;font-size:12px;opacity:.90'>"
                f"⚡ <em>{_esc(_qv)}</em></div>",
                unsafe_allow_html=True,
            )

        _render_detail_text_card("1. Tese", company.tese or "—", "neutral")

        if company.leitura:
            _render_detail_text_card("2. Direcionalidade", company.leitura, _tone_from_perspectiva(p))
        elif p == "forte":
            _render_detail_text_card("2. Direcionalidade", "Viés construtivo, com sinais qualitativos favoráveis no recorte analisado.", "good")
        elif p == "moderada":
            _render_detail_text_card("2. Direcionalidade", "Leitura equilibrada, com pontos positivos e ressalvas relevantes.", "warn")
        elif p == "fraca":
            _render_detail_text_card("2. Direcionalidade", "Leitura cautelosa, com sinais qualitativos desfavoráveis no recorte analisado.", "bad")

        _render_detail_key_value_card(
            "3. Qualidade da execução · Consistência do Discurso",
            company.consistencia,
            [("analise", "Análise"), ("grau_consistencia", "Grau"), ("contradicoes", "Contradições"), ("sinais_positivos", "Sinais positivos")],
            "neutral",
        )
        _render_detail_key_value_card(
            "3. Qualidade da execução · Execução vs Promessa",
            company.execucao,
            [("analise", "Análise"), ("avaliacao_execucao", "Avaliação"), ("entregas_confirmadas", "Entregas confirmadas"), ("entregas_pendentes_ou_incertas", "Entregas pendentes ou incertas"), ("entregas_pendentes", "Entregas pendentes")],
            "neutral",
        )
        entregas = company.delivered_promises or []
        if not entregas and isinstance(company.execucao, dict):
            entregas = company.execucao.get("entregas_confirmadas", []) if isinstance(company.execucao.get("entregas_confirmadas"), list) else []
        _render_detail_list_card("3. Qualidade da execução · Entregas confirmadas", entregas, limit=6, tone="good")

        _render_detail_key_value_card(
            "4. Evolução estratégica",
            company.evolucao,
            [("historico", "Histórico"), ("fase_atual", "Fase atual"), ("tendencia", "Tendência")],
            "neutral",
        )
        _render_strategy_detector(company.strategy_detector)
        _render_detail_list_card("4. Evolução estratégica · Mudanças detectadas", company.mudancas, limit=6, tone="warn")

        risk_rank = getattr(company, "risk_rank", None) or []
        if risk_rank:
            _render_detail_list_card("5. Risco", risk_rank, limit=6, tone="bad")
        else:
            _render_detail_list_card("5. Risco", company.riscos, limit=6, tone="bad")

        _render_detail_list_card("6. Drivers · Catalisadores", company.catalisadores, limit=6, tone="good")
        _render_detail_list_card("6. Drivers · O que monitorar", company.monitorar, limit=6, tone="warn")

        normalized_evidences: List[Dict[str, str]] = []
        for item in company.evidencias[:10]:
            if isinstance(item, dict):
                normalized_evidences.append({
                    "topico": strip_html(item.get("topico") or item.get("ano") or ""),
                    "trecho": strip_html(item.get("trecho") or item.get("citacao") or ""),
                    "interpretacao": strip_html(item.get("interpretacao") or item.get("leitura") or ""),
                })
            elif isinstance(item, str) and item.strip():
                normalized_evidences.append({"topico": "", "trecho": strip_html(item), "interpretacao": ""})
        if normalized_evidences:
            st.markdown("<div class='p6-detail-section-label'>7. Evidência</div>", unsafe_allow_html=True)
            for item in normalized_evidences:
                header = item["topico"] or "Evidência"
                body = ""
                if item["trecho"]:
                    body += f"<div class='p6-kv-row'><div class='p6-kv-label'>Trecho</div><div class='p6-kv-value'>{_esc(item['trecho'])}</div></div>"
                if item["interpretacao"]:
                    body += f"<div class='p6-kv-row'><div class='p6-kv-label'>Leitura</div><div class='p6-kv-value'>{_esc(item['interpretacao'])}</div></div>"
                _render_detail_card(header, body, "neutral")

        attn_tone = "bad" if company.attention_level == "alta" else "warn" if company.attention_level == "média" else "neutral"
        if company.attention_score > 0:
            _render_detail_metric_strip([
                ("Prioridade", (company.attention_level or "—").upper()),
                ("Score de acompanhamento", f"{company.attention_score:.0f}/100"),
                ("Ação sugerida", action_src or "—"),
            ])

        if company.forward_score > 0:
            _render_detail_metric_strip([
                ("Sinal prospectivo", _fmt_score(company.forward_score)),
                ("Direção", company.forward_direction or "—"),
                ("Confiança prospectiva", _fmt_confidence(company.forward_confidence)),
            ])
        _render_detail_list_card("8. Camada prospectiva · Fatores prospectivos", company.forward_drivers, limit=6, tone=attn_tone if company.forward_score > 0 else "neutral")

        _render_detail_text_card("9. Conclusão · Papel estratégico", company.papel_estrategico, "neutral")
        _render_detail_text_card("9. Conclusão · Alocação sugerida (faixa)", company.alocacao_sugerida_faixa, "good")
        _render_detail_text_card("9. Conclusão · Racional de alocação", company.racional_alocacao, "neutral")
        _render_detail_text_card("9. Conclusão · Considerações da LLM", company.consideracoes, "neutral")


# ────────────────────────────────────────────────────────────────────────────────
# Entry point
# ────────────────────────────────────────────────────────────────────────────────

_P6_CSS = """
<style>
.p6-card{
  border:1px solid rgba(255,255,255,0.08);
  background:linear-gradient(180deg, rgba(255,255,255,0.05) 0%, rgba(255,255,255,0.03) 100%);
  border-radius:16px;
  padding:16px 18px;
  box-shadow:0 10px 24px rgba(0,0,0,0.22);
  min-height:110px;
}
.p6-card-label{font-size:12px;opacity:0.72;margin-bottom:8px;letter-spacing:0.3px;}
.p6-card-value{font-size:28px;font-weight:900;margin-bottom:6px;line-height:1.1;}
.p6-card-extra{font-size:12px;opacity:0.68;line-height:1.45;}
.p6-card-metric{min-height:92px;}
.p6-card-allocation{min-height:138px;}

.p6-ec{
  border:1px solid rgba(255,255,255,0.08);
  border-radius:16px;
  padding:16px 18px;
  box-shadow:0 8px 20px rgba(0,0,0,0.22);
  margin-bottom:10px;
  border-left-width:4px;
  border-left-style:solid;
}
.p6-ec-forte   { border-left-color:#22c55e; background:linear-gradient(135deg,rgba(34,197,94,.08) 0%,rgba(255,255,255,.03) 100%); }
.p6-ec-moderada{ border-left-color:#f59e0b; background:linear-gradient(135deg,rgba(245,158,11,.08) 0%,rgba(255,255,255,.03) 100%); }
.p6-ec-fraca   { border-left-color:#ef4444; background:linear-gradient(135deg,rgba(239,68,68,.08) 0%,rgba(255,255,255,.03) 100%); }
.p6-ec-unknown { border-left-color:#64748b; background:linear-gradient(135deg,rgba(100,116,139,.08) 0%,rgba(255,255,255,.03) 100%); }

.p6-ec-head{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:10px}
.p6-ec-ticker{font-size:18px;font-weight:900;letter-spacing:.3px}
.p6-ec-badges{display:flex;flex-wrap:wrap;gap:6px;align-items:center;justify-content:flex-end}
.p6-ec-sintese{font-size:13px;opacity:.86;line-height:1.55;margin-bottom:6px}

.p6-risk-pill{
  display:inline-block;
  padding:2px 9px;
  border-radius:999px;
  border:1px solid rgba(239,68,68,.40);
  background:rgba(239,68,68,.08);
  color:#ef4444;
  font-size:11px;
  font-weight:600;
  margin-top:5px;
}

.p6-action-pill{
  display:inline-block;
  padding:2px 9px;
  border-radius:999px;
  border:1px solid rgba(100,116,139,.35);
  background:rgba(100,116,139,.08);
  color:#cbd5e1;
  font-size:11px;
  font-weight:600;
  margin-top:5px;
  margin-left:2px;
}

.p6-detail-card{border:1px solid rgba(255,255,255,0.08);border-radius:14px;padding:14px 16px;box-shadow:0 8px 18px rgba(0,0,0,0.18);margin:10px 0;}
.p6-detail-card-title{font-size:16px;font-weight:800;letter-spacing:.2px;margin-bottom:10px;}
.p6-detail-card-body{font-size:14px;line-height:1.6;}
.p6-detail-bullet{font-size:14px;line-height:1.6;margin:6px 0;}
.p6-kv-row{padding:8px 0;border-top:1px solid rgba(255,255,255,.06);}
.p6-kv-row:first-child{padding-top:0;border-top:none;}
.p6-kv-label{font-size:11px;text-transform:uppercase;letter-spacing:.45px;opacity:.68;margin-bottom:5px;}
.p6-kv-value{font-size:14px;line-height:1.6;}
.p6-mini-card{border:1px solid rgba(255,255,255,0.08);background:rgba(255,255,255,0.03);border-radius:12px;padding:10px 12px;margin:4px 0;min-height:74px;}
.p6-mini-label{font-size:11px;opacity:.68;margin-bottom:5px;letter-spacing:.3px;}
.p6-mini-value{font-size:18px;font-weight:800;line-height:1.2;}
.p6-detail-section-label{font-size:13px;font-weight:800;letter-spacing:.25px;opacity:.82;margin:14px 0 8px 0;}
</style>
"""


def render_patch6_report(
    tickers: List[str],
    period_ref: str,
    llm_factory: Optional[Any] = None,
    show_company_details: bool = True,
    analysis_mode: str = "rigid",
    snapshot_id: str = "",
) -> None:
    st.markdown(_P6_CSS, unsafe_allow_html=True)

    # ── Data + computation (pure, no Streamlit) ──────────────────────────────
    analysis = build_portfolio_analysis(tickers, period_ref)
    if analysis is None or not analysis.companies:
        st.warning(
            "Não há execuções salvas em patch6_runs para este period_ref e tickers do portfólio. "
            "Rode a LLM e salve os resultados primeiro."
        )
        return

    stats = analysis.stats

    # ── v5 — enriquecimento macro (impacto por empresa + narrativa portfólio) ──
    try:
        from core.macro_context import load_latest_macro_context
        from core.patch6_analysis import enrich_macro_impact
        _macro_enrich = (
            st.session_state.get("macro_ctx_page")
            or st.session_state.get("macro_context")
            or load_latest_macro_context()
        )
        if _macro_enrich:
            enrich_macro_impact(analysis, _macro_enrich)
    except Exception:
        pass  # v5 enrichment is optional — never breaks the report

    # ── v6 — enriquecimento quantitativo (snapshot patches 1-5) ──────────────
    if snapshot_id:
        try:
            from core.patch6_snapshot_integration import load_snapshot_for_patch6
            from core.patch6_analysis import enrich_quant_snapshot
            _snap_map = load_snapshot_for_patch6(snapshot_id)
            if _snap_map:
                enrich_quant_snapshot(analysis, _snap_map)
        except Exception:
            pass  # v6 enrichment is optional — never breaks the report

    # ── Cor semântica para os cards de resumo ─────────────────────────────────
    def _summary_color(label: str) -> str:
        """Verde=favorável, âmbar=neutro, vermelho=cauteloso."""
        l = label.strip().lower()
        if l in ("alta", "construtiva", "forte"):
            return "#22c55e"
        if l in ("moderada", "neutra", "média"):
            return "#f59e0b"
        if l in ("baixa", "cautelosa", "fraca"):
            return "#ef4444"
        return "inherit"

    _q_lbl   = stats.label_qualidade()
    _p_lbl   = stats.label_perspectiva()
    _conf_lbl = _fmt_confidence(analysis.confianca_media)
    _sc_lbl   = _fmt_score(analysis.score_medio)

    # cor da confiança por faixa numérica
    _conf_color = (
        "#22c55e" if analysis.confianca_media >= 0.75
        else "#f59e0b" if analysis.confianca_media >= 0.55
        else "#ef4444" if analysis.confianca_media > 0
        else "inherit"
    )
    # cor do score médio por faixa
    _sc_color = (
        "#22c55e" if analysis.score_medio >= 70
        else "#f59e0b" if analysis.score_medio >= 50
        else "#ef4444" if analysis.score_medio > 0
        else "inherit"
    )

    # ── Portfolio summary cards ───────────────────────────────────────────────
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.markdown(
        f'<div class="p6-card"><div class="p6-card-label">Qualidade (heurística)</div>'
        f'<div class="p6-card-value" style="color:{_summary_color(_q_lbl)}">{_q_lbl}</div>'
        f'<div class="p6-card-extra">Leitura agregada dos sinais qualitativos do portfólio.</div></div>',
        unsafe_allow_html=True,
    )
    col2.markdown(
        f'<div class="p6-card"><div class="p6-card-label">Perspectiva 12m</div>'
        f'<div class="p6-card-value" style="color:{_summary_color(_p_lbl)}">{_p_lbl}</div>'
        f'<div class="p6-card-extra">Direcionalidade consolidada para os próximos 12 meses.</div></div>',
        unsafe_allow_html=True,
    )
    col3.markdown(
        f'<div class="p6-card"><div class="p6-card-label">Cobertura</div>'
        f'<div class="p6-card-value">{analysis.cobertura}</div>'
        f'<div class="p6-card-extra">Ativos cobertos com base documental suficiente no período.</div></div>',
        unsafe_allow_html=True,
    )
    col4.markdown(
        f'<div class="p6-card"><div class="p6-card-label">Confiança média</div>'
        f'<div class="p6-card-value" style="color:{_conf_color}">{_conf_lbl}</div>'
        f'<div class="p6-card-extra">Confiança média das leituras individuais.</div></div>',
        unsafe_allow_html=True,
    )
    col5.markdown(
        f'<div class="p6-card"><div class="p6-card-label">Score qualitativo médio</div>'
        f'<div class="p6-card-value" style="color:{_sc_color}">{_sc_lbl}</div>'
        f'<div class="p6-card-extra">Score médio consolidado das análises individuais.</div></div>',
        unsafe_allow_html=True,
    )
    _render_macro_panel()

    # ── LLM portfolio report (optional) ──────────────────────────────────────
    portfolio_report = run_portfolio_llm_report(
        llm_factory, analysis, analysis_mode,
        snapshot_id=snapshot_id,  # v6: pass snapshot_id for quant integration
    )

    if portfolio_report:
        mode_label = "Análise Rígida" if analysis_mode == "rigid" else "Análise Flexível"
        _render_structured_portfolio_report(portfolio_report, mode_label)
    else:
        st.markdown("## 🧠 Resumo Executivo")
        st.write(
            f"O portfólio apresenta leitura **{stats.label_perspectiva().lower()}** para 12 meses, com distribuição: "
            f"**{stats.fortes}** forte, **{stats.moderadas}** moderada e **{stats.fracas}** fraca. "
            f"Cobertura: **{analysis.cobertura}** ativos, confiança média **{_fmt_confidence(analysis.confianca_media)}**, "
            f"score qualitativo médio **{_fmt_score(analysis.score_medio)}**."
        )

    _render_allocation_section(analysis.allocation_rows)

    # ── Per-company detail ────────────────────────────────────────────────────
    if show_company_details:
        st.markdown("## 🏢 Relatórios por Empresa")
        for company in analysis.companies.values():
            _render_company_expander(company)

    # ── Conclusão estratégica ─────────────────────────────────────────────────
    st.markdown("## 🔎 Conclusão Estratégica")
    llm_client = None
    if llm_factory is not None:
        try:
            llm_client = llm_factory.get_llm_client()
        except Exception:
            pass

    prompt_conc = (
        "Escreva uma conclusão estratégica (research) para o portfólio, em até 10 linhas, com foco em:\n"
        "- coerência do conjunto do portfólio\n"
        "- principais alavancas para melhora ou deterioração\n"
        "- recomendação de acompanhamento nos próximos trimestres\n\n"
        f"Use SOMENTE os bullets abaixo.\n\nBULLETS:\n{analysis.contexto_portfolio}"
    )
    llm_conc = safe_call_llm(llm_client, prompt_conc)
    if llm_conc:
        st.write(llm_conc)
    else:
        st.write(
            "A carteira deve ser acompanhada por gatilhos de execução, evolução da narrativa corporativa, score qualitativo, "
            "mudanças estratégicas detectadas e sinais de alocação de capital. Reforce o monitoramento de resultados trimestrais, "
            "consistência entre discurso e entrega, dívida/custo financeiro e manutenção dos catalisadores já visíveis nas evidências do RAG."
        )
