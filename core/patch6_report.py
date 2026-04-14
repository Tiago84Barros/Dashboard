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
}


_DETAIL_SECTION_TONES = {
    "Como interpretar os scores": "neutral",
    "Tese (síntese)": "good",
    "Leitura / Direcionalidade": "neutral",
    "Evolução Estratégica": "neutral",
    "Detector de Mudança Estratégica": "warn",
    "Mudanças detectadas": "warn",
    "Consistência do Discurso": "good",
    "Execução vs Promessa": "good",
    "Mudanças Estratégicas": "warn",
    "Pontos-chave": "neutral",
    "Catalisadores": "good",
    "Riscos (prioritários)": "bad",
    "Riscos": "bad",
    "O que monitorar": "warn",
    "Evidências": "neutral",
    "Considerações da LLM": "neutral",
    "Entregas confirmadas (recorrentes)": "good",
    "Prioridade de Acompanhamento": "warn",
    "Sinal Prospectivo": "neutral",
    "Fatores prospectivos": "neutral",
}

def _tone_for_title(title: str, default: str = "neutral") -> str:
    return _DETAIL_SECTION_TONES.get(title, _SPECIAL_PORTFOLIO_TITLES.get(title, default))

def _render_detail_section(title: str, body_html: str, tone: Optional[str] = None) -> None:
    tone = tone or _tone_for_title(title)
    tone_map = {
        "good": ("rgba(34,197,94,.10)", "rgba(34,197,94,.28)", "#86efac"),
        "warn": ("rgba(245,158,11,.10)", "rgba(245,158,11,.28)", "#fcd34d"),
        "bad": ("rgba(239,68,68,.10)", "rgba(239,68,68,.28)", "#fca5a5"),
        "neutral": ("rgba(59,130,246,.10)", "rgba(59,130,246,.24)", "#93c5fd"),
    }
    bg, border, title_color = tone_map.get(tone, tone_map["neutral"])
    st.markdown(
        f"""
        <div style="margin:14px 0 10px 0; border:1px solid {border}; border-radius:18px;
                    background:linear-gradient(180deg,{bg}, rgba(255,255,255,.025));
                    box-shadow:0 10px 24px rgba(0,0,0,.18); overflow:hidden;">
          <div style="padding:12px 16px 10px 16px; border-bottom:1px solid rgba(255,255,255,.08);">
            <div style="font-size:19px; font-weight:900; color:{title_color}; letter-spacing:.2px;">{_esc(title)}</div>
          </div>
          <div style="padding:14px 16px 16px 16px; line-height:1.6; font-size:15px;">{body_html}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def _render_detail_text(title: str, text_value: str) -> None:
    clean = strip_html(text_value)
    if not clean:
        return
    body = f"<div style='font-size:15px;line-height:1.7'>{_esc(clean).replace(chr(10), '<br/>')}</div>"
    _render_detail_section(title, body)

def _render_detail_list(title: str, values: List[str], limit: Optional[int] = None) -> None:
    clean = [strip_html(v) for v in values if strip_html(v)]
    if limit is not None:
        clean = clean[:limit]
    if not clean:
        return
    body = ''.join([f"<div style='font-size:15px;line-height:1.7;margin:8px 0;'>• {_esc(item)}</div>" for item in clean])
    _render_detail_section(title, body)

def _render_detail_badges(title: str, badges_html: str, extra_html: str = "") -> None:
    body = f"<div style='display:flex;flex-wrap:wrap;gap:8px;align-items:center'>{badges_html}</div>{extra_html}"
    _render_detail_section(title, body)

def _render_detail_kv(title: str, blocks: List[str]) -> None:
    if not blocks:
        return
    body = "<div style='display:flex;flex-direction:column;gap:12px;'>" + "".join(
        [f"<div style='font-size:15px;line-height:1.7'>{b}</div>" for b in blocks]
    ) + "</div>"
    _render_detail_section(title, body)

def _render_logo_inline(ticker: str) -> str:
    try:
        from core.helpers import get_logo_url
        url = get_logo_url(ticker)
    except Exception:
        url = ""
    if not url:
        return ""
    return f"<img src='{html.escape(url)}' style='width:34px;height:34px;object-fit:contain;border-radius:8px;background:#fff;padding:3px;border:1px solid rgba(255,255,255,.08);'/>"

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
                <div style="border:1px solid rgba(255,255,255,0.08);
                    background:rgba(255,255,255,0.025);border-radius:12px;
                    padding:10px 12px;min-height:78px;margin-bottom:8px;">
                    <div style="font-size:11px;opacity:.70;margin-bottom:4px;">{_esc(label)}</div>
                    <div style="font-size:20px;font-weight:800;">{_esc(value)}</div>
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
    _render_detail_text(title, text_value)


def _render_section_list(title: str, values: List[str], limit: Optional[int] = None) -> None:
    clean = [strip_html(v) for v in values if strip_html(v)]
    if limit is not None:
        clean = clean[:limit]
    if not clean:
        return
    if title in _SPECIAL_PORTFOLIO_TITLES:
        _render_list_spotlight(title, clean, limit=None)
        return
    _render_detail_list(title, clean, limit=None)


def _render_key_value_section(title: str, data: Dict[str, Any], label_map: List[tuple]) -> None:
    if not data:
        return
    blocks: List[str] = []
    for key, label in label_map:
        value = data.get(key)
        if isinstance(value, str) and strip_html(value):
            blocks.append(f"<div><strong>{_esc(label)}:</strong> {_esc(value)}</div>")
        elif isinstance(value, list):
            clean = [strip_html(v) for v in value if strip_html(v)]
            if clean:
                blocks.append(f"<div><strong>{_esc(label)}:</strong> " + " • ".join(_esc(v) for v in clean) + "</div>")
    _render_detail_kv(title, blocks)


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

    cards = []
    for item in normalized:
        head = item["topico"] or "Evidência"
        body_parts = [f"<div style='font-size:12px;opacity:.72;margin-bottom:6px;font-weight:700'>{_esc(head)}</div>"]
        if item["trecho"]:
            body_parts.append(f"<div style='margin-bottom:8px;'><strong>Trecho:</strong> {_esc(item['trecho'])}</div>")
        if item["interpretacao"]:
            body_parts.append(f"<div><strong>Leitura:</strong> {_esc(item['interpretacao'])}</div>")
        cards.append(
            "<div style='border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.025);border-radius:12px;padding:12px 14px;margin:8px 0;'>"
            + "".join(body_parts)
            + "</div>"
        )
    _render_detail_section("Evidências", "".join(cards))


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

    parts = []
    header_bits = []
    if years:
        header_bits.append(f"<strong>Cobertura temporal:</strong> {_esc(', '.join([str(y) for y in years]))}")
    if n_events:
        header_bits.append(f"<strong>Eventos detectados:</strong> {n_events}")
    if summary:
        header_bits.append(_esc(summary))
    if header_bits:
        parts.append("<div style='margin-bottom:10px;line-height:1.7'>" + "<br/>".join(header_bits) + "</div>")
    if changes:
        parts.append("<div style='margin-bottom:8px;font-weight:700;opacity:.9'>Mudanças detectadas</div>")
        for c in changes[:10]:
            parts.append(f"<div style='margin:6px 0;line-height:1.65'>• {_esc(strip_html(c))}</div>")
    if timeline:
        for item in timeline[:6]:
            if not isinstance(item, dict):
                continue
            year = strip_html(item.get("year") or "—")
            summary_line = strip_html(item.get("summary") or "")
            evidences = item.get("evidences") if isinstance(item.get("evidences"), list) else []
            ev_text = " | ".join([strip_html(x) for x in evidences[:2] if strip_html(x)])
            parts.append(
                "<div style='border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.025);border-radius:12px;padding:12px 14px;margin:10px 0;'>"
                + f"<div style='font-size:13px;opacity:.8;margin-bottom:6px;font-weight:700'>{_esc(year)}</div>"
                + f"<div style='font-size:15px;line-height:1.6;font-weight:700'>{_esc(summary_line or 'Sem resumo temporal consolidado.')}</div>"
                + (f"<div style='font-size:12px;opacity:.72;margin-top:8px'>{_esc(ev_text)}</div>" if ev_text else "")
                + "</div>"
            )
    _render_detail_section("Detector de Mudança Estratégica", "".join(parts), "warn")



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

    summary = macro.get("macro_summary", {}) if isinstance(macro, dict) else {}
    anual = macro.get("anual", {}) if isinstance(macro, dict) else {}
    ref_date = str(summary.get("reference_date") or "")[:7]

    cards = [
        ("Selic (a.a.)", _fmt_macro_value(summary.get("selic_current"), "%"), str(summary.get("selic_trend") or "—"), "📈"),
        ("Câmbio (R$/USD)", _fmt_macro_value(summary.get("cambio_current"), decimals=4, prefix="R$ "), str(summary.get("cambio_trend") or "—"), "💵"),
        ("IPCA 12m", _fmt_macro_value(summary.get("ipca_12m_current"), "%"), str(summary.get("ipca_12m_trend") or "—"), "🧾"),
        (f"IPCA {anual.get('ipca_reference_year') or ''}".strip() or "IPCA anual", _fmt_macro_value(anual.get("ipca"), "%"), f"Acumulado até {int(anual.get('ipca_reference_month') or 0):02d}/{anual.get('ipca_reference_year') or ''}" if anual.get("ipca_interpretation") != "anual_fechado" and anual.get('ipca_reference_month') else ("Fechamento anual" if anual.get("ipca_interpretation") == "anual_fechado" else "—"), "🌎"),
    ]

    body = [f"<div style='font-size:12px;opacity:.75;font-weight:700;letter-spacing:.3px;text-transform:uppercase;margin-bottom:10px'>Cenário macro atual{(' • ref. ' + _esc(ref_date)) if ref_date else ''}</div>"]
    body.append("<div style='display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px'>")
    for label, value, extra, icon in cards:
        body.append(
            "<div style='border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.025);border-radius:16px;padding:14px 16px;box-shadow:0 10px 24px rgba(0,0,0,.18)'>"
            + f"<div style='font-size:12px;opacity:.74;margin-bottom:8px'>{_esc(label)}</div>"
            + f"<div style='font-size:18px;font-weight:900;margin-bottom:6px'>{icon} {_esc(value)}</div>"
            + f"<div style='font-size:12px;opacity:.7'>{_esc(extra)}</div>"
            + "</div>"
        )
    body.append("</div>")
    _render_spotlight_section("Painel Macro", "".join(body), "neutral")


def _render_score_explanations(company: CompanyAnalysis) -> None:
    score = company.score_qualitativo
    conf = company.confianca
    execucao = strip_html(company.execucao.get("avaliacao_execucao", "")) or "não classificada"         if isinstance(company.execucao, dict) else "não classificada"
    riscos = company.riscos
    evidencias = company.evidencias
    anos = company.strategy_detector.get("coverage_years", [])         if isinstance(company.strategy_detector.get("coverage_years"), list) else []

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

    body = f"<div><strong>{_esc(score_txt)}</strong></div><div style='margin-top:8px'>{_esc(conf_txt)}</div>"
    if company.score_source == "heuristic":
        body += "<div style='margin-top:10px'>" + _badge("Score heurístico — LLM não retornou valor; score estimado por estrutura do JSON", "warn") + "</div>"
    _render_detail_section("Como interpretar os scores", body, "neutral")


# ────────────────────────────────────────────────────────────────────────────────
# Portfolio-level sections
# ────────────────────────────────────────────────────────────────────────────────

def _render_allocation_section(allocation_rows: List[AllocationRow]) -> None:
    st.markdown("## 💼 Alocação Sugerida")
    st.caption("Distribuição percentual heurística entre todos os ativos cobertos no portfólio. Soma total = 100%.")
    cols_per_row = 4
    for i in range(0, len(allocation_rows), cols_per_row):
        row_items = allocation_rows[i:i + cols_per_row]
        cols = st.columns(len(row_items))
        for col, item in zip(cols, row_items):
            col.markdown(
                f"""
                <div class="p6-card">
                  <div class="p6-card-label">{_esc(item.ticker)}</div>
                  <div class="p6-card-value" style="font-size:24px">{item.allocation_pct:.2f}%</div>
                  <div class="p6-card-extra">
                    {_esc((item.perspectiva or '—').upper())} • Score {_fmt_score(item.score)} • Conf. {_fmt_confidence(item.confianca)}
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def _render_structured_portfolio_report(report: Dict[str, Any], mode_label: str) -> None:
    st.markdown("## 🧠 Relatório Estratégico do Portfólio")
    st.caption(f"Modo utilizado: {mode_label}")

    _render_section_text("Base analítica", report.get("analytical_basis", ""))
    _render_section_text("Diagnóstico executivo", report.get("executive_summary", ""))
    _render_section_text("Identidade da carteira", report.get("portfolio_identity", ""))
    _render_section_text("Cenário macro atual", report.get("current_market_context", ""))
    _render_section_text("Leitura macro", report.get("macro_reading", ""))
    _render_section_list("Riscos internacionais relevantes", report.get("international_risk_links", []), limit=8)
    _render_section_list("Dependências de cenário macro", report.get("macro_scenario_dependencies", []), limit=8)
    _render_section_list(
        "Vulnerabilidades da carteira sob o regime atual",
        report.get("portfolio_vulnerabilities_under_current_regime", []),
        limit=8,
    )
    _render_section_list(
        "O que a carteira está apostando implicitamente",
        report.get("what_the_portfolio_is_implicitly_betting_on", []),
        limit=8,
    )
    _render_section_text("Análise de concentração econômica", report.get("portfolio_concentration_analysis", ""))
    _render_section_text("Racional de ajuste de alocação", report.get("allocation_adjustment_rationale", ""))
    _render_section_list("Forças principais", report.get("key_strengths", []), limit=8)
    _render_section_list("Fragilidades principais", report.get("key_weaknesses", []), limit=8)
    _render_section_list("Riscos invisíveis", report.get("hidden_risks", []), limit=8)

    asset_roles = report.get("asset_roles", []) or []
    if asset_roles:
        _render_spotlight_section("Papel estratégico dos ativos", "<div style='font-size:13px;opacity:.82'>Leitura do papel de cada posição dentro da carteira, com foco em função estratégica e sensibilidade ao cenário.</div>", _SPECIAL_PORTFOLIO_TITLES.get("Papel estratégico dos ativos", "neutral"))
        for item in asset_roles[:12]:
            if not isinstance(item, dict):
                continue
            ticker = strip_html(item.get("ticker") or "—")
            role = strip_html(item.get("role") or "")
            rationale = strip_html(item.get("rationale") or "")
            logo = _render_logo_inline(ticker)
            st.markdown(
                f"""
                <div style="border:1px solid rgba(255,255,255,0.08);background:rgba(255,255,255,0.025);
                            border-radius:14px;padding:12px 14px;margin:10px 0;line-height:1.45;box-shadow:0 10px 24px rgba(0,0,0,.16);">
                    <div style="display:flex;align-items:center;gap:12px;margin-bottom:8px;">
                        {logo}
                        <div>
                            <div style="font-size:18px;font-weight:900;letter-spacing:.2px;">{_esc(ticker)}</div>
                            <div style="font-size:13px;opacity:.82;font-weight:700;">{_esc(role or 'Papel estratégico')}</div>
                        </div>
                    </div>
                    <div style="font-size:15px;line-height:1.65;">{_esc(rationale or '—')}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    suggested_allocations = report.get("suggested_allocations", []) or []
    if suggested_allocations:
        _render_spotlight_section("Alocação sugerida (visão estratégica)", "<div style='font-size:13px;opacity:.82'>Faixas de exposição sugeridas com racional resumido para facilitar decisões de ajuste.</div>", "good")
        for item in suggested_allocations[:15]:
            if not isinstance(item, dict):
                continue
            ticker = strip_html(item.get("ticker") or "—")
            suggested_range = strip_html(item.get("suggested_range") or "")
            rationale = strip_html(item.get("rationale") or "")
            logo = _render_logo_inline(ticker)
            badge = _badge(suggested_range or 'Faixa não informada', 'good') if suggested_range else ''
            st.markdown(
                f"""
                <div style="border:1px solid rgba(255,255,255,0.08);background:rgba(255,255,255,0.025);
                            border-radius:14px;padding:12px 14px;margin:10px 0;line-height:1.45;box-shadow:0 10px 24px rgba(0,0,0,.16);">
                    <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:8px;">
                        <div style="display:flex;align-items:center;gap:12px;">
                            {logo}
                            <div style="font-size:18px;font-weight:900;letter-spacing:.2px;">{_esc(ticker)}</div>
                        </div>
                        <div>{badge}</div>
                    </div>
                    <div style="font-size:15px;line-height:1.65;">{_esc(rationale or '—')}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    _render_section_list("Desalinhamentos", report.get("misalignments", []), limit=8)
    _render_section_list("Plano de ação", report.get("action_plan", []), limit=10)
    _render_section_text("Insight final", report.get("final_insight", ""))


# ────────────────────────────────────────────────────────────────────────────────
# Company detail section
# ────────────────────────────────────────────────────────────────────────────────

def _render_company_expander(company: CompanyAnalysis) -> None:
    """Card executivo por empresa.

    Hierarquia de informação:
      1. Decisão   — badge de perspectiva (FORTE / MODERADA / FRACA)
      2. Risco     — risco principal em destaque
      3. Síntese   — primeira frase da tese (sempre visível)
      4. Detalhe   — tudo mais dentro de st.expander (escondido por padrão)
    """
    tk = (company.ticker or "").strip() or "—"
    p = (company.perspectiva_compra or "").strip().lower()

    # ── Síntese curta: primeira frase da tese (máx 220 chars) ────────────────
    sintese_src = (company.tese or company.leitura or "").strip()
    if sintese_src:
        primeiro = sintese_src.split(".")[0].strip()
        sintese = (primeiro + ".") if primeiro else sintese_src[:220]
        if len(sintese) > 220:
            sintese = sintese[:217] + "…"
    else:
        sintese = "—"

    # ── Risco principal e ação recomendada ────────────────────────────────────
    risco_src = (
        company.riscos[0] if company.riscos
        else company.fragilidade_regime_atual or ""
    ).strip()
    if risco_src and len(risco_src) > 140:
        risco_src = risco_src[:140] + "…"

    action_src = (company.recommended_action or "").strip()
    if action_src and len(action_src) > 80:
        action_src = action_src[:80] + "…"

    attn_badge = ""
    if company.attention_level in ("alta", "média"):
        attn_tone = "bad" if company.attention_level == "alta" else "warn"
        attn_badge = _badge(f"⚡ {company.attention_level.upper()}", attn_tone)

    heuristic_badge = _badge("Score heurístico", "warn") if company.score_source == "heuristic" else ""

    # ── v4 decision badge ──────────────────────────────────────────────────────
    decision_badge = ""
    dl = (getattr(company, "decision_label", "") or "").strip().lower()
    if dl not in ("—", ""):
        ds = getattr(company, "decision_score", 0) or 0
        score_str = f" ({'+' if ds > 0 else ''}{ds})" if ds != 0 else ""
        decision_badge = _badge(
            f"→ {dl.upper()}{score_str}",
            _tone_from_decision(dl),
        )

    ec_class = {
        "forte":    "p6-ec p6-ec-forte",
        "moderada": "p6-ec p6-ec-moderada",
        "fraca":    "p6-ec p6-ec-fraca",
    }.get(p, "p6-ec p6-ec-unknown")

    badges_html = " ".join(
        [x for x in [
            _badge((p or "—").upper(), _tone_from_perspectiva(p)),
            decision_badge,
            attn_badge,
            heuristic_badge,
        ] if x]
    )

    extra_pills = []
    if risco_src:
        extra_pills.append(f'<span class="p6-risk-pill">⚠ {_esc(risco_src)}</span>')
    if action_src:
        extra_pills.append(f'<span class="p6-action-pill">→ {_esc(action_src)}</span>')
    extra_pills_html = "".join(extra_pills)

    card_html = (
        f'<div class="{ec_class}">'
        f'  <div class="p6-ec-head">'
        f'    <div class="p6-ec-ticker">{_esc(tk)}</div>'
        f'    <div class="p6-ec-badges">{badges_html}</div>'
        f'  </div>'
        f'  <div class="p6-ec-sintese">{_esc(sintese)}</div>'
        f'  <div style="display:flex;flex-wrap:wrap;gap:6px;align-items:center">{extra_pills_html}</div>'
        f'</div>'
    )
    st.markdown(card_html, unsafe_allow_html=True)

    # ── Detalhes: escondidos por padrão ──────────────────────────────────────
    with st.expander(f"Ver análise completa — {tk}", expanded=False):
        st.caption(
            f"Período analisado: {company.period_ref} • Atualizado em: {company.created_at}"
            + (f" • Confiança: {_fmt_confidence(company.confianca)}" if company.confianca > 0 else "")
            + (f" • Score: {_fmt_score(company.score_qualitativo)}" if company.score_qualitativo > 0 else "")
            + (" • ⚠️ Score calculado sem LLM" if company.score_source == "heuristic" else "")
        )

        metric_items = [
            ("Score qualitativo", _explicar_score(company)),
            ("Confiança", _explicar_confianca(company)),
        ]
        years = company.strategy_detector.get("coverage_years", []) \
            if isinstance(company.strategy_detector.get("coverage_years"), list) else []
        if years:
            metric_items.append(("Cobertura temporal", ", ".join([str(y) for y in years[:4]])))

        _render_metric_cards(metric_items, columns_per_row=2)
        _render_score_explanations(company)

        _render_section_text("Tese (síntese)", company.tese or "—")

        if company.leitura:
            _render_section_text("Leitura / Direcionalidade", company.leitura)
        elif p == "forte":
            _render_section_text(
                "Leitura / Direcionalidade",
                "Viés construtivo, com sinais qualitativos favoráveis no recorte analisado. "
                "Mantém assimetria positiva, com monitoramento de riscos.",
            )
        elif p == "moderada":
            _render_section_text(
                "Leitura / Direcionalidade",
                "Leitura equilibrada, com pontos positivos e ressalvas. "
                "Indica acompanhamento de gatilhos de execução, guidance e alocação de capital.",
            )
        elif p == "fraca":
            _render_section_text(
                "Leitura / Direcionalidade",
                "Leitura cautelosa, com sinais qualitativos desfavoráveis no recorte analisado. "
                "Recomenda postura defensiva e foco em mitigação de risco.",
            )

        _render_section_text("Papel estratégico", company.papel_estrategico)
        _render_section_list("Sensibilidades macro", company.sensibilidades_macro, limit=8)
        _render_section_text("Fragilidade sob o regime atual", company.fragilidade_regime_atual)
        _render_section_list("Dependências de cenário", company.dependencias_cenario, limit=8)
        _render_section_text("Faixa de alocação sugerida", company.alocacao_sugerida_faixa)
        _render_section_text("Racional de alocação", company.racional_alocacao)

        _render_key_value_section(
            "Evolução Estratégica", company.evolucao,
            [("historico", "Histórico"), ("fase_atual", "Fase atual"), ("tendencia", "Tendência")],
        )
        _render_strategy_detector(company.strategy_detector)
        _render_key_value_section(
            "Consistência do Discurso", company.consistencia,
            [
                ("analise", "Análise"),
                ("grau_consistencia", "Grau"),
                ("contradicoes", "Contradições"),
                ("sinais_positivos", "Sinais positivos"),
            ],
        )
        _render_key_value_section(
            "Execução vs Promessa", company.execucao,
            [
                ("analise", "Análise"),
                ("avaliacao_execucao", "Avaliação"),
                ("entregas_confirmadas", "Entregas confirmadas"),
                ("entregas_pendentes_ou_incertas", "Entregas pendentes ou incertas"),
                ("entregas_pendentes", "Entregas pendentes"),
            ],
        )
        _render_section_list("Mudanças Estratégicas", company.mudancas, limit=6)
        _render_section_list("Pontos-chave", company.pontos_chave, limit=8)
        _render_section_list("Catalisadores", company.catalisadores, limit=6)
        # v4: risk_rank (prioritized) takes precedence over raw riscos list
        risk_rank = getattr(company, "risk_rank", None) or []
        if risk_rank:
            _render_section_list("Riscos (prioritários)", risk_rank, limit=6)
        else:
            _render_section_list("Riscos", company.riscos, limit=6)
        _render_section_list("O que monitorar", company.monitorar, limit=6)
        _render_section_list("Ruídos e Contradições", company.contradicoes + company.sinais_ruido, limit=6)
        _render_key_value_section(
            "Qualidade Narrativa", company.qualidade_narrativa,
            [("clareza", "Clareza"), ("coerencia", "Coerência"), ("sinais_de_ruido", "Sinais de ruído")],
        )
        _render_evidence_section(company.evidencias, limit=10)
        _render_section_text("Considerações da LLM", company.consideracoes)

        _render_section_list("Entregas confirmadas (recorrentes)", company.delivered_promises, limit=5)

        if company.current_regime not in ("—", "indefinido", ""):
            st.markdown("**Mudança de Regime Qualitativo**")
            regime_tone = (
                "bad" if company.regime_change_intensity == "significativo"
                else "warn" if company.regime_change_intensity == "moderado"
                else "neutral"
            )
            badges = _badge(f"Atual: {company.current_regime}", "neutral")
            if company.previous_regime not in ("—", "indefinido", ""):
                badges += "  " + _badge(f"Anterior: {company.previous_regime}", "neutral")
            badges += "  " + _badge(
                f"Intensidade: {company.regime_change_intensity}", regime_tone
            )
            st.markdown(badges, unsafe_allow_html=True)
            if company.regime_change_explanation:
                st.markdown(_box_html(company.regime_change_explanation), unsafe_allow_html=True)

        if company.attention_score > 0:
            attn_tone = (
                "bad" if company.attention_level == "alta"
                else "warn" if company.attention_level == "média"
                else "neutral"
            )
            badges = (
                _badge(f"Nível: {company.attention_level.upper()}", attn_tone)
                + "&nbsp;&nbsp;"
                + _badge(f"Score: {company.attention_score:.0f}/100", attn_tone)
                + "&nbsp;&nbsp;"
                + _badge(company.recommended_action, attn_tone)
            )
            _render_detail_badges("Prioridade de Acompanhamento", badges)

        if company.forward_score > 0:
            fwd_tone = (
                "good" if company.forward_direction == "melhorando"
                else "bad" if company.forward_direction == "deteriorando"
                else "neutral"
            )
            badges = (
                _badge(f"Forward score: {_fmt_score(company.forward_score)}", fwd_tone)
                + "&nbsp;&nbsp;"
                + _badge(f"Direção: {company.forward_direction}", fwd_tone)
                + "&nbsp;&nbsp;"
                + _badge(f"Confiança: {_fmt_confidence(company.forward_confidence)}", "neutral")
            )
            _render_detail_badges("Sinal Prospectivo", badges)
            _render_section_list("Fatores prospectivos", company.forward_drivers, limit=6)


# ────────────────────────────────────────────────────────────────────────────────
# Entry point
# ────────────────────────────────────────────────────────────────────────────────

_P6_CSS = """
<style>
/* ── Cards base ── */
.p6-card{
  border:1px solid rgba(255,255,255,0.08);
  background:rgba(255,255,255,0.03);
  border-radius:16px;
  padding:16px 18px;
  box-shadow:0 10px 24px rgba(0,0,0,0.25);
  min-height:110px;
}
.p6-card-label{font-size:12px;opacity:0.7;margin-bottom:6px;letter-spacing:0.3px;}
.p6-card-value{font-size:28px;font-weight:900;margin-bottom:6px;}
.p6-card-extra{font-size:12px;opacity:0.65;}

/* ── Cards executivos por empresa: acento de cor pela decisão ── */
.p6-ec{
  border:1px solid rgba(255,255,255,0.08);
  border-radius:16px;
  padding:16px 18px;
  box-shadow:0 8px 20px rgba(0,0,0,0.22);
  margin-bottom:8px;
  border-left-width:4px;
  border-left-style:solid;
}
.p6-ec-forte   { border-left-color:#22c55e;
                  background:linear-gradient(135deg,rgba(34,197,94,.07) 0%,rgba(255,255,255,.02) 100%); }
.p6-ec-moderada{ border-left-color:#f59e0b;
                  background:linear-gradient(135deg,rgba(245,158,11,.07) 0%,rgba(255,255,255,.02) 100%); }
.p6-ec-fraca   { border-left-color:#ef4444;
                  background:linear-gradient(135deg,rgba(239,68,68,.07) 0%,rgba(255,255,255,.02) 100%); }
.p6-ec-unknown { border-left-color:#64748b;
                  background:rgba(255,255,255,0.02); }

/* ── Row: ticker + badges ── */
.p6-ec-head{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:8px}
.p6-ec-ticker{font-size:17px;font-weight:900;letter-spacing:.3px}
.p6-ec-badges{display:flex;flex-wrap:wrap;gap:6px;align-items:center}

/* ── Síntese ── */
.p6-ec-sintese{font-size:13px;opacity:.85;line-height:1.5;margin-bottom:4px}

/* ── Pill de risco ── */
.p6-risk-pill{
  display:inline-block;
  padding:2px 9px;
  border-radius:999px;
  border:1px solid rgba(239,68,68,.40);
  background:rgba(239,68,68,.08);
  color:#ef4444;
  font-size:11px;
  font-weight:500;
  margin-top:5px;
}

/* ── Pill de ação recomendada ── */
.p6-action-pill{
  display:inline-block;
  padding:2px 9px;
  border-radius:999px;
  border:1px solid rgba(100,116,139,.35);
  background:rgba(100,116,139,.08);
  color:#94a3b8;
  font-size:11px;
  font-weight:500;
  margin-top:4px;
  margin-left:2px;
}
</style>
"""


def render_patch6_report(
    tickers: List[str],
    period_ref: str,
    llm_factory: Optional[Any] = None,
    show_company_details: bool = True,
    analysis_mode: str = "rigid",
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
        f'<div class="p6-card-extra">Heurística agregada a partir dos sinais do RAG.</div></div>',
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
        f'<div class="p6-card-extra">Ativos com evidências suficientes no período analisado.</div></div>',
        unsafe_allow_html=True,
    )
    col4.markdown(
        f'<div class="p6-card"><div class="p6-card-label">Confiança média</div>'
        f'<div class="p6-card-value" style="color:{_conf_color}">{_conf_lbl}</div>'
        f'<div class="p6-card-extra">Média do campo confianca_analise nas leituras individuais.</div></div>',
        unsafe_allow_html=True,
    )
    col5.markdown(
        f'<div class="p6-card"><div class="p6-card-label">Score qualitativo médio</div>'
        f'<div class="p6-card-value" style="color:{_sc_color}">{_sc_lbl}</div>'
        f'<div class="p6-card-extra">Média do score_qualitativo salvo pela LLM.</div></div>',
        unsafe_allow_html=True,
    )
    st.caption(
        "🛈 Como a qualidade é estimada: combinação de cobertura do portfólio, perspectiva 12m agregada e distribuição de sinais. "
        f"A cobertura temporal do detector estratégico está presente em {analysis.temporal_covered} ativo(s)."
    )

    _render_macro_panel()

    # ── v4 portfolio_trend strip ──────────────────────────────────────────────
    portfolio_trend = getattr(analysis, "portfolio_trend", {}) or {}
    if portfolio_trend:
        _TREND_DISPLAY = [
            ("qualidade",  "📊 Qualidade"),
            ("execucao",   "⚙️ Execução"),
            ("governanca", "🏛️ Governança"),
            ("capital",    "💰 Capital"),
        ]
        trend_badges = []
        for key, label in _TREND_DISPLAY:
            val = portfolio_trend.get(key, "")
            if val:
                trend_badges.append(_badge(f"{label}: {val}", _tone_from_trend(val)))
        if trend_badges:
            st.markdown(
                "<div style='margin:10px 0 4px;'>"
                + "&nbsp;&nbsp;".join(trend_badges)
                + "</div>",
                unsafe_allow_html=True,
            )

    # ── v3 portfolio signals ──────────────────────────────────────────────────
    v3_items = []
    if analysis.alta_prioridade_count > 0:
        v3_items.append(
            _badge(f"{analysis.alta_prioridade_count} ativo(s) em alta prioridade", "bad")
        )
    if analysis.forward_score_medio > 0:
        fdir_overall = "—"
        fscores = [c.forward_score for c in analysis.companies.values() if c.forward_score > 0]
        if fscores and analysis.score_medio > 0:
            avg_delta = sum(fscores) / len(fscores) - analysis.score_medio
            fdir_overall = "melhorando" if avg_delta > 5 else ("deteriorando" if avg_delta < -5 else "estável")
        v3_items.append(
            _badge(f"Forward score médio: {analysis.forward_score_medio}/100 ({fdir_overall})", "neutral")
        )
    if analysis.regime_summary:
        v3_items.append(_badge("Mudanças de regime detectadas", "warn"))

    if v3_items:
        st.markdown("&nbsp;&nbsp;".join(v3_items) + (
            f"<br/><span style='font-size:11px;opacity:0.6'>{_esc(analysis.regime_summary)}</span>"
            if analysis.regime_summary else ""
        ), unsafe_allow_html=True)
        st.markdown("")  # spacing

    # ── v3 priority ranking (compact) ────────────────────────────────────────
    if analysis.priority_ranking:
        alta = [tk for tk in analysis.priority_ranking if analysis.companies[tk].attention_level == "alta"]
        media = [tk for tk in analysis.priority_ranking if analysis.companies[tk].attention_level == "média"]
        if alta or media:
            with st.expander("📋 Fila de Atenção do Portfólio", expanded=False):
                if alta:
                    st.markdown("**Alta prioridade**")
                    for tk in alta:
                        c = analysis.companies[tk]
                        st.markdown(
                            f"- **{tk}** — score {c.attention_score:.0f} | {c.recommended_action}"
                            + (f" | drivers: {', '.join(c.attention_drivers[:3])}" if c.attention_drivers else ""),
                        )
                if media:
                    st.markdown("**Média prioridade**")
                    for tk in media:
                        c = analysis.companies[tk]
                        st.markdown(f"- **{tk}** — score {c.attention_score:.0f} | {c.recommended_action}")

    # ── LLM portfolio report (optional) ──────────────────────────────────────
    portfolio_report = run_portfolio_llm_report(llm_factory, analysis, analysis_mode)

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
