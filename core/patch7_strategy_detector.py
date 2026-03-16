
# core/patch7_strategy_detector.py
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple
import json
import re
from collections import defaultdict


_YEAR_RE = re.compile(r"\b(20\d{2})\b")
_THEME_PATTERNS: Dict[str, List[str]] = {
    "alocacao_capital": [
        r"dividend",
        r"jcp",
        r"payout",
        r"recompra",
        r"buyback",
        r"retorno ao acionista",
        r"aloca(?:c|ç)[aã]o de capital",
    ],
    "desalavancagem_divida": [
        r"d[ií]vida",
        r"alavanc",
        r"desalavanc",
        r"amortiza",
        r"refinancia",
        r"covenant",
    ],
    "crescimento_capex": [
        r"capex",
        r"investiment",
        r"expans",
        r"nova planta",
        r"aumento de capacidade",
        r"crescimento org[aâ]nico",
    ],
    "mna_portfolio": [
        r"aquisi",
        r"fus[aã]o",
        r"incorpora",
        r"cis[aã]o",
        r"desinvest",
        r"aliena",
        r"venda de ativo",
    ],
    "guidance_execucao": [
        r"guidance",
        r"proje",
        r"meta",
        r"cronograma",
        r"execu(?:c|ç)[aã]o",
        r"entrega",
    ],
    "governanca": [
        r"governan",
        r"conselho",
        r"comit[eê]",
        r"minorit[aá]rio",
        r"tag along",
    ],
    "eficiencia_operacional": [
        r"efici[êe]ncia",
        r"produtividade",
        r"redu[cç][aã]o de custos",
        r"margem",
        r"ebitda",
        r"otimiza",
    ],
}

_THEME_LABELS = {
    "alocacao_capital": "alocação de capital",
    "desalavancagem_divida": "desalavancagem / dívida",
    "crescimento_capex": "crescimento / CAPEX",
    "mna_portfolio": "M&A / portfólio",
    "guidance_execucao": "guidance / execução",
    "governanca": "governança",
    "eficiencia_operacional": "eficiência operacional",
}

_SENTIMENT_POS = [
    r"redu[cç][aã]o de d[ií]vida",
    r"desalavanc",
    r"crescimento",
    r"expans",
    r"recompra",
    r"dividend",
    r"efici[êe]ncia",
    r"margem",
    r"entrega",
    r"disciplina",
]
_SENTIMENT_NEG = [
    r"press[aã]o",
    r"queda",
    r"revis[aã]o",
    r"atraso",
    r"incerteza",
    r"alavancagem",
    r"risco",
    r"deteriora",
    r"fraco",
    r"desafio",
]


@dataclass
class StrategyEvent:
    year: str
    theme_key: str
    theme_label: str
    direction: str
    intensity: float
    evidence: str
    source: str = ""


def _as_list(v: Any) -> List[Any]:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        return [v]
    return []


def _as_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    return str(v).strip()


def _extract_year(text: str) -> Optional[str]:
    m = _YEAR_RE.search(text or "")
    return m.group(1) if m else None


def _detect_theme(text: str) -> List[str]:
    found: List[str] = []
    lowered = (text or "").lower()
    for theme, patterns in _THEME_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, lowered, flags=re.IGNORECASE):
                found.append(theme)
                break
    return found or ["guidance_execucao"]


def _direction_score(text: str) -> Tuple[str, float]:
    lowered = (text or "").lower()
    pos = sum(1 for pat in _SENTIMENT_POS if re.search(pat, lowered, flags=re.IGNORECASE))
    neg = sum(1 for pat in _SENTIMENT_NEG if re.search(pat, lowered, flags=re.IGNORECASE))
    raw = pos - neg
    if raw >= 2:
        return "fortalecimento", 0.9
    if raw == 1:
        return "avanço", 0.7
    if raw == 0:
        return "reorientação", 0.5
    if raw == -1:
        return "pressão", 0.7
    return "deterioração", 0.9


def _normalize_snippet(text: str, limit: int = 220) -> str:
    text = re.sub(r"\s+", " ", _as_str(text))
    return text[:limit].rstrip() + ("..." if len(text) > limit else "")


def _extract_events_from_evidences(evidences: List[Any]) -> List[StrategyEvent]:
    events: List[StrategyEvent] = []

    for ev in evidences:
        if isinstance(ev, dict):
            topico = _as_str(ev.get("topico"))
            trecho = _as_str(ev.get("trecho") or ev.get("citacao"))
            interpretacao = _as_str(ev.get("interpretacao") or ev.get("leitura"))
            text = " ".join([topico, trecho, interpretacao]).strip()
            year = _extract_year(text) or _extract_year(topico) or "Atual"
            themes = _detect_theme(text)
            direction, intensity = _direction_score(text)

            for theme in themes:
                events.append(
                    StrategyEvent(
                        year=year,
                        theme_key=theme,
                        theme_label=_THEME_LABELS.get(theme, theme),
                        direction=direction,
                        intensity=intensity,
                        evidence=_normalize_snippet(text),
                        source="evidencias",
                    )
                )
        elif isinstance(ev, str) and ev.strip():
            text = _as_str(ev)
            year = _extract_year(text) or "Atual"
            themes = _detect_theme(text)
            direction, intensity = _direction_score(text)
            for theme in themes:
                events.append(
                    StrategyEvent(
                        year=year,
                        theme_key=theme,
                        theme_label=_THEME_LABELS.get(theme, theme),
                        direction=direction,
                        intensity=intensity,
                        evidence=_normalize_snippet(text),
                        source="evidencias",
                    )
                )

    return events


def _extract_events_from_topicos(topicos: Dict[str, Any]) -> List[StrategyEvent]:
    events: List[StrategyEvent] = []

    for topic_name, payload in (topicos or {}).items():
        if not isinstance(payload, dict):
            continue

        blocks: List[str] = []
        for key in [
            "resumo_topico",
            "fatos_relevantes",
            "mudancas_detectadas",
            "riscos",
            "catalisadores",
        ]:
            value = payload.get(key)
            if isinstance(value, list):
                blocks.extend(_as_str(x) for x in value if _as_str(x))
            else:
                txt = _as_str(value)
                if txt:
                    blocks.append(txt)

        evo = payload.get("evolucao_temporal")
        if isinstance(evo, dict):
            blocks.extend(_as_str(v) for v in evo.values() if _as_str(v))

        merged = " ".join(blocks).strip()
        if not merged:
            continue

        year = _extract_year(merged) or "Atual"
        direction, intensity = _direction_score(merged)
        themes = _detect_theme(topic_name + " " + merged)

        for theme in themes:
            events.append(
                StrategyEvent(
                    year=year,
                    theme_key=theme,
                    theme_label=_THEME_LABELS.get(theme, theme),
                    direction=direction,
                    intensity=intensity,
                    evidence=_normalize_snippet(f"{topic_name}: {merged}"),
                    source="topicos",
                )
            )

    return events


def _compress_timeline(events: List[StrategyEvent]) -> Dict[str, List[StrategyEvent]]:
    timeline: Dict[str, List[StrategyEvent]] = defaultdict(list)
    for ev in events:
        timeline[ev.year].append(ev)
    return dict(sorted(timeline.items(), key=lambda kv: kv[0]))


def _build_strategy_changes(events: List[StrategyEvent], limit: int = 6) -> List[str]:
    grouped: Dict[str, Dict[str, List[StrategyEvent]]] = defaultdict(lambda: defaultdict(list))
    for ev in events:
        grouped[ev.theme_key][ev.year].append(ev)

    changes: List[str] = []

    for theme_key, year_map in grouped.items():
        years = sorted(year_map.keys())
        if len(years) < 2:
            continue

        first_year = years[0]
        last_year = years[-1]
        first_dir = year_map[first_year][0].direction
        last_dir = year_map[last_year][0].direction
        label = _THEME_LABELS.get(theme_key, theme_key)

        if first_dir != last_dir:
            changes.append(
                f"{first_year} → {last_year}: mudança em {label}, saindo de '{first_dir}' para '{last_dir}'."
            )
        else:
            changes.append(
                f"{first_year} → {last_year}: continuidade em {label}, com padrão predominante de '{last_dir}'."
            )

    return changes[:limit]


def _build_year_summary(timeline: Dict[str, List[StrategyEvent]], limit_per_year: int = 3) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    for year, items in timeline.items():
        ranked = sorted(items, key=lambda x: x.intensity, reverse=True)[:limit_per_year]
        out.append(
            {
                "year": year,
                "themes": [ev.theme_label for ev in ranked],
                "directions": [ev.direction for ev in ranked],
                "summary": "; ".join(
                    f"{ev.theme_label}: {ev.direction}" for ev in ranked
                ),
                "evidences": [ev.evidence for ev in ranked],
            }
        )

    return out


def _build_detector_summary(events: List[StrategyEvent], changes: List[str]) -> str:
    if not events:
        return "Sem evidências suficientes para detectar mudança estratégica automática."
    if not changes:
        return "Há sinais temáticos relevantes, mas ainda sem mudança estratégica claramente detectável no recorte atual."
    return "Detector identificou mudança ou continuidade estratégica com base em temas recorrentes ao longo do histórico documental."


def detect_strategy_shift(result_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    Detecta mudanças estratégicas automaticamente a partir do result_json do Patch6.

    Entrada esperada:
    - result_json rico com campos como:
      - evidencias
      - topicos
      - evolucao_estrategica
      - mudancas_estrategicas (opcional)

    Saída:
    {
      "strategy_detector": {
        "summary": "...",
        "yearly_timeline": [...],
        "detected_changes": [...],
        "events": [...],
        "coverage_years": [...],
        "n_events": 0
      }
    }
    """
    result_json = result_json or {}

    evidences = _as_list(result_json.get("evidencias"))
    topicos = result_json.get("topicos") if isinstance(result_json.get("topicos"), dict) else {}

    events = []
    events.extend(_extract_events_from_evidences(evidences))
    events.extend(_extract_events_from_topicos(topicos))

    if not events:
        return {
            "strategy_detector": {
                "summary": "Sem evidências suficientes para detectar mudança estratégica automática.",
                "yearly_timeline": [],
                "detected_changes": [],
                "events": [],
                "coverage_years": [],
                "n_events": 0,
            }
        }

    timeline = _compress_timeline(events)
    changes = _build_strategy_changes(events)
    yearly = _build_year_summary(timeline)
    coverage_years = list(timeline.keys())

    return {
        "strategy_detector": {
            "summary": _build_detector_summary(events, changes),
            "yearly_timeline": yearly,
            "detected_changes": changes,
            "events": [asdict(ev) for ev in events[:40]],
            "coverage_years": coverage_years,
            "n_events": len(events),
        }
    }


def enrich_patch6_result(result_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    Adiciona strategy_detector ao result_json sem destruir a estrutura original.
    """
    base = dict(result_json or {})
    base.update(detect_strategy_shift(base))
    return base


def render_strategy_detector_markdown(result_json: Dict[str, Any]) -> str:
    """
    Gera markdown simples para uso em Streamlit ou relatórios.
    """
    enriched = enrich_patch6_result(result_json)
    detector = enriched.get("strategy_detector", {}) or {}

    lines: List[str] = []
    lines.append("### Detector de Mudança Estratégica")
    lines.append(detector.get("summary", "—"))

    years = detector.get("coverage_years") or []
    if years:
        lines.append("")
        lines.append("**Cobertura temporal:** " + ", ".join(years))

    changes = detector.get("detected_changes") or []
    if changes:
        lines.append("")
        lines.append("**Mudanças detectadas**")
        for item in changes[:6]:
            lines.append(f"- {item}")

    timeline = detector.get("yearly_timeline") or []
    if timeline:
        lines.append("")
        lines.append("**Linha do tempo estratégica**")
        for item in timeline[:6]:
            year = item.get("year", "—")
            summary = item.get("summary", "—")
            lines.append(f"- {year}: {summary}")

    return "\n".join(lines)


if __name__ == "__main__":
    sample = {
        "evidencias": [
            {
                "topico": "2022",
                "trecho": "Companhia priorizou desalavancagem e amortização de dívida.",
                "interpretacao": "Foco defensivo em balanço."
            },
            {
                "topico": "2023",
                "trecho": "Gestão reforçou plano de eficiência e retomada gradual de CAPEX.",
                "interpretacao": "Reorientação operacional."
            },
            {
                "topico": "2024",
                "trecho": "Empresa anunciou dividendos, JCP e programa de recompra de ações.",
                "interpretacao": "Maior retorno ao acionista."
            }
        ]
    }

    enriched = enrich_patch6_result(sample)
    print(json.dumps(enriched, ensure_ascii=False, indent=2))
