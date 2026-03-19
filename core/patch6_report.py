
"""core/patch6_report.py

Renderização profissional do Patch6 (relatório estilo casa de análise) usando dados persistidos em public.patch6_runs.

- Não mexe no pipeline (ingest/chunk/RAG/LLM). Apenas consolida e apresenta.
- Funciona mesmo sem LLM: usa templates + agregações.
- Se um cliente LLM estiver disponível (via llm_factory.get_llm_client()), cria Resumo Executivo e Conclusão com linguagem institucional.
- Compatível com result_json legado e com o schema rico do patch6_writer.
- Compatível com strategy_detector do patch7.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import html
import json
import os
import re

import pandas as pd
import streamlit as st
from sqlalchemy import text

from core.db_loader import get_supabase_engine


@dataclass
class PortfolioStats:
    fortes: int = 0
    moderadas: int = 0
    fracas: int = 0
    desconhecidas: int = 0

    @property
    def total(self) -> int:
        return self.fortes + self.moderadas + self.fracas + self.desconhecidas

    def label_qualidade(self) -> str:
        if self.total == 0:
            return "—"
        if self.fortes >= max(1, int(0.4 * self.total)):
            return "Alta"
        if self.fracas >= max(1, int(0.4 * self.total)):
            return "Baixa"
        return "Moderada"

    def label_perspectiva(self) -> str:
        if self.total == 0:
            return "—"
        if self.fortes > self.fracas and self.fortes >= self.moderadas:
            return "Construtiva"
        if self.fracas > self.fortes and self.fracas >= self.moderadas:
            return "Cautelosa"
        return "Neutra"


_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(value: Any) -> str:
    if value is None:
        return ""
    txt = str(value)
    txt = _TAG_RE.sub("", txt)
    txt = txt.replace("&nbsp;", " ")
    txt = re.sub(r"\s+\n", "\n", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)
    return txt.strip()


def _esc(value: Any) -> str:
    return html.escape(_strip_html(value))


def _safe_call_llm(llm_client: Any, prompt: str) -> Optional[str]:
    try:
        if llm_client is None:
            return None

        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

        if hasattr(llm_client, "responses") and hasattr(llm_client.responses, "create") and callable(llm_client.responses.create):
            resp = llm_client.responses.create(model=model, input=prompt)
            txt = getattr(resp, "output_text", None)
            if txt:
                return txt
            try:
                return resp.output[0].content[0].text
            except Exception:
                return str(resp)

        if hasattr(llm_client, "chat") and hasattr(llm_client.chat, "completions") and hasattr(llm_client.chat.completions, "create"):
            resp = llm_client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
            )
            return resp.choices[0].message.content

        if hasattr(llm_client, "complete") and callable(getattr(llm_client, "complete")):
            return llm_client.complete(prompt)
        if hasattr(llm_client, "chat") and callable(getattr(llm_client, "chat")):
            return llm_client.chat(prompt)
        if hasattr(llm_client, "invoke") and callable(getattr(llm_client, "invoke")):
            return llm_client.invoke(prompt)
        if callable(llm_client):
            return llm_client(prompt)

        return None
    except Exception:
        return None


def _load_latest_runs(tickers: List[str], period_ref: str) -> pd.DataFrame:
    tickers = [str(t).strip().upper() for t in (tickers or []) if str(t).strip()]
    if not tickers:
        return pd.DataFrame()

    engine = get_supabase_engine()

    q = text(
        """
        with ranked as (
            select
                ticker,
                period_ref,
                created_at,
                perspectiva_compra,
                resumo,
                result_json,
                row_number() over (partition by ticker, period_ref order by created_at desc) as rn
            from public.patch6_runs
            where period_ref = :pr and ticker = any(:tks)
        )
        select ticker, period_ref, created_at, perspectiva_compra, resumo, result_json
        from ranked
        where rn = 1
        order by ticker asc
        """
    )

    with engine.connect() as conn:
        df = pd.read_sql_query(q, conn, params={"pr": str(period_ref).strip(), "tks": tickers})
    return df


def _compute_stats(df_latest: pd.DataFrame) -> PortfolioStats:
    stats = PortfolioStats()
    if df_latest is None or df_latest.empty:
        return stats

    for p in df_latest["perspectiva_compra"].fillna("").astype(str).str.strip().str.lower().tolist():
        if p == "forte":
            stats.fortes += 1
        elif p == "moderada":
            stats.moderadas += 1
        elif p == "fraca":
            stats.fracas += 1
        else:
            stats.desconhecidas += 1
    return stats


def _badge(texto: str, tone: str = "neutral") -> str:
    tone_map = {
        "good": "#0ea5e9",
        "warn": "#f59e0b",
        "bad": "#ef4444",
        "neutral": "#94a3b8",
    }
    color = tone_map.get(tone, "#94a3b8")
    return (
        f"<span style='display:inline-block;padding:2px 10px;border-radius:999px;"
        f"border:1px solid {color};color:{color};font-weight:600;font-size:12px'>{texto}</span>"
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


def _as_result_obj(value: Any) -> dict:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    try:
        if isinstance(value, str) and value.strip():
            return json.loads(value)
    except Exception:
        return {}
    return {}


def _pick_text(obj: Dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = obj.get(key)
        if isinstance(value, str) and value.strip():
            return _strip_html(value)
        if isinstance(value, dict):
            nested = " ".join(
                _strip_html(v) for v in value.values() if isinstance(v, str) and _strip_html(v)
            ).strip()
            if nested:
                return nested
    return ""


def _pick_list(obj: Dict[str, Any], *keys: str) -> List[str]:
    for key in keys:
        value = obj.get(key)
        if isinstance(value, list):
            out = []
            for item in value:
                if isinstance(item, str) and item.strip():
                    out.append(_strip_html(item))
                elif isinstance(item, dict):
                    txt = " — ".join(
                        _strip_html(v) for v in item.values() if isinstance(v, str) and _strip_html(v)
                    ).strip(" —")
                    if txt:
                        out.append(txt)
            if out:
                return out
        if isinstance(value, str) and value.strip():
            return [_strip_html(value)]
    return []


def _pick_dict(obj: Dict[str, Any], *keys: str) -> Dict[str, Any]:
    for key in keys:
        value = obj.get(key)
        if isinstance(value, dict) and value:
            return value
    return {}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _fmt_confidence(value: float) -> str:
    if value <= 0:
        return "—"
    pct = round(max(0.0, min(1.0, value)) * 100)
    return f"{pct}%"


def _fmt_score(value: int) -> str:
    if value <= 0:
        return "—"
    return f"{max(0, min(100, value))}/100"


def _box_html(text: str) -> str:
    return f"""
        <div style="
            border:1px solid rgba(255,255,255,0.08);
            background:rgba(255,255,255,0.03);
            border-radius:14px;
            padding:14px 16px;
            box-shadow:0 10px 24px rgba(0,0,0,0.18);
            margin-top:8px;
            line-height:1.6;">
            {_esc(text).replace(chr(10), '<br/>')}
        </div>
    """



def _render_metric_cards(items: List[tuple[str, str]], columns_per_row: int = 3) -> None:
    clean_items = [(str(label), str(value)) for label, value in items if str(label).strip()]
    if not clean_items:
        return

    for i in range(0, len(clean_items), columns_per_row):
        row_items = clean_items[i:i + columns_per_row]
        cols = st.columns(len(row_items))
        for col, (label, value) in zip(cols, row_items):
            col.markdown(
                f"""
                <div style="
                    border:1px solid rgba(255,255,255,0.08);
                    background:rgba(255,255,255,0.025);
                    border-radius:12px;
                    padding:10px 12px;
                    min-height:78px;
                    margin-bottom:8px;">
                    <div style="font-size:11px;opacity:.70;margin-bottom:4px;">{_esc(label)}</div>
                    <div style="font-size:20px;font-weight:800;">{_esc(value)}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )



def _explicar_score(company: Dict[str, Any]) -> str:
    score = _safe_int(company.get("score_qualitativo"), 0)
    riscos = len(company.get("riscos") or [])
    evidencias = len(company.get("evidencias") or [])
    execucao = ""
    if isinstance(company.get("execucao"), dict):
        execucao = _strip_html(company["execucao"].get("avaliacao_execucao", "")) or "não classificada"
    else:
        execucao = "não classificada"

    if score >= 75:
        faixa = "🟢 Forte"
    elif score >= 55:
        faixa = "🟡 Moderada"
    elif score >= 40:
        faixa = "🟠 Atenção"
    else:
        faixa = "🔴 Fraca"

    return f"{_fmt_score(score)} • {faixa} | Execução: {execucao} | {riscos} riscos | {evidencias} evidências"


def _explicar_confianca(company: Dict[str, Any]) -> str:
    conf = _safe_float(company.get("confianca"), 0.0)
    pct = _fmt_confidence(conf)
    evidencias = len(company.get("evidencias") or [])
    detector = company.get("strategy_detector") or {}
    anos = len(detector.get("coverage_years", [])) if isinstance(detector, dict) else 0

    if conf >= 0.75:
        faixa = "🟢 Alta"
    elif conf >= 0.55:
        faixa = "🟡 Média"
    else:
        faixa = "🔴 Baixa"

    return f"{pct} • {faixa} | {evidencias} evidências | {anos} ano(s) analisado(s)"


def _render_score_explanations(company: Dict[str, Any]) -> None:
    score = _safe_int(company.get("score_qualitativo"), 0)
    conf = _safe_float(company.get("confianca"), 0.0)

    if isinstance(company.get("execucao"), dict):
        execucao = _strip_html(company["execucao"].get("avaliacao_execucao", "")) or "não classificada"
    else:
        execucao = "não classificada"

    riscos = company.get("riscos") or []
    evidencias = company.get("evidencias") or []
    detector = company.get("strategy_detector") or {}
    anos = detector.get("coverage_years", []) if isinstance(detector, dict) else []

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


def _render_section_text(title: str, text_value: str) -> None:
    clean = _strip_html(text_value)
    if not clean:
        return
    st.markdown(f"### {title}")
    st.markdown(_box_html(clean), unsafe_allow_html=True)


def _render_section_list(title: str, values: List[str], limit: Optional[int] = None) -> None:
    clean_values = [_strip_html(v) for v in values if _strip_html(v)]
    if limit is not None:
        clean_values = clean_values[:limit]
    if not clean_values:
        return
    st.markdown(f"### {title}")
    st.markdown(
        "<div style='border:1px solid rgba(255,255,255,0.08);background:rgba(255,255,255,0.02);"
        "border-radius:14px;padding:12px 16px;margin-top:8px;'>"
        + "".join([f"<div style='margin:8px 0;line-height:1.5;'><span style='font-weight:800;margin-right:8px;'>•</span>{_esc(item)}</div>" for item in clean_values])
        + "</div>",
        unsafe_allow_html=True,
    )


def _render_key_value_section(title: str, data: Dict[str, Any], label_map: List[tuple[str, str]]) -> None:
    if not data:
        return
    rows: List[str] = []
    for key, label in label_map:
        value = data.get(key)
        if isinstance(value, str) and _strip_html(value):
            rows.append(
                f"<div style='margin:10px 0 14px 0;'>"
                f"<div style='font-size:12px;opacity:.72;font-weight:700;letter-spacing:.2px;text-transform:uppercase;margin-bottom:4px;'>{_esc(label)}</div>"
                f"<div style='font-size:16px;line-height:1.72;'>{_esc(value)}</div>"
                f"</div>"
            )
        elif isinstance(value, list):
            clean_values = [_strip_html(v) for v in value if _strip_html(v)]
            if clean_values:
                joined = "<br/>".join([f"• {_esc(v)}" for v in clean_values])
                rows.append(
                    f"<div style='margin:10px 0 14px 0;'>"
                    f"<div style='font-size:12px;opacity:.72;font-weight:700;letter-spacing:.2px;text-transform:uppercase;margin-bottom:4px;'>{_esc(label)}</div>"
                    f"<div style='font-size:16px;line-height:1.72;'>{joined}</div>"
                    f"</div>"
                )
    if rows:
        st.markdown(f"### {title}")
        st.markdown(
            "<div style='border:1px solid rgba(255,255,255,0.08);background:rgba(255,255,255,0.03);"
            "border-radius:14px;padding:14px 16px;box-shadow:0 10px 24px rgba(0,0,0,0.18);margin-top:8px;line-height:1.5;'>"
            + "".join(rows)
            + "</div>",
            unsafe_allow_html=True,
        )


def _render_evidence_section(evidences: List[Any], limit: int = 10) -> None:
    normalized: List[Dict[str, str]] = []
    for item in evidences[:limit]:
        if isinstance(item, dict):
            normalized.append(
                {
                    "topico": _strip_html(item.get("topico") or item.get("ano") or ""),
                    "trecho": _strip_html(item.get("trecho") or item.get("citacao") or ""),
                    "interpretacao": _strip_html(item.get("interpretacao") or item.get("leitura") or ""),
                }
            )
        elif isinstance(item, str) and item.strip():
            normalized.append({"topico": "", "trecho": _strip_html(item), "interpretacao": ""})

    normalized = [item for item in normalized if item["trecho"] or item["interpretacao"]]
    if not normalized:
        return

    st.markdown("### Evidências")
    st.caption("Trechos priorizados por relevância material, diversidade temporal e utilidade analítica para a tese.")
    for item in normalized:
        head = item["topico"] or "Evidência documental"
        trecho_html = f"<div style='font-size:12px;opacity:.72;font-weight:700;letter-spacing:.2px;text-transform:uppercase;margin-bottom:4px;'>Trecho selecionado</div><div style='font-size:16px;line-height:1.72;'>{_esc(item['trecho'])}</div>" if item["trecho"] else ""
        leitura_html = f"<div style='font-size:12px;opacity:.72;font-weight:700;letter-spacing:.2px;text-transform:uppercase;margin:12px 0 4px 0;'>Leitura analítica</div><div style='font-size:16px;line-height:1.72;'>{_esc(item['interpretacao'])}</div>" if item["interpretacao"] else ""
        st.markdown(
            f"""
            <div style="border:1px solid rgba(255,255,255,0.08);background:rgba(255,255,255,0.025);
                        border-radius:12px;padding:12px 14px;margin:10px 0;line-height:1.5;">
                <div style="font-size:13px;opacity:0.82;margin-bottom:10px;font-weight:700;">{_esc(head)}</div>
                {trecho_html}
                {leitura_html}
            </div>
            """,
            unsafe_allow_html=True,
        )


def _render_strategy_detector(detector: Dict[str, Any]) -> None:
    if not detector:
        return

    summary = _strip_html(detector.get("summary"))
    years = detector.get("coverage_years") if isinstance(detector.get("coverage_years"), list) else []
    changes = detector.get("detected_changes") if isinstance(detector.get("detected_changes"), list) else []
    timeline = detector.get("yearly_timeline") if isinstance(detector.get("yearly_timeline"), list) else []
    n_events = _safe_int(detector.get("n_events"), 0)

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
        _render_section_list("Mudanças detectadas", [_strip_html(v) for v in changes], limit=6)

    if timeline:
        st.markdown("**Linha do Tempo Estratégica**")
        for item in timeline[:6]:
            if not isinstance(item, dict):
                continue
            year = _strip_html(item.get("year") or "—")
            summary_line = _strip_html(item.get("summary") or "")
            evidences = item.get("evidences") if isinstance(item.get("evidences"), list) else []
            extra = ""
            if evidences:
                extra = "<br/><span style='opacity:.90;font-size:15px;line-height:1.75;'>" + _esc(" | ".join([_strip_html(x) for x in evidences[:2] if _strip_html(x)])) + "</span>"
            st.markdown(
                f"""
                <div style="border:1px solid rgba(255,255,255,0.08);background:rgba(255,255,255,0.025);
                            border-radius:12px;padding:12px 14px;margin:8px 0;line-height:1.45;">
                    <div style="font-size:13px;opacity:0.82;margin-bottom:8px;font-weight:700;">{_esc(year)}</div>
                    <div style='font-size:20px;font-weight:800;line-height:1.55;margin-bottom:8px;'>{_esc(summary_line or 'Sem resumo temporal consolidado.')}</div>
                    {extra}
                </div>
                """,
                unsafe_allow_html=True,
            )


def _resolve_company_view(row: Any) -> Dict[str, Any]:
    result_obj = _as_result_obj(getattr(row, "result_json", None))

    tese = _pick_text(
        result_obj,
        "tese_sintese",
        "tese_final",
        "resumo",
        "tese",
    ) or _strip_html(getattr(row, "resumo", ""))

    evolucao = _pick_dict(result_obj, "evolucao_estrategica", "evolucao_temporal")
    consistencia = _pick_dict(result_obj, "consistencia_discurso", "consistencia_narrativa")
    execucao = _pick_dict(result_obj, "execucao_vs_promessa")
    qualidade_narrativa = _pick_dict(result_obj, "qualidade_narrativa")
    strategy_detector = _pick_dict(result_obj, "strategy_detector")

    leitura = _pick_text(result_obj, "leitura_direcionalidade", "direcionalidade")
    if not leitura:
        leitura = str(getattr(row, "perspectiva_compra", "") or "").strip().lower()

    riscos = _pick_list(result_obj, "riscos_identificados", "riscos")
    catalisadores = _pick_list(result_obj, "catalisadores", "gatilhos_futuros")
    monitorar = _pick_list(result_obj, "o_que_monitorar")
    mudancas = _pick_list(result_obj, "mudancas_estrategicas")
    pontos_chave = _pick_list(result_obj, "pontos_chave")
    contradicoes = _pick_list(consistencia, "contradicoes", "contradicoes_ou_ruidos")
    sinais_ruido = _pick_list(qualidade_narrativa, "sinais_de_ruido")
    evidencias = result_obj.get("evidencias") if isinstance(result_obj.get("evidencias"), list) else []
    consideracoes = _pick_text(result_obj, "consideracoes_llm")
    confianca = _safe_float(result_obj.get("confianca_analise"), 0.0)
    score_qualitativo = _safe_int(result_obj.get("score_qualitativo"), 0)

    return {
        "raw": result_obj,
        "tese": tese,
        "leitura": leitura,
        "evolucao": evolucao,
        "consistencia": consistencia,
        "execucao": execucao,
        "qualidade_narrativa": qualidade_narrativa,
        "strategy_detector": strategy_detector,
        "riscos": riscos,
        "catalisadores": catalisadores,
        "monitorar": monitorar,
        "mudancas": mudancas,
        "pontos_chave": pontos_chave,
        "contradicoes": contradicoes,
        "sinais_ruido": sinais_ruido,
        "evidencias": evidencias,
        "consideracoes": consideracoes,
        "confianca": confianca,
        "score_qualitativo": score_qualitativo,
    }


def _portfolio_context_line(row: Any, company: Dict[str, Any]) -> str:
    tese = company["tese"] or "sem tese consolidada"
    historico = _pick_text(company["evolucao"], "historico")
    atual = _pick_text(company["evolucao"], "fase_atual")
    execucao = _pick_text(company["execucao"], "analise")
    riscos = "; ".join(company["riscos"][:3])
    catalisadores = "; ".join(company["catalisadores"][:3])
    score = _fmt_score(company.get("score_qualitativo", 0))
    detector = company.get("strategy_detector") or {}
    years = detector.get("coverage_years") if isinstance(detector.get("coverage_years"), list) else []
    years_txt = ",".join([str(y) for y in years[:4]]) if years else ""
    return (
        f"- {row.ticker}: perspectiva={row.perspectiva_compra}; score={score}; tese={tese}; "
        f"historico={historico}; fase_atual={atual}; execucao={execucao}; "
        f"riscos={riscos}; catalisadores={catalisadores}; cobertura_temporal={years_txt}"
    )




def _build_allocation_heuristic(df_latest: pd.DataFrame, company_views: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in df_latest.itertuples(index=False):
        company = company_views.get(row.ticker) or {}
        score = max(15, _safe_int(company.get("score_qualitativo"), 0)) / 100.0
        conf = max(0.35, _safe_float(company.get("confianca"), 0.0))
        p = str(getattr(row, "perspectiva_compra", "") or "").strip().lower()
        factor = 1.15 if p == "forte" else 1.0 if p == "moderada" else 0.8 if p == "fraca" else 0.9
        raw = score * (0.65 + 0.35 * conf) * factor
        rows.append({"ticker": row.ticker, "raw": max(raw, 0.01)})
    total = sum(r["raw"] for r in rows) or 1.0
    for r in rows:
        r["pct"] = round((r["raw"] / total) * 100.0, 1)
    diff = round(100.0 - sum(r["pct"] for r in rows), 1)
    if rows and abs(diff) >= 0.1:
        rows[0]["pct"] = round(rows[0]["pct"] + diff, 1)
    rows.sort(key=lambda x: x["pct"], reverse=True)
    return rows


def _try_parse_json_loose(text_value: Optional[str]) -> Dict[str, Any]:
    if not text_value:
        return {}
    txt = str(text_value).strip()
    txt = re.sub(r"^```(?:json)?\s*", "", txt, flags=re.IGNORECASE).strip()
    txt = re.sub(r"\s*```$", "", txt).strip()
    try:
        return json.loads(txt)
    except Exception:
        pass
    m = re.search(r"\{.*\}", txt, flags=re.DOTALL)
    if not m:
        return {}
    try:
        return json.loads(m.group(0))
    except Exception:
        return {}


def _render_allocation_section(allocation_rows: List[Dict[str, Any]]) -> None:
    if not allocation_rows:
        return
    st.markdown("## 💼 Alocação sugerida")
    st.caption("Heurística guiada pela leitura qualitativa: score, confiança e perspectiva relativa de cada ativo dentro do portfólio analisado.")
    rows = [allocation_rows[i:i+4] for i in range(0, len(allocation_rows), 4)]
    for row_group in rows:
        cols = st.columns(len(row_group))
        for idx, row in enumerate(row_group):
            cols[idx].markdown(
                f"""
                <div style="border:1px solid rgba(255,255,255,0.08);background:rgba(255,255,255,0.025);border-radius:12px;padding:12px 14px;margin-bottom:8px;">
                  <div style="font-size:12px;opacity:.72;margin-bottom:4px;">{_esc(row['ticker'])}</div>
                  <div style="font-size:26px;font-weight:900;">{row['pct']:.1f}%</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

def render_patch6_report(
    tickers: List[str],
    period_ref: str,
    llm_factory: Optional[Any] = None,
    show_company_details: bool = True,
) -> None:
    #st.markdown("# 📘 Relatório de Análise de Portfólio (Patch6)")
    #st.caption("Consolidação qualitativa com base em evidências do RAG. Formato institucional (research).")
    st.markdown(
        """
        <style>
        .p6-card{
          border:1px solid rgba(255,255,255,0.08);
          background:rgba(255,255,255,0.03);
          border-radius:16px;
          padding:16px 18px;
          box-shadow:0 10px 24px rgba(0,0,0,0.25);
          min-height:110px;
        }
        .p6-card-label{
          font-size:12px;
          opacity:0.7;
          margin-bottom:6px;
          letter-spacing:0.3px;
        }
        .p6-card-value{
          font-size:28px;
          font-weight:900;
          margin-bottom:6px;
        }
        .p6-card-extra{
          font-size:12px;
          opacity:0.65;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    df_latest = _load_latest_runs(tickers=tickers, period_ref=period_ref)
    if df_latest.empty:
        st.warning(
            "Não há execuções salvas em patch6_runs para este period_ref e tickers do portfólio. "
            "Rode a LLM e salve os resultados primeiro."
        )
        return

    stats = _compute_stats(df_latest)
    coverage_total = max(len([t for t in tickers if str(t).strip()]), 1)
    qualidade = stats.label_qualidade()
    perspectiva = stats.label_perspectiva()
    cobertura = f"{stats.total}/{coverage_total}"

    company_views: Dict[str, Dict[str, Any]] = {}
    confidence_values: List[float] = []
    score_values: List[int] = []
    temporal_covered = 0

    for row in df_latest.itertuples(index=False):
        view = _resolve_company_view(row)
        company_views[row.ticker] = view
        if view["confianca"] > 0:
            confidence_values.append(view["confianca"])
        if view["score_qualitativo"] > 0:
            score_values.append(view["score_qualitativo"])
        detector = view.get("strategy_detector") or {}
        if isinstance(detector.get("coverage_years"), list) and detector.get("coverage_years"):
            temporal_covered += 1

    confianca_media = sum(confidence_values) / len(confidence_values) if confidence_values else 0.0
    score_medio = round(sum(score_values) / len(score_values)) if score_values else 0

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.markdown(
        f"""
        <div class="p6-card">
          <div class="p6-card-label">Qualidade (heurística)</div>
          <div class="p6-card-value">{qualidade}</div>
          <div class="p6-card-extra">Heurística agregada a partir dos sinais do RAG.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    col2.markdown(
        f"""
        <div class="p6-card">
          <div class="p6-card-label">Perspectiva 12m</div>
          <div class="p6-card-value">{perspectiva}</div>
          <div class="p6-card-extra">Direcionalidade consolidada para os próximos 12 meses.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    col3.markdown(
        f"""
        <div class="p6-card">
          <div class="p6-card-label">Cobertura</div>
          <div class="p6-card-value">{cobertura}</div>
          <div class="p6-card-extra">Ativos com evidências suficientes no período analisado.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    col4.markdown(
        f"""
        <div class="p6-card">
          <div class="p6-card-label">Confiança média</div>
          <div class="p6-card-value">{_fmt_confidence(confianca_media)}</div>
          <div class="p6-card-extra">Média do campo confianca_analise nas leituras individuais.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    col5.markdown(
        f"""
        <div class="p6-card">
          <div class="p6-card-label">Score qualitativo médio</div>
          <div class="p6-card-value">{_fmt_score(score_medio)}</div>
          <div class="p6-card-extra">Média do score_qualitativo salvo pela LLM.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.caption(
        "🛈 Como a qualidade é estimada: combinação de cobertura do portfólio, perspectiva 12m agregada e distribuição de sinais. "
        "A confiança média depende do campo confianca_analise salvo pela LLM. "
        f"A cobertura temporal do detector estratégico está presente em {temporal_covered} ativo(s)."
    )

    contexto_lines = []
    for row in df_latest.itertuples(index=False):
        contexto_lines.append(_portfolio_context_line(row, company_views[row.ticker]))
    contexto_portfolio = "\n".join(contexto_lines)

    st.markdown("## 🧠 Resumo Executivo")

    llm_client = None
    if llm_factory is not None:
        try:
            llm_client = llm_factory.get_llm_client()
        except Exception:
            llm_client = None

    prompt_exec = f"""
Você é um analista buy-side disciplinado, com foco em qualidade do negócio, governança, previsibilidade, alocação de capital e margem de segurança.
Escreva um resumo executivo profissional do portfólio com base SOMENTE nos bullets abaixo.
Estruture em 4 blocos curtos:
1) Leitura geral do portfólio
2) Principais oportunidades realmente diferenciadas
3) Principais riscos estruturais
4) Perspectiva base para 12 meses

Regras:
- linguagem institucional, objetiva e analítica
- não invente fatos não citados
- não floreie; interprete os fatos como um investidor profissional
- use no máximo 14 linhas
- não use tabelas

BULLETS:
{contexto_portfolio}
"""

    llm_text = _safe_call_llm(llm_client, prompt_exec)
    if llm_text:
        st.write(llm_text)
    else:
        st.write(
            f"O portfólio apresenta leitura **{stats.label_perspectiva().lower()}** para 12 meses, com distribuição de perspectivas: "
            f"**{stats.fortes}** forte, **{stats.moderadas}** moderada e **{stats.fracas}** fraca. "
            f"A cobertura atual é de **{stats.total}** ativos, com confiança média de **{_fmt_confidence(confianca_media)}** "
            f"e score qualitativo médio de **{_fmt_score(score_medio)}**. "
            "Abaixo, os relatórios por empresa mostram evolução estratégica, execução, riscos, mudança estratégica e evidências documentais."
        )

    if show_company_details:
        st.markdown("## 🏢 Relatórios por Empresa")
        for row in df_latest.itertuples(index=False):
            tk = row.ticker
            p = str(row.perspectiva_compra or "").strip().lower()
            badge = _badge((p or "—").upper(), _tone_from_perspectiva(p))
            company = company_views[tk]

            with st.expander(f"{tk}", expanded=False):
                st.markdown(f"### {tk}  {badge}", unsafe_allow_html=True)
                st.caption(
                    f"Período analisado: {row.period_ref} • Atualizado em: {row.created_at}"
                    + (f" • Confiança: {_fmt_confidence(company['confianca'])}" if company["confianca"] > 0 else "")
                    + (f" • Score: {_fmt_score(company['score_qualitativo'])}" if company["score_qualitativo"] > 0 else "")
                )

                metric_items = [
                    ("Score qualitativo", _explicar_score(company)),
                    ("Confiança", _explicar_confianca(company)),
                ]

                detector = company.get("strategy_detector") or {}
                years = detector.get("coverage_years") if isinstance(detector.get("coverage_years"), list) else []
                if years:
                    metric_items.append(("Cobertura temporal", ", ".join([str(y) for y in years[:4]])))

                _render_metric_cards(metric_items, columns_per_row=2)
                _render_score_explanations(company)

                _render_section_text("Tese (síntese)", company["tese"] or "—")

                if company["leitura"]:
                    _render_section_text("Leitura / Direcionalidade", company["leitura"])
                elif p == "forte":
                    _render_section_text(
                        "Leitura / Direcionalidade",
                        "Viés construtivo, com sinais qualitativos favoráveis no recorte analisado. Mantém assimetria positiva, com monitoramento de riscos.",
                    )
                elif p == "moderada":
                    _render_section_text(
                        "Leitura / Direcionalidade",
                        "Leitura equilibrada, com pontos positivos e ressalvas. Indica acompanhamento de gatilhos de execução, guidance e alocação de capital.",
                    )
                elif p == "fraca":
                    _render_section_text(
                        "Leitura / Direcionalidade",
                        "Leitura cautelosa, com sinais qualitativos desfavoráveis no recorte analisado. Recomenda postura defensiva e foco em mitigação de risco.",
                    )

                _render_key_value_section(
                    "Evolução Estratégica",
                    company["evolucao"],
                    [
                        ("historico", "Histórico"),
                        ("fase_atual", "Fase atual"),
                        ("tendencia", "Tendência"),
                    ],
                )

                _render_strategy_detector(company["strategy_detector"])

                _render_key_value_section(
                    "Consistência do Discurso",
                    company["consistencia"],
                    [
                        ("analise", "Análise"),
                        ("grau_consistencia", "Grau"),
                        ("contradicoes", "Contradições"),
                        ("sinais_positivos", "Sinais positivos"),
                    ],
                )

                _render_key_value_section(
                    "Execução vs Promessa",
                    company["execucao"],
                    [
                        ("analise", "Análise"),
                        ("avaliacao_execucao", "Avaliação"),
                        ("entregas_confirmadas", "Entregas confirmadas"),
                        ("entregas_pendentes_ou_incertas", "Entregas pendentes ou incertas"),
                        ("entregas_pendentes", "Entregas pendentes"),
                    ],
                )

                _render_section_list("Mudanças Estratégicas", company["mudancas"], limit=6)
                _render_section_list("Pontos-chave", company["pontos_chave"], limit=8)
                _render_section_list("Catalisadores", company["catalisadores"], limit=6)
                _render_section_list("Riscos", company["riscos"], limit=6)
                _render_section_list("O que monitorar", company["monitorar"], limit=6)

                ruido_total = company["contradicoes"] + company["sinais_ruido"]
                _render_section_list("Ruídos e Contradições", ruido_total, limit=6)

                _render_key_value_section(
                    "Qualidade Narrativa",
                    company["qualidade_narrativa"],
                    [
                        ("clareza", "Clareza"),
                        ("coerencia", "Coerência"),
                        ("sinais_de_ruido", "Sinais de ruído"),
                    ],
                )

                _render_evidence_section(company["evidencias"], limit=14)
                _render_section_text("Considerações da LLM", company["consideracoes"])

        allocation_base = _build_allocation_heuristic(df_latest, company_views)
    allocation_lines = "\n".join([f"- {x['ticker']}: {x['pct']:.1f}%" for x in allocation_base])

    st.markdown("## 🔎 Conclusão Estratégica")
    prompt_conc = f"""
Você é um investidor fundamentalista disciplinado, com foco em qualidade de negócio, previsibilidade, governança, uso racional do caixa e alocação de capital.
Escreva uma conclusão estratégica para o portfólio usando SOMENTE os bullets abaixo.

Entregue APENAS JSON válido no formato:
{{
  "conclusao": "texto em até 12 linhas, analítico e opinativo, sem floreio",
  "alocacao_sugerida": [
    {{"ticker": "AAA1", "pct": 12.5}}
  ]
}}

Regras:
- a conclusão deve soar como parecer profissional, não como marketing
- destaque coerência do conjunto, riscos estruturais, qualidade de gestão e disciplina de alocação de capital
- use a base de pesos abaixo como âncora quantitativa; você pode fazer pequenos ajustes, mas a soma final deve ser 100%
- não use linguagem vaga

Base de pesos:
{allocation_lines}

BULLETS:
{contexto_portfolio}
"""

    llm_conc = _safe_call_llm(llm_client, prompt_conc)
    parsed_conc = _try_parse_json_loose(llm_conc)
    conclusao_text = _strip_html(parsed_conc.get("conclusao") or "")
    alloc_from_llm = parsed_conc.get("alocacao_sugerida") if isinstance(parsed_conc.get("alocacao_sugerida"), list) else []

    if conclusao_text:
        st.write(conclusao_text)
    elif llm_conc and not parsed_conc:
        st.write(llm_conc)
    else:
        st.write(
            "A carteira mostra assimetria mais favorável nos nomes com melhor combinação entre score qualitativo, confiança, coerência estratégica e disciplina de capital. "
            "Os principais pontos de atenção permanecem na execução, no endividamento, na qualidade das mudanças estratégicas e na persistência dos catalisadores documentais. "
            "O acompanhamento deve priorizar entregas concretas, narrativa consistente e capacidade de transformar planos em retorno ajustado ao risco."
        )

    final_allocation: List[Dict[str, Any]] = []
    valid_llm = True
    if alloc_from_llm:
        base_map = {str(x["ticker"]).upper(): float(x["pct"]) for x in allocation_base}
        acc_map: Dict[str, float] = {}
        for item in alloc_from_llm:
            if not isinstance(item, dict):
                continue
            tk = str(item.get("ticker") or "").strip().upper()
            if not tk or tk not in company_views:
                continue
            try:
                pct = float(item.get("pct"))
            except Exception:
                continue
            if pct <= 0:
                continue
            acc_map[tk] = pct

        if acc_map:
            # completa tickers faltantes com a heurística base, em vez de descartar a resposta inteira
            for tk, pct in base_map.items():
                if tk not in acc_map:
                    acc_map[tk] = pct

            total_pct = sum(acc_map.values())
            if total_pct > 0:
                final_allocation = [
                    {"ticker": tk, "pct": round(pct / total_pct * 100.0, 1)}
                    for tk, pct in acc_map.items()
                ]
                diff = round(100.0 - sum(x["pct"] for x in final_allocation), 1)
                if final_allocation and abs(diff) >= 0.1:
                    final_allocation[0]["pct"] = round(final_allocation[0]["pct"] + diff, 1)
                final_allocation.sort(key=lambda x: x["pct"], reverse=True)
            else:
                valid_llm = False
        else:
            valid_llm = False
    else:
        valid_llm = False

    if not valid_llm:
        final_allocation = allocation_base

    _render_allocation_section(final_allocation)
