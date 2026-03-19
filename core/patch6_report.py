
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
                temperature=0.0,
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
            line-height:1.5;">
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
    if not _strip_html(text_value):
        return
    st.markdown(f"**{title}**")
    st.markdown(_box_html(text_value), unsafe_allow_html=True)


def _render_section_list(title: str, values: List[str], limit: Optional[int] = None) -> None:
    clean_values = [_strip_html(v) for v in values if _strip_html(v)]
    if limit is not None:
        clean_values = clean_values[:limit]
    if not clean_values:
        return
    st.markdown(f"**{title}**")
    for item in clean_values:
        st.markdown(f"<div style='font-size:15px;line-height:1.6;margin:4px 0 4px 0;'>• {_esc(item)}</div>", unsafe_allow_html=True)


def _render_key_value_section(title: str, data: Dict[str, Any], label_map: List[tuple[str, str]]) -> None:
    if not data:
        return
    rendered = False
    blocks: List[str] = []
    for key, label in label_map:
        value = data.get(key)
        if isinstance(value, str) and _strip_html(value):
            rendered = True
            blocks.append(f"**{label}:** {_esc(value)}")
        elif isinstance(value, list):
            clean_values = [_strip_html(v) for v in value if _strip_html(v)]
            if clean_values:
                rendered = True
                blocks.append(f"**{label}:** " + " • ".join(_esc(v) for v in clean_values))
    if rendered:
        st.markdown(f"**{title}**")
        st.markdown(
            "<div style='border:1px solid rgba(255,255,255,0.08);background:rgba(255,255,255,0.03);"
            "border-radius:14px;padding:14px 16px;box-shadow:0 10px 24px rgba(0,0,0,0.18);margin-top:8px;line-height:1.5;'>"
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
        _render_section_list("Mudanças detectadas", [_strip_html(v) for v in changes], limit=10)

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
                extra = "<br/><span style='opacity:.75;font-size:12px;'>" + _esc(" | ".join([_strip_html(x) for x in evidences[:2] if _strip_html(x)])) + "</span>"
            st.markdown(
                f"""
                <div style="border:1px solid rgba(255,255,255,0.08);background:rgba(255,255,255,0.025);
                            border-radius:12px;padding:12px 14px;margin:8px 0;line-height:1.45;">
                    <div style="font-size:13px;opacity:0.80;margin-bottom:6px;font-weight:700;letter-spacing:.2px;">{_esc(year)}</div>
                    <div style="font-size:16px;line-height:1.55;font-weight:700;">{_esc(summary_line or 'Sem resumo temporal consolidado.')}</div>
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




def _allocation_base_from_company(company: Dict[str, Any], perspectiva: str) -> float:
    score = _safe_int(company.get('score_qualitativo'), 0)
    conf = _safe_float(company.get('confianca'), 0.0)
    evid = len(company.get('evidencias') or [])
    execucao = _pick_text(company.get('execucao') or {}, 'avaliacao_execucao').lower()
    mult = 1.0
    p = (perspectiva or '').strip().lower()
    if p == 'forte':
        mult *= 1.20
    elif p == 'moderada':
        mult *= 1.00
    elif p == 'fraca':
        mult *= 0.72
    if 'forte' in execucao:
        mult *= 1.08
    elif 'fraca' in execucao or 'inconsistente' in execucao:
        mult *= 0.84
    base = max(0.5, (score / 100.0) * (0.65 + conf) * mult * (1.0 + min(evid, 14) / 40.0))
    return base


def _normalize_allocations(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    total = sum(max(0.0, float(r.get('raw_weight', 0.0))) for r in rows)
    if total <= 0:
        n = max(1, len(rows))
        for r in rows:
            r['allocation_pct'] = round(100.0 / n, 2)
        return rows
    acc = 0.0
    for i, r in enumerate(rows):
        pct = round((max(0.0, float(r.get('raw_weight', 0.0))) / total) * 100.0, 2)
        r['allocation_pct'] = pct
        acc += pct
    if rows:
        rows[-1]['allocation_pct'] = round(rows[-1]['allocation_pct'] + (100.0 - acc), 2)
    return rows


def _render_allocation_section(df_latest: pd.DataFrame, company_views: Dict[str, Dict[str, Any]], llm_client: Any) -> None:
    rows: List[Dict[str, Any]] = []
    for row in df_latest.itertuples(index=False):
        company = company_views[row.ticker]
        rows.append({
            'ticker': row.ticker,
            'perspectiva': str(row.perspectiva_compra or '').strip().lower(),
            'raw_weight': _allocation_base_from_company(company, str(row.perspectiva_compra or '')),
            'score': _safe_int(company.get('score_qualitativo'), 0),
            'confianca': _safe_float(company.get('confianca'), 0.0),
        })
    rows = sorted(_normalize_allocations(rows), key=lambda x: (-x['allocation_pct'], x['ticker']))

    st.markdown('## 💼 Alocação Sugerida')
    st.caption('Distribuição percentual heurística entre todos os ativos cobertos no portfólio. Soma total = 100%.')
    cols_per_row = 4
    for i in range(0, len(rows), cols_per_row):
        row_items = rows[i:i+cols_per_row]
        cols = st.columns(len(row_items))
        for col, item in zip(cols, row_items):
            col.markdown(
                f"""
                <div class="p6-card">
                  <div class="p6-card-label">{_esc(item['ticker'])}</div>
                  <div class="p6-card-value" style="font-size:24px">{item['allocation_pct']:.2f}%</div>
                  <div class="p6-card-extra">{_esc((item['perspectiva'] or '—').upper())} • Score {_fmt_score(item['score'])} • Conf. {_fmt_confidence(item['confianca'])}</div>
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
Você é um analista sell-side (research). Escreva um resumo executivo profissional do portfólio com base SOMENTE nos bullets abaixo.
Estruture em 4 blocos curtos:
1) Leitura geral do portfólio
2) Principais oportunidades
3) Principais riscos/alertas
4) Perspectiva base para 12 meses

Regras:
- linguagem institucional e objetiva
- não invente fatos não citados
- use no máximo 12 linhas
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

    _render_allocation_section(df_latest, company_views, llm_client)

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

                _render_evidence_section(company["evidencias"], limit=10)
                _render_section_text("Considerações da LLM", company["consideracoes"])

    st.markdown("## 🔎 Conclusão Estratégica")
    prompt_conc = f"""
Escreva uma conclusão estratégica (research) para o portfólio, em até 10 linhas, com foco em:
- coerência do conjunto do portfólio
- principais alavancas para melhora ou deterioração
- recomendação de acompanhamento nos próximos trimestres

Use SOMENTE os bullets abaixo.

BULLETS:
{contexto_portfolio}
"""

    llm_conc = _safe_call_llm(llm_client, prompt_conc)
    if llm_conc:
        st.write(llm_conc)
    else:
        st.write(
            "A carteira deve ser acompanhada por gatilhos de execução, evolução da narrativa corporativa, score qualitativo, "
            "mudanças estratégicas detectadas e sinais de alocação de capital. Reforce o monitoramento de resultados trimestrais, "
            "consistência entre discurso e entrega, dívida/custo financeiro e manutenção dos catalisadores já visíveis nas evidências do RAG."
        )
