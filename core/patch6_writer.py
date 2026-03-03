
# core/patch6_writer.py
from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping


def _safe_json_load(s: str) -> Dict[str, Any]:
    try:
        return json.loads(s)
    except Exception:
        return {}


def _topic_prompt(ticker: str, topic: str, context: str) -> str:
    return f"""Você é analista sell-side. Extraia APENAS fatos e inferências cautelosas do contexto.
Ticker: {ticker}
Tópico: {topic}

Contexto (trechos):
{context}

Responda em JSON com:
{{
  "pontos": ["...","..."],
  "observacoes": ["..."],
  "lacunas": ["..."]
}}
""".strip()


def _final_prompt(ticker: str, topic_summaries: Mapping[str, Any]) -> str:
    return f"""Consolide um relatório final (institucional) para {ticker}, usando os resumos por tópico.
Regras:
- Não invente fatos. Se faltar, declare lacuna.
- Seja específico: o que monitorar, riscos dominantes, catalisadores prováveis.
- Saída OBRIGATORIAMENTE em JSON.

Entrada (resumos):
{json.dumps(topic_summaries, ensure_ascii=False)}

Saída JSON no formato:
{{
  "perspectiva_compra": "forte|moderada|fraca",
  "resumo": "1 parágrafo objetivo",
  "pontos_chave": ["..."],
  "riscos": ["..."],
  "catalisadores": ["..."],
  "o_que_monitorar": ["..."],
  "consideracoes_llm": "limitações das evidências",
  "topicos": {{
      "Tese e drivers": {{...}},
      "Riscos e pontos de atenção": {{...}}
  }}
}}
""".strip()


def build_rich_report_json(
    *,
    ticker: str,
    llm_client,
    chunks_by_topic: Dict[str, List[str]],
    per_topic_chars: int = 4500,
) -> Dict[str, Any]:
    """Executa MAP/REDUCE: 1 chamada por tópico + 1 consolidação final.

    chunks_by_topic: {topic: [chunk_text, ...]}
    llm_client.chat(prompt: str) -> str
    """
    topic_summaries: Dict[str, Any] = {}

    for topic, chunks in chunks_by_topic.items():
        # Deep mode: concatena trechos, mas limita tamanho para evitar prompt explode.
        context = "\n\n---\n\n".join([str(c)[:1500] for c in (chunks or [])])[:per_topic_chars]
        raw = llm_client.chat(_topic_prompt(ticker, topic, context))
        parsed = _safe_json_load(raw)
        if not parsed:
            parsed = {"pontos": [], "observacoes": [], "lacunas": ["JSON inválido ou vazio do modelo"]}
        topic_summaries[topic] = parsed

    raw_final = llm_client.chat(_final_prompt(ticker, topic_summaries))
    final = _safe_json_load(raw_final)
    if not final:
        final = {
            "perspectiva_compra": "moderada",
            "resumo": "",
            "pontos_chave": [],
            "riscos": [],
            "catalisadores": [],
            "o_que_monitorar": [],
            "consideracoes_llm": "Modelo retornou JSON inválido.",
            "topicos": topic_summaries,
        }
    # garante campos essenciais
    final.setdefault("topicos", topic_summaries)
    if "resumo" not in final:
        # cria resumo curto de fallback
        pontos = final.get("pontos_chave") or []
        final["resumo"] = "; ".join([str(x) for x in pontos[:3]])
    return final
