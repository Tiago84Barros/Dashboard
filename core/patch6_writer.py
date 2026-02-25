# core/patch6_writer.py
from __future__ import annotations
from typing import Dict, List
import json

def _safe_json_load(s: str) -> dict:
    try:
        return json.loads(s)
    except Exception:
        return {}

def _topic_prompt(ticker: str, topic: str, context: str) -> str:
    return f"""
Você é analista sell-side. Extraia APENAS fatos e inferências cautelosas do contexto.
Ticker: {ticker}
Tópico: {topic}

Contexto (trechos):
{context}

Responda em JSON com:
{{
  "pontos": ["...","..."],
  "observacoes": ["..."],
  "lacunas": ["..."]  // diga o que faltou no material
}}
""".strip()

def _final_prompt(ticker: str, topic_summaries: Dict[str, dict]) -> str:
    return f"""
Consolide um relatório final (institucional) para {ticker}, usando os resumos por tópico.
Regras:
- Não invente fatos. Se faltar, declare lacuna.
- Traga direção para investidor em termos de: o que monitorar, quais riscos dominam, quais catalisadores.
- Saída OBRIGATORIAMENTE em JSON.

Entrada (resumos):
{json.dumps(topic_summaries, ensure_ascii=False)}

Saída JSON no formato:
{{
  "perspectiva_compra": "forte|moderada|fraca",
  "pontos_chave": ["..."],
  "riscos": ["..."],
  "catalisadores": ["..."],
  "o_que_monitorar": ["..."],
  "consideracoes_llm": "texto curto sobre limites das evidências",
  "topicos": {{ "resultado": {{...}}, "divida": {{...}} }}
}}
""".strip()

def build_rich_report_json(
    *,
    ticker: str,
    llm_client,
    chunks_by_topic: Dict[str, List[str]],
    per_topic_chars: int = 3500,
) -> dict:
    # MAP: um JSON por tópico
    topic_summaries: Dict[str, dict] = {}
    for topic, chunks in chunks_by_topic.items():
        context = "\n\n---\n\n".join([c[:1200] for c in chunks])[:per_topic_chars]
        raw = llm_client.chat(_topic_prompt(ticker, topic, context))
        topic_summaries[topic] = _safe_json_load(raw) or {"pontos": [], "observacoes": [], "lacunas": ["JSON inválido"]}

    # REDUCE: consolida tudo
    raw_final = llm_client.chat(_final_prompt(ticker, topic_summaries))
    final = _safe_json_load(raw_final) or {}

    # guarda auditoria no próprio resultado
    final.setdefault("topicos", topic_summaries)
    return final
