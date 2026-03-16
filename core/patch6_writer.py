
# core/patch6_writer.py
# Version: patch6_rich_v3
# Improvements:
# - Strategic change detection
# - Qualitative scoring
# - Temporal awareness hints
# - Stronger prompts for richer analysis
# - Compatible with existing dashboard schema

from __future__ import annotations

from typing import Any, Dict, List
from datetime import datetime
import json
import re


def _safe_json_load(s: str) -> dict:
    if not s:
        return {}

    text = s.strip()

    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else {}
    except Exception:
        pass

    fenced = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.IGNORECASE | re.DOTALL).strip()

    try:
        data = json.loads(fenced)
        return data if isinstance(data, dict) else {}
    except Exception:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidate = text[start:end+1]
        try:
            data = json.loads(candidate)
            return data if isinstance(data, dict) else {}
        except Exception:
            pass

    return {}


def _as_list(v: Any) -> List[Any]:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        return [v]
    return []


def _as_str(v: Any, default="") -> str:
    if v is None:
        return default
    if isinstance(v, str):
        return v.strip()
    return str(v)


def _clip(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[:limit]


def _build_context(chunks: List[str], limit: int) -> str:
    out = []
    total = 0

    for i, c in enumerate(chunks, 1):
        piece = _clip(c, 1000)
        block = f"[CHUNK {i}]\n{piece}"

        if total + len(block) > limit:
            break

        out.append(block)
        total += len(block)

    return "\n\n---\n\n".join(out)


def _topic_prompt(ticker: str, topic: str, context: str) -> str:
    return f"""
Você é um analista fundamentalista institucional.

Analise o material documental sobre a empresa {ticker}.

Objetivo:
Produzir análise estratégica rica, identificando:

- evolução temporal
- consistência narrativa
- riscos recorrentes
- mudanças estratégicas
- catalisadores
- execução versus promessa

Importante:
Não invente fatos.
Baseie-se apenas no material.

Contexto:
{context}

Responda APENAS em JSON:

{{
 "resumo_topico": "análise detalhada",
 "fatos_relevantes": [],
 "mudancas_detectadas": [],
 "riscos": [],
 "catalisadores": [],
 "evidencias": [
   {{
     "trecho": "",
     "interpretacao": ""
   }}
 ]
}}
"""


def _final_prompt(ticker: str, topic_data: Dict[str, dict]) -> str:
    return f"""
Você é um analista sell-side sênior.

Com base nos resumos abaixo, produza uma análise institucional consolidada da empresa {ticker}.

Analise:

- evolução estratégica ao longo do tempo
- consistência da narrativa da gestão
- execução versus promessa
- riscos recorrentes
- mudanças estratégicas
- catalisadores de valor

Entrada:
{json.dumps(topic_data, ensure_ascii=False)}

Responda apenas JSON:

{{
 "tese_sintese": "",
 "leitura_direcionalidade": "",
 "score_qualitativo": 0,
 "confianca_analise": 0.0,
 "evolucao_estrategica": {{
   "historico": "",
   "fase_atual": "",
   "tendencia": ""
 }},
 "mudancas_estrategicas":[],
 "pontos_chave":[],
 "riscos":[],
 "catalisadores":[],
 "evidencias":[]
}}
"""


def _normalize_topic(data: dict) -> dict:
    if not isinstance(data, dict):
        data = {}

    return {
        "resumo_topico": _as_str(data.get("resumo_topico")),
        "fatos_relevantes": _as_list(data.get("fatos_relevantes")),
        "mudancas_detectadas": _as_list(data.get("mudancas_detectadas")),
        "riscos": _as_list(data.get("riscos")),
        "catalisadores": _as_list(data.get("catalisadores")),
        "evidencias": _as_list(data.get("evidencias"))
    }


def _normalize_final(data: dict, ticker: str, topics: Dict[str, dict]) -> dict:
    if not isinstance(data, dict):
        data = {}

    out = {}

    out["ticker"] = ticker
    out["generated_at"] = datetime.utcnow().isoformat()

    out["tese_sintese"] = _as_str(data.get("tese_sintese"))
    out["leitura_direcionalidade"] = _as_str(data.get("leitura_direcionalidade"), "equilibrada")

    try:
        out["score_qualitativo"] = int(data.get("score_qualitativo", 50))
    except:
        out["score_qualitativo"] = 50

    try:
        out["confianca_analise"] = float(data.get("confianca_analise", 0.5))
    except:
        out["confianca_analise"] = 0.5

    out["evolucao_estrategica"] = data.get("evolucao_estrategica", {})
    out["mudancas_estrategicas"] = _as_list(data.get("mudancas_estrategicas"))
    out["pontos_chave"] = _as_list(data.get("pontos_chave"))
    out["riscos"] = _as_list(data.get("riscos"))
    out["catalisadores"] = _as_list(data.get("catalisadores"))
    out["evidencias"] = _as_list(data.get("evidencias"))

    out["topicos"] = topics

    return out


def build_rich_report_json(
    ticker: str,
    llm_client,
    chunks_by_topic: Dict[str, List[str]],
    per_topic_chars: int = 3500
) -> dict:

    topic_results = {}

    for topic, chunks in chunks_by_topic.items():

        context = _build_context(chunks, per_topic_chars)

        if not context:
            topic_results[topic] = {}
            continue

        prompt = _topic_prompt(ticker, topic, context)

        raw = llm_client.chat(prompt)

        parsed = _safe_json_load(raw)

        topic_results[topic] = _normalize_topic(parsed)

    final_prompt = _final_prompt(ticker, topic_results)

    raw_final = llm_client.chat(final_prompt)

    parsed_final = _safe_json_load(raw_final)

    final = _normalize_final(parsed_final, ticker, topic_results)

    return final
