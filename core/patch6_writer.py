# core/patch6_writer.py
from __future__ import annotations

from typing import Any, Dict, List
from datetime import datetime
import json
import re


def _safe_json_load(s: str) -> dict:
    """
    Tenta carregar JSON de forma resiliente.
    Aceita:
    - JSON puro
    - JSON dentro de bloco ```json ... ```
    - Texto com JSON embutido
    """
    if not s:
        return {}

    text = s.strip()

    # 1) tentativa direta
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else {}
    except Exception:
        pass

    # 2) remover cercas markdown
    fenced = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.IGNORECASE | re.DOTALL).strip()
    try:
        data = json.loads(fenced)
        return data if isinstance(data, dict) else {}
    except Exception:
        pass

    # 3) extrair primeiro objeto JSON plausível
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidate = text[start : end + 1]
        try:
            data = json.loads(candidate)
            return data if isinstance(data, dict) else {}
        except Exception:
            pass

    return {}


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _as_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _clip_text(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _build_topic_context(chunks: List[str], per_topic_chars: int) -> str:
    """
    Consolida chunks por tópico com separadores claros.
    """
    parts: List[str] = []
    total = 0

    for idx, chunk in enumerate(chunks, start=1):
        piece = _clip_text(_as_str(chunk), 1200)
        if not piece:
            continue

        block = f"[CHUNK {idx}]\n{piece}"
        if total + len(block) > per_topic_chars:
            remaining = per_topic_chars - total
            if remaining > 80:
                parts.append(_clip_text(block, remaining))
            break

        parts.append(block)
        total += len(block) + 6

    return "\n\n---\n\n".join(parts).strip()


def _topic_prompt(ticker: str, topic: str, context: str) -> str:
    return f"""
Você é um analista fundamentalista institucional com postura conservadora na inferência.

Sua tarefa é analisar APENAS o material abaixo sobre a empresa {ticker}, no tópico "{topic}".

Objetivo:
- extrair fatos relevantes;
- identificar evolução temporal quando houver;
- apontar consistência ou mudança de narrativa;
- listar riscos, gatilhos e lacunas;
- usar linguagem analítica, sem floreio;
- não inventar fatos;
- se faltar base documental, declarar isso explicitamente.

Regras obrigatórias:
1. Trabalhe somente com o contexto fornecido.
2. Não use conhecimento externo.
3. Se a evidência for insuficiente, diga isso.
4. Diferencie fato observado de inferência cautelosa.
5. Sempre que possível, destaque se há mudança ao longo do tempo.
6. Responda SOMENTE em JSON válido.
7. Não inclua markdown, comentários, explicações fora do JSON ou texto antes/depois.

Ticker: {ticker}
Tópico: {topic}

Contexto documental:
{context}

Retorne JSON exatamente neste formato:
{{
  "resumo_topico": "parágrafo analítico de 80 a 180 palavras",
  "fatos_relevantes": [
    "fato 1",
    "fato 2",
    "fato 3"
  ],
  "inferencias_cautelosas": [
    "inferência 1",
    "inferência 2"
  ],
  "evolucao_temporal": {
    "historico": "o que os materiais sugerem sobre a trajetória passada",
    "fase_atual": "qual parece ser a situação ou foco atual",
    "tendencia": "qual direção parece emergir, se houver base"
  },
  "consistencia_narrativa": {
    "sinais_positivos": [
      "sinal 1",
      "sinal 2"
    ],
    "contradicoes_ou_ruidos": [
      "ponto 1",
      "ponto 2"
    ],
    "grau": "alto|medio|baixo"
  },
  "riscos": [
    "risco 1",
    "risco 2"
  ],
  "catalisadores": [
    "catalisador 1",
    "catalisador 2"
  ],
  "o_que_monitorar": [
    "monitorar 1",
    "monitorar 2"
  ],
  "evidencias": [
    {{
      "trecho": "trecho curto ou paráfrase curta da evidência",
      "interpretacao": "por que isso importa"
    }},
    {{
      "trecho": "outra evidência",
      "interpretacao": "leitura analítica"
    }}
  ],
  "lacunas": [
    "o que faltou no material",
    "outra limitação"
  ]
}}
""".strip()


def _final_prompt(ticker: str, topic_summaries: Dict[str, dict]) -> str:
    return f"""
Você é um analista sell-side sênior elaborando uma leitura qualitativa institucional sobre {ticker}.

Abaixo estão resumos por tópico, já extraídos de documentos corporativos via RAG.
Sua tarefa é consolidar uma visão final mais rica, conectando:
- evolução estratégica;
- consistência do discurso;
- execução versus promessa;
- riscos recorrentes;
- catalisadores;
- pontos de monitoramento;
- qualidade da narrativa corporativa.

Regras obrigatórias:
1. Não invente fatos.
2. Se a evidência for insuficiente, declare limitação.
3. Procure identificar o que mudou ao longo do tempo.
4. Procure diferenciar promessa, execução, risco e gatilho.
5. Seja analítico, não genérico.
6. Responda SOMENTE em JSON válido.
7. Não inclua markdown, comentários ou texto fora do JSON.
8. As seções textuais devem ser suficientemente desenvolvidas, não apenas uma frase curta.

Entrada (resumos por tópico):
{json.dumps(topic_summaries, ensure_ascii=False)}

Retorne JSON exatamente neste formato:
{{
  "tese_sintese": "síntese analítica de 100 a 220 palavras",
  "leitura_direcionalidade": "construtiva|equilibrada|cautelosa|negativa",
  "perspectiva_compra": "forte|moderada|fraca",
  "evolucao_estrategica": {{
    "historico": "como a estratégia evoluiu",
    "fase_atual": "qual parece ser o foco atual",
    "tendencia": "qual direção futura parece mais provável"
  }},
  "consistencia_discurso": {{
    "analise": "avaliação do alinhamento entre discurso e material histórico",
    "grau_consistencia": "alto|medio|baixo",
    "contradicoes": [
      "contradição ou ruído 1",
      "contradição ou ruído 2"
    ]
  }},
  "execucao_vs_promessa": {{
    "analise": "avaliação da execução observada em relação ao discurso",
    "entregas_confirmadas": [
      "entrega 1",
      "entrega 2"
    ],
    "entregas_pendentes_ou_incertas": [
      "pendência 1",
      "pendência 2"
    ],
    "avaliacao_execucao": "forte|moderada|fraca"
  }},
  "mudancas_estrategicas": [
    "mudança 1",
    "mudança 2",
    "mudança 3"
  ],
  "pontos_chave": [
    "ponto-chave 1",
    "ponto-chave 2",
    "ponto-chave 3"
  ],
  "riscos_identificados": [
    "risco 1",
    "risco 2",
    "risco 3"
  ],
  "riscos": [
    "risco 1",
    "risco 2",
    "risco 3"
  ],
  "catalisadores": [
    "catalisador 1",
    "catalisador 2",
    "catalisador 3"
  ],
  "o_que_monitorar": [
    "monitorar 1",
    "monitorar 2",
    "monitorar 3"
  ],
  "qualidade_narrativa": {{
    "clareza": "avaliação textual da clareza",
    "coerencia": "avaliação textual da coerência",
    "sinais_de_ruido": [
      "ruído 1",
      "ruído 2"
    ]
  }},
  "consideracoes_llm": "limites das evidências, lacunas e cuidados na leitura",
  "confianca_analise": 0.0,
  "evidencias": [
    {{
      "topico": "nome do tópico",
      "trecho": "evidência curta",
      "interpretacao": "leitura analítica"
    }},
    {{
      "topico": "nome do tópico",
      "trecho": "outra evidência",
      "interpretacao": "leitura analítica"
    }}
  ],
  "topicos": {{}}
}}
""".strip()


def _normalize_topic_summary(data: dict) -> dict:
    if not isinstance(data, dict):
        data = {}

    normalized = {
        "resumo_topico": _as_str(data.get("resumo_topico")),
        "fatos_relevantes": _as_list(data.get("fatos_relevantes")),
        "inferencias_cautelosas": _as_list(data.get("inferencias_cautelosas")),
        "evolucao_temporal": data.get("evolucao_temporal") if isinstance(data.get("evolucao_temporal"), dict) else {
            "historico": "",
            "fase_atual": "",
            "tendencia": "",
        },
        "consistencia_narrativa": data.get("consistencia_narrativa") if isinstance(data.get("consistencia_narrativa"), dict) else {
            "sinais_positivos": [],
            "contradicoes_ou_ruidos": [],
            "grau": "",
        },
        "riscos": _as_list(data.get("riscos")),
        "catalisadores": _as_list(data.get("catalisadores")),
        "o_que_monitorar": _as_list(data.get("o_que_monitorar")),
        "evidencias": _as_list(data.get("evidencias")),
        "lacunas": _as_list(data.get("lacunas")),
    }

    evo = normalized["evolucao_temporal"]
    evo["historico"] = _as_str(evo.get("historico"))
    evo["fase_atual"] = _as_str(evo.get("fase_atual"))
    evo["tendencia"] = _as_str(evo.get("tendencia"))

    cons = normalized["consistencia_narrativa"]
    cons["sinais_positivos"] = _as_list(cons.get("sinais_positivos"))
    cons["contradicoes_ou_ruidos"] = _as_list(cons.get("contradicoes_ou_ruidos"))
    cons["grau"] = _as_str(cons.get("grau"))

    cleaned_evidences: List[Dict[str, str]] = []
    for item in normalized["evidencias"]:
        if isinstance(item, dict):
            trecho = _as_str(item.get("trecho"))
            interpretacao = _as_str(item.get("interpretacao"))
            if trecho or interpretacao:
                cleaned_evidences.append({
                    "trecho": trecho,
                    "interpretacao": interpretacao,
                })
        elif isinstance(item, str) and item.strip():
            cleaned_evidences.append({
                "trecho": item.strip(),
                "interpretacao": "",
            })
    normalized["evidencias"] = cleaned_evidences

    if not normalized["resumo_topico"]:
        normalized["resumo_topico"] = "Material insuficiente para consolidar leitura analítica robusta deste tópico."

    return normalized


def _fallback_final_report(ticker: str, topic_summaries: Dict[str, dict]) -> dict:
    """
    Gera um fallback interno para não deixar o pipeline sem estrutura,
    mesmo quando a LLM devolver JSON inválido.
    """
    pontos_chave: List[str] = []
    riscos: List[str] = []
    catalisadores: List[str] = []
    monitorar: List[str] = []
    evidencias: List[Dict[str, str]] = []
    contradicoes: List[str] = []
    lacunas_globais: List[str] = []

    for topic, summary in topic_summaries.items():
        for item in _as_list(summary.get("fatos_relevantes"))[:2]:
            pontos_chave.append(f"{topic}: {item}")
        for item in _as_list(summary.get("riscos"))[:2]:
            riscos.append(f"{topic}: {item}")
        for item in _as_list(summary.get("catalisadores"))[:2]:
            catalisadores.append(f"{topic}: {item}")
        for item in _as_list(summary.get("o_que_monitorar"))[:2]:
            monitorar.append(f"{topic}: {item}")
        for item in _as_list(summary.get("lacunas"))[:2]:
            lacunas_globais.append(f"{topic}: {item}")

        cons = summary.get("consistencia_narrativa", {})
        if isinstance(cons, dict):
            for item in _as_list(cons.get("contradicoes_ou_ruidos"))[:2]:
                contradicoes.append(f"{topic}: {item}")

        for ev in _as_list(summary.get("evidencias"))[:2]:
            if isinstance(ev, dict):
                evidencias.append({
                    "topico": topic,
                    "trecho": _as_str(ev.get("trecho")),
                    "interpretacao": _as_str(ev.get("interpretacao")),
                })

    consideracoes = "Consolidação gerada com fallback interno devido a resposta final inválida ou incompleta da LLM."
    if lacunas_globais:
        consideracoes += " Lacunas recorrentes: " + "; ".join(lacunas_globais[:4]) + "."

    return {
        "tese_sintese": (
            f"Leitura qualitativa preliminar para {ticker}, construída a partir da consolidação tópica. "
            "Há sinais úteis no material, mas a síntese final automática encontrou limitações de estrutura. "
            "A interpretação deve ser lida com cautela e acompanhada por evidências adicionais."
        ),
        "leitura_direcionalidade": "equilibrada",
        "perspectiva_compra": "moderada",
        "evolucao_estrategica": {
            "historico": "A evolução histórica não pôde ser consolidada com alta confiança no fallback.",
            "fase_atual": "Os tópicos sugerem elementos de acompanhamento, mas sem síntese final robusta.",
            "tendencia": "A direção futura exige validação manual do material recuperado.",
        },
        "consistencia_discurso": {
            "analise": "A consistência do discurso não pôde ser consolidada integralmente pela etapa final.",
            "grau_consistencia": "medio",
            "contradicoes": contradicoes[:6],
        },
        "execucao_vs_promessa": {
            "analise": "A execução versus promessa requer leitura manual complementar.",
            "entregas_confirmadas": [],
            "entregas_pendentes_ou_incertas": [],
            "avaliacao_execucao": "moderada",
        },
        "mudancas_estrategicas": [],
        "pontos_chave": pontos_chave[:8],
        "riscos_identificados": riscos[:8],
        "riscos": riscos[:8],
        "catalisadores": catalisadores[:8],
        "o_que_monitorar": monitorar[:8],
        "qualidade_narrativa": {
            "clareza": "Não foi possível consolidar avaliação final confiável da clareza narrativa.",
            "coerencia": "A coerência geral depende de revisão manual adicional.",
            "sinais_de_ruido": contradicoes[:6],
        },
        "consideracoes_llm": consideracoes,
        "confianca_analise": 0.35,
        "evidencias": evidencias[:10],
        "topicos": topic_summaries,
    }


def _normalize_final_report(data: dict, ticker: str, topic_summaries: Dict[str, dict]) -> dict:
    if not isinstance(data, dict):
        data = {}

    if not data:
        data = _fallback_final_report(ticker, topic_summaries)

    out = dict(data)

    out["tese_sintese"] = _as_str(out.get("tese_sintese"))
    out["leitura_direcionalidade"] = _as_str(out.get("leitura_direcionalidade"), "equilibrada")
    out["perspectiva_compra"] = _as_str(out.get("perspectiva_compra"), "moderada")

    if not isinstance(out.get("evolucao_estrategica"), dict):
        out["evolucao_estrategica"] = {}
    out["evolucao_estrategica"] = {
        "historico": _as_str(out["evolucao_estrategica"].get("historico")),
        "fase_atual": _as_str(out["evolucao_estrategica"].get("fase_atual")),
        "tendencia": _as_str(out["evolucao_estrategica"].get("tendencia")),
    }

    if not isinstance(out.get("consistencia_discurso"), dict):
        out["consistencia_discurso"] = {}
    out["consistencia_discurso"] = {
        "analise": _as_str(out["consistencia_discurso"].get("analise")),
        "grau_consistencia": _as_str(out["consistencia_discurso"].get("grau_consistencia"), "medio"),
        "contradicoes": _as_list(out["consistencia_discurso"].get("contradicoes")),
    }

    if not isinstance(out.get("execucao_vs_promessa"), dict):
        out["execucao_vs_promessa"] = {}
    out["execucao_vs_promessa"] = {
        "analise": _as_str(out["execucao_vs_promessa"].get("analise")),
        "entregas_confirmadas": _as_list(out["execucao_vs_promessa"].get("entregas_confirmadas")),
        "entregas_pendentes_ou_incertas": _as_list(out["execucao_vs_promessa"].get("entregas_pendentes_ou_incertas")),
        "avaliacao_execucao": _as_str(out["execucao_vs_promessa"].get("avaliacao_execucao"), "moderada"),
    }

    out["mudancas_estrategicas"] = _as_list(out.get("mudancas_estrategicas"))
    out["pontos_chave"] = _as_list(out.get("pontos_chave"))

    riscos_identificados = _as_list(out.get("riscos_identificados"))
    riscos_compat = _as_list(out.get("riscos"))
    if not riscos_identificados and riscos_compat:
        riscos_identificados = riscos_compat
    if not riscos_compat and riscos_identificados:
        riscos_compat = riscos_identificados

    out["riscos_identificados"] = riscos_identificados
    out["riscos"] = riscos_compat
    out["catalisadores"] = _as_list(out.get("catalisadores"))
    out["o_que_monitorar"] = _as_list(out.get("o_que_monitorar"))

    if not isinstance(out.get("qualidade_narrativa"), dict):
        out["qualidade_narrativa"] = {}
    out["qualidade_narrativa"] = {
        "clareza": _as_str(out["qualidade_narrativa"].get("clareza")),
        "coerencia": _as_str(out["qualidade_narrativa"].get("coerencia")),
        "sinais_de_ruido": _as_list(out["qualidade_narrativa"].get("sinais_de_ruido")),
    }

    out["consideracoes_llm"] = _as_str(out.get("consideracoes_llm"))

    try:
        out["confianca_analise"] = float(out.get("confianca_analise", 0.5))
    except Exception:
        out["confianca_analise"] = 0.5

    cleaned_evidences: List[Dict[str, str]] = []
    for item in _as_list(out.get("evidencias")):
        if isinstance(item, dict):
            cleaned_evidences.append({
                "topico": _as_str(item.get("topico")),
                "trecho": _as_str(item.get("trecho")),
                "interpretacao": _as_str(item.get("interpretacao")),
            })
        elif isinstance(item, str) and item.strip():
            cleaned_evidences.append({
                "topico": "",
                "trecho": item.strip(),
                "interpretacao": "",
            })
    out["evidencias"] = cleaned_evidences

    # Mantém compatibilidade com o renderer legado
    if "topicos" not in out or not isinstance(out.get("topicos"), dict):
        out["topicos"] = topic_summaries

    out.setdefault("ticker", ticker)
    out.setdefault("schema_version", "patch6_rich_v2")
    out.setdefault("generated_at", datetime.utcnow().isoformat() + "Z")

    if not out["tese_sintese"]:
        out["tese_sintese"] = f"Leitura qualitativa de {ticker} sem síntese suficientemente estruturada pela LLM."

    if not out["pontos_chave"]:
        out["pontos_chave"] = [
            "A análise qualitativa exige leitura complementar das evidências recuperadas.",
        ]

    if not out["consideracoes_llm"]:
        out["consideracoes_llm"] = "A resposta foi normalizada automaticamente para preservar a estrutura do relatório."

    return out


def build_rich_report_json(
    *,
    ticker: str,
    llm_client,
    chunks_by_topic: Dict[str, List[str]],
    per_topic_chars: int = 3500,
) -> dict:
    """
    Pipeline MAP -> REDUCE para análise qualitativa mais rica.

    Etapa 1:
    - resume cada tópico com estrutura analítica ampliada

    Etapa 2:
    - consolida os tópicos em um relatório final institucional

    Retorno:
    - dict final pronto para persistir em patch6_runs.result_json
    """
    topic_summaries: Dict[str, dict] = {}

    for topic, chunks in chunks_by_topic.items():
        context = _build_topic_context(chunks or [], per_topic_chars=per_topic_chars)

        if not context:
            topic_summaries[topic] = _normalize_topic_summary({
                "resumo_topico": "Nenhum conteúdo foi recuperado para este tópico.",
                "lacunas": ["Sem chunks disponíveis para análise."],
            })
            continue

        raw = llm_client.chat(_topic_prompt(ticker, topic, context))
        parsed = _safe_json_load(raw)
        topic_summaries[topic] = _normalize_topic_summary(parsed)

    raw_final = llm_client.chat(_final_prompt(ticker, topic_summaries))
    final = _safe_json_load(raw_final)
    final = _normalize_final_report(final, ticker, topic_summaries)

    # auditoria / compatibilidade
    final.setdefault("topicos", topic_summaries)

    return final
