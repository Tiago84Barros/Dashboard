# page/analises_portfolio.py
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

# Optional deps (present in dashboard runtime)
try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None  # type: ignore

try:
    from sqlalchemy import text  # type: ignore
except Exception:  # pragma: no cover
    text = None  # type: ignore

# RAG (Top-K inteligente)
try:
    from core.rag_retriever import get_topk_chunks_inteligente  # type: ignore
except Exception:  # pragma: no cover
    get_topk_chunks_inteligente = None  # type: ignore

# Patch6 run store (optional)
try:
    from core.patch6_runs_store import save_patch6_run, list_patch6_history  # type: ignore
except Exception:  # pragma: no cover
    save_patch6_run = None  # type: ignore
    list_patch6_history = None  # type: ignore

# LLM factory
try:
    import core.ai_models.llm_client.factory as llm_factory  # type: ignore
except Exception:  # pragma: no cover
    llm_factory = None  # type: ignore


def _get_engine():
    """Project-specific DB helper (best effort)."""
    for mod_path in ("core.db", "core.db_loader", "core.db_utils", "core.database"):
        try:
            mod = __import__(mod_path, fromlist=["get_engine"])
            if hasattr(mod, "get_engine"):
                return getattr(mod, "get_engine")()
        except Exception:
            continue
    return None


_PORTFOLIO_CSS = """
<style>
.p6-muted { color: rgba(255,255,255,0.72); font-size: 0.92rem; }

.cf-header{
  display:flex; justify-content:space-between; align-items:flex-end;
  padding: 1.0rem 1.1rem;
  border-radius: 18px;
  background: linear-gradient(135deg, rgba(255,255,255,0.07), rgba(255,255,255,0.03));
  border: 1px solid rgba(255,255,255,0.10);
  box-shadow: 0 10px 26px rgba(0,0,0,0.35);
  gap: 1rem;
}
.cf-title{ margin:0; font-size: 1.55rem; letter-spacing: -0.2px; }
.cf-subtitle{ margin:0.2rem 0 0 0; color: rgba(255,255,255,0.70); font-size: 0.95rem; }
.cf-pill{
  display:inline-flex; align-items:center; gap:0.5rem;
  padding: 0.35rem 0.65rem;
  border-radius: 999px;
  border: 1px solid rgba(255,255,255,0.12);
  background: rgba(0,0,0,0.18);
  font-size: 0.85rem;
  color: rgba(255,255,255,0.85);
}

.cf-card{
  border-radius: 18px;
  padding: 0.95rem 1.0rem;
  border: 1px solid rgba(255,255,255,0.10);
  background: rgba(255,255,255,0.04);
  box-shadow: 0 10px 24px rgba(0,0,0,0.30);
  min-height: 124px;
}
.cf-card-label{ color: rgba(255,255,255,0.72); font-size: 0.88rem; }
.cf-card-value{ font-size: 1.55rem; font-weight: 750; margin-top: 0.25rem; letter-spacing: -0.3px; }
.cf-card-extra{ color: rgba(255,255,255,0.68); font-size: 0.85rem; margin-top: 0.35rem; line-height: 1.25rem; }

.p6-chipgrid{ display:flex; flex-wrap: wrap; gap: 0.65rem; padding: 0.1rem 0.1rem 0.3rem 0.1rem; }
.p6-chip{
  display:flex; align-items:center; gap: 0.55rem;
  border-radius: 16px;
  padding: 0.55rem 0.75rem;
  border: 1px solid rgba(255,255,255,0.10);
  background: rgba(255,255,255,0.03);
}
.p6-chip img{ width: 28px; height: 28px; border-radius: 8px; object-fit: cover; background: rgba(255,255,255,0.06); }
.p6-chip span{ font-weight: 700; letter-spacing: 0.2px; }

.p6-card{
  border-radius: 20px;
  padding: 1.0rem 1.05rem;
  border: 1px solid rgba(255,255,255,0.10);
  background: linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.03));
  box-shadow: 0 12px 30px rgba(0,0,0,0.32);
  margin-bottom: 0.9rem;
}
.p6-card-top{ display:flex; justify-content:space-between; align-items:flex-start; gap: 0.8rem; }
.p6-card-title{ margin:0; font-size: 1.25rem; font-weight: 820; letter-spacing: -0.2px; }
.p6-badges{ display:flex; gap: 0.4rem; flex-wrap: wrap; justify-content:flex-end; }
.p6-badge{
  font-size: 0.78rem;
  padding: 0.18rem 0.55rem;
  border-radius: 999px;
  border: 1px solid rgba(255,255,255,0.12);
  background: rgba(0,0,0,0.18);
  color: rgba(255,255,255,0.88);
}
.p6-badge-strong{ background: rgba(34,197,94,0.16); border-color: rgba(34,197,94,0.30); }
.p6-badge-mid{ background: rgba(59,130,246,0.16); border-color: rgba(59,130,246,0.30); }
.p6-badge-cautious{ background: rgba(245,158,11,0.16); border-color: rgba(245,158,11,0.30); }
.p6-badge-risk{ background: rgba(239,68,68,0.16); border-color: rgba(239,68,68,0.30); }

.p6-card-meta{ margin-top: 0.45rem; color: rgba(255,255,255,0.68); font-size: 0.86rem; }
.p6-grid2{ display:grid; grid-template-columns: 1fr 1fr; gap: 0.8rem; margin-top: 0.8rem; }
.p6-box{
  border-radius: 16px;
  padding: 0.8rem 0.85rem;
  border: 1px solid rgba(255,255,255,0.08);
  background: rgba(0,0,0,0.12);
}
.p6-box h4{ margin:0 0 0.25rem 0; font-size: 0.95rem; }
.p6-box p{ margin:0; color: rgba(255,255,255,0.72); font-size: 0.88rem; line-height: 1.25rem; }

.p6-divider{ height: 1px; background: rgba(255,255,255,0.08); margin: 1.0rem 0; }
</style>
"""


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace("%", "").replace(",", ".")
        if s == "" or s.lower() in ("nan", "none"):
            return None
        return float(s)
    except Exception:
        return None


def _fmt_pct(x: Any) -> str:
    v = _safe_float(x)
    if v is None:
        return "—"
    if abs(v) <= 1.0:
        v *= 100.0
    return f"{v:.2f}%".replace(".", ",")


def _fmt_num(x: Any) -> str:
    try:
        if x is None:
            return "—"
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            return f"{int(x):,}".replace(",", ".")
        s = str(x).strip()
        return s if s else "—"
    except Exception:
        return "—"


def _ticker_logo_url(ticker: str) -> str:
    tk = ticker.strip().upper()
    return f"https://raw.githubusercontent.com/ranaroussi/yfinance/main/yfinance/resources/logo/{tk}.png"


def _load_snapshot_from_session() -> Dict[str, Any]:
    for key in ("snapshot_portfolio", "snapshot", "portfolio_snapshot", "snapshot_data"):
        obj = st.session_state.get(key)
        if isinstance(obj, dict) and obj:
            return obj
    return {}


def _load_latest_snapshot_from_db(user_id: Optional[str]) -> Dict[str, Any]:
    eng = _get_engine()
    if eng is None or text is None or pd is None:
        return {}

    candidates = [
        ("snapshot_portfolio", "select * from snapshot_portfolio {where} order by created_at desc limit 1"),
        ("snapshot_portfolios", "select * from snapshot_portfolios {where} order by created_at desc limit 1"),
    ]

    where = ""
    params: Dict[str, Any] = {}
    if user_id:
        where = "where user_id = :uid"
        params = {"uid": user_id}

    with eng.connect() as conn:
        for tbl, q in candidates:
            try:
                df = pd.read_sql(text(q.format(where=where)), conn, params=params)
                if df is not None and len(df) > 0:
                    row = df.iloc[0].to_dict()
                    row["_table"] = tbl
                    return row
            except Exception:
                continue
    return {}


def _extract_portfolio_meta(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    selic = snapshot.get("selic") or snapshot.get("selic_aa") or snapshot.get("selic_utilizada") or snapshot.get("selic_used")
    acima = snapshot.get("margem_minima") or snapshot.get("margem_minima_sobre") or snapshot.get("acima_benchmark") or snapshot.get("pct_acima_benchmark")

    tickers: List[str] = []
    for key in ("tickers", "tickers_json", "tickers_escolhidos", "assets", "ativos"):
        val = snapshot.get(key)
        if not val:
            continue
        try:
            parsed = json.loads(val) if isinstance(val, str) else val
            if isinstance(parsed, list):
                tickers = [str(x).strip().upper() for x in parsed if str(x).strip()]
                break
            if isinstance(parsed, dict) and isinstance(parsed.get("tickers"), list):
                tickers = [str(x).strip().upper() for x in parsed["tickers"] if str(x).strip()]
                break
        except Exception:
            continue

    if not tickers:
        sess = st.session_state.get("tickers_selecionados") or st.session_state.get("portfolio_tickers") or st.session_state.get("tickers")
        if isinstance(sess, list):
            tickers = [str(x).strip().upper() for x in sess if str(x).strip()]

    return {"selic": selic, "acima_benchmark": acima, "tickers": tickers}


def _count_segments_db(tickers: List[str]) -> Optional[int]:
    if not tickers:
        return None
    eng = _get_engine()
    if eng is None or text is None or pd is None:
        return None

    queries = [
        "select count(distinct segmento) as n from setores where ticker = any(:tks)",
        "select count(distinct setor) as n from setores where ticker = any(:tks)",
        "select count(distinct segmento) as n from setores_b3 where ticker = any(:tks)",
        "select count(distinct segmento) as n from empresas_setor where ticker = any(:tks)",
    ]

    with eng.connect() as conn:
        for q in queries:
            try:
                df = pd.read_sql(text(q), conn, params={"tks": tickers})
                if df is not None and len(df) > 0 and "n" in df.columns:
                    return int(df.iloc[0]["n"])
            except Exception:
                continue
    return None


@dataclass
class LLMResult:
    ticker: str
    status: str  # OK | ERRO_LLM | SEM_DADOS
    tese: str = ""
    direcionalidade: str = ""
    pontos_chave: List[str] = None  # type: ignore
    riscos: List[str] = None  # type: ignore
    dividendos: str = ""
    capex: str = ""
    divida: str = ""
    expansao: str = ""
    mna: str = ""
    recomendacao: str = ""
    qualidade: str = ""
    perspectiva_12m: str = ""
    evidencias_docs: int = 0
    evidencias_trechos: int = 0
    erro: str = ""


def _llm_schema() -> Dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "tese": {"type": "string"},
            "direcionalidade": {"type": "string"},
            "pontos_chave": {"type": "array", "items": {"type": "string"}},
            "riscos": {"type": "array", "items": {"type": "string"}},
            "capex": {"type": "string"},
            "divida": {"type": "string"},
            "dividendos": {"type": "string"},
            "expansao": {"type": "string"},
            "mna": {"type": "string"},
            "perspectiva_12m": {"type": "string"},
            "recomendacao": {"type": "string"},
            "qualidade": {"type": "string"},
        },
        "required": [
            "tese",
            "direcionalidade",
            "pontos_chave",
            "riscos",
            "capex",
            "divida",
            "dividendos",
            "expansao",
            "mna",
            "perspectiva_12m",
            "recomendacao",
            "qualidade",
        ],
    }


def _build_prompt(ticker: str, contexto: str) -> Tuple[str, str]:
    system = (
        "Você é um analista sell-side focado em governança, alocação de capital e criação de valor ao acionista minoritário. "
        "Você só pode usar as evidências fornecidas (trechos do RAG). Se não houver evidência suficiente, sinalize explicitamente. "
        "Responda SEM números inventados; use linguagem objetiva, institucional."
    )
    user = f"""
Ticker: {ticker}

OBJETIVO:
Inferir a trajetória e a intenção futura da companhia com base em evidências textuais (RAG), cobrindo:
- CAPEX / investimentos e expansão
- Dívida / desalavancagem / refinanciamento / covenants
- Dividendos / payout / recompra
- M&A / desinvestimentos
- Guidance / metas / prioridades do management
- Destino do lucro e alocação de capital

TAREFA:
1) Escreva uma TESE (síntese) do que a companhia está tentando fazer no horizonte 12 meses.
2) Classifique a DIRECIONALIDADE (Construtiva / Equilibrada / Cautelosa) sob a ótica do minoritário.
3) Aponte 3-6 PONTOS-CHAVE e 2-5 RISCOS (sempre conectados às evidências).
4) Preencha os campos CAPEX, DÍVIDA, DIVIDENDOS, EXPANSÃO, M&A em 1-2 frases cada.
5) Defina PERSPECTIVA_12M (Positiva/Neutra/Negativa) e uma RECOMENDAÇÃO (Construtiva/Equilibrada/Cautelosa).
6) Defina QUALIDADE (Alta/Média/Baixa): Alta = muitas evidências diretas e atuais; Média = evidência parcial; Baixa = pouco material/ambíguo.

EVIDÊNCIAS (RAG):
{contexto}
""".strip()
    return system, user


def _normalize_label(x: str, allowed: List[str], fallback: str) -> str:
    if not x:
        return fallback
    s = str(x).strip().lower()
    mapping = {
        "construtiva": "Construtiva",
        "equilibrada": "Equilibrada",
        "cautelosa": "Cautelosa",
        "positiva": "Positiva",
        "neutra": "Neutra",
        "negativa": "Negativa",
        "alta": "Alta",
        "média": "Média",
        "media": "Média",
        "baixa": "Baixa",
    }
    out = mapping.get(s, str(x).strip())
    return out if out in allowed else fallback


def _call_llm_for_ticker(ticker: str, chunks: List[Dict[str, Any]], docs_count: int) -> LLMResult:
    if not chunks:
        return LLMResult(ticker=ticker, status="SEM_DADOS", erro="Sem evidências (RAG).")

    contexto_lines: List[str] = []
    for i, ch in enumerate(chunks, 1):
        txt = ch.get("chunk") or ch.get("text") or ch.get("content") or ""
        txt = re.sub(r"\s+", " ", str(txt).strip())
        if not txt:
            continue
        if len(txt) > 900:
            txt = txt[:900] + "…"
        src = ch.get("doc_id") or ch.get("document_id") or ch.get("source") or ch.get("titulo") or ""
        contexto_lines.append(f"[{i}] {txt}\n(Fonte: {src})")

    contexto = "\n\n".join(contexto_lines).strip()
    if not contexto:
        return LLMResult(ticker=ticker, status="SEM_DADOS", erro="Evidências vazias.")

    if llm_factory is None or not hasattr(llm_factory, "get_client"):
        return LLMResult(ticker=ticker, status="ERRO_LLM", erro="Factory de LLM indisponível.")

    try:
        client = llm_factory.get_client()
    except Exception as e:
        return LLMResult(ticker=ticker, status="ERRO_LLM", erro=f"Falha ao obter cliente LLM: {e}")

    system, user = _build_prompt(ticker, contexto)
    schema = _llm_schema()

    try:
        payload = client.generate_json(system=system, user=user, schema=schema, temperature=0.2)
        if not isinstance(payload, dict):
            return LLMResult(ticker=ticker, status="ERRO_LLM", erro="Resposta LLM inválida (não JSON).")

        r = LLMResult(
            ticker=ticker,
            status="OK",
            tese=str(payload.get("tese", "")).strip(),
            direcionalidade=str(payload.get("direcionalidade", "")).strip(),
            pontos_chave=list(payload.get("pontos_chave") or []),
            riscos=list(payload.get("riscos") or []),
            dividendos=str(payload.get("dividendos", "")).strip(),
            capex=str(payload.get("capex", "")).strip(),
            divida=str(payload.get("divida", "")).strip(),
            expansao=str(payload.get("expansao", "")).strip(),
            mna=str(payload.get("mna", "")).strip(),
            recomendacao=str(payload.get("recomendacao", "")).strip(),
            qualidade=str(payload.get("qualidade", "")).strip(),
            perspectiva_12m=str(payload.get("perspectiva_12m", "")).strip(),
            evidencias_docs=int(docs_count or 0),
            evidencias_trechos=int(len(chunks)),
        )

        r.recomendacao = _normalize_label(r.recomendacao, ["Construtiva", "Equilibrada", "Cautelosa"], "Equilibrada")
        r.perspectiva_12m = _normalize_label(r.perspectiva_12m, ["Positiva", "Neutra", "Negativa"], "Neutra")
        r.qualidade = _normalize_label(r.qualidade, ["Alta", "Média", "Baixa"], "Média")
        r.direcionalidade = _normalize_label(r.direcionalidade, ["Construtiva", "Equilibrada", "Cautelosa"], r.recomendacao)

        if r.pontos_chave is None:
            r.pontos_chave = []
        if r.riscos is None:
            r.riscos = []
        return r
    except Exception as e:
        return LLMResult(ticker=ticker, status="ERRO_LLM", erro=str(e))


def _badge_class_recomendacao(x: str) -> str:
    s = (x or "").lower()
    if "constr" in s:
        return "p6-badge p6-badge-strong"
    if "equil" in s:
        return "p6-badge p6-badge-mid"
    if "caut" in s:
        return "p6-badge p6-badge-cautious"
    return "p6-badge"


def _badge_class_perspectiva(x: str) -> str:
    s = (x or "").lower()
    if "posit" in s:
        return "p6-badge p6-badge-strong"
    if "neutr" in s:
        return "p6-badge p6-badge-mid"
    if "neg" in s:
        return "p6-badge p6-badge-risk"
    return "p6-badge"


def _badge_class_qualidade(x: str) -> str:
    s = (x or "").lower()
    if "alta" in s:
        return "p6-badge p6-badge-strong"
    if "méd" in s or "med" in s:
        return "p6-badge p6-badge-mid"
    if "baixa" in s:
        return "p6-badge p6-badge-risk"
    return "p6-badge"


def _render_llm_card(r: LLMResult) -> None:
    if r.status != "OK":
        st.markdown(
            f"""
            <div class="p6-card">
              <div class="p6-card-top">
                <h3 class="p6-card-title">{r.ticker}</h3>
                <div class="p6-badges"><span class="p6-badge p6-badge-risk">Sem leitura</span></div>
              </div>
              <div class="p6-card-meta">Status: {r.status} • {r.erro or ""}</div>
              <div class="p6-divider"></div>
              <div class="p6-muted">Não foi possível gerar a tese. Verifique configuração da LLM e evidências do RAG.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    badges = (
        f"<span class='{_badge_class_recomendacao(r.recomendacao)}'>{r.recomendacao}</span>"
        f"<span class='{_badge_class_perspectiva(r.perspectiva_12m)}'>Perspectiva 12m: {r.perspectiva_12m}</span>"
        f"<span class='{_badge_class_qualidade(r.qualidade)}'>Qualidade: {r.qualidade}</span>"
    )
    meta = f"Documentos analisados: {r.evidencias_docs} • Trechos relevantes: {r.evidencias_trechos}"

    def esc(s: str) -> str:
        return (s or "—").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    pontos = "".join([f"<li>{esc(x)}</li>" for x in (r.pontos_chave or [])[:6]]) or "<li>—</li>"
    riscos = "".join([f"<li>{esc(x)}</li>" for x in (r.riscos or [])[:6]]) or "<li>—</li>"

    st.markdown(
        f"""
        <div class="p6-card">
          <div class="p6-card-top">
            <h3 class="p6-card-title">{r.ticker}</h3>
            <div class="p6-badges">{badges}</div>
          </div>
          <div class="p6-card-meta">{meta}</div>

          <div class="p6-grid2">
            <div class="p6-box">
              <h4>Tese (síntese)</h4>
              <p>{esc(r.tese)}</p>
            </div>
            <div class="p6-box">
              <h4>Direcionalidade</h4>
              <p>{esc(r.direcionalidade)}</p>
            </div>
          </div>

          <div class="p6-grid2">
            <div class="p6-box">
              <h4>Alocação de capital</h4>
              <p><b>CAPEX:</b> {esc(r.capex)}</p>
              <p><b>Expansão:</b> {esc(r.expansao)}</p>
              <p><b>M&amp;A:</b> {esc(r.mna)}</p>
            </div>
            <div class="p6-box">
              <h4>Retorno ao acionista / risco financeiro</h4>
              <p><b>Dividendos:</b> {esc(r.dividendos)}</p>
              <p><b>Dívida:</b> {esc(r.divida)}</p>
            </div>
          </div>

          <div class="p6-grid2">
            <div class="p6-box">
              <h4>Pontos‑chave</h4>
              <ul style="margin:0.25rem 0 0 1.1rem; color: rgba(255,255,255,0.78); font-size:0.88rem; line-height:1.25rem;">{pontos}</ul>
            </div>
            <div class="p6-box">
              <h4>Riscos / ressalvas</h4>
              <ul style="margin:0.25rem 0 0 1.1rem; color: rgba(255,255,255,0.78); font-size:0.88rem; line-height:1.25rem;">{riscos}</ul>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _aggregate_portfolio(results: List[LLMResult]) -> Dict[str, Any]:
    ok = [r for r in results if r.status == "OK"]
    if not ok:
        return {
            "qualidade": "Baixa",
            "perspectiva_12m": "Neutra",
            "cobertura": f"0/{len(results)}",
            "dist": {"Construtiva": 0, "Equilibrada": 0, "Cautelosa": 0},
            "explicacao_qualidade": "Sem leituras válidas. Verifique evidências (RAG) e configuração da LLM.",
        }

    dist = {"Construtiva": 0, "Equilibrada": 0, "Cautelosa": 0}
    for r in ok:
        dist[r.recomendacao] = dist.get(r.recomendacao, 0) + 1

    persp_counts = {"Positiva": 0, "Neutra": 0, "Negativa": 0}
    for r in ok:
        persp_counts[r.perspectiva_12m] = persp_counts.get(r.perspectiva_12m, 0) + 1
    persp = max(persp_counts.items(), key=lambda kv: kv[1])[0]

    avg_docs = sum(r.evidencias_docs for r in ok) / max(1, len(ok))
    avg_trechos = sum(r.evidencias_trechos for r in ok) / max(1, len(ok))
    if avg_trechos >= 10 and avg_docs >= 8:
        qualidade = "Alta"
        expl = "Leitura com boa profundidade: muitos documentos e trechos relevantes sustentando a tese por empresa."
    elif avg_trechos >= 5 and avg_docs >= 4:
        qualidade = "Média"
        expl = "Leitura razoável: há evidências, mas parte das teses depende de sinais parciais (nem sempre diretos/atuais)."
    else:
        qualidade = "Baixa"
        expl = "Leitura fraca: poucas evidências ou trechos curtos/ambíguos, reduzindo confiabilidade da inferência."

    return {
        "qualidade": qualidade,
        "perspectiva_12m": persp,
        "cobertura": f"{len(ok)}/{len(results)}",
        "dist": dist,
        "explicacao_qualidade": expl,
    }


def _run_ingest_and_chunks(tickers: List[str], meses: int, max_docs: int, max_pdfs: int) -> Dict[str, Any]:
    out: Dict[str, Any] = {"status": "SKIP", "tickers": tickers, "tempo_s": 0.0, "erro": ""}
    t0 = datetime.now(timezone.utc)

    runner = None
    for mod_path, fn_name in (
        ("core.patch6_ingest", "run_ingest"),
        ("core.patch6_ingest", "ingest_runner"),
        ("core.patch6_store", "ingest_and_chunk"),
        ("core.patch6_store", "run_patch6_ingest"),
        ("core.patch6", "run_ingest"),
        ("core.patch6", "ingest_runner"),
    ):
        try:
            mod = __import__(mod_path, fromlist=[fn_name])
            if hasattr(mod, fn_name):
                runner = getattr(mod, fn_name)
                break
        except Exception:
            continue

    if runner is None:
        out["status"] = "INDISPONIVEL"
        out["erro"] = "Runner de ingest/chunks não encontrado no projeto."
        return out

    try:
        out["status"] = "OK"
        kwargs = dict(
            tickers=tickers,
            months=meses,
            window_months=meses,
            max_docs=max_docs,
            max_docs_per_ticker=max_docs,
            max_pdfs=max_pdfs,
        )
        try:
            runner(**kwargs)
        except TypeError:
            runner(tickers, meses, max_docs, max_pdfs)
    except Exception as e:
        out["status"] = "ERRO"
        out["erro"] = str(e)

    out["tempo_s"] = (datetime.now(timezone.utc) - t0).total_seconds()
    return out


def render() -> None:
    st.set_page_config(page_title="Análises de Portfólio", layout="wide")
    st.markdown(_PORTFOLIO_CSS, unsafe_allow_html=True)

    user_id = None
    for k in ("user_id", "uid", "usuario_id"):
        if st.session_state.get(k):
            user_id = str(st.session_state.get(k))
            break

    snapshot = _load_snapshot_from_session()
    if not snapshot:
        snapshot = _load_latest_snapshot_from_db(user_id)

    meta = _extract_portfolio_meta(snapshot)
    tickers = meta.get("tickers") or []

    if not tickers:
        st.markdown(
            """
            <div class="cf-header">
              <div>
                <h1 class="cf-title">🧠 Análises de Portfólio (Patch6)</h1>
                <p class="cf-subtitle">Nenhum portfólio encontrado. Execute “Criação de Portfólio” para gerar os dados.</p>
              </div>
              <div><span class="cf-pill">Dados salvos: indisponível</span></div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    st.markdown(
        """
        <div class="cf-header">
            <div>
                <h1 class="cf-title">🧠 Análises de Portfólio (Patch6)</h1>
                <p class="cf-subtitle">Leitura qualitativa orientada a intenção futura (CAPEX, dívida, dividendos, expansão e alocação de capital).</p>
            </div>
            <div><span class="cf-pill">Dados salvos</span></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Dados salvos (cards)
    selic = meta.get("selic")
    acima = meta.get("acima_benchmark")
    if selic in (None, "", "—"):
        for k in ("selic", "selic_escolhida", "selic_input", "selic_aa"):
            if st.session_state.get(k) is not None:
                selic = st.session_state.get(k)
                break
    if acima in (None, "", "—"):
        for k in ("margem_minima", "margem_minima_sobre", "pct_acima_benchmark", "acima_benchmark"):
            if st.session_state.get(k) is not None:
                acima = st.session_state.get(k)
                break

    n_acoes = len(tickers)
    n_segs = _count_segments_db(tickers)

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(
        f"""
        <div class="cf-card">
            <div class="cf-card-label">Selic utilizada</div>
            <div class="cf-card-value">{_fmt_pct(selic) if selic is not None else "—"}</div>
            <div class="cf-card-extra">Taxa base informada na criação do portfólio.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    c2.markdown(
        f"""
        <div class="cf-card">
            <div class="cf-card-label">% acima do benchmark</div>
            <div class="cf-card-value">{_fmt_pct(acima) if acima is not None else "—"}</div>
            <div class="cf-card-extra">Margem mínima exigida vs índice base.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    c3.markdown(
        f"""
        <div class="cf-card">
            <div class="cf-card-label">Ações selecionadas</div>
            <div class="cf-card-value">{n_acoes}</div>
            <div class="cf-card-extra">Quantidade total de ativos no snapshot.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    c4.markdown(
        f"""
        <div class="cf-card">
            <div class="cf-card-label">Segmentos</div>
            <div class="cf-card-value">{_fmt_num(n_segs) if n_segs is not None else "—"}</div>
            <div class="cf-card-extra">Diversificação setorial do portfólio.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<div class='p6-divider'></div>", unsafe_allow_html=True)

    st.markdown("## Ativos selecionados")
    chips = ["<div class='p6-chipgrid'>"]
    for tk in tickers:
        chips.append(
            f"""
            <div class="p6-chip">
              <img src="{_ticker_logo_url(tk)}" onerror="this.style.display='none'" />
              <span>{tk}</span>
            </div>
            """
        )
    chips.append("</div>")
    st.markdown("\n".join(chips), unsafe_allow_html=True)

    st.markdown("<div class='p6-divider'></div>", unsafe_allow_html=True)

    st.markdown("## 📦 Atualizar evidências")
    with st.expander("Configurar atualização", expanded=False):
        a, b, c = st.columns(3)
        meses = a.number_input("Janela (meses)", min_value=3, max_value=36, value=12, step=1)
        max_docs = b.number_input("Máx. docs/ticker", min_value=10, max_value=200, value=80, step=5)
        max_pdfs = c.number_input("Máx. PDFs/ticker", min_value=5, max_value=80, value=20, step=5)

        if st.button("Atualizar documentos", use_container_width=True):
            with st.spinner("Atualizando evidências (ingest + trechos)…"):
                res_ing = _run_ingest_and_chunks(tickers, int(meses), int(max_docs), int(max_pdfs))
            if res_ing["status"] == "OK":
                st.success(f"Atualização concluída em {res_ing['tempo_s']:.1f}s.")
            else:
                st.error(f"Falha na atualização: {res_ing.get('erro','')}")

    st.markdown("<div class='p6-divider'></div>", unsafe_allow_html=True)

    st.markdown("## 📘 Relatório de Análise de Portfólio")

    cx, cy, cz = st.columns([1.2, 1.2, 2.6])
    top_k = cx.slider("Top‑K (trechos no contexto)", min_value=4, max_value=18, value=6, step=1)
    janela_meses = cy.slider("Janela (meses) p/ Top‑K inteligente", min_value=6, max_value=24, value=12, step=1)
    debug_topk = cz.checkbox("Debug Top‑K (score detalhado)", value=False)

    period_ref = st.text_input("period_ref (ex.: 2024Q4)", value=st.session_state.get("period_ref", "2024Q4"))
    st.session_state["period_ref"] = period_ref

    run_all = st.button("Rodar LLM agora", use_container_width=True)

    if "patch6_results" not in st.session_state:
        st.session_state["patch6_results"] = []

    if run_all:
        results: List[LLMResult] = []
        prog = st.progress(0, text="Preparando…")
        stat = st.empty()

        for i, tk in enumerate(tickers, 1):
            prog.progress(int((i - 1) / max(1, len(tickers)) * 100), text=f"Processando {tk} ({i}/{len(tickers)})…")
            stat.info(f"Processando {tk} ({i}/{len(tickers)})…")

            chunks: List[Dict[str, Any]] = []
            docs_count = 0
            try:
                if get_topk_chunks_inteligente is not None:
                    chunks, dbg = get_topk_chunks_inteligente(
                        ticker=tk,
                        top_k=int(top_k),
                        window_months=int(janela_meses),
                        period_ref=period_ref,
                        debug=bool(debug_topk),
                    )
                    if debug_topk and dbg:
                        st.caption(f"Debug Top‑K {tk}: {str(dbg)[:1200]}")
                docs_count = len({(c.get('doc_id') or c.get('document_id') or c.get('source')) for c in chunks if isinstance(c, dict)})
            except Exception:
                chunks = []
                docs_count = 0

            r = _call_llm_for_ticker(tk, chunks, docs_count)
            results.append(r)

            if save_patch6_run is not None and r.status == "OK":
                try:
                    save_patch6_run(
                        ticker=tk,
                        period_ref=period_ref,
                        perspectiva_compra=r.recomendacao.lower(),
                        resumo=r.tese[:240],
                        raw_json=json.dumps(r.__dict__, ensure_ascii=False),
                    )
                except Exception:
                    pass

            _render_llm_card(r)

        prog.progress(100, text="Concluído.")
        stat.success("Leitura concluída.")
        st.session_state["patch6_results"] = [r.__dict__ for r in results]

        agg = _aggregate_portfolio(results)
        st.markdown("<div class='p6-divider'></div>", unsafe_allow_html=True)
        st.markdown("### 📌 Resumo executivo do portfólio")

        d1, d2, d3, d4 = st.columns(4)
        d1.markdown(
            f"""
            <div class="cf-card">
              <div class="cf-card-label">Qualidade (heurística)</div>
              <div class="cf-card-value">{agg['qualidade']}</div>
              <div class="cf-card-extra">{agg['explicacao_qualidade']}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        d2.markdown(
            f"""
            <div class="cf-card">
              <div class="cf-card-label">Perspectiva 12m</div>
              <div class="cf-card-value">{agg['perspectiva_12m']}</div>
              <div class="cf-card-extra">Direcionalidade agregada do conjunto.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        d3.markdown(
            f"""
            <div class="cf-card">
              <div class="cf-card-label">Cobertura analisada</div>
              <div class="cf-card-value">{agg['cobertura']}</div>
              <div class="cf-card-extra">Ativos com leitura qualitativa válida.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        dist = agg["dist"]
        d4.markdown(
            f"""
            <div class="cf-card">
              <div class="cf-card-label">Distribuição (qualitativa)</div>
              <div class="cf-card-value">{dist.get('Construtiva',0)} • {dist.get('Equilibrada',0)} • {dist.get('Cautelosa',0)}</div>
              <div class="cf-card-extra">Construtivas • Equilibradas • Cautelosas</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    prev = st.session_state.get("patch6_results") or []
    if (not run_all) and prev:
        st.markdown("### Última leitura (cache da sessão)")
        for d in prev:
            try:
                _render_llm_card(LLMResult(**d))
            except Exception:
                continue

    if list_patch6_history is not None:
        try:
            hist = list_patch6_history(limit=20)
            if hist is not None and pd is not None:
                st.markdown("<div class='p6-divider'></div>", unsafe_allow_html=True)
                st.markdown("## 🧾 Histórico (patch6_runs)")
                st.dataframe(hist, use_container_width=True, hide_index=True)
        except Exception:
            pass
