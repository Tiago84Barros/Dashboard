# page/analises_portfolio.py
# Patch6 — Análises de Portfólio
#
# Objetivo deste módulo: carregar (de forma robusta) o portfólio salvo pela página
# "Criação de Portfólio" e, a partir dele, habilitar as rotinas do Patch6.
#
# IMPORTANTE: este arquivo foi feito para ser resiliente a mudanças de nome de
# helper (get_engine) e de tabela/colunas no Supabase. Ele NÃO assume um schema fixo.

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

# ---------------------------
# CSS (leve e seguro)
# ---------------------------

_CSS = """
<style>
.p6-header{
  display:flex; align-items:flex-start; justify-content:space-between; gap:16px;
  padding:18px 18px; border-radius:18px;
  background: linear-gradient(135deg, rgba(255,255,255,0.06), rgba(255,255,255,0.02));
  border: 1px solid rgba(255,255,255,0.08);
}
.p6-title{ font-size:40px; font-weight:800; margin:0; letter-spacing:-0.4px; }
.p6-sub{ margin:6px 0 0 0; opacity:0.86; font-size:14px; line-height:1.35; }
.p6-pill{
  display:inline-flex; align-items:center; gap:8px;
  padding:8px 12px; border-radius:999px;
  border:1px solid rgba(255,255,255,0.10);
  background: rgba(255,255,255,0.04);
  font-size:12px; opacity:0.95;
}
.p6-section{ margin-top: 14px; }
.p6-card{
  padding:14px 14px; border-radius:16px;
  border:1px solid rgba(255,255,255,0.10);
  background: rgba(255,255,255,0.04);
  box-shadow: 0 10px 28px rgba(0,0,0,0.22);
  height: 100%;
}
.p6-card .lbl{ font-size:12px; opacity:0.78; margin-bottom:6px; }
.p6-card .val{ font-size:26px; font-weight:800; letter-spacing:-0.3px; }
.p6-card .extra{ margin-top:6px; font-size:12px; opacity:0.78; line-height:1.35; }
.p6-asset{
  display:flex; align-items:center; gap:10px;
  padding:10px 12px; border-radius:14px;
  border:1px solid rgba(255,255,255,0.10);
  background: rgba(255,255,255,0.04);
}
.p6-asset img{ width:28px; height:28px; border-radius:6px; object-fit:contain; background:#fff; padding:3px; }
.p6-asset .tck{ font-weight:800; letter-spacing:0.2px; }
.p6-muted{ opacity:0.75; font-size:12px; }
</style>
"""

# ---------------------------
# DB helpers (imports flexíveis)
# ---------------------------

def _get_engine():
    """
    Tenta resolver o engine do Supabase/Postgres, independentemente do nome do helper
    no seu projeto.
    """
    # Ordem: helpers mais comuns no seu histórico de projeto
    candidates = [
        ("core.db_loader", "get_supabase_engine"),
        ("core.db_loader", "get_engine"),
        ("core.db", "get_engine"),
        ("core.database", "get_engine"),
        ("db_loader", "get_engine"),
    ]
    last_err = None
    for mod_name, fn_name in candidates:
        try:
            mod = __import__(mod_name, fromlist=[fn_name])
            fn = getattr(mod, fn_name)
            return fn()
        except Exception as e:
            last_err = e
            continue
    raise ImportError(f"Não consegui obter engine do banco. Último erro: {last_err}")

def _sql_text():
    try:
        from sqlalchemy import text
        return text
    except Exception:
        return None

def _query_df(sql: str, params: Optional[dict] = None):
    text = _sql_text()
    if text is None:
        raise ImportError("sqlalchemy.text indisponível")
    import pandas as pd
    eng = _get_engine()
    with eng.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

def _table_exists(table: str) -> bool:
    try:
        df = _query_df(
            "select 1 as ok from information_schema.tables where table_schema='public' and table_name=:t limit 1",
            {"t": table},
        )
        return not df.empty
    except Exception:
        return False

def _table_columns(table: str) -> List[str]:
    try:
        df = _query_df(
            "select column_name from information_schema.columns where table_schema='public' and table_name=:t",
            {"t": table},
        )
        return [str(x) for x in df["column_name"].tolist()]
    except Exception:
        return []

# ---------------------------
# Snapshot model
# ---------------------------

@dataclass
class Snapshot:
    snapshot_id: Optional[str]
    tickers: List[str]
    weights: Dict[str, float]
    selic: Optional[float]
    acima_benchmark: Optional[float]
    benchmark: Optional[str]
    segmentos: List[str]
    raw: Dict[str, Any]

def _parse_jsonish(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, str):
        s = v.strip()
        # JSON puro
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            try:
                return json.loads(s)
            except Exception:
                return v
        # "A,B,C"
        if "," in s and len(s) < 120:
            return [x.strip() for x in s.split(",") if x.strip()]
    return v

def _extract_tickers(row: Dict[str, Any]) -> List[str]:
    keys = ["tickers", "acoes", "ativos", "empresas", "portfolio", "carteira"]
    for k in keys:
        if k in row and row[k] is not None:
            val = _parse_jsonish(row[k])
            if isinstance(val, list):
                return [str(x).upper().strip() for x in val if str(x).strip()]
            if isinstance(val, dict):
                # às vezes vem {"BRAP3":0.1,...}
                return [str(x).upper().strip() for x in val.keys()]
            if isinstance(val, str):
                return [val.upper().strip()]
    # fallback: tenta inferir por colunas "ticker_1"... etc
    t = []
    for k, v in row.items():
        if "ticker" in k.lower() and v:
            t.append(str(v).upper().strip())
    # dedup mantendo ordem
    seen = set()
    out = []
    for x in t:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out

def _extract_weights(row: Dict[str, Any]) -> Dict[str, float]:
    for k in ["weights", "pesos", "alloc", "alocacao", "alocação"]:
        if k in row and row[k] is not None:
            val = _parse_jsonish(row[k])
            if isinstance(val, dict):
                out = {}
                for kk, vv in val.items():
                    try:
                        out[str(kk).upper().strip()] = float(vv)
                    except Exception:
                        continue
                return out
    # fallback: weight columns per ticker?
    return {}

def _extract_numeric(row: Dict[str, Any], keys: List[str]) -> Optional[float]:
    for k in keys:
        if k in row and row[k] not in (None, "", "-"):
            try:
                return float(row[k])
            except Exception:
                try:
                    return float(str(row[k]).replace(",", "."))
                except Exception:
                    continue
    return None

def _extract_segmentos(row: Dict[str, Any]) -> List[str]:
    for k in ["segmentos", "setores", "sectors"]:
        if k in row and row[k] is not None:
            val = _parse_jsonish(row[k])
            if isinstance(val, list):
                return [str(x).strip() for x in val if str(x).strip()]
            if isinstance(val, dict):
                return [str(x).strip() for x in val.keys()]
            if isinstance(val, str):
                return [val.strip()]
    return []

# ---------------------------
# Snapshot loading (session_state -> DB fallback)
# ---------------------------

def _get_user_id() -> Optional[str]:
    # tenta chaves comuns
    for k in ["user_id", "auth_user_id", "uid", "email", "user_email"]:
        v = st.session_state.get(k)
        if v:
            return str(v)
    return None

def _pick_session_snapshot() -> Optional[Snapshot]:
    # 1) se a página de criação gravou o dicionário completo
    if isinstance(st.session_state.get("portfolio_salvo"), dict):
        row = dict(st.session_state["portfolio_salvo"])
        tickers = _extract_tickers(row)
        return Snapshot(
            snapshot_id=str(row.get("snapshot_id") or row.get("id") or ""),
            tickers=tickers,
            weights=_extract_weights(row),
            selic=_extract_numeric(row, ["selic", "selic_usada", "taxa_selic"]),
            acima_benchmark=_extract_numeric(row, ["acima_benchmark", "pct_acima_benchmark", "percent_acima_benchmark"]),
            benchmark=str(row.get("benchmark") or row.get("indice_base") or row.get("index_base") or "") or None,
            segmentos=_extract_segmentos(row),
            raw=row,
        )

    # 2) se só gravou snapshot_id
    for k in ["snapshot_id", "snapshot_portfolio_id", "snapshot", "snapshot_hash"]:
        v = st.session_state.get(k)
        if v:
            return Snapshot(
                snapshot_id=str(v),
                tickers=[],
                weights={},
                selic=None,
                acima_benchmark=None,
                benchmark=None,
                segmentos=[],
                raw={"snapshot_id": str(v)},
            )
    return None

def _load_snapshot_from_db() -> Optional[Snapshot]:
    user_id = _get_user_id()

    # candidatos de tabela (mantém compatibilidade com versões)
    candidates = [
        "snapshot_portfolio",
        "snapshot_portfolios",
        "snapshot_portfolio_v2",
        "portfolio_snapshot",
        "snapshot",
    ]
    table = None
    for t in candidates:
        if _table_exists(t):
            table = t
            break

    if not table:
        return None

    cols = _table_columns(table)
    # escolhe colunas possíveis
    id_col = "snapshot_id" if "snapshot_id" in cols else ("id" if "id" in cols else None)
    created_col = "created_at" if "created_at" in cols else ("created" if "created" in cols else None)
    user_col = "user_id" if "user_id" in cols else ("uid" if "uid" in cols else None)

    order = ""
    if created_col:
        order = f" order by {created_col} desc"
    elif id_col:
        order = f" order by {id_col} desc"

    where = ""
    params: Dict[str, Any] = {}
    if user_id and user_col:
        where = f" where {user_col} = :uid"
        params["uid"] = user_id

    # pega 1 linha mais recente
    select_cols = "*"
    try:
        df = _query_df(f"select {select_cols} from public.{table}{where}{order} limit 1", params)
    except Exception:
        # fallback sem schema prefix
        df = _query_df(f"select {select_cols} from {table}{where}{order} limit 1", params)

    if df.empty:
        # se filtrou por user_id e não achou, tenta sem filtro (para evitar bloqueio por mismatch)
        if where:
            try:
                df = _query_df(f"select {select_cols} from public.{table}{order} limit 1")
            except Exception:
                df = _query_df(f"select {select_cols} from {table}{order} limit 1")
        if df.empty:
            return None

    row = df.iloc[0].to_dict()
    tickers = _extract_tickers(row)
    snap_id = None
    if id_col and row.get(id_col) is not None:
        snap_id = str(row.get(id_col))
    return Snapshot(
        snapshot_id=snap_id,
        tickers=tickers,
        weights=_extract_weights(row),
        selic=_extract_numeric(row, ["selic", "selic_usada", "taxa_selic"]),
        acima_benchmark=_extract_numeric(row, ["acima_benchmark", "pct_acima_benchmark", "percent_acima_benchmark"]),
        benchmark=str(row.get("benchmark") or row.get("indice_base") or row.get("index_base") or "") or None,
        segmentos=_extract_segmentos(row),
        raw=row,
    )

def _ensure_snapshot() -> Optional[Snapshot]:
    snap = _pick_session_snapshot()
    if snap and snap.tickers:
        return snap

    # se tinha snapshot_id mas sem tickers, tenta completar no DB
    dbsnap = _load_snapshot_from_db()
    if dbsnap:
        # guarda em sessão para outras páginas
        st.session_state["portfolio_salvo"] = dict(dbsnap.raw)
        if dbsnap.snapshot_id:
            st.session_state["snapshot_id"] = dbsnap.snapshot_id
        return dbsnap

    return snap  # pode ser apenas id

# ---------------------------
# Logos (não quebra se não existir)
# ---------------------------

def _get_logo_url(ticker: str) -> Optional[str]:
    # tenta reaproveitar o helper da página Básica (se existir)
    candidates = [
        ("core.tickers", "get_logo_url"),
        ("core.utils", "get_logo_url"),
        ("core.logos", "get_logo_url"),
        ("page.basica", "get_logo_url"),
        ("page.basica", "ticker_logo_url"),
    ]
    for mod_name, fn_name in candidates:
        try:
            mod = __import__(mod_name, fromlist=[fn_name])
            fn = getattr(mod, fn_name)
            url = fn(ticker)
            if url:
                return str(url)
        except Exception:
            continue
    return None

# ---------------------------
# Render
# ---------------------------

def render():
    st.set_page_config(page_title="Análises de Portfólio (Patch6)", layout="wide")

    st.markdown(_CSS, unsafe_allow_html=True)

    # header
    st.markdown(
        """
        <div class="p6-header">
          <div>
            <h1 class="p6-title">🧠 Análises de Portfólio (Patch6)</h1>
            <p class="p6-sub">
              Consolidação qualitativa baseada em evidências (RAG) + tese por empresa (LLM).
            </p>
          </div>
          <div>
            <span class="p6-pill">Janela padrão: <b>12 meses</b></span>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    snap = _ensure_snapshot()

    if not snap or (snap and not snap.tickers):
        # Em vez de "morrer" aqui, mostramos diagnóstico para destravar rápido
        st.markdown('<div class="p6-section"></div>', unsafe_allow_html=True)
        st.markdown(
            """
            <div class="p6-card">
              <div class="val" style="font-size:22px;">Nenhum portfólio encontrado</div>
              <div class="extra">Execute <b>Criação de Portfólio</b> para gerar os dados (snapshot). Se você já executou e ainda assim não aparece, o problema costuma ser <b>user_id</b> (filtro) ou <b>nome da tabela</b>.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        with st.expander("Diagnóstico (clique para ver)"):
            st.write("Chaves em st.session_state:")
            st.write(sorted(list(st.session_state.keys())))
            try:
                uid = _get_user_id()
                st.write("user_id detectado:", uid)
            except Exception as e:
                st.write("Falha ao detectar user_id:", e)

            try:
                tables = _query_df(
                    "select table_name from information_schema.tables where table_schema='public' and table_name ilike '%snapshot%' order by table_name"
                )
                st.write("Tabelas com 'snapshot' no nome (public):")
                st.dataframe(tables, use_container_width=True)
            except Exception as e:
                st.write("Falha ao listar tabelas:", e)

        return

    # -----------------------
    # DADOS SALVOS (cards)
    # -----------------------
    st.markdown('<div class="p6-section"></div>', unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)

    # selic e acima_benchmark podem estar no snapshot.raw, mas com nomes distintos.
    selic = snap.selic
    acima = snap.acima_benchmark
    n_acoes = len(snap.tickers)
    n_seg = len(snap.segmentos) if snap.segmentos else None

    def _fmt_num(v: Optional[float], suf: str = "") -> str:
        if v is None:
            return "—"
        # exibe com 2 casas e vírgula PT-BR
        s = f"{v:.2f}".replace(".", ",")
        return f"{s}{suf}"

    col1.markdown(
        f"""
        <div class="p6-card">
          <div class="lbl">Selic usada</div>
          <div class="val">{_fmt_num(selic, "%")}</div>
          <div class="extra">Taxa de referência do snapshot (criação de portfólio).</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    col2.markdown(
        f"""
        <div class="p6-card">
          <div class="lbl">Ações</div>
          <div class="val">{n_acoes}</div>
          <div class="extra">Quantidade de ativos incluídos no portfólio salvo.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    col3.markdown(
        f"""
        <div class="p6-card">
          <div class="lbl">% acima do benchmark</div>
          <div class="val">{_fmt_num(acima, "%")}</div>
          <div class="extra">Diferença percentual projetada vs índice base (se disponível).</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    col4.markdown(
        f"""
        <div class="p6-card">
          <div class="lbl">Segmentos</div>
          <div class="val">{n_seg if n_seg is not None else "—"}</div>
          <div class="extra">Diversificação setorial do portfólio (se disponível no snapshot).</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("### Ativos selecionados")
    grid_cols = st.columns(6)
    for i, tck in enumerate(snap.tickers):
        with grid_cols[i % 6]:
            logo = _get_logo_url(tck)
            if logo:
                st.markdown(
                    f"""
                    <div class="p6-asset">
                      <img src="{logo}" />
                      <div>
                        <div class="tck">{tck}</div>
                        <div class="p6-muted">ativo</div>
                      </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f"""
                    <div class="p6-asset">
                      <div style="width:28px;height:28px;border-radius:6px;background:rgba(255,255,255,0.12);display:flex;align-items:center;justify-content:center;font-weight:800;">{tck[:1]}</div>
                      <div>
                        <div class="tck">{tck}</div>
                        <div class="p6-muted">ativo</div>
                      </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    st.markdown("---")

    # Mantém compatibilidade com o seu pipeline: se existir um módulo "patch6_report"
    # com render_portfolio_report(...) o utilizamos. Caso contrário, mostramos aviso.
    st.markdown("## 📘 Relatório de análise de portfólio")

    used = False
    for mod_name, fn_name in [
        ("page.patch6_report", "render_portfolio_report"),
        ("patch6_report", "render_portfolio_report"),
        ("core.patch6_report", "render_portfolio_report"),
    ]:
        try:
            mod = __import__(mod_name, fromlist=[fn_name])
            fn = getattr(mod, fn_name)
            fn(snapshot=snap.raw, tickers=snap.tickers)  # não quebra se a função ignorar kwargs extras
            used = True
            break
        except TypeError:
            # assinatura diferente
            try:
                fn()  # type: ignore
                used = True
                break
            except Exception:
                continue
        except Exception:
            continue

    if not used:
        st.info(
            "O relatório detalhado (por empresa) depende do módulo `patch6_report` do seu projeto. "
            "Como este arquivo aqui foi enviado isolado, não consegui localizar a função `render_portfolio_report`. "
            "Quando o seu módulo estiver no repositório, ele será renderizado aqui."
        )

    # Botão de atualização de evidências (ingest+chunks) — tenta chamar o runner do seu projeto
    st.markdown("---")
    st.markdown("## 📦 Atualizar evidências")
    if st.button("Atualizar documentos", use_container_width=True):
        ran = False
        for mod_name, fn_name in [
            ("core.patch6_ingest_runner", "ingest_runner"),
            ("core.patch6_ingest", "ingest_runner"),
            ("page.patch6_teste", "ingest_runner"),
            ("patch6_teste", "ingest_runner"),
        ]:
            try:
                mod = __import__(mod_name, fromlist=[fn_name])
                fn = getattr(mod, fn_name)
                fn(tickers=snap.tickers, window_months=12, max_docs_per_ticker=80, max_pdfs_per_ticker=20, time_limit_s=25)
                ran = True
                break
            except TypeError:
                try:
                    fn(snap.tickers, 12)  # type: ignore
                    ran = True
                    break
                except Exception:
                    continue
            except Exception:
                continue
        if not ran:
            st.warning(
                "Não encontrei o runner de ingest no ambiente atual (ex.: core.patch6_ingest_runner.ingest_runner). "
                "Se ele existir no seu repositório, este botão executará ingest+chunks automaticamente."
            )
