from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional, Dict, Any

import pandas as pd
import streamlit as st

from core.db_loader import (
    load_setores_from_db,
    load_multiplos_from_db,
    load_data_from_db,
)
from core.helpers import (
    obter_setor_da_empresa,
    get_logo_url,
)
from core.yf_data import (
    get_company_info,
    get_fundamentals_yf,
    get_price,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Utilitários
# ─────────────────────────────────────────────────────────────

def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = out.columns.astype(str).str.strip().str.replace("\ufeff", "", regex=False)
    return out


def _norm_sa(ticker: str) -> str:
    t = (ticker or "").strip().upper()
    return t if t.endswith(".SA") else f"{t}.SA"


def _strip_sa(ticker: str) -> str:
    return (ticker or "").strip().upper().replace(".SA", "").strip()


def _safe_float(x) -> Optional[float]:
    try:
        v = float(x)
        return v
    except Exception:
        return None


def _fmt_pct(x: Optional[float]) -> str:
    if x is None:
        return "—"
    return f"{x:.2f}%"


def _fmt_num(x: Optional[float]) -> str:
    if x is None:
        return "—"
    return f"{x:.2f}"


def _get_setores_df_cached() -> pd.DataFrame:
    setores_df = st.session_state.get("setores_df")
    if setores_df is None or getattr(setores_df, "empty", True):
        setores_df = load_setores_from_db()
        if setores_df is None or setores_df.empty:
            return pd.DataFrame()
        setores_df = _clean_columns(setores_df)
        st.session_state["setores_df"] = setores_df
    return setores_df


def _load_db_payload(ticker_sa: str) -> Dict[str, Any]:
    """
    Carrega dados do DB (multiplos + dre) e devolve payload padronizado.
    """
    mult = load_multiplos_from_db(ticker_sa)
    dre = load_data_from_db(ticker_sa)

    if mult is None:
        mult = pd.DataFrame()
    if dre is None:
        dre = pd.DataFrame()

    if not mult.empty:
        mult = _clean_columns(mult)
    if not dre.empty:
        dre = _clean_columns(dre)

    # adiciona Ano quando possível (compatível com vários módulos)
    if not mult.empty and "Data" in mult.columns and "Ano" not in mult.columns:
        mult["Ano"] = pd.to_datetime(mult["Data"], errors="coerce").dt.year
    if not dre.empty and "Data" in dre.columns and "Ano" not in dre.columns:
        dre["Ano"] = pd.to_datetime(dre["Data"], errors="coerce").dt.year

    return {"multiplos": mult, "dre": dre}


def _extract_latest_from_multiplos(mult: pd.DataFrame) -> Dict[str, Optional[float]]:
    """
    Tenta extrair do DB os campos mais comuns para o card de múltiplos.
    Não quebra se as colunas não existirem.
    """
    if mult is None or mult.empty:
        return {"DY": None, "P_L": None, "P_VP": None}

    df = mult.copy()

    # normaliza datas
    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
        df = df.dropna(subset=["Data"]).sort_values("Data")
        last = df.iloc[-1]
    else:
        last = df.iloc[-1]

    # tenta pegar por vários nomes possíveis (robustez)
    def pick(*cols):
        for c in cols:
            if c in df.columns:
                return _safe_float(last.get(c))
        return None

    dy = pick("DY", "Dividend_Yield", "DividendYield", "dividend_yield")
    pl = pick("P/L", "PL", "P_L", "preco_lucro")
    pvp = pick("P/VP", "PVP", "P_VP", "preco_valor_patrimonial")

    # caso DY no DB esteja em fração, converte para %
    if dy is not None and dy <= 1.5:
        dy = dy * 100.0

    return {"DY": dy, "P_L": pl, "P_VP": pvp}


# ─────────────────────────────────────────────────────────────
# Render Streamlit
# ─────────────────────────────────────────────────────────────

def render():
    st.markdown("<h1 style='text-align: center;'>Empresa</h1>", unsafe_allow_html=True)

    setores_df = _get_setores_df_cached()
    if setores_df is None or setores_df.empty:
        st.error("Não foi possível carregar a base de setores.")
        st.stop()

    if "ticker" not in setores_df.columns:
        st.error("Base de setores sem coluna 'ticker'.")
        st.stop()

    # Universo de tickers (limpo)
    tickers = (
        setores_df["ticker"].astype(str)
        .str.upper()
        .str.replace(".SA", "", regex=False)
        .str.strip()
    )
    tickers = sorted({t for t in tickers.tolist() if t})

    # Sidebar: seleção
    with st.sidebar:
        st.markdown("### Seleção")
        ticker_sel = st.selectbox("Ticker", options=tickers, index=0 if tickers else None)

        st.markdown("---")
        st.markdown("### Dados externos (Yahoo Finance)")
        st.caption("Para evitar rate limit, os dados do Yahoo só são consultados sob demanda.")
        btn_yahoo = st.button("Atualizar dados do Yahoo", use_container_width=True)

    if not ticker_sel:
        st.warning("Selecione um ticker.")
        st.stop()

    ticker = _strip_sa(ticker_sel)
    ticker_sa = _norm_sa(ticker)
    logo_url = get_logo_url(ticker)

    # Setor/subsetor/segmento
    setor_info = obter_setor_da_empresa(ticker, setores_df) if setores_df is not None else {}
    if setor_info is None:
        setor_info = {}

    setor = setor_info.get("SETOR") or setor_info.get("setor") or "—"
    subsetor = setor_info.get("SUBSETOR") or setor_info.get("subsetor") or "—"
    segmento = setor_info.get("SEGMENTO") or setor_info.get("segmento") or "—"

    # Carrega DB
    db_payload = _load_db_payload(ticker_sa)
    mult = db_payload["multiplos"]
    dre = db_payload["dre"]

    # Extrai “últimos” múltiplos do DB (fallback padrão)
    db_mult = _extract_latest_from_multiplos(mult)

    # ─────────────────────────────────────────────────────────
    # Yahoo (somente sob demanda)
    # ─────────────────────────────────────────────────────────
    # Cache por ticker em session_state para evitar repetir chamadas em reruns.
    yf_key = f"yf_snapshot::{ticker}"
    yf_snapshot = st.session_state.get(yf_key, None)

    if btn_yahoo or yf_snapshot is None:
        # só chama Yahoo se usuário pediu (btn_yahoo)
        # ou se ainda não existe snapshot (primeira vez, mas sem forçar).
        if btn_yahoo:
            try:
                nome_yf, site_yf = get_company_info(ticker_sa)
                fund_yf = get_fundamentals_yf(ticker_sa)  # 1 linha
                price = get_price(ticker_sa)

                yf_snapshot = {
                    "nome": nome_yf,
                    "site": site_yf,
                    "fund": fund_yf,
                    "price": price,
                    "ts": datetime.now().isoformat(timespec="seconds"),
                }
                st.session_state[yf_key] = yf_snapshot
            except Exception as e:
                logger.debug("Falha ao atualizar Yahoo (%s): %s", ticker, e, exc_info=True)
                st.warning("Não foi possível atualizar dados do Yahoo agora (possível rate limit).")

    # ─────────────────────────────────────────────────────────
    # Header
    # ─────────────────────────────────────────────────────────
    col_a, col_b = st.columns([1, 4])
    with col_a:
        st.image(logo_url, width=70)
    with col_b:
        titulo = ticker
        if yf_snapshot and yf_snapshot.get("nome"):
            titulo = f"{yf_snapshot.get('nome')} ({ticker})"
        st.markdown(f"## {titulo}")
        st.caption(f"{setor} > {subsetor} > {segmento}")

        if yf_snapshot and yf_snapshot.get("site"):
            st.write(f"Website: {yf_snapshot.get('site')}")
        if yf_snapshot and yf_snapshot.get("ts"):
            st.caption(f"Yahoo atualizado em: {yf_snapshot.get('ts')}")

    st.markdown("---")

    # ─────────────────────────────────────────────────────────
    # Cards principais (DB como fonte base; Yahoo como complemento)
    # ─────────────────────────────────────────────────────────
    # Yahoo fundamentals (quando existir)
    yf_dy = yf_pvp = yf_pl = None
    if yf_snapshot and isinstance(yf_snapshot.get("fund"), pd.DataFrame) and not yf_snapshot["fund"].empty:
        row = yf_snapshot["fund"].iloc[0].to_dict()
        yf_dy = _safe_float(row.get("DY"))
        yf_pvp = _safe_float(row.get("P/VP"))
        yf_pl = _safe_float(row.get("P/L"))

    # DB como base; se DB vier vazio, tenta Yahoo
    dy = db_mult.get("DY") if db_mult.get("DY") is not None else yf_dy
    pvp = db_mult.get("P_VP") if db_mult.get("P_VP") is not None else yf_pvp
    pl = db_mult.get("P_L") if db_mult.get("P_L") is not None else yf_pl
    price = yf_snapshot.get("price") if yf_snapshot else None

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Preço (Yahoo)", value=_fmt_num(price))
    c2.metric("Dividend Yield", value=_fmt_pct(dy))
    c3.metric("P/VP", value=_fmt_num(pvp))
    c4.metric("P/L", value=_fmt_num(pl))

    st.caption("Observação: por padrão, o app usa DB (CVM/ETL) e consulta Yahoo apenas sob demanda.")

    st.markdown("---")

    # ─────────────────────────────────────────────────────────
    # Abas: Multiplos (DB) | DRE (DB) | Yahoo (snapshot)
    # ─────────────────────────────────────────────────────────
    tab1, tab2, tab3 = st.tabs(["Múltiplos (DB)", "DRE (DB)", "Yahoo (snapshot)"])

    with tab1:
        if mult is None or mult.empty:
            st.info("Sem dados de múltiplos no banco para este ticker.")
        else:
            st.dataframe(mult, use_container_width=True)

    with tab2:
        if dre is None or dre.empty:
            st.info("Sem dados de DRE no banco para este ticker.")
        else:
            st.dataframe(dre, use_container_width=True)

    with tab3:
        if not yf_snapshot:
            st.info("Clique em “Atualizar dados do Yahoo” para carregar snapshot.")
        else:
            st.write(
                {
                    "ticker": ticker,
                    "atualizado_em": yf_snapshot.get("ts"),
                    "preco": yf_snapshot.get("price"),
                    "nome": yf_snapshot.get("nome"),
                    "site": yf_snapshot.get("site"),
                }
            )
            fund = yf_snapshot.get("fund")
            if isinstance(fund, pd.DataFrame) and not fund.empty:
                st.dataframe(fund, use_container_width=True)
            else:
                st.info("Fundamentals do Yahoo indisponíveis (possível rate limit ou ausência de dados).")
