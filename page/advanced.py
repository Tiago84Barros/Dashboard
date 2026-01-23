from __future__ import annotations

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

from core.db_loader import (
    load_setores_from_db,
    load_data_from_db,
    load_multiplos_from_db,
    load_macro_summary,
)

from core.helpers import get_logo_url, determinar_lideres
from core.scoring import (
    calcular_score_acumulado,
    penalizar_plato,
)
from core.portfolio import (
    gerir_carteira,
    calcular_patrimonio_selic_macro,
)
from core.weights import get_pesos
from core.yf_data import baixar_precos, coletar_dividendos


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────
def _norm_sa(ticker: str) -> str:
    t = (ticker or "").strip().upper()
    return t if t.endswith(".SA") else f"{t}.SA"


def _strip_sa(ticker: str) -> str:
    return (ticker or "").strip().upper().replace(".SA", "").strip()


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = out.columns.astype(str).str.strip().str.replace("\ufeff", "", regex=False)
    return out


def _safe_year_count_from_dre(dre: pd.DataFrame) -> int:
    if dre is None or dre.empty:
        return 0
    if "Data" not in dre.columns:
        return 0
    years = pd.to_datetime(dre["Data"], errors="coerce").dt.year
    return int(years.dropna().nunique())


@st.cache_data(show_spinner=False, ttl=6 * 60 * 60)  # 6h
def _get_setores_cached() -> pd.DataFrame:
    df = load_setores_from_db()
    if df is None:
        return pd.DataFrame()
    return _clean_columns(df)


@st.cache_data(show_spinner=False, ttl=6 * 60 * 60)  # 6h
def _get_macro_cached() -> pd.DataFrame:
    df = load_macro_summary()
    if df is None:
        return pd.DataFrame()
    df = _clean_columns(df)
    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
        df = df.dropna(subset=["Data"]).sort_values("Data").reset_index(drop=True)
    return df


# ─────────────────────────────────────────────────────────────
# Render
# ─────────────────────────────────────────────────────────────
def render() -> None:
    st.markdown("<h1 style='text-align:center'>Análise Avançada de Ações</h1>", unsafe_allow_html=True)

    setores_df = _get_setores_cached()
    if setores_df is None or setores_df.empty:
        st.error("Não foi possível carregar a base de setores do banco.")
        st.stop()

    required_cols = {"SETOR", "SUBSETOR", "SEGMENTO", "ticker"}
    if not required_cols.issubset(set(setores_df.columns)):
        st.error(f"Base de setores não contém as colunas esperadas: {sorted(required_cols)}")
        st.stop()

    dados_macro = _get_macro_cached()
    if dados_macro is None or dados_macro.empty or "Data" not in dados_macro.columns:
        st.error("Não foi possível carregar/normalizar os dados macroeconômicos.")
        st.stop()

    # Sidebar filtros
    with st.sidebar:
        setor_sel = st.selectbox("Setor:", sorted(setores_df["SETOR"].dropna().astype(str).unique().tolist()))

        subsetores = setores_df.loc[setores_df["SETOR"] == setor_sel, "SUBSETOR"].dropna().astype(str).unique().tolist()
        subsetor_sel = st.selectbox("Subsetor:", sorted(subsetores))

        segmentos = setores_df.loc[
            (setores_df["SETOR"] == setor_sel) & (setores_df["SUBSETOR"] == subsetor_sel),
            "SEGMENTO",
        ].dropna().astype(str).unique().tolist()
        segmento_sel = st.selectbox("Segmento:", sorted(segmentos))

        st.markdown("Perfil de empresa:")
        perfil = st.radio(
            "Perfil de empresa:",
            options=["Crescimento (<10 anos)", "Estabelecida (≥10 anos)", "Todas"],
            index=2,
            label_visibility="collapsed",
        )

        with st.expander("Scoring (opções)", expanded=False):
            anos_minimos = st.slider("Anos mínimos (scoring):", min_value=3, max_value=12, value=4)
            st.caption("Mais anos mínimos = mais robustez, porém menor universo.")
        executar = st.button("Executar análise avançada")

    st.markdown("#### Diagnóstico (dados do banco)")
    st.caption("Este painel usa dados fundamentalistas do banco e preços do Yahoo (quando disponível).")

    df_filtrado = setores_df[
        (setores_df["SETOR"] == setor_sel) &
        (setores_df["SUBSETOR"] == subsetor_sel) &
        (setores_df["SEGMENTO"] == segmento_sel)
    ].copy()

    if df_filtrado.empty:
        st.info("Nenhuma empresa encontrada para os filtros selecionados.")
        return

    tickers = df_filtrado["ticker"].astype(str).map(_strip_sa).tolist()
    tickers = [t for t in tickers if t]

    # Aplica perfil (anos de DRE)
    tickers_ok: list[str] = []
    for tk in tickers:
        dre = load_data_from_db(_norm_sa(tk))
        anos = _safe_year_count_from_dre(dre) if isinstance(dre, pd.DataFrame) else 0

        if perfil.startswith("Crescimento") and anos >= 10:
            continue
        if perfil.startswith("Estabelecida") and anos < 10:
            continue
        tickers_ok.append(tk)

    tickers_ok = sorted(set(tickers_ok))
    if len(tickers_ok) <= 1:
        st.info("Filtro retornou universo insuficiente para análise (<= 1 ticker).")
        return

    # Cards "Empresas no filtro"
    st.markdown("## Empresas no filtro")
    cards = df_filtrado[df_filtrado["ticker"].astype(str).map(_strip_sa).isin(set(tickers_ok))].copy()
    cards = cards.drop_duplicates(subset=["ticker"]).reset_index(drop=True)

    if not cards.empty:
        for i in range(0, len(cards), 2):
            cols = st.columns(2, gap="large")
            for j in range(2):
                if i + j >= len(cards):
                    continue
                row = cards.iloc[i + j]
                tk = _strip_sa(str(row["ticker"]))
                dre = load_data_from_db(_norm_sa(tk))
                anos_dre = _safe_year_count_from_dre(dre) if isinstance(dre, pd.DataFrame) else 0
                with cols[j]:
                    st.markdown(
                        f"""
                        <div style="border:1px solid #ddd;border-radius:10px;padding:14px;background:#fff;text-align:center;">
                            <img src="{get_logo_url(tk)}" width="52" />
                            <div style="margin-top:8px;font-weight:700;">{row.get('nome_empresa', tk)}</div>
                            <div style="color:#666;font-size:13px;">({tk})</div>
                            <div style="color:#999;font-size:12px;margin-top:6px;">Histórico DRE: {anos_dre} ano(s)</div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

    if not executar:
        return

    # Carrega payloads (DRE + múltiplos) do banco
    lista_empresas: list[dict] = []
    for tk in tickers_ok:
        tk_sa = _norm_sa(tk)
        mult = load_multiplos_from_db(tk_sa)
        dre = load_data_from_db(tk_sa)

        if not isinstance(mult, pd.DataFrame) or not isinstance(dre, pd.DataFrame):
            continue
        if mult.empty or dre.empty:
            continue

        mult = _clean_columns(mult)
        dre = _clean_columns(dre)

        if "Data" in mult.columns and "Ano" not in mult.columns:
            mult["Ano"] = pd.to_datetime(mult["Data"], errors="coerce").dt.year
        if "Data" in dre.columns and "Ano" not in dre.columns:
            dre["Ano"] = pd.to_datetime(dre["Data"], errors="coerce").dt.year

        # tenta puxar nome da base de setores (se houver)
        nome_db = (
            df_filtrado.loc[df_filtrado["ticker"].astype(str).map(_strip_sa) == tk, "nome_empresa"]
            if "nome_empresa" in df_filtrado.columns
            else None
        )
        nome = str(nome_db.iloc[0]) if (nome_db is not None and len(nome_db) > 0) else tk

        lista_empresas.append({"ticker": tk, "nome": nome, "multiplos": mult, "dre": dre})

    if len(lista_empresas) <= 1:
        st.info("Sem dados suficientes no banco para executar o scoring no filtro selecionado.")
        return

    # Score
    pesos = get_pesos(setor_sel)
    setores_empresa = {e["ticker"]: {"SETOR": setor_sel, "SUBSETOR": subsetor_sel, "SEGMENTO": segmento_sel} for e in lista_empresas}

    try:
        score = calcular_score_acumulado(lista_empresas, setores_empresa, pesos, dados_macro, anos_minimos=anos_minimos)
    except Exception as e:
        st.error(f"Falha no cálculo do score: {e}")
        return

    if score is None or score.empty:
        st.info("Score vazio para o filtro selecionado.")
        return

    # Preços (Yahoo) para penalidade de platô e backtest
    tickers_yf = [_norm_sa(e["ticker"]) for e in lista_empresas]
    precos = baixar_precos(tickers_yf)

    if precos is None or precos.empty:
        st.warning("Não foi possível baixar preços para o segmento selecionado.")
        return

    precos.index = pd.to_datetime(precos.index, errors="coerce")
    precos = precos.dropna(how="all")
    if precos.empty:
        st.warning("Preços vazios após normalização.")
        return

    # Penalidade de platô (mensal)
    try:
        precos_mensal = precos.resample("M").last()
        score = penalizar_plato(score, precos_mensal, meses=12, penal=0.30)
    except Exception:
        pass

    # Dividendos (opcional; pode falhar com rate-limit)
    dividendos = {}
    try:
        dividendos = coletar_dividendos(tickers_yf)
    except Exception:
        dividendos = {}

    # Backtest
    try:
        lideres = determinar_lideres(score)
        if lideres is None or lideres.empty:
            st.info("Não foi possível determinar líderes para o filtro selecionado.")
            return

        patrimonio_empresas, datas_aportes = gerir_carteira(precos, score, lideres, dividendos)
        if patrimonio_empresas is None or patrimonio_empresas.empty:
            st.info("Backtest vazio.")
            return

        patrimonio_selic = calcular_patrimonio_selic_macro(dados_macro, datas_aportes)
        if patrimonio_selic is None or patrimonio_selic.empty:
            st.info("Benchmark Selic indisponível.")
            return

        final_emp = float(patrimonio_empresas.iloc[-1].drop("Patrimônio", errors="ignore").sum())
        final_selic = float(patrimonio_selic.iloc[-1]["Tesouro Selic"])

        st.markdown("## Resultado do filtro")
        st.success(f"Valor final da estratégia: R$ {final_emp:,.2f} | Tesouro Selic: R$ {final_selic:,.2f}")

        # Gráfico simples
        st.markdown("### Evolução do patrimônio")
        df_plot = pd.DataFrame({
            "Estratégia": patrimonio_empresas.drop(columns=["Patrimônio"], errors="ignore").sum(axis=1),
            "Tesouro Selic": patrimonio_selic["Tesouro Selic"],
        }).dropna()

        fig, ax = plt.subplots(figsize=(10, 4))
        df_plot["Estratégia"].plot(ax=ax, label="Estratégia")
        df_plot["Tesouro Selic"].plot(ax=ax, label="Tesouro Selic")
        ax.set_ylabel("R$")
        ax.grid(True, linestyle="--", alpha=0.4)
        ax.legend()
        st.pyplot(fig)

    except Exception as e:
        st.error(f"Falha no backtest: {e}")
        return
