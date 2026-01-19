# advanced.py
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px

from core.db_loader import load_empresas, load_scores



# ============================================================
# CONFIGURAÇÃO DA PÁGINA
# ============================================================
st.set_page_config(
    page_title="Análise Avançada | Segmento vs Líderes",
    layout="wide"
)

st.title("Análise Avançada — Segmento vs Líderes")
st.caption(
    "Comparação econômica direta entre a mediana do segmento e as empresas líderes, "
    "com base no score fundamentalista."
)

# ============================================================
# CONSTANTES E REGRAS ECONÔMICAS
# ============================================================
LOWER_IS_BETTER = {
    "P/L", "PL", "P_L",
    "P/VP", "PVP", "P_VP",
    "Divida_Liquida_EBITDA",
    "Divida_Liquida",
    "Endividamento_Total",
    "Alavancagem_Financeira",
}

RESERVED_COLS = {
    "Ano", "Data", "ticker", "Ticker",
    "Empresa", "nome", "Nome",
    "CNPJ", "Setor", "Subsetor", "Segmento"
}


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================
def ensure_year_column(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "Ano" not in df.columns and "Data" in df.columns:
        df = df.copy()
        df["Ano"] = pd.to_datetime(df["Data"], errors="coerce").dt.year
    return df


def list_numeric_indicators(df: pd.DataFrame) -> list[str]:
    if df is None or df.empty:
        return []
    indicators = []
    for col in df.columns:
        if col in RESERVED_COLS:
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        if series.notna().sum() >= 3:
            indicators.append(col)
    return indicators


def build_long_df(empresas, source_attr: str, indicator: str) -> pd.DataFrame:
    rows = []
    for e in empresas:
        df = getattr(e, source_attr, None)
        if df is None or df.empty:
            continue

        df = ensure_year_column(df)
        if "Ano" not in df.columns or indicator not in df.columns:
            continue

        tmp = df[["Ano", indicator]].copy()
        tmp["Ano"] = pd.to_numeric(tmp["Ano"], errors="coerce")
        tmp[indicator] = pd.to_numeric(tmp[indicator], errors="coerce")
        tmp = tmp.dropna(subset=["Ano", indicator])
        if tmp.empty:
            continue

        if source_attr == "mult":
            tmp = tmp.groupby("Ano", as_index=False)[indicator].mean()
        else:
            tmp = tmp.groupby("Ano", as_index=False)[indicator].sum()

        for _, r in tmp.iterrows():
            rows.append({
                "Ano": int(r["Ano"]),
                "Ticker": e.ticker,
                "Valor": float(r[indicator])
            })

    return pd.DataFrame(rows)


def segment_vs_leaders(df: pd.DataFrame, top_tickers: list[str], year: int, indicator: str):
    df = df[df["Ano"] == year].copy()
    if df.empty:
        return None, None

    segment_median = df["Valor"].median()

    leaders = (
        df[df["Ticker"].isin(top_tickers)]
        .groupby("Ticker", as_index=False)["Valor"]
        .mean()
    )

    leaders["Mediana_Segmento"] = segment_median
    leaders["Gap_%"] = (leaders["Valor"] / segment_median - 1) * 100

    if indicator in LOWER_IS_BETTER:
        leaders["Status"] = np.where(leaders["Gap_%"] < 0, "Melhor", "Pior")
        leaders = leaders.sort_values("Gap_%")
    else:
        leaders["Status"] = np.where(leaders["Gap_%"] > 0, "Melhor", "Pior")
        leaders = leaders.sort_values("Gap_%", ascending=False)

    return segment_median, leaders


def render_panel(df_long, indicator, top_tickers, year, max_bars):
    median, leaders = segment_vs_leaders(df_long, top_tickers, year, indicator)
    if median is None or leaders is None or leaders.empty:
        st.warning("Sem dados suficientes para este indicador.")
        return

    leader = leaders.iloc[0]

    c1, c2, c3 = st.columns(3)
    c1.metric("Mediana do Segmento", f"{median:,.2f}")
    c2.metric("Líder do Segmento", f"{leader['Ticker']} | {leader['Valor']:,.2f}")
    c3.metric("Gap vs Segmento", f"{leader['Gap_%']:+.1f}%")

    fig = px.bar(
        leaders.head(max_bars),
        x="Gap_%",
        y="Ticker",
        orientation="h",
        title="Líderes vs Mediana do Segmento (Gap %)"
    )
    fig.update_layout(height=350, xaxis_title="Gap (%)", yaxis_title="")
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(
        leaders[["Ticker", "Valor", "Mediana_Segmento", "Gap_%", "Status"]]
        .round(2),
        use_container_width=True
    )


# ============================================================
# CARGA DE DADOS
# ============================================================
empresas = load_empresas()
score = load_scores()

ano_base = int(pd.to_numeric(score["Ano"], errors="coerce").max())

score_ano = score[pd.to_numeric(score["Ano"], errors="coerce") == ano_base].copy()
score_ano["Score_Ajustado"] = pd.to_numeric(score_ano["Score_Ajustado"], errors="coerce")
score_ano = score_ano.dropna(subset=["ticker", "Score_Ajustado"])
score_ano = score_ano.sort_values("Score_Ajustado", ascending=False)

# ============================================================
# CONTROLES DO USUÁRIO
# ============================================================
st.markdown("## Configurações")

col1, col2, col3 = st.columns(3)
with col1:
    top_n = st.slider("Quantidade de líderes (Top N)", 3, 20, 5)
with col2:
    max_bars = st.slider("Máx. líderes no gráfico", 3, 10, 5)
with col3:
    fonte = st.selectbox("Fonte de dados", ["Múltiplos", "DRE"], index=0)

top_tickers = score_ano["ticker"].head(top_n).astype(str).tolist()

source_attr = "mult" if fonte == "Múltiplos" else "dre"

# ============================================================
# LISTA DE INDICADORES DISPONÍVEIS
# ============================================================
available_indicators = set()
for e in empresas:
    df = getattr(e, source_attr, None)
    if df is None or df.empty:
        continue
    df = ensure_year_column(df)
    available_indicators.update(list_numeric_indicators(df))

available_indicators = sorted(available_indicators)

if not available_indicators:
    st.warning("Nenhum indicador numérico disponível.")
    st.stop()

indicator = st.selectbox("Indicador", available_indicators)

# ============================================================
# EXECUÇÃO PRINCIPAL
# ============================================================
df_long = build_long_df(empresas, source_attr, indicator)

st.markdown("---")
st.markdown(f"## {indicator} — Ano base {ano_base}")

render_panel(
    df_long=df_long,
    indicator=indicator,
    top_tickers=top_tickers,
    year=ano_base,
    max_bars=max_bars
)
