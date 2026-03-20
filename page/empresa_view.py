
import pandas as pd
import streamlit as st

def normalize_df(df):
    if df is None:
        return df
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    if "data" in df.columns and "Data" not in df.columns:
        df = df.rename(columns={"data": "Data"})
    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
    return df

def render_empresa_view(load_data_from_db, load_multiplos_from_db, ticker):
    df = load_data_from_db(ticker)

    if df is None or df.empty:
        st.warning("Dados financeiros não encontrados para este ticker.")
        return

    df = normalize_df(df)

    st.subheader("Demonstrações Financeiras (Histórico do Banco)")

    if "Data" not in df.columns:
        st.info("Sem coluna de data para exibir.")
    else:
        df = df.dropna(subset=["Data"]).sort_values("Data")
        st.dataframe(df.tail(10), use_container_width=True)

    st.subheader("Gráfico de Múltiplos (Histórico do Banco)")

    mult_hist = load_multiplos_from_db(ticker)

    if mult_hist is None or mult_hist.empty:
        st.info("Histórico de múltiplos não encontrado no banco.")
    else:
        mult_hist = normalize_df(mult_hist)

        if "Data" not in mult_hist.columns:
            st.info("Histórico de múltiplos sem coluna de data.")
            return

        mult_hist = mult_hist.dropna(subset=["Data"]).sort_values("Data")

        st.line_chart(mult_hist.set_index("Data").select_dtypes(include="number"))
