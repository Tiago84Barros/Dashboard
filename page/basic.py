with st.sidebar:
    if st.button("Atualizar dados", key="refresh_button"):
        st.cache_data.clear()
        st.experimental_rerun()

    ticker_input = st.text_input("Buscar ticker (ex.: PETR4)", key="ticker_box")
    if ticker_input.strip():
        ticker = ticker_input.upper()
        if not ticker.endswith(".SA"):
            ticker += ".SA"
        st.session_state["ticker"] = ticker
    elif "ticker" in st.session_state:
        del st.session_state["ticker"]
