# configuracoes_progress_v2.py
# Monitor visual de progresso no Streamlit

def _render_v2_progress():
    engine = get_engine()

    row = engine.execute("""
        SELECT *
        FROM cvm_ingestion_runs
        ORDER BY started_at DESC
        LIMIT 1
    """).fetchone()

    if not row:
        st.info("Nenhuma execução ainda.")
        return

    metrics = row.metrics or {}

    st.subheader("Progresso CVM V2")

    st.write("Status:", row.status)
    st.write("Ano atual:", metrics.get("current_year"))
    st.write("Progresso:", f"{metrics.get('years_done', 0)} / {metrics.get('years_total', 0)}")

    progress = 0
    if metrics.get("years_total"):
        progress = metrics.get("years_done", 0) / metrics["years_total"]

    st.progress(progress)

    st.write("Linhas inseridas:", metrics.get("inserted_rows_accum"))
