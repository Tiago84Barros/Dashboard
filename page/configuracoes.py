from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Optional

import streamlit as st

from core.cvm_sync import apply_update, get_sync_status


def _fmt_dt(x: Optional[str]) -> str:
    if not x:
        return "—"
    try:
        d = dt.datetime.fromisoformat(str(x).replace("Z", "+00:00"))
        return d.astimezone().strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(x)


def render() -> None:
    st.markdown("## Configurações")
    st.caption("Atualize todas as tabelas do Supabase e acompanhe o status.")

    status: Dict[str, Any] = get_sync_status() or {}
    last_year = status.get("last_year")
    last_run_at = status.get("last_run_at")
    remote_latest_year = status.get("remote_latest_year")
    has_updates = status.get("has_updates")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Último ano no banco", value=str(last_year) if last_year else "—")
    with c2:
        st.metric("Última atualização", value=_fmt_dt(last_run_at))
    with c3:
        st.metric("Ano alvo", value=str(remote_latest_year) if remote_latest_year else "—")
    with c4:
        if has_updates is True:
            st.warning("Há atualizações pendentes")
        elif has_updates is False:
            st.success("Base atualizada")
        else:
            st.info("Status indisponível")

    st.divider()

    colA, colB = st.columns([1, 1])
    with colA:
        st.subheader("Atualizar agora")

        start_year = st.number_input("Ano inicial", min_value=2000, max_value=2100, value=2010, step=1)
        end_year = st.number_input("Ano final", min_value=2000, max_value=2100, value=dt.datetime.now().year, step=1)

        years_per_run = st.number_input("DFP por clique (anos)", min_value=1, max_value=10, value=1, step=1)
        quarters_per_run = st.number_input("ITR por clique (trimestres)", min_value=1, max_value=12, value=1, step=1)

        run = st.button("Atualizar banco (tudo)", use_container_width=True)
        progress = st.progress(0, text="Aguardando…")
        log_box = st.empty()

        if run:
            logs: list[str] = []

            def cb(pct: float, msg: str = "") -> None:
                pct = max(0.0, min(100.0, float(pct)))
                progress.progress(int(pct), text=msg or f"{pct:.0f}%")
                if msg:
                    logs.append(msg)
                    log_box.markdown("\n".join([f"- {x}" for x in logs[-14:]]))

            try:
                apply_update(
                    start_year=int(start_year),
                    end_year=int(end_year),
                    years_per_run=int(years_per_run),
                    quarters_per_run=int(quarters_per_run),
                    progress_cb=cb,
                )
                st.success("Atualização concluída.")
                st.rerun()
            except Exception as e:
                st.error(f"Falha ao atualizar: {e}")

    with colB:
        st.subheader("Último log")
        notes = status.get("notes")
        if notes:
            st.code(notes)
        else:
            st.info("Sem log ainda.")
