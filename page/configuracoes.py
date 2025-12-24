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
    st.markdown(
        """
        <style>
          .cfg-title {font-size: 2rem; font-weight: 800; margin-bottom: .25rem;}
          .cfg-sub {color: rgba(255,255,255,.65); margin-bottom: 1.25rem;}
          .card {border: 1px solid rgba(255,255,255,.08); border-radius: 16px; padding: 16px; background: rgba(255,255,255,.03);}
          .pill {display:inline-block; padding: 4px 10px; border-radius: 999px; font-size:.85rem; border:1px solid rgba(255,255,255,.10);}
          .pill-ok {background: rgba(16,185,129,.12);}
          .pill-warn {background: rgba(245,158,11,.12);}
          .pill-bad {background: rgba(239,68,68,.12);}
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<div class='cfg-title'>Configurações</div>", unsafe_allow_html=True)
    st.markdown("<div class='cfg-sub'>Atualize todas as tabelas do Supabase e acompanhe o status.</div>", unsafe_allow_html=True)

    status: Dict[str, Any] = get_sync_status()

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
            st.markdown("<span class='pill pill-warn'>Há atualizações</span>", unsafe_allow_html=True)
        elif has_updates is False:
            st.markdown("<span class='pill pill-ok'>Atualizado</span>", unsafe_allow_html=True)
        else:
            st.markdown("<span class='pill pill-bad'>Status indisponível</span>", unsafe_allow_html=True)

    st.markdown("")
    left, right = st.columns([1, 1])

    with left:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.subheader("Atualizar banco (todas as tabelas)")

        y1, y2 = st.columns(2)
        with y1:
            start_year = st.number_input("Ano inicial", min_value=2000, max_value=2100, value=2010, step=1)
        with y2:
            end_year = st.number_input("Ano final", min_value=2000, max_value=2100, value=dt.datetime.now().year, step=1)

        c5, c6 = st.columns(2)
        with c5:
            years_per_run = st.number_input("DFP por clique (anos)", min_value=1, max_value=10, value=1, step=1)
        with c6:
            quarters_per_run = st.number_input("ITR por clique (trimestres)", min_value=1, max_value=12, value=1, step=1)

        run = st.button("Atualizar agora", use_container_width=True)
        progress = st.progress(0, text="Aguardando…")
        log_box = st.empty()

        if run:
            logs = []

            def progress_cb(pct: float, msg: str = "") -> None:
                pct = max(0.0, min(100.0, float(pct)))
                progress.progress(int(pct), text=msg or f"Progresso: {pct:.0f}%")
                if msg:
                    logs.append(msg)
                    log_box.markdown("\n".join([f"- {x}" for x in logs[-12:]]))

            try:
                apply_update(
                    start_year=int(start_year),
                    end_year=int(end_year),
                    years_per_run=int(years_per_run),
                    quarters_per_run=int(quarters_per_run),
                    progress_cb=progress_cb,
                )
                st.success("Atualização concluída.")
                st.rerun()
            except Exception as e:
                st.error(f"Falha na atualização: {e}")

        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.subheader("Diagnóstico")

        st.markdown(
            f"""
            - **Último ano no banco:** {last_year if last_year else "—"}
            - **Última atualização:** {_fmt_dt(last_run_at)}
            - **Ano alvo:** {remote_latest_year if remote_latest_year else "—"}
            - **Pendências:** {("Sim" if has_updates else "Não") if has_updates in [True, False] else "Indisponível"}
            """.strip()
        )
        notes = status.get("notes")
        if notes:
            st.caption(notes)
        st.markdown("</div>", unsafe_allow_html=True)
