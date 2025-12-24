# page/configuracoes.py
from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Optional

import streamlit as st

from core.db.engine import get_engine
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
    engine = get_engine()

    st.markdown(
        """
        <style>
          /* remove “respiros” excessivos */
          .block-container { padding-top: 1.2rem; padding-bottom: 2rem; }

          .cfg-title {font-size: 2rem; font-weight: 800; margin-bottom: .25rem;}
          .cfg-sub {color: #6b7280; margin-bottom: 1.25rem;}

          .card {border: 1px solid rgba(0,0,0,.08); border-radius: 16px; padding: 16px; background: rgba(255,255,255,.85);}
          .pill {display:inline-block; padding: 4px 10px; border-radius: 999px; font-size:.85rem; border:1px solid rgba(0,0,0,.10);}
          .pill-ok {background: rgba(16,185,129,.12);}
          .pill-warn {background: rgba(245,158,11,.12);}
          .pill-bad {background: rgba(239,68,68,.12);}

          /* deixa o botão bem “produto” */
          .stButton > button { border-radius: 12px; padding: .6rem 1rem; font-weight: 700; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<div class='cfg-title'>Configurações</div>", unsafe_allow_html=True)
    st.markdown("<div class='cfg-sub'>Atualize a base do Supabase e acompanhe o status em tempo real.</div>", unsafe_allow_html=True)

    status: Dict[str, Any] = get_sync_status(engine=engine) or {}
    last_year = status.get("last_year")
    last_run_at = status.get("last_run_at")
    remote_latest_year = status.get("remote_latest_year")
    has_updates = status.get("has_updates")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Último ano no banco (DFP)", value=str(last_year) if last_year else "—")
    with c2:
        st.metric("Última atualização", value=_fmt_dt(last_run_at))
    with c3:
        st.metric("Último ano disponível (CVM)", value=str(remote_latest_year) if remote_latest_year else "—")
    with c4:
        if has_updates is True:
            st.markdown("<span class='pill pill-warn'>Há atualizações pendentes</span>", unsafe_allow_html=True)
        elif has_updates is False:
            st.markdown("<span class='pill pill-ok'>Base está atualizada</span>", unsafe_allow_html=True)
        else:
            st.markdown("<span class='pill pill-bad'>Status indisponível</span>", unsafe_allow_html=True)

    st.markdown("")
    left, right = st.columns([1, 1])

    with left:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.subheader("Atualização do banco (Supabase)")

        st.caption("Recomendado: rode incrementalmente para evitar travar o Streamlit Cloud.")

        dfp_years = st.number_input("DFP: anos por execução", min_value=1, max_value=10, value=1, step=1)
        itr_quarters = st.number_input("ITR/TRI: trimestres por execução", min_value=1, max_value=12, value=1, step=1)

        colA, colB = st.columns(2)
        with colA:
            start_year = st.number_input("Ano inicial", min_value=2000, max_value=2100, value=2010, step=1)
        with colB:
            end_year = st.number_input("Ano final", min_value=2000, max_value=2100, value=2025, step=1)

        run = st.button("Atualizar agora", use_container_width=True)

        progress = st.progress(0, text="Aguardando início…")
        log_box = st.empty()

        if run:
            logs = []

            def progress_cb(pct: float, msg: str = "") -> None:
                pct = max(0.0, min(100.0, float(pct)))
                progress.progress(int(pct), text=msg or f"Progresso: {pct:.0f}%")
                if msg:
                    logs.append(msg)
                    log_box.markdown("\n".join([f"- {x}" for x in logs[-14:]]))

            try:
                progress_cb(1, "Inicializando…")
                apply_update(
                    engine=engine,
                    progress_cb=progress_cb,
                    dfp_years_per_run=int(dfp_years),
                    itr_quarters_per_run=int(itr_quarters),
                    start_year=int(start_year),
                    end_year=int(end_year),
                )
                progress_cb(100, "Concluído.")
                st.success("Atualização concluída com sucesso.")
                st.rerun()
            except Exception as e:
                progress.progress(0, text="Falha na atualização.")
                st.error(f"Erro na atualização: {e}")

        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.subheader("Resumo e diagnóstico")

        st.markdown(
            f"""
            - **Último ano no banco (DFP):** {last_year if last_year else "—"}
            - **Última atualização:** {_fmt_dt(last_run_at)}
            - **Último ano disponível (CVM):** {remote_latest_year if remote_latest_year else "—"}
            - **Atualizações pendentes:** {("Sim" if has_updates else "Não") if has_updates in [True, False] else "Indisponível"}
            """.strip()
        )

        st.caption("O log completo de execuções fica em `public.sync_log` no Supabase.")

        st.markdown("</div>", unsafe_allow_html=True)
