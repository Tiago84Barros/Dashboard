from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Optional

import streamlit as st
from sqlalchemy import text
from sqlalchemy.engine import Engine


@st.cache_resource
def _engine() -> Engine:
    from core.db.engine import get_engine
    return get_engine()


def _fmt_dt(x: Any) -> str:
    if not x:
        return "—"
    try:
        if isinstance(x, dt.datetime):
            return x.astimezone().strftime("%d/%m/%Y %H:%M")
        return str(x)
    except Exception:
        return str(x)


def _get_sync_status(engine: Engine) -> Dict[str, Any]:
    # Lê último log
    latest = None
    try:
        q = text(
            """
            select run_at, status, last_year, remote_latest_year, message
            from cvm.sync_log
            order by run_at desc
            limit 1
            """
        )
        with engine.begin() as conn:
            latest = conn.execute(q).mappings().first()
    except Exception:
        latest = None

    # last_year direto da DFP (coerente com seu ingest)
    last_year = None
    try:
        with engine.begin() as conn:
            last_year = conn.execute(text("select max(extract(year from data))::int from cvm.demonstracoes_financeiras")).scalar()
    except Exception:
        last_year = None

    return {
        "last_year": last_year,
        "last_run_at": (latest["run_at"] if latest else None),
        "last_status": (latest["status"] if latest else None),
        "remote_latest_year": (latest["remote_latest_year"] if latest else None),
        "notes": (latest["message"] if latest else None),
    }


def render() -> None:
    engine = _engine()

    st.markdown(
        """
        <style>
          .cfg-title {font-size: 2rem; font-weight: 800; margin-bottom: .25rem;}
          .cfg-sub {color: #6b7280; margin-bottom: 1.25rem;}
          .card {border: 1px solid rgba(0,0,0,.08); border-radius: 16px; padding: 16px; background: rgba(255,255,255,.75);}
          .pill {display:inline-block; padding: 4px 10px; border-radius: 999px; font-size:.85rem; border:1px solid rgba(0,0,0,.10);}
          .pill-ok {background: rgba(16,185,129,.12);}
          .pill-warn {background: rgba(245,158,11,.12);}
          .pill-bad {background: rgba(239,68,68,.12);}
          .muted {color:#6b7280;}
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<div class='cfg-title'>Configurações</div>", unsafe_allow_html=True)
    st.markdown("<div class='cfg-sub'>Atualize todas as tabelas do Supabase necessárias para o app.</div>", unsafe_allow_html=True)

    status = _get_sync_status(engine)
    last_year = status.get("last_year")
    last_run_at = status.get("last_run_at")
    last_status = status.get("last_status")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Último ano (DFP) no banco", value=str(last_year) if last_year else "—")
    with c2:
        st.metric("Última atualização", value=_fmt_dt(last_run_at))
    with c3:
        st.metric("Status", value=str(last_status) if last_status else "—")
    with c4:
        if last_status == "success":
            st.markdown("<span class='pill pill-ok'>Base OK</span>", unsafe_allow_html=True)
        elif last_status == "error":
            st.markdown("<span class='pill pill-bad'>Falha na última atualização</span>", unsafe_allow_html=True)
        else:
            st.markdown("<span class='pill pill-warn'>Sem histórico</span>", unsafe_allow_html=True)

    st.markdown("")

    left, right = st.columns([1, 1], vertical_alignment="top")

    with left:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.subheader("Atualizar agora (tudo)")
        st.caption("Executa: DFP + ITR + Setores + Macro + Métricas + Score")

        now_year = dt.datetime.now().year
        default_start = int(last_year) if last_year else max(2010, now_year - 2)

        colA, colB, colC = st.columns(3)
        with colA:
            start_year = st.number_input("Ano inicial (DFP)", min_value=2000, max_value=now_year, value=default_start)
        with colB:
            end_year = st.number_input("Ano final (DFP)", min_value=2000, max_value=now_year, value=now_year)
        with colC:
            years_per_run = st.number_input("Anos por execução", min_value=1, max_value=10, value=1)

        st.markdown("<div class='muted'>Dica: seu DFP é incremental (um clique processa X anos do intervalo).</div>", unsafe_allow_html=True)

        run = st.button("Atualizar tudo", use_container_width=True)

        progress = st.progress(0, text="Aguardando início…")
        log_box = st.empty()
        logs: list[str] = []

        def progress_cb(pct: float, msg: str = "") -> None:
            pct = max(0.0, min(100.0, float(pct)))
            progress.progress(int(pct), text=msg or f"Progresso: {pct:.0f}%")
            if msg:
                logs.append(msg)
                log_box.markdown("\n".join([f"- {x}" for x in logs[-14:]]))

        if run:
            from core.sync.all_sync import SyncConfig, apply_full_update

            try:
                progress_cb(1, "Iniciando sincronização completa…")
                cfg = SyncConfig(
                    start_year=int(start_year),
                    end_year=int(end_year),
                    years_per_run=int(years_per_run),
                    run_dfp=True,
                    run_itr=True,
                    run_setores=True,
                    run_macro=True,
                    run_metrics_builder=True,
                    run_scoring=True,
                )
                apply_full_update(engine, cfg, progress_cb=progress_cb)
                st.success("Atualização completa concluída.")
                st.rerun()
            except Exception as e:
                progress.progress(0, text="Falha na atualização.")
                st.error(f"Erro na atualização completa: {e}")

        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.subheader("Resumo e logs")

        st.markdown(
            f"""
            - **Último ano no banco (DFP):** {last_year if last_year else "—"}
            - **Última atualização:** {_fmt_dt(last_run_at)}
            - **Status:** {last_status if last_status else "—"}
            """.strip()
        )

        notes = status.get("notes")
        if notes:
            st.caption(str(notes)[:1200])

        st.markdown("</div>", unsafe_allow_html=True)
