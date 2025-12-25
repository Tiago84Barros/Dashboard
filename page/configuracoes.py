from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo  # Python 3.9+

import streamlit as st

from core.cvm_sync import apply_update, get_sync_status
from ingest.macro_bcb_raw_ingest import run as run_macro_raw
from macro_bcb_ingest import run as run_macro_wide




BR_TZ = ZoneInfo("America/Sao_Paulo")

def _fmt_dt(x):
    if not x:
        return "—"
    try:
        d = dt.datetime.fromisoformat(str(x).replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone(BR_TZ).strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(x)



def render() -> None:
    st.markdown("## Configurações")
    st.caption("Atualize todas as tabelas do Supabase e acompanhe o status.")

    # FIX: garante que a navegação permaneça em Configurações após qualquer rerun
    st.session_state["pagina_atual"] = "Configurações"

    try:
        status: Dict[str, Any] = get_sync_status() or {}
    except Exception as e:
        st.error("Banco Supabase não está configurado para esta aplicação.")
        with st.expander("Como corrigir"):
            st.markdown(
                """Defina **SUPABASE_DB_URL** em **Secrets** no Streamlit Cloud (recomendado),
ou alternativamente as variáveis **SUPABASE_DB_USER**, **SUPABASE_DB_PASSWORD**, **SUPABASE_DB_HOST**, **SUPABASE_DB_PORT**, **SUPABASE_DB_NAME**.

Depois de salvar os Secrets, reinicie a app.

Erro capturado:
`{}`""".format(e)
            )
        return

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

        # FIX: manter valores entre reruns para não frustrar o usuário
        default_start_year = int(st.session_state.get("cfg_start_year", 2010))
        default_end_year = int(st.session_state.get("cfg_end_year", dt.datetime.now().year))
        default_years_per_run = int(st.session_state.get("cfg_years_per_run", 1))
        default_quarters_per_run = int(st.session_state.get("cfg_quarters_per_run", 1))

        start_year = st.number_input(
            "Ano inicial",
            min_value=2000,
            max_value=2100,
            value=default_start_year,
            step=1,
        )
        end_year = st.number_input(
            "Ano final",
            min_value=2000,
            max_value=2100,
            value=default_end_year,
            step=1,
        )

        years_per_run = st.number_input(
            "DFP por clique (anos)",
            min_value=1,
            max_value=10,
            value=default_years_per_run,
            step=1,
        )
        quarters_per_run = st.number_input(
            "ITR por clique (trimestres)",
            min_value=1,
            max_value=12,
            value=default_quarters_per_run,
            step=1,
        )

        # Persistência dos inputs
        st.session_state["cfg_start_year"] = int(start_year)
        st.session_state["cfg_end_year"] = int(end_year)
        st.session_state["cfg_years_per_run"] = int(years_per_run)
        st.session_state["cfg_quarters_per_run"] = int(quarters_per_run)

        # FIX: validação simples para evitar chamadas inválidas
        if int(end_year) < int(start_year):
            st.error("Ano final não pode ser menor que o ano inicial.")
            st.stop()

        run = st.button("Atualizar banco (tudo)", use_container_width=True)
        progress = st.progress(0, text="Aguardando…")
        log_box = st.empty()

        if run:
            # FIX: reafirma a página antes da execução (sobrevive a rerun do Streamlit)
            st.session_state["pagina_atual"] = "Configurações"

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

                # FIX CRÍTICO: manter Configurações após o rerun
                st.session_state["pagina_atual"] = "Configurações"
                st.rerun()

            except Exception as e:
                st.error(f"Falha ao atualizar: {e}")
                # Mantém na página mesmo após erro
                st.session_state["pagina_atual"] = "Configurações"

    with colB:
        st.subheader("Último log")
        notes = status.get("notes")
        if notes:
            st.code(notes)
        else:
            st.info("Sem log ainda.")
