# page/configuracoes.py
from __future__ import annotations

import datetime as dt
import traceback
from typing import Any, Dict
from zoneinfo import ZoneInfo  # Python 3.9+

import streamlit as st

from core.cvm_sync import apply_update, get_sync_status

BR_TZ = ZoneInfo("America/Sao_Paulo")


def _fmt_dt(x) -> str:
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

    # garante que a navegação permaneça em Configurações após qualquer rerun
    st.session_state["pagina_atual"] = "Configurações"

    try:
        status: Dict[str, Any] = get_sync_status() or {}
    except Exception as e:
        st.error("Banco Supabase não está configurado para esta aplicação.")
        with st.expander("Como corrigir", expanded=True):
            st.markdown(
                """Defina **SUPABASE_DB_URL** em **Secrets** no Streamlit Cloud (recomendado),
ou alternativamente as variáveis **SUPABASE_DB_USER**, **SUPABASE_DB_PASSWORD**, **SUPABASE_DB_HOST**, **SUPABASE_DB_PORT**, **SUPABASE_DB_NAME**.

Depois de salvar os Secrets, reinicie a app."""
            )
            st.markdown("**Erro capturado:**")
            st.exception(e)
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

        # Como o seu apply_update() NÃO aceita start_year/end_year etc,
        # mantemos estes inputs apenas como referência visual (opcionais).
        # Se você quiser removê-los, pode.
        default_start_year = int(st.session_state.get("cfg_start_year", 2010))
        default_end_year = int(st.session_state.get("cfg_end_year", dt.datetime.now().year))

        start_year = st.number_input(
            "Ano inicial (referência)",
            min_value=2000,
            max_value=2100,
            value=default_start_year,
            step=1,
            help="Seu apply_update() atual não usa este parâmetro; é apenas informativo.",
        )
        end_year = st.number_input(
            "Ano final (referência)",
            min_value=2000,
            max_value=2100,
            value=default_end_year,
            step=1,
            help="Seu apply_update() atual não usa este parâmetro; é apenas informativo.",
        )

        st.session_state["cfg_start_year"] = int(start_year)
        st.session_state["cfg_end_year"] = int(end_year)

        if int(end_year) < int(start_year):
            st.error("Ano final não pode ser menor que o ano inicial.")
            st.stop()

        show_debug = st.checkbox(
            "Mostrar detalhes técnicos em caso de erro",
            value=True,
            help="Exibe stack trace completo no painel ao falhar.",
        )

        run = st.button("Atualizar banco (tudo)", use_container_width=True)
        progress = st.progress(0, text="Aguardando…")
        log_box = st.empty()

        if run:
            st.session_state["pagina_atual"] = "Configurações"

            logs: list[str] = []

            def cb(pct: float, msg: str = "") -> None:
                pct = max(0.0, min(100.0, float(pct)))
                progress.progress(int(pct), text=msg or f"{pct:.0f}%")
                if msg:
                    logs.append(msg)
                    log_box.markdown("\n".join([f"- {x}" for x in logs[-20:]]))

            try:
                # ✅ chamada compatível com o seu core atual
                apply_update(progress_cb=cb)

                st.success("Atualização concluída.")
                st.session_state["pagina_atual"] = "Configurações"
                st.rerun()

            except Exception as e:
                st.session_state["pagina_atual"] = "Configurações"
                st.error("Falha ao atualizar banco. Veja detalhes abaixo.")

                if show_debug:
                    st.exception(e)
                    tb = traceback.format_exc()
                    with st.expander("Detalhes técnicos (stack trace completo)", expanded=True):
                        st.code(tb, language="python")
                        if logs:
                            st.markdown("**Logs coletados durante a execução (últimos 200):**")
                            st.code("\n".join(logs[-200:]))
                else:
                    st.error(f"Erro: {e}")

    with colB:
        st.subheader("Último log")
        notes = status.get("notes")
        if notes:
            st.code(notes)
        else:
            st.info("Sem log ainda.")
