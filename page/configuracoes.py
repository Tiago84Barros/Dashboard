from __future__ import annotations

import datetime as dt
import importlib
from typing import Any, Dict
from zoneinfo import ZoneInfo  # Python 3.9+

import streamlit as st

from core.cvm_sync import apply_update, get_sync_status
from core.db_supabase import get_engine


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


def _import_first(*module_paths: str):
    """
    Tenta importar o primeiro módulo disponível entre os paths informados.
    Retorna o módulo importado.
    """
    last_err: Exception | None = None
    for p in module_paths:
        try:
            return importlib.import_module(p)
        except Exception as e:  # noqa: BLE001
            last_err = e
            continue
    if last_err:
        raise last_err
    raise RuntimeError("Falha inesperada ao importar módulo.")


def _render_logs(logs: list[str], container) -> None:
    if not logs:
        container.info("Sem logs ainda.")
        return
    container.markdown("\n".join([f"- {x}" for x in logs[-18:]]))


def render() -> None:
    st.markdown("## Configurações")
    st.caption("Atualize as tabelas do Supabase e acompanhe o status.")

    # Mantém navegação na página após reruns
    st.session_state["pagina_atual"] = "Configurações"

    # ─────────────────────────────────────────────────────────────
    # Engine (Supabase)
    # ─────────────────────────────────────────────────────────────
    try:
        engine = get_engine()
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

    # ─────────────────────────────────────────────────────────────
    # Status CVM Sync
    # ─────────────────────────────────────────────────────────────
    try:
        status: Dict[str, Any] = get_sync_status() or {}
    except Exception:
        status = {}

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

    # ─────────────────────────────────────────────────────────────
    # Layout em duas colunas
    # ─────────────────────────────────────────────────────────────
    colA, colB = st.columns([1, 1])

    # ============================================================
    # COLUNA A — Atualização CVM (DFP/ITR etc.)
    # ============================================================
    with colA:
        st.subheader("Atualizar CVM (DFP/ITR)")

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

        st.session_state["cfg_start_year"] = int(start_year)
        st.session_state["cfg_end_year"] = int(end_year)
        st.session_state["cfg_years_per_run"] = int(years_per_run)
        st.session_state["cfg_quarters_per_run"] = int(quarters_per_run)

        if int(end_year) < int(start_year):
            st.error("Ano final não pode ser menor que o ano inicial.")
            st.stop()

        run_cvm = st.button("Atualizar banco (CVM)", use_container_width=True)
        progress_cvm = st.progress(0, text="Aguardando…")
        log_box_cvm = st.empty()

        if run_cvm:
            st.session_state["pagina_atual"] = "Configurações"

            logs: list[str] = []

            def cb(pct: float, msg: str = "") -> None:
                pct = max(0.0, min(100.0, float(pct)))
                progress_cvm.progress(int(pct), text=msg or f"{pct:.0f}%")
                if msg:
                    logs.append(msg)
                    _render_logs(logs, log_box_cvm)

            try:
                apply_update(
                    start_year=int(start_year),
                    end_year=int(end_year),
                    years_per_run=int(years_per_run),
                    quarters_per_run=int(quarters_per_run),
                    progress_cb=cb,
                )
                st.success("Atualização CVM concluída.")
                st.session_state["pagina_atual"] = "Configurações"
                st.rerun()
            except Exception as e:
                st.error(f"Falha ao atualizar CVM: {e}")
                st.session_state["pagina_atual"] = "Configurações"

    # ============================================================
    # COLUNA B — Atualização Macro (BCB) em 2 etapas
    # ============================================================
    with colB:
        st.subheader("Atualizar Macro (BCB)")

        st.caption(
            "Fluxo recomendado: 1) Ingest RAW (BCB → cvm.macro_bcb) "
            "2) Gerar tabelas analíticas (→ info_economica / info_economica_mensal)."
        )

        run_macro = st.button("Atualizar Macro (BCB)", use_container_width=True)
        progress_macro = st.progress(0, text="Aguardando…")
        log_box_macro = st.empty()

        if run_macro:
            st.session_state["pagina_atual"] = "Configurações"

            logs: list[str] = []

            def log(msg: str) -> None:
                logs.append(msg)
                _render_logs(logs, log_box_macro)

            def set_pct(p: int, msg: str) -> None:
                p = max(0, min(100, int(p)))
                progress_macro.progress(p, text=msg)
                log(msg)

            try:
                # 1) Importa e roda o RAW ingest
                # Suporta: ingest/macro_bcb_raw_ingest.py OU macro_bcb_raw_ingest.py na raiz
                set_pct(5, "Importando módulo de ingest RAW (BCB)...")
                mod_raw = _import_first(
                    "ingest.macro_bcb_raw_ingest",
                    "macro_bcb_raw_ingest",
                )
                if not hasattr(mod_raw, "run"):
                    raise RuntimeError(
                        "O módulo macro_bcb_raw_ingest.py não possui a função run(engine). "
                        "Crie def run(engine): ingest_macro_bcb(engine)."
                    )

                set_pct(15, "Executando ingest RAW (BCB → cvm.macro_bcb)...")
                mod_raw.run(engine)  # type: ignore[attr-defined]
                set_pct(55, "RAW atualizado com sucesso.")

                # 2) Importa e roda o WIDE ingest (transformações)
                # Suporta: macro_bcb_ingest.py na raiz ou em ingest/ (se você mover no futuro)
                set_pct(60, "Importando módulo de transformação macro (wide)...")
                mod_wide = _import_first(
                    "macro_bcb_ingest",
                    "ingest.macro_bcb_ingest",
                )
                if not hasattr(mod_wide, "run"):
                    raise RuntimeError(
                        "O módulo macro_bcb_ingest.py não possui a função run(engine, progress_cb=...)."
                    )

                set_pct(70, "Gerando tabelas analíticas (info_economica / info_economica_mensal)...")

                # progress_cb opcional (se o seu macro_bcb_ingest suportar)
                def wide_progress(msg: str) -> None:
                    # Mantém barra indo até 95% durante o wide
                    # (sem depender de percentuais internos)
                    current = min(95, 70 + max(0, min(25, len(logs))))
                    progress_macro.progress(current, text=msg)
                    log(msg)

                try:
                    mod_wide.run(engine, progress_cb=wide_progress)  # type: ignore[attr-defined]
                except TypeError:
                    # caso seu run(engine) não aceite progress_cb
                    mod_wide.run(engine)  # type: ignore[attr-defined]

                set_pct(100, "Macro (BCB) atualizada com sucesso.")
                st.success("Atualização macro concluída.")
                st.session_state["pagina_atual"] = "Configurações"
                st.rerun()

            except Exception as e:
                st.error(f"Falha ao atualizar Macro (BCB): {e}")
                st.session_state["pagina_atual"] = "Configurações"

        st.divider()

        st.subheader("Último log (CVM)")
        notes = status.get("notes")
        if notes:
            st.code(notes)
        else:
            st.info("Sem log ainda.")
