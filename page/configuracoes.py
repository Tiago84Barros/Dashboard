from __future__ import annotations

import datetime as dt
import importlib
from typing import Any, Callable, Dict, Optional
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
    st.caption("Um clique para sincronizar CVM e Macro (BCB) no Supabase.")

    st.session_state["pagina_atual"] = "Configurações"

    # ─────────────────────────────────────────────────────────────
    # Engine (Supabase)
    # ─────────────────────────────────────────────────────────────
    try:
        engine = get_engine()
    except Exception as e:
        st.error("Banco Supabase não está configurado para esta aplicação.")
        with st.expander("Como corrigir"):
            st.code(str(e))
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
    # Parâmetros CVM (sem botão separado)
    # ─────────────────────────────────────────────────────────────
    st.subheader("Parâmetros (CVM)")
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

    st.divider()

    # ─────────────────────────────────────────────────────────────
    # Botão único de sincronização
    # ─────────────────────────────────────────────────────────────
    st.subheader("Sincronização")
    st.caption("Executa: 1) CVM (DFP/ITR)  2) Macro RAW (BCB→macro_bcb)  3) Macro WIDE (→info_economica/…_mensal)")

    sync_all = st.button("Sincronizar tudo (CVM + Macro)", use_container_width=True)
    progress = st.progress(0, text="Aguardando…")
    log_box = st.empty()

    if sync_all:
        logs: list[str] = []

        def log(msg: str) -> None:
            logs.append(msg)
            _render_logs(logs, log_box)

        def set_pct(p: int, msg: str) -> None:
            p = max(0, min(100, int(p)))
            progress.progress(p, text=msg)
            log(msg)

        # Callback CVM (o apply_update usa percentual)
        def cvm_cb(pct: float, msg: str = "") -> None:
            pct = max(0.0, min(100.0, float(pct)))
            # CVM ocupa 0–60% da barra
            mapped = int(pct * 0.60)
            progress.progress(mapped, text=msg or f"CVM {pct:.0f}%")
            if msg:
                log(msg)

        try:
            # 1) CVM
            set_pct(1, "Iniciando sincronização CVM (DFP/ITR)...")
            apply_update(
                start_year=int(start_year),
                end_year=int(end_year),
                years_per_run=int(years_per_run),
                quarters_per_run=int(quarters_per_run),
                progress_cb=cvm_cb,
            )
            set_pct(60, "CVM concluído.")

            # 2) Macro RAW
            set_pct(62, "Importando ingest RAW do BCB...")
            mod_raw = _import_first(
                "ingest.macro_bcb_raw_ingest",
                "macro_bcb_raw_ingest",
            )
            if not hasattr(mod_raw, "run"):
                raise RuntimeError(
                    "macro_bcb_raw_ingest.py não possui run(engine). "
                    "Crie def run(engine): ingest_macro_bcb(engine)."
                )

            set_pct(65, "Executando ingest RAW (BCB → cvm.macro_bcb)...")
            mod_raw.run(engine)  # type: ignore[attr-defined]
            set_pct(80, "Macro RAW concluído.")

            # 3) Macro WIDE
            set_pct(82, "Importando transformação macro (wide)...")
            mod_wide = _import_first(
                "macro_bcb_ingest",
                "ingest.macro_bcb_ingest",
            )
            if not hasattr(mod_wide, "run"):
                raise RuntimeError("macro_bcb_ingest.py não possui run(engine, ...).")

            set_pct(85, "Gerando info_economica / info_economica_mensal...")

            def wide_progress(msg: str) -> None:
                # mantém barra entre 85 e 99
                progress.progress(90, text=msg)
                log(msg)

            # ⚠️ Evita o seu erro: run() não aceita progress_cb em alguns módulos
            try:
                mod_wide.run(engine, progress_cb=wide_progress)  # type: ignore[attr-defined]
            except TypeError:
                # fallback se não aceitar progress_cb
                mod_wide.run(engine)  # type: ignore[attr-defined]

            set_pct(100, "Sincronização completa.")
            st.success("Sincronização concluída com sucesso.")
            st.rerun()

        except Exception as e:
            st.error(f"Falha na sincronização: {e}")
            log(f"ERRO: {e}")

    st.divider()

    st.subheader("Último log (CVM)")
    notes = status.get("notes")
    if notes:
        st.code(notes)
    else:
        st.info("Sem log ainda.")
