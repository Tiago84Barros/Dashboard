from __future__ import annotations

import datetime as dt
from typing import Any, Dict
from zoneinfo import ZoneInfo  # Python 3.9+

import pandas as pd
import streamlit as st
from sqlalchemy import text

from core.cvm_sync import apply_update, get_sync_status
from core.db_supabase import get_engine

# Pipelines novos
from cvm.prices_sync_bulk import sync_prices_universe
from cvm.multiplos_sync_universe import rebuild_multiplos_universe

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


def _get_universe_tickers(engine) -> list[str]:
    """
    Universo:
    - Preferência: cvm.setores (se existir e estiver populada)
    - Fallback: tickers presentes em cvm.demonstracoes_financeiras_dfp
    """
    # 1) setores
    try:
        df = pd.read_sql(
            text("select distinct ticker from cvm.setores where ticker is not null"),
            con=engine,
        )
        tickers = (
            df["ticker"]
            .dropna()
            .astype(str)
            .str.upper()
            .str.replace(".SA", "", regex=False)
            .tolist()
        )
        tickers = sorted(set(tickers))
        if tickers:
            return tickers
    except Exception:
        pass

    # 2) fallback: dfp
    df = pd.read_sql(
        text(
            "select distinct ticker from cvm.demonstracoes_financeiras_dfp where ticker is not null"
        ),
        con=engine,
    )
    tickers = (
        df["ticker"]
        .dropna()
        .astype(str)
        .str.upper()
        .str.replace(".SA", "", regex=False)
        .tolist()
    )
    return sorted(set(tickers))


def render() -> None:
    st.markdown("## Configurações")
    st.caption("Atualize todas as tabelas do Supabase e acompanhe o status.")

    # garante permanecer na página após rerun
    st.session_state["pagina_atual"] = "Configurações"

    try:
        status: Dict[str, Any] = get_sync_status() or {}
    except Exception:
        st.error("Banco Supabase não está configurado para esta aplicação.")
        return

    colA, colB = st.columns([1.2, 1])

    with colA:
        st.subheader("Status do sincronismo")

        last_run = status.get("last_run")
        last_ok = status.get("last_ok")
        last_error = status.get("last_error")

        st.write(
            {
                "Última execução": _fmt_dt(last_run),
                "Último sucesso": _fmt_dt(last_ok),
                "Último erro": _fmt_dt(last_error),
            }
        )

        st.divider()
        st.subheader("Parâmetros de atualização (CVM)")

        default_start_year = int(st.session_state.get("cfg_start_year", 2010))
        default_end_year = int(st.session_state.get("cfg_end_year", dt.date.today().year))
        default_years_per_run = int(st.session_state.get("cfg_years_per_run", 2))
        default_quarters_per_run = int(st.session_state.get("cfg_quarters_per_run", 8))

        start_year = st.number_input(
            "Ano inicial (DFP/ITR)",
            min_value=1995,
            max_value=dt.date.today().year,
            value=default_start_year,
            step=1,
        )
        end_year = st.number_input(
            "Ano final (DFP/ITR)",
            min_value=1995,
            max_value=dt.date.today().year,
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

        # persistência dos inputs
        st.session_state["cfg_start_year"] = int(start_year)
        st.session_state["cfg_end_year"] = int(end_year)
        st.session_state["cfg_years_per_run"] = int(years_per_run)
        st.session_state["cfg_quarters_per_run"] = int(quarters_per_run)

        if int(end_year) < int(start_year):
            st.error("Ano final não pode ser menor que o ano inicial.")
            st.stop()

        st.divider()
        st.subheader("Parâmetros de atualização (Preços/Múltiplos)")

        modo_seguro = st.checkbox(
            "Modo seguro (limitar tickers por execução)",
            value=True,
            help="Recomendado em deploy. Desmarque para rodar o universo inteiro num clique.",
        )
        max_tickers = st.number_input(
            "Máximo de tickers por execução (modo seguro)",
            min_value=10,
            max_value=5000,
            value=150,
            step=10,
            disabled=not modo_seguro,
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
                    log_box.markdown("\n".join([f"- {x}" for x in logs[-14:]]))

            try:
                # -------------------------
                # 1) Atualiza CVM (como já fazia)
                # -------------------------
                cb(1, "Iniciando sincronismo CVM (DFP/ITR)…")
                apply_update(
                    start_year=int(start_year),
                    end_year=int(end_year),
                    years_per_run=int(years_per_run),
                    quarters_per_run=int(quarters_per_run),
                    progress_cb=cb,
                )
                cb(60, "Sincronismo CVM concluído. Preparando preços (2010→hoje)…")

                # -------------------------
                # 2) Atualiza preços 2010→hoje para TODO universo
                # -------------------------
                engine = get_engine()
                tickers = _get_universe_tickers(engine)
                if not tickers:
                    raise RuntimeError(
                        "Universo de tickers vazio (cvm.setores e cvm.demonstracoes_financeiras_dfp)."
                    )

                if modo_seguro:
                    tickers = tickers[: int(max_tickers)]
                    cb(62, f"Modo seguro: processando {len(tickers)} tickers nesta execução.")
                else:
                    cb(62, f"Processando universo completo: {len(tickers)} tickers.")

                cb(65, "Baixando e gravando preços (2010→hoje) em cvm.prices_b3…")
                stats = sync_prices_universe(engine, tickers)
                cb(
                    85,
                    f"Preços concluídos: OK={stats.get('ok')} Falhas={stats.get('fail')} Total={stats.get('total')}. Recalculando múltiplos…",
                )

                # -------------------------
                # 3) Rebuild múltiplos do universo (usa prices_b3 year_end + DFP)
                # -------------------------
                res = rebuild_multiplos_universe(engine)
                if not res.get("ok"):
                    raise RuntimeError(f"Rebuild de múltiplos falhou: {res.get('error') or res.get('msg')}")

                cb(100, f"Concluído: múltiplos atualizados ({res.get('rows')} linhas).")
                st.success("Atualização completa concluída (CVM + Preços + Múltiplos).")

                st.session_state["pagina_atual"] = "Configurações"
                st.cache_data.clear()
                st.rerun()

            except Exception as e:
                st.error(f"Falha ao atualizar: {e}")
                st.session_state["pagina_atual"] = "Configurações"

    with colB:
        st.subheader("Último log")
        notes = status.get("notes")
        if notes:
            st.code(notes)
        else:
            st.info("Sem log ainda.")
