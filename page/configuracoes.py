
from __future__ import annotations

import contextlib
import importlib
import io
import json
import traceback
from dataclasses import dataclass
from typing import Any

import pandas as pd
import streamlit as st
from sqlalchemy import text

from core.db import get_engine

TZ_LOCAL = "America/Sao_Paulo"


@dataclass
class JobResult:
    ok: bool
    stdout: str
    stderr: str
    traceback_text: str
    current_summary: dict[str, Any] | None


def _safe_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _to_local_datetime_str(value: Any) -> str | None:
    if value is None:
        return None
    try:
        ts = pd.to_datetime(value, utc=True)
        return ts.tz_convert(TZ_LOCAL).strftime("%d/%m/%Y %H:%M")
    except Exception:
        try:
            ts = pd.to_datetime(value)
            if getattr(ts, "tzinfo", None) is None:
                return ts.strftime("%d/%m/%Y %H:%M")
            return ts.tz_convert(TZ_LOCAL).strftime("%d/%m/%Y %H:%M")
        except Exception:
            return str(value)


def _parse_json_lines(blob: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for raw in (blob or "").splitlines():
        line = raw.strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            events.append(payload)
    return events


def get_latest_ingestion_summary(pipeline: str) -> dict[str, Any] | None:
    sql = text(
        """
        SELECT
            pipeline,
            status,
            started_at,
            finished_at,
            rows_inserted,
            rows_updated,
            rows_skipped,
            warnings_count,
            errors_count,
            metrics,
            params
        FROM public.ingestion_log
        WHERE pipeline = :pipeline
          AND status = 'success'
        ORDER BY finished_at DESC
        LIMIT 1
        """
    )

    try:
        engine = get_engine()
        with engine.connect() as conn:
            row = conn.execute(sql, {"pipeline": pipeline}).mappings().first()
    except Exception:
        return None

    if not row:
        return None

    metrics = _safe_json(row.get("metrics"))
    params = _safe_json(row.get("params"))

    return {
        "pipeline": row.get("pipeline"),
        "status": row.get("status"),
        "last_update": _to_local_datetime_str(row.get("finished_at")),
        "rows_inserted": int(row.get("rows_inserted") or 0),
        "rows_updated": int(row.get("rows_updated") or 0),
        "rows_skipped": int(row.get("rows_skipped") or 0),
        "warnings_count": int(row.get("warnings_count") or 0),
        "errors_count": int(row.get("errors_count") or 0),
        "tickers": int(metrics.get("df_filtrado_tickers") or 0),
        "ano_inicial": params.get("ano_inicial"),
        "ultimo_ano": metrics.get("ultimo_ano_disponivel") or params.get("ultimo_ano"),
    }


def _extract_current_summary(stdout: str, pipeline: str) -> dict[str, Any] | None:
    events = _parse_json_lines(stdout)
    summary_event = None
    final_success = None

    for event in events:
        if event.get("pipeline") != pipeline:
            continue
        if event.get("event") == "summary" and event.get("status") == "success":
            summary_event = event
        if event.get("stage") == "final_success":
            final_success = event

    if not summary_event and not final_success:
        return None

    metrics = _safe_json(summary_event.get("metrics") if summary_event else {})
    params = _safe_json(summary_event.get("params") if summary_event else {})

    return {
        "pipeline": pipeline,
        "status": "success",
        "last_update": _to_local_datetime_str(summary_event.get("finished_at") if summary_event else None),
        "rows_inserted": int((summary_event or {}).get("rows_inserted") or (final_success or {}).get("rows") or 0),
        "rows_updated": int((summary_event or {}).get("rows_updated") or 0),
        "rows_skipped": int((summary_event or {}).get("rows_skipped") or 0),
        "warnings_count": int((summary_event or {}).get("warnings_count") or 0),
        "errors_count": int((summary_event or {}).get("errors_count") or 0),
        "tickers": int(metrics.get("df_filtrado_tickers") or (final_success or {}).get("tickers") or 0),
        "ano_inicial": (final_success or {}).get("year_start") or params.get("ano_inicial"),
        "ultimo_ano": (final_success or {}).get("year_end") or metrics.get("ultimo_ano_disponivel") or params.get("ultimo_ano"),
    }


def _format_period(summary: dict[str, Any] | None) -> str:
    if not summary:
        return "-"
    start = summary.get("ano_inicial")
    end = summary.get("ultimo_ano")
    if start and end:
        return f"{start} → {end}"
    if end:
        return str(end)
    return "-"


def render_summary_card(summary: dict[str, Any] | None, title: str = "Última atualização") -> None:
    st.subheader(title)
    if not summary:
        st.info("Nenhuma atualização bem-sucedida registrada ainda.")
        return

    period = _format_period(summary)
    last_update = summary.get("last_update") or "-"
    rows_inserted = int(summary.get("rows_inserted") or 0)
    tickers = int(summary.get("tickers") or 0)
    errors_count = int(summary.get("errors_count") or 0)
    warnings_count = int(summary.get("warnings_count") or 0)

    st.success(
        "\n".join(
            [
                f"Última atualização: {last_update}",
                f"Linhas inseridas: {rows_inserted}",
                f"Empresas (tickers): {tickers}",
                f"Período: {period}",
                f"Erros: {errors_count}",
                f"Warnings: {warnings_count}",
            ]
        )
    )


def _run_job(module_name: str, main_func_name: str, pipeline: str) -> JobResult:
    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()
    traceback_text = ""
    ok = False

    try:
        mod = importlib.import_module(module_name)
        mod = importlib.reload(mod)

        with contextlib.redirect_stdout(stdout_buffer), contextlib.redirect_stderr(stderr_buffer):
            getattr(mod, main_func_name)()

        ok = True
    except Exception:
        traceback_text = traceback.format_exc()

    stdout_value = stdout_buffer.getvalue()
    stderr_value = stderr_buffer.getvalue()
    current_summary = _extract_current_summary(stdout_value, pipeline)

    return JobResult(
        ok=ok,
        stdout=stdout_value,
        stderr=stderr_value,
        traceback_text=traceback_text,
        current_summary=current_summary,
    )


def _render_job_section(
    *,
    title: str,
    description: str,
    button_label: str,
    pipeline: str,
    module_name: str,
    main_func_name: str = "main",
    state_key: str,
) -> None:
    st.markdown(f"## {title}")
    st.info(description)

    latest_summary = get_latest_ingestion_summary(pipeline)
    latest_key = f"{state_key}_last_success"
    if latest_key not in st.session_state:
        st.session_state[latest_key] = latest_summary

    button_clicked = st.button(button_label, key=f"btn_{state_key}", use_container_width=True)

    if button_clicked:
        with st.spinner("Executando rotina..."):
            result = _run_job(module_name, main_func_name, pipeline)

        if result.ok:
            if result.current_summary:
                st.session_state[latest_key] = result.current_summary
            else:
                refreshed = get_latest_ingestion_summary(pipeline)
                if refreshed:
                    st.session_state[latest_key] = refreshed
            st.success("Rotina concluída (sem exceções Python).")
        else:
            st.error("Falha ao executar a rotina.")
            with st.expander("Ver detalhes do erro"):
                st.code(result.traceback_text or result.stderr or result.stdout)

    render_summary_card(st.session_state.get(latest_key))


def main() -> None:
    st.title("Configurações")

    with st.expander("Ações de manutenção", expanded=False):
        st.caption("Use os botões abaixo para atualizar as bases do sistema.")

    _render_job_section(
        title="1. Demonstrações anuais (DFP)",
        description=(
            "Este botão executa o script pickup/dados_cvm_dfp.py para baixar os DFP consolidados da CVM, "
            "consolidar e gravar em public.Demonstracoes_Financeiras no Supabase."
        ),
        button_label="Atualizar Demonstrações Anuais (DFP)",
        pipeline="dfp",
        module_name="pickup.dados_cvm_dfp",
        state_key="dfp",
    )

    st.divider()

    _render_job_section(
        title="2. Demonstrações trimestrais (ITR/TRI)",
        description=(
            "Este botão executa o script pickup/dados_cvm_itr.py para baixar os ITR consolidados da CVM, "
            "consolidar e gravar em public.Demonstracoes_Financeiras_TRI no Supabase."
        ),
        button_label="Atualizar Demonstrações Trimestrais (TRI/ITR)",
        pipeline="itr",
        module_name="pickup.dados_cvm_itr",
        state_key="itr",
    )


if __name__ == "__main__":
    main()
