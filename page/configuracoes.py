from __future__ import annotations

import io
import json
import os
import traceback
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from typing import Any, Optional

import pandas as pd
import streamlit as st

try:
    from core.db import get_engine
    from sqlalchemy import text
except Exception:  # pragma: no cover
    get_engine = None
    text = None


LOCAL_TZ = "America/Sao_Paulo"
PIPELINES_COM_TICKERS = {"dfp", "itr", "multiplos_dfp", "multiplos_itr"}


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def _format_dt(value: Any) -> str:
    if value in (None, ""):
        return "—"
    try:
        dt = pd.to_datetime(value, utc=True)
        if getattr(dt, "tzinfo", None) is None:
            dt = dt.tz_localize("UTC")
        dt = dt.tz_convert(LOCAL_TZ)
        return dt.strftime("%d/%m/%Y %H:%M")
    except Exception:
        try:
            if isinstance(value, datetime):
                return value.strftime("%d/%m/%Y %H:%M")
        except Exception:
            pass
        return str(value)


def _normalize_year(value: Any) -> Optional[int]:
    if value in (None, "", "auto"):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _summary_kind(pipeline_name: Optional[str]) -> str:
    if pipeline_name in PIPELINES_COM_TICKERS:
        return "tickers"
    if pipeline_name == "macro":
        return "macro"
    return "default"


def _extract_summary_from_stdout(stdout_text: str, pipeline_name: Optional[str] = None) -> Optional[dict[str, Any]]:
    summary_event: Optional[dict[str, Any]] = None
    final_success_event: Optional[dict[str, Any]] = None

    for raw_line in stdout_text.splitlines():
        line = raw_line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            evt = json.loads(line)
        except Exception:
            continue

        event_name = str(evt.get("event", "")).strip()
        if event_name == "summary" and str(evt.get("status", "")).lower() == "success":
            summary_event = evt
        elif event_name == "pipeline_log" and evt.get("stage") == "final_success":
            final_success_event = evt

    if summary_event:
        metrics = summary_event.get("metrics") or {}
        params = summary_event.get("params") or {}
        return {
            "kind": _summary_kind(summary_event.get("pipeline") or pipeline_name),
            "pipeline": summary_event.get("pipeline") or pipeline_name,
            "finished_at": summary_event.get("finished_at"),
            "rows_inserted": _safe_int(summary_event.get("rows_inserted")),
            "tickers": _safe_int(metrics.get("df_filtrado_tickers")),
            "year_start": _normalize_year(params.get("ano_inicial")),
            "year_end": _normalize_year(metrics.get("ultimo_ano_disponivel") or params.get("ultimo_ano")),
            "errors_count": _safe_int(summary_event.get("errors_count")),
            "warnings_count": _safe_int(summary_event.get("warnings_count")),
        }

    if final_success_event:
        return {
            "kind": _summary_kind(final_success_event.get("pipeline") or pipeline_name),
            "pipeline": final_success_event.get("pipeline") or pipeline_name,
            "finished_at": final_success_event.get("ts"),
            "rows_inserted": _safe_int(final_success_event.get("rows")),
            "tickers": _safe_int(final_success_event.get("tickers")),
            "year_start": _normalize_year(final_success_event.get("year_start")),
            "year_end": _normalize_year(final_success_event.get("year_end")),
            "errors_count": 0,
            "warnings_count": 0,
        }

    return None


def _load_latest_success_summary(pipeline_name: str) -> Optional[dict[str, Any]]:
    if not pipeline_name or get_engine is None or text is None:
        return None

    try:
        engine = get_engine()
        query = text(
            """
            SELECT
                pipeline,
                status,
                started_at,
                finished_at,
                rows_inserted,
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
        with engine.connect() as conn:
            row = conn.execute(query, {"pipeline": pipeline_name}).mappings().first()

        if not row:
            return None

        metrics = row.get("metrics") or {}
        params = row.get("params") or {}
        return {
            "kind": _summary_kind(pipeline_name),
            "pipeline": row.get("pipeline"),
            "finished_at": row.get("finished_at"),
            "rows_inserted": _safe_int(row.get("rows_inserted")),
            "tickers": _safe_int(metrics.get("df_filtrado_tickers")),
            "year_start": _normalize_year(params.get("ano_inicial")),
            "year_end": _normalize_year(metrics.get("ultimo_ano_disponivel") or params.get("ultimo_ano")),
            "errors_count": _safe_int(row.get("errors_count")),
            "warnings_count": _safe_int(row.get("warnings_count")),
        }
    except Exception:
        return None


def _render_summary(summary: Optional[dict[str, Any]], *, empty_message: str = "Nenhuma atualização bem-sucedida registrada ainda.") -> None:
    st.markdown("### Última atualização")
    if not summary:
        st.info(empty_message)
        return

    year_start = summary.get("year_start")
    year_end = summary.get("year_end")
    if year_start is None and year_end is None:
        period_text = "—"
    elif year_start is None:
        period_text = f"— → {year_end}"
    elif year_end is None:
        period_text = f"{year_start} → —"
    else:
        period_text = f"{year_start} → {year_end}"

    lines = [
        f"Última atualização: {_format_dt(summary.get('finished_at'))}",
        f"Linhas inseridas: {_safe_int(summary.get('rows_inserted'))}",
    ]

    if summary.get("kind") == "tickers":
        lines.append(f"Empresas (tickers): {_safe_int(summary.get('tickers'))}")

    lines.extend(
        [
            f"Período: {period_text}",
            f"Erros: {_safe_int(summary.get('errors_count'))}",
            f"Warnings: {_safe_int(summary.get('warnings_count'))}",
        ]
    )

    st.success("\n".join(lines))


def _run_job(
    *,
    job_key: str,
    button_label: str,
    info_text: str,
    status_label: str,
    module_import_path: str,
    module_attr_name: str,
    main_func_name: str = "main",
    pipeline_name: Optional[str] = None,
) -> None:
    if job_key not in st.session_state:
        st.session_state[job_key] = False

    col1, col2 = st.columns([1, 2], gap="large")

    with col1:
        run = st.button(
            button_label,
            use_container_width=True,
            disabled=st.session_state[job_key],
        )

    with col2:
        st.info(info_text)

    with st.expander("Ações de manutenção", expanded=False):
        if st.button(f"Resetar trava do botão ({button_label})"):
            st.session_state[job_key] = False
            st.success("Trava resetada.")
            st.rerun()

    summary_placeholder = st.empty()
    latest_summary = _load_latest_success_summary(pipeline_name) if pipeline_name else None
    with summary_placeholder.container():
        _render_summary(latest_summary)

    if run:
        st.session_state[job_key] = True

        if not os.getenv("SUPABASE_DB_URL"):
            st.error("SUPABASE_DB_URL não está definida. Configure em Secrets/Env Vars e tente novamente.")
            st.session_state[job_key] = False
            st.stop()

        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()

        try:
            with st.status(status_label, expanded=True) as status:
                status.write(f"Importando módulo `{module_import_path}` …")

                with redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
                    mod = __import__(module_import_path, fromlist=[module_attr_name])
                    status.write(f"Executando `{module_attr_name}.{main_func_name}()` …")
                    getattr(mod, main_func_name)()

                status.update(label="Execução finalizada.", state="complete")

            out = stdout_buf.getvalue().strip()
            summary = (_extract_summary_from_stdout(out, pipeline_name=pipeline_name) if out else None) or (
                _load_latest_success_summary(pipeline_name) if pipeline_name else None
            )

            with summary_placeholder.container():
                _render_summary(summary)

            st.success("Rotina concluída (sem exceções Python).")

            try:
                st.cache_data.clear()
            except Exception:
                pass

        except Exception as e:
            st.error("Falha ao executar a rotina. Traceback completo:")
            st.code("".join(traceback.format_exception(type(e), e, e.__traceback__)), language="text")

            out = stdout_buf.getvalue().strip()
            err = stderr_buf.getvalue().strip()
            if out or err:
                with st.expander("Detalhes do erro", expanded=False):
                    if out:
                        st.markdown("#### stdout")
                        st.code(out, language="text")
                    if err:
                        st.markdown("#### stderr")
                        st.code(err, language="text")

        finally:
            st.session_state[job_key] = False


def render() -> None:
    st.header("Configurações")
    st.caption(
        "Use esta seção para executar rotinas de atualização das tabelas no Supabase. "
        "A execução ocorre no servidor do Streamlit e grava nas tabelas do schema public."
    )

    st.subheader("Atualização de Base")
    st.write(
        "**Ordem recomendada (parcial):**\n"
        "1) Demonstrações completas (DFP/anual)\n"
        "2) Demonstrações trimestrais (ITR/TRI)\n"
        "3) Setores/Subsetores/Segmentos (B3)\n"
        "4) Informações econômicas (macro Brasil)\n"
        "5) Multiplos (DFP/anual)\n"
        "6) Multiplos (ITR)\n"
    )

    st.markdown("### Diagnóstico rápido")
    st.write("SUPABASE_DB_URL definida?", bool(os.getenv("SUPABASE_DB_URL")))

    st.divider()

    st.markdown("## 0. Correlação CVM → Ticker (B3)")
    with st.expander("Detalhes / Variáveis de ambiente (CVM→Ticker)", expanded=False):
        st.write("B3_INSTRUMENTOS_URL:", os.getenv("B3_INSTRUMENTOS_URL", "(não definido)"))
        st.caption(
            "Esta rotina baixa o cadastro da CVM (CD_CVM + CNPJ) e cruza com o arquivo da B3 "
            "(Ticker + CNPJ do emissor) usando CNPJ raiz. "
            "Grava em public.cvm_to_ticker."
        )

    _run_job(
        job_key="job_cvm_ticker_running",
        button_label="Atualizar correlação CVM → Ticker",
        info_text=(
            "Executa **pickup/cvm_to_ticker_sync.py** e atualiza a tabela **public.cvm_to_ticker**.\n\n"
            "Requisitos: **SUPABASE_DB_URL** e **B3_INSTRUMENTOS_URL** definidos."
        ),
        status_label="Atualizando correlação CVM → Ticker (B3)...",
        module_import_path="pickup.cvm_to_ticker_sync",
        module_attr_name="cvm_to_ticker_sync",
        pipeline_name="cvm_to_ticker",
    )

    st.divider()

    st.markdown("## 1. Demonstrações completas (DFP/anual)")
    _run_job(
        job_key="job_dfp_running",
        button_label="Atualizar Demonstrações Completas (DFP)",
        info_text=(
            "Este botão executa o script **pickup/dados_cvm_dfp.py** para baixar os DFP da CVM, "
            "consolidar e gravar em **public.Demonstracoes_Financeiras** no Supabase.\n\n"
            "Requisitos: configurar **SUPABASE_DB_URL** em Secrets/Env Vars."
        ),
        status_label="Executando carga DFP (pode demorar alguns minutos)...",
        module_import_path="pickup.dados_cvm_dfp",
        module_attr_name="dados_cvm_dfp",
        pipeline_name="dfp",
    )

    st.divider()

    st.markdown("## 2. Demonstrações trimestrais (ITR/TRI)")
    _run_job(
        job_key="job_tri_running",
        button_label="Atualizar Demonstrações Trimestrais (TRI/ITR)",
        info_text=(
            "Este botão executa o script **pickup/dados_cvm_itr.py** para baixar os ITR consolidados da CVM, "
            "consolidar e gravar em **public.Demonstracoes_Financeiras_TRI** no Supabase.\n\n"
            "Requisitos: configurar **SUPABASE_DB_URL** em Secrets/Env Vars e garantir unique key em (Ticker, Data)."
        ),
        status_label="Executando carga ITR (pode demorar alguns minutos)...",
        module_import_path="pickup.dados_cvm_itr",
        module_attr_name="dados_cvm_itr",
        pipeline_name="itr",
    )

    st.divider()

    st.markdown("## 3. Setores / Subsetores / Segmentos (B3)")
    with st.expander("Detalhes / Variáveis de ambiente (setores)", expanded=False):
        st.write("SQLITE_METADADOS_PATH:", os.getenv("SQLITE_METADADOS_PATH", "data/metadados.db"))
        st.caption(
            "Observação: nesta etapa a fonte é o SQLite local (data/metadados.db, tabela setores) "
            "para migrar e manter o padrão legado do Algoritmo_2."
        )

    _run_job(
        job_key="job_setores_running",
        button_label="Atualizar Setores (SQLite → Supabase)",
        info_text=(
            "Executa **pickup/dados_setores_b3.py** para ler a tabela **setores** do SQLite local "
            "(**data/metadados.db**) e gravar via **UPSERT** em **public.setores** no Supabase.\n\n"
            "Requisitos: `SUPABASE_DB_URL` definida. Opcional: `SQLITE_METADADOS_PATH`."
        ),
        status_label="Executando carga de Setores (SQLite → Supabase)...",
        module_import_path="pickup.dados_setores_b3",
        module_attr_name="dados_setores_b3",
        pipeline_name="setores",
    )

    st.markdown("## 4. Informações Econômicas (Macro Brasil)")
    with st.expander("Detalhes / Variáveis de ambiente (macro)", expanded=False):
        st.write("ICC_MODE (final|mean):", os.getenv("ICC_MODE", "final"))
        st.write("MACRO_START_DATE (YYYY-MM-DD):", os.getenv("MACRO_START_DATE", "2010-01-01"))
        st.write("MACRO_MAX_YEARS_CHUNK:", os.getenv("MACRO_MAX_YEARS_CHUNK", "10"))
        st.write("MACRO_WRITE_MONTHLY (1 para gravar mensal):", os.getenv("MACRO_WRITE_MONTHLY", "0"))
        st.caption(
            "Observação: anual grava em public.info_economica. "
            "Se MACRO_WRITE_MONTHLY=1, tenta gravar também em public.info_economica_mensal."
        )

    _run_job(
        job_key="job_macro_running",
        button_label="Atualizar Informações Econômicas (BCB/SGS)",
        info_text=(
            "Executa **pickup/dados_macro_brasil.py** para coletar séries do BCB/SGS, gerar base **anual** "
            "para contexto/regime (tabela **public.info_economica**) e, opcionalmente, base **mensal** "
            "(tabela **public.info_economica_mensal**) quando `MACRO_WRITE_MONTHLY=1`.\n\n"
            "Requisitos: `SUPABASE_DB_URL` e dependência `python-bcb` no requirements."
        ),
        status_label="Executando carga Macro Brasil (BCB/SGS)...",
        module_import_path="pickup.dados_macro_brasil",
        module_attr_name="dados_macro_brasil",
        pipeline_name="macro",
    )

    st.divider()

    st.markdown("## 5. Múltiplos Fundamentalistas (DFP → yfinance → Supabase)")
    with st.expander("Detalhes / Variáveis de ambiente (múltiplos)", expanded=False):
        st.write("YF_START:", os.getenv("YF_START", "2010-01-01"))
        st.write("YF_END:", os.getenv("YF_END", "2023-12-31"))
        st.write("YF_BATCH_SIZE:", os.getenv("YF_BATCH_SIZE", "50"))
        st.caption(
            "Observação: esta rotina lê Demonstracoes_Financeiras no Supabase, "
            "baixa preços médios anuais via yfinance e grava em public.multiplos via UPSERT."
        )

    _run_job(
        job_key="job_multiplos_running",
        button_label="Atualizar Múltiplos (DFP)",
        info_text=(
            "Executa **pickup/dados_multiplos_dfp.py** para calcular múltiplos fundamentalistas a partir de "
            "**public.Demonstracoes_Financeiras**, integrar preço médio anual (yfinance) e gravar via **UPSERT** "
            "em **public.multiplos**.\n\n"
            "Requisitos: `SUPABASE_DB_URL` definida. Recomendado: índice unique em (Ticker, Data)."
        ),
        status_label="Executando cálculo e carga de Múltiplos (pode demorar)...",
        module_import_path="pickup.dados_multiplos_dfp",
        module_attr_name="dados_multiplos_dfp",
        pipeline_name="multiplos_dfp",
    )

    st.divider()

    st.markdown("## 6. Múltiplos Fundamentalistas Trimestrais (TRI → yfinance → Supabase)")
    _run_job(
        job_key="job_multiplos_tri_running",
        button_label="Atualizar Múltiplos Trimestrais (TRI)",
        info_text=(
            "Executa **pickup/dados_multiplos_itr.py** para calcular múltiplos trimestrais "
            "usando TTM (4 trimestres) para fluxos e último trimestre para estoques, "
            "integrando preço médio trimestral via yfinance e gravando via **UPSERT** "
            "em **public.multiplos_TRI**."
        ),
        status_label="Executando cálculo de Múltiplos Trimestrais (TRI)...",
        module_import_path="pickup.dados_multiplos_itr",
        module_attr_name="dados_multiplos_itr",
        pipeline_name="multiplos_itr",
    )


def configuracoes() -> None:
    render()
