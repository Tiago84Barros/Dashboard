from __future__ import annotations

import io
import json
import os
import sys
import traceback
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from typing import Any, Dict, Optional

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


class _StatusTee:
    """Stream que escreve simultaneamente em um buffer de texto E no st.status().

    Isso faz com que cada linha de `log()` do pipeline apareça em tempo real
    no widget st.status() enquanto o job executa, sem perder o conteúdo para
    o buffer (usado para análise pós-execução e exibição de erros).
    """

    def __init__(self, buf: "io.StringIO", status_widget: Any) -> None:
        self._buf = buf
        self._status = status_widget
        self._pending = ""   # caracteres ainda não terminados em \n

    def write(self, text: str) -> int:
        self._buf.write(text)
        self._pending += text
        # Emite uma linha completa a cada \n
        while "\n" in self._pending:
            line, self._pending = self._pending.split("\n", 1)
            stripped = line.strip()
            if stripped:
                try:
                    self._status.write(stripped)
                except Exception:
                    pass   # st.status pode rejeitar writes fora de contexto — ignorar
        return len(text)

    def flush(self) -> None:
        self._buf.flush()
        # Emite qualquer resto pendente (sem \n) ao fazer flush
        if self._pending.strip():
            try:
                self._status.write(self._pending.strip())
            except Exception:
                pass
            self._pending = ""

    def isatty(self) -> bool:
        return False


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
    env_overrides: Optional[Dict[str, str]] = None,
) -> None:
    """Renderiza um botão de job e executa o pipeline quando acionado.

    Args:
        env_overrides: Variáveis de ambiente a serem definidas temporariamente
            durante a execução. Útil para jobs que lêem configuração via env
            (ex: CVM_DOC_TYPE=DFP). O módulo é removido do cache de importação
            (sys.modules) para garantir que seja re-avaliado com os novos valores.
            As variáveis são restauradas ao estado original após a execução.
    """
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

        # ── Suporte a env_overrides: guarda env atual e força reload do módulo
        _saved_env: Dict[str, Optional[str]] = {}
        if env_overrides:
            for k, v in env_overrides.items():
                _saved_env[k] = os.environ.get(k)
                os.environ[k] = v
            # Remove módulo do cache para garantir re-importação com novas vars
            sys.modules.pop(module_import_path, None)

        try:
            with st.status(status_label, expanded=True) as status:
                if env_overrides:
                    status.write(
                        "Variáveis de ambiente aplicadas: "
                        + ", ".join(f"`{k}={v}`" for k, v in env_overrides.items())
                    )
                status.write(f"Importando módulo `{module_import_path}` …")

                # _StatusTee: cada log() do pipeline aparece no status em tempo real
                stdout_tee = _StatusTee(stdout_buf, status)
                with redirect_stdout(stdout_tee), redirect_stderr(stderr_buf):
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
            # Restaura variáveis de ambiente originais
            if env_overrides:
                for k, old_v in _saved_env.items():
                    if old_v is None:
                        os.environ.pop(k, None)
                    else:
                        os.environ[k] = old_v
                # Remove módulo do cache (próxima execução também será fresh)
                sys.modules.pop(module_import_path, None)

            st.session_state[job_key] = False


@st.cache_data(ttl=30)
def _check_v2_schema_cached() -> dict:
    """Verifica o schema V2 com cache de 30 s."""
    try:
        from core.cvm_v2_schema_check import check_v2_schema
        return check_v2_schema()
    except Exception as exc:
        return {"ready": False, "found": [], "missing": [], "error": str(exc)}


@st.cache_data(ttl=30)
def _get_v2_diagnostics_cached() -> dict:
    """Diagnóstico de conexão com cache de 30 s."""
    try:
        from core.cvm_v2_schema_check import get_connection_diagnostics
        return get_connection_diagnostics()
    except Exception as exc:
        return {"error": str(exc)}


def _render_v2_schema_status() -> bool:
    """Exibe card de status do schema CVM V2. Retorna True se schema estiver pronto."""
    with st.expander("Status do Schema CVM V2", expanded=True):
        if st.button("Reverificar schema", key="btn_v2_schema_check"):
            st.cache_data.clear()
            st.rerun()

        result = _check_v2_schema_cached()

        if result.get("error"):
            st.error(
                f"Falha ao verificar schema: {result['error']}\n\n"
                "Verifique a variável **SUPABASE_DB_URL** e a conexão com o banco."
            )
            return False

        found = result.get("found", [])
        missing = result.get("missing", [])

        if result.get("ready"):
            st.success(
                f"Schema CVM V2 completo — {len(found)} objeto(s) encontrado(s).\n\n"
                + "\n".join(f"  ✅ {n}" for n in found)
            )
            return True

        # Schema incompleto — mostrar diagnóstico de conexão para ajudar o usuário
        st.error(
            "Schema CVM V2 **incompleto**. Aplique o DDL institucional V2 antes de executar os jobs.\n\n"
            + ("\n".join(f"  ✅ {n}" for n in found) + "\n" if found else "")
            + "\n".join(f"  ❌ {n}" for n in missing)
        )

        # Diagnóstico de conexão — mostra banco conectado e tabelas existentes
        diag = _get_v2_diagnostics_cached()
        if diag.get("error"):
            st.warning(f"Não foi possível obter diagnóstico de conexão: {diag['error']}")
        else:
            with st.expander("🔍 Diagnóstico de conexão (clique para expandir)", expanded=True):
                st.markdown(
                    f"**Banco conectado:** `{diag.get('current_database', '?')}`  \n"
                    f"**Schema padrão:** `{diag.get('current_schema', '?')}`  \n"
                    f"**Tabelas em public:** {diag.get('public_table_count', '?')}"
                )

                v2_found = diag.get("cvm_v2_tables_found", [])
                if v2_found:
                    st.success(f"Tabelas V2 encontradas neste banco: {', '.join(v2_found)}")
                else:
                    st.error(
                        "**Nenhuma tabela V2 encontrada neste banco.**\n\n"
                        "Isso confirma que o DDL ainda não foi aplicado — ou foi aplicado em outro projeto/banco do Supabase."
                    )

                sample = diag.get("public_tables_sample", [])
                if sample:
                    st.markdown("**Primeiras tabelas no schema public** (para confirmar que é o banco certo):")
                    st.code(", ".join(sample), language="text")
                else:
                    st.warning("Nenhuma tabela encontrada no schema public.")

            st.info(
                "**Como resolver:** abra o SQL Editor do Supabase no projeto correto e execute o DDL institucional V2.\n\n"
                "Se o banco acima não é o esperado, verifique a variável **SUPABASE_DB_URL** nas configurações do app."
            )

        return False


@st.cache_data(ttl=10)
def _load_latest_v2_run(doc_type: str) -> Optional[dict]:
    """Retorna o run mais recente de cvm_ingestion_runs para doc_type. TTL=10 s."""
    if get_engine is None or text is None:
        return None
    try:
        engine = get_engine()
        sql = text(
            """
            SELECT run_id, source_doc, status, ano_inicial, ano_final,
                   ultimo_ano_disponivel, metrics, errors,
                   started_at, finished_at, updated_at
            FROM public.cvm_ingestion_runs
            WHERE source_doc = :doc
            ORDER BY started_at DESC
            LIMIT 1
            """
        )
        with engine.connect() as conn:
            row = conn.execute(sql, {"doc": doc_type}).fetchone()
        if row is None:
            return None
        keys = [
            "run_id", "source_doc", "status", "ano_inicial", "ano_final",
            "ultimo_ano_disponivel", "metrics", "errors",
            "started_at", "finished_at", "updated_at",
        ]
        return dict(zip(keys, row))
    except Exception as exc:
        return {"error": str(exc)}


def _render_single_run_card(doc_type: str) -> None:
    """Renderiza um card de status/progresso para o run mais recente de doc_type."""
    run = _load_latest_v2_run(doc_type)

    st.markdown(f"**{doc_type}**")

    if run is None:
        st.info("Nenhum run registrado ainda para este tipo de documento.")
        return

    if run.get("error"):
        st.warning(f"Erro ao consultar run: {run['error']}")
        return

    status = run.get("status", "?")
    STATUS_ICON = {
        "running": "🔄",
        "success": "✅",
        "partial_success": "⚠️",
        "failed": "❌",
    }
    icon = STATUS_ICON.get(status, "❓")

    metrics: dict = run.get("metrics") or {}
    if isinstance(metrics, str):
        try:
            metrics = json.loads(metrics)
        except Exception:
            metrics = {}

    errors_payload: dict = run.get("errors") or {}
    if isinstance(errors_payload, str):
        try:
            errors_payload = json.loads(errors_payload)
        except Exception:
            errors_payload = {}

    started = _format_dt(run.get("started_at"))
    finished = _format_dt(run.get("finished_at"))
    updated = _format_dt(run.get("updated_at"))

    st.markdown(
        f"{icon} **Status:** `{status}`  \n"
        f"**Run ID:** `{run.get('run_id', '?')}`  \n"
        f"**Início:** {started}  \n"
        f"**Fim:** {finished}  \n"
        f"**Última atualização:** {updated}"
    )

    # Progresso incremental
    years_total = _safe_int(metrics.get("years_total"), 0)
    years_done = _safe_int(metrics.get("years_done"), 0)
    years_failed = _safe_int(metrics.get("years_failed"), 0)
    raw_rows = _safe_int(metrics.get("raw_rows_accum"), 0)
    inserted = _safe_int(metrics.get("inserted_rows_accum"), 0)
    current_year = metrics.get("current_year")
    message = metrics.get("message", "")

    if years_total > 0:
        years_processed = years_done + years_failed
        pct = years_processed / years_total
        st.progress(min(pct, 1.0), text=f"{years_processed}/{years_total} anos processados")
        cols_m = st.columns(3)
        cols_m[0].metric("Anos OK", years_done)
        cols_m[1].metric("Anos c/ erro", years_failed)
        cols_m[2].metric("Ano atual", current_year or "—")
        st.caption(
            f"Raw rows acumuladas: **{raw_rows:,}** | Inseridas: **{inserted:,}**"
        )

    if message:
        st.caption(f"Mensagem: {message}")

    errors_list = errors_payload.get("errors", [])
    if errors_list:
        with st.expander(f"⚠️ {len(errors_list)} erro(s) registrado(s)", expanded=False):
            for e in errors_list[-10:]:
                st.code(e, language="text")


def _render_v2_run_monitor() -> None:
    """Renderiza o painel de monitoramento de runs V2 (DFP e ITR lado a lado)."""
    with st.expander("📊 Monitor de Runs V2", expanded=True):
        if st.button("🔄 Atualizar status dos runs", key="btn_v2_refresh_runs"):
            st.cache_data.clear()
            st.rerun()

        col_dfp, col_itr = st.columns(2)
        with col_dfp:
            _render_single_run_card("DFP")
        with col_itr:
            _render_single_run_card("ITR")


def _render_v2_section() -> None:
    """Renderiza a seção CVM V2 abaixo dos jobs legados."""
    st.divider()
    st.markdown("## CVM V2 — Pipeline Institucional Raw")
    st.caption(
        "Pipeline normalizado de ingestão CVM com rastreabilidade, deduplicação e mapeamento de contas. "
        "Mantém os dados legados intactos — opera em tabelas V2 separadas."
    )

    _render_v2_schema_status()
    _render_v2_run_monitor()

    st.markdown(
        "**Ordem recomendada:**\n"
        "1) Extract Raw (DFP) — extrai demonstrações anuais brutas\n"
        "2) Extract Raw (ITR) — extrai demonstrações trimestrais brutas\n"
        "3) Map Normalized — aplica mapeamento de contas e normaliza\n"
        "4) Publish Financials — publica em demonstracoes_financeiras_v2\n"
    )

    st.divider()

    st.markdown("### CVM V2 — Extract Raw (DFP)")
    _run_job(
        job_key="job_cvm_v2_extract_dfp_running",
        button_label="CVM V2 — Extract Raw (DFP)",
        info_text=(
            "Executa **pickup/cvm_extract_v2.py** com `CVM_DOC_TYPE=DFP`.\n\n"
            "Baixa ZIPs de DFP da CVM, extrai contas brutas e grava via UPSERT "
            "em **public.cvm_financial_raw**. Registra execução em **public.cvm_ingestion_runs**."
        ),
        status_label="Executando CVM V2 — Extract Raw (DFP)...",
        module_import_path="pickup.cvm_extract_v2",
        module_attr_name="cvm_extract_v2",
        env_overrides={"CVM_DOC_TYPE": "DFP"},
    )

    st.divider()

    st.markdown("### CVM V2 — Extract Raw (ITR)")
    _run_job(
        job_key="job_cvm_v2_extract_itr_running",
        button_label="CVM V2 — Extract Raw (ITR)",
        info_text=(
            "Executa **pickup/cvm_extract_v2.py** com `CVM_DOC_TYPE=ITR`.\n\n"
            "Baixa ZIPs de ITR da CVM, extrai contas brutas e grava via UPSERT "
            "em **public.cvm_financial_raw**. Registra execução em **public.cvm_ingestion_runs**."
        ),
        status_label="Executando CVM V2 — Extract Raw (ITR)...",
        module_import_path="pickup.cvm_extract_v2",
        module_attr_name="cvm_extract_v2",
        env_overrides={"CVM_DOC_TYPE": "ITR"},
    )

    st.divider()

    st.markdown("### CVM V2 — Map Normalized")
    _run_job(
        job_key="job_cvm_v2_map_running",
        button_label="CVM V2 — Map Normalized",
        info_text=(
            "Executa **pickup/cvm_map_v2.py**.\n\n"
            "Lê **public.cvm_financial_raw**, aplica mapeamento de contas de "
            "**public.cvm_account_map** (ativo=TRUE, por prioridade) e grava "
            "em **public.cvm_financial_normalized**.\n\n"
            "Pré-requisito: Extract Raw DFP e/ou ITR já executados."
        ),
        status_label="Executando CVM V2 — Map Normalized...",
        module_import_path="pickup.cvm_map_v2",
        module_attr_name="cvm_map_v2",
    )

    st.divider()

    st.markdown("### CVM V2 — Publish Financials")
    _run_job(
        job_key="job_cvm_v2_publish_running",
        button_label="CVM V2 — Publish Financials",
        info_text=(
            "Executa **pickup/cvm_publish_financials_v2.py**.\n\n"
            "Lê **public.vw_cvm_normalized_best_source**, consolida em formato wide "
            "(pivot por canonical_key), deriva EBITDA/FCF/dívida e publica via UPSERT "
            "em **public.demonstracoes_financeiras_v2**.\n\n"
            "Pré-requisito: Map Normalized já executado."
        ),
        status_label="Executando CVM V2 — Publish Financials...",
        module_import_path="pickup.cvm_publish_financials_v2",
        module_attr_name="cvm_publish_financials_v2",
    )


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

    # ── Seção CVM V2 (nova, abaixo dos jobs legados) ─────────────────────
    _render_v2_section()


def configuracoes() -> None:
    render()
