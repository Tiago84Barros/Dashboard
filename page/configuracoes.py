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
V2_STALE_MINUTES = int(os.getenv("V2_STALE_MINUTES", "5"))


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


def _load_latest_v2_extract_summary(doc_type: str) -> Optional[dict[str, Any]]:
    if not doc_type or get_engine is None or text is None:
        return None
    try:
        engine = get_engine()
        query = text(
            """
            SELECT
                run_id,
                source_doc,
                status,
                started_at,
                finished_at,
                updated_at,
                metrics,
                errors,
                ano_inicial,
                ano_final,
                ultimo_ano_disponivel
            FROM public.cvm_ingestion_runs
            WHERE source_doc = :doc
              AND status IN ('success', 'partial_success')
            ORDER BY finished_at DESC NULLS LAST, started_at DESC
            LIMIT 1
            """
        )
        with engine.connect() as conn:
            row = conn.execute(query, {"doc": doc_type}).mappings().first()

        if not row:
            return None

        metrics = row.get("metrics") or {}
        errors = row.get("errors") or {}
        errors_list = errors.get("errors", []) if isinstance(errors, dict) else []
        return {
            "kind": "tickers",
            "pipeline": f"v2_extract_{str(doc_type).lower()}",
            "finished_at": row.get("finished_at") or row.get("updated_at"),
            "rows_inserted": _safe_int(metrics.get("inserted_rows_accum")),
            "tickers": _safe_int(metrics.get("tickers", 0)),
            "year_start": _normalize_year(row.get("ano_inicial")),
            "year_end": _normalize_year(row.get("ano_final") or row.get("ultimo_ano_disponivel")),
            "errors_count": len(errors_list),
            "warnings_count": 0,
        }
    except Exception:
        return None


def _load_latest_v2_stage_summary(source_doc: str) -> Optional[dict[str, Any]]:
    if not source_doc or get_engine is None or text is None:
        return None
    try:
        engine = get_engine()
        query = text(
            """
            SELECT
                run_id,
                source_doc,
                status,
                started_at,
                finished_at,
                updated_at,
                metrics,
                errors
            FROM public.cvm_ingestion_runs
            WHERE source_doc = :doc
              AND status IN ('success', 'partial_success')
            ORDER BY finished_at DESC NULLS LAST, started_at DESC
            LIMIT 1
            """
        )
        with engine.connect() as conn:
            row = conn.execute(query, {"doc": source_doc}).mappings().first()

        if not row:
            return None

        metrics = row.get("metrics") or {}
        errors = row.get("errors") or {}
        errors_list = errors.get("errors", []) if isinstance(errors, dict) else []

        rows_inserted = _safe_int(
            metrics.get("total_inserted", metrics.get("rows", metrics.get("inserted_rows_accum", 0)))
        )
        tickers = _safe_int(metrics.get("tickers", 0))

        return {
            "kind": "tickers" if tickers > 0 else "default",
            "pipeline": str(source_doc).lower(),
            "finished_at": row.get("finished_at") or row.get("updated_at"),
            "rows_inserted": rows_inserted,
            "tickers": tickers,
            "year_start": _normalize_year(metrics.get("year_start")),
            "year_end": _normalize_year(metrics.get("year_end")),
            "errors_count": len(errors_list),
            "warnings_count": 0,
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


def _parse_jsonish(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _coerce_dt(value: Any) -> Optional[pd.Timestamp]:
    try:
        dt = pd.to_datetime(value, utc=True)
        if pd.isna(dt):
            return None
        return dt
    except Exception:
        return None


def _utc_now_ts() -> pd.Timestamp:
    now = pd.Timestamp.utcnow()
    if getattr(now, "tzinfo", None) is None:
        return now.tz_localize("UTC")
    return now.tz_convert("UTC")


def _is_v2_run_stale(run: Optional[dict], stale_minutes: int = V2_STALE_MINUTES) -> bool:
    if not run or run.get("status") != "running":
        return False
    updated = _coerce_dt(run.get("updated_at"))
    if updated is None:
        return False
    age = _utc_now_ts() - updated
    return age.total_seconds() >= stale_minutes * 60


def _stale_age_minutes(run: Optional[dict]) -> Optional[int]:
    if not run:
        return None
    updated = _coerce_dt(run.get("updated_at"))
    if updated is None:
        return None
    age = _utc_now_ts() - updated
    return max(int(age.total_seconds() // 60), 0)


def _mark_run_failed(run_id: str, note: str) -> bool:
    if get_engine is None:
        return False
    try:
        engine = get_engine()
        raw_conn = engine.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE public.cvm_ingestion_runs
                    SET status = 'failed',
                        finished_at = NOW(),
                        updated_at = NOW(),
                        errors = COALESCE(errors, '{}'::jsonb) || jsonb_build_object('manual_note', %s)
                    WHERE run_id = %s
                    """,
                    (note, run_id),
                )
            raw_conn.commit()
        except Exception:
            raw_conn.rollback()
            raise
        finally:
            raw_conn.close()
        return True
    except Exception:
        return False


class _StatusTee:
    _MIN_INTERVAL_S: float = 1.0

    def __init__(self, buf: "io.StringIO", placeholder: Any) -> None:
        self._buf = buf
        self._placeholder = placeholder
        self._pending = ""
        self._last_ts: float = 0.0

    def _is_json(self, line: str) -> bool:
        s = line.strip()
        return s.startswith("{") and s.endswith("}")

    def _maybe_display(self, line: str) -> None:
        if self._is_json(line):
            return
        import time as _time
        now = _time.monotonic()
        if now - self._last_ts >= self._MIN_INTERVAL_S:
            try:
                self._placeholder.text(line[:200])
            except Exception:
                pass
            self._last_ts = now

    def write(self, text: str) -> int:
        self._buf.write(text)
        self._pending += text
        while "\n" in self._pending:
            line, self._pending = self._pending.split("\n", 1)
            stripped = line.strip()
            if stripped:
                self._maybe_display(stripped)
        return len(text)

    def flush(self) -> None:
        self._buf.flush()
        if self._pending.strip():
            self._maybe_display(self._pending.strip())
            self._pending = ""

    def isatty(self) -> bool:
        return False


@st.cache_data(ttl=10)
def _load_latest_v2_run(doc_type: str) -> Optional[dict]:
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
        run = dict(zip(keys, row))
        run["metrics"] = _parse_jsonish(run.get("metrics"))
        run["errors"] = _parse_jsonish(run.get("errors"))
        run["is_stale"] = _is_v2_run_stale(run)
        run["stale_age_minutes"] = _stale_age_minutes(run)
        return run
    except Exception as exc:
        return {"error": str(exc)}


@st.cache_data(ttl=10)
def _load_v2_population_diagnostics(source_doc: str) -> dict:
    base = {
        "source_doc": source_doc,
        "raw_count": 0,
        "normalized_count": 0,
        "final_count": 0,
        "best_source_count": 0,
        "active_mappings": 0,
        "latest_run": None,
        "error": None,
    }
    if get_engine is None or text is None:
        base["error"] = "Conexão com banco indisponível nesta página."
        return base
    try:
        engine = get_engine()
        with engine.connect() as conn:
            if source_doc == "MAP_V2":
                base["raw_count"] = _safe_int(conn.execute(text("SELECT COUNT(*) FROM public.cvm_financial_raw")).scalar())
                base["normalized_count"] = _safe_int(conn.execute(text("SELECT COUNT(*) FROM public.cvm_financial_normalized")).scalar())
                base["active_mappings"] = _safe_int(conn.execute(text("SELECT COUNT(*) FROM public.cvm_account_map WHERE ativo = TRUE")).scalar())
            elif source_doc == "PUBLISH_V2":
                base["normalized_count"] = _safe_int(conn.execute(text("SELECT COUNT(*) FROM public.cvm_financial_normalized")).scalar())
                base["best_source_count"] = _safe_int(conn.execute(text("SELECT COUNT(*) FROM public.vw_cvm_normalized_best_source")).scalar())
                base["final_count"] = _safe_int(conn.execute(text("SELECT COUNT(*) FROM public.demonstracoes_financeiras_v2")).scalar())
        base["latest_run"] = _load_latest_v2_run(source_doc)
        return base
    except Exception as exc:
        base["error"] = str(exc)
        return base


@st.cache_data(ttl=10)
def _load_top_unmapped_accounts(limit: int = 10) -> pd.DataFrame:
    if get_engine is None or text is None:
        return pd.DataFrame()
    try:
        engine = get_engine()
        query = text(
            """
            SELECT
                r.cd_conta,
                MIN(r.ds_conta) AS ds_conta,
                MIN(r.source_doc) AS source_doc_exemplo,
                COUNT(*) AS ocorrencias
            FROM public.cvm_financial_raw r
            LEFT JOIN public.cvm_account_map m
              ON m.ativo = TRUE
             AND m.cd_conta IS NOT NULL
             AND TRIM(m.cd_conta) = TRIM(r.cd_conta)
            WHERE m.cd_conta IS NULL
              AND r.cd_conta IS NOT NULL
            GROUP BY r.cd_conta
            ORDER BY COUNT(*) DESC, r.cd_conta
            LIMIT :limit
            """
        )
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"limit": int(limit)})
        return df
    except Exception:
        return pd.DataFrame()


def _render_v2_population_diagnostics(source_doc: Optional[str]) -> None:
    if source_doc not in {"MAP_V2", "PUBLISH_V2"}:
        return
    diag = _load_v2_population_diagnostics(source_doc)
    st.markdown("### Diagnóstico do povoamento")
    if diag.get("error"):
        st.warning(f"Não foi possível montar o diagnóstico agora: {diag['error']}")
        return

    latest_run = diag.get("latest_run") or {}
    metrics = latest_run.get("metrics") or {}
    message = metrics.get("message") or ""
    status = latest_run.get("status") or "sem run"

    if source_doc == "MAP_V2":
        cols = st.columns(3)
        cols[0].metric("Linhas brutas", f"{diag['raw_count']:,}")
        cols[1].metric("Regras ativas", f"{diag['active_mappings']:,}")
        cols[2].metric("Linhas normalizadas", f"{diag['normalized_count']:,}")

        if diag["raw_count"] == 0:
            st.error("O banco ainda não tem material bruto para traduzir. Execute primeiro o Extract Raw DFP e/ou ITR.")
        elif diag["active_mappings"] == 0:
            st.error("A tabela de regras está vazia. O sistema até lê os dados brutos, mas não sabe como interpretar as contas.")
        elif status == "failed":
            st.warning("O Map terminou como falha. Isso normalmente significa que os dados brutos existem, mas as regras não conseguiram reconhecer as contas necessárias.")
        elif diag["normalized_count"] == 0:
            st.warning("Há dados brutos e regras, mas nenhuma linha organizada foi gerada. O problema mais provável é desencontro entre os códigos ou nomes das contas e as regras de mapeamento.")
        else:
            st.success("A etapa de tradução já gerou linhas organizadas. Se a base final ainda estiver vazia, o gargalo provavelmente está na publicação.")

        unmapped = _load_top_unmapped_accounts(limit=10)
        if not unmapped.empty:
            st.markdown("#### Contas brutas mais frequentes ainda sem regra")
            st.caption("Lista enxuta para celular. Priorize primeiro as contas com mais ocorrências.")
            view = unmapped.rename(
                columns={
                    "cd_conta": "Código",
                    "ds_conta": "Descrição",
                    "source_doc_exemplo": "Exemplo",
                    "ocorrencias": "Ocorrências",
                }
            ).copy()
            view["Descrição"] = view["Descrição"].astype(str).str.slice(0, 70)
            view["Ocorrências"] = view["Ocorrências"].map(lambda x: f"{int(x):,}")
            st.table(view[["Código", "Ocorrências", "Exemplo", "Descrição"]])

    if source_doc == "PUBLISH_V2":
        cols = st.columns(3)
        cols[0].metric("Linhas normalizadas", f"{diag['normalized_count']:,}")
        cols[1].metric("Linhas prontas para publicar", f"{diag['best_source_count']:,}")
        cols[2].metric("Linhas na base final", f"{diag['final_count']:,}")

        if diag["normalized_count"] == 0:
            st.error("Ainda não há material traduzido para montar a base final. O Publish depende do sucesso do Map Normalized.")
        elif diag["best_source_count"] == 0:
            st.error("Existem linhas traduzidas, mas nada ficou pronto para publicação. Isso indica que a visão intermediária não conseguiu selecionar uma base válida por empresa e data.")
        elif status == "failed" and diag["final_count"] == 0:
            st.warning("O Publish terminou como falha. O sistema encontrou algum material antes, mas não conseguiu montar ou gravar a estrutura final.")
        elif diag["final_count"] == 0:
            st.warning("Há material para publicar, mas a base final continua vazia. Isso sugere problema na etapa de gravação final ou na montagem da estrutura final.")
        else:
            st.success("A publicação final já gerou registros. Se algum número estiver faltando, o problema passa a ser de cobertura e qualidade, não de povoamento zero.")

    if message:
        st.caption(f"Última mensagem registrada: {message}")


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
    active_run_check_doc_type: Optional[str] = None,
    latest_summary_doc_type: Optional[str] = None,
    latest_summary_source_doc: Optional[str] = None,
    diagnostic_source_doc: Optional[str] = None,
) -> None:
    if job_key not in st.session_state:
        st.session_state[job_key] = False

    latest_v2_run = _load_latest_v2_run(active_run_check_doc_type) if active_run_check_doc_type else None
    run_is_stale = bool(latest_v2_run and latest_v2_run.get("is_stale"))
    run_is_active = bool(latest_v2_run and latest_v2_run.get("status") == "running" and not run_is_stale)

    col1, col2 = st.columns([1, 2], gap="large")

    with col1:
        run = st.button(
            button_label,
            use_container_width=True,
            disabled=st.session_state[job_key] or run_is_active,
        )

    with col2:
        if run_is_active:
            st.warning(
                f"Existe um run `{active_run_check_doc_type}` em andamento. "
                f"Run ID: `{latest_v2_run.get('run_id')}` | Ano atual: `{(latest_v2_run.get('metrics') or {}).get('current_year', '—')}` | "
                f"Última atualização: {_format_dt(latest_v2_run.get('updated_at'))}"
            )
        elif run_is_stale:
            age = latest_v2_run.get("stale_age_minutes")
            st.warning(
                f"O último run `{active_run_check_doc_type}` parece travado (stale). "
                f"Run ID: `{latest_v2_run.get('run_id')}` | Sem atualização há ~{age} min. "
                f"Use as ações de manutenção para marcá-lo como failed e liberar nova execução."
            )
        else:
            st.info(info_text)

    with st.expander("Ações de manutenção", expanded=False):
        if st.button(f"Resetar trava do botão ({button_label})"):
            st.session_state[job_key] = False
            st.success("Trava resetada.")
            st.rerun()

        if active_run_check_doc_type and latest_v2_run and latest_v2_run.get("status") == "running":
            if latest_v2_run.get("is_stale"):
                if st.button(f"Marcar último run {active_run_check_doc_type} como failed (stale)"):
                    note = (
                        f"Run marcado como failed manualmente pela UI após ficar stale por ~"
                        f"{latest_v2_run.get('stale_age_minutes')} min sem atualizar progresso."
                    )
                    ok = _mark_run_failed(latest_v2_run.get("run_id"), note)
                    if ok:
                        st.session_state[job_key] = False
                        st.cache_data.clear()
                        st.success("Run marcado como failed e botão liberado.")
                        st.rerun()
                    else:
                        st.error("Não foi possível marcar o run como failed.")
            else:
                st.caption("A nova execução será liberada automaticamente quando o run ativo terminar ou falhar.")

    summary_placeholder = st.empty()
    diag_placeholder = st.empty()
    if latest_summary_doc_type:
        latest_summary = _load_latest_v2_extract_summary(latest_summary_doc_type)
    elif latest_summary_source_doc:
        latest_summary = _load_latest_v2_stage_summary(latest_summary_source_doc)
    else:
        latest_summary = _load_latest_success_summary(pipeline_name) if pipeline_name else None
    with summary_placeholder.container():
        _render_summary(latest_summary)
    if diagnostic_source_doc:
        with diag_placeholder.container():
            _render_v2_population_diagnostics(diagnostic_source_doc)

    if run:
        st.session_state[job_key] = True

        if not os.getenv("SUPABASE_DB_URL"):
            st.error("SUPABASE_DB_URL não está definida. Configure em Secrets/Env Vars e tente novamente.")
            st.session_state[job_key] = False
            st.stop()

        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()
        _saved_env: Dict[str, Optional[str]] = {}
        if env_overrides:
            for k, v in env_overrides.items():
                _saved_env[k] = os.environ.get(k)
                os.environ[k] = v
            sys.modules.pop(module_import_path, None)

        try:
            with st.status(status_label, expanded=True) as status:
                if env_overrides:
                    status.write(
                        "Variáveis de ambiente aplicadas: "
                        + ", ".join(f"`{k}={v}`" for k, v in env_overrides.items())
                    )
                status.write(f"Importando módulo `{module_import_path}` …")

                log_placeholder = st.empty()
                stdout_tee = _StatusTee(stdout_buf, log_placeholder)
                with redirect_stdout(stdout_tee), redirect_stderr(stderr_buf):
                    mod = __import__(module_import_path, fromlist=[module_attr_name])
                    status.write(f"Executando `{module_attr_name}.{main_func_name}()` …")
                    getattr(mod, main_func_name)()
                log_placeholder.empty()

                status.update(label="Execução finalizada.", state="complete")

            out = stdout_buf.getvalue().strip()
            if latest_summary_doc_type:
                summary = _load_latest_v2_extract_summary(latest_summary_doc_type)
            elif latest_summary_source_doc:
                summary = _load_latest_v2_stage_summary(latest_summary_source_doc)
            else:
                summary = (_extract_summary_from_stdout(out, pipeline_name=pipeline_name) if out else None) or (
                    _load_latest_success_summary(pipeline_name) if pipeline_name else None
                )

            with summary_placeholder.container():
                _render_summary(summary)
            if diagnostic_source_doc:
                with diag_placeholder.container():
                    _render_v2_population_diagnostics(diagnostic_source_doc)

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
            if env_overrides:
                for k, old_v in _saved_env.items():
                    if old_v is None:
                        os.environ.pop(k, None)
                    else:
                        os.environ[k] = old_v
                sys.modules.pop(module_import_path, None)

            st.session_state[job_key] = False


@st.cache_data(ttl=15)
def _count_years_in_db(doc_type: str) -> list:
    if get_engine is None or text is None:
        return []
    try:
        engine = get_engine()
        sql = text(
            """
            SELECT DISTINCT EXTRACT(YEAR FROM dt_refer::date)::int AS ano
            FROM public.cvm_financial_raw
            WHERE source_doc = :doc
            ORDER BY ano
            """
        )
        with engine.connect() as conn:
            rows = conn.execute(sql, {"doc": doc_type}).fetchall()
        return sorted(int(r[0]) for r in rows if r[0] is not None)
    except Exception:
        return []


def _render_single_run_card(doc_type: str) -> None:
    run = _load_latest_v2_run(doc_type)

    st.markdown(f"**{doc_type}**")

    if run is None:
        st.info("Nenhum run registrado ainda para este tipo de documento.")
        return

    if run.get("error"):
        st.warning(f"Erro ao consultar run: {run['error']}")
        return

    status = run.get("status", "?")
    display_status = status
    if run.get("is_stale"):
        display_status = "stale"

    STATUS_ICON = {
        "running": "🔄",
        "success": "✅",
        "partial_success": "⚠️",
        "failed": "❌",
        "stale": "⏸️",
    }
    icon = STATUS_ICON.get(display_status, "❓")

    metrics: dict = run.get("metrics") or {}
    errors_payload: dict = run.get("errors") or {}

    started = _format_dt(run.get("started_at"))
    finished = _format_dt(run.get("finished_at"))
    updated = _format_dt(run.get("updated_at"))

    st.markdown(
        f"{icon} **Status:** `{display_status}`  \n"
        f"**Run ID:** `{run.get('run_id', '?')}`  \n"
        f"**Início:** {started}  \n"
        f"**Fim:** {finished}  \n"
        f"**Última atualização:** {updated}"
    )

    if run.get("is_stale"):
        st.warning(
            f"Run sem atualização há ~{run.get('stale_age_minutes')} min. "
            "Provavelmente travado. Pode ser marcado como failed nas ações de manutenção do botão."
        )

    years_total = _safe_int(metrics.get("years_total"), 0)
    years_done = _safe_int(metrics.get("years_done"), 0)
    years_failed = _safe_int(metrics.get("failed_years_count", metrics.get("years_failed")), 0)
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
        skipped_files = _safe_int(metrics.get("skipped_files_accum", 0))
        skipped_rows = _safe_int(metrics.get("skipped_rows_accum", 0))
        st.caption(
            f"Úteis: **{raw_rows:,}** linhas | Inseridas: **{inserted:,}** | "
            f"Filtradas: **{skipped_rows:,}** linhas + **{skipped_files}** arquivos ignorados"
        )

    if message:
        st.caption(f"Mensagem: {message}")

    year_details = metrics.get("year_details") or {}
    if isinstance(year_details, dict) and year_details:
        with st.expander("Detalhes por ano", expanded=display_status in ("failed", "partial_success", "stale")):
            for year_key in sorted(year_details.keys()):
                info = year_details.get(year_key) or {}
                info_metrics = info.get("metrics") or {}
                rows_prepared = _safe_int(info.get("rows_prepared", info_metrics.get("rows_prepared")), 0)
                rows_inserted = _safe_int(info.get("rows_inserted"), 0)
                rows_with_ticker = _safe_int(info_metrics.get("rows_with_ticker"), 0)
                rows_without_ticker = _safe_int(info_metrics.get("rows_without_ticker"), 0)
                st.markdown(
                    f"**{year_key}** — status: `{info.get('status', '?')}`  \n"
                    f"Preparadas: **{rows_prepared:,}** | Inseridas: **{rows_inserted:,}** | "
                    f"Com ticker: **{rows_with_ticker:,}** | Sem ticker: **{rows_without_ticker:,}**"
                )
                if info.get("errors"):
                    st.code("\n\n".join(info.get("errors", [])[-3:]), language="text")

    errors_list = errors_payload.get("errors", [])
    if errors_list:
        auto_expand = display_status in ("failed", "partial_success", "stale")
        with st.expander(f"⚠️ {len(errors_list)} erro(s) registrado(s)", expanded=auto_expand):
            for e in errors_list[-20:]:
                st.code(e, language="text")


def _render_v2_run_monitor() -> None:
    with st.expander("📊 Monitor de Runs V2", expanded=True):
        if st.button("🔄 Atualizar status dos runs", key="btn_v2_refresh_runs"):
            st.cache_data.clear()
            st.rerun()

        dfp_years = _count_years_in_db("DFP")
        itr_years = _count_years_in_db("ITR")
        ano_ref = 2010
        ano_atual = datetime.now().year
        total_esperado = ano_atual - ano_ref + 1

        st.markdown("##### Progresso acumulado no banco (todos os runs)")
        col_prog_dfp, col_prog_itr = st.columns(2)
        with col_prog_dfp:
            pct_dfp = len(dfp_years) / total_esperado if total_esperado else 0
            st.progress(min(pct_dfp, 1.0), text=f"DFP: {len(dfp_years)}/{total_esperado} anos salvos")
            if dfp_years:
                st.caption(f"De {dfp_years[0]} a {dfp_years[-1]}")
        with col_prog_itr:
            pct_itr = len(itr_years) / total_esperado if total_esperado else 0
            st.progress(min(pct_itr, 1.0), text=f"ITR: {len(itr_years)}/{total_esperado} anos salvos")
            if itr_years:
                st.caption(f"De {itr_years[0]} a {itr_years[-1]}")

        st.divider()
        st.markdown("##### Último run por tipo")
        col_dfp, col_itr = st.columns(2)
        with col_dfp:
            _render_single_run_card("DFP")
        with col_itr:
            _render_single_run_card("ITR")


def _render_v2_section() -> None:
    st.divider()
    st.markdown("## CVM V2 — Pipeline Institucional Raw")
    st.caption(
        "Pipeline normalizado de ingestão CVM com rastreabilidade, deduplicação e mapeamento de contas. "
        "Mantém os dados legados intactos — opera em tabelas V2 separadas."
    )

    st.markdown(
        "**Ordem recomendada:**\n"
        "1) Extract Raw (DFP) — extrai demonstrações anuais brutas *(execute várias vezes até completar todos os anos)*\n"
        "2) Extract Raw (ITR) — extrai demonstrações trimestrais brutas *(idem)*\n"
        "3) Map Normalized — aplica mapeamento de contas e normaliza\n"
        "4) Publish Financials — publica em demonstracoes_financeiras_v2\n"
    )

    st.divider()
    st.markdown("#### ⚙️ Configuração do lote de extração")
    st.caption(
        "O Streamlit Cloud tem limite de memória e tempo. "
        "Processe poucos anos por execução e repita até completar. "
        "Anos já salvos no banco são pulados automaticamente."
    )
    max_anos = st.number_input(
        "Máximo de anos por execução (0 = todos)",
        min_value=0, max_value=50, value=0, step=1,
        key="v2_max_anos_por_run",
        help="0 = processa todos os anos pendentes de uma vez. Use 1–3 apenas se o servidor travar.",
    )
    ano_inicial = st.number_input(
        "Ano inicial da extração",
        min_value=2000, max_value=2030, value=2010, step=1,
        key="v2_ano_inicial",
        help="Padrão: 2010. O pipeline pula automaticamente anos já no banco.",
    )
    env_extract = {
        "MAX_ANOS_POR_RUN": str(int(max_anos)),
        "ANO_INICIAL": str(int(ano_inicial)),
    }

    st.divider()

    st.markdown("### CVM V2 — Extract Raw (DFP)")
    _run_job(
        job_key="job_cvm_v2_extract_dfp_running",
        button_label="CVM V2 — Extract Raw (DFP)",
        info_text=(
            f"Executa **pickup/cvm_extract_v2.py** com `CVM_DOC_TYPE=DFP`.\n\n"
            f"Processa {'**todos** os anos pendentes' if int(max_anos) == 0 else f'até **{int(max_anos)} ano(s)**'} "
            f"a partir de **{int(ano_inicial)}**. Anos já no banco são pulados automaticamente."
        ),
        status_label="Executando CVM V2 — Extract Raw (DFP)...",
        module_import_path="pickup.cvm_extract_v2",
        module_attr_name="cvm_extract_v2",
        env_overrides={"CVM_DOC_TYPE": "DFP", **env_extract},
        active_run_check_doc_type="DFP",
        latest_summary_doc_type="DFP",
    )

    st.divider()

    st.markdown("### CVM V2 — Extract Raw (ITR)")
    _run_job(
        job_key="job_cvm_v2_extract_itr_running",
        button_label="CVM V2 — Extract Raw (ITR)",
        info_text=(
            f"Executa **pickup/cvm_extract_v2.py** com `CVM_DOC_TYPE=ITR`.\n\n"
            f"Processa {'**todos** os anos pendentes' if int(max_anos) == 0 else f'até **{int(max_anos)} ano(s)**'} "
            f"a partir de **{int(ano_inicial)}**. Anos já no banco são pulados automaticamente."
        ),
        status_label="Executando CVM V2 — Extract Raw (ITR)...",
        module_import_path="pickup.cvm_extract_v2",
        module_attr_name="cvm_extract_v2",
        env_overrides={"CVM_DOC_TYPE": "ITR", **env_extract},
        active_run_check_doc_type="ITR",
        latest_summary_doc_type="ITR",
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
        latest_summary_source_doc="MAP_V2",
        diagnostic_source_doc="MAP_V2",
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
        latest_summary_source_doc="PUBLISH_V2",
        diagnostic_source_doc="PUBLISH_V2",
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

    _render_v2_section()


def configuracoes() -> None:
    render()
