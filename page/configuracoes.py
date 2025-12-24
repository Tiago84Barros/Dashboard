from __future__ import annotations

import datetime as dt
import importlib
import traceback
from typing import Any, Callable, Dict, Optional, Tuple

import streamlit as st
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ==========================
# Configuração do banco
# ==========================
SCHEMA = "cvm"
TARGET_TABLE = f"{SCHEMA}.demonstracoes_financeiras_dfp"
SYNC_LOG = f"{SCHEMA}.sync_log"


# ==========================
# Helpers
# ==========================
def _import_first(*module_paths: str):
    errors = []
    for p in module_paths:
        try:
            return importlib.import_module(p)
        except Exception as e:
            errors.append((p, e))
    msg = "Falha ao importar módulos. Tentativas:\n" + "\n".join([f"- {p}: {repr(e)}" for p, e in errors])
    raise ImportError(msg)


def _get_engine() -> Engine:
    mod = _import_first("core.db.engine")
    fn = getattr(mod, "get_engine", None)
    if not callable(fn):
        raise ImportError("get_engine() não encontrado em core.db.engine")
    return fn()


def _fmt_dt(x: Optional[dt.datetime]) -> str:
    if not x:
        return "—"
    try:
        # já vem timestamptz do PG
        return x.astimezone().strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(x)


def _synclog_insert(engine: Engine, status: str, last_year: Optional[int], remote_latest_year: Optional[int], message: str) -> None:
    q = text(
        f"""
        insert into {SYNC_LOG} (run_at, status, last_year, remote_latest_year, message)
        values (now(), :status, :last_year, :remote_latest_year, :message)
        """
    )
    with engine.begin() as conn:
        conn.execute(
            q,
            {
                "status": status,
                "last_year": last_year,
                "remote_latest_year": remote_latest_year,
                "message": (message or "")[:4000],
            },
        )


def _synclog_latest(engine: Engine) -> Optional[dict]:
    q = text(
        f"""
        select id, run_at, status, last_year, remote_latest_year, message
        from {SYNC_LOG}
        order by run_at desc
        limit 1
        """
    )
    with engine.begin() as conn:
        row = conn.execute(q).mappings().first()
        return dict(row) if row else None


def _get_last_year_in_db(engine: Engine) -> Optional[int]:
    """
    Tabela alvo possui coluna `data` (date). O último ano no banco = max(extract(year from data)).
    """
    q = text(f"select max(extract(year from data))::int as last_year from {TARGET_TABLE}")
    with engine.begin() as conn:
        return conn.execute(q).scalar()


def _guess_remote_latest_year() -> Optional[int]:
    """
    Se você tiver alguma função no cvm_dfp_ingest que detecte o último ano remoto,
    ela será usada automaticamente. Caso contrário, retorna None.
    """
    try:
        mod = _import_first("cvm.cvm_dfp_ingest", "cvm_dfp_ingest")
        fn = getattr(mod, "get_remote_latest_year", None) or getattr(mod, "get_latest_remote_year", None)
        if callable(fn):
            return int(fn())
    except Exception:
        pass
    return None


def get_sync_status(engine: Engine) -> Dict[str, Any]:
    """
    Status “fonte única”:
    - last_year: max year em cvm.demonstracoes_financeiras_dfp
    - last_run_at: último run_at do sync_log
    - remote_latest_year: (se detectável) via função do ingest
    - has_updates: remote_latest_year > last_year
    """
    last_year = _get_last_year_in_db(engine)
    latest = _synclog_latest(engine)
    last_run_at = latest["run_at"] if latest else None

    remote_latest_year = _guess_remote_latest_year()
    has_updates = None
    if last_year and remote_latest_year:
        has_updates = remote_latest_year > last_year

    notes = None
    if remote_latest_year is None:
        notes = "Não foi possível detectar automaticamente o último ano disponível no site da CVM (função não encontrada no ingest)."

    return {
        "last_year": last_year,
        "last_run_at": last_run_at,
        "remote_latest_year": remote_latest_year,
        "has_updates": has_updates,
        "notes": notes,
    }


# ==========================
# Pipeline de atualização
# ==========================
def _run_step(step_name: str, pct: int, progress_cb: Optional[Callable[[float, str], None]], fn: Callable[[], Any]) -> Any:
    if progress_cb:
        progress_cb(float(pct), step_name)
    return fn()


def apply_update(
    engine: Engine,
    start_year: int,
    end_year: int,
    progress_cb: Optional[Callable[[float, str], None]] = None,
) -> None:
    """
    Orquestra atualização completa:
      1) DFP (cvm_dfp_ingest)
      2) Setores (setores_ingest)
      3) Métricas (finance_metrics_builder)
      4) Score (fundamental_scoring)
    E registra tudo em cvm.sync_log.
    """

    # Importa seus módulos reais
    dfp_mod = _import_first("cvm.cvm_dfp_ingest", "cvm_dfp_ingest")
    setores_mod = _import_first("cvm.setores_ingest", "setores_ingest")
    metrics_mod = _import_first("cvm.finance_metrics_builder", "finance_metrics_builder")
    score_mod = _import_first("cvm.fundamental_scoring", "fundamental_scoring")

    dfp_run = getattr(dfp_mod, "run", None)
    setores_run = getattr(setores_mod, "run", None)
    metrics_run = getattr(metrics_mod, "run", None)
    score_run = getattr(score_mod, "run", None)

    if not callable(dfp_run):
        raise ImportError("run() não encontrado em cvm_dfp_ingest.py")
    if not callable(setores_run):
        raise ImportError("run() não encontrado em setores_ingest.py")
    if not callable(metrics_run):
        raise ImportError("run() não encontrado em finance_metrics_builder.py")
    if not callable(score_run):
        raise ImportError("run() não encontrado em fundamental_scoring.py")

    last_year_before = _get_last_year_in_db(engine)
    remote_latest_year = _guess_remote_latest_year()

    try:
        _synclog_insert(engine, "running", last_year_before, remote_latest_year, f"Iniciando update DFP {start_year}-{end_year}")

        # 1) DFP
        def _dfp():
            # tenta chamadas comuns. Se sua assinatura for diferente, ajuste aqui.
            try:
                return dfp_run(engine=engine, start_year=start_year, end_year=end_year, table=TARGET_TABLE)
            except TypeError:
                try:
                    return dfp_run(engine=engine, start_year=start_year, end_year=end_year)
                except TypeError:
                    return dfp_run(engine, start_year, end_year)

        _run_step("Atualizando DFP (CVM → Supabase)", 15, progress_cb, _dfp)

        # 2) Setores
        def _setores():
            try:
                return setores_run(engine=engine)
            except TypeError:
                return setores_run(engine)

        _run_step("Atualizando Setores", 45, progress_cb, _setores)

        # 3) Métricas
        def _metrics():
            try:
                return metrics_run(engine=engine)
            except TypeError:
                return metrics_run(engine)

        _run_step("Recalculando Métricas", 75, progress_cb, _metrics)

        # 4) Score
        def _score():
            try:
                return score_run(engine=engine)
            except TypeError:
                return score_run(engine)

        _run_step("Recalculando Score", 95, progress_cb, _score)

        last_year_after = _get_last_year_in_db(engine)
        if progress_cb:
            progress_cb(100.0, "Concluído.")

        _synclog_insert(engine, "success", last_year_after, remote_latest_year, "Pipeline concluído com sucesso")

    except Exception as e:
        tb = traceback.format_exc()
        _synclog_insert(engine, "error", last_year_before, remote_latest_year, f"{repr(e)}\n{tb}")
        raise


# ==========================
# UI
# ==========================
def render() -> None:
    engine = _get_engine()

    st.markdown(
        """
        <style>
          .cfg-title {font-size: 2rem; font-weight: 800; margin-bottom: .25rem;}
          .cfg-sub {color: #9ca3af; margin-bottom: 1.25rem;}
          .card {border: 1px solid rgba(255,255,255,.08); border-radius: 16px; padding: 16px; background: rgba(255,255,255,.03);}
          .pill {display:inline-block; padding: 4px 10px; border-radius: 999px; font-size:.85rem; border:1px solid rgba(255,255,255,.10);}
          .pill-ok {background: rgba(16,185,129,.12);}
          .pill-warn {background: rgba(245,158,11,.12);}
          .pill-bad {background: rgba(239,68,68,.12);}
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<div class='cfg-title'>Configurações</div>", unsafe_allow_html=True)
    st.markdown("<div class='cfg-sub'>Atualize a base no Supabase e acompanhe o status.</div>", unsafe_allow_html=True)

    status = get_sync_status(engine)

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
        st.metric("Último ano disponível (CVM)", value=str(remote_latest_year) if remote_latest_year else "—")
    with c4:
        if has_updates is True:
            st.markdown("<span class='pill pill-warn'>Há atualizações pendentes</span>", unsafe_allow_html=True)
        elif has_updates is False:
            st.markdown("<span class='pill pill-ok'>Base está atualizada</span>", unsafe_allow_html=True)
        else:
            st.markdown("<span class='pill pill-bad'>Status parcial</span>", unsafe_allow_html=True)

    st.markdown("")

    left, right = st.columns([1, 1], vertical_alignment="top")

    with left:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.subheader("Atualizar base CVM (DFP)")
        st.caption(f"Tabela alvo: `{TARGET_TABLE}`")

        now_year = dt.datetime.now().year
        default_start = int(last_year) if last_year else max(2010, now_year - 2)
        default_end = now_year

        colA, colB = st.columns(2)
        with colA:
            start_year = st.number_input("Ano inicial", min_value=2000, max_value=now_year, value=default_start)
        with colB:
            end_year = st.number_input("Ano final", min_value=2000, max_value=now_year, value=default_end)

        if start_year > end_year:
            st.warning("Ano inicial não pode ser maior que o ano final.")
            st.markdown("</div>", unsafe_allow_html=True)
            st.stop()

        run = st.button("Atualizar agora", use_container_width=True)

        progress = st.progress(0, text="Aguardando início…")
        log_box = st.empty()
        logs: list[str] = []

        def progress_cb(pct: float, msg: str = "") -> None:
            pct = max(0.0, min(100.0, float(pct)))
            progress.progress(int(pct), text=msg or f"Progresso: {pct:.0f}%")
            if msg:
                logs.append(msg)
                log_box.markdown("\n".join([f"- {x}" for x in logs[-12:]]))

        if run:
            try:
                progress_cb(2, "Preparando atualização…")
                apply_update(engine=engine, start_year=int(start_year), end_year=int(end_year), progress_cb=progress_cb)
                st.success("Atualização concluída com sucesso.")
                st.rerun()
            except Exception as e:
                progress.progress(0, text="Falha na atualização.")
                st.error(f"Erro na atualização: {e}")

        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.subheader("Resumo e diagnóstico")

        st.markdown(
            f"""
            - **Último ano no banco:** {last_year if last_year else "—"}
            - **Última atualização:** {_fmt_dt(last_run_at)}
            - **Último ano disponível (CVM):** {remote_latest_year if remote_latest_year else "—"}
            - **Atualizações pendentes:** {("Sim" if has_updates else "Não") if has_updates in [True, False] else "Indisponível"}
            """.strip()
        )

        notes = status.get("notes")
        if notes:
            st.caption(notes)

        st.markdown("</div>", unsafe_allow_html=True)
