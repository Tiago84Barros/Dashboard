from __future__ import annotations

import datetime as dt
import inspect
import logging
from typing import Any, Callable, Dict, Optional, Tuple, List

import requests
from sqlalchemy import text

from core.db.engine import get_engine

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# CVM "latest year" resolver
# ─────────────────────────────────────────────────────────────────────
def _cvm_candidate_urls(year: int) -> List[str]:
    """
    Endpoints típicos do data lake da CVM.
    Você pode ajustar se sua fonte for outra.
    """
    # DFP e ITR são os mais comuns em projetos desse tipo.
    # Existem variações por "CIA_ABERTA" e pastas.
    return [
        f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_{year}.zip",
        f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/itr_cia_aberta_{year}.zip",
        # Algumas bases existem em "dados/CIA_ABERTA" com outros documentos.
    ]


def _url_exists(url: str, timeout: int = 15) -> bool:
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True)
        if r.status_code == 405:  # alguns servidores bloqueiam HEAD
            r = requests.get(url, timeout=timeout, stream=True, allow_redirects=True)
        return 200 <= r.status_code < 400
    except Exception:
        return False


def _get_remote_latest_year(
    start_year: Optional[int] = None,
    max_back: int = 15,
) -> Optional[int]:
    """
    Procura o último ano disponível na CVM testando URLs esperadas.
    - start_year default: ano corrente
    - max_back: quantos anos pra trás pesquisar
    """
    y0 = start_year or dt.datetime.now().year
    for y in range(y0, y0 - max_back, -1):
        urls = _cvm_candidate_urls(y)
        if any(_url_exists(u) for u in urls):
            return y
    return None


# ─────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────
def _ensure_sync_log_table(engine) -> None:
    """
    Cria tabela de log se não existir (idempotente).
    Se você já criou via SQL editor, isso só garante.
    """
    sql = """
    create schema if not exists cvm;

    create table if not exists cvm.sync_log (
      id bigserial primary key,
      run_at timestamptz not null default now(),
      status text not null default 'ok',
      last_year int,
      remote_latest_year int,
      message text
    );

    create index if not exists idx_sync_log_run_at on cvm.sync_log (run_at desc);
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def _read_last_log(engine) -> Tuple[Optional[int], Optional[str]]:
    """
    Retorna (last_year, last_run_at_iso) do último log.
    """
    with engine.begin() as conn:
        row = conn.execute(
            text("select last_year, run_at from cvm.sync_log order by run_at desc limit 1")
        ).fetchone()
    if not row:
        return None, None
    last_year = row[0]
    run_at = row[1]
    return last_year, (run_at.isoformat() if run_at else None)


def _get_last_year_dfp(engine) -> Optional[int]:
    """
    Retorna o último ano inserido com base na tabela
    cvm.demonstracoes_financeiras_dfp
    """
    sql = """
        select max(ano) 
        from cvm.demonstracoes_financeiras_dfp
    """
    with engine.begin() as conn:
        val = conn.execute(text(sql)).scalar()
    return int(val) if val is not None else None


def _write_log(engine, status: str, last_year: Optional[int], remote_latest_year: Optional[int], message: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                insert into cvm.sync_log (status, last_year, remote_latest_year, message)
                values (:status, :last_year, :remote_latest_year, :message)
                """
            ),
            {
                "status": status,
                "last_year": last_year,
                "remote_latest_year": remote_latest_year,
                "message": message[:2000],
            },
        )


# ─────────────────────────────────────────────────────────────────────
# Public API expected by page/configuracoes.py
# ─────────────────────────────────────────────────────────────────────
def get_sync_status(engine=None) -> Dict[str, Any]:
    engine = engine or get_engine()

    try:
        _ensure_sync_log_table(engine)
    except Exception:
        pass

    # 1️⃣ Último ano inserido (FONTE ÚNICA)
    try:
        last_year = _get_last_year_dfp(engine)
    except Exception:
        last_year = None

    # 2️⃣ Última execução registrada
    try:
        _, last_run_at = _read_last_log(engine)
    except Exception:
        last_run_at = None

    # 3️⃣ Último ano disponível na CVM
    try:
        remote_latest_year = _get_remote_latest_year()
    except Exception:
        remote_latest_year = None

    # 4️⃣ Existe atualização?
    has_updates = None
    if last_year is not None and remote_latest_year is not None:
        has_updates = remote_latest_year > last_year

    return {
        "last_year": last_year,
        "last_run_at": last_run_at,
        "remote_latest_year": remote_latest_year,
        "has_updates": has_updates,
    }


# ─────────────────────────────────────────────────────────────────────
# Update runner
# ─────────────────────────────────────────────────────────────────────
def _call_best_fn(module, engine, year: Optional[int], progress_cb: Optional[Callable[[float, str], None]]) -> None:
    """
    Tenta encontrar uma função executável no módulo (run/main/ingest/sync/update).
    Se aceitar (engine, year, progress_cb), passamos. Caso contrário, adaptamos.
    """
    for name in ("run", "main", "ingest", "sync", "update"):
        fn = getattr(module, name, None)
        if callable(fn):
            sig = inspect.signature(fn)
            kwargs = {}
            if "engine" in sig.parameters:
                kwargs["engine"] = engine
            if year is not None and "year" in sig.parameters:
                kwargs["year"] = year
            if "progress_cb" in sig.parameters and progress_cb is not None:
                kwargs["progress_cb"] = progress_cb

            # Alguns autores usam ano como primeiro positional
            try:
                if len(kwargs) > 0:
                    fn(**kwargs)
                else:
                    fn()
                return
            except TypeError:
                # Último fallback: tenta positional (engine, year)
                args = []
                if "engine" in sig.parameters:
                    args.append(engine)
                if year is not None and "year" in sig.parameters:
                    args.append(year)
                fn(*args)
                return

    raise RuntimeError(f"Nenhuma função executável encontrada em {module.__name__} (esperado: run/main/ingest/sync/update).")


def apply_update(engine=None, progress_cb: Optional[Callable[[float, str], None]] = None) -> None:
    """
    Executa atualização CVM → Supabase.
    Estratégia:
      - Lê last_year (log ou inferência)
      - Descobre remote_latest_year na CVM
      - Atualiza do (last_year+1) até remote_latest_year (inclusive)
    """
    engine = engine or get_engine()

    def cb(pct: float, msg: str) -> None:
        if progress_cb:
            progress_cb(pct, msg)
        logger.info("%s%% - %s", pct, msg)

    try:
        _ensure_sync_log_table(engine)
    except Exception:
        pass

    status = get_sync_status(engine=engine)
    last_year = status.get("last_year")
    remote_latest_year = status.get("remote_latest_year")

    # Se não achou o remote, faz fallback pro ano corrente
    if remote_latest_year is None:
        remote_latest_year = dt.datetime.now().year

    # Se não existe last_year ainda, vamos pelo ano remoto apenas (1 ano) para não explodir runtime
    if last_year is None:
        years = [remote_latest_year]
    else:
        if remote_latest_year <= last_year:
            cb(100, "Base já está atualizada. Nenhuma atualização necessária.")
            try:
                _write_log(engine, "ok", last_year, remote_latest_year, "Nenhuma atualização necessária.")
            except Exception:
                pass
            return
        years = list(range(last_year + 1, remote_latest_year + 1))

    # Importa módulos de ingest existentes
    # Ajuste aqui se seus nomes forem diferentes.
    try:
        import cvm.cvm_dfp_ingest as dfp_mod
    except Exception:
        dfp_mod = None

    try:
        import cvm.cvm_itr_ingest as itr_mod
    except Exception:
        itr_mod = None

    if dfp_mod is None and itr_mod is None:
        raise RuntimeError("Não encontrei módulos cvm.cvm_dfp_ingest nem cvm.cvm_itr_ingest. Verifique nomes/caminhos.")

    total_steps = max(1, len(years) * (2 if (dfp_mod and itr_mod) else 1))
    done = 0

    cb(2, f"Preparando atualização. Anos-alvo: {years[0]}..{years[-1]}")

    try:
        for y in years:
            if dfp_mod:
                cb(5 + (done / total_steps) * 90, f"DFP: iniciando ingest do ano {y}...")
                _call_best_fn(dfp_mod, engine=engine, year=y, progress_cb=progress_cb)
                done += 1
                cb(5 + (done / total_steps) * 90, f"DFP: concluído {y}")

            if itr_mod:
                cb(5 + (done / total_steps) * 90, f"ITR: iniciando ingest do ano {y}...")
                _call_best_fn(itr_mod, engine=engine, year=y, progress_cb=progress_cb)
                done += 1
                cb(5 + (done / total_steps) * 90, f"ITR: concluído {y}")

        cb(98, "Finalizando e registrando log...")
        try:
            _write_log(engine, "ok", years[-1], remote_latest_year, f"Atualizado com sucesso até {years[-1]}.")
        except Exception:
            pass
        cb(100, "Atualização concluída.")
    except Exception as e:
        try:
            _write_log(engine, "error", last_year, remote_latest_year, f"Falha: {e}")
        except Exception:
            pass
        raise
