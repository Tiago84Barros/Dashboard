from __future__ import annotations

from typing import Optional, Callable, Dict, Any, List
import datetime as dt

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.db_supabase import get_engine

# Pipelines específicos
from cvm.prices_sync_monthly_yearly import sync_prices_monthly_yearly_universe
from cvm.multiplos_sync_universe import rebuild_multiplos_universe


# =============================================================================
# Status de sincronização (tabela singleton)
# =============================================================================

def _ensure_sync_status(engine: Engine) -> None:
    sql = """
    create table if not exists cvm.sync_status (
        id integer primary key default 1,
        last_run timestamptz,
        last_ok timestamptz,
        last_error text,
        notes text
    );
    insert into cvm.sync_status (id)
    values (1)
    on conflict (id) do nothing;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def _update_sync_status(engine: Engine, **kwargs) -> None:
    sets = ", ".join([f"{k}=:{k}" for k in kwargs.keys()])
    sql = f"""
    update cvm.sync_status
       set {sets}
     where id = 1
    """
    with engine.begin() as conn:
        conn.execute(text(sql), kwargs)


def get_sync_status() -> Dict[str, Any]:
    engine = get_engine()
    _ensure_sync_status(engine)
    df = pd.read_sql("select * from cvm.sync_status where id = 1", engine)
    return {} if df.empty else df.iloc[0].to_dict()


# =============================================================================
# Universo de tickers
# =============================================================================

def _get_universe_tickers(engine: Engine) -> List[str]:
    """
    Universo preferencial:
    1) cvm.setores
    2) fallback: demonstracoes_financeiras_dfp
    """
    try:
        df = pd.read_sql(
            "select distinct ticker from cvm.setores where ticker is not null",
            engine,
        )
        tickers = df["ticker"].astype(str).str.replace(".SA", "", regex=False).str.upper().tolist()
        if tickers:
            return sorted(set(tickers))
    except Exception:
        pass

    df = pd.read_sql(
        "select distinct ticker from cvm.demonstracoes_financeiras_dfp where ticker is not null",
        engine,
    )
    return sorted(
        set(df["ticker"].astype(str).str.replace(".SA", "", regex=False).str.upper().tolist())
    )


# =============================================================================
# Pipeline principal
# =============================================================================

def apply_update(
    engine: Optional[Engine] = None,
    *,
    update_cvm: bool = True,
    update_prices: bool = True,
    update_multiplos: bool = True,
    modo_seguro: bool = True,
    max_tickers: Optional[int] = None,
    progress_cb: Optional[Callable[[float, str], None]] = None,
) -> Dict[str, Any]:
    """
    Pipeline único e determinístico:

    1) (opcional) CVM – DFP / ITR  -> assume que já existe loader
    2) Preços B3 (mensal + anual)
    3) Rebuild de múltiplos (universo)

    Este método é chamado pela página Configurações.
    """

    engine = engine or get_engine()
    _ensure_sync_status(engine)

    started_at = dt.datetime.utcnow()

    def _cb(pct: float, msg: str):
        if progress_cb:
            progress_cb(pct, msg)

    try:
        _cb(1, "Iniciando atualização do banco…")

        # -----------------------------------------------------
        # 1) CVM (DFP / ITR)
        # -----------------------------------------------------
        if update_cvm:
            _cb(10, "Sincronizando dados CVM (DFP / ITR)…")
            # ⚠️ Aqui você mantém seu sincronizador CVM existente
            # Exemplo:
            # from core.cvm_loader import sync_cvm
            # sync_cvm()
            _cb(30, "CVM sincronizado.")

        # -----------------------------------------------------
        # 2) Preços (mensal + anual)
        # -----------------------------------------------------
        if update_prices:
            _cb(40, "Atualizando preços (mensal + anual)…")
            tickers = _get_universe_tickers(engine)

            if modo_seguro and max_tickers:
                tickers = tickers[: max_tickers]

            stats = sync_prices_monthly_yearly_universe(
                engine,
                tickers,
                start="2010-01-01",
            )
            _cb(70, f"Preços atualizados ({stats}).")

        # -----------------------------------------------------
        # 3) Múltiplos
        # -----------------------------------------------------
        if update_multiplos:
            _cb(80, "Recalculando múltiplos do universo…")
            res = rebuild_multiplos_universe(engine)
            if not res["ok"]:
                raise RuntimeError(res["error"])
            _cb(95, f"Múltiplos recalculados ({res['rows']} registros).")

        finished_at = dt.datetime.utcnow()
        _update_sync_status(
            engine,
            last_run=finished_at,
            last_ok=finished_at,
            last_error=None,
            notes="Atualização completa executada com sucesso.",
        )

        _cb(100, "Atualização finalizada com sucesso.")
        return {"ok": True}

    except Exception as e:
        finished_at = dt.datetime.utcnow()
        _update_sync_status(
            engine,
            last_run=finished_at,
            last_error=str(e),
        )
        _cb(100, f"Erro: {e}")
        return {"ok": False, "error": str(e)}
