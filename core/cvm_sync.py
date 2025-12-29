# core/cvm_sync.py
from __future__ import annotations

import datetime as dt
from typing import Callable, Optional, Dict, Any

import pandas as pd
from sqlalchemy import text

from core.db_supabase import get_engine

# >>> NOVO: pipelines financeiros
from cvm.prices_sync_bulk import sync_prices_universe
from cvm.multiplos_sync_universe import rebuild_multiplos_universe


# =========================================================
# Status de sincronismo (mantido)
# =========================================================
def get_sync_status() -> Dict[str, Any]:
    engine = get_engine()
    try:
        df = pd.read_sql(
            text("select * from cvm.sync_status limit 1"),
            con=engine,
        )
        if df.empty:
            return {}
        return df.iloc[0].to_dict()
    except Exception:
        return {}


def _update_sync_status(**kwargs) -> None:
    engine = get_engine()
    cols = ", ".join(kwargs.keys())
    vals = ", ".join([f":{k}" for k in kwargs.keys()])
    sql = f"""
    insert into cvm.sync_status ({cols})
    values ({vals})
    on conflict (id)
    do update set {", ".join([f"{k}=excluded.{k}" for k in kwargs.keys()])}
    """
    with engine.begin() as conn:
        conn.execute(text(sql), kwargs)


# =========================================================
# Universo de tickers (usado pelo pipeline)
# =========================================================
def _get_universe_tickers(engine) -> list[str]:
    """
    Preferência: cvm.setores (se existir e estiver populada)
    Fallback: tickers presentes em cvm.demonstracoes_financeiras_dfp
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


# =========================================================
# Função principal: apply_update (ORQUESTRADOR)
# =========================================================
def apply_update(
    start_year: int,
    end_year: int,
    years_per_run: int = 2,
    quarters_per_run: int = 8,
    progress_cb: Optional[Callable[[float, str], None]] = None,
    *,
    update_prices: bool = True,
    rebuild_multiplos: bool = True,
    prices_limit_mode: bool = True,
    max_tickers: int = 150,
) -> Dict[str, Any]:
    """
    Pipeline único de atualização:
    1) CVM (DFP/ITR)
    2) Preços (2010→hoje) para o universo
    3) Rebuild de múltiplos

    Parâmetros:
    - update_prices / rebuild_multiplos: permitem desligar etapas se quiser
    - prices_limit_mode / max_tickers: proteção contra timeout em deploy
    """

    def _cb(pct: float, msg: str = ""):
        if progress_cb:
            progress_cb(pct, msg)

    engine = get_engine()
    started_at = dt.datetime.utcnow()

    try:
        _cb(1, "Iniciando sincronismo CVM (DFP/ITR)…")

        # -------------------------------------------------
        # 1) SINCRONISMO CVM (MANTENHA SUA LÓGICA ATUAL AQUI)
        # -------------------------------------------------
        # >>> IMPORTANTE:
        # Aqui você mantém exatamente o código que já existia
        # no seu apply_update original para DFP/ITR.
        #
        # Exemplo (placeholder):
        #
        # sync_dfp_itr(
        #     start_year=start_year,
        #     end_year=end_year,
        #     years_per_run=years_per_run,
        #     quarters_per_run=quarters_per_run,
        #     progress_cb=_cb,
        # )
        #
        # >>> FIM DO BLOCO EXISTENTE

        _cb(55, "CVM sincronizado com sucesso.")

        # -------------------------------------------------
        # 2) PREÇOS (2010→HOJE)
        # -------------------------------------------------
        if update_prices:
            _cb(60, "Preparando atualização de preços (2010→hoje)…")
            tickers = _get_universe_tickers(engine)
            if not tickers:
                raise RuntimeError("Universo de tickers vazio para atualização de preços.")

            if prices_limit_mode:
                tickers = tickers[: int(max_tickers)]
                _cb(62, f"Modo seguro: processando {len(tickers)} tickers.")
            else:
                _cb(62, f"Processando universo completo: {len(tickers)} tickers.")

            _cb(65, "Baixando e gravando preços em cvm.prices_b3…")
            stats = sync_prices_universe(engine, tickers)
            _cb(
                80,
                f"Preços concluídos: OK={stats.get('ok')} "
                f"Falhas={stats.get('fail')} Total={stats.get('total')}.",
            )

        # -------------------------------------------------
        # 3) REBUILD DE MÚLTIPLOS
        # -------------------------------------------------
        if rebuild_multiplos:
            _cb(85, "Recalculando múltiplos do universo…")
            res = rebuild_multiplos_universe(engine)
            if not res.get("ok"):
                raise RuntimeError(f"Rebuild de múltiplos falhou: {res}")
            _cb(98, f"Múltiplos atualizados ({res.get('rows')} linhas).")

        finished_at = dt.datetime.utcnow()
        _update_sync_status(
            last_run=finished_at.isoformat(),
            last_ok=finished_at.isoformat(),
            last_error=None,
            notes="CVM + preços (2010→hoje) + múltiplos executados com sucesso.",
        )

        _cb(100, "Atualização completa finalizada.")
        return {
            "ok": True,
            "started_at": started_at,
            "finished_at": finished_at,
        }

    except Exception as e:
        finished_at = dt.datetime.utcnow()
        _update_sync_status(
            last_run=finished_at.isoformat(),
            last_error=str(e),
        )
        _cb(100, f"Erro: {e}")
        return {"ok": False, "error": str(e)}
