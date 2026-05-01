#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pickup/ingest_pdfs_all.py
--------------------------
Orquestrador de ingestão de documentos corporativos para TODAS as empresas
do banco de dados Supabase.

Estratégia:
  1. Busca todos os tickers com dados financeiros no Supabase (tabela multiplos).
  2. Para cada ticker, verifica quantos docs já existem e quando foi o último.
  3. Pula tickers recentemente indexados (controle incremental).
  4. Roda a pipeline: IPE (CVM dataset aberto) → ENET (CVM consulta externa).
  5. Grava log de progresso em pickup/ingest_pdfs_all.log (JSON lines).

Uso:
  # Todos os tickers com dados no banco
  python pickup/ingest_pdfs_all.py

  # Apenas uma empresa
  python pickup/ingest_pdfs_all.py --ticker PETR4

  # Lista de empresas
  python pickup/ingest_pdfs_all.py --ticker PETR4,VALE3,ITUB4

  # Forçar reprocessamento (ignora controle incremental)
  python pickup/ingest_pdfs_all.py --force

  # Janela maior de documentos (padrão: 36 meses)
  python pickup/ingest_pdfs_all.py --months 60

  # Pular tickers que já foram atualizados nos últimos N dias (padrão: 30)
  python pickup/ingest_pdfs_all.py --skip-days 7

  # Modo seco: mostra o que faria sem processar nada
  python pickup/ingest_pdfs_all.py --dry-run

Requisitos adicionais ao requirements.txt:
  pypdf  (ou PyPDF2)  — já usado nos ingest_docs_*
  requests            — já presente
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

# ── garante que o root do projeto está no PYTHONPATH ──────────────────────────
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd
from sqlalchemy import text

from core.db_loader import get_supabase_engine
from core.ticker_utils import normalize_ticker

# ── pipelines existentes ──────────────────────────────────────────────────────
from pickup.ingest_docs_cvm_ipe import ingest_ipe_for_tickers
from pickup.ingest_docs_cvm_enet import ingest_enet_for_tickers

# ── constantes ────────────────────────────────────────────────────────────────
LOG_FILE = Path(__file__).parent / "ingest_pdfs_all.log"
PROGRESS_FILE = Path(__file__).parent / "ingest_pdfs_all_progress.json"

DEFAULT_WINDOW_MONTHS   = 36    # janela de documentos a buscar
DEFAULT_MAX_DOCS        = 80    # máx docs por ticker por pipeline
DEFAULT_MAX_PDFS        = 20    # máx PDFs baixados por ticker
DEFAULT_SKIP_DAYS       = 30    # pula tickers atualizados há menos de N dias
DEFAULT_SLEEP_S         = 1.0   # pausa entre tickers (evita rate-limit CVM)
DEFAULT_BATCH_SIZE      = 10    # tickers por lote (log intermediário)


# ─────────────────────────────────────────────────────────────────────────────
# Log helpers
# ─────────────────────────────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(level: str, event: str, **fields: Any) -> None:
    record = {"ts": _ts(), "level": level, "event": event}
    record.update(fields)
    line = json.dumps(record, ensure_ascii=False, default=str)
    print(line, flush=True)
    try:
        with LOG_FILE.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception:
        pass


def _print(msg: str) -> None:
    """Saída legível para humanos (além do log JSON)."""
    print(msg, flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# Progresso persistente (retomada automática)
# ─────────────────────────────────────────────────────────────────────────────

def _load_progress() -> Dict[str, Any]:
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"done": [], "errors": {}}


def _save_progress(prog: Dict[str, Any]) -> None:
    try:
        PROGRESS_FILE.write_text(
            json.dumps(prog, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Consultas Supabase
# ─────────────────────────────────────────────────────────────────────────────

def _get_all_tickers() -> List[str]:
    """Retorna todos os tickers distintos que têm dados financeiros no Supabase."""
    engine = get_supabase_engine()
    tickers: List[str] = []
    for table in ("multiplos", "Demonstracoes_Financeiras"):
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(f'SELECT DISTINCT "Ticker" FROM public."{table}" WHERE "Ticker" IS NOT NULL ORDER BY 1')
                ).fetchall()
            tickers += [str(r[0]).strip().upper() for r in rows if r[0]]
        except Exception as e:
            _log("WARN", "ticker_query_failed", table=table, error=str(e))

    # deduplica e normaliza
    seen: Dict[str, str] = {}
    for tk in tickers:
        norm = normalize_ticker(tk)
        if norm and norm not in seen:
            seen[norm] = norm
    result = sorted(seen.keys())
    _log("INFO", "tickers_found", total=len(result))
    return result


def _get_doc_status(tickers: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    """
    Retorna {ticker: {count, last_date}} para tickers que já têm documentos.
    Tickers sem documentos não aparecem no dict.
    """
    if not tickers:
        return {}
    engine = get_supabase_engine()
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT upper(ticker) as ticker,
                           count(*) as qtd,
                           max(coalesce(data, created_at, updated_at)) as last_date
                    FROM public.docs_corporativos
                    WHERE upper(ticker) = ANY(:tks)
                    GROUP BY upper(ticker)
                    """
                ),
                {"tks": [normalize_ticker(t) for t in tickers]},
            ).fetchall()
        return {
            str(r[0]).upper(): {
                "count": int(r[1]),
                "last_date": r[2],
            }
            for r in rows
        }
    except Exception as e:
        _log("WARN", "doc_status_query_failed", error=str(e))
        return {}


def _should_skip(ticker: str, status: Dict[str, Any], skip_days: int, force: bool) -> bool:
    """True se o ticker tem docs recentes e não está em modo force."""
    if force:
        return False
    info = status.get(ticker.upper())
    if not info or not info.get("count"):
        return False  # sem docs → não pular
    last = info.get("last_date")
    if last is None:
        return False
    try:
        last_dt = pd.to_datetime(last, utc=True)
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=skip_days)
        return last_dt >= pd.Timestamp(cutoff)
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline para um único ticker
# ─────────────────────────────────────────────────────────────────────────────

def _ingest_one(
    ticker: str,
    *,
    window_months: int,
    max_docs: int,
    max_pdfs: int,
) -> Dict[str, Any]:
    """
    Roda IPE → ENET para um ticker.
    Retorna dict com stats combinados.
    """
    result: Dict[str, Any] = {
        "ticker": ticker,
        "ipe": {},
        "enet": {},
        "ok": True,
        "error": None,
    }

    # ── Pipeline A: IPE (dataset aberto CVM) ─────────────────────────────────
    try:
        ipe_res = ingest_ipe_for_tickers(
            [ticker],
            window_months=window_months,
            max_docs_per_ticker=max_docs,
            max_pdfs_per_ticker=max_pdfs,
            strategic_only=True,
            download_pdfs=True,
            pdf_max_pages=30,
            request_timeout=30,
            max_runtime_s=300.0,   # sem timeout apertado — rodamos localmente
            sleep_s=0.2,
            verbose=False,
        )
        result["ipe"] = ipe_res.get("stats", {}).get(ticker, {})
    except Exception as e:
        result["ipe"] = {"error": str(e)}
        _log("WARN", "ipe_failed", ticker=ticker, error=str(e))

    # ── Pipeline B: ENET (CVM consulta externa por código CVM) ───────────────
    try:
        enet_res = ingest_enet_for_tickers(
            [ticker],
            anos=max(1, window_months // 12),
            max_docs_por_ticker=max_docs,
            baixar_e_extrair=True,
            sleep_s=0.2,
        )
        result["enet"] = enet_res.get("stats", {}).get(ticker, {})
    except Exception as e:
        result["enet"] = {"error": str(e)}
        _log("WARN", "enet_failed", ticker=ticker, error=str(e))

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Orquestrador principal
# ─────────────────────────────────────────────────────────────────────────────

def main(
    tickers_arg: Optional[List[str]] = None,
    *,
    window_months: int = DEFAULT_WINDOW_MONTHS,
    max_docs: int = DEFAULT_MAX_DOCS,
    max_pdfs: int = DEFAULT_MAX_PDFS,
    skip_days: int = DEFAULT_SKIP_DAYS,
    sleep_s: float = DEFAULT_SLEEP_S,
    force: bool = False,
    dry_run: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> None:
    start_ts = time.time()
    _log("INFO", "run_start", window_months=window_months, max_docs=max_docs,
         max_pdfs=max_pdfs, skip_days=skip_days, force=force, dry_run=dry_run)

    # ── 1. Lista de tickers ───────────────────────────────────────────────────
    if tickers_arg:
        tickers = [normalize_ticker(t) for t in tickers_arg if t.strip()]
        _print(f"\n🎯  Modo seletivo: {len(tickers)} ticker(s) solicitado(s).")
    else:
        _print("\n🔍  Buscando todos os tickers com dados no Supabase...")
        tickers = _get_all_tickers()
        _print(f"    → {len(tickers)} tickers encontrados.")

    if not tickers:
        _print("⚠️  Nenhum ticker para processar. Encerrando.")
        return

    # ── 2. Status atual de docs ───────────────────────────────────────────────
    _print("\n📊  Verificando status de documentos existentes...")
    status = _get_doc_status(tickers)
    already_indexed = sum(1 for t in tickers if t in status and status[t]["count"] > 0)
    _print(f"    → {already_indexed} tickers já têm documentos no banco.")

    # ── 3. Filtra tickers a processar ────────────────────────────────────────
    to_process = [t for t in tickers if not _should_skip(t, status, skip_days, force)]
    skipped = len(tickers) - len(to_process)

    _print(f"    → {skipped} tickers pulados (docs recentes ≤ {skip_days} dias).")
    _print(f"    → {len(to_process)} tickers a processar.\n")

    if not to_process:
        _print("✅  Nada a fazer. Todos os tickers estão atualizados.")
        _print(f"    Use --force ou --skip-days 0 para forçar reprocessamento.")
        return

    if dry_run:
        _print("🌵  Modo dry-run: listando tickers que seriam processados:\n")
        for i, tk in enumerate(to_process, 1):
            info = status.get(tk, {})
            docs_str = f"{info['count']} docs" if info.get("count") else "sem docs"
            _print(f"    {i:4d}. {tk:<12} ({docs_str})")
        _print(f"\nTotal: {len(to_process)} tickers.")
        return

    # ── 4. Carrega progresso anterior ────────────────────────────────────────
    prog = _load_progress()
    done_set = set(prog.get("done", []))

    # Remove já concluídos desta sessão (progresso persiste entre execuções)
    pending = [t for t in to_process if t not in done_set]
    if len(pending) < len(to_process):
        resuming = len(to_process) - len(pending)
        _print(f"⏩  Retomando: {resuming} tickers já concluídos em execução anterior. "
               f"{len(pending)} restantes.\n")

    # ── 5. Loop principal ─────────────────────────────────────────────────────
    total   = len(pending)
    ok_count    = 0
    err_count   = 0
    skip_count  = 0

    _print("=" * 60)
    _print(f"  INICIANDO INGESTÃO  —  {total} tickers  —  {_ts()}")
    _print("=" * 60)

    for idx, ticker in enumerate(pending, 1):
        elapsed = time.time() - start_ts
        eta_s   = (elapsed / idx) * (total - idx) if idx > 1 else 0
        eta_str = f"{int(eta_s // 60)}min {int(eta_s % 60)}s" if eta_s > 0 else "?"

        _print(f"\n[{idx:4d}/{total}]  {ticker:<12}  |  ETA: {eta_str}")

        # mostra status atual
        info = status.get(ticker, {})
        if info.get("count"):
            _print(f"            ↳ Atualização  (já tem {info['count']} docs)")
        else:
            _print(f"            ↳ Primeira ingestão")

        try:
            res = _ingest_one(
                ticker,
                window_months=window_months,
                max_docs=max_docs,
                max_pdfs=max_pdfs,
            )

            # resumo por ticker
            ipe_ins  = res["ipe"].get("inserted", res["ipe"].get("docs_inserted", 0)) or 0
            enet_ins = res["enet"].get("inserted", res["enet"].get("docs_inserted", 0)) or 0
            ipe_pdf  = res["ipe"].get("downloaded", res["ipe"].get("pdfs_downloaded", 0)) or 0

            _print(f"            ✓ IPE: {ipe_ins} docs novos ({ipe_pdf} PDFs)  |  "
                   f"ENET: {enet_ins} docs novos")

            _log("INFO", "ticker_done", ticker=ticker,
                 ipe_inserted=ipe_ins, enet_inserted=enet_ins, ipe_pdfs=ipe_pdf)

            prog["done"].append(ticker)
            ok_count += 1

        except Exception as e:
            _print(f"            ✗ ERRO: {e}")
            _log("ERROR", "ticker_failed", ticker=ticker, error=str(e))
            prog["errors"][ticker] = str(e)
            err_count += 1

        _save_progress(prog)

        # ── lote: log intermediário ───────────────────────────────────────
        if idx % batch_size == 0:
            elapsed_m = int((time.time() - start_ts) // 60)
            _print(f"\n  ── LOTE {idx // batch_size} ──  "
                   f"ok={ok_count}  erros={err_count}  "
                   f"tempo={elapsed_m}min")

        # pausa gentil entre tickers
        if idx < total:
            time.sleep(sleep_s)

    # ── 6. Resumo final ───────────────────────────────────────────────────────
    total_s = int(time.time() - start_ts)
    _print("\n" + "=" * 60)
    _print(f"  CONCLUÍDO  —  {_ts()}")
    _print(f"  Tempo total : {total_s // 60}min {total_s % 60}s")
    _print(f"  ✅ Sucesso  : {ok_count}")
    _print(f"  ❌ Erros    : {err_count}")
    _print(f"  ⏭️ Pulados  : {skipped + skip_count}")
    _print("=" * 60)

    if err_count:
        _print(f"\n⚠️  {err_count} ticker(s) com erro. Detalhes em: {LOG_FILE}")
        _print("    Rode novamente — tickers com erro não foram marcados como concluídos.")

    if ok_count == total:
        # limpa arquivo de progresso (tudo feito)
        try:
            PROGRESS_FILE.unlink(missing_ok=True)
        except Exception:
            pass

    _log("INFO", "run_end", ok=ok_count, errors=err_count,
         skipped=skipped, total_s=total_s)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Ingestão de documentos corporativos para todas as empresas do Supabase.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python pickup/ingest_pdfs_all.py
  python pickup/ingest_pdfs_all.py --ticker PETR4
  python pickup/ingest_pdfs_all.py --ticker PETR4,VALE3,ITUB4
  python pickup/ingest_pdfs_all.py --force --months 60
  python pickup/ingest_pdfs_all.py --dry-run
  python pickup/ingest_pdfs_all.py --skip-days 7
        """,
    )
    p.add_argument(
        "--ticker", "-t",
        default=None,
        help="Ticker(s) separados por vírgula. Se omitido, processa todos do banco.",
    )
    p.add_argument(
        "--months", "-m",
        type=int,
        default=DEFAULT_WINDOW_MONTHS,
        help=f"Janela de meses para buscar documentos (padrão: {DEFAULT_WINDOW_MONTHS}).",
    )
    p.add_argument(
        "--max-docs",
        type=int,
        default=DEFAULT_MAX_DOCS,
        help=f"Máximo de documentos por ticker por pipeline (padrão: {DEFAULT_MAX_DOCS}).",
    )
    p.add_argument(
        "--max-pdfs",
        type=int,
        default=DEFAULT_MAX_PDFS,
        help=f"Máximo de PDFs baixados por ticker (padrão: {DEFAULT_MAX_PDFS}).",
    )
    p.add_argument(
        "--skip-days",
        type=int,
        default=DEFAULT_SKIP_DAYS,
        help=f"Pula tickers atualizados há menos de N dias (padrão: {DEFAULT_SKIP_DAYS}).",
    )
    p.add_argument(
        "--sleep",
        type=float,
        default=DEFAULT_SLEEP_S,
        help=f"Pausa em segundos entre tickers (padrão: {DEFAULT_SLEEP_S}).",
    )
    p.add_argument(
        "--force", "-f",
        action="store_true",
        help="Reprocessa mesmo tickers com docs recentes.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Lista o que seria processado sem executar nada.",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Tickers por lote para log intermediário (padrão: {DEFAULT_BATCH_SIZE}).",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    tickers_arg: Optional[List[str]] = None
    if args.ticker:
        tickers_arg = [t.strip() for t in args.ticker.split(",") if t.strip()]

    main(
        tickers_arg=tickers_arg,
        window_months=args.months,
        max_docs=args.max_docs,
        max_pdfs=args.max_pdfs,
        skip_days=args.skip_days,
        sleep_s=args.sleep,
        force=args.force,
        dry_run=args.dry_run,
        batch_size=args.batch_size,
    )
