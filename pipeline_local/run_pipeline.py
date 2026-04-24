"""
pipeline_local/run_pipeline.py
Orquestrador do pipeline local completo.

Executa as fases em sequência ou individualmente:
  extract   → download CVM ZIPs → banco local raw
  transform → normaliza e enriquece → tabelas enriched e final
  publish   → publica no Supabase (dry_run por padrão)
  audit     → valida qualidade e consistência

Uso:
  # Pipeline completo (extract + transform + dry_run publish + audit)
  python -m pipeline_local.run_pipeline --stage all

  # Só extração
  python -m pipeline_local.run_pipeline --stage extract

  # Extração + transformação
  python -m pipeline_local.run_pipeline --stage extract,transform

  # Publish de fato (não é dry_run)
  python -m pipeline_local.run_pipeline --stage publish --publish-mode publish

  # DFP apenas de 2020 a 2024
  python -m pipeline_local.run_pipeline --stage extract,transform --source DFP --year-start 2020 --year-end 2024

  # Audit completo
  python -m pipeline_local.run_pipeline --stage audit
"""
from __future__ import annotations

import argparse
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipeline_local.utils.logger import get_logger

log = get_logger("run_pipeline")

# ---------------------------------------------------------------------------
# Stages
# ---------------------------------------------------------------------------

def _stage_extract(source: str, year_start: Optional[int], year_end: Optional[int], force: bool) -> Dict:
    results = {}
    if source.upper() in ("DFP", "ALL"):
        from pipeline_local.extract.extract_cvm_dfp_local import run as run_dfp
        log.info("Iniciando extração DFP")
        t0 = time.time()
        r = run_dfp(start_year=year_start, end_year=year_end, force_reload=force)
        results["dfp"] = {**r, "elapsed_s": round(time.time() - t0, 1)}

    if source.upper() in ("ITR", "ALL"):
        from pipeline_local.extract.extract_cvm_itr_local import run as run_itr
        log.info("Iniciando extração ITR")
        t0 = time.time()
        r = run_itr(start_year=year_start, end_year=year_end, force_reload=force)
        results["itr"] = {**r, "elapsed_s": round(time.time() - t0, 1)}

    return results


def _stage_transform(source: str) -> Dict:
    results = {}
    enrich_source = source.upper() if source.upper() in ("DFP", "ITR") else "ALL"

    from pipeline_local.transform.enrich_cvm_raw_local import run as run_enrich
    log.info("Iniciando enriquecimento")
    t0 = time.time()
    r = run_enrich(source=enrich_source)
    results["enrich"] = {**r, "elapsed_s": round(time.time() - t0, 1)}

    from pipeline_local.transform.build_financials_local import run as run_build
    log.info("Iniciando build financials")
    t0 = time.time()
    r = run_build(source=enrich_source)
    results["build"] = {**r, "elapsed_s": round(time.time() - t0, 1)}

    return results


def _stage_publish(mode: str, source: str, tickers: Optional[List[str]]) -> Dict:
    from pipeline_local.publish.publish_financials_supabase import run as run_pub
    log.info("Iniciando publish", mode=mode)
    t0 = time.time()
    r = run_pub(mode=mode, source=source, tickers=tickers)
    return {**r, "elapsed_s": round(time.time() - t0, 1)}


def _stage_audit(ticker: Optional[str], year: Optional[int]) -> Dict:
    from pipeline_local.audit.pipeline_audit import run_all
    log.info("Iniciando auditoria")
    t0 = time.time()
    r = run_all(ticker=ticker, year=year)
    return {**r, "elapsed_s": round(time.time() - t0, 1)}


# ---------------------------------------------------------------------------
# Orquestrador principal
# ---------------------------------------------------------------------------

def run(
    stages: List[str],
    source: str = "ALL",
    year_start: Optional[int] = None,
    year_end: Optional[int] = None,
    publish_mode: str = "dry_run",
    tickers: Optional[List[str]] = None,
    force_reload: bool = False,
    audit_ticker: Optional[str] = None,
) -> Dict:
    run_id = str(uuid.uuid4())
    started_at = datetime.now(tz=timezone.utc)
    log.info(
        "Pipeline local iniciado",
        run_id=run_id,
        stages=stages,
        source=source,
        year_start=year_start,
        year_end=year_end,
        publish_mode=publish_mode,
    )

    results: Dict = {"run_id": run_id, "stages": {}}
    overall_ok = True

    if "extract" in stages or "all" in stages:
        try:
            results["stages"]["extract"] = _stage_extract(source, year_start, year_end, force_reload)
            log.info("Estágio extract concluído", run_id=run_id)
        except Exception as exc:
            log.error("Estágio extract falhou", run_id=run_id, erro=str(exc))
            results["stages"]["extract"] = {"error": str(exc)}
            overall_ok = False
            if "all" not in stages:
                return results  # para aqui se explícito

    if "transform" in stages or "all" in stages:
        try:
            results["stages"]["transform"] = _stage_transform(source)
            log.info("Estágio transform concluído", run_id=run_id)
        except Exception as exc:
            log.error("Estágio transform falhou", run_id=run_id, erro=str(exc))
            results["stages"]["transform"] = {"error": str(exc)}
            overall_ok = False
            if "all" not in stages:
                return results

    if "publish" in stages or "all" in stages:
        try:
            results["stages"]["publish"] = _stage_publish(publish_mode, source, tickers)
            log.info("Estágio publish concluído", run_id=run_id, mode=publish_mode)
        except Exception as exc:
            log.error("Estágio publish falhou", run_id=run_id, erro=str(exc))
            results["stages"]["publish"] = {"error": str(exc)}
            overall_ok = False

    if "audit" in stages or "all" in stages:
        try:
            results["stages"]["audit"] = _stage_audit(audit_ticker, year_end)
            log.info("Estágio audit concluído", run_id=run_id)
        except Exception as exc:
            log.error("Estágio audit falhou", run_id=run_id, erro=str(exc))
            results["stages"]["audit"] = {"error": str(exc)}
            overall_ok = False

    elapsed = (datetime.now(tz=timezone.utc) - started_at).total_seconds()
    results["overall_status"] = "success" if overall_ok else "partial"
    results["elapsed_s"] = round(elapsed, 1)

    log.summary(
        pipeline="run_pipeline",
        status=results["overall_status"],
        run_id=run_id,
        stages_run=stages,
        elapsed_s=elapsed,
    )
    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Orquestrador do pipeline local DFP/ITR → Supabase",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--stage",
        default="all",
        help="Estágios a executar, separados por vírgula: extract,transform,publish,audit,all",
    )
    parser.add_argument(
        "--source",
        default="ALL",
        choices=["DFP", "ITR", "ALL"],
        help="Fonte de dados (DFP | ITR | ALL)",
    )
    parser.add_argument("--year-start", type=int, default=None, help="Ano inicial da extração")
    parser.add_argument("--year-end", type=int, default=None, help="Ano final da extração")
    parser.add_argument(
        "--publish-mode",
        default="dry_run",
        choices=["dry_run", "publish"],
        help="dry_run (padrão — seguro) ou publish (escreve no Supabase)",
    )
    parser.add_argument(
        "--tickers",
        default=None,
        help="Tickers separados por vírgula para publish parcial (ex: PETR3,VALE3)",
    )
    parser.add_argument(
        "--force-reload",
        action="store_true",
        help="Força re-download dos ZIPs mesmo se já cacheados",
    )
    parser.add_argument("--audit-ticker", default=None, help="Ticker para sample check na auditoria")

    args = parser.parse_args()
    stages = [s.strip().lower() for s in args.stage.split(",")]
    tickers = [t.strip().upper() for t in args.tickers.split(",")] if args.tickers else None

    import json
    result = run(
        stages=stages,
        source=args.source,
        year_start=args.year_start,
        year_end=args.year_end,
        publish_mode=args.publish_mode,
        tickers=tickers,
        force_reload=args.force_reload,
        audit_ticker=args.audit_ticker,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    sys.exit(0 if result.get("overall_status") == "success" else 1)


if __name__ == "__main__":
    main()
