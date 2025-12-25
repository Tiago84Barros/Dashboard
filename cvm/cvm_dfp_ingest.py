# cvm_dfp_ingest.py - Antigo Algoritmo 1
from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.sync_state import ensure_sync_table, get_state, set_state
from core.config.settings import get_settings

URL_BASE_DFP = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"

DEFAULT_START_YEAR = 2010
DEFAULT_END_YEAR = 2025

STATE_TARGET_START = "cvm:dfp_target_start_year"
STATE_TARGET_END = "cvm:dfp_target_end_year"
STATE_NEXT_YEAR = "cvm:dfp_next_year"
STATE_LAST_DONE = "cvm:dfp_last_completed_year"


@dataclass(frozen=True)
class DfpConfig:
    start_year: int = DEFAULT_START_YEAR
    end_year: int = DEFAULT_END_YEAR
    years_per_run: int = 1
    timeout_sec: int = 120


def _col_as_series(df: pd.DataFrame, col: str) -> pd.Series:
    """
    Garante que df[col] seja uma Series.
    Em alguns cenários (colunas duplicadas), df[col] pode retornar um DataFrame.
    """
    obj = df[col]
    if isinstance(obj, pd.DataFrame):
        obj = obj.iloc[:, 0]
    return obj


def _dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove colunas duplicadas mantendo a primeira ocorrência.
    Isso evita erros do tipo:
      - DataFrame object has no attribute 'str'
      - operações de filtro/sort com df["COL"] retornando DataFrame
    """
    if df is None or df.empty:
        return df
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()
    return df


def _find_ticker_map() -> Path:
    s = get_settings()
    p = Path(s.cvm_to_ticker_path)
    if p.exists():
        return p
    raise FileNotFoundError(
        f"Não encontrei cvm_to_ticker.csv em {p}. Coloque em data/cvm_to_ticker.csv no repositório."
    )


def _ensure_table(engine: Engine) -> None:
    ddl = """
    create schema if not exists cvm;

    create table if not exists cvm.demonstracoes_financeiras_dfp (
        ticker text not null,
        data date not null,

        receita_liquida double precision,
        ebit double precision,
        lucro_liquido double precision,
        lpa double precision,

        ativo_total double precision,
        ativo_circulante double precision,

        passivo_circulante double precision,
        passivo_total double precision,
        patrimonio_liquido double precision,

        dividendos double precision,
        caixa_e_equivalentes double precision,

        divida_total double precision,
        divida_liquida double precision,

        primary key (ticker, data)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _download_year_zip(ano: int, timeout_sec: int) -> bytes:
    url = URL_BASE_DFP + f"dfp_cia_aberta_{ano}.zip"
    r = requests.get(url, timeout=timeout_sec)
    if r.status_code != 200:
        raise RuntimeError(f"Falha ao baixar DFP {ano}. HTTP {r.status_code}. URL={url}")
    return r.content


def _read_consolidated_csvs(zip_bytes: bytes) -> dict[str, pd.DataFrame]:
    out: dict[str, list[pd.DataFrame]] = {"DRE": [], "BPA": [], "BPP": [], "DFC": []}
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for name in z.namelist():
            if not (name.endswith(".csv") and "_con_" in name.lower()):
                continue
            with z.open(name) as f:
                df = pd.read_csv(
                    f, sep=";", decimal=",", encoding="ISO-8859-1", low_memory=False
                )

            # Dedup de colunas para impedir df["COL"] virar DataFrame
            df = _dedupe_columns(df)

            if "ORDEM_EXERC" in df.columns:
                # ORDEM_EXERC às vezes vem com espaços; normaliza
                ord_series = _col_as_series(df, "ORDEM_EXERC").astype(str).str.strip()
                df = df[ord_series == "ÚLTIMO"]

            u = name.upper()
            if "DRE" in u:
                out["DRE"].append(df)
            elif "BPA" in u:
                out["BPA"].append(df)
            elif "BPP" in u:
                out["BPP"].append(df)
            elif "DFC" in u:
                out["DFC"].append(df)

    return {k: (pd.concat(v, ignore_index=True) if v else pd.DataFrame()) for k, v in out.items()}


def _pick_value(
    df: pd.DataFrame,
    cd_conta: Optional[str] = None,
    ds_conta_in: Optional[list[str]] = None
) -> pd.DataFrame:
    if df.empty:
        return df

    x = _dedupe_columns(df.copy())

    if cd_conta is not None and "CD_CONTA" in x.columns:
        cd = _col_as_series(x, "CD_CONTA").astype(str).str.strip()
        x = x[cd == str(cd_conta).strip()]

    if ds_conta_in is not None and "DS_CONTA" in x.columns:
        ds = _col_as_series(x, "DS_CONTA").astype(str).str.strip()
        x = x[ds.isin([str(s).strip() for s in ds_conta_in])]

    # Ordena e remove duplicatas por empresa/data
    if "DT_REFER" in x.columns and "CD_CVM" in x.columns:
        # usa séries seguras
        x = x.sort_values("DT_REFER")
        x = x.drop_duplicates(subset=["CD_CVM", "DT_REFER"], keep="first")

    # Seleciona as colunas essenciais
    needed = ["CD_CVM", "DT_REFER", "VL_CONTA"]
    missing = [c for c in needed if c not in x.columns]
    if missing:
        # Se faltou algo, devolve vazio para não quebrar o fluxo
        return pd.DataFrame()

    out = x[needed].copy()

    # >>> FIX CRÍTICO: normaliza para numérico AQUI
    out["VL_CONTA"] = pd.to_numeric(out["VL_CONTA"], errors="coerce")

    return out


def _build_year_frame(
    df_dre: pd.DataFrame,
    df_bpa: pd.DataFrame,
    df_bpp: pd.DataFrame,
    df_dfc: pd.DataFrame
) -> pd.DataFrame:
    receita = _pick_value(df_dre, cd_conta="3.01").rename(columns={"VL_CONTA": "receita_liquida"})
    ebit = _pick_value(df_dre, cd_conta="3.05").rename(columns={"VL_CONTA": "ebit"})
    lucro = _pick_value(
        df_dre,
        ds_conta_in=[
            "Lucro/Prejuízo Consolidado do Período",
            "Lucro ou Prejuízo Líquido Consolidado do Período",
        ],
    ).rename(columns={"VL_CONTA": "lucro_liquido"})
    lpa = _pick_value(df_dre, cd_conta="3.99.01.01").rename(columns={"VL_CONTA": "lpa"})

    ativo_total = _pick_value(df_bpa, cd_conta="1").rename(columns={"VL_CONTA": "ativo_total"})
    ativo_circ = _pick_value(df_bpa, cd_conta="1.01").rename(columns={"VL_CONTA": "ativo_circulante"})
    # OBS: se o objetivo era caixa, provavelmente NÃO é 1.01; mantendo como no seu código original
    caixa = _pick_value(df_bpa, cd_conta="1.01").rename(columns={"VL_CONTA": "caixa_e_equivalentes"})

    passivo_circ = _pick_value(df_bpp, cd_conta="2.01").rename(columns={"VL_CONTA": "passivo_circulante"})
    passivo_total = _pick_value(df_bpp, cd_conta="2").rename(columns={"VL_CONTA": "passivo_total"})
    pl = _pick_value(df_bpp, cd_conta="2.02").rename(columns={"VL_CONTA": "patrimonio_liquido"})

    dividendos = _pick_value(df_dfc, cd_conta="6.01").rename(columns={"VL_CONTA": "dividendos"})

    dfs = [
        receita, ebit, lucro, lpa,
        ativo_total, ativo_circ, caixa,
        passivo_circ, passivo_total, pl,
        dividendos
    ]

    base = None
    for d in dfs:
        if d.empty:
            continue
        base = d.copy() if base is None else base.merge(d, on=["CD_CVM", "DT_REFER"], how="outer")

    if base is None or base.empty:
        return pd.DataFrame()

    # Dedup defensivo
    base = _dedupe_columns(base)

    # DT_REFER pode ficar estranho se vier duplicado; garantimos Series
    dt_ref = _col_as_series(base, "DT_REFER")
    base["data"] = pd.to_datetime(dt_ref, errors="coerce").dt.date
    base = base.drop(columns=["DT_REFER"])

    base["divida_total"] = base.get("passivo_total")

    # >>> FIX CRÍTICO: garantir numérico ANTES da subtração
    if "divida_total" in base.columns:
        base["divida_total"] = pd.to_numeric(base["divida_total"], errors="coerce")
    if "caixa_e_equivalentes" in base.columns:
        base["caixa_e_equivalentes"] = pd.to_numeric(base["caixa_e_equivalentes"], errors="coerce")

    if "divida_total" in base.columns and "caixa_e_equivalentes" in base.columns:
        base["divida_liquida"] = base["divida_total"] - base["caixa_e_equivalentes"]
    else:
        base["divida_liquida"] = None

    return base


def _load_cvm_to_ticker(map_path: Path) -> pd.DataFrame:
    df = pd.read_csv(map_path, sep=",", encoding="utf-8")
    df = _dedupe_columns(df)

    cols = {str(c).lower(): c for c in df.columns}
    if "cd_cvm" not in cols:
        raise ValueError("cvm_to_ticker.csv precisa ter coluna CD_CVM.")
    if "ticker" not in cols:
        raise ValueError("cvm_to_ticker.csv precisa ter coluna Ticker (ou ticker).")

    df = df.rename(columns={cols["cd_cvm"]: "CD_CVM", cols["ticker"]: "ticker"})
    df = _dedupe_columns(df)

    df["CD_CVM"] = pd.to_numeric(_col_as_series(df, "CD_CVM"), errors="coerce").astype("Int64")
    df["ticker"] = _col_as_series(df, "ticker").astype(str).str.strip().str.upper()

    return df.dropna(subset=["CD_CVM", "ticker"])[["CD_CVM", "ticker"]].drop_duplicates()


def _upsert(engine: Engine, df: pd.DataFrame, batch_size: int = 5000) -> None:
    if df.empty:
        return

    cols = [
        "ticker", "data",
        "receita_liquida", "ebit", "lucro_liquido", "lpa",
        "ativo_total", "ativo_circulante",
        "passivo_circulante", "passivo_total", "patrimonio_liquido",
        "dividendos", "caixa_e_equivalentes",
        "divida_total", "divida_liquida",
    ]
    df2 = df.copy()[cols]

    sql = """
    insert into cvm.demonstracoes_financeiras_dfp (
        ticker, data,
        receita_liquida, ebit, lucro_liquido, lpa,
        ativo_total, ativo_circulante,
        passivo_circulante, passivo_total, patrimonio_liquido,
        dividendos, caixa_e_equivalentes,
        divida_total, divida_liquida
    ) values (
        :ticker, :data,
        :receita_liquida, :ebit, :lucro_liquido, :lpa,
        :ativo_total, :ativo_circulante,
        :passivo_circulante, :passivo_total, :patrimonio_liquido,
        :dividendos, :caixa_e_equivalentes,
        :divida_total, :divida_liquida
    )
    on conflict (ticker, data) do update set
        receita_liquida = excluded.receita_liquida,
        ebit = excluded.ebit,
        lucro_liquido = excluded.lucro_liquido,
        lpa = excluded.lpa,
        ativo_total = excluded.ativo_total,
        ativo_circulante = excluded.ativo_circulante,
        passivo_circulante = excluded.passivo_circulante,
        passivo_total = excluded.passivo_total,
        patrimonio_liquido = excluded.patrimonio_liquido,
        dividendos = excluded.dividendos,
        caixa_e_equivalentes = excluded.caixa_e_equivalentes,
        divida_total = excluded.divida_total,
        divida_liquida = excluded.divida_liquida;
    """

    rows = df2.to_dict(orient="records")
    with engine.begin() as conn:
        for i in range(0, len(rows), batch_size):
            conn.execute(text(sql), rows[i:i + batch_size])


def _resolve_year_to_process(engine: Engine, cfg: DfpConfig) -> Optional[int]:
    ensure_sync_table(engine)
    set_state(engine, STATE_TARGET_START, str(cfg.start_year))
    set_state(engine, STATE_TARGET_END, str(cfg.end_year))

    st_next = get_state(engine, STATE_NEXT_YEAR)
    if not st_next or st_next.get("value") in (None, ""):
        year = cfg.end_year
    else:
        try:
            year = int(float(st_next["value"]))
        except Exception:
            year = cfg.end_year

    if year < cfg.start_year:
        return None
    return year


def run(
    engine: Engine,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
    start_year: int = DEFAULT_START_YEAR,
    end_year: int = DEFAULT_END_YEAR,
    years_per_run: int = 1,
    timeout_sec: int = 120,
) -> None:
    cfg = DfpConfig(
        start_year=start_year,
        end_year=end_year,
        years_per_run=years_per_run,
        timeout_sec=timeout_sec
    )

    _ensure_table(engine)
    map_path = _find_ticker_map()
    df_map = _load_cvm_to_ticker(map_path)

    for _ in range(cfg.years_per_run):
        year = _resolve_year_to_process(engine, cfg)
        if year is None:
            if progress_cb:
                progress_cb("DFP já está completo dentro do range definido.")
            return

        if progress_cb:
            progress_cb(f"DFP: processando ano {year}...")

        zip_bytes = _download_year_zip(year, timeout_sec=cfg.timeout_sec)
        d = _read_consolidated_csvs(zip_bytes)

        df_year = _build_year_frame(d["DRE"], d["BPA"], d["BPP"], d["DFC"])
        if df_year.empty:
            set_state(engine, STATE_LAST_DONE, str(year))
            set_state(engine, STATE_NEXT_YEAR, str(year - 1))
            if progress_cb:
                progress_cb(f"DFP: ano {year} sem dados úteis (marcado como concluído).")
            continue

        # Garantias defensivas
        df_year = _dedupe_columns(df_year)

        df_year["CD_CVM"] = pd.to_numeric(_col_as_series(df_year, "CD_CVM"), errors="coerce").astype("Int64")
        df_year = df_year.merge(df_map, on="CD_CVM", how="inner")

        num_cols = [c for c in df_year.columns if c not in ("CD_CVM", "ticker", "data")]
        for c in num_cols:
            df_year[c] = pd.to_numeric(df_year[c], errors="coerce")

        _upsert(engine, df_year)

        set_state(engine, STATE_LAST_DONE, str(year))
        set_state(engine, STATE_NEXT_YEAR, str(year - 1))

        if progress_cb:
            progress_cb(f"DFP: ano {year} concluído. Próximo: {year - 1}")
