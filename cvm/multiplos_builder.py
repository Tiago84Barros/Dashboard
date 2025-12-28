from __future__ import annotations

import numpy as np
import pandas as pd


def compute_multiplos_full(
    df_dfp: pd.DataFrame,
    df_year_price: pd.DataFrame,
) -> pd.DataFrame:
    """
    Replica a filosofia do notebook:
    - DataFrame base contábil (DFP anual)
    - Merge com preço de referência (último pregão do ano)
    - Cálculo vetorizado de múltiplos e indicadores
    Retorna DF pronto para UPSERT em cvm.multiplos (por ticker, ano).
    """

    df = df_dfp.copy()

    # Normalizações mínimas
    df["ticker"] = df["ticker"].astype(str).str.upper().str.replace(".SA", "", regex=False)
    df["data"] = pd.to_datetime(df["data"], errors="coerce")
    df["ano"] = pd.to_numeric(df["ano"], errors="coerce").fillna(df["data"].dt.year).astype("int64")

    # Merge com preço anual (último pregão do ano)
    px = df_year_price.copy()
    px["ticker"] = px["ticker"].astype(str).str.upper().str.replace(".SA", "", regex=False)
    px["ano"] = pd.to_numeric(px["ano"], errors="coerce").astype("int64")

    df = df.merge(px[["ticker", "ano", "ref_date", "price_close"]], on=["ticker", "ano"], how="left")

    # Garante numéricos
    num_cols = [
        "receita_liquida", "ebit", "lucro_liquido", "lpa",
        "ativo_total", "ativo_circulante",
        "passivo_circulante", "passivo_total",
        "patrimonio_liquido",
        "dividendos",
        "caixa_e_equivalentes",
        "divida_total", "divida_liquida",
        "price_close",
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # --- Indicadores de Liquidez (notebook-like) ---
    # Liquidez Corrente = Ativo Circulante / Passivo Circulante
    df["liquidez_corrente"] = np.where(
        (df.get("passivo_circulante") > 0),
        df.get("ativo_circulante") / df.get("passivo_circulante"),
        np.nan,
    )

    # --- Estrutura de capital / Endividamento ---
    # Endividamento Total = Passivo Total / Ativo Total
    df["endividamento_total"] = np.where(
        (df.get("ativo_total") > 0),
        df.get("passivo_total") / df.get("ativo_total"),
        np.nan,
    )

    # Alavancagem Financeira (definição comum e estável) = Ativo Total / Patrimônio Líquido
    df["alavancagem_financeira"] = np.where(
        (df.get("patrimonio_liquido") != 0) & df.get("patrimonio_liquido").notna(),
        df.get("ativo_total") / df.get("patrimonio_liquido"),
        np.nan,
    )

    # --- Margens ---
    df["margem_operacional"] = np.where(
        (df.get("receita_liquida") > 0),
        df.get("ebit") / df.get("receita_liquida"),
        np.nan,
    )

    df["margem_liquida"] = np.where(
        (df.get("receita_liquida") > 0),
        df.get("lucro_liquido") / df.get("receita_liquida"),
        np.nan,
    )

    # --- Rentabilidade ---
    df["roe"] = np.where(
        (df.get("patrimonio_liquido") != 0) & df.get("patrimonio_liquido").notna(),
        df.get("lucro_liquido") / df.get("patrimonio_liquido"),
        np.nan,
    )

    df["roa"] = np.where(
        (df.get("ativo_total") > 0),
        df.get("lucro_liquido") / df.get("ativo_total"),
        np.nan,
    )

    # ROIC: como não temos NOPAT e capital investido detalhado, usamos aproximação estável:
    # ROIC ~ EBIT / (Ativo Total - Passivo Circulante)  (proxy de capital investido)
    capital_investido = df.get("ativo_total") - df.get("passivo_circulante")
    df["roic"] = np.where(
        (capital_investido > 0),
        df.get("ebit") / capital_investido,
        np.nan,
    )

    # --- Valuation (sua estrutura atual + estilo notebook) ---
    # shares_est = lucro / lpa
    df["shares_est"] = np.where(
        df.get("lpa").abs() > 1e-12,
        df.get("lucro_liquido") / df.get("lpa"),
        np.nan,
    )

    # VPA estimado = PL / shares_est
    vpa_est = np.where(
        np.abs(df["shares_est"]) > 1e-12,
        df.get("patrimonio_liquido") / df["shares_est"],
        np.nan,
    )

    # DPS estimado = dividendos / shares_est
    dps_est = np.where(
        np.abs(df["shares_est"]) > 1e-12,
        df.get("dividendos") / df["shares_est"],
        np.nan,
    )

    # P/L = preço / LPA
    df["pl"] = np.where(
        df.get("lpa").abs() > 1e-12,
        df.get("price_close") / df.get("lpa"),
        np.nan,
    )

    # P/VP = preço / VPA
    df["pvp"] = np.where(
        np.abs(vpa_est) > 1e-12,
        df.get("price_close") / vpa_est,
        np.nan,
    )

    # DY = DPS / preço
    df["dy"] = np.where(
        df.get("price_close").abs() > 1e-12,
        dps_est / df.get("price_close"),
        np.nan,
    )

    # Payout = dividendos / lucro líquido
    df["payout"] = np.where(
        df.get("lucro_liquido").abs() > 1e-12,
        df.get("dividendos") / df.get("lucro_liquido"),
        np.nan,
    )

    out_cols = [
        "ticker", "ano", "ref_date", "price_close",
        "liquidez_corrente", "endividamento_total", "alavancagem_financeira",
        "margem_operacional", "margem_liquida",
        "roe", "roa", "roic",
        "dy", "pl", "pvp", "payout",
        "shares_est",
    ]

    out = df[out_cols].copy()
    return out
