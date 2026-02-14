# portfolio_patches.py  (PATCH 3 REMOVIDO + RENUMERAÇÃO + Patch3 só com gráfico)
from __future__ import annotations

from typing import Any, Dict, List, Optional
import textwrap
import hashlib
import json
import time

import numpy as np
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

import yfinance as yf


# ─────────────────────────────────────────────────────────────
# Helpers internos
# ─────────────────────────────────────────────────────────────

def _norm_tk(t: str) -> str:
    return (t or "").upper().replace(".SA", "").strip()


def _strip_sa(ticker: str) -> str:
    return (ticker or "").strip().upper().replace(".SA", "").strip()


def _get_nome(ticker: str, empresas_lideres_finais: List[Dict]) -> str:
    tk = _norm_tk(ticker)
    return next((e.get("nome", tk) for e in (empresas_lideres_finais or []) if _norm_tk(e.get("ticker", "")) == tk), tk)


def _safe_df(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    return df if isinstance(df, pd.DataFrame) else pd.DataFrame()


def _short_label(s: str, max_len: int = 22) -> str:
    s = (s or "").strip()
    if not s:
        return "OUTROS"
    s = s.replace("  ", " ")
    return "\n".join(textwrap.wrap(s, width=max_len)) if len(s) > max_len else s


def _ensure_prices_df(precos: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Normaliza índice datetime, ordena, remove colunas vazias."""
    if precos is None or not isinstance(precos, pd.DataFrame) or precos.empty:
        return pd.DataFrame()

    df = precos.copy()
    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df[~df.index.isna()].sort_index()
    df.columns = [_strip_sa(str(c)) for c in df.columns.astype(str).tolist()]
    df = df.dropna(how="all", axis=0)
    df = df.dropna(how="all", axis=1)
    return df


def _retorno_preco_no_ano(precos: pd.DataFrame, tickers: List[str], ano: int) -> pd.Series:
    """
    Retorno simples de preço (sem dividendos) no ano calendário.
    Não baixa nada: usa apenas 'precos' já carregado na execução.
    """
    df = _ensure_prices_df(precos)
    if df.empty:
        return pd.Series(dtype=float)

    tks = [_strip_sa(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys(tks))
    if not tks:
        return pd.Series(dtype=float)

    cols = [t for t in tks if t in df.columns]
    if not cols:
        return pd.Series(dtype=float)

    df = df[cols].resample("B").last().ffill()

    ini = pd.Timestamp(f"{ano}-01-01")
    fim = pd.Timestamp(f"{ano}-12-31")
    df = df.loc[(df.index >= ini) & (df.index <= fim)]
    if df.empty or df.shape[0] < 2:
        return pd.Series(dtype=float)

    first = pd.to_numeric(df.iloc[0], errors="coerce")
    last = pd.to_numeric(df.iloc[-1], errors="coerce")
    mask = (first > 0) & np.isfinite(first) & np.isfinite(last)

    ret = (last[mask] / (first[mask] + 1e-12)) - 1.0
    ret = pd.to_numeric(ret, errors="coerce").dropna()
    ret.index = [_strip_sa(c) for c in ret.index.astype(str).tolist()]
    return ret


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if pd.isna(v):
            return None
        return v
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────
# PATCH 1 — Régua de Convicção (sem preço)
# ─────────────────────────────────────────────────────────────

def render_patch1_regua_conviccao(
    score_global: pd.DataFrame,
    lideres_global: pd.DataFrame,
    empresas_lideres_finais: List[Dict],
) -> None:
    st.markdown("## 🧭 Patch 1 — Régua de Convicção Fundamental")
    st.caption(
        "Mede quão forte foi a seleção no último ano do score. "
        "Quanto maior o score normalizado e maior o gap para o 2º colocado, maior a convicção da tese."
    )

    sg = _safe_df(score_global).copy()
    lg = _safe_df(lideres_global).copy()

    if sg.empty or "Score_Ajustado" not in sg.columns or not empresas_lideres_finais:
        st.info("Régua de convicção indisponível para esta execução.")
        return

    sg["ticker"] = sg["ticker"].astype(str).apply(_norm_tk)
    sg["Ano"] = pd.to_numeric(sg["Ano"], errors="coerce")
    sg = sg.dropna(subset=["Ano", "ticker", "Score_Ajustado"])
    if sg.empty:
        st.info("Régua de convicção indisponível: score vazio após normalização.")
        return

    ultimo_ano = int(sg["Ano"].max())
    df_ano = sg[sg["Ano"] == ultimo_ano].copy()
    if df_ano.empty:
        st.info("Não há dados de score para o último ano.")
        return

    tickers_finais = {_norm_tk(e.get("ticker", "")) for e in (empresas_lideres_finais or [])}
    df_ano = df_ano[df_ano["ticker"].isin(tickers_finais)].copy()
    if df_ano.empty:
        st.info("Nenhum ticker selecionado encontrado no score do último ano.")
        return

    df_ano = df_ano.sort_values("Score_Ajustado", ascending=False).reset_index(drop=True)
    df_ano["rank"] = df_ano.index + 1

    smin = float(df_ano["Score_Ajustado"].min())
    smax = float(df_ano["Score_Ajustado"].max())
    df_ano["score_norm"] = ((df_ano["Score_Ajustado"] - smin) / ((smax - smin) + 1e-9)) * 100.0

    if not lg.empty and {"Ano", "ticker"}.issubset(lg.columns):
        lg = lg.copy()
        lg["ticker"] = lg["ticker"].astype(str).apply(_norm_tk)
        lg["Ano"] = pd.to_numeric(lg["Ano"], errors="coerce")
        lg = lg.dropna(subset=["Ano", "ticker"])
    else:
        lg = pd.DataFrame()

    for _, row in df_ano.iterrows():
        tk = _norm_tk(str(row["ticker"]))
        nome = _get_nome(tk, empresas_lideres_finais)

        with st.expander(
            f"{nome} ({tk}) — Rank #{int(row['rank'])} | Score {float(row['score_norm']):.1f}/100",
            expanded=False,
        ):
            st.progress(min(max(float(row["score_norm"]) / 100.0, 0.0), 1.0))

            if len(df_ano) > 1 and int(row["rank"]) == 1:
                segundo = float(df_ano.iloc[1]["Score_Ajustado"])
                gap = float(row["Score_Ajustado"]) - segundo
                st.markdown(f"• Vantagem sobre o 2º colocado (no universo selecionado): **{gap:.4f} pontos**")

            if not lg.empty:
                anos_lider = (
                    lg.loc[lg["ticker"] == tk, "Ano"]
                    .dropna().astype(int).unique().tolist()
                )
                anos_lider = sorted(anos_lider)
                if anos_lider:
                    st.markdown(
                        f"• Anos como líder (histórico): **{len(anos_lider)}** ({', '.join(map(str, anos_lider))})"
                    )
                else:
                    st.markdown("• Sem histórico de liderança (líder emergente ou não recorrente).")

            st.caption("Leitura: score alto e liderança recorrente reforçam convicção (robustez de tese).")


# ─────────────────────────────────────────────────────────────
# PATCH 2 — Dominância (sem preço)
# ─────────────────────────────────────────────────────────────

def render_patch2_dominancia(
    score_global: pd.DataFrame,
    lideres_global: pd.DataFrame,
    empresas_lideres_finais: List[Dict],
) -> None:
    st.markdown("## 🏆 Patch 2 — Mapa de Dominância no Segmento")
    st.caption(
        "Avalia se a liderança é estrutural ou pontual. "
        "Alta frequência de liderança no histórico sugere tese mais durável; baixa frequência sugere ciclo/oportunidade específica."
    )

    sg = _safe_df(score_global).copy()
    lg = _safe_df(lideres_global).copy()

    if sg.empty or "Score_Ajustado" not in sg.columns or not empresas_lideres_finais:
        st.info("Mapa de dominância indisponível para esta execução.")
        return

    sg["ticker"] = sg["ticker"].astype(str).apply(_norm_tk)
    sg["Ano"] = pd.to_numeric(sg["Ano"], errors="coerce")
    sg = sg.dropna(subset=["Ano", "ticker", "Score_Ajustado"])
    if sg.empty:
        st.info("Mapa de dominância indisponível: score vazio após normalização.")
        return

    tickers_finais = {_norm_tk(e.get("ticker", "")) for e in (empresas_lideres_finais or [])}
    df = sg[sg["ticker"].isin(tickers_finais)].copy()
    if df.empty:
        st.info("Não há histórico suficiente para os ativos selecionados.")
        return

    df = df.sort_values(["ticker", "Ano"])

    resumo = (
        df.groupby("ticker")
        .agg(
            anos_no_ranking=("Ano", "nunique"),
            score_medio=("Score_Ajustado", "mean"),
            ultimo_ano=("Ano", "max"),
            score_ultimo=("Score_Ajustado", "last"),
        )
        .reset_index()
    )

    if not lg.empty and {"Ano", "ticker"}.issubset(lg.columns):
        lg = lg.copy()
        lg["ticker"] = lg["ticker"].astype(str).apply(_norm_tk)
        lg["Ano"] = pd.to_numeric(lg["Ano"], errors="coerce")
        lg = lg.dropna(subset=["Ano", "ticker"])

        lider_counts = (
            lg[lg["ticker"].isin(tickers_finais)]
            .groupby("ticker")["Ano"]
            .nunique()
            .rename("anos_lider")
            .reset_index()
        )
        resumo = resumo.merge(lider_counts, on="ticker", how="left")
    else:
        resumo["anos_lider"] = 0

    resumo["anos_lider"] = resumo["anos_lider"].fillna(0).astype(int)
    resumo["frequencia_lideranca"] = (resumo["anos_lider"] / resumo["anos_no_ranking"]).fillna(0.0)

    def classificar(row) -> str:
        if int(row["anos_lider"]) >= 4 and float(row["frequencia_lideranca"]) >= 0.5:
            return "Líder estrutural"
        if int(row["anos_lider"]) >= 2:
            return "Líder recorrente"
        if int(row["anos_lider"]) == 1:
            return "Líder emergente"
        return "Oportunidade pontual"

    resumo["classificacao"] = resumo.apply(classificar, axis=1)
    resumo["empresa"] = resumo["ticker"].apply(lambda t: _get_nome(t, empresas_lideres_finais))

    resumo = resumo.sort_values(["anos_lider", "score_medio"], ascending=[False, False]).reset_index(drop=True)

    st.dataframe(
        resumo[
            [
                "empresa",
                "ticker",
                "anos_no_ranking",
                "anos_lider",
                "frequencia_lideranca",
                "score_medio",
                "score_ultimo",
                "classificacao",
            ]
        ],
        use_container_width=True,
    )


# ─────────────────────────────────────────────────────────────
# PATCH 3 — Diversificação (APENAS GRÁFICO)  [era Patch 4]
# ─────────────────────────────────────────────────────────────

def render_patch3_diversificacao(
    empresas_lideres_finais: List[Dict],
    contrib_globais: Optional[List[Dict]] = None,
) -> None:
    st.markdown("## 📊 Patch 3 — Diversificação (gráfico por setor)")
    st.caption("Somente o gráfico de concentração por setor (versão enxuta).")

    if not empresas_lideres_finais:
        st.info("Gráfico indisponível: portfólio final vazio.")
        return

    tickers = [_norm_tk(e.get("ticker", "")) for e in (empresas_lideres_finais or []) if _norm_tk(e.get("ticker", ""))]
    tickers = list(dict.fromkeys(tickers))
    if not tickers:
        st.info("Gráfico indisponível: tickers inválidos.")
        return

    setores = [
        str(e.get("setor", e.get("SETOR", "OUTROS")))
        for e in (empresas_lideres_finais or [])
        if _norm_tk(e.get("ticker", "")) in tickers
    ]
    if not setores:
        setores = ["OUTROS"] * len(tickers)

    # pesos (se houver contrib_globais); senão equal-weight
    weights = None
    if contrib_globais:
        try:
            dfc = pd.DataFrame(contrib_globais).copy()
            if not dfc.empty and {"ticker", "valor_final"}.issubset(dfc.columns):
                dfc["ticker"] = dfc["ticker"].astype(str).apply(_norm_tk)
                agg = dfc.groupby("ticker")["valor_final"].sum()
                agg = agg[agg.index.isin(tickers)]
                if not agg.empty and float(agg.sum()) > 0:
                    weights = (agg / agg.sum()).to_dict()
        except Exception:
            weights = None

    if weights is None:
        w = 1.0 / max(1, len(tickers))
        weights = {tk: w for tk in tickers}

    df_set = pd.DataFrame(
        {
            "ticker": tickers,
            "setor": setores[: len(tickers)] if setores else ["OUTROS"] * len(tickers),
        }
    )
    df_set["peso"] = df_set["ticker"].map(lambda t: float(weights.get(_norm_tk(t), 0.0)))
    set_agg = df_set.groupby("setor")["peso"].sum().sort_values(ascending=False)

    if set_agg.empty:
        st.info("Sem dados suficientes para montar o gráfico por setor.")
        return

    set_agg = set_agg.sort_values(ascending=True)
    labels = [_short_label(str(x), max_len=22) for x in set_agg.index.astype(str).tolist()]
    values = set_agg.values.astype(float)

    h = max(3.5, 0.55 * len(labels))
    fig, ax = plt.subplots(figsize=(10, h))
    ax.barh(labels, values)
    ax.set_xlabel("Peso (0–1)")
    ax.set_ylabel("Setor")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.set_xlim(0, max(0.05, float(values.max()) * 1.15))
    fig.tight_layout()
    st.pyplot(fig)


# ─────────────────────────────────────────────────────────────
# PATCH 4 — Benchmark do Segmento (era Patch 5)
# ─────────────────────────────────────────────────────────────

def render_patch4_benchmark_segmento(
    score_global: pd.DataFrame,
    empresas_lideres_finais: List[Dict],
    precos: Optional[pd.DataFrame],
    max_universe: int = 80,
) -> None:
    st.markdown("## 📌 Patch 4 — Benchmark do Segmento (último ano do score)")
    st.caption(
        "Compara o retorno das empresas escolhidas no último ano do score com o retorno médio do "
        "segmento (SETOR > SUBSETOR > SEGMENTO). Retorno por preço (sem dividendos)."
    )

    df_prices = _ensure_prices_df(precos)
    if df_prices.empty:
        st.info("Benchmark indisponível: DataFrame de preços está vazio.")
        return

    if score_global is None or score_global.empty or not empresas_lideres_finais:
        st.info("Benchmark indisponível nesta execução (faltam dados de score ou líderes finais).")
        return

    required = {"Ano", "ticker", "SETOR", "SUBSETOR", "SEGMENTO"}
    if not required.issubset(set(score_global.columns)):
        st.info(f"Benchmark indisponível: score_global não contém colunas {sorted(required)}.")
        return

    df = score_global.copy()
    df["Ano"] = pd.to_numeric(df["Ano"], errors="coerce")
    df["ticker"] = df["ticker"].astype(str).map(_strip_sa)
    df["SETOR"] = df["SETOR"].astype(str).fillna("OUTROS")
    df["SUBSETOR"] = df["SUBSETOR"].astype(str).fillna("OUTROS")
    df["SEGMENTO"] = df["SEGMENTO"].astype(str).fillna("OUTROS")
    df = df.dropna(subset=["Ano", "ticker"])
    if df.empty:
        st.info("Benchmark indisponível: score_global vazio após normalização.")
        return

    ultimo_ano = int(df["Ano"].max())
    df_ano = df[df["Ano"] == ultimo_ano].copy()
    if df_ano.empty:
        st.info("Benchmark indisponível: não há linhas no último ano do score.")
        return

    meta = (
        df_ano[["ticker", "SETOR", "SUBSETOR", "SEGMENTO"]]
        .drop_duplicates(subset=["ticker"])
        .reset_index(drop=True)
    )
    meta_map = meta.set_index("ticker")[["SETOR", "SUBSETOR", "SEGMENTO"]].to_dict("index")

    tickers_finais = sorted({_strip_sa(str(e.get("ticker", ""))) for e in (empresas_lideres_finais or []) if str(e.get("ticker", "")).strip()})
    if not tickers_finais:
        st.info("Benchmark indisponível: tickers finais vazios.")
        return

    df_ano["id_segmento"] = df_ano["SETOR"] + " > " + df_ano["SUBSETOR"] + " > " + df_ano["SEGMENTO"]
    uni_segmento = (
        df_ano.groupby("id_segmento")["ticker"]
        .apply(lambda s: sorted(set(s.dropna().tolist())))
        .to_dict()
    )

    all_needed: List[str] = []
    for tk in tickers_finais:
        info = meta_map.get(tk)
        if not info:
            continue
        id_seg = f"{info['SETOR']} > {info['SUBSETOR']} > {info['SEGMENTO']}"
        universo = uni_segmento.get(id_seg, [])
        universo_lim = universo[:max_universe]
        all_needed.extend(universo_lim)
        all_needed.append(tk)

    all_needed = list(dict.fromkeys([_strip_sa(x) for x in all_needed if str(x).strip()]))

    ret_all = _retorno_preco_no_ano(df_prices, all_needed, ultimo_ano)

    linhas: List[Dict] = []
    faltantes_precos = []

    for tk in tickers_finais:
        info = meta_map.get(tk)
        if not info:
            continue

        id_seg = f"{info['SETOR']} > {info['SUBSETOR']} > {info['SEGMENTO']}"
        universo = uni_segmento.get(id_seg, [])
        universo_lim = [_strip_sa(x) for x in universo[:max_universe] if str(x).strip()]

        universo_disp = [u for u in universo_lim if u in df_prices.columns]
        if not universo_disp:
            seg_mean = float("nan")
        else:
            ret_uni = ret_all.reindex(universo_disp).dropna()
            seg_mean = float(ret_uni.mean()) if not ret_uni.empty else float("nan")

        tk_ret = ret_all.get(tk, float("nan"))
        if tk not in df_prices.columns:
            faltantes_precos.append(tk)

        linhas.append(
            {
                "ticker": tk,
                "empresa": next((e.get("nome") for e in (empresas_lideres_finais or []) if _strip_sa(str(e.get("ticker", ""))) == tk), tk),
                "SETOR": info["SETOR"],
                "SUBSETOR": info["SUBSETOR"],
                "SEGMENTO": info["SEGMENTO"],
                "retorno_empresa_%": float(tk_ret) * 100.0 if pd.notna(tk_ret) else float("nan"),
                "retorno_medio_segmento_%": float(seg_mean) * 100.0 if pd.notna(seg_mean) else float("nan"),
                "alpha_vs_segmento_pp": ((float(tk_ret) - float(seg_mean)) * 100.0) if (pd.notna(tk_ret) and pd.notna(seg_mean)) else float("nan"),
                "tamanho_universo_usado": len(universo_disp),
                "precos_disponiveis_para_ticker": (tk in df_prices.columns),
            }
        )

    if not linhas:
        st.info("Não foi possível montar o comparativo (metadados do segmento não encontrados para os tickers finais).")
        return

    out = pd.DataFrame(linhas).sort_values(["alpha_vs_segmento_pp", "retorno_empresa_%"], ascending=[False, False])

    st.markdown(f"**Ano analisado (último ano do score): {ultimo_ano}**")
    st.dataframe(out, use_container_width=True)

    if faltantes_precos:
        st.warning("Alguns tickers selecionados não existem no DataFrame de preços fornecido: " + ", ".join(sorted(set(faltantes_precos))))

    with st.expander("📊 Gráfico: Desempenho relativo vs média do segmento", expanded=False):
        plot_df = out.dropna(subset=["alpha_vs_segmento_pp"]).copy()
        if plot_df.empty:
            st.info("Sem dados suficientes para plotar.")
        else:
            fig, ax = plt.subplots(figsize=(10, 4))
            ax.bar(plot_df["ticker"].astype(str), plot_df["alpha_vs_segmento_pp"].astype(float))
            ax.set_ylabel("Alpha vs segmento (p.p.)")
            ax.set_xlabel("Ticker")
            ax.set_title("Retorno da empresa menos retorno médio do segmento (último ano do score)")
            ax.grid(True, linestyle="--", alpha=0.4)
            st.pyplot(fig)



# ─────────────────────────────────────────────────────────────
# PATCH 5 — Desempenho das empresas (Preço/DY via yfinance + Lucro via DB)
# ─────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False, ttl=60 * 60)
def _yf_fetch_history(ticker: str, period: str = "5y") -> pd.DataFrame:
    tk = ticker if ticker.endswith(".SA") else f"{ticker}.SA"
    try:
        df = yf.Ticker(tk).history(period=period, auto_adjust=False)
        if df is None:
            return pd.DataFrame()
        df = df.copy()
        df.index = pd.to_datetime(df.index, errors="coerce")
        df = df[~df.index.isna()].sort_index()
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(show_spinner=False, ttl=60 * 60)
def _yf_fetch_dividends(ticker: str) -> pd.Series:
    tk = ticker if ticker.endswith(".SA") else f"{ticker}.SA"
    try:
        s = yf.Ticker(tk).dividends
        if s is None:
            return pd.Series(dtype=float)
        s = pd.to_numeric(s, errors="coerce").dropna()
        s.index = pd.to_datetime(s.index, errors="coerce")
        s = s[~s.index.isna()].sort_index()
        return s
    except Exception:
        return pd.Series(dtype=float)


@st.cache_data(show_spinner=False, ttl=6 * 60 * 60)
def _db_fetch_lucro_liquido_ano(ticker: str) -> pd.Series:
    """Retorna série (ano -> Lucro_Liquido) da tabela Demonstracoes_Financeiras."""
    try:
        from core.db_loader import load_data_from_db  # type: ignore
    except Exception:
        return pd.Series(dtype=float)

    df = load_data_from_db(ticker)  # pode retornar None
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.Series(dtype=float)

    if "Data" not in df.columns or "Lucro_Liquido" not in df.columns:
        return pd.Series(dtype=float)

    dfx = df.copy()
    dfx["Data"] = pd.to_datetime(dfx["Data"], errors="coerce")
    dfx = dfx.dropna(subset=["Data"])
    if dfx.empty:
        return pd.Series(dtype=float)

    dfx["Ano"] = dfx["Data"].dt.year.astype(int)
    dfx["Lucro_Liquido"] = pd.to_numeric(dfx["Lucro_Liquido"], errors="coerce")
    dfx = dfx.dropna(subset=["Lucro_Liquido"])

    # DFP anual costuma ter 1 linha por ano; usamos o último registro do ano (maior Data)
    dfx = dfx.sort_values(["Ano", "Data"])
    s = dfx.groupby("Ano")["Lucro_Liquido"].last()
    s = pd.to_numeric(s, errors="coerce").dropna()
    return s


def _cagr(v0: float, v1: float, years: float) -> Optional[float]:
    try:
        if years <= 0:
            return None
        if v0 <= 0 or v1 <= 0:
            return None
        return (v1 / v0) ** (1.0 / years) - 1.0
    except Exception:
        return None


def _max_drawdown(prices: pd.Series) -> Optional[float]:
    try:
        s = pd.to_numeric(prices, errors="coerce").dropna()
        if s.empty:
            return None
        roll_max = s.cummax()
        dd = (s / roll_max) - 1.0
        return float(dd.min())
    except Exception:
        return None


def render_patch5_desempenho_empresas(
    empresas_lideres_finais: List[Dict],
) -> None:
    """Mostra métricas por empresa: volatilidade, DY médio 5a, crescimento de lucros, retorno e drawdown."""
    st.markdown("## 🧩 Patch 5 — Desempenho das Empresas (Preço/DY + Lucros)")
    st.caption(
        "Preço e dividendos via yfinance. Lucro Líquido via tabela Demonstracoes_Financeiras (coluna Lucro_Liquido). "
        "Métricas são aproximadas e dependem da disponibilidade de dados."
    )

    if not empresas_lideres_finais:
        st.info("Sem empresas selecionadas para analisar neste patch.")
        return

    # CSS cards no estilo do dashboard (blocos)
    st.markdown(
        """
        <style>
        .cp5-card{
            background: rgba(255,255,255,0.08);
            border: 1px solid rgba(255,255,255,0.18);
            border-radius: 18px;
            padding: 16px 16px 14px 16px;
            margin: 10px 0 14px 0;
            box-shadow: 0 10px 30px rgba(0,0,0,0.25);
        }
        .cp5-title{font-size: 18px; font-weight: 700; margin: 0 0 4px 0; color: #EAF0FF;}
        .cp5-sub{font-size: 12px; color: rgba(234,240,255,0.75); margin: 0 0 10px 0;}
        .cp5-grid{display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px 12px;}
        .cp5-kv{padding: 8px 10px; border-radius: 12px; background: rgba(0,0,0,0.18); border: 1px solid rgba(255,255,255,0.10);}
        .cp5-k{font-size: 11px; color: rgba(234,240,255,0.70); margin: 0;}
        .cp5-v{font-size: 15px; font-weight: 700; color: #FFFFFF; margin: 0;}
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Ordena por peso (se existir) para refletir relevância na carteira
    emp_sorted = sorted(empresas_lideres_finais, key=lambda e: float(e.get("peso", 0.0) or 0.0), reverse=True)

    for emp in emp_sorted:
        tk = _strip_sa(str(emp.get("ticker", "")))
        nome = str(emp.get("nome") or tk).strip()
        seg = f"{emp.get('setor','')} > {emp.get('subsetor','')} > {emp.get('segmento','')}"
        peso = _safe_float(emp.get("peso"))
        peso_txt = f"{(peso*100):.1f}%" if peso is not None else "—"

        hist = _yf_fetch_history(tk, period="5y")
        divs = _yf_fetch_dividends(tk)

        # Preço
        if not hist.empty and "Close" in hist.columns:
            close = pd.to_numeric(hist["Close"], errors="coerce").dropna()
        else:
            close = pd.Series(dtype=float)

        # Retornos diários
        if not close.empty and close.shape[0] > 20:
            rets = close.pct_change().dropna()
            vol = float(rets.std() * np.sqrt(252)) if not rets.empty else None
            mdd = _max_drawdown(close)
            # 12m
            try:
                d_12m = close.index.max() - pd.Timedelta(days=365)
                c_12m = close.loc[close.index >= d_12m]
                ret12 = float(c_12m.iloc[-1] / c_12m.iloc[0] - 1.0) if c_12m.shape[0] >= 2 else None
            except Exception:
                ret12 = None
            # CAGR 5a (usa primeiras/últimas observações do período retornado)
            try:
                years = max(1e-6, (close.index.max() - close.index.min()).days / 365.25)
                cagr5 = _cagr(float(close.iloc[0]), float(close.iloc[-1]), years)
            except Exception:
                cagr5 = None
        else:
            vol = None
            mdd = None
            ret12 = None
            cagr5 = None

        # DY médio 5a
        dy_mean = None
        try:
            if not divs.empty and not close.empty:
                div_year = divs.groupby(divs.index.year).sum()
                # preço médio anual pelo Close
                px_year = close.groupby(close.index.year).mean()
                years = sorted(set(div_year.index).intersection(set(px_year.index)))
                # pega últimos 5 anos disponíveis
                years = years[-5:]
                if years:
                    dy_year = (div_year.reindex(years) / px_year.reindex(years)).replace([np.inf, -np.inf], np.nan).dropna()
                    if not dy_year.empty:
                        dy_mean = float(dy_year.mean())
        except Exception:
            dy_mean = None

        # Lucro (CAGR 5a)
        lucro_cagr = None
        lucro_ult = None
        try:
            luc = _db_fetch_lucro_liquido_ano(tk)
            if not luc.empty:
                luc = luc.sort_index()
                luc_last5 = luc.tail(5)
                lucro_ult = float(luc_last5.iloc[-1])
                if luc_last5.shape[0] >= 2:
                    years = float(luc_last5.index.max() - luc_last5.index.min())
                    lucro_cagr = _cagr(float(luc_last5.iloc[0]), float(luc_last5.iloc[-1]), max(1.0, years))
        except Exception:
            lucro_cagr = None
            lucro_ult = None

        def fmt_pct(v: Optional[float]) -> str:
            if v is None or pd.isna(v):
                return "N/D"
            return f"{v*100:.1f}%"

        def fmt_money(v: Optional[float]) -> str:
            if v is None or pd.isna(v):
                return "N/D"
            # formato curto
            abs_v = abs(v)
            if abs_v >= 1e9:
                return f"R$ {v/1e9:.2f} bi"
            if abs_v >= 1e6:
                return f"R$ {v/1e6:.2f} mi"
            if abs_v >= 1e3:
                return f"R$ {v/1e3:.2f} mil"
            return f"R$ {v:.2f}"

        html = f"""
        <div class="cp5-card">
            <div class="cp5-title">{nome} ({tk})</div>
            <div class="cp5-sub">{seg} • Peso sugerido: <b>{peso_txt}</b></div>
            <div class="cp5-grid">
                <div class="cp5-kv"><p class="cp5-k">Volatilidade (5a, anual)</p><p class="cp5-v">{fmt_pct(vol)}</p></div>
                <div class="cp5-kv"><p class="cp5-k">Max Drawdown (5a)</p><p class="cp5-v">{fmt_pct(mdd)}</p></div>
                <div class="cp5-kv"><p class="cp5-k">Retorno 12m (preço)</p><p class="cp5-v">{fmt_pct(ret12)}</p></div>
                <div class="cp5-kv"><p class="cp5-k">CAGR (preço, janela)</p><p class="cp5-v">{fmt_pct(cagr5)}</p></div>
                <div class="cp5-kv"><p class="cp5-k">DY médio (últ. 5 anos)</p><p class="cp5-v">{fmt_pct(dy_mean)}</p></div>
                <div class="cp5-kv"><p class="cp5-k">CAGR Lucro (últ. 5 anos)</p><p class="cp5-v">{fmt_pct(lucro_cagr)}</p></div>
                <div class="cp5-kv"><p class="cp5-k">Lucro Líquido (últ. ano)</p><p class="cp5-v">{fmt_money(lucro_ult)}</p></div>
                <div class="cp5-kv"><p class="cp5-k">Fonte</p><p class="cp5-v">yfinance + Supabase</p></div>
            </div>
        </div>
        """
        st.markdown(html, unsafe_allow_html=True)

    st.caption("Notas: retornos e volatilidade são por preço (sem reinvestimento). DY é aproximado por dividendos/Preço médio anual.")

# ─────────────────────────────────────────────────────────────
# PATCH 6 — Perspectivas & Factibilidade (NLP + régua financeira)
# ─────────────────────────────────────────────────────────────

def _pick_last_numeric(df: Optional[pd.DataFrame], candidates: List[str]) -> Optional[float]:
    """Tenta achar o último valor numérico válido dentre várias colunas possíveis."""
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return None
    cols = {str(c).strip(): c for c in df.columns}
    for name in candidates:
        if name in cols:
            s = pd.to_numeric(df[cols[name]], errors="coerce").dropna()
            if not s.empty:
                return _safe_float(s.iloc[-1])
    # fallback: busca por match case-insensitive
    lower_map = {str(c).strip().lower(): c for c in df.columns}
    for name in candidates:
        k = name.strip().lower()
        if k in lower_map:
            s = pd.to_numeric(df[lower_map[k]], errors="coerce").dropna()
            if not s.empty:
                return _safe_float(s.iloc[-1])
    return None


def _clamp(x: float, lo: float, hi: float) -> float:
    try:
        return float(max(lo, min(hi, float(x))))
    except Exception:
        return float(lo)


def _score_bucket(value: Optional[float], *, good_when_low: bool, cuts: List[float], scores: List[float]) -> float:
    """
    Converte métrica em score por faixas.
    cuts: limites ordenados (ex.: [2.5, 3.5, 5.0])
    scores: pontuações correspondentes (len = len(cuts)+1)
    """
    if value is None or not np.isfinite(value):
        return 0.0
    v = float(value)
    if good_when_low:
        # menor é melhor
        for i, c in enumerate(cuts):
            if v <= c:
                return float(scores[i])
        return float(scores[-1])
    else:
        # maior é melhor
        for i, c in enumerate(cuts):
            if v >= c:
                return float(scores[i])
        return float(scores[-1])


def _patch6_regua_financeira_factibilidade(indicadores_df: Optional[pd.DataFrame]) -> Dict[str, Any]:
    """
    Régua objetiva 0–100 usando indicadores básicos.
    Funciona com diferentes nomes de colunas (robusto).
    """
    df = _safe_df(indicadores_df)

    # Nomes comuns (ajuste se você quiser “encaixar” nos seus campos reais)
    div_liq_ebitda = _pick_last_numeric(df, [
        "DIV_LIQ_EBITDA", "DIVIDA_LIQ_EBITDA", "Dívida Líquida/EBITDA", "DIVIDA_LIQUIDA_EBITDA",
        "Divida_Liq_Ebitda", "div_liq_ebitda"
    ])
    liquidez_corrente = _pick_last_numeric(df, [
        "LIQ_CORRENTE", "LIQUIDEZ_CORRENTE", "Liquidez Corrente", "liquidez_corrente"
    ])
    margem_ebitda = _pick_last_numeric(df, [
        "MARGEM_EBITDA", "Margem EBITDA", "margem_ebitda"
    ])
    margem_liquida = _pick_last_numeric(df, [
        "MARGEM_LIQ", "MARGEM_LIQUIDA", "Margem Líquida", "margem_liquida"
    ])
    roe = _pick_last_numeric(df, ["ROE", "roe"])
    roic = _pick_last_numeric(df, ["ROIC", "roic"])

    # Subscores (pesos pensados para “capacidade de executar plano”)
    # 1) Alavancagem (peso alto) — bom quando baixo
    s_alav = _score_bucket(
        div_liq_ebitda, good_when_low=True,
        cuts=[2.0, 3.0, 4.0, 5.0],
        scores=[25.0, 20.0, 14.0, 7.0, 2.0]
    )

    # 2) Liquidez (capacidade de atravessar ciclos)
    s_liq = _score_bucket(
        liquidez_corrente, good_when_low=False,
        cuts=[1.5, 1.2, 1.0],
        scores=[20.0, 15.0, 10.0, 4.0]
    )

    # 3) Rentabilidade/eficiência (proxy para “motor de caixa no futuro”)
    # margens podem variar por setor, então damos peso moderado
    s_margem = 0.0
    s_margem += _score_bucket(margem_ebitda, good_when_low=False, cuts=[25, 15, 8], scores=[12.0, 9.0, 6.0, 2.0])
    s_margem += _score_bucket(margem_liquida, good_when_low=False, cuts=[15, 8, 3], scores=[8.0, 6.0, 4.0, 1.0])

    # 4) Retorno sobre capital (qualidade estrutural)
    s_ret = 0.0
    s_ret += _score_bucket(roe, good_when_low=False, cuts=[18, 12, 8], scores=[8.0, 6.0, 4.0, 1.0])
    s_ret += _score_bucket(roic, good_when_low=False, cuts=[14, 10, 6], scores=[7.0, 5.0, 3.0, 1.0])

    raw = float(s_alav + s_liq + s_margem + s_ret)
    raw = _clamp(raw, 0.0, 100.0)

    drivers_pos = []
    drivers_neg = []

    if div_liq_ebitda is not None:
        if div_liq_ebitda <= 2.0:
            drivers_pos.append(f"Alavancagem saudável (Dívida Líq/EBITDA ≈ {div_liq_ebitda:.2f}).")
        elif div_liq_ebitda >= 4.0:
            drivers_neg.append(f"Alavancagem elevada (Dívida Líq/EBITDA ≈ {div_liq_ebitda:.2f}).")

    if liquidez_corrente is not None:
        if liquidez_corrente >= 1.5:
            drivers_pos.append(f"Liquidez corrente robusta (≈ {liquidez_corrente:.2f}).")
        elif liquidez_corrente < 1.0:
            drivers_neg.append(f"Liquidez corrente abaixo de 1 (≈ {liquidez_corrente:.2f}).")

    if margem_ebitda is not None and margem_ebitda >= 15:
        drivers_pos.append(f"Margem EBITDA consistente (≈ {margem_ebitda:.1f}%).")
    if margem_liquida is not None and margem_liquida < 3:
        drivers_neg.append(f"Margem líquida muito baixa (≈ {margem_liquida:.1f}%).")

    if roic is not None and roic >= 10:
        drivers_pos.append(f"ROIC bom (≈ {roic:.1f}%).")
    if roe is not None and roe < 8:
        drivers_neg.append(f"ROE baixo (≈ {roe:.1f}%).")

    return {
        "score_regua_0_100": raw,
        "metrics": {
            "div_liq_ebitda": div_liq_ebitda,
            "liquidez_corrente": liquidez_corrente,
            "margem_ebitda_%": margem_ebitda,
            "margem_liquida_%": margem_liquida,
            "roe_%": roe,
            "roic_%": roic,
        },
        "drivers_pos": drivers_pos[:4],
        "drivers_neg": drivers_neg[:4],
    }


def _patch6_llm_extract_and_assess(
    *,
    llm: Any,
    empresa: str,
    ticker: str,
    textos: List[Dict[str, Any]],
    regua_obj: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Usa o LLMClient.generate_json para:
    - extrair iniciativas futuras (eventos) do texto
    - classificar riscos/dependências
    - propor ajuste leve em cima da régua objetiva (sem virar “achismo”)
    """
    # schema com saída estável (sem texto solto)
    schema_hint = r"""
{
  "iniciativas": [
    {
      "tipo": "expansao|m&a|desinvestimento|capex|concessao_licitacao|pesquisa_exploracao|reestruturacao|guidance|outros",
      "descricao_curta": "STRING",
      "horizonte": "curto|medio|longo|nao_informado",
      "dependencias": ["STRING"],
      "impacto_esperado": "receita|margem|eficiencia|divida|caixa|ambivalente|nao_informado",
      "sinal": "positivo|negativo|ambivalente",
      "evidencia": {"fonte": "STRING", "data": "STRING", "trecho": "STRING"}
    }
  ],
  "avaliacao_execucao": {
    "risco_execucao": "baixo|medio|alto|nao_informado",
    "pontos_a_favor": ["STRING"],
    "pontos_contra": ["STRING"],
    "perguntas_criticas": ["STRING"],
    "ajuste_sugerido_no_score_regua_pp": 0
  },
  "resumo_1_paragrafo": "STRING"
}
""".strip()

    # guardrails: LLM não inventa fatos nem datas; só usa os textos fornecidos.
    system = """
Você é um analista buy-side, cético e orientado a evidência.
Regras obrigatórias:
- NÃO invente fatos, números, datas, nomes, operações.
- Use APENAS o conteúdo fornecido em 'textos' para extrair iniciativas e trechos de evidência.
- Se não houver evidência, retorne iniciativas vazias e explique em 'resumo_1_paragrafo'.
- Ajuste sugerido no score deve ser pequeno (entre -12 e +12 p.p.) e justificado.
- Trate a régua objetiva como base; você só pode sugerir ajuste leve.
- Saída SEMPRE no schema solicitado.
""".strip()

    # Contexto de textos (limitado para custo e para ficar estável)
    # Cada item: {source, date, text}
    ctx = []
    for t in (textos or [])[:10]:
        ctx.append({
            "source": str(t.get("source", "NA")).strip(),
            "date": str(t.get("date", "NA")).strip(),
            "text": str(t.get("text", "")).strip()[:4000],  # evita payload gigante
        })

    user = f"""
Empresa: {empresa} ({ticker})
Base objetiva de factibilidade (régua 0–100): {regua_obj.get("score_regua_0_100")}
Métricas disponíveis: {json.dumps(regua_obj.get("metrics", {}), ensure_ascii=False)}

Tarefa:
1) Extraia do material fornecido as iniciativas/planos futuros.
2) Para cada iniciativa, capture um trecho de evidência e rotule tipo/horizonte/dependências/sinal.
3) Faça uma avaliação de execução (risco baixo/médio/alto) e sugira ajuste leve no score da régua (p.p.).
""".strip()

    # Usa a interface do projeto (LLMClient)
    out = llm.generate_json(system=system, user=user, schema_hint=schema_hint, context=ctx)

    if not isinstance(out, dict):
        out = {}

    # sanitiza ajuste
    try:
        adj = float(out.get("avaliacao_execucao", {}).get("ajuste_sugerido_no_score_regua_pp", 0) or 0)
    except Exception:
        adj = 0.0
    adj = _clamp(adj, -12.0, 12.0)
    out.setdefault("avaliacao_execucao", {})
    out["avaliacao_execucao"]["ajuste_sugerido_no_score_regua_pp"] = float(adj)

    return out


def _patch6_weight_factor(score_final: float) -> float:
    """
    Sugere multiplicador de peso do aporte (não mexe no score base).
    Regras conservadoras por faixa.
    """
    s = float(score_final)
    if s >= 80:
        return 1.07
    if s >= 70:
        return 1.04
    if s >= 55:
        return 1.00
    if s >= 45:
        return 0.96
    return 0.92


def render_patch6_perspectivas_factibilidade(
    empresas_lideres_finais: List[Dict[str, Any]],
    *,
    indicadores_por_ticker: Optional[Dict[str, pd.DataFrame]] = None,
    docs_by_ticker: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    ativar_ajuste_peso: bool = True,
    cache_horas_default: int = 24,
) -> Optional[pd.DataFrame]:
    """
    PATCH 6 UI:
    - Para cada empresa final, extrai “o que pretende fazer” (via LLM em JSON)
    - Calcula factibilidade objetiva (régua financeira)
    - Ajusta score final com ajuste leve do LLM (±12 p.p.)
    - Sugere fator de peso de aporte (opcional)

    Retorna DataFrame com:
    [ticker, empresa, score_regua, ajuste_pp, score_final, fator_peso, risco_execucao, iniciativas_count]
    """
    st.markdown("## 🧩 Patch 6 — Perspectivas & Factibilidade (Plano futuro + capacidade de execução)")
    st.caption(
        "MVP do Patch6: você informa textos (CVM/RI/notícias) e a IA transforma em iniciativas estruturadas. "
        "A factibilidade parte de uma régua financeira objetiva; a IA só ajusta levemente com base em evidência do texto."
    )

    if not empresas_lideres_finais:
        st.info("Sem líderes finais nesta execução — Patch 6 não tem o que analisar.")
        return None

    # cliente LLM do projeto
    try:
        from core.ai_models.llm_client.factory import get_llm_client
        llm = get_llm_client()
    except Exception as e:
        st.error(f"Não consegui inicializar o cliente de IA (LLM). Erro: {type(e).__name__}: {e}")
        return None

    # cache
    if "patch6_cache" not in st.session_state:
        st.session_state["patch6_cache"] = {}  # run_key -> {"value": dict, "expires_at": float}
    if "patch6_textos_man" not in st.session_state:
        st.session_state["patch6_textos_man"] = {}  # ticker -> str

    def _cache_get(k: str) -> Optional[dict]:
        it = st.session_state["patch6_cache"].get(k)
        if not it:
            return None
        if time.time() > float(it.get("expires_at", 0)):
            return None
        v = it.get("value")
        return v if isinstance(v, dict) else None

    def _cache_set(k: str, v: dict, ttl_seconds: int) -> None:
        st.session_state["patch6_cache"][k] = {"value": v, "expires_at": time.time() + int(ttl_seconds)}

    with st.form("patch6_form", clear_on_submit=False):
        c1, c2, c3 = st.columns([1.0, 1.0, 1.2])
        with c1:
            ttl_h = st.number_input("Cache (horas)", 1, 168, int(cache_horas_default), 1)
        with c2:
            max_textos = st.number_input("Máx. textos por empresa (RAG)", 1, 15, 8, 1)
        with c3:
            submitted = st.form_submit_button("🧠 Executar Patch 6")

    # UI por empresa: texto manual + hint de docs_by_ticker
    st.markdown("### 📥 Fontes (cole aqui trechos oficiais)")
    st.caption("Dica: cole trechos de Fato Relevante / release de RI / call transcript / apresentação. Quanto mais oficial, melhor.")

    tickers = []
    for e in empresas_lideres_finais:
        tk = _norm_tk(e.get("ticker", ""))
        if tk:
            tickers.append(tk)
    tickers = sorted(list(dict.fromkeys(tickers)))

    for tk in tickers:
        nome = _get_nome(tk, empresas_lideres_finais)
        with st.expander(f"📌 {nome} ({tk}) — colar textos (opcional)", expanded=False):
            default_text = st.session_state["patch6_textos_man"].get(tk, "")
            txt = st.text_area(
                "Cole aqui (pode ser 1 ou vários parágrafos, separados por linha).",
                value=default_text,
                height=160,
                key=f"patch6_txt_{tk}",
                placeholder="Cole trechos de CVM/RI/notícias. Se deixar vazio, Patch6 só usa docs_by_ticker (se fornecido).",
            )
            st.session_state["patch6_textos_man"][tk] = txt

            has_auto = bool(docs_by_ticker and docs_by_ticker.get(tk))
            if has_auto:
                st.caption(f"Também existem {len(docs_by_ticker.get(tk) or [])} textos via docs_by_ticker (automático).")

    if not submitted:
        st.info("Clique em **🧠 Executar Patch 6** para gerar o relatório.")
        return None

    # execução
    resultados: List[Dict[str, Any]] = []
    ttl_seconds = int(ttl_h) * 3600

    for tk in tickers:
        nome = _get_nome(tk, empresas_lideres_finais)

        # monta textos: manual + automático
        textos: List[Dict[str, Any]] = []
        manual = (st.session_state["patch6_textos_man"].get(tk, "") or "").strip()
        if manual:
            textos.append({"source": "manual", "date": "NA", "text": manual})

        if docs_by_ticker and docs_by_ticker.get(tk):
            # espera lista de {source, date, text}
            for it in (docs_by_ticker.get(tk) or [])[: int(max_textos)]:
                if isinstance(it, dict) and str(it.get("text", "")).strip():
                    textos.append({
                        "source": str(it.get("source", "auto")).strip(),
                        "date": str(it.get("date", "NA")).strip(),
                        "text": str(it.get("text", "")).strip()
                    })

        # indicadores
        ind_df = None
        if indicadores_por_ticker and tk in indicadores_por_ticker:
            ind_df = indicadores_por_ticker.get(tk)

        regua_obj = _patch6_regua_financeira_factibilidade(ind_df)

        # cache key: depende do texto + métricas base
        payload = {
            "ticker": tk,
            "empresa": nome,
            "regua": regua_obj.get("score_regua_0_100"),
            "metrics": regua_obj.get("metrics", {}),
            "text_hash": hashlib.md5(("|".join([t.get("text", "") for t in textos])[:12000]).encode("utf-8")).hexdigest(),
            "ver": 1,
        }
        run_key = hashlib.md5(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()

        cached = _cache_get(run_key)
        if cached:
            out = cached
        else:
            # Se não houver texto algum, devolve vazio com aviso (sem inventar)
            if not textos:
                out = {
                    "iniciativas": [],
                    "avaliacao_execucao": {
                        "risco_execucao": "nao_informado",
                        "pontos_a_favor": [],
                        "pontos_contra": ["Sem textos fornecidos (CVM/RI/notícias) para extrair iniciativas futuras."],
                        "perguntas_criticas": ["Você pode colar um trecho de Fato Relevante ou release de RI?"],
                        "ajuste_sugerido_no_score_regua_pp": 0
                    },
                    "resumo_1_paragrafo": "Sem material textual suficiente para extrair perspectivas. A régua objetiva foi calculada apenas com indicadores numéricos."
                }
            else:
                out = _patch6_llm_extract_and_assess(
                    llm=llm,
                    empresa=nome,
                    ticker=tk,
                    textos=textos,
                    regua_obj=regua_obj,
                )
            _cache_set(run_key, out, ttl_seconds)

        # score final = régua + ajuste_pp
        try:
            ajuste_pp = float(out.get("avaliacao_execucao", {}).get("ajuste_sugerido_no_score_regua_pp", 0) or 0)
        except Exception:
            ajuste_pp = 0.0
        ajuste_pp = _clamp(ajuste_pp, -12.0, 12.0)

        score_regua = float(regua_obj.get("score_regua_0_100") or 0.0)
        score_final = _clamp(score_regua + ajuste_pp, 0.0, 100.0)

        risco_exec = str(out.get("avaliacao_execucao", {}).get("risco_execucao", "nao_informado")).strip()
        iniciativas = out.get("iniciativas", []) or []
        iniciativas_count = int(len(iniciativas)) if isinstance(iniciativas, list) else 0

        fator_peso = _patch6_weight_factor(score_final) if ativar_ajuste_peso else 1.0

        resultados.append({
            "ticker": tk,
            "empresa": nome,
            "score_regua_0_100": round(score_regua, 1),
            "ajuste_ia_pp": round(ajuste_pp, 1),
            "score_final_0_100": round(score_final, 1),
            "fator_peso_aporte": round(float(fator_peso), 3),
            "risco_execucao": risco_exec,
            "iniciativas_count": iniciativas_count,
            "_debug": out,
            "_drivers_pos": regua_obj.get("drivers_pos", []),
            "_drivers_neg": regua_obj.get("drivers_neg", []),
            "_metrics": regua_obj.get("metrics", {}),
        })

    df_out = pd.DataFrame(resultados)
    if df_out.empty:
        st.warning("Patch 6 não gerou resultados.")
        return None

    # exibição: tabela “profissional”
    st.markdown("### 📌 Resultado consolidado (Patch 6)")
    show_cols = ["ticker", "empresa", "score_final_0_100", "score_regua_0_100", "ajuste_ia_pp", "risco_execucao", "iniciativas_count", "fator_peso_aporte"]
    st.dataframe(df_out[show_cols].sort_values("score_final_0_100", ascending=False), use_container_width=True)

    # cards por empresa
    st.markdown("### 🗂️ Detalhe por empresa (iniciativas + evidência + leitura)")
    for _, row in df_out.sort_values("score_final_0_100", ascending=False).iterrows():
        tk = str(row["ticker"])
        nome = str(row["empresa"])
        dbg = row.get("_debug", {}) if isinstance(row.get("_debug", {}), dict) else {}
        drivers_pos = row.get("_drivers_pos", []) or []
        drivers_neg = row.get("_drivers_neg", []) or []
        metrics = row.get("_metrics", {}) if isinstance(row.get("_metrics", {}), dict) else {}

        with st.expander(f"🔎 {nome} ({tk}) — Score {row['score_final_0_100']}/100 | risco {row['risco_execucao']}", expanded=False):
            c1, c2, c3 = st.columns([1.0, 1.0, 1.0])
            with c1:
                st.metric("Score (régua)", row["score_regua_0_100"])
            with c2:
                st.metric("Ajuste IA (p.p.)", row["ajuste_ia_pp"])
            with c3:
                st.metric("Fator peso aporte", row["fator_peso_aporte"] if ativar_ajuste_peso else 1.0)

            st.caption("Régua objetiva (drivers):")
            if drivers_pos:
                st.success("A favor: " + " ".join([f"• {x}" for x in drivers_pos]))
            if drivers_neg:
                st.warning("Contra: " + " ".join([f"• {x}" for x in drivers_neg]))

            with st.expander("Ver métricas usadas (debug)", expanded=False):
                st.json(metrics)

            resumo = str(dbg.get("resumo_1_paragrafo", "")).strip()
            if resumo:
                st.write(resumo)

            iniciativas = dbg.get("iniciativas", []) or []
            if not iniciativas:
                st.info("Sem iniciativas extraídas (ou sem textos fornecidos).")
            else:
                st.markdown("**Iniciativas identificadas (com evidência):**")
                for it in iniciativas[:8]:
                    try:
                        tipo = str(it.get("tipo", "outros"))
                        desc = str(it.get("descricao_curta", "")).strip()
                        horizonte = str(it.get("horizonte", "nao_informado"))
                        impacto = str(it.get("impacto_esperado", "nao_informado"))
                        sinal = str(it.get("sinal", "ambivalente"))
                        dep = it.get("dependencias", []) or []
                        ev = it.get("evidencia", {}) or {}
                        fonte = str(ev.get("fonte", "NA"))
                        data = str(ev.get("data", "NA"))
                        trecho = str(ev.get("trecho", "")).strip()
                    except Exception:
                        continue

                    st.markdown(f"- **[{tipo}]** {desc}  \n  • Horizonte: `{horizonte}` | Impacto: `{impacto}` | Sinal: `{sinal}`")
                    if dep:
                        st.caption("Dependências: " + ", ".join([str(x) for x in dep[:6]]))
                    if trecho:
                        st.markdown(f"> {trecho[:240]}{'…' if len(trecho) > 240 else ''}")
                        st.caption(f"Fonte: {fonte} | Data: {data}")

            aval = dbg.get("avaliacao_execucao", {}) or {}
            st.markdown("**Leitura de execução:**")
            risco = str(aval.get("risco_execucao", "nao_informado"))
            st.write(f"Risco de execução: **{risco}**")

            pf = aval.get("pontos_a_favor", []) or []
            pc = aval.get("pontos_contra", []) or []
            pq = aval.get("perguntas_criticas", []) or []

            if pf:
                st.markdown("- **Pontos a favor:**")
                for x in pf[:6]:
                    st.write(f"  - {x}")
            if pc:
                st.markdown("- **Pontos contra:**")
                for x in pc[:6]:
                    st.write(f"  - {x}")
            if pq:
                st.markdown("- **Perguntas críticas (pra validar antes do aporte):**")
                for x in pq[:6]:
                    st.write(f"  - {x}")

            with st.expander("Ver JSON completo (debug)", expanded=False):
                st.json(dbg)

    return df_out
