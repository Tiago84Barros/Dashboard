# portfolio_patches.py — Patches 1..5 (Patch 5 = Desempenho/CAGR)
from __future__ import annotations

from typing import Any, Dict, List, Optional
import textwrap as _tw

import numpy as np
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt


# ─────────────────────────────────────────────────────────────
# Helpers internos
# ─────────────────────────────────────────────────────────────

def _norm_tk(t: str) -> str:
    return (t or "").upper().replace(".SA", "").strip()


def _strip_sa(ticker: str) -> str:
    return (ticker or "").strip().upper().replace(".SA", "").strip()


def _get_nome(ticker: str, empresas_lideres_finais: List[Dict]) -> str:
    tk = _norm_tk(ticker)
    return next(
        (e.get("nome", tk) for e in (empresas_lideres_finais or []) if _norm_tk(e.get("ticker", "")) == tk),
        tk,
    )


def _safe_df(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    return df if isinstance(df, pd.DataFrame) else pd.DataFrame()


def _short_label(s: str, max_len: int = 22) -> str:
    s = (s or "").strip()
    if not s:
        return "OUTROS"
    s = s.replace("  ", " ")
    return "\n".join(_tw.wrap(s, width=max_len)) if len(s) > max_len else s


def _ensure_prices_df(precos: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Normaliza índice datetime, ordena, remove colunas vazias, remove .SA dos tickers."""
    if precos is None or not isinstance(precos, pd.DataFrame) or precos.empty:
        return pd.DataFrame()

    df = precos.copy()
    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df[~df.index.isna()].sort_index()
    df.columns = [_strip_sa(str(c)) for c in df.columns.astype(str).tolist()]
    df = df.dropna(how="all", axis=0)
    df = df.dropna(how="all", axis=1)
    return df


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


def _retorno_preco_no_ano(precos: pd.DataFrame, tickers: List[str], ano: int) -> pd.Series:
    """Retorno simples de preço (sem dividendos) no ano calendário, usando apenas 'precos'."""
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


def _max_drawdown(series: pd.Series) -> Optional[float]:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.shape[0] < 2:
        return None
    roll_max = s.cummax()
    dd = (s / (roll_max + 1e-12)) - 1.0
    v = float(dd.min())
    return v if np.isfinite(v) else None


def _cagr(series: pd.Series, years: float) -> Optional[float]:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.shape[0] < 2 or years <= 0:
        return None
    first = float(s.iloc[0])
    last = float(s.iloc[-1])
    if not (np.isfinite(first) and np.isfinite(last)) or first <= 0 or last <= 0:
        return None
    return (last / first) ** (1.0 / years) - 1.0


def _ann_vol(daily_returns: pd.Series) -> Optional[float]:
    r = pd.to_numeric(daily_returns, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if r.shape[0] < 10:
        return None
    v = float(r.std() * np.sqrt(252))
    return v if np.isfinite(v) else None


def _dy_medio_anual(div_ser: pd.Series, price_ser: pd.Series, janela_anos: int) -> Optional[float]:
    """
    DY médio anual aproximado: (dividendos somados / preço médio) em cada ano,
    e média dos últimos 'janela_anos'. Usa series de dividendos e preços com índice datetime.
    """
    try:
        d = pd.to_numeric(div_ser, errors="coerce").dropna()
        p = pd.to_numeric(price_ser, errors="coerce").dropna()
        if d.empty or p.empty:
            return None

        d.index = pd.to_datetime(d.index, errors="coerce")
        p.index = pd.to_datetime(p.index, errors="coerce")
        d = d[~d.index.isna()].sort_index()
        p = p[~p.index.isna()].sort_index()

        if d.empty or p.empty:
            return None

        last_dt = min(d.index.max(), p.index.max())
        start_dt = last_dt - pd.DateOffset(years=int(janela_anos))
        d = d.loc[d.index >= start_dt]
        p = p.loc[p.index >= start_dt]
        if d.empty or p.empty:
            return None

        # somatório anual de dividendos
        d_year = d.resample("YE").sum()
        # preço médio anual
        p_year = p.resample("YE").mean()

        common = d_year.index.intersection(p_year.index)
        if common.empty:
            return None

        dy = (d_year.loc[common] / (p_year.loc[common] + 1e-12)).replace([np.inf, -np.inf], np.nan).dropna()
        if dy.empty:
            return None
        v = float(dy.mean())
        return v if np.isfinite(v) else None
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
        "Quanto maior o score normalizado e maior o gap para o 2º colocado, maior a convicção."
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
                st.markdown(f"• Vantagem sobre o 2º colocado (no universo selecionado): **{gap:.4f}**")

            if not lg.empty:
                anos_lider = lg.loc[lg["ticker"] == tk, "Ano"].dropna().astype(int).unique().tolist()
                anos_lider = sorted(anos_lider)
                if anos_lider:
                    st.markdown(f"• Anos como líder (histórico): **{len(anos_lider)}** ({', '.join(map(str, anos_lider))})")
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
        "Alta frequência de liderança sugere tese mais durável; baixa frequência sugere ciclo/oportunidade específica."
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
# PATCH 3 — Diversificação (gráfico por setor)
# ─────────────────────────────────────────────────────────────

def render_patch3_diversificacao(
    empresas_lideres_finais: List[Dict],
    contrib_globais: Optional[List[Dict]] = None,
) -> None:
    st.markdown("## 📊 Patch 3 — Diversificação (gráfico por setor)")
    st.caption("Gráfico de concentração por setor.")

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

    df_set = pd.DataFrame({"ticker": tickers, "setor": setores[: len(tickers)] if setores else ["OUTROS"] * len(tickers)})
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
# PATCH 4 — Benchmark do Segmento (último ano do score)
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

    meta = df_ano[["ticker", "SETOR", "SUBSETOR", "SEGMENTO"]].drop_duplicates(subset=["ticker"]).reset_index(drop=True)
    meta_map = meta.set_index("ticker")[["SETOR", "SUBSETOR", "SEGMENTO"]].to_dict("index")

    tickers_finais = sorted({_strip_sa(str(e.get("ticker", ""))) for e in (empresas_lideres_finais or []) if str(e.get("ticker", "")).strip()})
    if not tickers_finais:
        st.info("Benchmark indisponível: tickers finais vazios.")
        return

    df_ano["id_segmento"] = df_ano["SETOR"] + " > " + df_ano["SUBSETOR"] + " > " + df_ano["SEGMENTO"]
    uni_segmento = df_ano.groupby("id_segmento")["ticker"].apply(lambda s: sorted(set(s.dropna().tolist()))).to_dict()

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
    faltantes_precos: List[str] = []

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
        st.warning("Alguns tickers selecionados não existem no DataFrame de preços: " + ", ".join(sorted(set(faltantes_precos))))

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
# PATCH 5 — Desempenho das Empresas (CAGR / Vol / Drawdown / DY)
# ─────────────────────────────────────────────────────────────

def render_patch5_desempenho_empresas(
    empresas_lideres_finais: List[Dict[str, Any]],
    precos: pd.DataFrame,
    score_global: Optional[pd.DataFrame] = None,
    dividendos: Optional[Dict[str, Any]] = None,
    janela_anos: int = 5,
) -> None:
    st.markdown("## 📈 Patch 5 — Desempenho das Empresas (CAGR / Vol / Drawdown / DY)")
    st.caption(
        "Resumo quantitativo: retorno 12m, CAGR, volatilidade anualizada e drawdown (por preço). "
        "DY médio aparece apenas se você fornecer a série de dividendos."
    )

    if not empresas_lideres_finais:
        st.info("Sem líderes finais nesta execução.")
        return

    df_prices = _ensure_prices_df(precos)
    if df_prices.empty:
        st.info("Patch 5 indisponível: DataFrame de preços vazio.")
        return

    tickers = [_strip_sa(str(e.get("ticker", ""))) for e in (empresas_lideres_finais or []) if str(e.get("ticker", "")).strip()]
    tickers = list(dict.fromkeys([t for t in tickers if t]))
    if not tickers:
        st.info("Patch 5 indisponível: tickers inválidos.")
        return

    # prepara janelas
    px = df_prices[[t for t in tickers if t in df_prices.columns]].copy()
    if px.empty:
        st.info("Patch 5 indisponível: nenhum ticker está presente na tabela de preços.")
        return

    px = px.resample("B").last().ffill()
    last_dt = px.index.max()
    # 12m e janela_anos
    start_12m = last_dt - pd.Timedelta(days=365)
    start_ny = last_dt - pd.DateOffset(years=int(janela_anos))

    px_12 = px.loc[px.index >= start_12m].copy()
    px_ny = px.loc[px.index >= start_ny].copy()

    linhas: List[Dict[str, Any]] = []
    for tk in tickers:
        if tk not in px.columns:
            continue

        s_full = px[tk].dropna()
        s12 = px_12[tk].dropna() if not px_12.empty else pd.Series(dtype=float)
        sny = px_ny[tk].dropna() if not px_ny.empty else pd.Series(dtype=float)

        ret_12m = None
        if s12.shape[0] >= 2:
            ret_12m = float(s12.iloc[-1] / (s12.iloc[0] + 1e-12) - 1.0)

        # CAGR baseado em janela_anos (se houver dados suficientes)
        cagr = None
        if sny.shape[0] >= 2:
            years = max(0.25, (sny.index.max() - sny.index.min()).days / 365.25)
            cagr = _cagr(sny, years)

        # vol anualizada usando retornos diários da janela_anos
        vol = None
        if sny.shape[0] >= 10:
            vol = _ann_vol(sny.pct_change())

        dd = _max_drawdown(sny) if sny.shape[0] >= 2 else None

        # DY médio anual (opcional)
        dy = None
        if dividendos is not None:
            # Seleção segura do dividendos (evita ambiguidade de Series)
            div_ser = dividendos.get(tk, None)
            if div_ser is None:
                div_ser = dividendos.get(tk + ".SA", None)

            if isinstance(div_ser, pd.DataFrame):
                div_ser = div_ser.iloc[:, 0] if div_ser.shape[1] else pd.Series(dtype="float64")
            elif div_ser is None:
                div_ser = pd.Series(dtype="float64")

            dy = _dy_medio_anual(div_ser, sny, janela_anos=int(janela_anos))

        # score do último ano (opcional)
        score_last = None
        if isinstance(score_global, pd.DataFrame) and (not score_global.empty) and {"Ano", "ticker"}.issubset(score_global.columns):
            try:
                sg = score_global.copy()
                sg["Ano"] = pd.to_numeric(sg["Ano"], errors="coerce")
                sg["ticker"] = sg["ticker"].astype(str).map(_strip_sa)
                ultimo_ano = int(sg["Ano"].max())
                sl = sg.loc[(sg["Ano"] == ultimo_ano) & (sg["ticker"] == tk)]
                if not sl.empty and "Score_Ajustado" in sl.columns:
                    score_last = _safe_float(sl.iloc[0].get("Score_Ajustado"))
            except Exception:
                score_last = None

        linhas.append(
            {
                "Empresa": _get_nome(tk, empresas_lideres_finais),
                "Ticker": tk,
                "Retorno_12m_%": (ret_12m * 100.0) if ret_12m is not None else np.nan,
                f"CAGR_{int(janela_anos)}a_%": (cagr * 100.0) if cagr is not None else np.nan,
                "Vol_anual_%": (vol * 100.0) if vol is not None else np.nan,
                "Max_Drawdown_%": (dd * 100.0) if dd is not None else np.nan,
                "DY_medio_%": (dy * 100.0) if dy is not None else np.nan,
                "Score_ultimo_ano": score_last if score_last is not None else np.nan,
            }
        )

    if not linhas:
        st.info("Patch 5: não consegui computar métricas para nenhum ticker.")
        return

    out = pd.DataFrame(linhas)
    out = out.sort_values(by=[f"CAGR_{int(janela_anos)}a_%", "Retorno_12m_%"], ascending=[False, False])

    st.dataframe(out, use_container_width=True, hide_index=True)

    with st.expander("📊 Gráfico: CAGR (top 12)", expanded=False):
        top = out.dropna(subset=[f"CAGR_{int(janela_anos)}a_%"]).head(12)
        if top.empty:
            st.info("Sem CAGR suficiente para plotar.")
        else:
            fig, ax = plt.subplots(figsize=(10, 4))
            ax.bar(top["Ticker"].astype(str), top[f"CAGR_{int(janela_anos)}a_%"].astype(float))
            ax.set_ylabel(f"CAGR {int(janela_anos)} anos (%)")
            ax.set_xlabel("Ticker")
            ax.grid(True, linestyle="--", alpha=0.4)
            st.pyplot(fig)
