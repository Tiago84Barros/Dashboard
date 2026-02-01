from __future__ import annotations

from typing import Any, Dict, List, Optional
import textwrap

import numpy as np
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import hashlib
import time
import json
import pandas as pd


# ─────────────────────────────────────────────────────────────
# Helpers internos
# ─────────────────────────────────────────────────────────────

def _norm_tk(t: str) -> str:
    return (t or "").upper().replace(".SA", "").strip()


def _strip_sa(ticker: str) -> str:
    return (ticker or "").strip().upper().replace(".SA", "").strip()


def _get_nome(ticker: str, empresas_lideres_finais: List[Dict]) -> str:
    tk = _norm_tk(ticker)
    return next((e.get("nome", tk) for e in empresas_lideres_finais if _norm_tk(e.get("ticker", "")) == tk), tk)


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

    # mantém apenas colunas disponíveis
    cols = [t for t in tks if t in df.columns]
    if not cols:
        return pd.Series(dtype=float)

    # reamostra para dias úteis e forward-fill (evita buracos)
    df = df[cols].resample("B").last().ffill()

    ini = pd.Timestamp(f"{ano}-01-01")
    fim = pd.Timestamp(f"{ano}-12-31")
    df = df.loc[(df.index >= ini) & (df.index <= fim)]
    if df.empty or df.shape[0] < 2:
        return pd.Series(dtype=float)

    first = df.iloc[0]
    last = df.iloc[-1]

    # evita divisão por 0 / negativos
    first = pd.to_numeric(first, errors="coerce")
    last = pd.to_numeric(last, errors="coerce")
    mask = (first > 0) & np.isfinite(first) & np.isfinite(last)

    ret = (last[mask] / (first[mask] + 1e-12)) - 1.0
    ret = pd.to_numeric(ret, errors="coerce").dropna()
    ret.index = [_strip_sa(c) for c in ret.index.astype(str).tolist()]
    return ret


# ─────────────────────────────────────────────────────────────
# PATCH 1 — Régua de Convicção (sem preço)
# ─────────────────────────────────────────────────────────────

def render_patch1_regua_conviccao(
    score_global: pd.DataFrame,
    lideres_global: pd.DataFrame,
    empresas_lideres_finais: List[Dict],
) -> None:
    st.markdown("## 🧭 Régua de Convicção Fundamental")
    st.caption(
        "Patch 1 (Régua de Convicção): mede quão forte foi a seleção no último ano do score. "
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

    tickers_finais = {_norm_tk(e.get("ticker", "")) for e in empresas_lideres_finais}
    df_ano = df_ano[df_ano["ticker"].isin(tickers_finais)].copy()
    if df_ano.empty:
        st.info("Nenhum ticker selecionado encontrado no score do último ano.")
        return

    df_ano = df_ano.sort_values("Score_Ajustado", ascending=False).reset_index(drop=True)
    df_ano["rank"] = df_ano.index + 1

    smin = float(df_ano["Score_Ajustado"].min())
    smax = float(df_ano["Score_Ajustado"].max())
    df_ano["score_norm"] = ((df_ano["Score_Ajustado"] - smin) / ((smax - smin) + 1e-9)) * 100.0

    # Normaliza lideres_global
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
    st.markdown("## 🏆 Mapa de Dominância no Segmento")
    st.caption(
        "Patch 2 (Dominância): avalia se a liderança é estrutural ou pontual. "
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

    tickers_finais = {_norm_tk(e.get("ticker", "")) for e in empresas_lideres_finais}
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

    st.caption(
        "Interpretação: alta frequência de liderança sugere dominância estrutural; "
        "baixa frequência pode indicar caso cíclico/pontual."
    )


# ─────────────────────────────────────────────────────────────
# PATCH 3 — Stress Test (sem preço)
# ─────────────────────────────────────────────────────────────

def render_patch3_stress_test(
    score_global: pd.DataFrame,
    lideres_global: pd.DataFrame,
    empresas_lideres_finais: List[Dict],
) -> None:
    st.markdown("## 🧪 Stress Test de Robustez")
    st.caption(
        "Patch 3 (Stress Test): verifica se a seleção é robusta. "
        "Se pequenas mudanças (suavização/tempo) mudam muito os líderes, o portfólio pode estar frágil e mais sujeito a reversões."
    )

    sg = _safe_df(score_global).copy()
    lg = _safe_df(lideres_global).copy()

    if sg.empty or "Score_Ajustado" not in sg.columns or lg.empty or not empresas_lideres_finais:
        st.info("Stress test indisponível para esta execução.")
        return

    sg["ticker"] = sg["ticker"].astype(str).apply(_norm_tk)
    lg["ticker"] = lg["ticker"].astype(str).apply(_norm_tk)
    sg["Ano"] = pd.to_numeric(sg["Ano"], errors="coerce")
    lg["Ano"] = pd.to_numeric(lg["Ano"], errors="coerce")
    sg = sg.dropna(subset=["Ano", "ticker", "Score_Ajustado"])
    lg = lg.dropna(subset=["Ano", "ticker"])
    if sg.empty or lg.empty:
        st.info("Stress test indisponível: histórico vazio após normalização.")
        return

    tickers_finais = {_norm_tk(e.get("ticker", "")) for e in empresas_lideres_finais}
    anos_disp = sorted(set(sg["Ano"].dropna().astype(int).tolist()))
    if len(anos_disp) < 3:
        st.info("Stress test requer pelo menos 3 anos de histórico no score.")
        return

    ultimo_ano = int(max(anos_disp))

    lideres_por_ano = (
        lg.groupby("Ano")["ticker"]
        .apply(lambda s: set(s.dropna().astype(str).tolist()))
        .to_dict()
    )

    jaccard_rows = []
    for a1, a2 in zip(anos_disp[:-1], anos_disp[1:]):
        s1 = lideres_por_ano.get(int(a1), set())
        s2 = lideres_por_ano.get(int(a2), set())
        un = s1.union(s2)
        inter = s1.intersection(s2)
        jac = (len(inter) / (len(un) + 1e-9)) if un else 0.0
        jaccard_rows.append({"Ano_t": int(a1), "Ano_t+1": int(a2), "Jaccard_lideres": float(jac)})

    df_jaccard = pd.DataFrame(jaccard_rows)

    pres_rows = []
    for tk in sorted(tickers_finais):
        anos_lider = sorted(lg.loc[lg["ticker"] == tk, "Ano"].dropna().astype(int).unique().tolist())
        pres_rows.append({"ticker": tk, "anos_como_lider": len(anos_lider), "anos": ", ".join(map(str, anos_lider))})
    df_pres = pd.DataFrame(pres_rows)
    pct_com_hist = float((df_pres["anos_como_lider"] > 0).mean()) if not df_pres.empty else 0.0

    sg2 = sg.sort_values(["ticker", "Ano"]).copy()
    sg2["Score_Suavizado_3y"] = (
        sg2.groupby("ticker")["Score_Ajustado"]
        .transform(lambda s: s.rolling(window=3, min_periods=2).mean())
    )

    grupo_col = None
    for cand in ["SEGMENTO", "SUBSETOR", "SETOR"]:
        if cand in sg2.columns:
            grupo_col = cand
            break

    orig_last = lideres_por_ano.get(ultimo_ano, set())

    def _lideres_por_grupo(df_ano: pd.DataFrame, grupo: str, score_col: str) -> set:
        out = set()
        for _, g in df_ano.groupby(grupo):
            g2 = g.dropna(subset=[score_col])
            if g2.empty:
                continue
            out.add(str(g2.sort_values(score_col, ascending=False).iloc[0]["ticker"]))
        return out

    if grupo_col is None:
        df_last = sg2[sg2["Ano"] == ultimo_ano].dropna(subset=["Score_Suavizado_3y"])
        df_last = df_last.sort_values("Score_Suavizado_3y", ascending=False)
        suav_last = set(df_last["ticker"].head(len(orig_last)).tolist()) if not df_last.empty else set()
    else:
        df_last = sg2[sg2["Ano"] == ultimo_ano].copy()
        suav_last = _lideres_por_grupo(df_last, grupo_col, "Score_Suavizado_3y")

    un = orig_last.union(suav_last)
    inter = orig_last.intersection(suav_last)
    jacc_suav = (len(inter) / (len(un) + 1e-9)) if un else 0.0

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Estabilidade média (Jaccard)", f"{df_jaccard['Jaccard_lideres'].mean()*100:.1f}%" if not df_jaccard.empty else "—")
    with c2:
        st.metric("Tickers finais com histórico de liderança", f"{pct_com_hist*100:.1f}%")
    with c3:
        st.metric("Robustez à suavização (3y)", f"{jacc_suav*100:.1f}%")

    with st.expander("📌 Detalhes: estabilidade ano a ano (Jaccard)", expanded=False):
        st.dataframe(df_jaccard, use_container_width=True)

    with st.expander("📌 Detalhes: consistência histórica dos tickers escolhidos", expanded=False):
        df_pres_show = df_pres.copy()
        df_pres_show["empresa"] = df_pres_show["ticker"].apply(lambda t: _get_nome(t, empresas_lideres_finais))
        st.dataframe(df_pres_show[["empresa", "ticker", "anos_como_lider", "anos"]], use_container_width=True)

    with st.expander("📌 Detalhes: overlap líderes originais vs líderes suavizados (último ano)", expanded=False):
        st.write(f"Último ano do score: **{ultimo_ano}**")
        st.write(f"Coluna de grupo usada: **{grupo_col if grupo_col else 'UNIVERSO'}**")
        st.write(f"Líderes originais (n={len(orig_last)}): {', '.join(sorted(orig_last)) if orig_last else '—'}")
        st.write(f"Líderes suavizados 3y (n={len(suav_last)}): {', '.join(sorted(suav_last)) if suav_last else '—'}")


# ─────────────────────────────────────────────────────────────
# PATCH 4 — Diversificação (sem preço)
# ─────────────────────────────────────────────────────────────

def render_patch4_diversificacao(
    empresas_lideres_finais: List[Dict],
    contrib_globais: Optional[List[Dict]] = None,
) -> None:
    st.markdown("## 🧯 Diversificação e Concentração de Risco")
    st.caption(
        "Patch 4 (Diversificação): mede concentração e risco de dependência. "
        "Mesmo um portfólio vencedor pode ser frágil se estiver concentrado em poucos tickers ou em um setor dominante."
    )

    if not empresas_lideres_finais:
        st.info("Painel de diversificação indisponível: portfólio final vazio.")
        return

    tickers = [_norm_tk(e.get("ticker", "")) for e in empresas_lideres_finais if _norm_tk(e.get("ticker", ""))]
    tickers = list(dict.fromkeys(tickers))

    setores = [str(e.get("setor", "OUTROS")) for e in empresas_lideres_finais if _norm_tk(e.get("ticker", "")) in tickers]

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

    w_series = pd.Series(weights).reindex(tickers).fillna(0.0)
    w_series = w_series / (w_series.sum() + 1e-9)

    hhi = float((w_series**2).sum())
    effective_n = float(1.0 / (hhi + 1e-9))
    w_max = float(w_series.max())
    top3 = float(w_series.sort_values(ascending=False).head(3).sum())

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Concentração (HHI)", f"{hhi:.3f}")
    with c2:
        st.metric("N efetivo de apostas", f"{effective_n:.1f}")
    with c3:
        st.metric("Maior peso (ticker)", f"{w_max*100:.1f}%")

    st.caption(f"Top 3 tickers concentram: **{top3*100:.1f}%** do portfólio (estimado).")

    dfw = pd.DataFrame(
        {
            "ticker": tickers,
            "empresa": [_get_nome(tk, empresas_lideres_finais) for tk in tickers],
            "peso_%": (w_series.values * 100.0),
        }
    ).sort_values("peso_%", ascending=False)

    st.dataframe(dfw, use_container_width=True)

    df_set = pd.DataFrame({"ticker": tickers, "setor": setores[:len(tickers)] if setores else ["OUTROS"] * len(tickers)})
    df_set["peso"] = df_set["ticker"].map(lambda t: float(weights.get(_norm_tk(t), 0.0)))
    set_agg = df_set.groupby("setor")["peso"].sum().sort_values(ascending=False)

    if not set_agg.empty:
        st.markdown("### Concentração por setor")

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
# PATCH 5 — Benchmark do Segmento (AGORA sem yfinance)
# ─────────────────────────────────────────────────────────────

def render_patch5_benchmark_segmento(
    score_global: pd.DataFrame,
    empresas_lideres_finais: List[Dict],
    precos: Optional[pd.DataFrame],
    max_universe: int = 80,
) -> None:
    """
    Compara retorno no último ano do score vs retorno médio do segmento (SETOR>SUBSETOR>SEGMENTO).
    Retorno por preço (sem dividendos). Não baixa nada: usa somente 'precos' fornecido pela execução.
    """
    st.markdown("## 📌 Benchmark do Segmento (último ano do score)")
    st.caption(
        "Compara o retorno das empresas escolhidas no último ano do score com o retorno médio do "
        "segmento (SETOR > SUBSETOR > SEGMENTO) em que elas estão inseridas."
    )

    df_prices = _ensure_prices_df(precos)
    if df_prices.empty:
        st.info("Benchmark do segmento indisponível: DataFrame de preços está vazio (sem usar yfinance nos patches).")
        return

    if score_global is None or score_global.empty or not empresas_lideres_finais:
        st.info("Benchmark do segmento indisponível nesta execução (faltam dados de score ou líderes finais).")
        return

    required = {"Ano", "ticker", "SETOR", "SUBSETOR", "SEGMENTO"}
    if not required.issubset(set(score_global.columns)):
        st.info(f"Benchmark do segmento indisponível: score_global não contém colunas {sorted(required)}.")
        return

    df = score_global.copy()
    df["Ano"] = pd.to_numeric(df["Ano"], errors="coerce")
    df["ticker"] = df["ticker"].astype(str).map(_strip_sa)
    df["SETOR"] = df["SETOR"].astype(str).fillna("OUTROS")
    df["SUBSETOR"] = df["SUBSETOR"].astype(str).fillna("OUTROS")
    df["SEGMENTO"] = df["SEGMENTO"].astype(str).fillna("OUTROS")
    df = df.dropna(subset=["Ano", "ticker"])
    if df.empty:
        st.info("Benchmark do segmento indisponível: score_global vazio após normalização.")
        return

    ultimo_ano = int(df["Ano"].max())
    df_ano = df[df["Ano"] == ultimo_ano].copy()
    if df_ano.empty:
        st.info("Benchmark do segmento indisponível: não há linhas no último ano do score.")
        return

    meta = (
        df_ano[["ticker", "SETOR", "SUBSETOR", "SEGMENTO"]]
        .drop_duplicates(subset=["ticker"])
        .reset_index(drop=True)
    )
    meta_map = meta.set_index("ticker")[["SETOR", "SUBSETOR", "SEGMENTO"]].to_dict("index")

    tickers_finais = sorted({_strip_sa(str(e.get("ticker", ""))) for e in empresas_lideres_finais if str(e.get("ticker", "")).strip()})
    if not tickers_finais:
        st.info("Benchmark do segmento indisponível: tickers finais vazios.")
        return

    df_ano["id_segmento"] = df_ano["SETOR"] + " > " + df_ano["SUBSETOR"] + " > " + df_ano["SEGMENTO"]
    uni_segmento = (
        df_ano.groupby("id_segmento")["ticker"]
        .apply(lambda s: sorted(set(s.dropna().tolist())))
        .to_dict()
    )

    # Conjunto total necessário (para retorno do universo e dos tickers finais),
    # mas SEM baixar: apenas filtra colunas existentes em df_prices.
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

    # calcula retornos APENAS para colunas disponíveis
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

        # universo disponível no df_prices
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
                "empresa": next((e.get("nome") for e in empresas_lideres_finais if _strip_sa(str(e.get("ticker", ""))) == tk), tk),
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
        st.warning(
            "Alguns tickers selecionados não existem no DataFrame de preços fornecido (sem usar yfinance nos patches): "
            + ", ".join(sorted(set(faltantes_precos)))
        )

    st.caption(
        f"Nota técnica: retorno é de **preço** (sem dividendos). "
        f"O benchmark do segmento usa até **{max_universe} tickers**, limitado aos que existem no DataFrame de preços carregado."
    )

    with st.expander("📊 Gráfico: Desempenho relativo das empresas vs média do segmento", expanded=False):
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
# PATCH 6 — IA (OpenAI) — Seleção/validação amigável
# (usa core/ai_models/* e mantém resultado em sessão)
# ─────────────────────────────────────────────────────────────
def render_patch6_ia_selecao_lideres(
    score_global: pd.DataFrame,
    lideres_global: pd.DataFrame,
    empresas_lideres_finais: List[Dict[str, Any]],
    *,
    max_recs_default: int = 10,
) -> Optional[Dict[str, Any]]:
    """
    Patch 6 (IA): gera um relatório amigável de validação/seleção usando LLM (OpenAI via core.ai_models).

    Retorna:
      - dict (JSON) com recomendações/relatório quando executado,
      - None se não executar (ou se não conseguir inicializar LLM).
    """

    st.markdown("## 🤖 Validação por IA (relatório amigável)")
    st.caption(
        "A IA usa **apenas** os dados que você já calculou (score e histórico de liderança). "
        "Ela **não conhece o futuro**: se o score vai até 2024, ela não usa performance de 2025."
    )

    if not empresas_lideres_finais:
        st.info("Sem líderes finais nesta execução — Patch 6 não tem o que analisar.")
        return None

    # ── Import correto (SEMPRE via core.ai_models)
    try:
        from core.ai_models.llm_client.factory import get_llm_client  # ✅ caminho correto
    except Exception as e:
        st.error(
            "Não consegui inicializar o cliente de IA (LLM). "
            "Confirme que existe `core/ai_models/llm_client/factory.py` no deploy.\n\n"
            f"Erro: {type(e).__name__}: {e}"
        )
        return None

    # ── Chave de cache (para segurar resultado estável mesmo com reruns)
    def _norm_tk(t: str) -> str:
        return (t or "").upper().replace(".SA", "").strip()

    tickers = sorted({_norm_tk(e.get("ticker", "")) for e in empresas_lideres_finais if _norm_tk(e.get("ticker", ""))})
    base_key_payload = {
        "tickers": tickers,
        "max_recs_default": int(max_recs_default),
        "score_cols": list(score_global.columns) if isinstance(score_global, pd.DataFrame) else [],
        "lideres_cols": list(lideres_global.columns) if isinstance(lideres_global, pd.DataFrame) else [],
    }
    run_key = hashlib.md5(json.dumps(base_key_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()

    if "patch6_ia_cache" not in st.session_state:
        st.session_state["patch6_ia_cache"] = {}

    cached = st.session_state["patch6_ia_cache"].get(run_key)
    if cached:
        st.success("Mostrando resultado já gerado nesta sessão (cache).")
        _render_patch6_report(cached)
        return cached

    # ── Controles
    c1, c2, c3 = st.columns([1.0, 1.0, 1.2])
    with c1:
        max_recs = st.number_input(
            "Quantidade de recomendações",
            min_value=3,
            max_value=25,
            value=int(max_recs_default),
            step=1,
            key=f"patch6_max_recs_{run_key}",
        )
    with c2:
        mostrar_tabela = st.checkbox("Mostrar tabela", value=False, key=f"patch6_show_table_{run_key}")
    with c3:
        executar = st.button("Executar IA (Patch 6)", key=f"patch6_run_btn_{run_key}")

    if not executar:
        st.info("Clique em **Executar IA (Patch 6)** para gerar o relatório.")
        return None

    # ── Preparação dos dados (compacto e legível pro modelo)
    sg = score_global.copy() if isinstance(score_global, pd.DataFrame) else pd.DataFrame()
    lg = lideres_global.copy() if isinstance(lideres_global, pd.DataFrame) else pd.DataFrame()

    # Normalizações seguras
    if not sg.empty and "ticker" in sg.columns:
        sg["ticker"] = sg["ticker"].astype(str).map(_norm_tk)
    if not lg.empty and "ticker" in lg.columns:
        lg["ticker"] = lg["ticker"].astype(str).map(_norm_tk)

    # Descobre último ano do score
    ultimo_ano = None
    if not sg.empty and "Ano" in sg.columns:
        try:
            ultimo_ano = int(pd.to_numeric(sg["Ano"], errors="coerce").max())
        except Exception:
            ultimo_ano = None

    # Score do último ano (se existir)
    score_last = pd.DataFrame()
    if ultimo_ano is not None and not sg.empty and "Ano" in sg.columns:
        score_last = sg[pd.to_numeric(sg["Ano"], errors="coerce") == ultimo_ano].copy()

    # Monta “cartões de contexto”
    # (a IA precisa de justificativas humanas, não de dumps gigantes)
    cards: List[Dict[str, Any]] = []
    for e in empresas_lideres_finais:
        tk = _norm_tk(str(e.get("ticker", "")))
        if not tk:
            continue

        nome = str(e.get("nome", tk))
        setor = str(e.get("setor", e.get("SETOR", "OUTROS")))
        subsetor = str(e.get("subsetor", e.get("SUBSETOR", "")))
        segmento = str(e.get("segmento", e.get("SEGMENTO", "")))

        # score_ultimo
        score_ultimo = None
        if not score_last.empty and "ticker" in score_last.columns:
            rows = score_last[score_last["ticker"] == tk]
            if not rows.empty:
                # tenta pegar Score_Ajustado se existir; senão qualquer Score*
                if "Score_Ajustado" in rows.columns:
                    score_ultimo = _safe_float(rows.iloc[0].get("Score_Ajustado"))
                else:
                    # fallback: procura primeira coluna que comece com "Score"
                    sc_cols = [c for c in rows.columns if str(c).lower().startswith("score")]
                    score_ultimo = _safe_float(rows.iloc[0].get(sc_cols[0])) if sc_cols else None

        # histórico de liderança
        anos_lider: List[int] = []
        if not lg.empty and {"ticker", "Ano"}.issubset(lg.columns):
            try:
                anos = (
                    lg.loc[lg["ticker"] == tk, "Ano"]
                    .dropna()
                    .astype(int)
                    .unique()
                    .tolist()
                )
                anos_lider = sorted(anos)
            except Exception:
                anos_lider = []

        cards.append(
            {
                "ticker": tk,
                "empresa": nome,
                "setor": setor,
                "subsetor": subsetor,
                "segmento": segmento,
                "score_ultimo_ano": score_ultimo,
                "anos_lider": anos_lider,
                "qtd_anos_lider": int(len(anos_lider)),
                "ano_base_score": ultimo_ano,
            }
        )

    if not cards:
        st.warning("Não consegui montar contexto para a IA (cards vazios).")
        return None

    # ── Prompt (amigável + objetivo)
    schema_hint = """
{
  "resumo_executivo": "STRING (curto, amigável)",
  "observacao_importante": "STRING (explica que score é até o último ano e não vê o futuro)",
  "selecionadas": [
    {
      "ticker": "STRING",
      "empresa": "STRING",
      "nota_0_100": 0,
      "confianca_0_1": 0.0,
      "por_que_entra": ["STRING", "..."],
      "riscos_principais": ["STRING", "..."],
      "como_eu_usaria": "STRING (1-2 frases, bem prático)"
    }
  ],
  "nao_selecionadas": [
    {
      "ticker": "STRING",
      "empresa": "STRING",
      "por_que_ficou_fora": ["STRING", "..."],
      "o_que_precisa_melhorar_ou_confirmar": ["STRING", "..."]
    }
  ],
  "alertas_metodologicos": ["STRING", "..."]
}
""".strip()

    system = (
        "Você é um analista fundamentalista prudente. "
        "Escreva para o usuário final (linguagem simples, sem jargões). "
        "NÃO use dados futuros. Não invente fatos. "
        "Se faltarem dados, diga que faltou e como validar."
    )

    user = (
        f"Eu tenho uma carteira candidata (líderes selecionadas pelo meu score). "
        f"O score vai até o ano {ultimo_ano if ultimo_ano is not None else 'desconhecido'}. "
        f"Quero um mini-relatório amigável: quais empresas você manteria como candidatas, "
        f"quais você colocaria como 'em observação', e por quê.\n\n"
        f"Regras:\n"
        f"- Selecione no máximo {int(max_recs)} como 'selecionadas' (pode ser menos).\n"
        f"- O resto vá para 'nao_selecionadas'.\n"
        f"- Não use performance de 2025+ se o score termina antes.\n"
        f"- Baseie-se apenas no contexto fornecido.\n\n"
        f"Contexto (lista de empresas):\n{json.dumps(cards, ensure_ascii=False)}"
    )

    # ── Chama LLM
    try:
        llm = get_llm_client()  # usa secrets do Streamlit Cloud
        resp = llm.generate_json(system=system, user=user, schema_hint=schema_hint, context=None)
    except Exception as e:
        st.error(f"Falha ao chamar IA: {type(e).__name__}: {e}")
        return None

    # ── Pós-processamento defensivo + cache
    if not isinstance(resp, dict):
        st.error("A IA retornou algo inválido (não é dict).")
        return None

    # Garante campos mínimos
    resp.setdefault("resumo_executivo", "")
    resp.setdefault("observacao_importante", "")
    resp.setdefault("selecionadas", [])
    resp.setdefault("nao_selecionadas", [])
    resp.setdefault("alertas_metodologicos", [])

    st.session_state["patch6_ia_cache"][run_key] = resp

    _render_patch6_report(resp, mostrar_tabela=bool(mostrar_tabela))
    return resp


# ─────────────────────────────────────────────────────────────
# Helpers do Patch 6
# ─────────────────────────────────────────────────────────────

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


def _render_patch6_report(resp: Dict[str, Any], *, mostrar_tabela: bool = False) -> None:
    # Resumo
    resumo = str(resp.get("resumo_executivo", "")).strip()
    obs = str(resp.get("observacao_importante", "")).strip()

    if resumo:
        st.markdown("### 🧾 Resumo")
        st.write(resumo)

    if obs:
        st.markdown("### ℹ️ Observação importante")
        st.info(obs)

    # Selecionadas
    selecionadas = resp.get("selecionadas", []) or []
    st.markdown("### ✅ Candidatas mais fortes (segundo a IA)")
    if not selecionadas:
        st.warning("A IA não marcou nenhuma como forte (pode indicar falta de dados no contexto ou critérios conservadores).")
    else:
        for item in selecionadas:
            tk = str(item.get("ticker", "")).strip()
            emp = str(item.get("empresa", tk)).strip()
            nota = item.get("nota_0_100", "")
            conf = item.get("confianca_0_1", "")
            st.markdown(f"**{emp} ({tk})** — nota **{nota}** | confiança **{conf}**")

            pq = item.get("por_que_entra", []) or []
            if pq:
                st.markdown("- **Por que entra:**")
                for p in pq[:6]:
                    st.write(f"  - {p}")

            riscos = item.get("riscos_principais", []) or []
            if riscos:
                st.markdown("- **Riscos que eu monitoraria:**")
                for r in riscos[:6]:
                    st.write(f"  - {r}")

            usar = str(item.get("como_eu_usaria", "")).strip()
            if usar:
                st.markdown(f"- **Como eu usaria na prática:** {usar}")

            st.markdown("---")

    # Não selecionadas
    nao = resp.get("nao_selecionadas", []) or []
    st.markdown("### 🟡 Em observação / fora (segundo a IA)")
    if not nao:
        st.write("Nenhuma ficou de fora.")
    else:
        for item in nao:
            tk = str(item.get("ticker", "")).strip()
            emp = str(item.get("empresa", tk)).strip()
            st.markdown(f"**{emp} ({tk})**")

            pq = item.get("por_que_ficou_fora", []) or []
            if pq:
                st.markdown("- **Por que ficou fora:**")
                for p in pq[:6]:
                    st.write(f"  - {p}")

            mel = item.get("o_que_precisa_melhorar_ou_confirmar", []) or []
            if mel:
                st.markdown("- **O que confirmar antes de descartar:**")
                for m in mel[:6]:
                    st.write(f"  - {m}")

            st.markdown("---")

    alertas = resp.get("alertas_metodologicos", []) or []
    if alertas:
        st.markdown("### ⚠️ Alertas metodológicos")
        for a in alertas[:10]:
            st.write(f"- {a}")

    # Extras: tabela/JSON
    if mostrar_tabela and selecionadas:
        try:
            df = pd.DataFrame(selecionadas)
            cols = [c for c in ["ticker", "empresa", "nota_0_100", "confianca_0_1", "como_eu_usaria"] if c in df.columns]
            st.markdown("### 📋 Tabela (opcional)")
            st.dataframe(df[cols] if cols else df, use_container_width=True)
        except Exception:
            pass

    with st.expander("Ver JSON completo (debug)", expanded=False):
        st.json(resp)

# =========================
# PATCH 7 — Evidências externas + Resumo do Portfólio (UI premium)
# (versão "blindada" contra reruns: session_state + checkpoints + cache de notícias)
# =========================

def _p7_strip_sa(ticker: str) -> str:
    return (ticker or "").strip().upper().replace(".SA", "").strip()


def _p7_get_nome(ticker: str, empresas_lideres_finais: List[Dict]) -> str:
    tk = _p7_strip_sa(ticker)
    for e in (empresas_lideres_finais or []):
        if _p7_strip_sa(str(e.get("ticker", ""))) == tk:
            return str(e.get("nome") or tk)
    return tk


def _p7_bullets(xs: Any, max_items: int = 6) -> List[str]:
    if xs is None:
        return []
    if isinstance(xs, str):
        s = xs.strip()
        return [s] if s else []
    if isinstance(xs, list):
        out: List[str] = []
        for x in xs:
            s = str(x).strip()
            if s:
                out.append(s)
            if len(out) >= max_items:
                break
        return out
    return []


def _p7_make_key(*, tickers_and_names: List[List[str]], days: int, max_items: int) -> str:
    payload = {"t": tickers_and_names, "days": int(days), "max_items": int(max_items)}
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.md5(raw).hexdigest()


def _p7_get_store() -> Dict[str, Any]:
    if "patch7_last_payload" not in st.session_state:
        st.session_state["patch7_last_payload"] = {}
    return st.session_state["patch7_last_payload"]


def _p7_store_set(key: str, value: dict, ttl_seconds: int) -> None:
    store = _p7_get_store()
    store[key] = {"value": value, "expires_at": time.time() + int(ttl_seconds)}


def _p7_store_get(key: str) -> Optional[dict]:
    store = _p7_get_store()
    item = store.get(key)
    if not item:
        return None
    if time.time() > float(item.get("expires_at", 0)):
        return None
    return item.get("value")


def _p7_schema_fallback() -> str:
    """
    Fallback se SCHEMA_PATCH7 não existir no core.ai_models.prompts.schemas.
    """
    return """
    {
      "ticker": "STRING",
      "empresa": "STRING",
      "veredito": "fortalece|neutro|enfraquece",
      "resumo": "STRING",
      "catalisadores": ["..."],
      "riscos": ["..."]
    }
    """.strip()


def _p7_inject_css() -> None:
    """
    Injeta CSS apenas uma vez para deixar o Patch 7 com aparência de relatório premium.
    """
    if st.session_state.get("_patch7_css_injected"):
        return

    st.markdown(
        """
        <style>
          /* --- Patch7 typography & blocks --- */
          .p7-title {
            font-size: 28px;
            font-weight: 800;
            margin: 0 0 6px 0;
            letter-spacing: 0.2px;
            color: #6DD5FA;
          }
          .p7-subtitle {
            font-size: 14px;
            opacity: 0.85;
            margin: 0 0 14px 0;
          }
          .p7-card {
            border: 1px solid rgba(255,255,255,0.10);
            border-radius: 14px;
            padding: 14px 16px;
            background: rgba(255,255,255,0.03);
            margin: 10px 0 14px 0;
          }
          .p7-section-title {
            font-size: 16px;
            font-weight: 800;
            margin: 0 0 8px 0;
            letter-spacing: 0.2px;
          }
          .p7-text {
            font-size: 16px;
            line-height: 1.65;
            margin: 0;
          }
          .p7-bullets {
            font-size: 16px;
            line-height: 1.65;
            margin: 6px 0 0 0;
            padding-left: 18px;
          }
          .p7-badge {
            display: inline-block;
            padding: 2px 10px;
            border-radius: 999px;
            font-size: 12px;
            font-weight: 800;
            letter-spacing: 0.3px;
            margin-left: 8px;
            vertical-align: middle;
          }
          .p7-badge-strong { background: rgba(124,252,152,0.18); color: #7CFC98; border: 1px solid rgba(124,252,152,0.35); }
          .p7-badge-neutral { background: rgba(243,210,80,0.18); color: #F3D250; border: 1px solid rgba(243,210,80,0.35); }
          .p7-badge-weak { background: rgba(255,107,107,0.18); color: #FF6B6B; border: 1px solid rgba(255,107,107,0.35); }

          .p7-kpi {
            font-size: 13px;
            opacity: 0.9;
            margin-top: 8px;
          }
          .p7-muted {
            opacity: 0.78;
          }

          /* Improve expander header readability */
          div[data-testid="stExpander"] summary {
            font-weight: 700;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.session_state["_patch7_css_injected"] = True


def _p7_badge_html(veredito: str) -> str:
    v = (veredito or "").strip().lower()
    if v == "fortalece":
        return '<span class="p7-badge p7-badge-strong">FORTALECE</span>'
    if v == "enfraquece":
        return '<span class="p7-badge p7-badge-weak">ENFRAQUECE</span>'
    return '<span class="p7-badge p7-badge-neutral">NEUTRO</span>'


@st.cache_data(ttl=3600, show_spinner=False)
def _p7_fetch_news_cached(
    tickers_and_names_tuples: List[tuple],
    days: int,
    max_items: int,
) -> Dict[str, Any]:
    """
    Wrapper cacheado para reduzir reruns/re-execuções e evitar "sumir" em tarefas de rede.
    """
    from core.ai_models.pipelines.news_pipeline import build_news_for_portfolio

    return (
        build_news_for_portfolio(
            tickers_and_names=tickers_and_names_tuples,
            days=int(days),
            max_items_per_ticker=int(max_items),
        )
        or {}
    )


def render_patch7_validacao_evidencias(
    score_global: pd.DataFrame,
    lideres_global: pd.DataFrame,
    empresas_lideres_finais: List[Dict],
    *,
    days_default: int = 60,
    max_items_per_ticker_default: int = 10,
    cache_ttl_hours_default: int = 12,
) -> Optional[dict]:

    _p7_inject_css()

    # Estado para não "evaporar" em reruns
    if "patch7_run" not in st.session_state:
        st.session_state["patch7_run"] = False
    if "patch7_last_key" not in st.session_state:
        st.session_state["patch7_last_key"] = None

    st.markdown(
        """
        <div>
          <div class="p7-title">📊 Patch 7 — Evidências externas & Leitura do Portfólio</div>
          <div class="p7-subtitle p7-muted">
            Validação qualitativa baseada em notícias/fontes recentes. Se o score vai até 2024, a IA não usa performance de 2025+.
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if not empresas_lideres_finais:
        st.info("Patch 7 indisponível: não há líderes finais.")
        return None

    tickers_and_names = [
        [_p7_strip_sa(e.get("ticker", "")), _p7_get_nome(e.get("ticker", ""), empresas_lideres_finais)]
        for e in (empresas_lideres_finais or [])
        if _p7_strip_sa(e.get("ticker", ""))
    ]

    if not tickers_and_names:
        st.info("Patch 7 indisponível: tickers inválidos.")
        return None

    # UI estável: form + submit (evita múltiplos clicks e melhora rerun)
    with st.form("patch7_form", clear_on_submit=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            days = st.number_input("Janela (dias)", 7, 365, int(days_default), 7)
        with c2:
            max_items = st.number_input("Evidências por empresa", 3, 20, int(max_items_per_ticker_default), 1)
        with c3:
            ttl_h = st.number_input("Cache (horas)", 1, 72, int(cache_ttl_hours_default), 1)

        submitted = st.form_submit_button("Rodar Patch 7")

    # Se clicou, trava execução na sessão (para não perder no rerun)
    if submitted:
        st.session_state["patch7_run"] = True

    cache_key = _p7_make_key(tickers_and_names=tickers_and_names, days=int(days), max_items=int(max_items))
    st.session_state["patch7_last_key"] = cache_key

    cached = _p7_store_get(cache_key)

    # Sempre reexibe cache se existir (mesmo sem clicar)
    if cached and not st.session_state["patch7_run"]:
        st.success("Mostrando último resultado do Patch 7 (cache).")
        _render_patch7_output(cached, empresas_lideres_finais)
        return cached

    if not st.session_state["patch7_run"]:
        st.info("Clique em **Rodar Patch 7** para gerar o relatório.")
        return None

    # Checkpoints visíveis (debug rápido)
    st.write("Patch7 checkpoint: antes dos imports IA ✅")

    # Imports reais do projeto (padrão do Patch 6)
    try:
        from core.ai_models.llm_client.factory import get_llm_client
        from core.ai_models.prompts.system import SYSTEM_GUARDRAILS

        try:
            from core.ai_models.prompts.schemas import SCHEMA_PATCH7  # pode não existir
        except Exception:
            SCHEMA_PATCH7 = _p7_schema_fallback()
    except Exception as e:
        st.error(f"Patch 7 indisponível: erro ao importar módulos IA. {type(e).__name__}: {e}")
        st.session_state["patch7_run"] = False
        return None

    st.write("Patch7 checkpoint: imports IA OK ✅")

    # 1) Coleta notícias (cacheada)
    st.write("Patch7 checkpoint: vou coletar evidências ✅")
    with st.spinner("Coletando evidências recentes..."):
        try:
            news_map = _p7_fetch_news_cached(
                tickers_and_names_tuples=[(a[0], a[1]) for a in tickers_and_names],
                days=int(days),
                max_items=int(max_items),
            ) or {}
        except Exception as e:
            st.error(f"Falha ao coletar notícias: {type(e).__name__}: {e}")
            st.session_state["patch7_run"] = False
            return None

    st.write(f"Patch7 checkpoint: evidências coletadas ✅ (tickers={len(news_map)})")

    # 2) Cliente IA
    st.write("Patch7 checkpoint: vou inicializar LLM ✅")
    try:
        llm = get_llm_client()
    except Exception as e:
        st.error(f"Falha ao inicializar IA: {type(e).__name__}: {e}")
        st.session_state["patch7_run"] = False
        return None

    st.write("Patch7 checkpoint: LLM OK ✅")

    resultados: Dict[str, dict] = {}
    falhas: List[Dict[str, str]] = []

    # 3) Análise por ticker (cada um protegido) com progress bar
    progress = st.progress(0)
    total = max(1, len(tickers_and_names))

    for i, (tk, nome) in enumerate(tickers_and_names, start=1):
        progress.progress(min(i / total, 1.0))
        st.write(f"Analisando: {nome} ({tk})…")

        items = news_map.get(tk, []) or []
        ctx_items = []
        for it in items:
            # news_pipeline pode devolver dataclass ou dict; tratamos os dois
            try:
                title = getattr(it, "title", None) or (it.get("title") if isinstance(it, dict) else "")
                source = getattr(it, "source", None) or (it.get("source") if isinstance(it, dict) else "")
                link = getattr(it, "link", None) or (it.get("link") if isinstance(it, dict) else "")
                published_at = getattr(it, "published_at", None) or (it.get("published_at") if isinstance(it, dict) else None)
                snippet = getattr(it, "snippet", None) or (it.get("snippet") if isinstance(it, dict) else "")
                dt = published_at.isoformat() if hasattr(published_at, "isoformat") and published_at else ""
            except Exception:
                title, source, link, dt, snippet = "", "", "", "", ""

            ctx_items.append(
                {"title": str(title), "source": str(source), "date": str(dt), "url": str(link), "snippet": str(snippet)}
            )

        user_task = (
            f"Analise a empresa {nome} ({tk}) usando APENAS as evidências no contexto.\n\n"
            "Entregue:\n"
            "1) Resumo (4–6 linhas, linguagem simples)\n"
            "2) Catalisadores (3–5)\n"
            "3) Riscos (3–5)\n"
            "4) Veredito: 'fortalece', 'neutro' ou 'enfraquece' a tese\n\n"
            "Não invente fatos. Se as evidências forem fracas, diga isso."
        )

        try:
            out = llm.generate_json(
                system=SYSTEM_GUARDRAILS,
                user=user_task,
                schema_hint=SCHEMA_PATCH7,
                context=ctx_items,
            )
            if not isinstance(out, dict):
                out = {}
            out["ticker"] = tk
            out["empresa"] = nome
            out["evidencias"] = ctx_items
            resultados[tk] = out
        except Exception as e:
            falhas.append({"ticker": tk, "erro": f"{type(e).__name__}: {e}"})
            resultados[tk] = {
                "ticker": tk,
                "empresa": nome,
                "veredito": "indisponível",
                "resumo": "Falha ao gerar relatório (timeout/erro). Tente novamente com menos evidências por empresa.",
                "catalisadores": [],
                "riscos": [],
                "evidencias": ctx_items,
            }

    progress.progress(1.0)

    # 4) Resumo do portfólio (protegido)
    resumo_portfolio: dict = {}
    try:
        resumo_portfolio = llm.generate_json(
            system=SYSTEM_GUARDRAILS,
            user=(
                "Com base nos relatórios por empresa, gere um resumo do portfólio:\n"
                "- visao_geral (3–6 linhas)\n"
                "- destaques (3–6 bullets)\n"
                "- riscos_comuns (3–6 bullets)\n"
                "- catalisadores_comuns (3–6 bullets)\n"
                "- acoes_praticas (3–6 bullets)\n"
                "Tom: relatório curto, simples e amigável."
            ),
            schema_hint='{"visao_geral":"str","destaques":["str"],"riscos_comuns":["str"],"catalisadores_comuns":["str"],"acoes_praticas":["str"]}',
            context=list(resultados.values())[:25],
        )
        if not isinstance(resumo_portfolio, dict):
            resumo_portfolio = {}
    except Exception as e:
        resumo_portfolio = {"erro": f"{type(e).__name__}: {e}"}

    payload = {
        "resultados_por_ticker": resultados,
        "resumo_portfolio": resumo_portfolio,
        "falhas": falhas,
        "generated_at": int(time.time()),
        "params": {"days": int(days), "max_items": int(max_items)},
    }

    _p7_store_set(cache_key, payload, int(ttl_h) * 3600)
    _render_patch7_output(payload, empresas_lideres_finais)

    # Reseta o estado para não rerodar sem querer
    st.session_state["patch7_run"] = False

    return payload


def _render_patch7_output(payload: dict, empresas_lideres_finais: List[Dict]) -> None:
    _p7_inject_css()

    resultados = (payload or {}).get("resultados_por_ticker", {}) or {}
    resumo = (payload or {}).get("resumo_portfolio", {}) or {}
    falhas = (payload or {}).get("falhas", []) or []
    params = (payload or {}).get("params", {}) or {}
    days = params.get("days")
    max_items = params.get("max_items")

    # ===== Resumo do portfólio =====
    st.markdown(
        f"""
        <div class="p7-card">
          <div class="p7-section-title" style="color:#F3D250;">🧠 Contexto Estratégico</div>
          <div class="p7-kpi p7-muted">Janela: <b>{days}</b> dias • Evidências por empresa: <b>{max_items}</b></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="p7-card">
          <div class="p7-section-title" style="color:#6DD5FA;">📌 Resumo do portfólio</div>
        """,
        unsafe_allow_html=True,
    )

    if isinstance(resumo, dict) and resumo.get("erro"):
        st.warning(f"Falha ao gerar resumo do portfólio: {resumo.get('erro')}")
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        visao = str(resumo.get("visao_geral") or "—").strip()
        st.markdown(f'<p class="p7-text">{visao}</p>', unsafe_allow_html=True)

        def _render_list(title: str, key: str, color: str) -> None:
            items = _p7_bullets(resumo.get(key), max_items=8)
            if not items:
                return
            st.markdown(
                f'<div style="margin-top:12px;"><div class="p7-section-title" style="color:{color};">{title}</div></div>',
                unsafe_allow_html=True,
            )
            st.markdown('<ul class="p7-bullets">', unsafe_allow_html=True)
            for x in items:
                st.markdown(f"<li>{x}</li>", unsafe_allow_html=True)
            st.markdown("</ul>", unsafe_allow_html=True)

        _render_list("🚀 Destaques & Catalisadores", "destaques", "#7CFC98")
        _render_list("⚠️ Riscos comuns", "riscos_comuns", "#FF6B6B")
        _render_list("🧩 Catalisadores comuns", "catalisadores_comuns", "#7CFC98")
        _render_list("✅ Ações práticas", "acoes_praticas", "#6DD5FA")

        st.markdown("</div>", unsafe_allow_html=True)

    # ===== Relatório por empresa =====
    st.markdown(
        """
        <div class="p7-card">
          <div class="p7-section-title" style="color:#6DD5FA;">🧩 Relatório por empresa</div>
          <div class="p7-muted" style="font-size:13px;">Abra cada empresa para ver resumo, catalisadores e riscos.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    for e in (empresas_lideres_finais or []):
        tk = _p7_strip_sa(str(e.get("ticker", "")))
        if not tk:
            continue
        rep = resultados.get(tk, {}) or {}

        nome = _p7_get_nome(tk, empresas_lideres_finais)
        ver = str(rep.get("veredito") or "neutro").strip().lower()
        res = str(rep.get("resumo") or "—").strip()
        cats = _p7_bullets(rep.get("catalisadores"), max_items=8)
        risks = _p7_bullets(rep.get("riscos"), max_items=8)

        badge = _p7_badge_html(ver)

        exp_title = f"{nome} ({tk})"
        with st.expander(exp_title, expanded=False):
            st.markdown(
                f"""
                <div class="p7-card">
                  <div class="p7-section-title" style="color:#6DD5FA;">
                    🏭 {nome} ({tk}) {badge}
                  </div>
                  <p class="p7-text">{res}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )

            if cats:
                st.markdown(
                    """
                    <div class="p7-card">
                      <div class="p7-section-title" style="color:#7CFC98;">🚀 Catalisadores</div>
                    """,
                    unsafe_allow_html=True,
                )
                st.markdown('<ul class="p7-bullets">', unsafe_allow_html=True)
                for x in cats:
                    st.markdown(f"<li>{x}</li>", unsafe_allow_html=True)
                st.markdown("</ul></div>", unsafe_allow_html=True)

            if risks:
                st.markdown(
                    """
                    <div class="p7-card">
                      <div class="p7-section-title" style="color:#FF6B6B;">⚠️ Riscos</div>
                    """,
                    unsafe_allow_html=True,
                )
                st.markdown('<ul class="p7-bullets">', unsafe_allow_html=True)
                for x in risks:
                    st.markdown(f"<li>{x}</li>", unsafe_allow_html=True)
                st.markdown("</ul></div>", unsafe_allow_html=True)

            evs = rep.get("evidencias", []) or []
            if evs:
                with st.expander("📰 Evidências usadas (títulos/fontes)", expanded=False):
                    for it in evs[:20]:
                        title = str(it.get("title") or "").strip()
                        source = str(it.get("source") or "").strip()
                        date = str(it.get("date") or "").strip()
                        url = str(it.get("url") or "").strip()
                        snippet = str(it.get("snippet") or "").strip()

                        line = f"- **{title or 'Sem título'}**"
                        if source or date:
                            line += f" ({source}{' • ' if source and date else ''}{date})"
                        if url:
                            line += f"\n  - {url}"
                        if snippet:
                            line += f"\n  - {snippet[:240]}{'...' if len(snippet) > 240 else ''}"
                        st.markdown(line)

    if falhas:
        with st.expander("🛠️ Detalhes de falhas (debug)", expanded=False):
            st.json(falhas)
