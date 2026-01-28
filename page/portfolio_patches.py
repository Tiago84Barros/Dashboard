from __future__ import annotations

from typing import List, Dict, Optional
import textwrap

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
# PATCH 6 — IA (OpenAI) Seleção Assistida entre Líderes
# ─────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────
# PATCH 6 — IA (OpenAI) Seleção Assistida entre Líderes
# ─────────────────────────────────────────────────────────────

def _coletar_metricas_ultima_linha(df: pd.DataFrame, ticker: str) -> Dict:
    if df is None or df.empty or "ticker" not in df.columns:
        return {}

    d = df.copy()
    d["ticker"] = d["ticker"].astype(str).map(_strip_sa)
    rows = d[d["ticker"] == _strip_sa(ticker)]

    if rows.empty:
        return {}

    if "Ano" in rows.columns:
        rows["Ano"] = pd.to_numeric(rows["Ano"], errors="coerce")
        rows = rows.dropna(subset=["Ano"])
        if not rows.empty:
            rows = rows[rows["Ano"] == rows["Ano"].max()]

    r0 = rows.iloc[-1].to_dict()

    keep = [
        "Score_Ajustado", "Score",
        "P/VP", "P/L", "EV/EBITDA",
        "DY", "ROE", "ROIC",
        "Margem_Bruta", "Margem_EBITDA", "Margem_Liquida",
        "Crescimento_Receita", "Crescimento_Lucro",
        "Liquidez_Corrente", "Endividamento_Total", "Divida_Liquida_EBITDA",
        "SETOR", "SUBSETOR", "SEGMENTO", "Ano",
    ]

    out = {}
    for k in keep:
        if k in r0:
            v = r0.get(k)
            if isinstance(v, (float, int)) and pd.notna(v):
                out[k] = float(v)
            elif isinstance(v, str):
                out[k] = v[:120]

    return out


def _historico_lideranca(lideres_global: pd.DataFrame, ticker: str) -> Dict:
    if lideres_global is None or lideres_global.empty:
        return {"anos_lider": 0, "anos": []}

    lg = lideres_global.copy()
    if not {"ticker", "Ano"}.issubset(lg.columns):
        return {"anos_lider": 0, "anos": []}

    lg["ticker"] = lg["ticker"].astype(str).map(_strip_sa)
    lg["Ano"] = pd.to_numeric(lg["Ano"], errors="coerce")
    lg = lg.dropna(subset=["ticker", "Ano"])

    anos = sorted(lg.loc[lg["ticker"] == _strip_sa(ticker), "Ano"].astype(int).unique().tolist())
    return {"anos_lider": len(anos), "anos": anos}


def _montar_contexto_patch6(
    score_global: pd.DataFrame,
    lideres_global: pd.DataFrame,
    empresas_lideres_finais: List[Dict],
) -> List[Dict]:

    ctx = []

    for e in empresas_lideres_finais:
        tk = _strip_sa(str(e.get("ticker", "")))
        if not tk:
            continue

        nome = e.get("nome", tk)
        metrics = _coletar_metricas_ultima_linha(score_global, tk)
        hist = _historico_lideranca(lideres_global, tk)

        texto = (
            f"Ticker: {tk}\n"
            f"Empresa: {nome}\n"
            f"Ano líder: {e.get('ano_lider')}\n"
            f"Ano compra: {e.get('ano_compra')}\n"
            f"Histórico liderança: {hist['anos_lider']} anos {hist['anos']}\n"
            f"Métricas fundamentais (último ano): {metrics}\n"
            "Use apenas essas informações.\n"
        )

        ctx.append(
            {
                "title": f"{tk} - {nome}",
                "source": "dados internos",
                "published_at": "",
                "text": texto,
            }
        )

    return ctx


@st.cache_data(ttl=60 * 60 * 24, show_spinner=False)
def _patch6_ai_cached(context_texts: tuple, max_recs: int) -> Dict:
    from core.ai_models.llm_client.factory import get_llm_client

    llm = get_llm_client()

    system = (
        "Você é um módulo de apoio à decisão do algoritmo. "
        "Não forneça recomendação de investimento. "
        "Ranqueie os tickers com base apenas nos dados fornecidos. "
        "Responda somente em JSON válido."
    )

    user = (
        "Tarefa: selecionar os tickers com maior probabilidade de desempenho relativo positivo "
        "no próximo ano, usando consistência, qualidade e valuation.\n"
        f"Retorne no máximo {max_recs} tickers.\n"
        "Para cada um informe score_ia (0-100), confidence (0-1), "
        "rationale curto, riscos e catalisadores.\n"
        "Para os não selecionados, informe o motivo.\n"
    )

    schema_hint = (
        '{'
        '"recomendadas":[{"ticker":"XXXX","score_ia":80,"confidence":0.7,'
        '"rationale":"...","riscos":["..."],"catalisadores":["..."]}],'
        '"nao_selecionadas":[{"ticker":"YYYY","motivo":"..."}]'
        '}'
    )

    context = [{"title": "", "text": t, "source": "ctx", "published_at": ""} for t in context_texts]

    return llm.generate_json(system=system, user=user, schema_hint=schema_hint, context=context)


def render_patch6_ia_selecao_lideres(
    score_global: pd.DataFrame,
    lideres_global: pd.DataFrame,
    empresas_lideres_finais: List[Dict],
    max_recs_default: int = 10,
) -> Optional[Dict]:

    st.markdown("## 🤖 IA (OpenAI) — Seleção assistida entre líderes")
    st.caption(
        "Patch 6: a IA ranqueia as líderes finais usando apenas sinais internos "
        "(fundamentos + histórico de liderança)."
    )

    if not empresas_lideres_finais:
        st.info("Patch 6 indisponível: não há líderes finais.")
        return None

    with st.form("patch6_form"):
        use_ai = st.checkbox("Ativar IA neste patch", value=False)
        max_recs = st.slider(
            "Máximo de empresas recomendadas",
            min_value=3,
            max_value=min(20, len(empresas_lideres_finais)),
            value=min(max_recs_default, len(empresas_lideres_finais)),
            step=1,
        )
        run_ai = st.form_submit_button("Executar IA (Patch 6)")

    if not run_ai:
        st.info("Configure as opções acima e clique em **Executar IA (Patch 6)**.")
        return None

    if not use_ai:
        st.warning("Ative a opção **Ativar IA neste patch** para executar.")
        return None

    ctx = _montar_contexto_patch6(score_global, lideres_global, empresas_lideres_finais)
    if not ctx:
        st.warning("Não foi possível montar contexto interno para a IA.")
        return None

    ctx_texts = tuple(c["text"] for c in ctx)

    try:
        with st.spinner("IA analisando líderes..."):
            ai_resp = _patch6_ai_cached(ctx_texts, int(max_recs))
    except Exception as e:
        st.error("Falha ao executar IA.")
        st.exception(e)
        return None

    if not isinstance(ai_resp, dict):
        st.warning("Resposta da IA inválida.")
        st.json(ai_resp)
        return ai_resp

    recs = ai_resp.get("recomendadas", [])
    if recs:
        rows = []
        for r in recs:
            rows.append(
                {
                    "ticker": r.get("ticker"),
                    "empresa": _get_nome(r.get("ticker"), empresas_lideres_finais),
                    "score_ia": r.get("score_ia"),
                    "confidence": r.get("confidence"),
                    "rationale": r.get("rationale"),
                    "riscos": ", ".join(r.get("riscos", []) or []),
                    "catalisadores": ", ".join(r.get("catalisadores", []) or []),
                }
            )
        st.dataframe(pd.DataFrame(rows), use_container_width=True)

    nao = ai_resp.get("nao_selecionadas", [])
    if nao:
        with st.expander("Empresas não selecionadas pela IA"):
            st.json(nao)

    st.caption(
        "⚠️ Uso recomendado: sinal auxiliar. "
        "O score fundamentalista continua sendo o principal driver da decisão."
    )

    return ai_resp
