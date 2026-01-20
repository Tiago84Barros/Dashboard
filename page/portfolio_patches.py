from __future__ import annotations

from typing import List, Dict, Optional
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt


# ─────────────────────────────────────────────────────────────
# Helpers internos
# ─────────────────────────────────────────────────────────────

def _norm_tk(t: str) -> str:
    return (t or "").upper().replace(".SA", "").strip()


def _get_nome(ticker: str, empresas_lideres_finais: List[Dict]) -> str:
    tk = _norm_tk(ticker)
    return next((e.get("nome", tk) for e in empresas_lideres_finais if _norm_tk(e.get("ticker", "")) == tk), tk)


def _safe_df(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    return df if isinstance(df, pd.DataFrame) else pd.DataFrame()


# ─────────────────────────────────────────────────────────────
# PATCH 1 — Régua de Convicção
# ─────────────────────────────────────────────────────────────

def render_patch1_regua_conviccao(
    score_global: pd.DataFrame,
    lideres_global: pd.DataFrame,
    empresas_lideres_finais: List[Dict],
) -> None:
    st.markdown("## 🧭 Régua de Convicção Fundamental")
    st.caption(
        "Força relativa das empresas selecionadas no último ano disponível do score, "
        "normalizada em 0–100 com base no Score_Ajustado."
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
                    st.markdown(f"• Anos como líder (histórico): **{len(anos_lider)}** ({', '.join(map(str, anos_lider))})")
                else:
                    st.markdown("• Sem histórico de liderança (líder emergente ou não recorrente).")

            st.caption("Leitura: score alto e liderança recorrente reforçam convicção (robustez de tese).")


# ─────────────────────────────────────────────────────────────
# PATCH 2 — Dominância (frequência de liderança)
# ─────────────────────────────────────────────────────────────

def render_patch2_dominancia(
    score_global: pd.DataFrame,
    lideres_global: pd.DataFrame,
    empresas_lideres_finais: List[Dict],
) -> None:
    st.markdown("## 🏆 Mapa de Dominância no Segmento")
    st.caption(
        "Frequência de liderança mede em quantos anos a empresa foi líder "
        "dividido pelo número de anos em que ela participou do ranking (histórico do score)."
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

    colunas_exibicao = [
        "empresa",
        "ticker",
        "anos_no_ranking",
        "anos_lider",
        "frequencia_lideranca",
        "score_medio",
        "score_ultimo",
        "classificacao",
    ]
    st.dataframe(resumo[colunas_exibicao], use_container_width=True)

    st.caption(
        "Interpretação: alta frequência de liderança sugere dominância estrutural; "
        "baixa frequência pode indicar caso cíclico/pontual."
    )

# ─────────────────────────────────────────────────────────────
# PATCH 3 — Stress Test de Robustez
# ─────────────────────────────────────────────────────────────

def render_patch3_stress_test(
    score_global: pd.DataFrame,
    lideres_global: pd.DataFrame,
    empresas_lideres_finais: List[Dict],
) -> None:
    st.markdown("## 🧪 Stress Test de Robustez")
    st.caption(
        "Mede robustez da seleção por: estabilidade ao longo do tempo (Jaccard), "
        "consistência histórica de liderança e sensibilidade a suavização (rolling 3 anos)."
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

    # 1) Estabilidade anual do conjunto de líderes (Jaccard)
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

    # 2) Consistência dos tickers finais (quantos anos foi líder)
    pres_rows = []
    for tk in sorted(tickers_finais):
        anos_lider = sorted(lg.loc[lg["ticker"] == tk, "Ano"].dropna().astype(int).unique().tolist())
        pres_rows.append({"ticker": tk, "anos_como_lider": len(anos_lider), "anos": ", ".join(map(str, anos_lider))})
    df_pres = pd.DataFrame(pres_rows)
    pct_com_hist = float((df_pres["anos_como_lider"] > 0).mean()) if not df_pres.empty else 0.0

    # 3) Sensibilidade à suavização (rolling 3y)
    sg2 = sg.sort_values(["ticker", "Ano"]).copy()
    sg2["Score_Suavizado_3y"] = (
        sg2.groupby("ticker")["Score_Ajustado"]
        .transform(lambda s: s.rolling(window=3, min_periods=2).mean())
    )

    # grupo_col se existir
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

    # UI
    c1, c2, c3 = st.columns(3)
    with c1:
        if not df_jaccard.empty:
            st.metric("Estabilidade média (Jaccard)", f"{df_jaccard['Jaccard_lideres'].mean()*100:.1f}%")
        else:
            st.metric("Estabilidade média (Jaccard)", "—")
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
# PATCH 4 — Diversificação e Concentração de Risco (novo)
# ─────────────────────────────────────────────────────────────

def render_patch4_diversificacao(
    empresas_lideres_finais: List[Dict],
    contrib_globais: Optional[List[Dict]] = None,
) -> None:
    st.markdown("## 🧯 Diversificação e Concentração de Risco")
    st.caption(
        "Avalia concentração por ticker e por setor. Se não houver pesos reais disponíveis, "
        "assume pesos iguais. Se houver contribuições (Patch 5), usa pesos finais agregados."
    )

    if not empresas_lideres_finais:
        st.info("Painel de diversificação indisponível: portfólio final vazio.")
        return

    tickers = [_norm_tk(e.get("ticker", "")) for e in empresas_lideres_finais if _norm_tk(e.get("ticker", ""))]
    tickers = list(dict.fromkeys(tickers))  # preserva ordem e remove duplicatas

    # setores (se existir no dict)
    setores = [str(e.get("setor", "OUTROS")) for e in empresas_lideres_finais if _norm_tk(e.get("ticker", "")) in tickers]

    # pesos: tenta usar contrib_globais (se existir) agregando por ticker; senão pesos iguais
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

    # ticker concentration
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

    # tabela pesos
    dfw = pd.DataFrame({
        "ticker": tickers,
        "empresa": [_get_nome(tk, empresas_lideres_finais) for tk in tickers],
        "peso_%": (w_series.values * 100.0),
    }).sort_values("peso_%", ascending=False)

    st.dataframe(dfw, use_container_width=True)

    # concentração setorial (usa setor do dict; se não houver, cai em OUTROS)
    df_set = pd.DataFrame({"ticker": tickers, "setor": setores[:len(tickers)] if setores else ["OUTROS"] * len(tickers)})
    df_set["peso"] = df_set["ticker"].map(lambda t: float(weights.get(_norm_tk(t), 0.0)))
    set_agg = df_set.groupby("setor")["peso"].sum().sort_values(ascending=False)

    if not set_agg.empty:
        st.markdown("### Concentração por setor")
        fig, ax = plt.subplots()
        ax.bar(set_agg.index.astype(str), set_agg.values.astype(float))
        ax.set_ylabel("Peso (0–1)")
        ax.set_xlabel("Setor")
        ax.grid(True, linestyle="--", alpha=0.4)
        st.pyplot(fig)
