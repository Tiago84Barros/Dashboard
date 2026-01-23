from __future__ import annotations

from typing import List, Dict, Optional, Sequence
import textwrap

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

from core.yf_data import baixar_precos


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


def _short_label(s: str, max_len: int = 22) -> str:
    s = (s or "").strip()
    if not s:
        return "OUTROS"
    s = s.replace("  ", " ")
    return "\n".join(textwrap.wrap(s, width=max_len)) if len(s) > max_len else s


def _strip_sa(ticker: str) -> str:
    return (ticker or "").strip().upper().replace(".SA", "").strip()


def _chunks(seq: Sequence[str], size: int) -> List[List[str]]:
    out: List[List[str]] = []
    seq = [s for s in seq if s]
    for i in range(0, len(seq), max(1, int(size))):
        out.append(list(seq[i:i + size]))
    return out


# ─────────────────────────────────────────────────────────────
# Cache local (session_state) de preços por ticker (Series)
# IMPORTANTE: yf_data.baixar_precos retorna colunas SEM ".SA"
# ─────────────────────────────────────────────────────────────

def _get_precos_cached(tickers: List[str], chunk_size: int = 80) -> pd.DataFrame:
    """
    Retorna DF index=Data, cols=tickers (SEM .SA).
    Cache por ticker (Series) em st.session_state para evitar chamadas repetidas.
    """
    def _key(t: str) -> str:
        return _strip_sa(str(t).strip().upper())

    req = [_key(t) for t in tickers if str(t).strip()]
    req = list(dict.fromkeys(req))
    if not req:
        return pd.DataFrame()

    cache: Dict[str, pd.Series] = st.session_state.setdefault("_precos_cache_series", {})

    missing = [t for t in req if t not in cache]

    for lote in _chunks(missing, chunk_size):
        df = baixar_precos(lote)  # pode passar sem .SA
        if df is None or df.empty:
            continue

        df.index = pd.to_datetime(df.index, errors="coerce")
        df = df.dropna(how="all")
        if df.empty:
            continue

        for col in df.columns.astype(str).tolist():
            c = _key(col)  # garante SEM .SA
            try:
                s = pd.to_numeric(df[col], errors="coerce").dropna()
                if not s.empty:
                    cache[c] = s
            except Exception:
                continue

    series_list: List[pd.Series] = []
    cols: List[str] = []

    for t in req:
        s = cache.get(t)
        if isinstance(s, pd.Series) and not s.empty:
            series_list.append(s.rename(t))
            cols.append(t)

    if not series_list:
        return pd.DataFrame()

    out = pd.concat(series_list, axis=1).sort_index()
    out.columns = cols
    return out


@st.cache_data(show_spinner=False, ttl=3600)
def _retornos_ano_preco_bulk(tickers: List[str], ano: int, chunk_size: int = 80) -> pd.Series:
    """
    Retorno simples de preço (sem dividendos) no ano calendário para LISTA de tickers.
    Retorna Series index=ticker_sem_SA, values=retorno (float).
    Usa cache + chunking para reduzir chamadas.
    """
    tickers = [_strip_sa(t) for t in (tickers or []) if str(t).strip()]
    tickers = list(dict.fromkeys(tickers))
    if not tickers:
        return pd.Series(dtype=float)

    precos = _get_precos_cached(tickers, chunk_size=chunk_size)
    if precos is None or precos.empty:
        return pd.Series(dtype=float)

    precos.index = pd.to_datetime(precos.index, errors="coerce")
    precos = precos.dropna(how="all")
    if precos.empty:
        return pd.Series(dtype=float)

    ini = pd.Timestamp(f"{ano}-01-01")
    fim = pd.Timestamp(f"{ano}-12-31")
    precos = precos.loc[(precos.index >= ini) & (precos.index <= fim)]
    if precos.empty:
        return pd.Series(dtype=float)

    precos = precos.resample("B").last().ffill()
    if precos.shape[0] < 2:
        return pd.Series(dtype=float)

    first = precos.iloc[0]
    last = precos.iloc[-1]
    ret = (last / (first + 1e-12)) - 1.0

    ret.index = [_strip_sa(c) for c in ret.index.astype(str).tolist()]
    ret = pd.to_numeric(ret, errors="coerce").dropna()
    return ret


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
# PATCH 4 — Diversificação e Concentração de Risco
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

    tickers = [_strip_sa(e.get("ticker", "")) for e in empresas_lideres_finais if _strip_sa(e.get("ticker", ""))]
    tickers = list(dict.fromkeys(tickers))

    setores = [str(e.get("setor", "OUTROS")) for e in empresas_lideres_finais if _strip_sa(e.get("ticker", "")) in tickers]

    weights = None
    if contrib_globais:
        try:
            dfc = pd.DataFrame(contrib_globais).copy()
            if not dfc.empty and {"ticker", "valor_final"}.issubset(dfc.columns):
                dfc["ticker"] = dfc["ticker"].astype(str).apply(_strip_sa)
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

    dfw = pd.DataFrame({
        "ticker": tickers,
        "empresa": [_get_nome(tk, empresas_lideres_finais) for tk in tickers],
        "peso_%": (w_series.values * 100.0),
    }).sort_values("peso_%", ascending=False)

    st.dataframe(dfw, use_container_width=True)

    df_set = pd.DataFrame({"ticker": tickers, "setor": setores[:len(tickers)] if setores else ["OUTROS"] * len(tickers)})
    df_set["peso"] = df_set["ticker"].map(lambda t: float(weights.get(_strip_sa(t), 0.0)))
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
# PATCH 5 — Benchmark do Segmento (último ano do score)
# (otimizado: 1 chamada bulk por ano para todos os tickers necessários)
# ─────────────────────────────────────────────────────────────

def render_patch5_benchmark_segmento(score_global: pd.DataFrame, empresas_lideres_finais: list[dict]) -> None:
    """
    Compara retorno no último ano do score vs retorno médio do segmento (SETOR>SUBSETOR>SEGMENTO).
    Retorno de preço (sem dividendos).
    OTIMIZAÇÃO: usa retorno bulk no ano para todo o universo necessário (1 chamada consolidada).
    """

    st.markdown("## 📌 Benchmark do Segmento (último ano do score)")
    st.caption(
        "Compara o retorno das empresas escolhidas no último ano do score com o retorno médio do "
        "segmento (SETOR > SUBSETOR > SEGMENTO) em que elas estão inseridas."
    )

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

    MAX_UNIVERSE = 40

    # Monta um conjunto único de tickers que serão necessários no cálculo (para 1 chamada bulk)
    needed_all: List[str] = []
    per_ticker_universe: Dict[str, List[str]] = {}

    for tk in tickers_finais:
        info = meta_map.get(tk)
        if not info:
            continue
        id_seg = f"{info['SETOR']} > {info['SUBSETOR']} > {info['SEGMENTO']}"
        universo = uni_segmento.get(id_seg, [])
        universo_lim = universo[:MAX_UNIVERSE]
        per_ticker_universe[tk] = universo_lim
        needed_all.extend(universo_lim)
        needed_all.append(tk)

    needed_all = list(dict.fromkeys([_strip_sa(x) for x in needed_all if x]))

    # 1 única chamada bulk para retornos do ano em todos os tickers necessários
    ret_all = _retornos_ano_preco_bulk(needed_all, ultimo_ano, chunk_size=80)

    linhas = []
    for tk in tickers_finais:
        info = meta_map.get(tk)
        if not info:
            continue

        universo_lim = per_ticker_universe.get(tk, [])
        if not universo_lim:
            continue

        ret_uni = ret_all.reindex([_strip_sa(x) for x in universo_lim]).dropna()
        seg_mean = float(ret_uni.mean()) if not ret_uni.empty else float("nan")

        tk_ret = float(ret_all.get(_strip_sa(tk), float("nan")))

        linhas.append({
            "ticker": tk,
            "empresa": next((e.get("nome") for e in empresas_lideres_finais if _strip_sa(str(e.get("ticker",""))) == tk), tk),
            "SETOR": info["SETOR"],
            "SUBSETOR": info["SUBSETOR"],
            "SEGMENTO": info["SEGMENTO"],
            "retorno_empresa_%": tk_ret * 100.0 if pd.notna(tk_ret) else float("nan"),
            "retorno_medio_segmento_%": seg_mean * 100.0 if pd.notna(seg_mean) else float("nan"),
            "alpha_vs_segmento_pp": ((tk_ret - seg_mean) * 100.0) if (pd.notna(tk_ret) and pd.notna(seg_mean)) else float("nan"),
            "tamanho_universo_usado": len(universo_lim),
        })

    if not linhas:
        st.info("Não foi possível montar o comparativo (metadados do segmento não encontrados para os tickers finais).")
        return

    out = pd.DataFrame(linhas)
    out = out.sort_values(["alpha_vs_segmento_pp", "retorno_empresa_%"], ascending=[False, False])

    st.markdown(f"**Ano analisado (último ano do score): {ultimo_ano}**")
    st.dataframe(out, use_container_width=True)

    st.caption(
        f"Nota técnica: retorno é de **preço** (sem dividendos). "
        f"O benchmark do segmento usa até **{MAX_UNIVERSE} tickers** do segmento para evitar travamentos."
    )

    with st.expander("📊 Gráfico: Alpha vs média do segmento (p.p.)", expanded=False):
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
