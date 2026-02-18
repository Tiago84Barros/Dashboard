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
# PATCH 5 — IA (OpenAI) — Seleção/validação amigável (era Patch 6)
# ─────────────────────────────────────────────────────────────

def _render_patch5_report(resp: Dict[str, Any], *, mostrar_tabela: bool = False) -> None:
    resumo = str(resp.get("resumo_executivo", "")).strip()
    obs = str(resp.get("observacao_importante", "")).strip()

    if resumo:
        st.markdown("### 🧾 Resumo")
        st.write(resumo)

    if obs:
        st.markdown("### ℹ️ Observação importante")
        st.info(obs)

    selecionadas = resp.get("selecionadas", []) or []
    st.markdown("### ✅ Candidatas mais fortes (segundo a IA)")
    if not selecionadas:
        st.warning("A IA não marcou nenhuma como forte.")
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


def render_patch5_ia_selecao_lideres(
    score_global: pd.DataFrame,
    lideres_global: pd.DataFrame,
    empresas_lideres_finais: List[Dict[str, Any]],
    *,
    max_recs_default: int = 10,
) -> Optional[Dict[str, Any]]:
    st.markdown("## 🤖 Patch 5 — Validação por IA (relatório amigável)")
    st.caption(
        "A IA usa **apenas** os dados que você já calculou (score e histórico de liderança). "
        "Ela **não conhece o futuro**."
    )

    if not empresas_lideres_finais:
        st.info("Sem líderes finais nesta execução — Patch 5 não tem o que analisar.")
        return None

    try:
        from core.ai_models.llm_client.factory import get_llm_client
    except Exception as e:
        st.error(f"Não consegui inicializar o cliente de IA (LLM). Erro: {type(e).__name__}: {e}")
        return None

    tickers = sorted({_norm_tk(e.get("ticker", "")) for e in empresas_lideres_finais if _norm_tk(e.get("ticker", ""))})
    if not tickers:
        st.info("Patch 5 indisponível: tickers finais inválidos.")
        return None

    ultimo_ano = None
    try:
        if isinstance(score_global, pd.DataFrame) and (not score_global.empty) and ("Ano" in score_global.columns):
            ultimo_ano = int(pd.to_numeric(score_global["Ano"], errors="coerce").max())
    except Exception:
        ultimo_ano = None

    if "patch5_ia_cache" not in st.session_state:
        st.session_state["patch5_ia_cache"] = {}  # run_key -> {"value": dict, "expires_at": float}

    with st.form("patch5_form", clear_on_submit=False):
        c1, c2, c3, c4 = st.columns([1.0, 1.0, 1.0, 1.2])
        with c1:
            max_recs = st.number_input("Quantidade de recomendações", 3, 25, int(max_recs_default), 1)
        with c2:
            mostrar_tabela = st.checkbox("Mostrar tabela", value=False)
        with c3:
            ttl_h = st.number_input("Cache (horas)", 1, 72, 12, 1)
        with c4:
            submitted = st.form_submit_button("🤖 Executar IA (Patch 5)")

    base_key_payload = {"tickers": tickers, "ultimo_ano": ultimo_ano, "max_recs": int(max_recs), "ver": 2}
    run_key = hashlib.md5(json.dumps(base_key_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()

    busy_flag = f"patch5_busy_{run_key}"
    run_flag = f"patch5_run_{run_key}"
    st.session_state.setdefault(busy_flag, False)
    st.session_state.setdefault(run_flag, False)

    def _cache_get(k: str) -> Optional[dict]:
        it = st.session_state["patch5_ia_cache"].get(k)
        if not it:
            return None
        if time.time() > float(it.get("expires_at", 0)):
            return None
        v = it.get("value")
        return v if isinstance(v, dict) else None

    def _cache_set(k: str, v: dict, ttl_seconds: int) -> None:
        st.session_state["patch5_ia_cache"][k] = {"value": v, "expires_at": time.time() + int(ttl_seconds)}

    cached = _cache_get(run_key)
    if cached and not submitted:
        st.success("Mostrando resultado já gerado (cache).")
        _render_patch5_report(cached, mostrar_tabela=bool(mostrar_tabela))
        return cached

    if submitted:
        st.session_state[run_flag] = True

    if not st.session_state[run_flag]:
        st.info("Clique em **🤖 Executar IA (Patch 5)** para gerar o relatório.")
        return None

    if st.session_state[busy_flag]:
        st.warning("Patch 5 já está em execução. Aguarde finalizar.")
        return None

    st.session_state[busy_flag] = True

    try:
        sg = score_global.copy() if isinstance(score_global, pd.DataFrame) else pd.DataFrame()
        lg = lideres_global.copy() if isinstance(lideres_global, pd.DataFrame) else pd.DataFrame()

        if not sg.empty and "ticker" in sg.columns:
            sg["ticker"] = sg["ticker"].astype(str).map(_norm_tk)
        if not lg.empty and "ticker" in lg.columns:
            lg["ticker"] = lg["ticker"].astype(str).map(_norm_tk)

        score_last = pd.DataFrame()
        if ultimo_ano is not None and (not sg.empty) and ("Ano" in sg.columns):
            score_last = sg[pd.to_numeric(sg["Ano"], errors="coerce") == ultimo_ano].copy()

        def _clip(s: Any, n: int = 60) -> str:
            t = str(s or "").strip()
            return (t[:n] + "…") if len(t) > n else t

        cards: List[Dict[str, Any]] = []
        for e in empresas_lideres_finais:
            tk = _norm_tk(str(e.get("ticker", "")))
            if not tk:
                continue

            nome = _clip(e.get("nome", tk), 70)
            setor = _clip(e.get("setor", e.get("SETOR", "OUTROS")), 40)

            score_ultimo = None
            if not score_last.empty and "ticker" in score_last.columns:
                rows = score_last[score_last["ticker"] == tk]
                if not rows.empty:
                    if "Score_Ajustado" in rows.columns:
                        score_ultimo = _safe_float(rows.iloc[0].get("Score_Ajustado"))
                    else:
                        sc_cols = [c for c in rows.columns if str(c).lower().startswith("score")]
                        score_ultimo = _safe_float(rows.iloc[0].get(sc_cols[0])) if sc_cols else None

            anos_lider: List[int] = []
            if not lg.empty and {"ticker", "Ano"}.issubset(lg.columns):
                try:
                    anos = lg.loc[lg["ticker"] == tk, "Ano"].dropna().astype(int).unique().tolist()
                    anos_lider = sorted(anos)
                except Exception:
                    anos_lider = []

            cards.append(
                {
                    "ticker": tk,
                    "empresa": nome,
                    "setor": setor,
                    "score_ultimo_ano": score_ultimo,
                    "qtd_anos_lider": int(len(anos_lider)),
                    "anos_lider_recent": anos_lider[-6:] if anos_lider else [],
                    "ano_base_score": ultimo_ano,
                }
            )

        cards = cards[:20]
        if not cards:
            st.warning("Não consegui montar contexto para a IA (cards vazios).")
            return None

        schema_hint = """
{
  "resumo_executivo": "STRING",
  "observacao_importante": "STRING",
  "selecionadas": [{"ticker":"STRING","empresa":"STRING","nota_0_100":0,"confianca_0_1":0.0,"por_que_entra":["STRING"],"riscos_principais":["STRING"],"como_eu_usaria":"STRING"}],
  "nao_selecionadas": [{"ticker":"STRING","empresa":"STRING","por_que_ficou_fora":["STRING"],"o_que_precisa_melhorar_ou_confirmar":["STRING"]}],
  "alertas_metodologicos": ["STRING"]
}
""".strip()

        system = (
            "Você é um analista fundamentalista prudente. "
            "Escreva em linguagem simples. "
            "NÃO use dados futuros. Não invente fatos. "
            "Se faltarem dados, diga que faltou e como validar."
        )

        user = (
            f"Tenho uma carteira candidata (líderes selecionadas por score). "
            f"O score vai até {ultimo_ano if ultimo_ano is not None else 'desconhecido'}. "
            f"Quero um mini-relatório amigável: candidatas mais fortes vs em observação.\n\n"
            f"Regras:\n"
            f"- Selecione no máximo {int(max_recs)} em 'selecionadas'.\n"
            f"- O resto em 'nao_selecionadas'.\n"
            f"- Não use performance futura.\n"
            f"- Baseie-se apenas no contexto.\n\n"
            f"Contexto (lista de empresas):\n{json.dumps(cards, ensure_ascii=False)}"
        )

        with st.spinner("Rodando IA (Patch 5)..."):
            llm = get_llm_client()
            resp = llm.generate_json(system=system, user=user, schema_hint=schema_hint, context=None)

        if not isinstance(resp, dict):
            st.error("A IA retornou algo inválido (não é dict).")
            return None

        resp.setdefault("resumo_executivo", "")
        resp.setdefault("observacao_importante", "")
        resp.setdefault("selecionadas", [])
        resp.setdefault("nao_selecionadas", [])
        resp.setdefault("alertas_metodologicos", [])

        _cache_set(run_key, resp, int(ttl_h) * 3600)
        _render_patch5_report(resp, mostrar_tabela=bool(mostrar_tabela))
        return resp

    except Exception as e:
        st.error(f"Falha ao chamar IA: {type(e).__name__}: {e}")
        return None

    finally:
        st.session_state[run_flag] = False
        st.session_state[busy_flag] = False


# ─────────────────────────────────────────────────────────────
# PATCH 6 — Evidências externas + Resumo do Portfólio (era Patch 7)
# ─────────────────────────────────────────────────────────────

def _p6_strip_sa(ticker: str) -> str:
    return (ticker or "").strip().upper().replace(".SA", "").strip()


def _p6_get_nome(ticker: str, empresas_lideres_finais: List[Dict]) -> str:
    tk = _p6_strip_sa(ticker)
    for e in (empresas_lideres_finais or []):
        if _p6_strip_sa(str(e.get("ticker", ""))) == tk:
            return str(e.get("nome") or tk)
    return tk


def _p6_bullets(xs: Any, max_items: int = 6) -> List[str]:
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


def _p6_make_key(*, tickers_and_names: List[List[str]], days: int, max_items: int) -> str:
    payload = {"t": tickers_and_names, "days": int(days), "max_items": int(max_items)}
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.md5(raw).hexdigest()


def _p6_get_store() -> Dict[str, Any]:
    if "patch6_last_payload" not in st.session_state:
        st.session_state["patch6_last_payload"] = {}
    return st.session_state["patch6_last_payload"]


def _p6_store_set(key: str, value: dict, ttl_seconds: int) -> None:
    store = _p6_get_store()
    store[key] = {"value": value, "expires_at": time.time() + int(ttl_seconds)}


def _p6_store_get(key: str) -> Optional[dict]:
    store = _p6_get_store()
    item = store.get(key)
    if not item:
        return None
    if time.time() > float(item.get("expires_at", 0)):
        return None
    return item.get("value")


def _p6_schema_fallback() -> str:
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


def _p6_inject_css() -> None:
    if st.session_state.get("_patch6_css_injected"):
        return

    st.markdown(
        """
        <style>
          .p6-title { font-size: 28px; font-weight: 800; margin: 0 0 6px 0; letter-spacing: 0.2px; color: #6DD5FA; }
          .p6-subtitle { font-size: 14px; opacity: 0.85; margin: 0 0 14px 0; }
          .p6-card { border: 1px solid rgba(255,255,255,0.10); border-radius: 14px; padding: 14px 16px; background: rgba(255,255,255,0.03); margin: 10px 0 14px 0; }
          .p6-section-title { font-size: 16px; font-weight: 800; margin: 0 0 8px 0; letter-spacing: 0.2px; }
          .p6-text { font-size: 16px; line-height: 1.65; margin: 0; }
          .p6-bullets { font-size: 16px; line-height: 1.65; margin: 6px 0 0 0; padding-left: 18px; }
          .p6-badge { display: inline-block; padding: 2px 10px; border-radius: 999px; font-size: 12px; font-weight: 800; letter-spacing: 0.3px; margin-left: 8px; vertical-align: middle; }
          .p6-badge-strong { background: rgba(124,252,152,0.18); color: #7CFC98; border: 1px solid rgba(124,252,152,0.35); }
          .p6-badge-neutral { background: rgba(243,210,80,0.18); color: #F3D250; border: 1px solid rgba(243,210,80,0.35); }
          .p6-badge-weak { background: rgba(255,107,107,0.18); color: #FF6B6B; border: 1px solid rgba(255,107,107,0.35); }
          .p6-kpi { font-size: 13px; opacity: 0.9; margin-top: 8px; }
          .p6-muted { opacity: 0.78; }
          div[data-testid="stExpander"] summary { font-weight: 700; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.session_state["_patch6_css_injected"] = True


def _p6_badge_html(veredito: str) -> str:
    v = (veredito or "").strip().lower()
    if v == "fortalece":
        return '<span class="p6-badge p6-badge-strong">FORTALECE</span>'
    if v == "enfraquece":
        return '<span class="p6-badge p6-badge-weak">ENFRAQUECE</span>'
    return '<span class="p6-badge p6-badge-neutral">NEUTRO</span>'


@st.cache_data(ttl=3600, show_spinner=False)
def _p6_fetch_news_cached(
    tickers_and_names_tuples: List[tuple],
    days: int,
    max_items: int,
) -> Dict[str, Any]:
    from core.ai_models.pipelines.news_pipeline import build_news_for_portfolio
    return (
        build_news_for_portfolio(
            tickers_and_names=tickers_and_names_tuples,
            days=int(days),
            max_items_per_ticker=int(max_items),
        )
        or {}
    )


def render_patch6_validacao_evidencias(
    score_global: pd.DataFrame,
    lideres_global: pd.DataFrame,
    empresas_lideres_finais: List[Dict],
    *,
    days_default: int = 60,
    max_items_per_ticker_default: int = 10,
    cache_ttl_hours_default: int = 12,
) -> Optional[dict]:

    _p6_inject_css()

    st.session_state.setdefault("patch6_run", False)
    st.session_state.setdefault("patch6_last_key", None)

    st.markdown(
        """
        <div>
          <div class="p6-title">📊 Patch 6 — Evidências externas & Leitura do Portfólio</div>
          <div class="p6-subtitle p6-muted">Validação qualitativa baseada em notícias/fontes recentes.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if not empresas_lideres_finais:
        st.info("Patch 6 indisponível: não há líderes finais.")
        return None

    tickers_and_names = [
        [_p6_strip_sa(e.get("ticker", "")), _p6_get_nome(e.get("ticker", ""), empresas_lideres_finais)]
        for e in (empresas_lideres_finais or [])
        if _p6_strip_sa(e.get("ticker", ""))
    ]
    if not tickers_and_names:
        st.info("Patch 6 indisponível: tickers inválidos.")
        return None

    with st.form("patch6_form", clear_on_submit=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            days = st.number_input("Janela (dias)", 7, 365, int(days_default), 7)
        with c2:
            max_items = st.number_input("Evidências por empresa", 3, 20, int(max_items_per_ticker_default), 1)
        with c3:
            ttl_h = st.number_input("Cache (horas)", 1, 72, int(cache_ttl_hours_default), 1)
        submitted = st.form_submit_button("Rodar Patch 6")

    if submitted:
        st.session_state["patch6_run"] = True

    cache_key = _p6_make_key(tickers_and_names=tickers_and_names, days=int(days), max_items=int(max_items))
    st.session_state["patch6_last_key"] = cache_key

    cached = _p6_store_get(cache_key)
    if cached and not st.session_state["patch6_run"]:
        st.success("Mostrando último resultado do Patch 6 (cache).")
        _render_patch6_output(cached, empresas_lideres_finais)
        return cached

    if not st.session_state["patch6_run"]:
        st.info("Clique em **Rodar Patch 6** para gerar o relatório.")
        return None

    try:
        from core.ai_models.llm_client.factory import get_llm_client
        from core.ai_models.prompts.system import SYSTEM_GUARDRAILS
        try:
            from core.ai_models.prompts.schemas import SCHEMA_PATCH7 as SCHEMA_PATCH6  # se existir
        except Exception:
            SCHEMA_PATCH6 = _p6_schema_fallback()
    except Exception as e:
        st.error(f"Patch 6 indisponível: erro ao importar módulos IA. {type(e).__name__}: {e}")
        st.session_state["patch6_run"] = False
        return None

    with st.spinner("Coletando evidências recentes..."):
        try:
            news_map = _p6_fetch_news_cached(
                tickers_and_names_tuples=[(a[0], a[1]) for a in tickers_and_names],
                days=int(days),
                max_items=int(max_items),
            ) or {}
        except Exception as e:
            st.error(f"Falha ao coletar notícias: {type(e).__name__}: {e}")
            st.session_state["patch6_run"] = False
            return None

    try:
        llm = get_llm_client()
    except Exception as e:
        st.error(f"Falha ao inicializar IA: {type(e).__name__}: {e}")
        st.session_state["patch6_run"] = False
        return None

    resultados: Dict[str, dict] = {}
    falhas: List[Dict[str, str]] = []

    progress = st.progress(0)
    total = max(1, len(tickers_and_names))

    for i, (tk, nome) in enumerate(tickers_and_names, start=1):
        progress.progress(min(i / total, 1.0))
        st.write(f"Analisando: {nome} ({tk})…")

        items = news_map.get(tk, []) or []
        ctx_items = []
        for it in items:
            try:
                title = getattr(it, "title", None) or (it.get("title") if isinstance(it, dict) else "")
                source = getattr(it, "source", None) or (it.get("source") if isinstance(it, dict) else "")
                link = getattr(it, "link", None) or (it.get("link") if isinstance(it, dict) else "")
                published_at = getattr(it, "published_at", None) or (it.get("published_at") if isinstance(it, dict) else None)
                snippet = getattr(it, "snippet", None) or (it.get("snippet") if isinstance(it, dict) else "")
                dt = published_at.isoformat() if hasattr(published_at, "isoformat") and published_at else ""
            except Exception:
                title, source, link, dt, snippet = "", "", "", "", ""

            ctx_items.append({"title": str(title), "source": str(source), "date": str(dt), "url": str(link), "snippet": str(snippet)})

        user_task = (
            f"Analise a empresa {nome} ({tk}) usando APENAS as evidências no contexto.\n\n"
            "Entregue:\n"
            "1) Resumo (4–6 linhas)\n"
            "2) Catalisadores (3–5)\n"
            "3) Riscos (3–5)\n"
            "4) Veredito: 'fortalece', 'neutro' ou 'enfraquece'\n\n"
            "Não invente fatos. Se as evidências forem fracas, diga isso."
        )

        try:
            out = llm.generate_json(system=SYSTEM_GUARDRAILS, user=user_task, schema_hint=SCHEMA_PATCH6, context=ctx_items)
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
                "Tom: curto, simples e amigável."
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

    _p6_store_set(cache_key, payload, int(ttl_h) * 3600)
    _render_patch6_output(payload, empresas_lideres_finais)
    st.session_state["patch6_run"] = False
    return payload


def _render_patch6_output(payload: dict, empresas_lideres_finais: List[Dict]) -> None:
    _p6_inject_css()

    resultados = (payload or {}).get("resultados_por_ticker", {}) or {}
    resumo = (payload or {}).get("resumo_portfolio", {}) or {}
    falhas = (payload or {}).get("falhas", []) or []
    params = (payload or {}).get("params", {}) or {}
    days = params.get("days")
    max_items = params.get("max_items")

    st.markdown(
        f"""
        <div class="p6-card">
          <div class="p6-section-title" style="color:#F3D250;">🧠 Contexto Estratégico</div>
          <div class="p6-kpi p6-muted">Janela: <b>{days}</b> dias • Evidências por empresa: <b>{max_items}</b></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="p6-card">
          <div class="p6-section-title" style="color:#6DD5FA;">📌 Resumo do portfólio</div>
        """,
        unsafe_allow_html=True,
    )

    if isinstance(resumo, dict) and resumo.get("erro"):
        st.warning(f"Falha ao gerar resumo do portfólio: {resumo.get('erro')}")
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        visao = str(resumo.get("visao_geral") or "—").strip()
        st.markdown(f'<p class="p6-text">{visao}</p>', unsafe_allow_html=True)

        def _render_list(title: str, key: str, color: str) -> None:
            items = _p6_bullets(resumo.get(key), max_items=8)
            if not items:
                return
            st.markdown(
                f'<div style="margin-top:12px;"><div class="p6-section-title" style="color:{color};">{title}</div></div>',
                unsafe_allow_html=True,
            )
            st.markdown('<ul class="p6-bullets">', unsafe_allow_html=True)
            for x in items:
                st.markdown(f"<li>{x}</li>", unsafe_allow_html=True)
            st.markdown("</ul>", unsafe_allow_html=True)

        _render_list("🚀 Destaques & Catalisadores", "destaques", "#7CFC98")
        _render_list("⚠️ Riscos comuns", "riscos_comuns", "#FF6B6B")
        _render_list("🧩 Catalisadores comuns", "catalisadores_comuns", "#7CFC98")
        _render_list("✅ Ações práticas", "acoes_praticas", "#6DD5FA")
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown(
        """
        <div class="p6-card">
          <div class="p6-section-title" style="color:#6DD5FA;">🧩 Relatório por empresa</div>
          <div class="p6-muted" style="font-size:13px;">Abra cada empresa para ver resumo, catalisadores e riscos.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    for e in (empresas_lideres_finais or []):
        tk = _p6_strip_sa(str(e.get("ticker", "")))
        if not tk:
            continue
        rep = resultados.get(tk, {}) or {}

        nome = _p6_get_nome(tk, empresas_lideres_finais)
        ver = str(rep.get("veredito") or "neutro").strip().lower()
        res = str(rep.get("resumo") or "—").strip()
        cats = _p6_bullets(rep.get("catalisadores"), max_items=8)
        risks = _p6_bullets(rep.get("riscos"), max_items=8)

        badge = _p6_badge_html(ver)
        exp_title = f"{nome} ({tk})"
        with st.expander(exp_title, expanded=False):
            st.markdown(
                f"""
                <div class="p6-card">
                  <div class="p6-section-title" style="color:#6DD5FA;">
                    🏭 {nome} ({tk}) {badge}
                  </div>
                  <p class="p6-text">{res}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )

            if cats:
                st.markdown('<div class="p6-card"><div class="p6-section-title" style="color:#7CFC98;">🚀 Catalisadores</div>', unsafe_allow_html=True)
                st.markdown('<ul class="p6-bullets">', unsafe_allow_html=True)
                for x in cats:
                    st.markdown(f"<li>{x}</li>", unsafe_allow_html=True)
                st.markdown("</ul></div>", unsafe_allow_html=True)

            if risks:
                st.markdown('<div class="p6-card"><div class="p6-section-title" style="color:#FF6B6B;">⚠️ Riscos</div>', unsafe_allow_html=True)
                st.markdown('<ul class="p6-bullets">', unsafe_allow_html=True)
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



# ─────────────────────────────────────────────────────────────
# PATCH 6 (CVM/IPE) — Direcionalidade Estratégica (RAG) + Overlay de Aporte
# (Novo Patch para Criação de Portfólio; UI enxuta, sem "chunk" (usa "variáveis úteis"))
# ─────────────────────────────────────────────────────────────

def render_patch6_cvm_ipe_direcionalidade(
    *,
    empresas_lideres_finais: List[Dict[str, Any]],
    score_global: Optional[pd.DataFrame] = None,
    precos: Optional[pd.DataFrame] = None,
    aporte_mes: Optional[float] = None,
    window_months_default: int = 12,
    top_k_default: int = 25,
) -> Optional[Dict[str, float]]:
    """
    Patch 6 (CVM/IPE): coleta docs -> chunking -> RAG -> LLM.
    Saída: dicionário {ticker: pct_aporte} sugerido (0-1), baseado na perspectiva da LLM.
    Não altera portfólio (não troca ativos). Apenas modula o aporte mensal.
    """
    st.markdown("## 🧠 Patch 6 — Direcionalidade Estratégica (CVM/IPE)")
    st.caption(
        "Este patch usa documentos **CVM/IPE** recentes (estratégicos) para inferir "
        "direcionalidade (forte/moderada/fraca) e sugerir **modulação de aporte** dentro da cesta já selecionada."
    )

    if not empresas_lideres_finais:
        st.info("Sem ativos selecionados — Patch 6 não tem o que analisar.")
        return None

    # CSS (cards) — enxuto e legível
    st.markdown(
        """
        <style>
          .p6x-wrap { display:block; margin: 8px 0 18px 0; }
          .p6x-card {
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.10);
            border-radius: 14px;
            padding: 14px 14px 12px 14px;
            box-shadow: 0 6px 18px rgba(0,0,0,0.22);
            margin: 10px 0;
          }
          .p6x-head { display:flex; justify-content:space-between; align-items:baseline; gap:12px; }
          .p6x-title { font-weight: 900; font-size: 16px; color:#fff; }
          .p6x-pill {
            font-weight: 900;
            padding: 4px 10px;
            border-radius: 999px;
            font-size: 12px;
            border: 1px solid rgba(255,255,255,0.14);
            background: rgba(255,255,255,0.06);
          }
          .p6x-sub { color: rgba(255,255,255,0.75); font-size: 12.5px; margin-top: 6px; }
          .p6x-grid { display:grid; grid-template-columns: 1fr 1fr; gap:10px; margin-top: 10px; }
          .p6x-box {
            background: rgba(0,0,0,0.18);
            border: 1px solid rgba(255,255,255,0.10);
            border-radius: 12px;
            padding: 10px 10px 8px 10px;
          }
          .p6x-box h4 { margin:0 0 6px 0; font-size: 13px; color: rgba(255,255,255,0.92); }
          .p6x-box ul { margin: 0; padding-left: 16px; color: rgba(255,255,255,0.82); }
          .p6x-box li { margin: 2px 0; font-size: 12.5px; }
          .p6x-why { margin-top: 10px; color: rgba(255,255,255,0.86); font-size: 13px; }
          .p6x-meta { margin-top: 8px; color: rgba(255,255,255,0.62); font-size: 12px; }
          .p6x-divider { height:1px; background: rgba(255,255,255,0.08); margin: 10px 0; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Estado
    st.session_state.setdefault("p6x_reports", {})   # ticker -> dict
    st.session_state.setdefault("p6x_vars_count", {})  # ticker -> int
    st.session_state.setdefault("p6x_last_ingest", None)
    st.session_state.setdefault("p6x_last_run_key", None)

    tickers = sorted({_norm_tk(e.get("ticker", "")) for e in (empresas_lideres_finais or []) if _norm_tk(e.get("ticker",""))})
    if not tickers:
        st.info("Tickers inválidos.")
        return None

    # Pequenas métricas quantitativas para enriquecer a LLM (opcional)
    def _compute_quant_metrics() -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {t: {} for t in tickers}

        # Score do último ano (se houver)
        try:
            sg = score_global.copy() if isinstance(score_global, pd.DataFrame) else pd.DataFrame()
            if not sg.empty and "ticker" in sg.columns:
                sg["ticker"] = sg["ticker"].astype(str).map(_norm_tk)
            if not sg.empty and "Ano" in sg.columns:
                sg["Ano"] = pd.to_numeric(sg["Ano"], errors="coerce")
                ultimo = int(sg["Ano"].max())
                sg_last = sg[sg["Ano"] == ultimo].copy()
                for t in tickers:
                    r = sg_last[sg_last["ticker"] == t]
                    if not r.empty:
                        if "Score_Ajustado" in r.columns:
                            out[t]["score_ultimo_ano"] = _safe_float(r.iloc[0].get("Score_Ajustado"))
                        else:
                            sc_cols = [c for c in r.columns if str(c).lower().startswith("score")]
                            out[t]["score_ultimo_ano"] = _safe_float(r.iloc[0].get(sc_cols[0])) if sc_cols else None
                        out[t]["ano_score"] = ultimo
        except Exception:
            pass

        # Preço/vol/drawdown 12m (se houver)
        try:
            px = _ensure_prices_df(precos)
            if not px.empty:
                px = px.resample("B").last().ffill()
                last_dt = px.index.max()
                start_dt = last_dt - pd.Timedelta(days=365)
                px12 = px.loc[px.index >= start_dt].copy()
                rets = px12.pct_change().replace([np.inf, -np.inf], np.nan)
                for t in tickers:
                    if t in px12.columns:
                        s = px12[t].dropna()
                        if s.shape[0] >= 2:
                            out[t]["retorno_12m_preco"] = float(s.iloc[-1] / (s.iloc[0] + 1e-12) - 1.0)
                            rr = rets[t].dropna()
                            if rr.shape[0] >= 10:
                                out[t]["vol_12m_anualizada"] = float(rr.std() * np.sqrt(252))
                            # drawdown
                            roll_max = s.cummax()
                            dd = (s / (roll_max + 1e-12) - 1.0).min()
                            out[t]["drawdown_12m"] = float(dd)
        except Exception:
            pass

        return out

    quant_metrics = _compute_quant_metrics()

    # Supabase helpers (para contar e ler "variáveis úteis")
    def _get_engine():
        from core.db_loader import get_supabase_engine
        return get_supabase_engine()

    def _read_sql_df(sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        from sqlalchemy import text as _sql_text
        eng = _get_engine()
        with eng.connect() as conn:
            return pd.read_sql_query(_sql_text(sql), conn, params=params or {})

    def _count_vars_uteis_by_tickers(tks: List[str]) -> Dict[str, int]:
        if not tks:
            return {}
        df = _read_sql_df(
            """
            select ticker, count(*)::int as cnt
            from public.docs_corporativos_chunks
            where ticker = any(:tks)
            group by ticker
            """,
            {"tks": tks},
        )
        out = {t: 0 for t in tks}
        for _, r in df.iterrows():
            out[str(r["ticker"])] = int(r["cnt"])
        return out

    def _get_vars_uteis_for_rag(ticker: str, top_k: int) -> List[Dict[str, Any]]:
        df = _read_sql_df(
            """
            select id, doc_id, ticker, chunk_index, chunk_text, created_at
            from public.docs_corporativos_chunks
            where ticker = :tk
            order by id desc
            limit :lim
            """,
            {"tk": ticker, "lim": int(top_k)},
        )
        return [] if df is None or df.empty else df.to_dict(orient="records")

    # LLM helpers
    def _build_prompt(ticker: str, context: str, qvars: Dict[str, Any]) -> str:
        q_block = json.dumps(qvars or {}, ensure_ascii=False)
        return f"""
Você é um analista fundamentalista buy-side focado em direcionalidade estratégica (capex, expansão, guidance, investimentos futuros,
desalavancagem, alocação de capital e prioridades do management).

Seu trabalho é avaliar a empresa **{ticker}** com base em:
(1) variáveis úteis extraídas de documentos (CVM/IPE) e
(2) variáveis quantitativas (se disponíveis).

ENTREGA (responda em JSON):
{{
  "ticker": "{ticker}",
  "perspectiva_compra": "forte|moderada|fraca",
  "resumo": "2-4 frases, direto",
  "pontos_chave": ["...","...","..."],
  "riscos_ou_alertas": ["...","..."],
  "sinais_de_investimento_futuro": ["capex","expansão","projetos","guidance","M&A","desalavancagem", "..."],
  "porque": "1 parágrafo objetivo (por que forte/moderada/fraca)",
  "evidencias": [
    {{"fonte":"CVM/IPE","trecho":"<=240 chars","observacao":"por que isso importa"}}
  ]
}}

REGRAS:
- Não invente números/dados. Se não houver, diga explicitamente "não informado".
- Foque em intenção estratégica e direcionamento, não em DFP/ITR.
- Evidências devem vir do contexto fornecido (variáveis úteis).

[VARIÁVEIS QUANTITATIVAS]
{q_block}

[VARIÁVEIS ÚTEIS - RAG]
{context}
""".strip()

    def _run_llm(ticker: str, top_k: int) -> Dict[str, Any]:
        vars_uteis = _get_vars_uteis_for_rag(ticker, top_k=int(top_k))
        if not vars_uteis:
            return {"ok": False, "error": f"Sem variáveis úteis no banco para {ticker}. Rode a aquisição antes."}

        parts: List[str] = []
        for c in vars_uteis[::-1]:
            txt = str(c.get("chunk_text", "") or "").strip()
            if txt:
                parts.append(txt[:1800])
        context = "\n\n---\n\n".join(parts)

        prompt = _build_prompt(ticker=ticker, context=context, qvars=quant_metrics.get(ticker, {}))

        schema_hint = r"""
{
  "ticker": "STRING",
  "perspectiva_compra": "forte|moderada|fraca",
  "resumo": "STRING",
  "pontos_chave": ["STRING"],
  "riscos_ou_alertas": ["STRING"],
  "sinais_de_investimento_futuro": ["STRING"],
  "porque": "STRING",
  "evidencias": [{"fonte":"STRING","trecho":"STRING","observacao":"STRING"}]
}
""".strip()

        from core.ai_models.llm_client.factory import get_llm_client
        llm = get_llm_client()

        system = """
Você é um analista buy-side, cético e orientado a evidência.
- NÃO invente fatos, números, datas.
- Use APENAS o contexto fornecido.
- Responda OBRIGATORIAMENTE em JSON válido.
""".strip()

        out = llm.generate_json(system=system, user=prompt, schema_hint=schema_hint, context=None)
        return {"ok": True, "result": out, "meta": {"vars_uteis_usadas": len(vars_uteis), "top_k": int(top_k)}}

    # UI
    with st.form("p6x_form", clear_on_submit=False):
        c1, c2, c3, c4 = st.columns([1.0, 1.0, 1.0, 1.1])
        with c1:
            window_months = st.number_input("Janela (meses)", min_value=1, max_value=24, value=int(window_months_default), step=1)
        with c2:
            max_docs = st.number_input("Máx docs por ticker", min_value=5, max_value=200, value=60, step=5)
        with c3:
            top_k = st.number_input("Variáveis úteis por empresa", min_value=5, max_value=120, value=int(top_k_default), step=5)
        with c4:
            strategic_only = st.checkbox("Somente estratégicos", value=True)

        c5, c6 = st.columns([1.0, 1.0])
        with c5:
            btn_ingest = st.form_submit_button("⬇️ Buscar informações (CVM/IPE)")
        with c6:
            btn_llm = st.form_submit_button("🤖 Gerar relatórios (IA)")

    # A) Aquisição + chunking
    if btn_ingest:
        try:
            from pickup.ingest_docs_cvm_ipe import ingest_ipe_for_tickers
        except Exception:
            try:
                from core.ingest_docs_cvm_ipe import ingest_ipe_for_tickers  # type: ignore
            except Exception as e:
                st.error(f"Não encontrei o ingest runner: {type(e).__name__}: {e}")
                ingest_ipe_for_tickers = None  # type: ignore

        if ingest_ipe_for_tickers is not None:
            with st.spinner("Buscando documentos CVM/IPE (com heurística estratégica)..."):
                out = ingest_ipe_for_tickers(
                    tickers=tickers,
                    window_months=int(window_months),
                    max_docs_per_ticker=int(max_docs),
                    strategic_only=bool(strategic_only),
                    download_pdfs=True,
                    max_pdfs_per_ticker=12,
                    max_runtime_s=45.0,
                    verbose=False,
                )
            st.session_state["p6x_last_ingest"] = out

            # chunking integrado (missing chunks)
            try:
                from core.patch6_store import process_missing_chunks_for_ticker
                res_all = {}
                with st.spinner("Gerando variáveis úteis (chunking)..."):
                    for tk in tickers:
                        res_all[tk] = process_missing_chunks_for_ticker(tk, limit_docs=60, only_with_text=True)
                st.success("Aquisição concluída + variáveis úteis geradas.")
                with st.expander("Ver auditoria da aquisição (opcional)", expanded=False):
                    st.json({"ingest": out, "chunking": res_all})
            except Exception as e:
                st.warning(f"Aquisição OK, mas chunking falhou: {type(e).__name__}: {e}")

        # contagem de variáveis úteis
        try:
            counts = _count_vars_uteis_by_tickers(tickers)
            st.session_state["p6x_vars_count"] = counts
            st.info("Variáveis úteis disponíveis por empresa:")
            st.json(counts)
        except Exception as e:
            st.warning(f"Não consegui contar variáveis úteis: {type(e).__name__}: {e}")

    # B) Relatórios IA (progressivo)
    if btn_llm:
        counts = st.session_state.get("p6x_vars_count") or _count_vars_uteis_by_tickers(tickers)
        st.session_state["p6x_vars_count"] = counts

        # Evita recomputar se já tem resultado e nada mudou
        run_key_payload = {
            "tickers": tickers,
            "top_k": int(top_k),
            "window_months": int(window_months),
            "ver": 1,
        }
        run_key = hashlib.md5(json.dumps(run_key_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
        st.session_state["p6x_last_run_key"] = run_key

        st.markdown("### 📄 Relatórios por empresa (gerando...)")
        wrap = st.container()
        placeholders = {tk: wrap.empty() for tk in tickers}

        # Mostra o que já existe imediatamente
        existing = st.session_state.get("p6x_reports", {}) or {}
        for tk in tickers:
            if tk in existing:
                placeholders[tk].markdown(_p6x_render_card_html(tk, existing[tk], counts.get(tk, 0)), unsafe_allow_html=True)

        # Processa faltantes
        for tk in tickers:
            if tk in existing:
                continue
            with st.spinner(f"IA analisando {tk}..."):
                try:
                    out = _run_llm(tk, int(top_k))
                    if out.get("ok"):
                        rep = out.get("result") or {}
                        meta = out.get("meta") or {}
                        rep["_meta"] = meta
                        st.session_state["p6x_reports"][tk] = rep
                        placeholders[tk].markdown(_p6x_render_card_html(tk, rep, counts.get(tk, 0)), unsafe_allow_html=True)
                    else:
                        placeholders[tk].warning(out.get("error") or f"Falha ao analisar {tk}.")
                except Exception as e:
                    placeholders[tk].error(f"Erro em {tk}: {type(e).__name__}: {e}")

        st.success("Relatórios concluídos.")

    # Render cards (se já existem)
    reports: Dict[str, Any] = st.session_state.get("p6x_reports", {}) or {}
    counts: Dict[str, int] = st.session_state.get("p6x_vars_count", {}) or {}
    if reports:
        st.markdown("### 📌 Relatórios disponíveis")
        for tk in tickers:
            if tk in reports:
                st.markdown(_p6x_render_card_html(tk, reports[tk], counts.get(tk, 0)), unsafe_allow_html=True)

    # Sugestão de percentuais de aporte
    if reports:
        st.markdown("### 💰 Sugestão de aporte por empresa (com base na IA)")

        def _mult(p: str) -> float:
            p = (p or "").strip().lower()
            if p == "forte":
                return 1.15
            if p == "fraca":
                return 0.85
            return 1.00

        mults = {}
        for tk in tickers:
            rep = reports.get(tk) or {}
            mults[tk] = _mult(str(rep.get("perspectiva_compra", "moderada")))
        s = sum(mults.values()) or 1.0
        pct = {tk: (mults[tk] / s) for tk in tickers}

        df = pd.DataFrame(
            [
                {
                    "Ticker": tk,
                    "Empresa": _get_nome(tk, empresas_lideres_finais),
                    "Perspectiva (IA)": str((reports.get(tk) or {}).get("perspectiva_compra", "moderada")),
                    "% Aporte": round(pct[tk] * 100.0, 2),
                }
                for tk in tickers
            ]
        ).sort_values("% Aporte", ascending=False)

        st.dataframe(df, use_container_width=True, hide_index=True)

        if aporte_mes is not None:
            try:
                ap = float(aporte_mes)
                if ap > 0:
                    df2 = df.copy()
                    df2["R$ Aporte sugerido"] = df2["% Aporte"].astype(float) / 100.0 * ap
                    st.caption("Distribuição sugerida para o aporte do mês informado.")
                    st.dataframe(df2, use_container_width=True, hide_index=True)
            except Exception:
                pass

        # Persistir para uso posterior na ordem do mês (sem export)
        st.session_state["patch6_overlay_pct"] = pct

        return pct

    st.info("1) Busque informações CVM/IPE. 2) Gere relatórios IA. 3) Veja a sugestão de aporte.")
    return None


def _p6x_render_card_html(ticker: str, rep: Dict[str, Any], vars_count: int) -> str:
    tk = _norm_tk(ticker)
    perspectiva = str(rep.get("perspectiva_compra", "") or "moderada").strip().lower()
    pill = "MODERADA"
    if perspectiva == "forte":
        pill = "FORTE"
    elif perspectiva == "fraca":
        pill = "FRACA"

    resumo = str(rep.get("resumo", "") or "").strip()
    porque = str(rep.get("porque", "") or "").strip()

    pontos = rep.get("pontos_chave", []) or []
    riscos = rep.get("riscos_ou_alertas", []) or []
    sinais = rep.get("sinais_de_investimento_futuro", []) or []
    evs = rep.get("evidencias", []) or []
    meta = rep.get("_meta", {}) or {}
    used = int(meta.get("vars_uteis_usadas", vars_count) or vars_count)

    def _lis(items, n=6):
        out=[]
        for x in (items or [])[:n]:
            s=str(x).strip()
            if s:
                out.append(f"<li>{s}</li>")
        return "".join(out) if out else "<li>—</li>"

    ev_html = ""
    if evs:
        eparts = []
        for it in evs[:4]:
            trecho = str(it.get("trecho") or "").strip()
            obs = str(it.get("observacao") or "").strip()
            if trecho:
                eparts.append(f"<li><b>Trecho:</b> {trecho} <br/><span style='opacity:.8'>{obs}</span></li>")
        if eparts:
            ev_html = f"""
            <div class="p6x-box">
              <h4>Evidências (amostra)</h4>
              <ul>{''.join(eparts)}</ul>
            </div>
            """

    return f"""
    <div class="p6x-card">
      <div class="p6x-head">
        <div class="p6x-title">{tk}</div>
        <div class="p6x-pill">{pill}</div>
      </div>
      <div class="p6x-sub">{resumo or ""}</div>

      <div class="p6x-grid">
        <div class="p6x-box">
          <h4>Pontos-chave</h4>
          <ul>{_lis(pontos, 6)}</ul>
        </div>
        <div class="p6x-box">
          <h4>Riscos/alertas</h4>
          <ul>{_lis(riscos, 6)}</ul>
        </div>
        <div class="p6x-box">
          <h4>Sinais de investimento futuro</h4>
          <ul>{_lis(sinais, 6)}</ul>
        </div>
        {ev_html}
      </div>

      <div class="p6x-divider"></div>
      <div class="p6x-why">{porque}</div>
      <div class="p6x-meta">Variáveis úteis usadas: <b>{used}</b> (disponíveis no banco: {int(vars_count)})</div>
    </div>
    """
