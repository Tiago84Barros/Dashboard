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

from core.ticker_utils import normalize_ticker


# ─────────────────────────────────────────────────────────────
# Helpers internos
# ─────────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────────
# Debug (Patch 5) — utilitário para stacktrace e inspeção
# Ative na sidebar: "Debug Patch 5 (variáveis)" e "Debug Patch 5 (stacktrace)"
# ─────────────────────────────────────────────────────────────
try:
    from core.debug_tools import dbg, show_trace  # type: ignore
except Exception:
    dbg = None  # type: ignore
    show_trace = None  # type: ignore

def _patch_debug_guard(patch_label: str):
    """Decorator: envolve o patch com try/except e (opcionalmente) imprime snapshots."""
    def _decorator(fn):
        def _wrapped(*args, **kwargs):
            import streamlit as st  # local import para evitar circular

            debug_vars = st.sidebar.toggle(f"Debug {patch_label} (variáveis)", value=False, key=f"dbg_{patch_label}_vars")
            debug_trace = st.sidebar.toggle(f"Debug {patch_label} (stacktrace)", value=False, key=f"dbg_{patch_label}_trace")

            try:
                if debug_vars and dbg is not None:
                    # snapshot genérico dos primeiros argumentos (útil para achar Series/DataFrame problemáticos)
                    for i, a in enumerate(list(args)[:6]):
                        dbg(f"{patch_label}.arg{i}", a)
                    for k, v in list(kwargs.items())[:6]:
                        dbg(f"{patch_label}.kw_{k}", v)

                return fn(*args, **kwargs)

            except Exception as e:
                if debug_trace and show_trace is not None:
                    show_trace(e, title=f"{patch_label} falhou (stacktrace)")
                else:
                    st.error(f"{patch_label} falhou: {type(e).__name__}: {e}")
                st.stop()
        return _wrapped
    return _decorator

def _norm_tk(t: str) -> str:
    return normalize_ticker(t)


def _strip_sa(ticker: str) -> str:
    return normalize_ticker(ticker)


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



def _normalize_df_date_cols(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame):
        return pd.DataFrame()
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    if "data" in out.columns and "Data" not in out.columns:
        out = out.rename(columns={"data": "Data"})
    return out

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


@_patch_debug_guard("Patch 5")
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
    return normalize_ticker(ticker)


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
# Patch 5 — Desempenho das Empresas (Preço/DY + Lucros)
# ─────────────────────────────────────────────────────────────
def _css_metric_cards() -> None:
    st.markdown(
        """
        <style>
        .metric-card {
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 14px;
            padding: 16px;
            box-shadow: 0 4px 18px rgba(0,0,0,0.25);
            margin-bottom: 14px;
        }
        .metric-title {
            font-weight: 900;
            font-size: 16px;
            margin-bottom: 10px;
            color: #ffffff;
        }
        .metric-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px 18px;
        }
        .metric-row {
            display: flex;
            justify-content: space-between;
            padding: 6px 0;
            border-bottom: 1px solid rgba(255,255,255,0.05);
            font-size: 13px;
        }
        .metric-label { color: #cfcfcf; }
        .metric-value { font-weight: 800; color: #ffffff; }
        .metric-note { color: rgba(255,255,255,0.7); font-size: 12px; margin-top: 8px; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _safe_last_number(df: pd.DataFrame, col_candidates: List[str]) -> Optional[float]:
    if df is None or df.empty:
        return None
    cols = set(df.columns)
    for c in col_candidates:
        if c in cols:
            s = pd.to_numeric(df[c], errors="coerce").dropna()
            if not s.empty:
                return float(s.iloc[-1])
    return None


def _get_net_income_from_supabase(ticker: str) -> Optional[float]:
    try:
        from core.ui_bridge import load_data_from_db, load_data_tri_from_db
    except Exception:
        return None

    # tenta anual e trimestral
    for loader in (load_data_from_db, load_data_tri_from_db):
        try:
            df = loader(ticker)
        except Exception:
            df = None
        val = _safe_last_number(
            df,
            col_candidates=[
                "Lucro_Liquido", "LUCRO_LIQUIDO", "lucro_liquido", "LucroLiquido",
                "Lucro Líquido", "LUCRO LIQUIDO", "resultado_liquido", "Resultado_Liquido",
            ],
        )
        if val is not None:
            return val
    return None


def _normalize_ratio_value(v: Any) -> Optional[float]:
    """Normaliza razão percentual para fração quando necessário."""
    fv = _safe_float(v)
    if fv is None:
        return None
    return fv / 100.0 if abs(fv) > 1.0 else fv

def _is_invalid_dy(v: Any) -> bool:
    fv = _safe_float(v)
    if fv is None:
        return True
    if not np.isfinite(fv):
        return True
    if fv <= 0:
        return True
    if fv > 0.50:  # sanity check: >50% provavelmente erro de base/escala
        return True
    return False

def _get_yf_div_series(ticker: str) -> pd.Series:
    """Retorna série histórica de dividendos por ação via Yahoo para um único ticker."""
    try:
        from core.yf_data import coletar_dividendos
        div_map = coletar_dividendos([ticker])
        if isinstance(div_map, dict):
            for key in (_strip_sa(ticker), str(ticker).upper(), f"{_strip_sa(ticker)}.SA"):
                s = div_map.get(key)
                if isinstance(s, pd.Series) and not s.empty:
                    s = pd.to_numeric(s, errors="coerce").dropna()
                    if not s.empty:
                        s.index = pd.to_datetime(s.index, errors="coerce")
                        s = s[~s.index.isna()].sort_index()
                        return s
    except Exception:
        pass
    return pd.Series(dtype="float64")


def _get_dy_from_sources(ticker: str, score_global: pd.DataFrame | None) -> Optional[float]:
    tk_norm = _strip_sa(ticker)

    # 1) score_global (se tiver DY), mas ignorando zero/negativo
    if isinstance(score_global, pd.DataFrame) and (not score_global.empty):
        sg = score_global.copy()
        if "ticker" in sg.columns:
            sg["ticker"] = sg["ticker"].astype(str).map(_strip_sa)
            sg = sg[sg["ticker"] == tk_norm]
            if not sg.empty:
                for c in ["DY", "Dividend Yield", "DIVIDEND_YIELD", "dividend_yield", "dy"]:
                    if c in sg.columns:
                        s = pd.to_numeric(sg[c], errors="coerce").dropna()
                        if not s.empty:
                            v = _normalize_ratio_value(s.iloc[-1])
                            if v is not None and v > 0:
                                return v

    # 2) dividendos 12m via yfinance
    try:
        s_div = _get_yf_div_series(ticker)
        if not s_div.empty:
            cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=365)
            vals = pd.to_numeric(s_div[s_div.index >= cutoff], errors="coerce").dropna()
            if not vals.empty:
                try:
                    from core.yf_data import get_price
                    px = get_price(ticker)
                    if px and px > 0:
                        dy = float(vals.sum() / px)
                        if dy > 0:
                            return dy
                except Exception:
                    pass
    except Exception:
        pass

    # 3) dividendYield (Yahoo fundamentals)
    try:
        from core.yf_data import get_fundamentals_yf
        yf = get_fundamentals_yf(ticker)
        if isinstance(yf, pd.DataFrame) and not yf.empty:
            row_yf = yf.iloc[0].to_dict()
        elif isinstance(yf, dict):
            row_yf = yf
        else:
            row_yf = {}

        for c in ["DY", "dividendYield", "dividend_yield"]:
            v = _normalize_ratio_value(row_yf.get(c))
            if v is not None and v > 0:
                return v
    except Exception:
        pass

    return None


def _get_prices_series(ticker: str, precos: Optional[pd.DataFrame]) -> Optional[pd.Series]:
    # tenta precos já fornecidos (df_prices_global)
    if isinstance(precos, pd.DataFrame) and (not precos.empty):
        cols = precos.columns
        if ticker in cols:
            s = pd.to_numeric(precos[ticker], errors="coerce").dropna()
            return s if not s.empty else None
        # tenta ticker normalizado
        for c in cols:
            if str(c).upper() == ticker.upper():
                s = pd.to_numeric(precos[c], errors="coerce").dropna()
                return s if not s.empty else None

    # fallback: baixar via yfinance
    try:
        from core.yf_data import baixar_precos
        start_dt = (pd.Timestamp.today().normalize() - pd.DateOffset(years=6)).strftime("%Y-%m-%d")
        df = baixar_precos([ticker], start=start_dt)
        if isinstance(df, pd.DataFrame) and (not df.empty):
            key = _strip_sa(ticker)
            if key in df.columns:
                s = pd.to_numeric(df[key], errors="coerce").dropna()
                return s if not s.empty else None
    except Exception:
        return None
    return None


def _get_dividend_slope_from_yf(ticker: str) -> Optional[float]:
    s_div = _get_yf_div_series(ticker)
    if s_div.empty:
        return None
    annual = pd.to_numeric(s_div, errors="coerce").dropna().groupby(s_div.index.year).sum()
    annual = annual[annual > 0]
    if annual.empty:
        return None
    try:
        return float(_slope_5y_from_annual(annual))
    except Exception:
        return None


def _calc_metrics_from_prices(prices: pd.Series) -> Dict[str, Optional[float]]:
    # Não usar Series como bool em hipótese nenhuma
    prices = pd.to_numeric(prices, errors="coerce").dropna()
    if prices is None or prices.empty or len(prices) < 30:
        return {"ret_12m": None, "cagr": None, "vol": None, "mdd": None}

    # daily returns
    rets = prices.pct_change().dropna()
    if rets.empty:
        return {"ret_12m": None, "cagr": None, "vol": None, "mdd": None}

    # 12m ~ 252 pregões
    if len(prices) >= 252:
        p0 = float(prices.iloc[-252])
    else:
        p0 = float(prices.iloc[0])
    p1 = float(prices.iloc[-1])
    ret_12m = (p1 / p0 - 1.0) if (p0 and p0 > 0) else None

    years = max(len(prices) / 252.0, 1e-9)
    cagr = (p1 / float(prices.iloc[0])) ** (1.0 / years) - 1.0 if float(prices.iloc[0]) > 0 else None

    vol = float(rets.std()) * (252.0 ** 0.5) if len(rets) > 2 else None

    acc = (1.0 + rets).cumprod()
    peak = acc.cummax()
    dd = acc / peak - 1.0
    mdd = float(dd.min()) if not dd.empty else None

    return {"ret_12m": ret_12m, "cagr": cagr, "vol": vol, "mdd": mdd}


def render_patch5_desempenho_empresas(
    score_global: pd.DataFrame,
    lideres_global: pd.DataFrame,
    empresas_lideres_finais: List[Dict[str, Any]],
    *,
    precos: Optional[pd.DataFrame] = None,
    max_empresas: int = 20,
) -> None:
    """
    Patch 5 — Painel (CSS estilo "cf-card") com indicadores de qualidade/estabilidade.

    Fonte primária: Supabase (via core.db_loader, no mesmo padrão do empresa_view.py).
    Fallback: yfinance (core.yf_data) quando faltar preço ou algum fundamental.

    Exibe:
      - ROIC médio (5a) [Supabase múltiplos]
      - Coef. Regr. Receita (5a) [Supabase demonstrações]
      - Coef. Regr. Lucro (5a) [Supabase demonstrações]
      - Coef. Regr. Dividendos (5a) [Supabase demonstrações]
      - DY médio (5a) [Supabase múltiplos]
      - Dívida Líq./EBITDA [Supabase demonstrações]
      - Valorização do preço (12m) [yfinance/preços]
      - Coef. Regr. do preço (5a) [yfinance/preços]
      - Volatilidade (12m, a.a.) [yfinance/preços]
      - Máxima queda (5a) [yfinance/preços]  (termo em PT, sem “drawdown”)
    """

    import numpy as np
    import pandas as pd
    import streamlit as st

    # DB/YF loaders (mesmo padrão do empresa_view.py)
    from core.ui_bridge import load_data_from_db, load_multiplos_limitado_from_db
    from core.yf_data import get_fundamentals_yf

    try:
        from core.yf_data import baixar_precos  # type: ignore
    except Exception:
        baixar_precos = None  # type: ignore

    # ------------------------- CSS (padrão cf-*) -------------------------
    st.markdown(
        """
        <style>
          .cf-header{
            display:flex; justify-content:space-between; align-items:flex-start;
            padding: 6px 0 6px 0;
          }
          .cf-title{ margin:0; font-size: 26px; line-height: 1.1; }
          .cf-subtitle{ margin:8px 0 0 0; opacity:.85; font-size: 13px; }

          .cf-pill{
            display:inline-block;
            padding: 8px 12px;
            border-radius: 999px;
            border: 1px solid rgba(255,255,255,.14);
            background: rgba(255,255,255,.06);
            font-size: 12px;
            opacity: .95;
          }

          .cf-card{
            border-radius: 18px;
            padding: 14px 16px;
            border: 1px solid rgba(255,255,255,0.14);
            background: rgba(255,255,255,0.05);
            box-shadow: 0 0 0 1px rgba(255,255,255,0.06) inset;
            min-height: 112px;
            margin-bottom: 10px;
          }
          .cf-card-label{
            font-size: 11px;
            letter-spacing: .10em;
            text-transform: uppercase;
            opacity: .85;
            margin-bottom: 6px;
          }
          .cf-card-value{
            font-size: 26px;
            font-weight: 850;
            line-height: 1.05;
            margin-bottom: 6px;
          }
          .cf-card-extra{
            font-size: 12px;
            opacity: .85;
            line-height: 1.25;
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
            overflow: hidden;
          }

          .cf-card-income{ background: rgba(59,130,246,0.12); border-color: rgba(59,130,246,0.30); }
          .cf-card-expense{ background: rgba(245,158,11,0.12); border-color: rgba(245,158,11,0.30); }
          .cf-card-ratio{ background: rgba(148,163,184,0.10); border-color: rgba(148,163,184,0.24); }
          .cf-card-balance-positive{ background: rgba(34,197,94,0.12); border-color: rgba(34,197,94,0.30); }
          .cf-card-balance-negative{ background: rgba(239,68,68,0.12); border-color: rgba(239,68,68,0.30); }

          /* Mobile tweaks */
          @media (max-width: 480px){
            .cf-title{ font-size: 22px; }
            .cf-card{ padding: 12px 12px; min-height: 108px; }
            .cf-card-value{ font-size: 24px; }
          }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # ------------------------- guards -------------------------
    if not empresas_lideres_finais:
        st.info("Sem líderes finais — Patch 5 não tem o que mostrar.")
        return

    # ------------------------- format helpers -------------------------
    def _is_nan(x) -> bool:
        try:
            return x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x)))
        except Exception:
            return True

    def _fmt_pct(x, signed=False) -> str:
        if _is_nan(x):
            return "-"
        v = float(x) * 100.0
        return f"{v:+.2f}%" if signed else f"{v:.2f}%"

    def _fmt_num(x) -> str:
        if _is_nan(x):
            return "-"
        return f"{float(x):.2f}"

    def _fmt_coef(x) -> str:
        if _is_nan(x):
            return "-"
        return f"{float(x):+.4f}"

    def _fmt_short(x) -> str:
        # compacto para "Dívida Líq./EBITDA"
        if _is_nan(x):
            return "-"
        v = float(x)
        return f"{v:.2f}x"

    def _cls_posneg(x) -> str:
        if _is_nan(x):
            return "cf-card-ratio"
        return "cf-card-balance-positive" if float(x) >= 0 else "cf-card-balance-negative"

    def _cls_lowhigh(x, invert=False) -> str:
        # para risco: menor melhor => invert=True
        if _is_nan(x):
            return "cf-card-ratio"
        v = float(x)
        if invert:
            v = -v
        return "cf-card-balance-positive" if v >= 0 else "cf-card-balance-negative"

    # ------------------------- price helpers -------------------------
    def _ensure_prices(df: Optional[pd.DataFrame]) -> pd.DataFrame:
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return pd.DataFrame()
        out = df.copy()
        out.index = pd.to_datetime(out.index, errors="coerce")
        out = out[~out.index.isna()].sort_index()
        out.columns = [_strip_sa(str(c)) for c in out.columns.astype(str).tolist()]
        out = out.dropna(how="all", axis=0).dropna(how="all", axis=1)
        return out

    def _get_price_series(ticker: str) -> pd.Series:
        tk = _strip_sa(ticker)
        # 1) usa 'precos' já fornecido
        dpx = _ensure_prices(precos)
        if not dpx.empty and tk in dpx.columns:
            s = pd.to_numeric(dpx[tk], errors="coerce").dropna()
            s.index = pd.to_datetime(s.index, errors="coerce")
            s = s.dropna().sort_index()
            return s

        # 2) fallback: yfinance
        if baixar_precos is None:
            return pd.Series(dtype="float64")
        try:
            dfp = baixar_precos(tk, start="2010-01-01")
            if dfp is None or dfp.empty:
                return pd.Series(dtype="float64")
            col = tk
            if col not in dfp.columns:
                col = dfp.columns[0]
            s = pd.to_numeric(dfp[col], errors="coerce").dropna()
            s.index = pd.to_datetime(s.index, errors="coerce")
            s = s.dropna().sort_index()
            return s
        except Exception:
            return pd.Series(dtype="float64")

    def _retorno_12m(price: pd.Series) -> float:
        s = pd.to_numeric(price, errors="coerce").dropna()
        if s.shape[0] < 2:
            return np.nan
        if s.shape[0] >= 253:
            base = float(s.iloc[-253])
        else:
            base = float(s.iloc[0])
        if base == 0:
            return np.nan
        return float(s.iloc[-1] / base - 1.0)

    def _vol_12m(price: pd.Series) -> float:
        s = pd.to_numeric(price, errors="coerce").dropna()
        if s.shape[0] < 30:
            return np.nan
        r = s.pct_change().dropna().tail(252)
        if r.empty:
            return np.nan
        return float(r.std() * np.sqrt(252))

    def _slope_price_5a(price: pd.Series) -> float:
        s = pd.to_numeric(price, errors="coerce").dropna()
        if s.shape[0] < 60:
            return np.nan
        if isinstance(s.index, pd.DatetimeIndex):
            s = s.sort_index().resample("ME").last().dropna()
        if s.shape[0] < 24:
            return np.nan
        if s.shape[0] > 60:
            s = s.tail(60)
        s = s[s > 0]
        if s.shape[0] < 24:
            return np.nan
        x = np.arange(s.shape[0], dtype=float)
        y = np.log(s.astype(float).values)
        return _theil_sen_slope(x, y)

    def _maxima_queda_5a(price: pd.Series) -> float:
        s = pd.to_numeric(price, errors="coerce").dropna()
        if s.empty:
            return np.nan
        # janela ~5a se existir
        if s.shape[0] >= 252 * 5 + 1:
            s = s.tail(252 * 5 + 1)
        peak = s.cummax()
        dd = (s / peak) - 1.0
        return float(dd.min())  # negativo

    # ------------------------- financial helpers (Supabase) -------------------------
    def _prep_financial(df_fin: pd.DataFrame) -> pd.DataFrame:
        d = _normalize_df_date_cols(df_fin)
        if d is None or d.empty or "Data" not in d.columns:
            return pd.DataFrame()
        d["Data"] = pd.to_datetime(d["Data"], errors="coerce")
        d = d.dropna(subset=["Data"]).sort_values("Data")
        return d

    def _annual_series(df_fin: pd.DataFrame, col: str, how: str = "sum") -> pd.Series:
        df_fin = _normalize_df_date_cols(df_fin)
        if df_fin is None or df_fin.empty or col not in df_fin.columns or "Data" not in df_fin.columns:
            return pd.Series(dtype="float64")
        d = df_fin[["Data", col]].copy()
        d["Data"] = pd.to_datetime(d["Data"], errors="coerce")
        d[col] = pd.to_numeric(d[col], errors="coerce")
        d = d.dropna(subset=["Data", col])
        if d.empty:
            return pd.Series(dtype="float64")
        d["Ano"] = d["Data"].dt.year.astype(int)
        g = d.groupby("Ano")[col]
        s = (g.sum() if how == "sum" else g.mean()).sort_index()
        return pd.to_numeric(s, errors="coerce").dropna()

    def _theil_sen_slope(x: np.ndarray, y: np.ndarray) -> float:
        try:
            from scipy.stats import theilslopes
            slope = theilslopes(y, x)[0]
            return float(slope)
        except Exception:
            slope, _ = np.polyfit(x, y, 1)
            return float(slope)

    def _slope_5y_from_annual(s: pd.Series) -> float:
        if s is None or s.empty:
            return np.nan
        s = pd.to_numeric(s, errors="coerce").dropna()
        if s.empty:
            return np.nan
        s = s[s > 0]
        if s.shape[0] < 3:
            return np.nan
        last = s.tail(6)
        if last.shape[0] < 3:
            return np.nan
        x = np.arange(last.shape[0], dtype=float)
        y = np.log(last.astype(float).values)
        return _theil_sen_slope(x, y)

    def _mean_5y_from_mult(df_mult: pd.DataFrame, col: str) -> float:
        d = _normalize_df_date_cols(df_mult)
        if d is None or d.empty:
            return np.nan
    
        if "Data" in d.columns:
            d["Data"] = pd.to_datetime(d["Data"], errors="coerce")
            d = d.dropna(subset=["Data"]).sort_values("Data")
            d["Ano"] = d["Data"].dt.year.astype("Int64")
        elif "Ano" not in d.columns:
            return np.nan
    
        if col not in d.columns:
            return np.nan
    
        d[col] = pd.to_numeric(d[col], errors="coerce")
        d = d.dropna(subset=[col])
        if d.empty:
            return np.nan
    
        vals = d[col].copy()
    
        # Normalização somente para ROIC
        if col.upper() == "ROIC":
            med = float(vals.abs().median()) if not vals.empty else np.nan
            if np.isfinite(med) and med > 1.0:
                vals = vals / 100.0
    
        d[col] = vals
    
        if "Ano" in d.columns:
            byy = d.groupby("Ano")[col].mean().dropna().sort_index().tail(5)
            if byy.empty:
                return np.nan
            v = float(byy.mean())
        else:
            v = float(d[col].tail(5).mean())
    
        # sanity check apenas para ROIC
        if col.upper() == "ROIC":
            if not np.isfinite(v):
                return np.nan
            if v < -1.0 or v > 3.0:
                return np.nan
    
        return v

    def _latest_ratio_dl_ebitda(df_fin: pd.DataFrame) -> float:
        # usa últimos valores disponíveis de Divida_Liquida e EBITDA
        df_fin = _normalize_df_date_cols(df_fin)
        if df_fin is None or df_fin.empty:
            return np.nan
        if "Divida_Liquida" not in df_fin.columns or "EBITDA" not in df_fin.columns or "Data" not in df_fin.columns:
            return np.nan
        d = df_fin[["Data", "Divida_Liquida", "EBITDA"]].copy()
        d["Data"] = pd.to_datetime(d["Data"], errors="coerce")
        d["Divida_Liquida"] = pd.to_numeric(d["Divida_Liquida"], errors="coerce")
        d["EBITDA"] = pd.to_numeric(d["EBITDA"], errors="coerce")
        d = d.dropna(subset=["Data", "Divida_Liquida", "EBITDA"]).sort_values("Data")
        if d.empty:
            return np.nan
        dl = float(d["Divida_Liquida"].iloc[-1])
        eb = float(d["EBITDA"].iloc[-1])
        if eb == 0:
            return np.nan
        return float(dl / eb)

    # ------------------------- build tickers list -------------------------
    tickers = []
    for e in empresas_lideres_finais:
        tk = str(e.get("ticker", "")).strip()
        if tk:
            tickers.append(_strip_sa(tk))
    tickers = list(dict.fromkeys(tickers))[: max_empresas]

    # ------------------------- header -------------------------
    st.markdown(
        f"""
        <div class="cf-header">
            <div>
                <h1 class="cf-title">🏢 Patch 5 • Qualidade das Empresas</h1>
                <p class="cf-subtitle">
                    Indicadores a partir do <strong>Supabase</strong> (financeiros e múltiplos) com fallback para <strong>yfinance</strong> (preços).
                </p>
            </div>
            <div>
                <span class="cf-pill">Janela: 5 anos (quando houver)</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ------------------------- compute metrics -------------------------
    rows = []
    for tk in tickers:
        nome = _get_nome(tk, empresas_lideres_finais)
        # Supabase
        df_fin = _prep_financial(load_data_from_db(tk))
        df_mult = _normalize_df_date_cols(load_multiplos_limitado_from_db(tk, limite=60))

        # métricas supabase
        roic_5a = _mean_5y_from_mult(df_mult, "ROIC") if isinstance(df_mult, pd.DataFrame) else np.nan
        dy_5a = _mean_5y_from_mult(df_mult, "DY") if isinstance(df_mult, pd.DataFrame) else np.nan

        slope_receita_5a = np.nan
        slope_lucro_5a = np.nan
        slope_dividendos_5a = np.nan
        dl_ebitda = np.nan

        if not df_fin.empty:
            s_rev = _annual_series(df_fin, "Receita_Liquida", how="sum")
            s_luc = _annual_series(df_fin, "Lucro_Liquido", how="sum")
            s_div = _annual_series(df_fin, "Dividendos", how="sum")

            slope_receita_5a = _slope_5y_from_annual(s_rev)
            slope_lucro_5a = _slope_5y_from_annual(s_luc)
            slope_dividendos_5a = _slope_5y_from_annual(s_div)
            dl_ebitda = _latest_ratio_dl_ebitda(df_fin)

        # fallback de fundamentos / dividendos (quando Supabase estiver vazio ou incompleto)
        if (_is_nan(roic_5a) or _is_invalid_dy(dy_5a) or _is_nan(slope_dividendos_5a)) or (df_fin.empty):
            try:
                yf = get_fundamentals_yf(tk)
                if isinstance(yf, pd.DataFrame) and not yf.empty:
                    row_yf = yf.iloc[0].to_dict()
                elif isinstance(yf, dict):
                    row_yf = yf
                else:
                    row_yf = {}
        
                if _is_nan(roic_5a):
                    v = row_yf.get("ROIC") or row_yf.get("roic")
                    if isinstance(v, (int, float)):
                        roic_5a = float(v) / 100.0 if abs(float(v)) > 1 else float(v)
        
                if _is_invalid_dy(dy_5a):
                    v = row_yf.get("DY") or row_yf.get("dividendYield") or row_yf.get("dividend_yield")
                    if isinstance(v, (int, float)):
                        dy_5a = float(v) / 100.0 if abs(float(v)) > 1 else float(v)
        
                if _is_invalid_dy(dy_5a):
                    v = _get_dy_from_sources(tk, score_global if isinstance(score_global, pd.DataFrame) else None)
                    if v is not None and v > 0:
                        dy_5a = float(v)
        
                if _is_invalid_dy(dy_5a):
                    dy_5a = np.nan
        
                if _is_nan(slope_dividendos_5a):
                    v = _get_dividend_slope_from_yf(tk)
                    if v is not None:
                        slope_dividendos_5a = float(v)
            except Exception:
                pass
        # preços
        price = _get_price_series(tk)
        ret_12m = _retorno_12m(price) if not price.empty else np.nan
        slope_preco_5a = _slope_price_5a(price) if not price.empty else np.nan
        vol_12m = _vol_12m(price) if not price.empty else np.nan
        max_queda_5a = _maxima_queda_5a(price) if not price.empty else np.nan

        # score interno apenas para ordenar (não exibido)
        # (robusto: rank pct, invertendo onde menor é melhor)
        rows.append(
            dict(
                ticker=tk,
                nome=nome,
                roic_5a=roic_5a,
                slope_receita_5a=slope_receita_5a,
                slope_lucro_5a=slope_lucro_5a,
                slope_dividendos_5a=slope_dividendos_5a,
                dy_5a=dy_5a,
                dl_ebitda=dl_ebitda,
                ret_12m=ret_12m,
                slope_preco_5a=slope_preco_5a,
                vol_12m=vol_12m,
                max_queda_5a=max_queda_5a,
            )
        )

    dfm = pd.DataFrame(rows)
    if dfm.empty:
        st.info("Sem dados suficientes para exibir o Patch 5.")
        return

    # ------------------------- ordenação (sem exibir score) -------------------------
    # Normaliza via percentil (0..1). Onde menor é melhor, usa 1 - pct.
    def _pct_rank(s: pd.Series, invert: bool = False) -> pd.Series:
        s2 = pd.to_numeric(s, errors="coerce")
        pct = s2.rank(pct=True)
        return (1.0 - pct) if invert else pct

    # pesos simples e estáveis
    w = {
        "roic_5a": 0.18,
        "dy_5a": 0.16,
        "slope_dividendos_5a": 0.10,
        "slope_receita_5a": 0.12,
        "slope_lucro_5a": 0.12,
        "dl_ebitda": 0.10,      # menor melhor
        "vol_12m": 0.10,        # menor melhor
        "max_queda_5a": 0.08,   # menor (mais negativo) pior => invert=True usando o próprio valor (negativo)
        "slope_preco_5a": 0.04,
    }

    score_ord = (
        _pct_rank(dfm["roic_5a"]) * w["roic_5a"]
        + _pct_rank(dfm["dy_5a"]) * w["dy_5a"]
        + _pct_rank(dfm["slope_dividendos_5a"]) * w["slope_dividendos_5a"]
        + _pct_rank(dfm["slope_receita_5a"]) * w["slope_receita_5a"]
        + _pct_rank(dfm["slope_lucro_5a"]) * w["slope_lucro_5a"]
        + _pct_rank(dfm["dl_ebitda"], invert=True) * w["dl_ebitda"]
        + _pct_rank(dfm["vol_12m"], invert=True) * w["vol_12m"]
        + _pct_rank(dfm["max_queda_5a"], invert=True) * w["max_queda_5a"]
        + _pct_rank(dfm["slope_preco_5a"]) * w["slope_preco_5a"]
    )

    dfm["_ord"] = score_ord.fillna(-1.0)
    dfm = dfm.sort_values(["_ord", "ticker"], ascending=[False, True]).reset_index(drop=True)

    # ------------------------- render cards -------------------------
    for i, r in dfm.iterrows():
        rank = i + 1
        medal = "🥇" if rank == 1 else ("🥈" if rank == 2 else ("🥉" if rank == 3 else "🏅"))

        st.markdown(
            f"""
            <div style="margin-top:10px;margin-bottom:6px;">
              <div style="font-size:14px;opacity:.85">{medal} <b>#{rank}</b></div>
              <div style="font-size:22px;font-weight:900;line-height:1.12;margin-top:2px">{r['ticker']} — {r['nome']}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Linha 1 — Qualidade
        c1, c2, c3, c4 = st.columns(4)
        c1.markdown(
            f"""
            <div class="cf-card cf-card-income">
                <div class="cf-card-label">ROIC médio (5a)</div>
                <div class="cf-card-value">{_fmt_pct(r['roic_5a'])}</div>
                <div class="cf-card-extra">Eficiência do capital (média 5 anos).</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        c2.markdown(
            f"""
            <div class="cf-card cf-card-expense">
                <div class="cf-card-label">DY médio (5a)</div>
                <div class="cf-card-value">{_fmt_pct(r['dy_5a'])}</div>
                <div class="cf-card-extra">Dividend Yield médio (múltiplos do banco).</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        c3.markdown(
            f"""
            <div class="cf-card {_cls_posneg(r['slope_dividendos_5a'])}">
                <div class="cf-card-label">Coef. Regr. Dividendos (5a)</div>
                <div class="cf-card-value">{_fmt_coef(r['slope_dividendos_5a'])}</div>
                <div class="cf-card-extra">Inclinação log-linear robusta dos dividendos.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        c4.markdown(
            f"""
            <div class="cf-card cf-card-ratio">
                <div class="cf-card-label">Dívida Líq./EBITDA</div>
                <div class="cf-card-value">{_fmt_short(r['dl_ebitda'])}</div>
                <div class="cf-card-extra">Último disponível no Supabase.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Linha 2 — Crescimento
        c1, c2, c3, c4 = st.columns(4)
        c1.markdown(
            f"""
            <div class="cf-card {_cls_posneg(r['slope_receita_5a'])}">
                <div class="cf-card-label">Coef. Regr. Receita (5a)</div>
                <div class="cf-card-value">{_fmt_coef(r['slope_receita_5a'])}</div>
                <div class="cf-card-extra">Inclinação log-linear robusta da receita.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        c2.markdown(
            f"""
            <div class="cf-card {_cls_posneg(r['slope_lucro_5a'])}">
                <div class="cf-card-label">Coef. Regr. Lucro (5a)</div>
                <div class="cf-card-value">{_fmt_coef(r['slope_lucro_5a'])}</div>
                <div class="cf-card-extra">Inclinação log-linear robusta do lucro.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        c3.markdown(
            f"""
            <div class="cf-card {_cls_posneg(r['ret_12m'])}">
                <div class="cf-card-label">Valorização do preço (12m)</div>
                <div class="cf-card-value">{_fmt_pct(r['ret_12m'], signed=True)}</div>
                <div class="cf-card-extra">Retorno do preço em ~12 meses.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        c4.markdown(
            f"""
            <div class="cf-card {_cls_posneg(r['slope_preco_5a'])}">
                <div class="cf-card-label">Coef. Regr. do preço (5a)</div>
                <div class="cf-card-value">{_fmt_coef(r['slope_preco_5a'])}</div>
                <div class="cf-card-extra">Inclinação log-linear robusta do preço.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Linha 3 — Risco
        c1, c2, c3 = st.columns(3)
        c1.markdown(
            f"""
            <div class="cf-card cf-card-ratio">
                <div class="cf-card-label">Volatilidade (12m, a.a.)</div>
                <div class="cf-card-value">{_fmt_pct(r['vol_12m'])}</div>
                <div class="cf-card-extra">Desvio padrão anualizado do preço.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        c2.markdown(
            f"""
            <div class="cf-card cf-card-ratio">
                <div class="cf-card-label">Máxima queda (5a)</div>
                <div class="cf-card-value">{_fmt_pct(r['max_queda_5a'], signed=True)}</div>
                <div class="cf-card-extra">Pior queda do preço no período.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        c3.markdown(
            f"""
            <div class="cf-card cf-card-ratio">
                <div class="cf-card-label">Fonte</div>
                <div class="cf-card-value">DB + YF</div>
                <div class="cf-card-extra">Supabase (primário) + yfinance (fallback).</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown("<hr style='border:0;border-top:1px solid rgba(255,255,255,.08);margin: 8px 0 14px 0;'>", unsafe_allow_html=True)

    st.caption(
        "Notas: coeficientes de regressão são inclinações log-lineares robustas (Theil-Sen). Volatilidade 12m é anualizada (≈252 pregões). "
        "Máxima queda (5a) é o pior recuo do preço no período. "
        "Quando faltarem dados no banco, o patch usa fallback do yfinance."
    )
