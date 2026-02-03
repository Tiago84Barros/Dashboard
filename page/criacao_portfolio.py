# ============================================================
# page/criacao_portfolio.py
# (MODELO BASE + PATCHES 1 A 7 EMBUTIDOS)
# ============================================================

from __future__ import annotations

import time
import math
import json
import hashlib
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

# ============================================================
# Imports internos do projeto (mantidos)
# ============================================================

from core.db_loader import (
    load_setores_from_db,
    load_data_from_db,
    load_multiplos_from_db,
)

from core.helpers import (
    obter_setor_da_empresa,
    determinar_lideres,
)

from core.scoring import (
    calcular_score_acumulado,
    penalizar_plato,
)

# ============================================================
# Utilitários internos
# ============================================================

def _norm_tk(t: str) -> str:
    return (t or "").upper().replace(".SA", "").strip()

def _safe_df(df):
    return df if isinstance(df, pd.DataFrame) else pd.DataFrame()

# ============================================================
# PATCH 1 — Régua de Convicção
# ============================================================

def patch1_regua_conviccao(df: pd.DataFrame):
    st.subheader("📏 Patch 1 — Régua de Convicção")

    if df.empty or "score" not in df.columns:
        st.info("Dados insuficientes para aplicar a régua.")
        return

    bins = [-np.inf, -0.5, 0.5, np.inf]
    labels = ["Baixa", "Média", "Alta"]

    df["conviccao"] = pd.cut(df["score"], bins=bins, labels=labels)

    st.dataframe(df[["ticker", "score", "conviccao"]])

# ============================================================
# PATCH 2 — Dominância
# ============================================================

def patch2_dominancia(df: pd.DataFrame):
    st.subheader("🏆 Patch 2 — Dominância")

    if df.empty:
        st.info("Dados insuficientes.")
        return

    dom = df.groupby("segmento")["score"].agg(["mean", "std"])
    dom["dominancia"] = dom["mean"] / dom["std"].replace(0, np.nan)

    st.dataframe(dom)

# ============================================================
# PATCH 3 — Stress Test
# ============================================================

def patch3_stress_test(df: pd.DataFrame):
    st.subheader("⚠️ Patch 3 — Stress Test Simplificado")

    if df.empty or "score" not in df.columns:
        st.info("Dados insuficientes.")
        return

    stressed = df.copy()
    stressed["score_stress"] = stressed["score"] * 0.7

    st.dataframe(stressed[["ticker", "score", "score_stress"]])

# ============================================================
# PATCH 4 — Diversificação
# ============================================================

def patch4_diversificacao(df: pd.DataFrame):
    st.subheader("🌐 Patch 4 — Diversificação")

    if df.empty:
        st.info("Dados insuficientes.")
        return

    dist = df["segmento"].value_counts(normalize=True)
    st.bar_chart(dist)

# ============================================================
# PATCH 5 — Benchmark por Segmento
# ============================================================

def patch5_benchmark(df: pd.DataFrame):
    st.subheader("📊 Patch 5 — Benchmark Interno por Segmento")

    if df.empty:
        st.info("Dados insuficientes.")
        return

    bench = df.groupby("segmento")["score"].mean().sort_values(ascending=False)
    st.bar_chart(bench)

# ============================================================
# PATCH 6 — IA (Placeholder seguro)
# ============================================================

def patch6_ia_selecao(df: pd.DataFrame):
    st.subheader("🤖 Patch 6 — IA Seleção (Modo Seguro)")

    if df.empty:
        st.info("IA indisponível — sem dados.")
        return

    st.success("IA validaria riscos qualitativos e coerência setorial aqui.")

# ============================================================
# PATCH 7 — Evidências Externas (Placeholder)
# ============================================================

def patch7_evidencias(df: pd.DataFrame):
    st.subheader("🧠 Patch 7 — Evidências & Narrativa")

    if df.empty:
        st.info("Sem evidências externas disponíveis.")
        return

    st.write(
        "Este patch consolida notícias, riscos e catalisadores "
        "em uma narrativa explicável ao investidor."
    )

# ============================================================
# RENDER PRINCIPAL
# ============================================================

def render():
    st.title("📌 Criação de Portfólio — Modelo Integrado")

    setores = load_setores_from_db()
    if not setores:
        st.warning("Nenhum setor encontrado.")
        return

    score_global = []
    lideres_global = []

    for setor in setores:
        df = load_data_from_db(setor)
        df_mult = load_multiplos_from_db(setor)

        if df.empty:
            continue

        df_score = calcular_score_acumulado(df, df_mult)
        df_score = penalizar_plato(df_score)

        lideres = determinar_lideres(df_score)
        lideres["segmento"] = setor

        score_global.append(df_score)
        lideres_global.append(lideres)

    score_global = pd.concat(score_global, ignore_index=True) if score_global else pd.DataFrame()
    lideres_global = pd.concat(lideres_global, ignore_index=True) if lideres_global else pd.DataFrame()

    st.subheader("🏗️ Empresas Selecionadas")
    st.dataframe(lideres_global)

    st.markdown("---")
    st.header("🧩 Patches Avançados do Portfólio")

    patch1_regua_conviccao(lideres_global)
    patch2_dominancia(lideres_global)
    patch3_stress_test(lideres_global)
    patch4_diversificacao(lideres_global)
    patch5_benchmark(lideres_global)
    patch6_ia_selecao(lideres_global)
    patch7_evidencias(lideres_global)
