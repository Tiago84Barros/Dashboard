# auditoria_dados/ingest_quality.py
#
# Auditoria profunda de qualidade real da ingestão.
# Foco em: fidelidade, completude, consistência semântica, perda silenciosa.
#
# Uso:
#   python -m auditoria_dados.ingest_quality [--pipeline NOME] [--all] [--score-only]
#
# Pipelines:
#   dfp          -- Demonstrações Financeiras anuais
#   tri          -- Demonstrações Financeiras trimestrais
#   multiplos    -- Múltiplos DFP (anuais)
#   multiplos_tri-- Múltiplos ITR (trimestrais)
#   macro        -- Indicadores macroeconômicos BCB/SGS
#   docs         -- Documentos corporativos + chunks
#   patch6       -- Runs do Patch6 / LLM
#
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from dataclasses import dataclass, field
from datetime import date
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, text


# ─────────────────────────────────────────────────────────────────────────────
# DB
# ─────────────────────────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def get_engine():
    url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("Defina SUPABASE_DB_URL ou DATABASE_URL.")
    return create_engine(url, pool_pre_ping=True)


def q(sql: str, params: dict | None = None) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params or {})


# ─────────────────────────────────────────────────────────────────────────────
# Resultado de pipeline
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class PipelineQuality:
    pipeline: str
    source: str
    destination: str

    # Dimensões (0-100 cada)
    completeness: float = 0.0      # cobertura temporal e por ticker
    fidelity: float = 0.0          # fidelidade semântica da transformação
    consistency: float = 0.0       # consistência interna dos dados
    traceability: float = 0.0      # rastreabilidade / observabilidade
    coverage: float = 0.0          # cobertura por ticker vs. esperado

    findings: List[str] = field(default_factory=list)    # problemas encontrados
    warnings: List[str] = field(default_factory=list)    # alertas
    ok_points: List[str] = field(default_factory=list)   # pontos positivos

    @property
    def score(self) -> float:
        dims = [self.completeness, self.fidelity, self.consistency,
                self.traceability, self.coverage]
        weights = [0.25, 0.25, 0.20, 0.15, 0.15]
        return round(sum(d * w for d, w in zip(dims, weights)), 1)

    @property
    def status(self) -> str:
        s = self.score
        if s >= 75:
            return "SAUDÁVEL"
        if s >= 55:
            return "ATENÇÃO"
        return "CRÍTICO"

    def add_finding(self, msg: str) -> None:
        self.findings.append(msg)

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)

    def add_ok(self, msg: str) -> None:
        self.ok_points.append(msg)

    def print_report(self) -> None:
        bar = "─" * 68
        print(f"\n{bar}")
        print(f"  {self.pipeline.upper()} │ {self.source} → {self.destination}")
        print(f"  Score: {self.score}/100  [{self.status}]")
        print(f"  Completude:{self.completeness:5.1f}  Fidelidade:{self.fidelity:5.1f}  "
              f"Consistência:{self.consistency:5.1f}  Rastreab.:{self.traceability:5.1f}  Cobertura:{self.coverage:5.1f}")
        print(bar)
        for f in self.findings:
            print(f"  [PROBLEMA] {f}")
        for w in self.warnings:
            print(f"  [ALERTA]   {w}")
        for o in self.ok_points:
            print(f"  [OK]       {o}")
        if not self.findings and not self.warnings:
            print("  Nenhum problema detectável via banco.")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def pct(num: float, den: float) -> float:
    return round(100 * num / den, 1) if den > 0 else 0.0


def score_from_pct(pct_val: float, *, good_threshold: float = 90.0, bad_threshold: float = 60.0) -> float:
    """Map a percentage to 0-100 score."""
    if pct_val >= good_threshold:
        return 100.0
    if pct_val <= bad_threshold:
        return 0.0
    return round(100.0 * (pct_val - bad_threshold) / (good_threshold - bad_threshold), 1)


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE 1 — DFP (Demonstrações Financeiras anuais)
# ─────────────────────────────────────────────────────────────────────────────

def audit_dfp() -> PipelineQuality:
    pq = PipelineQuality(
        pipeline="dfp",
        source="CVM Open Data (DFP ZIPs)",
        destination='public."Demonstracoes_Financeiras"',
    )

    try:
        df = q("""
            SELECT
                "Ticker",
                COUNT(*) AS n_anos,
                MIN("Data") AS primeiro,
                MAX("Data") AS ultimo,
                SUM(CASE WHEN "Receita_Liquida" = 0 THEN 1 ELSE 0 END) AS n_receita_zero,
                SUM(CASE WHEN "Lucro_Liquido" = 0 THEN 1 ELSE 0 END) AS n_lucro_zero,
                SUM(CASE WHEN "Patrimonio_Liquido" = 0 THEN 1 ELSE 0 END) AS n_pl_zero,
                SUM(CASE WHEN "EBIT" = 0 THEN 1 ELSE 0 END) AS n_ebit_zero,
                SUM(CASE WHEN "LPA" = 0 THEN 1 ELSE 0 END) AS n_lpa_zero,
                SUM(CASE WHEN "Ativo_Total" <= 0 THEN 1 ELSE 0 END) AS n_ativo_neg,
                SUM(CASE WHEN "Passivo_Total" < 0 THEN 1 ELSE 0 END) AS n_passivo_neg
            FROM public."Demonstracoes_Financeiras"
            GROUP BY "Ticker"
        """)
    except Exception as e:
        pq.add_finding(f"Erro ao carregar Demonstracoes_Financeiras: {e}")
        return pq

    if df.empty:
        pq.add_finding("Tabela Demonstracoes_Financeiras está vazia.")
        return pq

    total_tickers = len(df)
    total_rows = df["n_anos"].sum()

    # ── COMPLETUDE ──────────────────────────────────────────────────────────

    # Anos esperados por ticker (primeiro ao último, sem lacunas)
    df["primeiro_ano"] = pd.to_datetime(df["primeiro"]).dt.year
    df["ultimo_ano"] = pd.to_datetime(df["ultimo"]).dt.year
    df["anos_esperados"] = df["ultimo_ano"] - df["primeiro_ano"] + 1
    df["cobertura_temporal"] = df["n_anos"] / df["anos_esperados"].clip(lower=1)

    tickers_lacuna = df[df["cobertura_temporal"] < 1.0]
    tickers_ok = df[df["cobertura_temporal"] == 1.0]
    cobertura_media = pct(len(tickers_ok), total_tickers)
    pq.completeness = score_from_pct(cobertura_media, good_threshold=85, bad_threshold=60)

    if len(tickers_lacuna) > 0:
        pq.add_warning(
            f"{len(tickers_lacuna)}/{total_tickers} tickers com lacunas temporais. "
            f"(ex: {tickers_lacuna['Ticker'].head(5).tolist()})"
        )
    else:
        pq.add_ok(f"Todos os {total_tickers} tickers com séries temporais contínuas.")

    # ── FIDELIDADE (qualidade da transformação) ──────────────────────────────

    fid_issues = 0

    # Receita Líquida = 0 em mais de 50% dos anos → suspeito
    df["pct_receita_zero"] = df["n_receita_zero"] / df["n_anos"]
    suspeitos_receita = df[df["pct_receita_zero"] > 0.5]
    if not suspeitos_receita.empty:
        pq.add_finding(
            f"{len(suspeitos_receita)} tickers com Receita_Liquida=0 em >50% dos períodos "
            f"(ex: {suspeitos_receita['Ticker'].head(5).tolist()}) — "
            f"possível falha na seleção de CD_CONTA 3.01 ou conta não encontrada."
        )
        fid_issues += 2

    # Passivo_Total < 0 (resultado de Passivo_Total = BPP_total - PL — pode ser correto mas merece alerta)
    n_passivo_neg = int(df["n_passivo_neg"].sum())
    if n_passivo_neg > 0:
        pct_neg = pct(n_passivo_neg, int(total_rows))
        pq.add_warning(
            f"{n_passivo_neg} linhas com Passivo_Total < 0 ({pct_neg}% do total). "
            f"Resultado do cálculo Passivo_Total = CD2(BPP) - PL. "
            f"Pode indicar empresas onde CD_CONTA '2' inclui PL — revisar lógica de subtração."
        )
        if pct_neg > 5:
            fid_issues += 1

    # Ativo_Total = 0 (deveria nunca ser zero para empresa ativa)
    n_ativo_neg = int(df["n_ativo_neg"].sum())
    if n_ativo_neg > 0:
        pq.add_finding(
            f"{n_ativo_neg} linhas com Ativo_Total ≤ 0 — dado claramente inconsistente."
        )
        fid_issues += 2

    # LPA = 0 em >70% do histórico por ticker → problema de normalização
    df["pct_lpa_zero"] = df["n_lpa_zero"] / df["n_anos"]
    lpa_zero_suspeito = df[df["pct_lpa_zero"] > 0.7]
    if not lpa_zero_suspeito.empty:
        pq.add_warning(
            f"{len(lpa_zero_suspeito)} tickers com LPA=0 em >70% dos períodos. "
            f"Pode ser resultado da normalização iterativa (/1000) ou ausência do CD_CONTA 3.99.01.01 no CVM."
        )
        fid_issues += 1

    pq.fidelity = max(0.0, 100.0 - fid_issues * 15)

    # ── CONSISTÊNCIA SEMÂNTICA ───────────────────────────────────────────────

    cons_issues = 0

    # Patrimônio Líquido = 0 quando Lucro ≠ 0 → inconsistente
    df_cross = q("""
        SELECT COUNT(*) as cnt
        FROM public."Demonstracoes_Financeiras"
        WHERE "Patrimonio_Liquido" = 0
          AND "Lucro_Liquido" != 0
          AND "Ativo_Total" > 0
    """)
    n_pl_inconsist = int(df_cross["cnt"].iloc[0])
    if n_pl_inconsist > 0:
        pq.add_warning(
            f"{n_pl_inconsist} linhas com PL=0 e Lucro≠0 e Ativo>0 — "
            f"indicativo de que Patrimônio Líquido não foi encontrado na DFP (DS_CONTA mismatch)."
        )
        cons_issues += 1

    # Receita < 0 (pode ser legítimo em alguns casos, mas >5% é suspeito)
    df_receita_neg = q("""
        SELECT COUNT(*) as cnt
        FROM public."Demonstracoes_Financeiras"
        WHERE "Receita_Liquida" < 0
    """)
    n_receita_neg = int(df_receita_neg["cnt"].iloc[0])
    if n_receita_neg > 0:
        pct_receita_neg = pct(n_receita_neg, int(total_rows))
        if pct_receita_neg > 2:
            pq.add_warning(
                f"{n_receita_neg} linhas com Receita_Liquida < 0 ({pct_receita_neg}%). "
                f"Pode ser empresas financeiras ou erro na conta 3.01."
            )

    # Dívida Líquida = Dívida Total - Caixa; verificar se consistente
    df_div = q("""
        SELECT COUNT(*) as cnt
        FROM public."Demonstracoes_Financeiras"
        WHERE ABS("Divida_Liquida" - ("Divida_Total" - "Caixa_Liquido")) > 1
          AND "Divida_Total" != 0
    """)
    n_div_incons = int(df_div["cnt"].iloc[0])
    if n_div_incons > 0:
        pq.add_finding(
            f"{n_div_incons} linhas onde Divida_Liquida ≠ Divida_Total - Caixa_Liquido (diff > R$1). "
            f"Possível erro de transformação."
        )
        cons_issues += 2

    pq.consistency = max(0.0, 100.0 - cons_issues * 15)

    # ── RASTREABILIDADE ──────────────────────────────────────────────────────

    # PROBLEMA ESTRUTURAL: filtrar_empresas() descarta silenciosamente.
    # Não há como saber quantos tickers do CVM foram rejeitados via banco.
    # O ingestion_log foi adicionado mas ainda é recente.
    pq.add_finding(
        "PERDA SILENCIOSA CRÍTICA: `filtrar_empresas()` descarta tickers com lacunas "
        "temporais, tickers que não chegam ao último ano e tickers com >10% de Receita=NULL, "
        "SEM qualquer log de quais foram descartados e por quê. "
        "Não há como auditar via banco quantos tickers do CVM foram excluídos."
    )
    pq.add_finding(
        "PERDA SILENCIOSA: processamento paralelo por ano usa `except Exception: continue` "
        "sem acumular contagem de falhas. Anos com erro de download são simplesmente ignorados."
    )
    pq.traceability = 20.0  # estruturalmente baixa

    # ── COBERTURA ────────────────────────────────────────────────────────────

    # Cobertura temporal: qual é a mediana de anos por ticker?
    mediana_anos = float(df["n_anos"].median())
    anos_desde_2010 = date.today().year - 2010 + 1
    pct_cobertura = pct(mediana_anos, anos_desde_2010)
    pq.coverage = score_from_pct(pct_cobertura, good_threshold=80, bad_threshold=50)

    if mediana_anos < anos_desde_2010 * 0.7:
        pq.add_warning(
            f"Mediana de {mediana_anos:.0f} anos por ticker (esperado ~{anos_desde_2010}). "
            f"Possível que muitos tickers foram rejeitados pelo filtro de continuidade."
        )
    else:
        pq.add_ok(f"Mediana de {mediana_anos:.0f} anos por ticker — cobertura temporal razoável.")

    # Nota positiva
    pq.add_ok(f"Total: {total_tickers} tickers, {int(total_rows)} linhas em Demonstracoes_Financeiras.")
    pq.add_ok("Normalização de escala (ESCALA_MOEDA) implementada corretamente com exceção para contas por ação.")
    pq.add_ok("Retry HTTP robusto (5x, backoff 1.2) para download dos ZIPs do CVM.")

    return pq


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE 2 — TRI (Demonstrações Financeiras trimestrais)
# ─────────────────────────────────────────────────────────────────────────────

def audit_tri() -> PipelineQuality:
    pq = PipelineQuality(
        pipeline="tri",
        source="CVM Open Data (ITR ZIPs)",
        destination='public."Demonstracoes_Financeiras_TRI"',
    )

    try:
        df = q("""
            SELECT
                "Ticker",
                COUNT(*) AS n_tri,
                MIN("Data") AS primeiro,
                MAX("Data") AS ultimo,
                SUM(CASE WHEN "Receita_Liquida" = 0 THEN 1 ELSE 0 END) AS n_receita_zero,
                SUM(CASE WHEN "Ativo_Total" <= 0 THEN 1 ELSE 0 END) AS n_ativo_inv,
                SUM(CASE WHEN "LPA" = 0 THEN 1 ELSE 0 END) AS n_lpa_zero,
                SUM(CASE WHEN "Patrimonio_Liquido" = 0 AND "Ativo_Total" > 0 THEN 1 ELSE 0 END) AS n_pl_zero
            FROM public."Demonstracoes_Financeiras_TRI"
            GROUP BY "Ticker"
        """)
    except Exception as e:
        pq.add_finding(f"Erro ao carregar TRI: {e}")
        return pq

    if df.empty:
        pq.add_finding("Tabela Demonstracoes_Financeiras_TRI está vazia.")
        return pq

    total_tickers = len(df)
    total_rows = df["n_tri"].sum()

    # Completude: trimestres por ticker (esperado: ~4 por ano)
    df["primeiro_ano"] = pd.to_datetime(df["primeiro"]).dt.year
    df["ultimo_ano"] = pd.to_datetime(df["ultimo"]).dt.year
    df["anos_cobertura"] = df["ultimo_ano"] - df["primeiro_ano"] + 1
    df["tri_esperados"] = df["anos_cobertura"] * 4
    df["cobertura_tri"] = df["n_tri"] / df["tri_esperados"].clip(lower=1)
    mediana_cobertura = float(df["cobertura_tri"].median())
    pq.completeness = score_from_pct(mediana_cobertura * 100, good_threshold=85, bad_threshold=60)

    tickers_incompletos = df[df["cobertura_tri"] < 0.75]
    if not tickers_incompletos.empty:
        pq.add_warning(
            f"{len(tickers_incompletos)} tickers com <75% dos trimestres esperados "
            f"(ex: {tickers_incompletos['Ticker'].head(5).tolist()})."
        )

    # Fidelidade: zero em campos críticos
    fid_issues = 0
    df["pct_receita_zero"] = df["n_receita_zero"] / df["n_tri"]
    suspeitos = df[df["pct_receita_zero"] > 0.5]
    if not suspeitos.empty:
        pq.add_finding(
            f"{len(suspeitos)} tickers com Receita_Liquida=0 em >50% dos trimestres — "
            f"possível falha no CD_CONTA 3.01 ou empresa financeira sem receita operacional."
        )
        fid_issues += 2

    n_ativo_inv = int(df["n_ativo_inv"].sum())
    if n_ativo_inv > 0:
        pq.add_finding(f"{n_ativo_inv} linhas TRI com Ativo_Total ≤ 0.")
        fid_issues += 1

    # ITR usa ORDEM_EXERC == "ÚLTIMO" (trimestre isolado) — correto para série temporal
    pq.add_ok("Filtragem por ORDEM_EXERC='ÚLTIMO' garante dados trimestrais isolados (não acumulados).")

    pq.fidelity = max(0.0, 100.0 - fid_issues * 15)

    # Consistência: PL = 0 com Ativo > 0
    n_pl_zero = int(df["n_pl_zero"].sum())
    if n_pl_zero > 0:
        pct_pl_zero = pct(n_pl_zero, int(total_rows))
        pq.add_warning(
            f"{n_pl_zero} linhas TRI com PL=0 e Ativo>0 ({pct_pl_zero}%) — "
            f"possível falha na busca de DS_CONTA 'Patrimônio Líquido Consolidado'."
        )
    pq.consistency = max(0.0, 95.0 - (n_pl_zero / max(total_rows, 1)) * 100 * 2)

    # Rastreabilidade: sem log de tickers rejeitados (mesmo problema que DFP)
    pq.add_finding(
        "Sem arquivo cvm_to_ticker.csv versionado — dependência de arquivo local "
        "que pode divergir da tabela cvm_to_ticker no banco."
    )
    pq.add_finding("Sem ingestion_log integrado — não há rastreamento de execuções TRI.")
    pq.traceability = 15.0

    # Cobertura
    mediana_tri = float(df["n_tri"].median())
    pq.coverage = score_from_pct(
        pct(mediana_tri, (date.today().year - 2010 + 1) * 4),
        good_threshold=75,
        bad_threshold=40,
    )
    pq.add_ok(f"{total_tickers} tickers, {int(total_rows)} linhas em Demonstracoes_Financeiras_TRI.")

    return pq


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE 3 — Múltiplos DFP
# ─────────────────────────────────────────────────────────────────────────────

def audit_multiplos() -> PipelineQuality:
    pq = PipelineQuality(
        pipeline="multiplos",
        source="Demonstracoes_Financeiras + yfinance",
        destination="public.multiplos",
    )

    try:
        df = q("""
            SELECT
                "Ticker",
                COUNT(*) AS n_anos,
                SUM(CASE WHEN "Preco" IS NULL OR "Preco" = 0 THEN 1 ELSE 0 END) AS n_sem_preco,
                SUM(CASE WHEN "P/L" = 0 THEN 1 ELSE 0 END) AS n_pl_zero,
                SUM(CASE WHEN "P/VP" = 0 THEN 1 ELSE 0 END) AS n_pvp_zero,
                SUM(CASE WHEN "DY" = 0 THEN 1 ELSE 0 END) AS n_dy_zero,
                SUM(CASE WHEN "ROE" = 0 THEN 1 ELSE 0 END) AS n_roe_zero,
                MAX("Data") AS ultimo_dado
            FROM public.multiplos
            GROUP BY "Ticker"
        """)
    except Exception as e:
        # Tabela pode ter colunas diferentes
        pq.add_finding(f"Erro ao auditar multiplos: {e}")
        # Tentar query mais simples
        try:
            df_simple = q('SELECT "Ticker", COUNT(*) as n, MAX("Data") as ultimo FROM public.multiplos GROUP BY "Ticker"')
            pq.add_warning(f"Auditoria parcial: {len(df_simple)} tickers, {int(df_simple['n'].sum())} linhas.")
        except Exception:
            pass
        return pq

    if df.empty:
        pq.add_finding("Tabela multiplos está vazia.")
        return pq

    total_tickers = len(df)
    total_rows = df["n_anos"].sum()

    # Completude: anos cobertos vs. esperado
    df["ultimo_ano"] = pd.to_datetime(df["ultimo_dado"]).dt.year
    anos_esperados = date.today().year - 2010 + 1
    df["pct_cobertura"] = df["n_anos"] / anos_esperados
    mediana_cob = float(df["pct_cobertura"].median())
    pq.completeness = score_from_pct(mediana_cob * 100, good_threshold=80, bad_threshold=50)

    # CRÍTICO: tickers com dados só até 2023 (YF_END default)
    tickers_2023 = df[df["ultimo_ano"] <= 2023]
    if not tickers_2023.empty:
        pct_desatual = pct(len(tickers_2023), total_tickers)
        pq.add_finding(
            f"CRÍTICO: {len(tickers_2023)}/{total_tickers} tickers ({pct_desatual}%) "
            f"com múltiplos somente até 2023. "
            f"Causa: YF_END padrão = '2023-12-31' em dados_multiplos_dfp.py. "
            f"P/L, DY, P/VP de 2024-2025 são ZEROS por falta de preço — "
            f"o app consome esses zeros como dados válidos."
        )

    # Fidelidade: zeros em campos dependentes de preço
    df["pct_sem_preco"] = df["n_sem_preco"] / df["n_anos"]
    sem_preco_alto = df[df["pct_sem_preco"] > 0.3]
    if not sem_preco_alto.empty:
        pq.add_finding(
            f"{len(sem_preco_alto)} tickers sem preço yfinance em >30% dos períodos. "
            f"P/L, DY, P/VP ficam = 0 (mascarados por fillna(0))."
        )

    pq.add_finding(
        "DISTORÇÃO SEMÂNTICA: fillna(0.0) em todos os múltiplos dependentes de preço. "
        "Impossível distinguir P/L=0 (dado ausente) de P/L genuinamente zero. "
        "App trata ambos como iguais — risco alto para análise quantitativa."
    )
    pq.add_finding(
        "FRAGILIDADE: N_Acoes = |Lucro_Liquido| / |LPA|. "
        "Quando LPA=0 (empresas com prejuízo normalizadas para zero), "
        "N_Acoes=NaN→0, tornando DY e P/VP também zero. "
        "Empresas com prejuízo sistemático terão múltiplos incorretos."
    )
    pq.add_finding(
        "INCONSISTÊNCIA DE ENV VAR: dados_multiplos_dfp.py usa SUPABASE_DB_URL "
        "(sem _PG), enquanto dados_cvm_dfp.py usa SUPABASE_DB_URL_PG. "
        "Se ambas não forem setadas, apenas um pipeline quebra."
    )
    pq.fidelity = 40.0

    # Consistência: P/L e P/VP altos demais (possível dado inválido não filtrado)
    try:
        df_outliers = q("""
            SELECT COUNT(*) as cnt
            FROM public.multiplos
            WHERE ABS("P/L") > 1000 OR ABS("P/VP") > 100 OR ABS("DY") > 1
        """)
        n_outliers = int(df_outliers["cnt"].iloc[0])
        if n_outliers > 0:
            pq.add_warning(
                f"{n_outliers} linhas com múltiplos extremos (P/L>1000 ou P/VP>100 ou DY>100%). "
                f"Verificar se são valores pós-capping ou dados mal calculados."
            )
    except Exception:
        pass

    pq.consistency = 65.0
    pq.traceability = 20.0  # sem ingestion_log no multiplos_dfp

    pq.add_warning(
        "yfinance: `except Exception: continue` em cada batch de 50 tickers. "
        "Falha de rede afeta silenciosamente até 50 tickers por vez. "
        "Não há retry, não há log de tickers falhados."
    )

    # Cobertura
    df_tickers_df = q('SELECT COUNT(DISTINCT "Ticker") as cnt FROM public."Demonstracoes_Financeiras"')
    tickers_df = int(df_tickers_df["cnt"].iloc[0])
    cobertura_rel = pct(total_tickers, tickers_df) if tickers_df > 0 else 100.0
    pq.coverage = score_from_pct(cobertura_rel, good_threshold=95, bad_threshold=70)

    if cobertura_rel < 95:
        pq.add_warning(
            f"Apenas {total_tickers}/{tickers_df} tickers ({cobertura_rel}%) de DF "
            f"têm múltiplos calculados."
        )

    pq.add_ok(f"{total_tickers} tickers, {int(total_rows)} linhas em multiplos.")
    return pq


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE 4 — Múltiplos TRI
# ─────────────────────────────────────────────────────────────────────────────

def audit_multiplos_tri() -> PipelineQuality:
    pq = PipelineQuality(
        pipeline="multiplos_tri",
        source='Demonstracoes_Financeiras_TRI + yfinance',
        destination='public."multiplos_TRI"',
    )

    try:
        df = q("""
            SELECT
                "Ticker",
                COUNT(*) AS n_tri,
                SUM(CASE WHEN "P/L" = 0 OR "P/L" IS NULL THEN 1 ELSE 0 END) AS n_pl_zero,
                SUM(CASE WHEN "ROE" = 0 THEN 1 ELSE 0 END) AS n_roe_zero,
                MAX("Data") AS ultimo
            FROM public."multiplos_TRI"
            GROUP BY "Ticker"
        """)
    except Exception as e:
        pq.add_finding(f"Erro ao auditar multiplos_TRI: {e}")
        return pq

    if df.empty:
        pq.add_finding("Tabela multiplos_TRI está vazia.")
        return pq

    total_tickers = len(df)
    total_rows = df["n_tri"].sum()

    pq.add_finding(
        "FALHA DE IMPORTAÇÃO: SUPABASE_DB_URL não setada causa RuntimeError na importação do módulo "
        "(ENGINE = sa.create_engine(...) no escopo global). "
        "Se importado em Streamlit sem a variável, o app inteiro falha no boot."
    )
    pq.add_finding(
        "EXCLUSÃO SILENCIOSA: ticker_valido_yf() filtra tickers por regex [A-Z]{4}\\d{1,2}. "
        "Tickers fora deste padrão (FIIs, ETFs, holdings com nome diferente) "
        "NUNCA recebem preço — ficam com múltiplos dependentes de preço = None/0."
    )

    # Completude
    df["ultimo_ano"] = pd.to_datetime(df["ultimo"]).dt.year
    tickers_antigos = df[df["ultimo_ano"] < date.today().year - 1]
    if not tickers_antigos.empty:
        pq.add_warning(
            f"{len(tickers_antigos)} tickers sem dados TRI nos últimos ~2 anos. "
            f"Pipeline pode não ter rodado recentemente ou tickers foram descontinuados."
        )

    mediana_tri = float(df["n_tri"].median())
    tri_esperados = (date.today().year - 2010 + 1) * 4
    pq.completeness = score_from_pct(
        pct(mediana_tri, tri_esperados), good_threshold=75, bad_threshold=40
    )

    pq.add_finding(
        "TTM via rolling(4, min_periods=4): empresas com <4 trimestres consecutivos "
        "produzem NaN no TTM — múltiplos ficam ausentes para o período inicial de qualquer empresa. "
        "Não há log de quantas linhas foram eliminadas pelo min_periods."
    )

    # Fidelidade
    df["pct_pl_zero"] = df["n_pl_zero"] / df["n_tri"].clip(lower=1)
    suspeitos_pl = df[df["pct_pl_zero"] > 0.6]
    fid_issues = 0
    if not suspeitos_pl.empty:
        pq.add_warning(
            f"{len(suspeitos_pl)} tickers com P/L=0 em >60% dos trimestres — "
            f"possível ausência de preço yfinance ou LPA=0 após normalização."
        )
        fid_issues += 1

    pq.fidelity = max(0.0, 70.0 - fid_issues * 15)
    pq.consistency = 65.0
    pq.traceability = 15.0

    pq.add_ok(f"{total_tickers} tickers, {int(total_rows)} linhas em multiplos_TRI.")
    pq.coverage = score_from_pct(
        pct(total_tickers, len(df)),  # todos os tickers carregados têm múltiplos
        good_threshold=90, bad_threshold=60
    )

    return pq


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE 5 — Macro
# ─────────────────────────────────────────────────────────────────────────────

def audit_macro() -> PipelineQuality:
    pq = PipelineQuality(
        pipeline="macro",
        source="BCB/SGS API",
        destination="public.info_economica + info_economica_mensal",
    )

    try:
        df_annual = q("SELECT * FROM public.info_economica ORDER BY data ASC")
    except Exception as e:
        pq.add_finding(f"Erro ao carregar info_economica: {e}")
        return pq

    if df_annual.empty:
        pq.add_finding("info_economica está vazia.")
        return pq

    n_linhas = len(df_annual)
    expected_series = ["selic", "ipca", "cambio", "balanca_comercial", "icc", "pib", "divida_publica"]

    # Colunas presentes
    cols_presentes = [c for c in expected_series if c in df_annual.columns]
    cols_ausentes = [c for c in expected_series if c not in df_annual.columns]

    if cols_ausentes:
        pq.add_finding(f"Séries ausentes na tabela: {cols_ausentes}")

    # Completude: anos cobertos
    datas = pd.to_datetime(df_annual["data"])
    primeiro_ano = datas.min().year
    ultimo_ano = datas.max().year
    anos_esperados = date.today().year - 2010
    anos_reais = ultimo_ano - 2010
    pq.completeness = score_from_pct(
        pct(anos_reais, anos_esperados), good_threshold=90, bad_threshold=70
    )

    dias_atraso = (date.today() - datas.max().date()).days
    if dias_atraso > 400:
        pq.add_finding(
            f"Último dado macro anual: {datas.max().date()} ({dias_atraso} dias atrás). "
            f"Pipeline de macro pode não ter rodado no último ano."
        )
    elif dias_atraso > 180:
        pq.add_warning(
            f"Último dado macro anual: {datas.max().date()} ({dias_atraso} dias atrás). "
            f"Considerar re-execução do pipeline."
        )
    else:
        pq.add_ok(f"Macro anual atualizado até {datas.max().date()}.")

    # Fidelidade: nulos por série
    fid_issues = 0
    for col in cols_presentes:
        n_null = int(df_annual[col].isna().sum())
        if n_null > 0:
            pct_null = pct(n_null, n_linhas)
            if pct_null > 20:
                pq.add_warning(f"Série '{col}': {n_null} nulos ({pct_null}% dos anos).")
                fid_issues += 1

    pq.fidelity = max(0.0, 95.0 - fid_issues * 10)

    pq.add_finding(
        "INCONSISTÊNCIA NO CÓDIGO: comentário diz 'icc: 14' mas código usa série 4393. "
        "Série 14 = ICC São Paulo (FCESP); série 4393 = ICC consolidado BCB. "
        "Dado pode não ser o esperado historicamente."
    )
    pq.add_finding(
        "ESCRITA MENSAL desativada por padrão (MACRO_WRITE_MONTHLY=0). "
        "info_economica_mensal pode estar desatualizada ou vazia — "
        "o app que a consome não saberá."
    )
    pq.add_warning(
        "Sem validação de séries descontinuadas pelo BCB. "
        "Se BCB renumerar uma série, o pipeline retorna dados vazios sem alarme."
    )

    # Monthly check
    try:
        df_monthly = q("SELECT MAX(data) as ultimo FROM public.info_economica_mensal")
        if df_monthly.empty or df_monthly["ultimo"].iloc[0] is None:
            pq.add_warning("info_economica_mensal está vazia ou sem dados.")
        else:
            ultimo_m = pd.to_datetime(df_monthly["ultimo"].iloc[0])
            meses_atraso = (pd.Timestamp.now() - ultimo_m).days / 30
            if meses_atraso > 4:
                pq.add_warning(
                    f"info_economica_mensal: último registro em {ultimo_m.date()} "
                    f"({meses_atraso:.0f} meses atrás)."
                )
    except Exception:
        pass

    pq.consistency = 80.0
    pq.traceability = 40.0  # melhor que os outros por ter config via dataclass
    pq.coverage = 85.0  # 7 séries, cobertura razoável

    pq.add_ok("Retry com backoff exponencial (min(2^n, 20)s, max 5 tentativas).")
    pq.add_ok("Fetch chunked por janela de anos evita timeouts da API BCB.")
    pq.add_ok(f"Macro anual: {n_linhas} linhas, {primeiro_ano}–{ultimo_ano}.")

    return pq


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE 6 — Docs Corporativos + Chunks
# ─────────────────────────────────────────────────────────────────────────────

def audit_docs() -> PipelineQuality:
    pq = PipelineQuality(
        pipeline="docs",
        source="CVM/IPE + CVM/ENET + RI Crawler",
        destination="public.docs_corporativos + docs_corporativos_chunks",
    )

    try:
        df_docs = q("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN raw_text IS NULL OR LENGTH(raw_text) < 50 THEN 1 ELSE 0 END) AS n_sem_texto,
                SUM(CASE WHEN LENGTH(raw_text) BETWEEN 50 AND 300 THEN 1 ELSE 0 END) AS n_texto_curto,
                SUM(CASE WHEN ticker IS NULL THEN 1 ELSE 0 END) AS n_sem_ticker,
                SUM(CASE WHEN doc_hash IS NULL THEN 1 ELSE 0 END) AS n_sem_hash,
                COUNT(DISTINCT ticker) AS n_tickers,
                COUNT(DISTINCT fonte) AS n_fontes
            FROM public.docs_corporativos
        """)
    except Exception as e:
        pq.add_finding(f"Erro ao auditar docs_corporativos: {e}")
        return pq

    row = df_docs.iloc[0]
    total = int(row["total"])
    n_sem_texto = int(row["n_sem_texto"])
    n_texto_curto = int(row["n_texto_curto"])
    n_tickers = int(row["n_tickers"])

    if total == 0:
        pq.add_finding("docs_corporativos está vazio.")
        return pq

    pct_sem_texto = pct(n_sem_texto, total)
    pct_texto_curto = pct(n_texto_curto, total)

    # Completude
    pq.completeness = score_from_pct(100 - pct_sem_texto, good_threshold=90, bad_threshold=70)

    if pct_sem_texto > 10:
        pq.add_finding(
            f"PERDA SILENCIOSA CRÍTICA: {n_sem_texto}/{total} documentos ({pct_sem_texto}%) "
            f"com raw_text NULL ou < 50 chars. "
            f"Esses documentos existem no banco MAS SÃO INÚTEIS para RAG. "
            f"O pipeline de ingestão não detecta ou alerta sobre falhas de PDF extraction."
        )
    elif pct_sem_texto > 3:
        pq.add_warning(
            f"{n_sem_texto}/{total} documentos ({pct_sem_texto}%) sem texto útil."
        )

    if pct_texto_curto > 5:
        pq.add_warning(
            f"{n_texto_curto}/{total} documentos ({pct_texto_curto}%) com texto entre 50-300 chars. "
            f"Possível extração parcial de PDF ou documento quase vazio."
        )

    # Fidelidade: docs com texto mas sem chunks
    try:
        df_orphan = q("""
            SELECT COUNT(*) as cnt
            FROM public.docs_corporativos d
            WHERE d.raw_text IS NOT NULL
              AND LENGTH(d.raw_text) > 300
              AND NOT EXISTS (
                  SELECT 1 FROM public.docs_corporativos_chunks c
                  WHERE c.doc_id = d.id
              )
        """)
        n_orphan = int(df_orphan["cnt"].iloc[0])
        if n_orphan > 0:
            pct_orphan = pct(n_orphan, total)
            pq.add_finding(
                f"PERDA SILENCIOSA: {n_orphan} documentos ({pct_orphan}%) com texto válido (>300 chars) "
                f"mas SEM CHUNKS. RAG via docs_corporativos_chunks não encontra esses documentos. "
                f"Pipeline ENET gera chunks; IPE não gera chunks diretamente."
            )
    except Exception:
        pass

    pq.fidelity = max(0.0, 70.0 - (pct_sem_texto * 1.5))

    # Consistência: dois pipelines, dois algoritmos de hash
    pq.add_finding(
        "DUPLICAÇÃO POTENCIAL: IPE e ENET usam algoritmos de doc_hash DIFERENTES. "
        "O mesmo documento pode aparecer em ambos com hashes distintos — "
        "sem deduplicação cruzada entre as duas fontes. "
        "RAG receberá o mesmo conteúdo duplicado, inflando o contexto enviado ao LLM."
    )
    pq.add_finding(
        "RASTREABILIDADE ZERO: campo 'fonte' indica IPE/ENET/RI mas não há "
        "referência à execução (run_id, data de ingestão, versão do pipeline). "
        "Impossível saber quando um documento foi coletado ou reprocessado."
    )

    pq.consistency = 55.0
    pq.traceability = 20.0

    # Cobertura: tickers no Patch6 vs. tickers com docs
    try:
        df_p6_tickers = q("""
            SELECT COUNT(DISTINCT ticker) as cnt
            FROM public.patch6_runs
            WHERE created_at >= NOW() - INTERVAL '90 days'
        """)
        n_p6 = int(df_p6_tickers["cnt"].iloc[0])
        if n_p6 > 0:
            df_docs_p6 = q("""
                SELECT COUNT(DISTINCT d.ticker) as cnt
                FROM public.docs_corporativos d
                WHERE d.ticker IN (
                    SELECT DISTINCT ticker FROM public.patch6_runs
                    WHERE created_at >= NOW() - INTERVAL '90 days'
                )
                AND d.raw_text IS NOT NULL AND LENGTH(d.raw_text) > 300
            """)
            n_com_docs = int(df_docs_p6["cnt"].iloc[0])
            pct_cobertura_docs = pct(n_com_docs, n_p6)
            pq.coverage = score_from_pct(pct_cobertura_docs, good_threshold=90, bad_threshold=60)
            if pct_cobertura_docs < 80:
                pq.add_finding(
                    f"Apenas {n_com_docs}/{n_p6} tickers do Patch6 ({pct_cobertura_docs}%) "
                    f"têm documentos com texto válido. "
                    f"LLM analisa sem evidências para {n_p6 - n_com_docs} tickers."
                )
        else:
            pq.coverage = 70.0
    except Exception:
        pq.coverage = 60.0

    pq.add_ok(f"{total} documentos, {n_tickers} tickers, {int(row['n_fontes'])} fontes distintas.")
    pq.add_ok("doc_hash SHA256 garante idempotência dentro de cada pipeline (IPE ou ENET).")

    return pq


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE 7 — Patch6 Runs
# ─────────────────────────────────────────────────────────────────────────────

def audit_patch6() -> PipelineQuality:
    pq = PipelineQuality(
        pipeline="patch6",
        source="LLM (OpenAI/Ollama) + Docs RAG",
        destination="public.patch6_runs",
    )

    REQUIRED_FIELDS = [
        "perspectiva_compra", "score_qualitativo", "confianca_analise",
        "tese_sintese", "riscos", "catalisadores",
    ]

    try:
        df = q("""
            SELECT ticker, period_ref, result_json, schema_version, created_at
            FROM public.patch6_runs
            ORDER BY created_at DESC
            LIMIT 1000
        """)
    except Exception as e:
        # schema_version pode não existir ainda
        try:
            df = q("""
                SELECT ticker, period_ref, result_json, created_at
                FROM public.patch6_runs
                ORDER BY created_at DESC
                LIMIT 1000
            """)
            df["schema_version"] = "v1"
            pq.add_warning(
                "Coluna schema_version não existe ainda em patch6_runs. "
                "Rodar migrations.sql para adicioná-la."
            )
        except Exception as e2:
            pq.add_finding(f"Erro ao carregar patch6_runs: {e2}")
            return pq

    if df.empty:
        pq.add_finding("patch6_runs está vazia — nenhuma análise LLM foi salva.")
        return pq

    total = len(df)

    # Schema version distribution
    versions = df.get("schema_version", pd.Series(["v1"] * total)).value_counts().to_dict()
    pq.add_ok(f"Distribuição de schema_version: {versions}")

    # JSON completeness
    incomplete_required = []
    invalid_json = []
    perspectiva_invalida = []
    score_invalido = []

    for _, row in df.iterrows():
        rj = row["result_json"]
        if rj is None:
            invalid_json.append(row["ticker"])
            continue
        try:
            obj = json.loads(rj) if isinstance(rj, str) else rj
        except Exception:
            invalid_json.append(row["ticker"])
            continue

        missing = [f for f in REQUIRED_FIELDS if f not in obj or obj[f] is None]
        if missing:
            incomplete_required.append({"ticker": row["ticker"], "missing": missing})

        # Perspectiva válida?
        persp = str(obj.get("perspectiva_compra", "") or "").strip().lower()
        if persp not in ("forte", "moderada", "fraca", ""):
            perspectiva_invalida.append({"ticker": row["ticker"], "valor": persp})

        # Score em range válido?
        sc = obj.get("score_qualitativo")
        if sc is not None:
            try:
                sc = float(sc)
                if not (0 <= sc <= 100):
                    score_invalido.append({"ticker": row["ticker"], "score": sc})
            except Exception:
                score_invalido.append({"ticker": row["ticker"], "score": sc})

    if invalid_json:
        pq.add_finding(
            f"{len(invalid_json)} runs com result_json NULL ou inválido "
            f"(ex: {invalid_json[:5]}). Dado corrompido no banco."
        )

    if incomplete_required:
        pq.add_finding(
            f"{len(incomplete_required)}/{total} runs ({pct(len(incomplete_required), total)}%) "
            f"com campos obrigatórios ausentes no result_json."
        )

    if perspectiva_invalida:
        pq.add_warning(
            f"{len(perspectiva_invalida)} runs com perspectiva_compra fora dos valores "
            f"esperados (forte/moderada/fraca): {perspectiva_invalida[:5]}"
        )

    if score_invalido:
        pq.add_warning(
            f"{len(score_invalido)} runs com score_qualitativo fora do range [0,100]: "
            f"{score_invalido[:5]}"
        )

    # Completude
    pct_incompleto = pct(len(incomplete_required) + len(invalid_json), total)
    pq.completeness = score_from_pct(100 - pct_incompleto, good_threshold=95, bad_threshold=75)

    # Fidelidade
    pq.add_warning(
        "period_ref é string livre (ex: '2024-Q3') sem validação de formato. "
        "Comparações temporais e ordenação cronológica podem falhar."
    )
    pq.add_finding(
        "Sem limite de tamanho para result_json. "
        "JSONs muito grandes podem degradar performance de leitura e análise."
    )
    pq.fidelity = 65.0

    # Consistência
    pq.consistency = max(0.0, 80.0 - pct_incompleto)

    # Rastreabilidade
    pq.add_ok("UPSERT com ON CONFLICT em (snapshot_id, ticker, period_ref) — idempotente.")
    pq.add_ok("schema_version recentemente adicionado — permite migração futura.")
    pq.traceability = 50.0

    # Cobertura: tickers em setores vs. tickers com análise recente
    try:
        df_setores = q("SELECT COUNT(DISTINCT ticker) as cnt FROM public.setores")
        df_p6_rec = q("""
            SELECT COUNT(DISTINCT ticker) as cnt
            FROM public.patch6_runs
            WHERE created_at >= NOW() - INTERVAL '90 days'
        """)
        n_setores = int(df_setores["cnt"].iloc[0])
        n_p6_rec = int(df_p6_rec["cnt"].iloc[0])
        pct_cov = pct(n_p6_rec, n_setores)
        pq.coverage = score_from_pct(pct_cov, good_threshold=70, bad_threshold=30)
        if pct_cov < 30:
            pq.add_warning(
                f"Apenas {n_p6_rec}/{n_setores} tickers ({pct_cov}%) do universo "
                f"têm análise Patch6 nos últimos 90 dias."
            )
    except Exception:
        pq.coverage = 50.0

    pq.add_ok(f"{total} runs analisadas (últimas 1000).")

    return pq


# ─────────────────────────────────────────────────────────────────────────────
# Score consolidado e quality gates
# ─────────────────────────────────────────────────────────────────────────────

def print_summary(results: List[PipelineQuality]) -> None:
    print("\n" + "=" * 68)
    print("  MATRIZ DE QUALIDADE — RESUMO EXECUTIVO")
    print("=" * 68)
    print(f"  {'Pipeline':<18} {'Score':>6}  {'Status':<12} {'Completude':>10} {'Fidelidade':>10} {'Consistência':>12} {'Rastreab.':>9}")
    print("  " + "─" * 64)

    for r in sorted(results, key=lambda x: x.score):
        print(
            f"  {r.pipeline:<18} {r.score:>6.1f}  {r.status:<12} "
            f"{r.completeness:>10.1f} {r.fidelity:>10.1f} {r.consistency:>12.1f} {r.traceability:>9.1f}"
        )

    print("=" * 68)

    scores = [r.score for r in results]
    avg = sum(scores) / len(scores)
    n_criticos = sum(1 for r in results if r.status == "CRÍTICO")
    n_atencao = sum(1 for r in results if r.status == "ATENÇÃO")
    n_saudaveis = sum(1 for r in results if r.status == "SAUDÁVEL")

    print(f"\n  Score médio geral: {avg:.1f}/100")
    print(f"  Críticos: {n_criticos} | Atenção: {n_atencao} | Saudáveis: {n_saudaveis}")

    # Veredito global
    print("\n" + "=" * 68)
    print("  VEREDITO GLOBAL")
    print("=" * 68)
    if avg >= 75:
        nivel = "BOM"
    elif avg >= 58:
        nivel = "ACEITÁVEL"
    elif avg >= 42:
        nivel = "FRÁGIL"
    else:
        nivel = "RUIM"

    print(f"\n  A ingestão do app está em nível: [{nivel}]")
    print(f"\n  Score médio: {avg:.1f}/100")

    # Principais riscos
    todos_findings = []
    for r in results:
        for f in r.findings:
            todos_findings.append((r.pipeline, f))

    if todos_findings:
        print(f"\n  Top problemas críticos ({len(todos_findings)} total):")
        for pipe, finding in todos_findings[:8]:
            short = finding[:100] + "..." if len(finding) > 100 else finding
            print(f"    [{pipe}] {short}")

    print()


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

ALL_PIPELINES = {
    "dfp": audit_dfp,
    "tri": audit_tri,
    "multiplos": audit_multiplos,
    "multiplos_tri": audit_multiplos_tri,
    "macro": audit_macro,
    "docs": audit_docs,
    "patch6": audit_patch6,
}


def main():
    parser = argparse.ArgumentParser(description="Auditoria de qualidade de ingestão")
    parser.add_argument("--all", action="store_true", help="Todos os pipelines")
    parser.add_argument("--pipeline", choices=list(ALL_PIPELINES.keys()), help="Pipeline específico")
    parser.add_argument("--score-only", action="store_true", help="Apenas matrix de scores, sem detalhes")
    args = parser.parse_args()

    if not args.all and not args.pipeline:
        parser.print_help()
        sys.exit(1)

    to_run = list(ALL_PIPELINES.keys()) if args.all else [args.pipeline]

    results = []
    for name in to_run:
        print(f"Auditando: {name}...", end=" ", flush=True)
        try:
            r = ALL_PIPELINES[name]()
            results.append(r)
            print(f"{r.score:.1f}/100 [{r.status}]")
        except Exception as e:
            print(f"ERRO: {e}")

    if not args.score_only:
        for r in results:
            r.print_report()

    if results:
        print_summary(results)

    n_criticos = sum(1 for r in results if r.status == "CRÍTICO")
    sys.exit(1 if n_criticos > 0 else 0)


if __name__ == "__main__":
    main()
