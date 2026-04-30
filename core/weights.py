from __future__ import annotations

"""
weights.py
~~~~~~~~~~
Pesos dos indicadores fundamentalistas por setor (B3).

Melhorias desta versão:
- Governança (versionamento e data).
- Normalização automática (soma dos pesos = 1.0 por setor).
- Direção (melhor_alto) correta no fallback genérico.
- Override opcional via weights_override.json (ao lado deste arquivo).
- Funções utilitárias de validação (para auditoria e testes).

Compatibilidade preservada:
- get_pesos(setor: str) -> Dict[str, Dict[str, float|bool]]
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple
import json
import logging

logger = logging.getLogger(__name__)

WEIGHTS_VERSION = "2026.04.30"
UPDATED_AT = "2026-04-30"


# ─────────────────────────────────────────────────────────────
# Direção padrão (fallback genérico)
# ─────────────────────────────────────────────────────────────

DEFAULT_DIRECTIONS: Dict[str, bool] = {
    # Qualidade (quanto maior, melhor)
    "Margem_Liquida_mean": True,
    "Margem_Operacional_mean": True,
    "ROE_mean": True,
    "ROA_mean": True,
    "ROIC_mean": True,

    # Valuation / risco (quanto menor, melhor)
    "P/VP_mean": False,
    "Endividamento_Total_mean": False,
    "Alavancagem_Financeira_mean": False,

    # Liquidez (maior, melhor)
    "Liquidez_Corrente_mean": True,
    "Liquidez_Seca_mean": True,        # novo: liquidez sem estoques
    "Liquidez_Imediata_mean": True,    # novo: só caixa vs passivo CP

    # Proventos (maior, melhor)
    "DY_mean": True,

    # Eficiência operacional (maior, melhor)
    "Giro_Ativo_mean": True,           # novo: receita / ativo total

    # Geração de caixa (maior, melhor)
    "Margem_FCO_mean": True,           # novo: FCO / receita
    "FCO_sobre_Divida_mean": True,     # novo: FCO / dívida bruta
    "Cobertura_Investimento_mean": True, # novo: FCO / |FCI|

    # Prazo de recebimento (menor, melhor — empresa recebe mais rápido)
    "Prazo_Medio_Recebimento_mean": False,

    # Crescimento (maior, melhor)
    "Receita_Liquida_slope_log": True,
    "Lucro_Liquido_slope_log": True,
    "Patrimonio_Liquido_slope_log": True,
    "Caixa_Liquido_slope_log": True,

    # Dívida líquida (quanto menor/menos crescente, melhor)
    "Divida_Liquida_slope_log": False,

    # Mercado (maior, melhor) — só será usado se existir no DF de score
    "Momentum_12m": True,
}


# ─────────────────────────────────────────────────────────────
# Pesos genéricos (base) — serão normalizados automaticamente
# ─────────────────────────────────────────────────────────────

indicadores_score: Dict[str, float] = {
    # --- Rentabilidade (existentes) ---
    "Margem_Liquida_mean": 0.15,
    "Margem_Operacional_mean": 0.20,
    "ROE_mean": 0.20,
    "ROA_mean": 0.20,
    "ROIC_mean": 0.20,
    # --- Valuation / risco (existentes) ---
    "P/VP_mean": 0.10,
    "DY_mean": 0.30,
    "Endividamento_Total_mean": 0.15,
    "Alavancagem_Financeira_mean": 0.15,
    # --- Liquidez (existentes + novos) ---
    "Liquidez_Corrente_mean": 0.12,
    "Liquidez_Seca_mean": 0.08,          # novo
    "Liquidez_Imediata_mean": 0.04,      # novo
    # --- Eficiência (novo) ---
    "Giro_Ativo_mean": 0.10,             # novo
    # --- Geração de caixa (novos) ---
    "Margem_FCO_mean": 0.12,             # novo
    "FCO_sobre_Divida_mean": 0.10,       # novo
    "Cobertura_Investimento_mean": 0.06, # novo
    # --- Prazo de recebimento (novo) ---
    "Prazo_Medio_Recebimento_mean": 0.06, # novo
    # --- Crescimento (existentes) ---
    "Receita_Liquida_slope_log": 0.15,
    "Lucro_Liquido_slope_log": 0.20,
    "Patrimonio_Liquido_slope_log": 0.15,
    "Divida_Liquida_slope_log": 0.15,
    "Caixa_Liquido_slope_log": 0.15,
    # --- Mercado (existente) ---
    "Momentum_12m": 0.15,
}


# ─────────────────────────────────────────────────────────────
# Pesos por setor (originais) — mantidos, mas serão normalizados
# ─────────────────────────────────────────────────────────────

pesos_por_setor: Dict[str, Dict[str, Dict[str, Any]]] = {
    "Financeiro": {
        "ROE_mean":                     {"peso": 0.28, "melhor_alto": True},
        "P/VP_mean":                    {"peso": 0.15, "melhor_alto": False},
        "DY_mean":                      {"peso": 0.15, "melhor_alto": True},
        "Endividamento_Total_mean":     {"peso": 0.05, "melhor_alto": False},
        "Liquidez_Corrente_mean":       {"peso": 0.05, "melhor_alto": True},
        "Liquidez_Imediata_mean":       {"peso": 0.05, "melhor_alto": True},   # novo: solvência imediata
        "FCO_sobre_Divida_mean":        {"peso": 0.06, "melhor_alto": True},   # novo: cobertura de caixa
        "Margem_Liquida_mean":          {"peso": 0.10, "melhor_alto": True},
        "Lucro_Liquido_slope_log":      {"peso": 0.10, "melhor_alto": True},
        "Momentum_12m":                 {"peso": 0.10, "melhor_alto": True},
        "Prazo_Medio_Recebimento_mean": {"peso": 0.06, "melhor_alto": False},  # novo: eficiência cobrança
    },
    "Tecnologia da Informação": {
        "Margem_Liquida_mean":          {"peso": 0.07, "melhor_alto": True},
        "Margem_Operacional_mean":      {"peso": 0.09, "melhor_alto": True},
        "ROE_mean":                     {"peso": 0.06, "melhor_alto": True},
        "ROA_mean":                     {"peso": 0.04, "melhor_alto": True},
        "ROIC_mean":                    {"peso": 0.07, "melhor_alto": True},
        "P/VP_mean":                    {"peso": 0.03, "melhor_alto": False},
        "DY_mean":                      {"peso": 0.02, "melhor_alto": True},
        "Endividamento_Total_mean":     {"peso": 0.03, "melhor_alto": False},
        "Alavancagem_Financeira_mean":  {"peso": 0.03, "melhor_alto": False},
        "Liquidez_Corrente_mean":       {"peso": 0.03, "melhor_alto": True},
        "Giro_Ativo_mean":              {"peso": 0.05, "melhor_alto": True},   # novo: eficiência
        "Margem_FCO_mean":              {"peso": 0.06, "melhor_alto": True},   # novo: qualidade do lucro
        "Prazo_Medio_Recebimento_mean": {"peso": 0.04, "melhor_alto": False},  # novo: ciclo de recebimento
        "Receita_Liquida_slope_log":    {"peso": 0.15, "melhor_alto": True},
        "Lucro_Liquido_slope_log":      {"peso": 0.12, "melhor_alto": True},
        "Patrimonio_Liquido_slope_log": {"peso": 0.05, "melhor_alto": True},
        "Divida_Liquida_slope_log":     {"peso": 0.02, "melhor_alto": False},
        "Caixa_Liquido_slope_log":      {"peso": 0.05, "melhor_alto": True},
        "Momentum_12m":                 {"peso": 0.09, "melhor_alto": True},
    },
    "Energia": {
        "Margem_Liquida_mean":          {"peso": 0.07, "melhor_alto": True},
        "Margem_Operacional_mean":      {"peso": 0.09, "melhor_alto": True},
        "ROE_mean":                     {"peso": 0.06, "melhor_alto": True},
        "ROA_mean":                     {"peso": 0.05, "melhor_alto": True},
        "ROIC_mean":                    {"peso": 0.06, "melhor_alto": True},
        "P/VP_mean":                    {"peso": 0.03, "melhor_alto": False},
        "DY_mean":                      {"peso": 0.16, "melhor_alto": True},
        "Endividamento_Total_mean":     {"peso": 0.06, "melhor_alto": False},
        "Alavancagem_Financeira_mean":  {"peso": 0.04, "melhor_alto": False},
        "Liquidez_Corrente_mean":       {"peso": 0.05, "melhor_alto": True},
        "FCO_sobre_Divida_mean":        {"peso": 0.06, "melhor_alto": True},   # novo: capacidade de pagar dívida
        "Cobertura_Investimento_mean":  {"peso": 0.05, "melhor_alto": True},   # novo: FCO cobre investimentos
        "Receita_Liquida_slope_log":    {"peso": 0.05, "melhor_alto": True},
        "Lucro_Liquido_slope_log":      {"peso": 0.05, "melhor_alto": True},
        "Patrimonio_Liquido_slope_log": {"peso": 0.02, "melhor_alto": True},
        "Divida_Liquida_slope_log":     {"peso": 0.02, "melhor_alto": False},
        "Caixa_Liquido_slope_log":      {"peso": 0.05, "melhor_alto": True},
        "Momentum_12m":                 {"peso": 0.13, "melhor_alto": True},
    },
    "Industrial": {
        "Margem_Liquida_mean":          {"peso": 0.07, "melhor_alto": True},
        "Margem_Operacional_mean":      {"peso": 0.09, "melhor_alto": True},
        "ROE_mean":                     {"peso": 0.07, "melhor_alto": True},
        "ROA_mean":                     {"peso": 0.05, "melhor_alto": True},
        "ROIC_mean":                    {"peso": 0.09, "melhor_alto": True},
        "P/VP_mean":                    {"peso": 0.06, "melhor_alto": False},
        "DY_mean":                      {"peso": 0.04, "melhor_alto": True},
        "Endividamento_Total_mean":     {"peso": 0.06, "melhor_alto": False},
        "Alavancagem_Financeira_mean":  {"peso": 0.04, "melhor_alto": False},
        "Liquidez_Seca_mean":           {"peso": 0.04, "melhor_alto": True},   # novo: liquidez real
        "Giro_Ativo_mean":              {"peso": 0.05, "melhor_alto": True},   # novo: eficiência produtiva
        "Cobertura_Investimento_mean":  {"peso": 0.04, "melhor_alto": True},   # novo: FCO cobre CAPEX
        "Prazo_Medio_Recebimento_mean": {"peso": 0.04, "melhor_alto": False},  # novo: ciclo operacional
        "Receita_Liquida_slope_log":    {"peso": 0.08, "melhor_alto": True},
        "Lucro_Liquido_slope_log":      {"peso": 0.08, "melhor_alto": True},
        "Patrimonio_Liquido_slope_log": {"peso": 0.04, "melhor_alto": True},
        "Divida_Liquida_slope_log":     {"peso": 0.04, "melhor_alto": False},
        "Caixa_Liquido_slope_log":      {"peso": 0.04, "melhor_alto": True},
        "Momentum_12m":                 {"peso": 0.07, "melhor_alto": True},
    },
    "Consumo Cíclico": {
        "Margem_Liquida_mean":          {"peso": 0.09, "melhor_alto": True},
        "Margem_Operacional_mean":      {"peso": 0.08, "melhor_alto": True},
        "ROE_mean":                     {"peso": 0.08, "melhor_alto": True},
        "ROA_mean":                     {"peso": 0.05, "melhor_alto": True},
        "ROIC_mean":                    {"peso": 0.08, "melhor_alto": True},
        "P/VP_mean":                    {"peso": 0.05, "melhor_alto": False},
        "DY_mean":                      {"peso": 0.05, "melhor_alto": True},
        "Endividamento_Total_mean":     {"peso": 0.06, "melhor_alto": False},
        "Alavancagem_Financeira_mean":  {"peso": 0.04, "melhor_alto": False},
        "Liquidez_Seca_mean":           {"peso": 0.04, "melhor_alto": True},   # novo: solvência real
        "Giro_Ativo_mean":              {"peso": 0.05, "melhor_alto": True},   # novo: giro do negócio
        "Prazo_Medio_Recebimento_mean": {"peso": 0.04, "melhor_alto": False},  # novo: eficiência recebimento
        "Margem_FCO_mean":              {"peso": 0.04, "melhor_alto": True},   # novo: caixa vs contábil
        "Receita_Liquida_slope_log":    {"peso": 0.14, "melhor_alto": True},
        "Lucro_Liquido_slope_log":      {"peso": 0.08, "melhor_alto": True},
        "Patrimonio_Liquido_slope_log": {"peso": 0.04, "melhor_alto": True},
        "Divida_Liquida_slope_log":     {"peso": 0.04, "melhor_alto": False},
        "Caixa_Liquido_slope_log":      {"peso": 0.04, "melhor_alto": True},
        "Momentum_12m":                 {"peso": 0.07, "melhor_alto": True},
    },
    "Consumo não Cíclico": {
        "Margem_Liquida_mean":          {"peso": 0.08, "melhor_alto": True},
        "Margem_Operacional_mean":      {"peso": 0.08, "melhor_alto": True},
        "ROE_mean":                     {"peso": 0.07, "melhor_alto": True},
        "ROA_mean":                     {"peso": 0.05, "melhor_alto": True},
        "ROIC_mean":                    {"peso": 0.07, "melhor_alto": True},
        "DY_mean":                      {"peso": 0.14, "melhor_alto": True},
        "P/VP_mean":                    {"peso": 0.05, "melhor_alto": False},
        "Endividamento_Total_mean":     {"peso": 0.05, "melhor_alto": False},
        "Alavancagem_Financeira_mean":  {"peso": 0.04, "melhor_alto": False},
        "Liquidez_Seca_mean":           {"peso": 0.04, "melhor_alto": True},   # novo
        "Margem_FCO_mean":              {"peso": 0.06, "melhor_alto": True},   # novo: qualidade do caixa
        "Prazo_Medio_Recebimento_mean": {"peso": 0.04, "melhor_alto": False},  # novo
        "Receita_Liquida_slope_log":    {"peso": 0.07, "melhor_alto": True},
        "Lucro_Liquido_slope_log":      {"peso": 0.07, "melhor_alto": True},
        "Patrimonio_Liquido_slope_log": {"peso": 0.03, "melhor_alto": True},
        "Divida_Liquida_slope_log":     {"peso": 0.03, "melhor_alto": False},
        "Caixa_Liquido_slope_log":      {"peso": 0.04, "melhor_alto": True},
        "Momentum_12m":                 {"peso": 0.09, "melhor_alto": True},
    },
    "Materiais Básicos": {
        "Margem_Operacional_mean":      {"peso": 0.11, "melhor_alto": True},
        "Margem_Liquida_mean":          {"peso": 0.06, "melhor_alto": True},
        "ROE_mean":                     {"peso": 0.07, "melhor_alto": True},
        "ROIC_mean":                    {"peso": 0.08, "melhor_alto": True},
        "DY_mean":                      {"peso": 0.11, "melhor_alto": True},
        "P/VP_mean":                    {"peso": 0.06, "melhor_alto": False},
        "Endividamento_Total_mean":     {"peso": 0.06, "melhor_alto": False},
        "Alavancagem_Financeira_mean":  {"peso": 0.04, "melhor_alto": False},
        "Liquidez_Corrente_mean":       {"peso": 0.03, "melhor_alto": True},
        "FCO_sobre_Divida_mean":        {"peso": 0.07, "melhor_alto": True},   # novo: FCO cobre dívida (ciclo commodity)
        "Cobertura_Investimento_mean":  {"peso": 0.06, "melhor_alto": True},   # novo: FCO cobre CAPEX intensivo
        "Receita_Liquida_slope_log":    {"peso": 0.06, "melhor_alto": True},
        "Lucro_Liquido_slope_log":      {"peso": 0.06, "melhor_alto": True},
        "Patrimonio_Liquido_slope_log": {"peso": 0.03, "melhor_alto": True},
        "Divida_Liquida_slope_log":     {"peso": 0.02, "melhor_alto": False},
        "Caixa_Liquido_slope_log":      {"peso": 0.02, "melhor_alto": True},
        "Momentum_12m":                 {"peso": 0.14, "melhor_alto": True},
    },
    "Petróleo, Gás e Biocombustíveis": {
        "DY_mean":                      {"peso": 0.24, "melhor_alto": True},
        "Margem_Operacional_mean":      {"peso": 0.20, "melhor_alto": True},
        "ROIC_mean":                    {"peso": 0.15, "melhor_alto": True},
        "FCO_sobre_Divida_mean":        {"peso": 0.10, "melhor_alto": True},   # novo: cobertura de dívida via caixa operacional
        "Cobertura_Investimento_mean":  {"peso": 0.08, "melhor_alto": True},   # novo: FCO vs CAPEX (setor capital-intensivo)
        "Liquidez_Corrente_mean":       {"peso": 0.07, "melhor_alto": True},
        "Endividamento_Total_mean":     {"peso": 0.08, "melhor_alto": False},
        "Momentum_12m":                 {"peso": 0.08, "melhor_alto": True},
    },
    "Saúde": {
        "Receita_Liquida_slope_log":    {"peso": 0.20, "melhor_alto": True},
        "Lucro_Liquido_slope_log":      {"peso": 0.20, "melhor_alto": True},
        "Margem_Operacional_mean":      {"peso": 0.16, "melhor_alto": True},
        "ROE_mean":                     {"peso": 0.12, "melhor_alto": True},
        "Giro_Ativo_mean":              {"peso": 0.08, "melhor_alto": True},   # novo: eficiência de ativos (hospitais/clínicas)
        "Margem_FCO_mean":              {"peso": 0.07, "melhor_alto": True},   # novo: qualidade do lucro contábil
        "Endividamento_Total_mean":     {"peso": 0.06, "melhor_alto": False},
        "Caixa_Liquido_slope_log":      {"peso": 0.04, "melhor_alto": True},
        "Momentum_12m":                 {"peso": 0.07, "melhor_alto": True},
    },
    "Comunicações": {
        "Margem_Liquida_mean":          {"peso": 0.06, "melhor_alto": True},
        "Margem_Operacional_mean":      {"peso": 0.13, "melhor_alto": True},
        "ROE_mean":                     {"peso": 0.07, "melhor_alto": True},
        "ROA_mean":                     {"peso": 0.04, "melhor_alto": True},
        "ROIC_mean":                    {"peso": 0.10, "melhor_alto": True},
        "P/VP_mean":                    {"peso": 0.04, "melhor_alto": False},
        "DY_mean":                      {"peso": 0.11, "melhor_alto": True},
        "Endividamento_Total_mean":     {"peso": 0.06, "melhor_alto": False},
        "Alavancagem_Financeira_mean":  {"peso": 0.04, "melhor_alto": False},
        "Liquidez_Corrente_mean":       {"peso": 0.05, "melhor_alto": True},
        "Margem_FCO_mean":              {"peso": 0.06, "melhor_alto": True},   # novo: qualidade do caixa em infra de telecom
        "FCO_sobre_Divida_mean":        {"peso": 0.07, "melhor_alto": True},   # novo: setor altamente alavancado
        "Receita_Liquida_slope_log":    {"peso": 0.06, "melhor_alto": True},
        "Lucro_Liquido_slope_log":      {"peso": 0.06, "melhor_alto": True},
        "Momentum_12m":                 {"peso": 0.07, "melhor_alto": True},
    },
    "Bens Industriais": {
        "Margem_Operacional_mean":      {"peso": 0.18, "melhor_alto": True},
        "ROIC_mean":                    {"peso": 0.18, "melhor_alto": True},
        "Giro_Ativo_mean":              {"peso": 0.08, "melhor_alto": True},   # novo: eficiência de ativos industriais
        "Cobertura_Investimento_mean":  {"peso": 0.08, "melhor_alto": True},   # novo: FCO cobre CAPEX de equipamentos
        "Receita_Liquida_slope_log":    {"peso": 0.11, "melhor_alto": True},
        "Liquidez_Corrente_mean":       {"peso": 0.09, "melhor_alto": True},
        "P/VP_mean":                    {"peso": 0.07, "melhor_alto": False},
        "Endividamento_Total_mean":     {"peso": 0.07, "melhor_alto": False},
        "Momentum_12m":                 {"peso": 0.14, "melhor_alto": True},
    },
    "Utilidade Pública": {
        "Margem_Liquida_mean":          {"peso": 0.06, "melhor_alto": True},
        "Margem_Operacional_mean":      {"peso": 0.08, "melhor_alto": True},
        "ROE_mean":                     {"peso": 0.05, "melhor_alto": True},
        "ROA_mean":                     {"peso": 0.03, "melhor_alto": True},
        "ROIC_mean":                    {"peso": 0.05, "melhor_alto": True},
        "P/VP_mean":                    {"peso": 0.04, "melhor_alto": False},
        "DY_mean":                      {"peso": 0.18, "melhor_alto": True},
        "Endividamento_Total_mean":     {"peso": 0.08, "melhor_alto": False},
        "Alavancagem_Financeira_mean":  {"peso": 0.06, "melhor_alto": False},
        "Liquidez_Corrente_mean":       {"peso": 0.08, "melhor_alto": True},
        "FCO_sobre_Divida_mean":        {"peso": 0.08, "melhor_alto": True},   # novo: sustentabilidade financeira regulada
        "Cobertura_Investimento_mean":  {"peso": 0.06, "melhor_alto": True},   # novo: FCO vs CAPEX de manutenção
        "Receita_Liquida_slope_log":    {"peso": 0.03, "melhor_alto": True},
        "Lucro_Liquido_slope_log":      {"peso": 0.04, "melhor_alto": True},
        "Patrimonio_Liquido_slope_log": {"peso": 0.02, "melhor_alto": True},
        "Divida_Liquida_slope_log":     {"peso": 0.03, "melhor_alto": False},
        "Caixa_Liquido_slope_log":      {"peso": 0.03, "melhor_alto": True},
        "Momentum_12m":                 {"peso": 0.12, "melhor_alto": True},
    },
}


# ─────────────────────────────────────────────────────────────
# Override opcional via JSON
# ─────────────────────────────────────────────────────────────

def _load_override_json() -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Se existir `weights_override.json` no mesmo diretório do weights.py,
    carrega e retorna dict no mesmo formato de pesos_por_setor.

    Formato esperado do JSON:
    {
      "Setor X": {
        "ROE_mean": {"peso": 0.2, "melhor_alto": true},
        ...
      },
      ...
    }
    """
    path = Path(__file__).resolve().parent / "weights_override.json"
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            logger.warning("weights_override.json inválido (não é dict). Ignorando.")
            return {}
        # validação superficial
        out: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for setor, conf in data.items():
            if not isinstance(conf, dict):
                continue
            setor_dict: Dict[str, Dict[str, Any]] = {}
            for ind, cfg in conf.items():
                if not isinstance(cfg, dict):
                    continue
                setor_dict[str(ind)] = {
                    "peso": float(cfg.get("peso", 0.0)),
                    "melhor_alto": bool(cfg.get("melhor_alto", DEFAULT_DIRECTIONS.get(str(ind), True))),
                }
            if setor_dict:
                out[str(setor)] = setor_dict
        return out
    except Exception as e:
        logger.exception("Falha ao ler weights_override.json: %s", e)
        return {}


def _merge_overrides(
    base: Dict[str, Dict[str, Dict[str, Any]]],
    overrides: Dict[str, Dict[str, Dict[str, Any]]],
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    out = {k: dict(v) for k, v in base.items()}
    for setor, conf in overrides.items():
        if setor not in out:
            out[setor] = {}
        out[setor].update(conf)
    return out


# ─────────────────────────────────────────────────────────────
# Normalização e validação
# ─────────────────────────────────────────────────────────────

def _normalize_setor(conf: Mapping[str, Mapping[str, Any]], eps: float = 1e-12) -> Dict[str, Dict[str, Any]]:
    """Normaliza pesos do setor para soma 1.0 (ignorando pesos <= 0)."""
    cleaned: Dict[str, Dict[str, Any]] = {}
    total = 0.0

    for ind, cfg in conf.items():
        try:
            w = float(cfg.get("peso", 0.0))
        except Exception:
            w = 0.0
        if w <= 0 or not (w == w):  # NaN
            continue
        melhor_alto = bool(cfg.get("melhor_alto", DEFAULT_DIRECTIONS.get(ind, True)))
        cleaned[ind] = {"peso": w, "melhor_alto": melhor_alto}
        total += w

    if total <= eps:
        # fallback: monta genérico normalizado
        return _build_generic_weights()

    for ind in cleaned:
        cleaned[ind]["peso"] = float(cleaned[ind]["peso"]) / total

    return cleaned


def _build_generic_weights() -> Dict[str, Dict[str, Any]]:
    """Constrói pesos genéricos normalizados com direções corretas."""
    raw = {}
    for ind, w in indicadores_score.items():
        raw[ind] = {
            "peso": float(w),
            "melhor_alto": bool(DEFAULT_DIRECTIONS.get(ind, True)),
        }
    return _normalize_setor(raw)


def validate_pesos(conf: Mapping[str, Mapping[str, Any]], tol: float = 1e-6) -> Tuple[bool, float]:
    """Retorna (ok, soma_pesos) após sanitização simples."""
    s = 0.0
    for _, cfg in conf.items():
        try:
            w = float(cfg.get("peso", 0.0))
        except Exception:
            w = 0.0
        if w > 0 and (w == w):  # not NaN
            s += w
    ok = abs(s - 1.0) <= tol
    return ok, float(s)


# ─────────────────────────────────────────────────────────────
# API pública
# ─────────────────────────────────────────────────────────────

# aplica overrides uma única vez no import
_pesos_final = _merge_overrides(pesos_por_setor, _load_override_json())


def get_pesos(setor: str) -> Dict[str, Dict[str, Any]]:
    """
    Recupera pesos normalizados para o setor especificado.
    Se o setor não estiver definido, retorna pesos genéricos normalizados.
    """
    setor_key = (setor or "").strip()
    conf = _pesos_final.get(setor_key)
    if not conf:
        return _build_generic_weights()
    return _normalize_setor(conf)


def list_setores() -> list[str]:
    """Lista setores disponíveis na configuração final (com override aplicado)."""
    return sorted(_pesos_final.keys())


__all__ = [
    "WEIGHTS_VERSION",
    "UPDATED_AT",
    "DEFAULT_DIRECTIONS",
    "pesos_por_setor",
    "indicadores_score",
    "get_pesos",
    "validate_pesos",
    "list_setores",
]
