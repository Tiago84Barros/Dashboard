"""weights.py
~~~~~~~~~~~~
Contém dicionários de pesos fundamentalistas por setor e um conjunto genérico
para fallback, além de helpers para consulta.

Objetivo
--------
Centralizar a definição dos *weights* de indicadores usados pelo módulo
`scoring`. Assim, qualquer ajuste de pesos fica isolado neste arquivo.

Uso rápido
~~~~~~~~~~
```python
from weights import PESOS_POR_SETOR, INDICADORES_SCORE, get_pesos_setor

pesos_financeiro = get_pesos_setor('Financeiro')
```
"""

from __future__ import annotations
from typing import Dict

# ---------------------------------------------------------------------------
# Pesos por setor ------------------------------------------------------------
# ---------------------------------------------------------------------------

PESOS_POR_SETOR: Dict[str, Dict[str, Dict[str, float | bool]]] = {
    # === Financeiro =========================================================
    "Financeiro": {
        "ROE_mean":                     {"peso": 0.28, "melhor_alto": True},
        "P/VP_mean":                   {"peso": 0.15, "melhor_alto": False},
        "DY_mean":                     {"peso": 0.15, "melhor_alto": True},
        "Endividamento_Total_mean":    {"peso": 0.05, "melhor_alto": False},
        "Liquidez_Corrente_mean":      {"peso": 0.07, "melhor_alto": True},
        "Margem_Liquida_mean":         {"peso": 0.10, "melhor_alto": True},
        "Lucro_Liquido_slope_log":     {"peso": 0.10, "melhor_alto": True},
        "Momentum_12m":                {"peso": 0.10, "melhor_alto": True},
    },
    # === Tecnologia da Informação ==========================================
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
        "Liquidez_Corrente_mean":       {"peso": 0.04, "melhor_alto": True},
        "Receita_Liquida_slope_log":    {"peso": 0.15, "melhor_alto": True},
        "Lucro_Liquido_slope_log":      {"peso": 0.14, "melhor_alto": True},
        "Patrimonio_Liquido_slope_log": {"peso": 0.05, "melhor_alto": True},
        "Divida_Liquida_slope_log":     {"peso": 0.02, "melhor_alto": False},
        "Caixa_Liquido_slope_log":      {"peso": 0.05, "melhor_alto": True},
        "Momentum_12m":                 {"peso": 0.11, "melhor_alto": True},
    },
    # === Energia ============================================================
    "Energia": {
        "Margem_Liquida_mean":          {"peso": 0.07, "melhor_alto": True},
        "Margem_Operacional_mean":      {"peso": 0.09, "melhor_alto": True},
        "ROE_mean":                     {"peso": 0.06, "melhor_alto": True},
        "ROA_mean":                     {"peso": 0.05, "melhor_alto": True},
        "ROIC_mean":                    {"peso": 0.06, "melhor_alto": True},
        "P/VP_mean":                    {"peso": 0.03, "melhor_alto": False},
        "DY_mean":                      {"peso": 0.16, "melhor_alto": True},
        "Endividamento_Total_mean":     {"peso": 0.08, "melhor_alto": False},
        "Alavancagem_Financeira_mean":  {"peso": 0.05, "melhor_alto": False},
        "Liquidez_Corrente_mean":       {"peso": 0.08, "melhor_alto": True},
        "Receita_Liquida_slope_log":    {"peso": 0.05, "melhor_alto": True},
        "Lucro_Liquido_slope_log":      {"peso": 0.05, "melhor_alto": True},
        "Patrimonio_Liquido_slope_log": {"peso": 0.02, "melhor_alto": True},
        "Divida_Liquida_slope_log":     {"peso": 0.02, "melhor_alto": False},
        "Caixa_Liquido_slope_log":      {"peso": 0.05, "melhor_alto": True},
        "Momentum_12m":                 {"peso": 0.13, "melhor_alto": True},
    },
    # Outras chaves de setor (Industrial, Consumo Cíclico etc.) foram
    # omitidas aqui por brevidade. Adicione as demais entradas completas.
}

# ---------------------------------------------------------------------------
# Pesos genéricos (fallback) --------------------------------------------------
# ---------------------------------------------------------------------------

INDICADORES_SCORE: Dict[str, float] = {
    "Margem_Liquida_mean": 0.15,
    "Margem_Operacional_mean": 0.20,
    "ROE_mean": 0.20,
    "ROA_mean": 0.20,
    "ROIC_mean": 0.20,
    "P/VP_mean": 0.10,
    "DY_mean": 0.30,
    "Endividamento_Total_mean": 0.15,
    "Alavancagem_Financeira_mean": 0.15,
    "Liquidez_Corrente_mean": 0.15,
    "Receita_Liquida_slope_log": 0.15,
    "Lucro_Liquido_slope_log": 0.20,
    "Patrimonio_Liquido_slope_log": 0.15,
    "Divida_Liquida_slope_log": 0.15,
    "Caixa_Liquido_slope_log": 0.15,
    "Momentum_12m": 0.15,
}

# ---------------------------------------------------------------------------
# Helpers --------------------------------------------------------------------
# ---------------------------------------------------------------------------

def get_pesos_setor(setor: str) -> Dict[str, Dict[str, float | bool]]:
    """Retorna o dicionário de pesos para *setor*; se não houver, devolve
    INDICADORES_SCORE num formato compatível (peso, melhor_alto=True)."""
    if setor in PESOS_POR_SETOR:
        return PESOS_POR_SETOR[setor]

    # Converter INDICADORES_SCORE em estrutura aninhada padrão
    return {
        k: {"peso": v, "melhor_alto": True} for k, v in INDICADORES_SCORE.items()
    }

# ---------------------------------------------------------------------------
__all__ = [
    "PESOS_POR_SETOR",
    "INDICADORES_SCORE",
    "get_pesos_setor",
]
