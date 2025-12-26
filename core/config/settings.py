# core/config/settings.py
from __future__ import annotations

"""
Configurações globais do projeto.

Este arquivo deve conter APENAS decisões de produto e parâmetros
compartilhados entre múltiplos módulos.

Não coloque lógica de negócio aqui.
"""

# ============================================================
# DECISÕES DE PRODUTO / CORTE HISTÓRICO
# ============================================================

# Ano inicial para ingestões macroeconômicas (BCB, IBGE, etc.)
START_YEAR: int = 2010

# ============================================================
# (se já existirem outros settings seus abaixo, eles podem
# continuar normalmente — o importante é que START_YEAR
# exista no topo e sem condicional)
# ============================================================
