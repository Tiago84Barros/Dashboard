# core/config/settings.py
from __future__ import annotations

"""
Arquivo de CONFIGURAÇÃO GLOBAL (sem lógica de negócio).
- Não deve importar pandas / sqlalchemy / engine.
- Deve conter apenas constantes e decisões de produto.
"""

# ============================================================
# DECISÕES DE PRODUTO / CORTE DE HISTÓRICO
# ============================================================

# Ano mínimo para ingestões/consultas (ex.: Macro BCB, etc.)
START_YEAR: int = 2010

# ============================================================
# BANCO / SCHEMAS PADRÃO
# ============================================================

CVM_SCHEMA: str = "cvm"
