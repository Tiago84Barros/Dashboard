# core/ticker_utils.py
# Fonte única de verdade para normalização de tickers B3.
#
# Uso:
#   from core.ticker_utils import normalize_ticker, add_sa_suffix
#
# Regras:
#   normalize_ticker  → forma canônica sem sufixo, usada em banco e comparações
#   add_sa_suffix     → forma com sufixo .SA, exigida pelo Yahoo Finance

from __future__ import annotations


def normalize_ticker(ticker: str) -> str:
    """Normaliza um ticker para a forma canônica sem sufixo .SA.

    - Strip de whitespace
    - Uppercase
    - Remove sufixo .SA (case-insensitive via replace)

    Exemplos:
        "petr4"     → "PETR4"
        "PETR4.SA"  → "PETR4"
        " PETR4.SA " → "PETR4"
        ""           → ""
    """
    return (ticker or "").strip().upper().replace(".SA", "")


def add_sa_suffix(ticker: str) -> str:
    """Retorna o ticker com sufixo .SA, no formato exigido pelo Yahoo Finance.

    Aplica normalize_ticker primeiro para garantir forma canônica,
    depois adiciona .SA se ainda não estiver presente.

    Exemplos:
        "petr4"     → "PETR4.SA"
        "PETR4.SA"  → "PETR4.SA"
        " petr4 "   → "PETR4.SA"
        ""           → ""
    """
    t = normalize_ticker(ticker)
    if not t:
        return t
    return f"{t}.SA"
