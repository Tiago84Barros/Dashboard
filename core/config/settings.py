# core/config/settings.py
from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Optional


# ============================================================
# Modelo de Settings (API pública do projeto)
# ============================================================

@dataclass(frozen=True)
class Settings:
    """
    Configurações globais do projeto.

    IMPORTANTE:
    - Este módulo é importado por vários pontos do app.
    - Ele precisa expor get_settings() para compatibilidade.
    """
    start_year: int = 2010

    # (Opcional) parâmetros comuns que costumam existir em apps streamlit/supabase
    # Mantive genérico e seguro: se não forem usados, não atrapalham.
    environment: str = "production"
    debug: bool = False


# ============================================================
# API LEGADA / ESPERADA PELO PROJETO
# ============================================================

_SETTINGS_SINGLETON: Optional[Settings] = None


def get_settings() -> Settings:
    """
    Retorna um singleton Settings.

    Motivo:
    - Vários módulos provavelmente fazem:
        from core.config.settings import get_settings
        s = get_settings()
    - Se remover, quebra o app (como você viu).
    """
    global _SETTINGS_SINGLETON
    if _SETTINGS_SINGLETON is not None:
        return _SETTINGS_SINGLETON

    # START_YEAR pode ser sobrescrito por variável de ambiente (opcional)
    start_year_env = os.getenv("START_YEAR")
    start_year = int(start_year_env) if start_year_env and start_year_env.isdigit() else 2010

    env = os.getenv("APP_ENV", "production")
    debug = os.getenv("DEBUG", "0") in ("1", "true", "True", "YES", "yes")

    _SETTINGS_SINGLETON = Settings(
        start_year=start_year,
        environment=env,
        debug=debug,
    )
    return _SETTINGS_SINGLETON


# ============================================================
# ATALHOS PARA COMPATIBILIDADE (imports diretos)
# ============================================================

# Muitos módulos fazem: from core.config.settings import START_YEAR
START_YEAR: int = get_settings().start_year
