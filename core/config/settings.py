# core/config/settings.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import os


PROJECT_ROOT = Path(__file__).resolve().parents[2]  # .../core/config/settings.py -> raiz do projeto


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None and v.strip() != "" else default


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


@dataclass(frozen=True)
class Settings:
    # ============================================================
    # NOVO PONTO (decisão de produto): corte histórico
    # ============================================================
    start_year: int = 2010

    # ============================================================
    # Ambiente
    # ============================================================
    environment: str = "production"
    debug: bool = False

    # ============================================================
    # Paths / arquivos auxiliares
    # ============================================================
    data_dir: str = str(PROJECT_ROOT / "data")
    assets_dir: str = str(PROJECT_ROOT / "assets")
    logs_dir: str = str(PROJECT_ROOT / "logs")

    # Campo que seu app está exigindo
    cvm_to_ticker_path: str = str(PROJECT_ROOT / "data" / "cvm_to_ticker.csv")

    # ============================================================
    # Conexões (se o seu projeto usar)
    # ============================================================
    database_url: str = ""
    supabase_url: str = ""
    supabase_key: str = ""


_SETTINGS: Optional[Settings] = None


def get_settings() -> Settings:
    """
    API global esperada pelo app.
    """
    global _SETTINGS
    if _SETTINGS is not None:
        return _SETTINGS

    start_year = _env_int("START_YEAR", 2010)
    environment = _env_str("APP_ENV", "production")
    debug = _env_bool("DEBUG", False)

    data_dir = _env_str("DATA_DIR", str(PROJECT_ROOT / "data"))
    assets_dir = _env_str("ASSETS_DIR", str(PROJECT_ROOT / "assets"))
    logs_dir = _env_str("LOGS_DIR", str(PROJECT_ROOT / "logs"))

    cvm_to_ticker_path = _env_str(
        "CVM_TO_TICKER_PATH",
        str(Path(data_dir) / "cvm_to_ticker.csv"),
    )

    database_url = _env_str("DATABASE_URL", "")
    supabase_url = _env_str("SUPABASE_URL", "")
    supabase_key = _env_str("SUPABASE_KEY", "")

    _SETTINGS = Settings(
        start_year=start_year,
        environment=environment,
        debug=debug,
        data_dir=data_dir,
        assets_dir=assets_dir,
        logs_dir=logs_dir,
        cvm_to_ticker_path=cvm_to_ticker_path,
        database_url=database_url,
        supabase_url=supabase_url,
        supabase_key=supabase_key,
    )
    return _SETTINGS


# Compatibilidade (muitos módulos fazem import direto)
START_YEAR: int = get_settings().start_year
