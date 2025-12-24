# core/config/settings.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import os


@dataclass(frozen=True)
class Settings:
    # Supabase (Postgres)
    supabase_user: str
    supabase_password: str
    supabase_host: str
    supabase_port: int
    supabase_dbname: str

    # Opcional: SQLite local (fallback legado)
    sqlite_path: str = "metadados.db"

    # CSV no repositório
    cvm_to_ticker_path: str = "data/cvm_to_ticker.csv"


def _get_from_streamlit_secrets(key: str) -> Optional[str]:
    try:
        import streamlit as st

        # suporta: st.secrets["SUPABASE_USER"] ou st.secrets["supabase"]["user"]
        if key in st.secrets:
            return str(st.secrets[key])
        if "supabase" in st.secrets and key.lower().replace("supabase_", "") in st.secrets["supabase"]:
            return str(st.secrets["supabase"][key.lower().replace("supabase_", "")])
    except Exception:
        pass
    return None


def _env_or_secret(key: str, default: Optional[str] = None) -> str:
    v = _get_from_streamlit_secrets(key)
    if v is not None and v.strip():
        return v.strip()
    v = os.getenv(key, default or "")
    return (v or "").strip()


def get_settings() -> Settings:
    """
    Lê configuração via:
    1) st.secrets
    2) variáveis de ambiente
    """
    user = _env_or_secret("SUPABASE_USER")
    pwd = _env_or_secret("SUPABASE_PASSWORD")
    host = _env_or_secret("SUPABASE_HOST")
    port = _env_or_secret("SUPABASE_PORT", "5432")
    dbname = _env_or_secret("SUPABASE_DBNAME", "postgres")

    if not all([user, pwd, host, port, dbname]):
        missing = [k for k, v in [
            ("SUPABASE_USER", user),
            ("SUPABASE_PASSWORD", pwd),
            ("SUPABASE_HOST", host),
            ("SUPABASE_PORT", port),
            ("SUPABASE_DBNAME", dbname),
        ] if not v]
        raise RuntimeError(f"Config Supabase incompleta. Faltando: {', '.join(missing)}")

    sqlite_path = _env_or_secret("SQLITE_PATH", "metadados.db")
    cvm_to_ticker_path = _env_or_secret("CVM_TO_TICKER_PATH", "data/cvm_to_ticker.csv")

    return Settings(
        supabase_user=user,
        supabase_password=pwd,
        supabase_host=host,
        supabase_port=int(port),
        supabase_dbname=dbname,
        sqlite_path=sqlite_path,
        cvm_to_ticker_path=cvm_to_ticker_path,
    )
