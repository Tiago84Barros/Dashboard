# core/config/settings.py
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    supabase_db_url: str | None
    schema: str = "cvm"          # padrão do seu projeto
    sqlite_path: str = "data/metadados.db"  # fallback local


def get_settings() -> Settings:
    # 1) tenta Streamlit secrets (quando rodando em Streamlit)
    supabase_db_url = None
    try:
        import streamlit as st  # type: ignore
        supabase_db_url = st.secrets.get("SUPABASE_DB_URL", None)
    except Exception:
        pass

    # 2) fallback env
    if not supabase_db_url:
        supabase_db_url = os.getenv("SUPABASE_DB_URL")

    return Settings(supabase_db_url=str(supabase_db_url) if supabase_db_url else None)
