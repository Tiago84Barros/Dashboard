# core/config/settings.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

import os


@dataclass(frozen=True)
class Settings:
    # Supabase (Postgres)
    supabase_user: str
    supabase_password: str
    supabase_host: str
    supabase_port: int
    supabase_dbname: str

    # Opcional: URL completa (preferencial). Mantida aqui para compat/uso futuro.
    supabase_db_url: str = ""

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


def _parse_db_url(db_url: str) -> tuple[str, str, str, int, str]:
    """
    Faz parse de SUPABASE_DB_URL do tipo:
      postgresql+psycopg2://user:pass@host:5432/dbname
      postgresql://user:pass@host:5432/dbname
    Retorna: (user, password, host, port, dbname)
    """
    # urlparse não entende o "+psycopg2" como scheme padrão de postgres,
    # mas ainda parseia corretamente.
    u = urlparse(db_url)

    user = u.username or ""
    pwd = u.password or ""
    host = u.hostname or ""
    port = int(u.port or 5432)
    dbname = (u.path or "").lstrip("/") or ""

    return user, pwd, host, port, dbname


def get_settings() -> Settings:
    """
    Lê configuração via:
    1) st.secrets
    2) variáveis de ambiente

    Prioridade:
    A) SUPABASE_DB_URL (recomendado) -> não exige SUPABASE_USER/...
    B) SUPABASE_DB_* (componentes novos) -> não exige SUPABASE_USER/...
    C) SUPABASE_* (componentes legados)
    """

    # ──────────────────────────────────────────────────────────────
    # A) Preferencial: URL completa
    # ──────────────────────────────────────────────────────────────
    db_url = _env_or_secret("SUPABASE_DB_URL")
    if db_url:
        user, pwd, host, port, dbname = _parse_db_url(db_url)
        missing = [k for k, v in [
            ("SUPABASE_DB_URL(user)", user),
            ("SUPABASE_DB_URL(password)", pwd),
            ("SUPABASE_DB_URL(host)", host),
            ("SUPABASE_DB_URL(dbname)", dbname),
        ] if not v]
        if missing:
            raise RuntimeError(
                "SUPABASE_DB_URL definido, mas incompleto/ inválido. Faltando: "
                + ", ".join(missing)
            )

        sqlite_path = _env_or_secret("SQLITE_PATH", "metadados.db")
        cvm_to_ticker_path = _env_or_secret("CVM_TO_TICKER_PATH", "data/cvm_to_ticker.csv")

        return Settings(
            supabase_user=user,
            supabase_password=pwd,
            supabase_host=host,
            supabase_port=int(port),
            supabase_dbname=dbname,
            supabase_db_url=db_url,
            sqlite_path=sqlite_path,
            cvm_to_ticker_path=cvm_to_ticker_path,
        )

    # ──────────────────────────────────────────────────────────────
    # B) Componentes novos: SUPABASE_DB_*
    # ──────────────────────────────────────────────────────────────
    user = _env_or_secret("SUPABASE_DB_USER")
    pwd = _env_or_secret("SUPABASE_DB_PASSWORD")
    host = _env_or_secret("SUPABASE_DB_HOST")
    port = _env_or_secret("SUPABASE_DB_PORT", "5432")
    dbname = _env_or_secret("SUPABASE_DB_NAME", "postgres")

    if all([user, pwd, host, port, dbname]):
        sqlite_path = _env_or_secret("SQLITE_PATH", "metadados.db")
        cvm_to_ticker_path = _env_or_secret("CVM_TO_TICKER_PATH", "data/cvm_to_ticker.csv")

        return Settings(
            supabase_user=user,
            supabase_password=pwd,
            supabase_host=host,
            supabase_port=int(port),
            supabase_dbname=dbname,
            supabase_db_url="",
            sqlite_path=sqlite_path,
            cvm_to_ticker_path=cvm_to_ticker_path,
        )

    # ──────────────────────────────────────────────────────────────
    # C) Legado: SUPABASE_*
    # ──────────────────────────────────────────────────────────────
    user = _env_or_secret("SUPABASE_USER")
    pwd = _env_or_secret("SUPABASE_PASSWORD")
    host = _env_or_secret("SUPABASE_HOST")
    port = _env_or_secret("SUPABASE_PORT", "5432")
    dbname = _env_or_secret("SUPABASE_DBNAME", "postgres")

    if not all([user, pwd, host, port, dbname]):
        missing = [k for k, v in [
            ("SUPABASE_DB_URL", db_url),
            ("SUPABASE_DB_USER", _env_or_secret("SUPABASE_DB_USER")),
            ("SUPABASE_DB_PASSWORD", _env_or_secret("SUPABASE_DB_PASSWORD")),
            ("SUPABASE_DB_HOST", _env_or_secret("SUPABASE_DB_HOST")),
            ("SUPABASE_DB_PORT", _env_or_secret("SUPABASE_DB_PORT")),
            ("SUPABASE_DB_NAME", _env_or_secret("SUPABASE_DB_NAME")),
            ("SUPABASE_USER", user),
            ("SUPABASE_PASSWORD", pwd),
            ("SUPABASE_HOST", host),
            ("SUPABASE_PORT", port),
            ("SUPABASE_DBNAME", dbname),
        ] if not v]
        # Remove duplicados e deixa mais legível
        missing_unique = []
        for m in missing:
            if m not in missing_unique:
                missing_unique.append(m)
        raise RuntimeError(
            "Config Supabase incompleta. Configure SUPABASE_DB_URL (recomendado) "
            "ou SUPABASE_DB_* (componentes) ou SUPABASE_* (legado). "
            f"Faltando: {', '.join(missing_unique)}"
        )

    sqlite_path = _env_or_secret("SQLITE_PATH", "metadados.db")
    cvm_to_ticker_path = _env_or_secret("CVM_TO_TICKER_PATH", "data/cvm_to_ticker.csv")

    return Settings(
        supabase_user=user,
        supabase_password=pwd,
        supabase_host=host,
        supabase_port=int(port),
        supabase_dbname=dbname,
        supabase_db_url="",
        sqlite_path=sqlite_path,
        cvm_to_ticker_path=cvm_to_ticker_path,
    )
