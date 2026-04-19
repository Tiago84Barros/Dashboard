"""
core/cvm_v2_schema_check.py
Verificação do schema CVM V2 no banco de dados.

Reutilizável pelos jobs V2 (pickup/) e pela UI (page/configuracoes.py).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

# Objetos obrigatórios do schema CVM V2
# Tuplas (nome_para_display, schema, nome_objeto)
V2_REQUIRED_OBJECTS: List[tuple] = [
    ("public.cvm_ingestion_runs",           "public", "cvm_ingestion_runs"),
    ("public.cvm_financial_raw",            "public", "cvm_financial_raw"),
    ("public.cvm_account_map",              "public", "cvm_account_map"),
    ("public.cvm_financial_normalized",     "public", "cvm_financial_normalized"),
    ("public.demonstracoes_financeiras_v2", "public", "demonstracoes_financeiras_v2"),
    ("public.vw_cvm_normalized_best_source","public", "vw_cvm_normalized_best_source"),
]

# Nomes literais para usar na query IN (evita problema com ANY(:list) em alguns drivers)
_V2_NAMES_LITERAL = ", ".join(f"'{obj[2]}'" for obj in V2_REQUIRED_OBJECTS)


def check_v2_schema(engine=None) -> Dict[str, Any]:
    """Verifica se todos os objetos do schema CVM V2 existem no banco.

    Usa IN com literais em vez de ANY(:list) para máxima compatibilidade
    com diferentes versões de SQLAlchemy e psycopg2.

    Returns:
        {
            "ready":   bool,
            "found":   List[str],   # display names encontrados
            "missing": List[str],   # display names ausentes
            "error":   str | None,
        }
    """
    result: Dict[str, Any] = {
        "ready": False,
        "found": [],
        "missing": [],
        "error": None,
    }

    try:
        if engine is None:
            from core.db import get_engine
            engine = get_engine()

        # Usa IN com valores literais — sem parâmetros de lista que variam por driver
        sql = f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name IN ({_V2_NAMES_LITERAL})
        """
        from sqlalchemy import text as sa_text
        with engine.connect() as conn:
            rows = conn.execute(sa_text(sql)).fetchall()

        found_set = {row[0] for row in rows}
        display_names = {obj[2]: obj[0] for obj in V2_REQUIRED_OBJECTS}

        result["found"] = [display_names[n] for n in [o[2] for o in V2_REQUIRED_OBJECTS] if n in found_set]
        result["missing"] = [display_names[n] for n in [o[2] for o in V2_REQUIRED_OBJECTS] if n not in found_set]
        result["ready"] = len(result["missing"]) == 0

    except Exception as exc:
        result["error"] = str(exc)
        result["ready"] = False

    return result


def get_connection_diagnostics(engine=None) -> Dict[str, Any]:
    """Retorna informações de diagnóstico da conexão e do schema public.

    Útil para confirmar se o app está conectado ao banco correto
    e se o DDL foi aplicado no lugar certo.

    Returns:
        {
            "current_database": str,
            "current_schema":   str,
            "public_table_count": int,
            "public_tables_sample": List[str],   # primeiras 20 tabelas
            "cvm_v2_tables_found": List[str],    # tabelas V2 encontradas (raw names)
            "error": str | None,
        }
    """
    diag: Dict[str, Any] = {
        "current_database": None,
        "current_schema": None,
        "public_table_count": None,
        "public_tables_sample": [],
        "cvm_v2_tables_found": [],
        "error": None,
    }

    try:
        if engine is None:
            from core.db import get_engine
            engine = get_engine()

        from sqlalchemy import text as sa_text
        with engine.connect() as conn:
            # Banco e schema atuais
            row = conn.execute(sa_text(
                "SELECT current_database(), current_schema()"
            )).fetchone()
            diag["current_database"] = row[0]
            diag["current_schema"] = row[1]

            # Todas as tabelas/views em public
            rows = conn.execute(sa_text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' ORDER BY table_name"
            )).fetchall()
            all_tables = [r[0] for r in rows]
            diag["public_table_count"] = len(all_tables)
            diag["public_tables_sample"] = all_tables[:30]

            # Quais das tabelas V2 existem (raw names)
            v2_names = {obj[2] for obj in V2_REQUIRED_OBJECTS}
            diag["cvm_v2_tables_found"] = [t for t in all_tables if t in v2_names]

    except Exception as exc:
        diag["error"] = str(exc)

    return diag


def assert_v2_schema_ready(engine=None) -> None:
    """Valida schema V2 e levanta RuntimeError com mensagem clara se incompleto.

    Projetado para ser chamado no início de main() dos jobs V2.

    Raises:
        RuntimeError: quando algum objeto obrigatório está ausente.
    """
    result = check_v2_schema(engine)

    if result["error"]:
        raise RuntimeError(
            f"Falha ao verificar schema CVM V2: {result['error']}. "
            "Verifique a conexão com o banco e tente novamente."
        )

    if not result["ready"]:
        missing_str = "\n  - ".join(result["missing"])
        raise RuntimeError(
            "Schema CVM V2 incompleto. Aplique o DDL institucional V2 ao banco antes de executar este job.\n\n"
            f"Objetos ausentes:\n  - {missing_str}\n\n"
            "Todos os seguintes objetos devem existir em public antes de executar qualquer job V2:\n"
            + "\n".join(f"  - {obj[0]}" for obj in V2_REQUIRED_OBJECTS)
        )
