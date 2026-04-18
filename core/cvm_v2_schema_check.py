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
    ("public.cvm_ingestion_runs",          "public", "cvm_ingestion_runs"),
    ("public.cvm_financial_raw",           "public", "cvm_financial_raw"),
    ("public.cvm_account_map",             "public", "cvm_account_map"),
    ("public.cvm_financial_normalized",    "public", "cvm_financial_normalized"),
    ("public.demonstracoes_financeiras_v2","public", "demonstracoes_financeiras_v2"),
    ("public.vw_cvm_normalized_best_source","public","vw_cvm_normalized_best_source"),
]


def check_v2_schema(engine=None) -> Dict[str, Any]:
    """Verifica se todos os objetos do schema CVM V2 existem no banco.

    Args:
        engine: SQLAlchemy engine opcional. Se None, tenta obter via core.db.get_engine().

    Returns:
        {
            "ready":   bool,           # True somente se todos os objetos estão presentes
            "found":   List[str],      # nomes dos objetos encontrados
            "missing": List[str],      # nomes dos objetos ausentes
            "error":   str | None,     # mensagem de erro se a verificação falhou
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

        names = [obj[2] for obj in V2_REQUIRED_OBJECTS]

        # information_schema.tables inclui tanto tabelas quanto views
        sql = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = ANY(:names)
        """
        from sqlalchemy import text as sa_text
        with engine.connect() as conn:
            rows = conn.execute(sa_text(sql), {"names": names}).fetchall()

        found_set = {row[0] for row in rows}
        all_names = [obj[2] for obj in V2_REQUIRED_OBJECTS]
        display_names = {obj[2]: obj[0] for obj in V2_REQUIRED_OBJECTS}

        result["found"] = [display_names[n] for n in all_names if n in found_set]
        result["missing"] = [display_names[n] for n in all_names if n not in found_set]
        result["ready"] = len(result["missing"]) == 0

    except Exception as exc:
        result["error"] = str(exc)
        result["ready"] = False

    return result


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
