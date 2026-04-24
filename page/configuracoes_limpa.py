from __future__ import annotations

import os
from typing import Optional

import streamlit as st

try:
    from core.db import get_engine
    from sqlalchemy import text
except Exception:  # pragma: no cover
    get_engine = None
    text = None


ESSENTIAL_TABLES = [
    'Demonstracoes_Financeiras',
    'Demonstracoes_Financeiras_TRI',
    'multiplos',
    'multiplos_TRI',
    'info_economica',
    'info_economica_mensal',
    'setores',
    'cvm_to_ticker',
    'docs_corporativos',
    'docs_corporativos_chunks',
    'patch6_runs',
    'portfolio_snapshots',
    'portfolio_snapshot_items',
    'portfolio_snapshot_analysis',
]


def _env_status(name: str) -> str:
    return 'OK' if os.getenv(name) else 'Não definida'


def _db_connection_status() -> tuple[str, Optional[str]]:
    if get_engine is None or text is None:
        return 'Indisponível', 'Módulos de conexão não carregados.'
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
        return 'OK', None
    except Exception as exc:
        return 'Falha', str(exc)


def _existing_tables() -> list[str]:
    if get_engine is None or text is None:
        return []
    try:
        engine = get_engine()
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT tablename
                    FROM pg_tables
                    WHERE schemaname = 'public'
                    ORDER BY tablename
                    """
                )
            ).fetchall()
        return [str(r[0]) for r in rows]
    except Exception:
        return []


def _render_environment() -> None:
    st.markdown('## Ambiente')
    cols = st.columns(3)
    cols[0].metric('SUPABASE_DB_URL', _env_status('SUPABASE_DB_URL'))
    cols[1].metric('OPENAI_API_KEY', _env_status('OPENAI_API_KEY'))
    cols[2].metric('AI_MODEL', os.getenv('AI_MODEL', 'Não definido'))

    status, detail = _db_connection_status()
    if status == 'OK':
        st.success('Conexão com o banco disponível.')
    elif status == 'Falha':
        st.error(f'Falha ao conectar no banco: {detail}')
    else:
        st.warning(detail or 'Conexão indisponível.')


def _render_schema_overview() -> None:
    st.markdown('## Estrutura atual do banco')
    tables = _existing_tables()
    if not tables:
        st.info('Não foi possível listar as tabelas do schema public.')
        return

    existing = set(tables)
    summary = []
    for name in ESSENTIAL_TABLES:
        summary.append((name, 'Presente' if name in existing else 'Ausente'))

    left, right = st.columns(2)
    with left:
        st.markdown('### Tabelas esperadas')
        for name, status in summary:
            st.write(f'- {name}: {status}')

    with right:
        st.markdown('### Todas as tabelas públicas')
        for name in tables:
            st.write(f'- {name}')


def _render_migration_notes() -> None:
    st.markdown('## Migração de arquitetura')
    st.info(
        'Esta página foi simplificada para remover o fluxo V2 antigo. '
        'A direção atual é manter o dashboard leve e mover ingestão e tratamento pesados para o pipeline local.'
    )

    st.markdown(
        '- App web: `dashboard.py`, `page/`, `core/`, `design/`\n'
        '- Pipeline local: `pipeline_local/`, parte de `pickup/`, `sql/`, `data/`, `auditoria_dados/`\n'
        '- Remoções em andamento: duplicados, utilitários avulsos e V2 legado'
    )


def render() -> None:
    st.title('Configurações')
    st.caption('Página simplificada e estável, sem dependências do fluxo V2 antigo.')

    _render_environment()
    st.divider()
    _render_schema_overview()
    st.divider()
    _render_migration_notes()
