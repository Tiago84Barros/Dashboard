from __future__ import annotations

"""
Teste de integração documental contra schema controlado em SQLite.

Execução local:
    python -m unittest /workspaces/Dashboard/tests/test_docs_store_integration.py
"""

import os
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine, text


def _identity_decorator(*args, **kwargs):
    def decorator(func):
        return func

    return decorator


if "streamlit" not in sys.modules:
    sys.modules["streamlit"] = types.SimpleNamespace(
        cache_resource=_identity_decorator,
        cache_data=_identity_decorator,
        error=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
        write=lambda *args, **kwargs: None,
    )

if "core.db_loader" not in sys.modules:
    sys.modules["core.db_loader"] = types.SimpleNamespace(
        get_supabase_engine=lambda: None,
    )

if "core.ticker_utils" not in sys.modules:
    sys.modules["core.ticker_utils"] = types.SimpleNamespace(
        normalize_ticker=lambda x: str(x or "").strip().upper(),
    )

from core import docs_corporativos_store as docs_store
from pickup import ingest_docs_cvm_enet as enet
from pickup import ingest_docs_fallback as fallback


def _build_sqlite_engine():
    tmp = tempfile.NamedTemporaryFile(prefix="docs-store-", suffix=".sqlite", delete=False)
    tmp.close()
    engine = create_engine(f"sqlite:///{tmp.name}")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                create table docs_corporativos (
                    id integer primary key autoincrement,
                    ticker text not null,
                    titulo text,
                    url text,
                    fonte text,
                    tipo text,
                    data text,
                    raw_text text,
                    doc_hash text not null unique,
                    texto_chars integer,
                    texto_qualidade text,
                    ingestion_run_id text,
                    extraction_version text,
                    content_hash text,
                    is_stub integer default 0,
                    created_at text default current_timestamp
                )
                """
            )
        )
        conn.execute(
            text(
                """
                create table docs_corporativos_chunks (
                    id integer primary key autoincrement,
                    doc_id integer not null,
                    ticker text not null,
                    chunk_index integer not null,
                    chunk_text text not null,
                    chunk_hash text not null unique,
                    document_date text,
                    categoria text,
                    context_preview text,
                    titulo text,
                    fonte text,
                    url text,
                    chunking_version text,
                    extraction_version text,
                    ingestion_run_id text,
                    content_hash text,
                    is_stub integer default 0
                )
                """
            )
        )
    return tmp.name, engine


class DocsStoreIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.db_path, self.engine = _build_sqlite_engine()

    def tearDown(self):
        self.engine.dispose()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)

    def test_persist_document_bundle_dedup_and_rerun_versioning(self):
        text_v1 = "Primeiro paragrafo.\n\nSegundo paragrafo com bastante contexto para quebrar em chunks."
        text_v2 = text_v1 + "\n\nTerceiro paragrafo novo para simular rerun com texto melhor."

        with self.engine.begin() as conn:
            first = docs_store.persist_document_bundle(
                conn,
                ticker="PETR4",
                titulo="Fato Relevante",
                url="https://example.com/docs/fato.pdf",
                fonte="CVM/IPE",
                tipo="Fato Relevante",
                data="2026-03-31",
                texto=text_v1,
                run_id="run-1",
                chunk_size=40,
                chunk_overlap=10,
                chunking_version="docs_v1",
                extraction_version="pdf_v1",
            )

            second = docs_store.persist_document_bundle(
                conn,
                ticker="PETR4",
                titulo="Fato Relevante",
                url="https://example.com/docs/fato.pdf",
                fonte="CVM/IPE",
                tipo="Fato Relevante",
                data="2026-03-31",
                texto=text_v1,
                run_id="run-2",
                chunk_size=40,
                chunk_overlap=10,
                chunking_version="docs_v1",
                extraction_version="pdf_v1",
            )

            third = docs_store.persist_document_bundle(
                conn,
                ticker="PETR4",
                titulo="Fato Relevante",
                url="https://example.com/docs/fato.pdf",
                fonte="CVM/IPE",
                tipo="Fato Relevante",
                data="2026-03-31",
                texto=text_v2,
                run_id="run-3",
                chunk_size=40,
                chunk_overlap=10,
                chunking_version="docs_v1",
                extraction_version="pdf_v1",
            )

            fourth = docs_store.persist_document_bundle(
                conn,
                ticker="PETR4",
                titulo="Fato Relevante",
                url="https://example.com/docs/fato.pdf",
                fonte="CVM/IPE",
                tipo="Fato Relevante",
                data="2026-03-31",
                texto=text_v2,
                run_id="run-4",
                chunk_size=40,
                chunk_overlap=10,
                chunking_version="docs_v2",
                extraction_version="pdf_v1",
            )

            doc_count = conn.execute(text("select count(*) from docs_corporativos")).scalar()
            chunk_count = conn.execute(text("select count(*) from docs_corporativos_chunks")).scalar()
            versions = conn.execute(
                text(
                    """
                    select distinct chunking_version, extraction_version, ingestion_run_id
                    from docs_corporativos_chunks
                    order by chunk_index
                    """
                )
            ).fetchall()

        self.assertTrue(first["inserted"])
        self.assertGreater(first["chunks_inserted"], 1)
        self.assertTrue(second["duplicate"])
        self.assertEqual(second["chunks_inserted"], 0)
        self.assertTrue(third["updated_text"])
        self.assertTrue(third["chunk_rebuilt"])
        self.assertTrue(fourth["chunk_rebuilt"])
        self.assertEqual(doc_count, 1)
        self.assertGreater(chunk_count, 1)
        self.assertEqual({row[0] for row in versions}, {"docs_v2"})
        self.assertEqual({row[1] for row in versions}, {"pdf_v1"})
        self.assertEqual({row[2] for row in versions}, {"run-4"})

    def test_pipeline_wrappers_use_institutional_store_contract(self):
        with patch.object(fallback, "get_supabase_engine", return_value=self.engine), patch.object(
            enet, "get_supabase_engine", return_value=self.engine
        ):
            fallback_resp = fallback.upsert_doc(
                ticker="VALE3",
                data="2026-02-15",
                fonte="RI",
                tipo="html",
                titulo="Atualização Operacional",
                url="https://ri.example.com/docs/atualizacao",
                raw_text="Texto do RI com detalhes suficientes para gerar chunks e validar a persistência.",
                extraction_version="ri_html_text_v1",
                is_stub=False,
                chunk_size=30,
                chunk_overlap=5,
                run_id="ri-run",
            )

            enet_resp = enet._upsert_doc_and_chunks(
                ticker="VALE3",
                data="2026-02-15",
                fonte="CVM",
                tipo="enet",
                categoria="Comunicado",
                titulo="Atualização Operacional",
                url="https://cvm.example.com/docs/atualizacao",
                raw_text="Texto ENET suficientemente longo para quebrar em mais de um chunk e verificar o contrato comum.",
                chunk_chars=35,
                overlap=5,
                extraction_version="enet_html_text_v1",
                is_stub=False,
                run_id="enet-run",
            )

            with self.engine.begin() as conn:
                docs = conn.execute(
                    text(
                        """
                        select fonte, tipo, extraction_version, ingestion_run_id, is_stub
                        from docs_corporativos
                        order by id
                        """
                    )
                ).fetchall()
                chunks = conn.execute(
                    text(
                        """
                        select fonte, chunking_version, extraction_version, ingestion_run_id
                        from docs_corporativos_chunks
                        order by id
                        """
                    )
                ).fetchall()

        self.assertTrue(fallback_resp["inserted"])
        self.assertTrue(enet_resp["inserted"])
        self.assertGreaterEqual(fallback_resp["chunks_inserted"], 1)
        self.assertGreaterEqual(enet_resp["chunks_inserted"], 1)
        self.assertEqual({row[0] for row in docs}, {"RI", "CVM"})
        self.assertIn(("RI", fallback._CHUNKING_VERSION, "ri_html_text_v1", "ri-run"), set(chunks))
        self.assertIn(("CVM", enet._CHUNKING_VERSION, "enet_html_text_v1", "enet-run"), set(chunks))


if __name__ == "__main__":
    unittest.main()
