from __future__ import annotations

import io
import json
import sys
import types
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import pandas as pd

if "streamlit" not in sys.modules:
    sys.modules["streamlit"] = types.SimpleNamespace()

if "core.db_loader" not in sys.modules:
    sys.modules["core.db_loader"] = types.SimpleNamespace(
        get_supabase_engine=lambda: None,
    )

if "core.ticker_utils" not in sys.modules:
    sys.modules["core.ticker_utils"] = types.SimpleNamespace(
        normalize_ticker=lambda x: str(x or "").strip().upper(),
    )


from pickup import ingest_docs_cvm_enet as enet
from pickup import ingest_docs_cvm_ipe as ipe
from pickup import ingest_docs_fallback as fallback
from pickup import dados_macro_brasil as macro
from auditoria_dados.ingestion_log import IngestionLog
from core import docs_corporativos_store as docs_store


class _FakeResult:
    def __init__(self, row=None):
        self._row = row

    def first(self):
        return self._row

    def fetchone(self):
        return self._row

    def fetchall(self):
        return self._row or []


class _FakeConn:
    def __init__(self, existing_doc_id=None, insert_row=None):
        self.existing_doc_id = existing_doc_id
        self.insert_row = insert_row
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append({"sql": str(sql), "params": params or {}})
        if self.existing_doc_id is not None and "select id" in str(sql).lower():
            return _FakeResult((self.existing_doc_id,))
        if self.insert_row is not None and "returning id" in str(sql).lower():
            return _FakeResult(self.insert_row)
        return _FakeResult(None)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeEngine:
    def __init__(self, conn):
        self.conn = conn

    def begin(self):
        return self.conn


class _FakeColumnsConn:
    def __init__(self, columns):
        self.columns = columns
        self.exec_sql = []

    def execute(self, sql, params=None):
        lower = str(sql).lower()
        if "information_schema.columns" in lower:
            return _FakeResult([(c,) for c in self.columns])
        raise AssertionError(f"SQL inesperado em fake conn: {sql}")

    def exec_driver_sql(self, sql, records):
        self.exec_sql.append({"sql": sql, "records": records})


class DocsIngestSmokeTests(unittest.TestCase):
    def test_enet_canonical_url_and_hash_are_stable(self):
        url_a = "HTTPS://EXAMPLE.COM/docs//file.pdf?x=1"
        url_b = "https://example.com/docs/file.pdf?x=1"

        self.assertEqual(enet._canonical_url(url_a), enet._canonical_url(url_b))

        hash_a = enet._stable_doc_hash(
            ticker="petr4",
            data="2026-01-31",
            fonte="CVM",
            tipo="enet",
            categoria="Fato Relevante",
            titulo="Teste",
            url=url_a,
        )
        hash_b = enet._stable_doc_hash(
            ticker="PETR4",
            data="2026-01-31",
            fonte="cvm",
            tipo="ENET",
            categoria="fato relevante",
            titulo="  Teste  ",
            url=url_b,
        )
        self.assertEqual(hash_a, hash_b)

    def test_enet_dedupes_docs_by_metadata(self):
        docs = [
            {
                "Assunto": "Fato Relevante",
                "Categoria": "Mercado",
                "TipoDocumento": "Comunicado",
                "DataEntrega": "01/02/2026",
                "Url": "https://example.com/docs//a.pdf",
            },
            {
                "Assunto": "Fato   Relevante",
                "Categoria": "Mercado",
                "TipoDocumento": "Comunicado",
                "DataEntrega": "01/02/2026",
                "Url": "https://example.com/docs/a.pdf",
            },
        ]

        deduped, dropped = enet._dedupe_docs(docs)
        self.assertEqual(len(deduped), 1)
        self.assertEqual(dropped, 1)

    def test_ipe_canonical_url_normalizes_variants(self):
        self.assertEqual(
            ipe._canonical_url("HTTPS://EXAMPLE.COM/docs//arquivo.pdf"),
            ipe._canonical_url("https://example.com/docs/arquivo.pdf"),
        )

    def test_fallback_upsert_skips_existing_metadata_duplicate(self):
        fake_engine = _FakeEngine(_FakeConn())

        with patch.object(fallback, "get_supabase_engine", return_value=fake_engine), patch.object(
            fallback,
            "persist_document_bundle",
            return_value={
                "ok": True,
                "inserted": False,
                "updated_text": False,
                "duplicate": True,
                "doc_hash": "abc123",
                "chunks_inserted": 0,
                "stub": False,
            },
        ):
            resp = fallback.upsert_doc(
                ticker="petr4",
                data="2026-01-31",
                fonte="RI",
                tipo="html",
                titulo="Documento RI",
                url="https://ri.example.com/docs//teste",
                raw_text="texto 1",
            )

        self.assertTrue(resp["ok"])
        self.assertFalse(resp["inserted"])
        self.assertEqual(resp["reason"], "existing_metadata")

    def test_fallback_hash_is_not_affected_by_raw_text(self):
        fake_engine_a = _FakeEngine(_FakeConn())
        with patch.object(fallback, "get_supabase_engine", return_value=fake_engine_a), patch.object(
            fallback,
            "persist_document_bundle",
            return_value={
                "ok": True,
                "inserted": True,
                "updated_text": False,
                "duplicate": False,
                "doc_hash": "hash-igual",
                "chunks_inserted": 2,
                "stub": False,
            },
        ):
            resp_a = fallback.upsert_doc(
                ticker="vale3",
                data="2026-02-15",
                fonte="RI",
                tipo="html",
                titulo="Atualização Operacional",
                url="https://ri.example.com/docs/atualizacao",
                raw_text="texto versão A",
            )

        fake_engine_b = _FakeEngine(_FakeConn())
        with patch.object(fallback, "get_supabase_engine", return_value=fake_engine_b), patch.object(
            fallback,
            "persist_document_bundle",
            return_value={
                "ok": True,
                "inserted": True,
                "updated_text": False,
                "duplicate": False,
                "doc_hash": "hash-igual",
                "chunks_inserted": 3,
                "stub": False,
            },
        ):
            resp_b = fallback.upsert_doc(
                ticker="VALE3",
                data="2026-02-15",
                fonte="RI",
                tipo="html",
                titulo="Atualização Operacional",
                url="https://ri.example.com/docs//atualizacao",
                raw_text="texto versão B",
            )

        self.assertEqual(resp_a["doc_hash"], resp_b["doc_hash"])

    def test_structured_log_is_json(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            fallback._log("INFO", "smoke_event", ticker="PETR4", inserted=1)
        payload = json.loads(buf.getvalue().strip())

        self.assertEqual(payload["pipeline"], "docs_fallback")
        self.assertEqual(payload["level"], "INFO")
        self.assertEqual(payload["event"], "smoke_event")
        self.assertEqual(payload["ticker"], "PETR4")
        self.assertEqual(payload["inserted"], 1)

    def test_ingestion_log_exposes_run_id_and_source_metrics(self):
        log = IngestionLog("docs_ipe")
        log.add_source_metrics(
            source="CVM/IPE",
            ticker="PETR4",
            documents_read=5,
            documents_inserted=2,
            duplicates=1,
            chunks_generated=7,
            stubs=1,
            failures=0,
        )

        summary = log.summary()
        self.assertTrue(summary["run_id"])
        self.assertEqual(summary["metrics"]["source_metrics"]["CVM/IPE:PETR4"]["documents_read"], 5)
        self.assertEqual(summary["metrics"]["source_metrics"]["CVM/IPE:PETR4"]["chunks_generated"], 7)

    def test_docs_store_chunk_hash_changes_with_version(self):
        hash_v1 = docs_store._chunk_hash(
            10,
            0,
            "texto",
            chunking_version="v1",
            extraction_version="pdf_v1",
            content_hash="abc",
        )
        hash_v2 = docs_store._chunk_hash(
            10,
            0,
            "texto",
            chunking_version="v2",
            extraction_version="pdf_v1",
            content_hash="abc",
        )
        self.assertNotEqual(hash_v1, hash_v2)

    def test_macro_upsert_fails_on_schema_drift(self):
        df = pd.DataFrame(
            {
                "data": pd.to_datetime(["2026-01-31"], utc=True),
                "selic": [13.25],
                "cambio": [5.10],
            }
        )
        conn = _FakeColumnsConn(["data", "selic"])

        with self.assertRaises(RuntimeError):
            macro.upsert_dataframe(
                conn,
                "public",
                "info_economica",
                df,
                pk="data",
                expected_columns=["data", "selic", "cambio"],
            )


if __name__ == "__main__":
    unittest.main()
