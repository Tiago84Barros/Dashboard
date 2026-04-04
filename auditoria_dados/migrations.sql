-- auditoria_dados/migrations.sql
--
-- Alterações de schema para suportar auditoria, log de ingestão e versionamento.
-- Rodar manualmente no Supabase SQL Editor ou via psql.
-- Todos os comandos são idempotentes (IF NOT EXISTS / IF EXISTS).
--
-- Aplicação segura recomendada:
-- 1. Executar primeiro em staging ou clone do banco, nunca direto em produção.
-- 2. Fazer snapshot/backup de:
--      public.ingestion_log
--      public.docs_corporativos
--      public.docs_corporativos_chunks
--      public.patch6_runs
-- 3. Rodar pré-checagens antes desta migration:
--      -- docs duplicados por hash
--      SELECT doc_hash, COUNT(*)
--      FROM public.docs_corporativos
--      WHERE doc_hash IS NOT NULL
--      GROUP BY doc_hash
--      HAVING COUNT(*) > 1;
--
--      -- possíveis duplicatas semânticas
--      SELECT
--          upper(coalesce(ticker, '')) AS ticker,
--          lower(coalesce(fonte, '')) AS fonte,
--          lower(coalesce(tipo, '')) AS tipo,
--          lower(coalesce(titulo, '')) AS titulo,
--          lower(coalesce(url, '')) AS url,
--          coalesce(data::date::text, '') AS data_ref,
--          COUNT(*)
--      FROM public.docs_corporativos
--      GROUP BY 1,2,3,4,5,6
--      HAVING COUNT(*) > 1;
--
--      -- chunks órfãos
--      SELECT COUNT(*)
--      FROM public.docs_corporativos_chunks c
--      LEFT JOIN public.docs_corporativos d ON d.id = c.doc_id
--      WHERE d.id IS NULL;
--
-- 4. Aplicar a migration.
-- 5. Executar rerun controlado de IPE, ENET e fallback em staging e validar:
--      - rerun sem duplicação semântica
--      - rebuild de chunks só quando conteúdo ou versão mudar
--      - novas execuções com run_id / extraction_version / chunking_version
-- 6. Só então promover para produção.
--

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Tabela de log de ingestão
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.ingestion_log (
    id            BIGSERIAL PRIMARY KEY,
    pipeline      TEXT NOT NULL,          -- 'dfp' | 'itr' | 'multiplos' | 'macro' | 'docs_ipe' | 'docs_enet' | ...
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    status        TEXT NOT NULL DEFAULT 'running',  -- 'running' | 'success' | 'partial' | 'failed'
    rows_inserted INT DEFAULT 0,
    rows_updated  INT DEFAULT 0,
    rows_skipped  INT DEFAULT 0,
    errors_count  INT DEFAULT 0,
    params        JSONB,                  -- parâmetros usados na execução (tickers, período etc.)
    error_detail  TEXT                    -- última mensagem de erro (truncada em 2000 chars)
);

CREATE INDEX IF NOT EXISTS idx_ingestion_log_pipeline
    ON public.ingestion_log (pipeline, started_at DESC);

ALTER TABLE public.ingestion_log
    ADD COLUMN IF NOT EXISTS run_id TEXT,
    ADD COLUMN IF NOT EXISTS warnings_count INT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS metrics JSONB,
    ADD COLUMN IF NOT EXISTS events JSONB;

CREATE UNIQUE INDEX IF NOT EXISTS uq_ingestion_log_run_id
    ON public.ingestion_log (run_id)
    WHERE run_id IS NOT NULL;

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Versão de schema no patch6_runs
-- ─────────────────────────────────────────────────────────────────────────────

ALTER TABLE public.patch6_runs
    ADD COLUMN IF NOT EXISTS schema_version TEXT DEFAULT 'v1';

-- Atualizar runs existentes sem schema_version
UPDATE public.patch6_runs
SET schema_version = 'v1'
WHERE schema_version IS NULL;

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Qualidade de texto nos documentos corporativos
-- ─────────────────────────────────────────────────────────────────────────────

ALTER TABLE public.docs_corporativos
    ADD COLUMN IF NOT EXISTS texto_chars     INT,      -- comprimento de raw_text em chars
    ADD COLUMN IF NOT EXISTS texto_qualidade TEXT,     -- 'ok' | 'vazio' | 'curto' | 'ruido'
    ADD COLUMN IF NOT EXISTS ingestion_run_id TEXT,
    ADD COLUMN IF NOT EXISTS extraction_version TEXT,
    ADD COLUMN IF NOT EXISTS content_hash TEXT,
    ADD COLUMN IF NOT EXISTS is_stub BOOLEAN DEFAULT FALSE;

-- Backfill para documentos existentes
UPDATE public.docs_corporativos
SET
    texto_chars     = LENGTH(COALESCE(raw_text, '')),
    texto_qualidade = CASE
        WHEN raw_text IS NULL OR LENGTH(raw_text) = 0 THEN 'vazio'
        WHEN LENGTH(raw_text) < 50                    THEN 'curto'
        ELSE 'ok'
    END
WHERE texto_qualidade IS NULL;

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. Índice de busca em docs_corporativos por ticker + data (RAG performance)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_docs_corporativos_ticker_data
    ON public.docs_corporativos (ticker, data DESC NULLS LAST, id DESC);

CREATE INDEX IF NOT EXISTS idx_docs_chunks_doc_id
    ON public.docs_corporativos_chunks (doc_id, chunk_index ASC);

ALTER TABLE public.docs_corporativos_chunks
    ADD COLUMN IF NOT EXISTS chunking_version TEXT,
    ADD COLUMN IF NOT EXISTS extraction_version TEXT,
    ADD COLUMN IF NOT EXISTS ingestion_run_id TEXT,
    ADD COLUMN IF NOT EXISTS content_hash TEXT,
    ADD COLUMN IF NOT EXISTS is_stub BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS context_preview TEXT,
    ADD COLUMN IF NOT EXISTS titulo TEXT,
    ADD COLUMN IF NOT EXISTS fonte TEXT,
    ADD COLUMN IF NOT EXISTS url TEXT;

CREATE INDEX IF NOT EXISTS idx_docs_chunks_doc_version
    ON public.docs_corporativos_chunks (doc_id, chunking_version, extraction_version);

-- ─────────────────────────────────────────────────────────────────────────────
-- 5. Constraint explícita em Demonstracoes_Financeiras (se não existir)
-- ─────────────────────────────────────────────────────────────────────────────

-- Nota: se já existir uma PK ou UNIQUE, este bloco pode ser ignorado.
-- Verificar antes de rodar: \d "Demonstracoes_Financeiras"

-- CREATE UNIQUE INDEX IF NOT EXISTS uq_df_ticker_data
--     ON public."Demonstracoes_Financeiras" ("Ticker", "Data");

-- CREATE UNIQUE INDEX IF NOT EXISTS uq_df_tri_ticker_data
--     ON public."Demonstracoes_Financeiras_TRI" ("Ticker", "Data");

-- ─────────────────────────────────────────────────────────────────────────────
-- 6. View de cobertura de dados (diagnóstico rápido)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW public.v_cobertura_dados AS
SELECT
    s.ticker,
    s."SETOR",
    s."SUBSETOR",
    COUNT(DISTINCT df."Data")      AS anos_df,
    MAX(df."Data")                 AS ultimo_df,
    COUNT(DISTINCT tri."Data")     AS trimestres_tri,
    MAX(tri."Data")                AS ultimo_tri,
    COUNT(DISTINCT m."Data")       AS anos_multiplos,
    COUNT(DISTINCT d.id)           AS n_docs,
    MAX(d.data)                    AS ultimo_doc,
    COUNT(DISTINCT p.period_ref)   AS n_patch6_runs,
    MAX(p.created_at)              AS ultimo_patch6
FROM
    public.setores s
    LEFT JOIN public."Demonstracoes_Financeiras"     df  ON df."Ticker"  = s.ticker
    LEFT JOIN public."Demonstracoes_Financeiras_TRI" tri ON tri."Ticker" = s.ticker
    LEFT JOIN public.multiplos                        m   ON m."Ticker"   = s.ticker
    LEFT JOIN public.docs_corporativos                d   ON d.ticker     = s.ticker
    LEFT JOIN public.patch6_runs                      p   ON p.ticker     = s.ticker
GROUP BY s.ticker, s."SETOR", s."SUBSETOR"
ORDER BY s.ticker;

COMMENT ON VIEW public.v_cobertura_dados IS
    'Visão consolidada de cobertura de dados por ticker: anos de DF, trimestres ITR, docs, runs Patch6.';
