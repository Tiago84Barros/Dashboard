-- Etapa 1 do motor semântico CVM V2
-- Expande public.cvm_account_map para suportar regras contextuais,
-- mantendo compatibilidade com o modelo atual.

BEGIN;

ALTER TABLE public.cvm_account_map
    ADD COLUMN IF NOT EXISTS canonical_key text,
    ADD COLUMN IF NOT EXISTS rule_scope text,
    ADD COLUMN IF NOT EXISTS source_doc text,
    ADD COLUMN IF NOT EXISTS statement_type text,
    ADD COLUMN IF NOT EXISTS ds_conta_pattern text,
    ADD COLUMN IF NOT EXISTS parent_cd_conta text,
    ADD COLUMN IF NOT EXISTS level_min integer,
    ADD COLUMN IF NOT EXISTS level_max integer,
    ADD COLUMN IF NOT EXISTS sector text,
    ADD COLUMN IF NOT EXISTS company_cvm integer,
    ADD COLUMN IF NOT EXISTS priority integer,
    ADD COLUMN IF NOT EXISTS confidence_score numeric(5,2),
    ADD COLUMN IF NOT EXISTS valid_from date,
    ADD COLUMN IF NOT EXISTS valid_to date,
    ADD COLUMN IF NOT EXISTS notes text,
    ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT NOW();

-- Backfill inicial preservando o comportamento legado.
UPDATE public.cvm_account_map
SET priority = COALESCE(priority, prioridade, 0)
WHERE priority IS NULL;

UPDATE public.cvm_account_map
SET confidence_score = COALESCE(confidence_score, 1.00)
WHERE confidence_score IS NULL;

UPDATE public.cvm_account_map
SET rule_scope = COALESCE(
    NULLIF(rule_scope, ''),
    CASE
        WHEN company_cvm IS NOT NULL THEN 'company'
        WHEN sector IS NOT NULL AND BTRIM(sector) <> '' THEN 'sector'
        ELSE 'global'
    END
)
WHERE rule_scope IS NULL OR BTRIM(rule_scope) = '';

ALTER TABLE public.cvm_account_map
    ALTER COLUMN priority SET DEFAULT 0,
    ALTER COLUMN confidence_score SET DEFAULT 1.00,
    ALTER COLUMN rule_scope SET DEFAULT 'global';

CREATE INDEX IF NOT EXISTS idx_cvm_account_map_active
    ON public.cvm_account_map (ativo);

CREATE INDEX IF NOT EXISTS idx_cvm_account_map_cd_conta_active
    ON public.cvm_account_map (cd_conta, ativo);

CREATE INDEX IF NOT EXISTS idx_cvm_account_map_scope_priority
    ON public.cvm_account_map (rule_scope, priority DESC, confidence_score DESC)
    WHERE ativo = TRUE;

CREATE INDEX IF NOT EXISTS idx_cvm_account_map_company
    ON public.cvm_account_map (company_cvm)
    WHERE ativo = TRUE AND company_cvm IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_cvm_account_map_sector
    ON public.cvm_account_map (sector)
    WHERE ativo = TRUE AND sector IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_cvm_account_map_source_stmt
    ON public.cvm_account_map (source_doc, statement_type)
    WHERE ativo = TRUE;

CREATE INDEX IF NOT EXISTS idx_cvm_account_map_validity
    ON public.cvm_account_map (valid_from, valid_to)
    WHERE ativo = TRUE;

COMMIT;
