-- Bootstrap único da Etapa 1 do motor semântico CVM V2
-- Ordem sugerida de execução:
-- 1) Expandir estrutura da public.cvm_account_map
-- 2) Popular seed mínimo de validação
-- 3) Rodar checks iniciais rápidos

BEGIN;

-- =========================================================
-- MIGRATION: expansão contextual da tabela de regras
-- =========================================================
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

-- =========================================================
-- SEED: conjunto mínimo de validação
-- =========================================================
INSERT INTO public.cvm_account_map (
    cd_conta,
    ds_conta_pattern,
    canonical_key,
    sinal,
    prioridade,
    priority,
    confidence_score,
    rule_scope,
    source_doc,
    statement_type,
    valid_from,
    valid_to,
    notes,
    ativo
)
VALUES
    (
        '3.01',
        '(?i)receita',
        'receita_liquida',
        1,
        100,
        100,
        0.99,
        'global',
        NULL,
        'dre',
        NULL,
        NULL,
        'Regra global estável para receita líquida',
        TRUE
    ),
    (
        '3.02',
        '(?i)custo',
        'custo_bens_servicos',
        -1,
        100,
        100,
        0.99,
        'global',
        NULL,
        'dre',
        NULL,
        NULL,
        'Regra global estável para custo',
        TRUE
    ),
    (
        '3.03',
        '(?i)resultado bruto|lucro bruto',
        'resultado_bruto',
        1,
        100,
        100,
        0.98,
        'global',
        NULL,
        'dre',
        NULL,
        NULL,
        'Regra global estável para resultado bruto',
        TRUE
    ),
    (
        '3.11',
        '(?i)lucro.*liquido|prejuizo.*liquido',
        'lucro_liquido',
        1,
        100,
        100,
        0.99,
        'global',
        NULL,
        'dre',
        NULL,
        NULL,
        'Regra global estável para lucro líquido',
        TRUE
    )
ON CONFLICT DO NOTHING;

INSERT INTO public.cvm_account_map (
    cd_conta,
    ds_conta_pattern,
    canonical_key,
    sinal,
    prioridade,
    priority,
    confidence_score,
    rule_scope,
    source_doc,
    statement_type,
    valid_from,
    valid_to,
    notes,
    ativo
)
VALUES
    (
        '3.05',
        '(?i)outras receitas|outras despesas',
        'outras_receitas_despesas_operacionais',
        1,
        80,
        80,
        0.85,
        'global',
        NULL,
        'dre',
        DATE '2010-01-01',
        DATE '2026-12-31',
        'Regra temporal de teste para 3.05',
        TRUE
    )
ON CONFLICT DO NOTHING;

INSERT INTO public.cvm_account_map (
    cd_conta,
    ds_conta_pattern,
    canonical_key,
    sinal,
    prioridade,
    priority,
    confidence_score,
    rule_scope,
    source_doc,
    statement_type,
    notes,
    ativo
)
VALUES
    (
        NULL,
        '(?i)lucro basico por acao|lucro básico por ação',
        'lucro_por_acao_basico',
        1,
        60,
        60,
        0.80,
        'global',
        NULL,
        'dre',
        'Fallback regex para lucro por ação básico',
        TRUE
    )
ON CONFLICT DO NOTHING;

COMMIT;

-- =========================================================
-- CHECKS rápidos pós-bootstrap
-- =========================================================
SELECT
    COUNT(*) AS regras_ativas_total,
    COUNT(*) FILTER (WHERE COALESCE(rule_scope, 'global') = 'global') AS regras_globais,
    COUNT(*) FILTER (WHERE company_cvm IS NOT NULL) AS regras_por_empresa,
    COUNT(*) FILTER (WHERE sector IS NOT NULL AND BTRIM(sector) <> '') AS regras_por_setor,
    COUNT(*) FILTER (WHERE ds_conta_pattern IS NOT NULL AND BTRIM(ds_conta_pattern) <> '') AS regras_com_regex,
    COUNT(*) FILTER (WHERE valid_from IS NOT NULL OR valid_to IS NOT NULL) AS regras_com_validade,
    COUNT(*) FILTER (WHERE COALESCE(confidence_score, 0) >= 0.90) AS regras_alta_confianca
FROM public.cvm_account_map
WHERE ativo = TRUE;

SELECT
    cd_conta,
    canonical_key,
    statement_type,
    priority,
    confidence_score,
    valid_from,
    valid_to
FROM public.cvm_account_map
WHERE canonical_key IN (
    'receita_liquida',
    'custo_bens_servicos',
    'resultado_bruto',
    'lucro_liquido',
    'outras_receitas_despesas_operacionais',
    'lucro_por_acao_basico'
)
ORDER BY priority DESC, canonical_key;
