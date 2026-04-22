-- Validação da Etapa 1 do motor semântico CVM V2
-- Rode após aplicar a migration e o seed mínimo de teste.

-- 1) Conferir estrutura expandida da tabela de regras
SELECT
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'cvm_account_map'
  AND column_name IN (
    'canonical_key', 'rule_scope', 'source_doc', 'statement_type',
    'ds_conta_pattern', 'parent_cd_conta', 'level_min', 'level_max',
    'sector', 'company_cvm', 'priority', 'confidence_score',
    'valid_from', 'valid_to', 'notes', 'created_at', 'updated_at'
  )
ORDER BY column_name;

-- 2) Conferir se o seed mínimo entrou
SELECT
    cd_conta,
    canonical_key,
    rule_scope,
    statement_type,
    priority,
    confidence_score,
    valid_from,
    valid_to,
    ativo
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

-- 3) Conferir volume bruto vs normalizado antes do teste
SELECT 'raw' AS camada, COUNT(*) AS total FROM public.cvm_financial_raw
UNION ALL
SELECT 'normalized' AS camada, COUNT(*) AS total FROM public.cvm_financial_normalized;

-- 4) Contas do seed presentes no bruto
SELECT
    cd_conta,
    MIN(ds_conta) AS descricao_exemplo,
    COUNT(*) AS ocorrencias
FROM public.cvm_financial_raw
WHERE cd_conta IN ('3.01', '3.02', '3.03', '3.05', '3.11', '3.99.01.01')
GROUP BY cd_conta
ORDER BY ocorrencias DESC;

-- 5) Linhas já normalizadas para as chaves do seed
SELECT
    canonical_key,
    COUNT(*) AS total
FROM public.cvm_financial_normalized
WHERE canonical_key IN (
    'receita_liquida',
    'custo_bens_servicos',
    'resultado_bruto',
    'lucro_liquido',
    'outras_receitas_despesas_operacionais',
    'lucro_por_acao_basico'
)
GROUP BY canonical_key
ORDER BY total DESC;

-- 6) Último run MAP_V2 com métricas enriquecidas da Etapa 1
SELECT
    run_id,
    status,
    finished_at,
    metrics->>'message' AS message,
    metrics->>'total_raw' AS total_raw,
    metrics->>'total_matched' AS total_matched,
    metrics->>'total_inserted' AS total_inserted,
    metrics->>'mapped_fast' AS mapped_fast,
    metrics->>'mapped_contextual' AS mapped_contextual,
    metrics->>'mapped_regex' AS mapped_regex,
    metrics->>'ambiguous_rows' AS ambiguous_rows,
    metrics->>'unmatched_rows' AS unmatched_rows
FROM public.cvm_ingestion_runs
WHERE source_doc = 'MAP_V2'
ORDER BY finished_at DESC NULLS LAST, updated_at DESC
LIMIT 5;

-- 7) Conferir sinais de ambiguidade ou ausência de match
SELECT
    status,
    metrics->>'ambiguous_rows' AS ambiguous_rows,
    metrics->>'unmatched_rows' AS unmatched_rows,
    metrics->>'mapped_fast' AS mapped_fast,
    metrics->>'mapped_contextual' AS mapped_contextual,
    metrics->>'mapped_regex' AS mapped_regex,
    finished_at
FROM public.cvm_ingestion_runs
WHERE source_doc = 'MAP_V2'
ORDER BY finished_at DESC NULLS LAST, updated_at DESC
LIMIT 10;

-- 8) Amostra das linhas normalizadas do seed para inspeção
SELECT
    ticker,
    source_doc,
    tipo_demo,
    dt_refer,
    canonical_key,
    valor,
    qualidade_mapeamento
FROM public.cvm_financial_normalized
WHERE canonical_key IN (
    'receita_liquida',
    'custo_bens_servicos',
    'resultado_bruto',
    'lucro_liquido',
    'outras_receitas_despesas_operacionais',
    'lucro_por_acao_basico'
)
ORDER BY dt_refer DESC
LIMIT 50;
