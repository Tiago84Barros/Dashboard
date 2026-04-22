-- Seed mínimo de validação da Etapa 1 do motor semântico CVM V2
-- Objetivo: testar prioridade, confiança, regex, validade temporal
-- e compatibilidade com o pipeline atual.

BEGIN;

-- Regras globais simples e estáveis (DRE)
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

-- Regra contextual temporal para validar janela de vigência
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

-- Regra regex sem cd_conta para validar fallback textual
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
