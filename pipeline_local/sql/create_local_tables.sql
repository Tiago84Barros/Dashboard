CREATE SCHEMA IF NOT EXISTS pipeline_local;

CREATE TABLE IF NOT EXISTS pipeline_local.pipeline_runs_local (
    run_id text PRIMARY KEY,
    pipeline_name text NOT NULL,
    status text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    metrics jsonb,
    error_message text
);

CREATE TABLE IF NOT EXISTS pipeline_local.cvm_dfp_raw_local (
    id bigserial PRIMARY KEY,
    source_doc text NOT NULL,
    tipo_demo text,
    grupo_demo text,
    arquivo_origem text,
    cd_cvm integer,
    cnpj_cia text,
    denom_cia text,
    ticker text,
    versao integer,
    ordem_exerc text,
    dt_refer date,
    dt_ini_exerc date,
    dt_fim_exerc date,
    cd_conta text,
    ds_conta text,
    nivel_conta integer,
    conta_pai text,
    vl_conta numeric,
    escala_moeda text,
    moeda text,
    st_conta_fixa text,
    row_hash text,
    payload jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pipeline_local.cvm_itr_raw_local (
    id bigserial PRIMARY KEY,
    source_doc text NOT NULL,
    tipo_demo text,
    grupo_demo text,
    arquivo_origem text,
    cd_cvm integer,
    cnpj_cia text,
    denom_cia text,
    ticker text,
    versao integer,
    ordem_exerc text,
    dt_refer date,
    dt_ini_exerc date,
    dt_fim_exerc date,
    cd_conta text,
    ds_conta text,
    nivel_conta integer,
    conta_pai text,
    vl_conta numeric,
    escala_moeda text,
    moeda text,
    st_conta_fixa text,
    row_hash text,
    payload jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pipeline_local.cvm_raw_enriched_local (
    id bigserial PRIMARY KEY,
    source_doc text NOT NULL,
    tipo_demo text,
    grupo_demo text,
    arquivo_origem text,
    cd_cvm integer,
    cnpj_cia text,
    denom_cia text,
    ticker text,
    dt_refer date,
    cd_conta text,
    ds_conta text,
    conta_pai text,
    nivel_conta integer,
    vl_conta numeric,
    period_year integer,
    period_quarter integer,
    period_month integer,
    period_label text,
    fiscal_period_type text,
    account_depth integer,
    top_account_code text,
    account_code_root text,
    is_leaf_account boolean,
    normalized_ds_conta text,
    normalized_denom_cia text,
    is_consolidated boolean,
    is_individual boolean,
    is_annual boolean,
    is_quarterly boolean,
    unit_scale_factor numeric(20,4),
    value_normalized_brl numeric,
    row_hash text,
    payload jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pipeline_local.dre_annual_local (
    id bigserial PRIMARY KEY,
    ticker text NOT NULL,
    cd_cvm integer,
    dt_refer date NOT NULL,
    period_label text,
    receita_liquida numeric,
    custo_bens_servicos numeric,
    resultado_bruto numeric,
    lucro_liquido numeric,
    outras_receitas_despesas_operacionais numeric,
    lucro_por_acao_basico numeric,
    source_doc text,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (ticker, dt_refer)
);

CREATE TABLE IF NOT EXISTS pipeline_local.dre_quarterly_local (
    id bigserial PRIMARY KEY,
    ticker text NOT NULL,
    cd_cvm integer,
    dt_refer date NOT NULL,
    period_label text,
    receita_liquida numeric,
    custo_bens_servicos numeric,
    resultado_bruto numeric,
    lucro_liquido numeric,
    outras_receitas_despesas_operacionais numeric,
    lucro_por_acao_basico numeric,
    source_doc text,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (ticker, dt_refer)
);

CREATE INDEX IF NOT EXISTS idx_cvm_dfp_raw_local_dt_refer
    ON pipeline_local.cvm_dfp_raw_local (dt_refer);

CREATE INDEX IF NOT EXISTS idx_cvm_dfp_raw_local_cd_conta
    ON pipeline_local.cvm_dfp_raw_local (cd_conta);

CREATE INDEX IF NOT EXISTS idx_cvm_itr_raw_local_dt_refer
    ON pipeline_local.cvm_itr_raw_local (dt_refer);

CREATE INDEX IF NOT EXISTS idx_cvm_itr_raw_local_cd_conta
    ON pipeline_local.cvm_itr_raw_local (cd_conta);

CREATE INDEX IF NOT EXISTS idx_cvm_raw_enriched_local_period
    ON pipeline_local.cvm_raw_enriched_local (period_year, period_quarter);

CREATE INDEX IF NOT EXISTS idx_cvm_raw_enriched_local_cd_conta
    ON pipeline_local.cvm_raw_enriched_local (cd_conta);

CREATE INDEX IF NOT EXISTS idx_cvm_raw_enriched_local_ticker_dt
    ON pipeline_local.cvm_raw_enriched_local (ticker, dt_refer);
