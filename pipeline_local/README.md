# Pipeline local

Estrutura inicial para executar ingestão, transformação e publicação fora do Supabase.

## Objetivo

- usar banco local para ingestão e staging pesados
- usar Supabase apenas como camada final do app
- separar processamento bruto de serving online

## Blocos iniciais

- `config/settings.py`: variáveis de ambiente e parâmetros do pipeline
- `config/connections.py`: conexões com banco local e Supabase
- `sql/create_local_tables.sql`: DDL inicial das tabelas locais

## Variáveis esperadas

### Banco local
- `LOCAL_DB_URL`

### Supabase
- `SUPABASE_DB_URL` ou `DATABASE_URL`

### Pipeline
- `PIPELINE_LOCAL_SCHEMA`
- `PIPELINE_LOG_LEVEL`
- `PIPELINE_BATCH_SIZE`
- `PIPELINE_CHUNK_SIZE`
- `PIPELINE_START_YEAR`
- `PIPELINE_END_YEAR`

## Próximos scripts sugeridos

- `extract/extract_cvm_dfp_local.py`
- `extract/extract_cvm_itr_local.py`
- `transform/enrich_cvm_raw_local.py`
- `transform/build_dre_annual_local.py`
- `transform/build_dre_quarterly_local.py`
- `publish/publish_financials_annual_supabase.py`
- `publish/publish_financials_quarterly_supabase.py`
