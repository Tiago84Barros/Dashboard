# Diagnóstico Executivo — Arquitetura de Dados

**Data:** Março 2026
**Escopo:** Todas as fontes de dados, pipelines de ingestão, qualidade do banco e propostas de evolução.

---

## 1. Inventário de Fontes de Dados

### 1.1 Fontes Externas Ativas

| Fonte | Script | Frequência Recomendada | Criticidade |
|-------|--------|------------------------|-------------|
| **CVM/DFP** — Demonstrações Anuais | `pickup/dados_cvm_dfp.py` | Anual (após fechamento ITR/DFP) | Alta |
| **CVM/ITR** — Demonstrações Trimestrais | `pickup/dados_cvm_itr.py` | Trimestral | Alta |
| **CVM/IPE** — Documentos Corporativos | `pickup/ingest_docs_cvm_ipe.py` | Semanal / on-demand | Alta |
| **CVM/ENET** — Documentos + Chunks RAG | `pickup/ingest_docs_cvm_enet.py` | Semanal / on-demand | Alta |
| **BCB/SGS API** — Indicadores Macro | `pickup/dados_macro_brasil.py` | Mensal | Média |
| **B3/CVM Cadastro** — Mapeamento de Tickers | `pickup/cvm_to_ticker_sync.py` | Mensal | Alta |
| **Yahoo Finance (yfinance)** — Preços/Múltiplos | `pickup/dados_multiplos_dfp.py`, `dados_multiplos_itr.py` | Após cada DFP/ITR | Alta |
| **RI Crawler** (Layer B) | `pickup/ingest_docs_fallback.py` | On-demand via fallback | Baixa |
| **SQLite local** (`data/metadados.db`) | `pickup/dados_setores_b3.py` | Manual / setup inicial | Média |

### 1.2 Tabelas no Supabase (PostgreSQL)

| Tabela | Escrita por | Lida por | Tipo de Dado |
|--------|-------------|----------|--------------|
| `Demonstracoes_Financeiras` | `dados_cvm_dfp` | `dados_multiplos_dfp`, `db.py` | Financeiro anual (DRE/BPA/BPP) |
| `Demonstracoes_Financeiras_TRI` | `dados_cvm_itr` | `dados_multiplos_itr`, `db.py` | Financeiro trimestral |
| `multiplos` | `dados_multiplos_dfp` | `db.py` | Múltiplos calculados (anual) |
| `multiplos_TRI` | `dados_multiplos_itr` | `db.py` | Múltiplos calculados (trimestral) |
| `setores` | `dados_setores_b3` | `db.py` | Classificação setorial B3 |
| `info_economica` | `dados_macro_brasil` | `db.py` | Macro anual agregado |
| `info_economica_mensal` | `dados_macro_brasil` | `db.py` | Macro mensal bruto |
| `docs_corporativos` | `ingest_docs_cvm_ipe/enet/fallback` | `db.py`, `docs_rag.py` | Documentos corporativos com texto |
| `docs_corporativos_chunks` | `ingest_docs_cvm_enet` | `db.py` | Chunks de texto para RAG |
| `patch6_runs` | `patch6_runs_store` | `patch6_runs_store` | Resultados LLM com JSON completo |
| `cvm_to_ticker` | `cvm_to_ticker_sync` | `dados_cvm_dfp`, `dados_cvm_itr` | Mapeamento CVM code → ticker |
| `ri_map` | Manual/externo | `ingest_docs_fallback` | URLs de RI por ticker |

---

## 2. Análise de Pipelines — Ponta a Ponta

### Pipeline 1: Dados Financeiros Estruturados (DFP/ITR)
```
CVM Open Data (ZIP/CSV)
  → coleta HTTP com retry (5x, backoff 1.2)
  → descompressão + leitura por CD_CONTA (DRE/BPA/BPP/DFC)
  → normalização escala_moeda (via cvm_quality.py)
  → mapeamento CVM → ticker (cvm_to_ticker.csv ou tabela)
  → capping LPA (evitar overflow)
  → drop_duplicates(Ticker, Data, keep="last")
  → UPSERT Supabase (ON CONFLICT DO UPDATE)
  → dados_multiplos_*.py: lê DF + busca preços yfinance
  → cálculo múltiplos (P/L, P/VP, DY, EV/EBITDA etc.)
  → UPSERT multiplos / multiplos_TRI
```

**Gaps identificados:**
- `cvm_to_ticker.csv` é arquivo local — não versionado, pode desincronizar com `cvm_to_ticker` (tabela)
- Não há log de execução persistido — sem auditoria de "quando rodou, quantos registros, erros"
- yfinance não tem retry robusto; batch falha silenciosamente
- Sem verificação de cobertura pós-ingestão (ex.: % tickers esperados com dados)

### Pipeline 2: Documentos Corporativos (RAG)
```
CVM/IPE ou CVM/ENET
  → listagem de documentos por ticker + janela temporal
  → heurística de priorização estratégica (A/B/C/D scoring)
  → download PDF + extração texto (PyPDF2 → pdfminer.six)
  → SHA256 doc_hash (idempotência)
  → UPSERT docs_corporativos
  → chunking (1500 chars / 200 overlap) → chunk_hash
  → UPSERT docs_corporativos_chunks
```

**Gaps identificados:**
- Dois pipelines independentes (IPE + ENET) sem reconciliação — possível duplicidade de conteúdo com doc_hash diferente
- Extração de PDF pode falhar silenciosamente; raw_text fica NULL sem alerta
- Sem indexação semântica (embeddings) — RAG é puramente textual (lexical)
- Sem campo `qualidade_texto` (comprimento, legibilidade, % NUL chars)

### Pipeline 3: Macro Econômico (BCB/SGS)
```
BCB/SGS API (JSON por série)
  → busca chunked por window temporal (evitar timeout)
  → agregação anual (YE-DEC para stocks, soma para flows)
  → cálculo TTM para PIB
  → UPSERT info_economica + info_economica_mensal
```

**Gaps identificados:**
- Série ICC (4393) pode ter descontinuidade histórica (BCB renomeia séries)
- Sem validação de completude (quantos meses/anos esperados vs. encontrados)
- Sem alertas se série retornar vazia (HTTP 404 com texto é tratado como "sem dados" — pode mascarar mudança na API)

### Pipeline 4: Análise LLM (Patch6)
```
patch6_runs (DB) + docs_corporativos (RAG)
  → build_rag_context (topic-budget)
  → LLM call (OpenAI / Ollama)
  → validate_result (schema_score)
  → compute_hybrid_score
  → análise temporal + memória + regime + forward + priority
  → UPSERT patch6_runs
```

**Gaps identificados:**
- `result_json` pode ter schemas diferentes entre versões (v1 ≠ v2 ≠ v3) — sem migração
- Sem limite de tamanho do `result_json`; JSONs corrompidos não são detectados no armazenamento
- `period_ref` é string livre (ex.: "2024-Q3") — sem validação de formato

---

## 3. Mapeamento de Fragilidades

### 3.1 Fragilidade Alta

| # | Fragilidade | Impacto | Script Afetado |
|---|-------------|---------|----------------|
| F1 | **Dependência única no Yahoo Finance** para preços históricos | Sem preço → múltiplos calculados com NaN | `dados_multiplos_dfp/itr` |
| F2 | **Arquivo `cvm_to_ticker.csv` local** não sincronizado com tabela `cvm_to_ticker` | Tickers mapeados incorretamente | `dados_cvm_dfp/itr` |
| F3 | **Sem log de execução persistido** para nenhum pipeline | Sem auditoria, sem alerta de falha silenciosa | Todos |
| F4 | **PDF extraction falha silenciosa** — raw_text fica NULL | RAG sem contexto → LLM analisa sem evidências | `ingest_docs_cvm_ipe/enet` |
| F5 | **ENET usa POST scraping** em endpoint não-documentado | Quebra sem aviso se API ENET mudar | `ingest_docs_cvm_enet` |

### 3.2 Fragilidade Média

| # | Fragilidade | Impacto |
|---|-------------|---------|
| F6 | **yfinance sem retry** robusto — timeout silencioso | Múltiplos ausentes para tickers falhados |
| F7 | **`ri_map` preenchida manualmente** sem pipeline de atualização | URLs de RI obsoletas → fallback ineficaz |
| F8 | **Sem versionamento de `result_json`** no patch6_runs | Análises antigas incompatíveis com schema atual |
| F9 | **Escala_moeda não validada** antes de normalização | Valor errado se CVM mudar código de escala |
| F10 | **RI Crawler sem rate limiting** configurável por domínio | Risco de bloqueio de IP |

### 3.3 Fragilidade Baixa

| # | Fragilidade | Impacto |
|---|-------------|---------|
| F11 | `period_ref` em patch6_runs sem validação de formato | Comparações temporais falham |
| F12 | `info_economica_mensal` sem TTL de cache definido | Dados macro mensais podem ficar obsoletos no app |
| F13 | Chunks sem embedding vetorial | RAG de baixa qualidade (lexical only) |

---

## 4. Qualidade do Banco — Análise por Tabela

| Tabela | Chave Natural | Constraint? | Temporal? | Problema Principal |
|--------|--------------|-------------|-----------|-------------------|
| `Demonstracoes_Financeiras` | (Ticker, Data) | Não explícito | Sim (anual) | Colunas em PascalCase misturadas com snake_case |
| `Demonstracoes_Financeiras_TRI` | (Ticker, Data) | Não explícito | Sim (trimestral) | Mesmo problema de nomenclatura |
| `multiplos` | (Ticker, Data) | Índice único criado programaticamente | Sim | Dependência total de yfinance para preços |
| `multiplos_TRI` | (Ticker, Data) | ON CONFLICT via INSERT | Sim | TTM calculado no ETL, não derivável no banco |
| `setores` | ticker | PK implícito | Não | Dados de classificação B3 podem desatualizar |
| `info_economica` | data | UPSERT | Sim | Séries BCB podem ser descontinuadas sem aviso |
| `docs_corporativos` | doc_hash | Sem FK | Sim (parcial) | raw_text NULL não detectado; sem score de qualidade |
| `docs_corporativos_chunks` | chunk_hash | FK para docs (implícita) | Via doc | Sem validação de completude do chunking |
| `patch6_runs` | (snapshot_id, ticker, period_ref) | Constraint explícito | Sim | result_json sem schema versioning |
| `cvm_to_ticker` | cvm | PK implícito | Não | Desync com arquivo CSV local |

---

## 5. Arquitetura-Alvo Proposta

### 5.1 Modelo de Camadas (Bronze/Silver/Gold)

```
BRONZE (raw — imutável)
  cvm_dfp_raw          ← CSVs brutos do CVM antes de normalização
  cvm_itr_raw          ← CSVs trimestrais brutos
  docs_raw             ← texto extraído antes de processamento
  bcb_series_raw       ← JSON bruto das séries BCB/SGS

SILVER (normalizado — validado)
  Demonstracoes_Financeiras         ← atual (manter)
  Demonstracoes_Financeiras_TRI     ← atual (manter)
  info_economica / info_economica_mensal
  docs_corporativos                 ← com campo qualidade_texto
  cvm_to_ticker                     ← tabela canônica (remover CSV)

GOLD (derivado — app-ready)
  multiplos              ← atual (manter)
  multiplos_TRI          ← atual (manter)
  patch6_runs            ← com schema_version
  docs_corporativos_chunks ← com embedding vetorial (futuro)
  ingestion_log          ← NOVO: audit trail de execuções
```

### 5.2 Tabela de Log de Ingestão (proposta)

```sql
CREATE TABLE public.ingestion_log (
    id            BIGSERIAL PRIMARY KEY,
    pipeline      TEXT NOT NULL,          -- 'dfp', 'itr', 'multiplos', 'macro', 'docs_ipe', etc.
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    status        TEXT NOT NULL,          -- 'running' | 'success' | 'partial' | 'failed'
    rows_inserted INT DEFAULT 0,
    rows_updated  INT DEFAULT 0,
    rows_skipped  INT DEFAULT 0,
    errors_count  INT DEFAULT 0,
    params        JSONB,                  -- tickers, period, options used
    error_detail  TEXT                    -- last error message if failed
);
```

### 5.3 Campo `schema_version` no patch6_runs (proposta)

```sql
ALTER TABLE public.patch6_runs
    ADD COLUMN IF NOT EXISTS schema_version TEXT DEFAULT 'v1';
```
Preenchido automaticamente pelo `patch6_runs_store.py` com a versão atual (`v3`).

---

## 6. Novas Fontes de Dados Propostas

| Fonte | Dado | Valor para o App | Complexidade |
|-------|------|-----------------|--------------|
| **CVM FRE** (Formulário de Referência) | Governança, remuneração, riscos estruturados | Enriquece análise qualitativa Patch6 | Média |
| **CVM DFP/ITR estruturado** (XBRL futuro) | Dados financeiros com tags XBRL | Elimina dependência do parsing ad-hoc de CSV | Alta |
| **Transcrições de earnings calls** | Falas da administração | Sinal de qualidade narrativa direto | Alta |
| **Dados de consenso de analistas** (Bloomberg/Refinitiv) | Target price, recomendações | Benchmark para score LLM | Alta (pago) |
| **Google Trends** (BR) | Interesse no ticker/empresa | Sinal de atenção pública | Baixa |
| **IBGE Open Data** | PIB regional, emprego setorial | Contexto macro setorial | Média |
| **Embeddings vetoriais** (pgvector) | RAG semântico para docs_corporativos | Melhora drasticamente qualidade RAG | Média |

---

## 7. Resumo de Prioridades

### Crítico (resolver imediatamente)
1. **Migrar `cvm_to_ticker.csv` para exclusividade da tabela `cvm_to_ticker`** — eliminar arquivo local
2. **Implementar `ingestion_log`** — auditabilidade mínima
3. **Detectar e alertar `raw_text IS NULL`** em docs_corporativos após ingestão

### Alto (próximo ciclo)
4. **Adicionar `schema_version` ao patch6_runs** — compatibilidade entre v1/v2/v3
5. **Criar scripts de auditoria automatizada** (ver `auditoria_dados/audit_tools.py`)
6. **Retry robusto para yfinance** com fallback para Alpha Vantage ou Brapi

### Médio (backlog)
7. Padronizar nomenclatura de colunas (snake_case consistente)
8. Adicionar campo `qualidade_texto` a docs_corporativos
9. Adicionar `schema_version` e validação de `period_ref` ao patch6_runs
10. Explorar integração pgvector para RAG semântico
