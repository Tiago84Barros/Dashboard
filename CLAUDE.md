# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

A Streamlit-based web dashboard for fundamental analysis of Brazilian stocks (B3). It supports stock screening, portfolio construction with backtesting, and AI-powered analysis using OpenAI or local LLMs.

## Running the App

```bash
pip install -r requirements.txt
streamlit run dashboard.py
```

Opens at `http://localhost:8501`. Streamlit hot-reloads on file changes.

## Required Environment Variables

```bash
OPENAI_API_KEY="sk-..."
SUPABASE_DB_URL="postgresql://..."
AI_PROVIDER="openai"   # or "ollama" or "dummy"
AI_MODEL="gpt-4.1-mini"
AI_TIMEOUT_S="45"
AI_MAX_RETRIES="2"
```

On Streamlit Cloud, these are set as secrets. Locally, set via shell exports or a `.env` file.

## Architecture

```
dashboard.py          # Entry point — sidebar routing to pages
design/layout.py      # Global Streamlit page config + CSS
page/                 # UI layer — each file exports render()
core/                 # Business logic (analytics, portfolio, AI, DB)
  ai_models/          # LLM abstraction (OpenAI / Ollama / Dummy)
pickup/               # ETL scripts — fetch external data into Supabase
data/                 # Local SQLite cache (metadados.db)
```

### Page Routing

`dashboard.py` loads pages dynamically. Each `page/*.py` module must expose a `render()` function. To add a page:
1. Create `page/my_page.py` with `def render(): ...`
2. Add it to the page mapping dict in `dashboard.py`

### Data Flow

- **Supabase (PostgreSQL)** is the primary database — accessed via `core/db_loader.py`
- **Yahoo Finance** (`core/yf_data.py`) provides historical prices and dividends
- **CVM** (Brazilian Securities Commission) data is ingested via `pickup/dados_cvm_*.py`
- `core/db_loader.py` functions are cached with `@st.cache_data` / `@st.cache_resource`

### Caching Pattern

```python
@st.cache_data(ttl=60*60)   # data (serializable)
@st.cache_resource           # resources like DB engines
```

Use these decorators on any expensive DB or network calls.

### AI Layer (`core/ai_models/`)

- **Factory pattern**: `llm_client/factory.py` returns the right client based on `AI_PROVIDER`
- Clients: `OpenAIChatClient`, `OllamaClient`, `DummyLLMClient` (for testing)
- Prompts live in `core/ai_models/prompts/`
- Governance/audit logging in `core/ai_models/governance/`
- To use AI in new features, import via the factory rather than calling OpenAI directly

### Scoring System

Three scoring algorithm versions coexist — pages import v2/v3 with fallback to v1:
```python
try:
    from core.scoring_v2 import calcular_score_acumulado_v2
except Exception:
    calcular_score_acumulado_v2 = None
```

### Patch System

Experimental features are isolated in `core/patch6_*.py`, `core/patch7_*.py` and exposed in `page/portfolio_patches.py`. Always import patches with try/except and degrade gracefully.

### Data Ingestion (ETL)

Scripts in `pickup/` follow the pattern: fetch → validate → store to Supabase. They are triggered from the Settings UI (`page/configuracoes.py`). To add new ingestion, create a script in `pickup/` with a `main()` function, then wire it into `configuracoes.py`.

## No Test Suite

There is no automated test framework. Testing is done via manual exploration in the running Streamlit app. `page/patch6_teste.py` is an experimental sandbox page.
