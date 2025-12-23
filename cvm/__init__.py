"""
Pacote CVM
==========

Este pacote concentra todos os módulos responsáveis por:

- Aquisição de dados públicos da CVM
- Ingestão e persistência em banco de dados (Supabase/Postgres)
- Padronização e preparação de dados financeiros para análises posteriores

Organização esperada:

cvm/
 ├── __init__.py
 ├── cvm_dfp_ingest.py        # Ingestão DFP (Demonstrações Financeiras Padronizadas) - Antigo algoritmo_1
 ├── cvm_itr_ingest.py        # (futuro) Ingestão ITR 
 ├── cvm_fre_ingest.py        # (futuro) Formulário de Referência
 ├── cvm_utils.py             # Funções auxiliares (download, parsing, validações)

Convenções:
- Cada módulo deve expor uma função `run(engine, **kwargs)`
- Nenhum módulo deve depender de UI (Streamlit)
- Persistência sempre via SQLAlchemy (engine)
"""

from importlib import metadata

# Versão do pacote (opcional, mas recomendado)
try:
    __version__ = metadata.version(__name__)
except metadata.PackageNotFoundError:
    __version__ = "0.1.0"

# Exposição explícita dos módulos principais
__all__ = [
    "cvm_dfp_ingest",
]
