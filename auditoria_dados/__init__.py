# auditoria_dados/__init__.py
#
# Módulos disponíveis:
#
#   ingestion_log   -- IngestionLog: audit trail de execuções de pipeline
#   audit_tools     -- checks de qualidade dentro do banco (7 checks)
#   reconciliation  -- reconciliação banco↔banco entre tabelas (6 checks)
#   ingest_quality  -- score dimensional por pipeline (completeness/fidelity/etc.)
#   source_recon    -- reconciliação fonte→banco: CVM e BCB vs Supabase (4 checks)
#
# Uso rápido via CLI:
#   python -m auditoria_dados.audit_tools    --all
#   python -m auditoria_dados.reconciliation --all
#   python -m auditoria_dados.ingest_quality --all
#   python -m auditoria_dados.source_recon   --all
