"""
pipeline_local — pipeline de processamento local DFP/ITR → Supabase.

Estágios:
  extract   → pipeline_local.extract.*
  transform → pipeline_local.transform.*
  publish   → pipeline_local.publish.*
  audit     → pipeline_local.audit.*

Orquestrador:
  python -m pipeline_local.run_pipeline --help
"""
