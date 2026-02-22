
# ==============================
# BLOCO CORRIGIDO PARA SALVAR SELIC E MARGEM
# ==============================
# Substitua no final do seu criacao_portfolio.py
# apenas o trecho de save_snapshot por este bloco.

# ─────────────────────────────────────────
# Captura Selic atual (último valor macro)
# ─────────────────────────────────────────
selic_ref = None
try:
    if dados_macro is not None and not dados_macro.empty and "Selic" in dados_macro.columns:
        selic_ref = float(
            pd.to_numeric(dados_macro["Selic"], errors="coerce")
            .dropna()
            .iloc[-1]
        )
except Exception:
    selic_ref = None

snapshot_id = save_snapshot(
    items=items,
    selic_ref=selic_ref,  # Agora salva corretamente a Selic
    margem_superior=float(margem_superior),  # Salva como percentual (ex: 10 e não 0.10)
    tipo_empresa="ESTABELECIDA_10A",
    filters_json=filters_json,
    notes="criado via criacao_portfolio",
    status="active",
    plan_hash=plan_hash,
)
