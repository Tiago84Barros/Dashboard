
# Atualizado: Selic via macro (igual criacao_portfolio (1).py)
# Atualizado: Rotulagem correta de Líder vs Maior Participação

def build_label(emp):
    motivos = emp.get("motivos", [])
    if not motivos:
        return "Selecionada"
    return " | ".join(motivos)

# Exemplo render ajustado
def render_card(emp):
    label = build_label(emp)
    return f"{emp['ticker']} -> {label}"
