NEWS_ANALYSIS_PROMPT = """
Tarefa:
Você receberá um conjunto de notícias (contexto) relacionadas a um ticker.
Extraia sinais para o próximo ano:

- sentiment: número entre -1 e +1 (negativo/positivo)
- confidence: 0 a 1 (quão sustentado pelas notícias)
- risks: lista curta de riscos
- catalysts: lista curta de catalisadores
- event_flags: flags objetivas quando houver evidência (ex.: guidance_revisado, investigação, M&A, reestruturação, aumento de alavancagem)
- justification: 3 a 5 bullets curtos apontando o porquê (sem inventar)

Se o contexto for insuficiente, retorne sentiment=0 e confidence baixa.
""".strip()
