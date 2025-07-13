"""weights.py
~~~~~~~~~~~~
Pesos dos indicadores fundamentalistas, agora cobrindo todos
os setores citados no script original.

Formato de cada entrada:
    "Indicador": {"peso": 0.12, "melhor_alto": True}
"""

from __future__ import annotations
from typing import Dict

# ================================================
# DEFINIÇÃO DE INDICADORES E PESOS PARA SCORE BASEADO NO SETOR DA EMPRESA
# ================================================
pesos_por_setor = {
    "Financeiro": {
        'ROE_mean': {'peso': 0.28, 'melhor_alto': True},  
        'P/VP_mean': {'peso': 0.15, 'melhor_alto': False},  
        'DY_mean': {'peso': 0.15, 'melhor_alto': True},  
        'Endividamento_Total_mean': {'peso': 0.05, 'melhor_alto': False},  
        'Liquidez_Corrente_mean': {'peso': 0.07, 'melhor_alto': True},  
        'Margem_Liquida_mean': {'peso': 0.10, 'melhor_alto': True},  
        'Lucro_Liquido_slope_log': {'peso': 0.10, 'melhor_alto': True}, 
        'Momentum_12m': {'peso': 0.10, 'melhor_alto': True},
    },
    "Tecnologia da Informação": {
        'Margem_Liquida_mean'          : {'peso': 0.07, 'melhor_alto': True},
        'Margem_Operacional_mean'      : {'peso': 0.09, 'melhor_alto': True},
        'ROE_mean'                     : {'peso': 0.06, 'melhor_alto': True},
        'ROA_mean'                     : {'peso': 0.04, 'melhor_alto': True},
        'ROIC_mean'                    : {'peso': 0.07, 'melhor_alto': True},
    
        'P/VP_mean'                    : {'peso': 0.03, 'melhor_alto': False},
        'DY_mean'                      : {'peso': 0.02, 'melhor_alto': True},
    
        'Endividamento_Total_mean'     : {'peso': 0.03, 'melhor_alto': False},
        'Alavancagem_Financeira_mean'  : {'peso': 0.03, 'melhor_alto': False},
        'Liquidez_Corrente_mean'       : {'peso': 0.04, 'melhor_alto': True},
    
        # crescimento / qualidade
        'Receita_Liquida_slope_log'    : {'peso': 0.15, 'melhor_alto': True},
        'Lucro_Liquido_slope_log'      : {'peso': 0.14, 'melhor_alto': True},
        'Patrimonio_Liquido_slope_log' : {'peso': 0.05, 'melhor_alto': True},
        'Divida_Liquida_slope_log'     : {'peso': 0.02, 'melhor_alto': False},
        'Caixa_Liquido_slope_log'      : {'peso': 0.05, 'melhor_alto': True},
    
        # NOVO — fator de preço
        'Momentum_12m'                 : {'peso': 0.11, 'melhor_alto': True},
    },
    "Energia": {
        'Margem_Liquida_mean'          : {'peso': 0.07, 'melhor_alto': True},
        'Margem_Operacional_mean'      : {'peso': 0.09, 'melhor_alto': True},
        'ROE_mean'                     : {'peso': 0.06, 'melhor_alto': True},
        'ROA_mean'                     : {'peso': 0.05, 'melhor_alto': True},
        'ROIC_mean'                    : {'peso': 0.06, 'melhor_alto': True},
    
        'P/VP_mean'                    : {'peso': 0.03, 'melhor_alto': False},
        'DY_mean'                      : {'peso': 0.16, 'melhor_alto': True},
    
        'Endividamento_Total_mean'     : {'peso': 0.08, 'melhor_alto': False},
        'Alavancagem_Financeira_mean'  : {'peso': 0.05, 'melhor_alto': False},
        'Liquidez_Corrente_mean'       : {'peso': 0.08, 'melhor_alto': True},
    
        'Receita_Liquida_slope_log'    : {'peso': 0.05, 'melhor_alto': True},
        'Lucro_Liquido_slope_log'      : {'peso': 0.05, 'melhor_alto': True},
        'Patrimonio_Liquido_slope_log' : {'peso': 0.02, 'melhor_alto': True},
        'Divida_Liquida_slope_log'     : {'peso': 0.02, 'melhor_alto': False},
        'Caixa_Liquido_slope_log'      : {'peso': 0.05, 'melhor_alto': True},
    
        'Momentum_12m'                 : {'peso': 0.13, 'melhor_alto': True},
    },
    "Industrial": {
        'Margem_Liquida_mean'          : {'peso': 0.07, 'melhor_alto': True},
        'Margem_Operacional_mean'      : {'peso': 0.10, 'melhor_alto': True},
        'ROE_mean'                     : {'peso': 0.07, 'melhor_alto': True},
        'ROA_mean'                     : {'peso': 0.05, 'melhor_alto': True},
        'ROIC_mean'                    : {'peso': 0.10, 'melhor_alto': True},
    
        'P/VP_mean'                    : {'peso': 0.07, 'melhor_alto': False},
        'DY_mean'                      : {'peso': 0.04, 'melhor_alto': True},
    
        'Endividamento_Total_mean'     : {'peso': 0.07, 'melhor_alto': False},
        'Alavancagem_Financeira_mean'  : {'peso': 0.04, 'melhor_alto': False},
        'Liquidez_Corrente_mean'       : {'peso': 0.04, 'melhor_alto': True},
    
        'Receita_Liquida_slope_log'    : {'peso': 0.08, 'melhor_alto': True},
        'Lucro_Liquido_slope_log'      : {'peso': 0.08, 'melhor_alto': True},
        'Patrimonio_Liquido_slope_log' : {'peso': 0.04, 'melhor_alto': True},
        'Divida_Liquida_slope_log'     : {'peso': 0.04, 'melhor_alto': False},
        'Caixa_Liquido_slope_log'      : {'peso': 0.04, 'melhor_alto': True},
    
        'Momentum_12m'                 : {'peso': 0.07, 'melhor_alto': True},
    },
    "Consumo Cíclico": {
        # --- Rentabilidade -------------------------------------------------
        'Margem_Liquida_mean'         : {'peso': 0.10, 'melhor_alto': True},
        'Margem_Operacional_mean'     : {'peso': 0.08, 'melhor_alto': True},
        'ROE_mean'                    : {'peso': 0.08, 'melhor_alto': True},
        'ROA_mean'                    : {'peso': 0.05, 'melhor_alto': True},
        'ROIC_mean'                   : {'peso': 0.09, 'melhor_alto': True},
    
        # --- Valuation & Dividendos ----------------------------------------
        'P/VP_mean'                   : {'peso': 0.06, 'melhor_alto': False},
        'DY_mean'                     : {'peso': 0.05, 'melhor_alto': True},
    
        # --- Balanço --------------------------------------------------------
        'Endividamento_Total_mean'    : {'peso': 0.07, 'melhor_alto': False},
        'Alavancagem_Financeira_mean' : {'peso': 0.04, 'melhor_alto': False},
        'Liquidez_Corrente_mean'      : {'peso': 0.04, 'melhor_alto': True},
    
        # --- Crescimento estrutural ----------------------------------------
        'Receita_Liquida_slope_log'   : {'peso': 0.15, 'melhor_alto': True},
        'Lucro_Liquido_slope_log'     : {'peso': 0.09, 'melhor_alto': True},
        'Patrimonio_Liquido_slope_log': {'peso': 0.04, 'melhor_alto': True},
        'Divida_Liquida_slope_log'    : {'peso': 0.04, 'melhor_alto': False},
        'Caixa_Liquido_slope_log'     : {'peso': 0.04, 'melhor_alto': True},
    
        # --- Sentimento de mercado -----------------------------------------
        'Momentum_12m'                : {'peso': 0.07, 'melhor_alto': True},
    },
    "Consumo não Cíclico": {
        # — Rentabilidade --------------------------------------------------------------------
        'Margem_Liquida_mean'         : {'peso': 0.09, 'melhor_alto': True},
        'Margem_Operacional_mean'     : {'peso': 0.09, 'melhor_alto': True},
        'ROE_mean'                    : {'peso': 0.08, 'melhor_alto': True},
        'ROA_mean'                    : {'peso': 0.05, 'melhor_alto': True},
        'ROIC_mean'                   : {'peso': 0.07, 'melhor_alto': True},
    
        # — Dividendos & Valuation ------------------------------------------------------------
        'DY_mean'                     : {'peso': 0.14, 'melhor_alto': True},
        'P/VP_mean'                   : {'peso': 0.06, 'melhor_alto': False},
    
        # — Balanço --------------------------------------------------------------------------
        'Endividamento_Total_mean'    : {'peso': 0.06, 'melhor_alto': False},
        'Alavancagem_Financeira_mean' : {'peso': 0.04, 'melhor_alto': False},
        'Liquidez_Corrente_mean'      : {'peso': 0.04, 'melhor_alto': True},
    
        # — Crescimento (mesmo que moderado) --------------------------------------------------
        'Receita_Liquida_slope_log'   : {'peso': 0.07, 'melhor_alto': True},
        'Lucro_Liquido_slope_log'     : {'peso': 0.07, 'melhor_alto': True},
        'Patrimonio_Liquido_slope_log': {'peso': 0.03, 'melhor_alto': True},
        'Divida_Liquida_slope_log'    : {'peso': 0.03, 'melhor_alto': False},
        'Caixa_Liquido_slope_log'     : {'peso': 0.04, 'melhor_alto': True},
    
        # — Sentimento de mercado -------------------------------------------------------------
        'Momentum_12m'                : {'peso': 0.10, 'melhor_alto': True},
    },
    "Materiais Básicos": {
        # — Rentabilidade de ciclo ---------------------------------------------
        'Margem_Operacional_mean'     : {'peso': 0.12, 'melhor_alto': True},
        'Margem_Liquida_mean'         : {'peso': 0.07, 'melhor_alto': True},
        'ROE_mean'                    : {'peso': 0.08, 'melhor_alto': True},
        'ROIC_mean'                   : {'peso': 0.08, 'melhor_alto': True},
    
        # — Fluxo ao acionista ---------------------------------------------------
        'DY_mean'                     : {'peso': 0.12, 'melhor_alto': True},
        'P/VP_mean'                   : {'peso': 0.06, 'melhor_alto': False},
    
        # — Balanço --------------------------------------------------------------
        'Endividamento_Total_mean'    : {'peso': 0.07, 'melhor_alto': False},
        'Alavancagem_Financeira_mean' : {'peso': 0.05, 'melhor_alto': False},
        'Liquidez_Corrente_mean'      : {'peso': 0.03, 'melhor_alto': True},
    
        # — Crescimento (5 a 10 anos) -------------------------------------------
        'Receita_Liquida_slope_log'   : {'peso': 0.06, 'melhor_alto': True},
        'Lucro_Liquido_slope_log'     : {'peso': 0.06, 'melhor_alto': True},
        'Patrimonio_Liquido_slope_log': {'peso': 0.03, 'melhor_alto': True},
        'Divida_Liquida_slope_log'    : {'peso': 0.02, 'melhor_alto': False},
        'Caixa_Liquido_slope_log'     : {'peso': 0.02, 'melhor_alto': True},
    
        # — Sentimento / virada de ciclo ----------------------------------------
        'Momentum_12m'                : {'peso': 0.15, 'melhor_alto': True},
    },
    "Petróleo, Gás e Biocombustíveis": {
        'DY_mean'                    : {'peso': 0.30, 'melhor_alto': True},   # ainda o driver nº 1
        'Margem_Operacional_mean'    : {'peso': 0.25, 'melhor_alto': True},
        'ROIC_mean'                  : {'peso': 0.18, 'melhor_alto': True},
        'Liquidez_Corrente_mean'     : {'peso': 0.10, 'melhor_alto': True},
        'Endividamento_Total_mean'   : {'peso': 0.10, 'melhor_alto': False},
        'Momentum_12m'               : {'peso': 0.10, 'melhor_alto': True},   # levemente reduzido
    },
    "Saúde": {
        'Receita_Liquida_slope_log' : {'peso': 0.25, 'melhor_alto': True},
        'Lucro_Liquido_slope_log'   : {'peso': 0.25, 'melhor_alto': True},
        'Margem_Operacional_mean'   : {'peso': 0.20, 'melhor_alto': True},
        'ROE_mean'                  : {'peso': 0.15, 'melhor_alto': True},
        'Endividamento_Total_mean'  : {'peso': 0.07, 'melhor_alto': False},
        'Momentum_12m'              : {'peso': 0.08, 'melhor_alto': True},
    },
    "Comunicações": {
        'Margem_Liquida_mean'        : {'peso': 0.07, 'melhor_alto': True},
        'Margem_Operacional_mean'    : {'peso': 0.15, 'melhor_alto': True},   # -
        'ROE_mean'                   : {'peso': 0.08, 'melhor_alto': True},
        'ROA_mean'                   : {'peso': 0.05, 'melhor_alto': True},
        'ROIC_mean'                  : {'peso': 0.12, 'melhor_alto': True},   # -
        'P/VP_mean'                  : {'peso': 0.05, 'melhor_alto': False},
        'DY_mean'                    : {'peso': 0.12, 'melhor_alto': True},   # -
        'Endividamento_Total_mean'   : {'peso': 0.06, 'melhor_alto': False},
        'Alavancagem_Financeira_mean': {'peso': 0.04, 'melhor_alto': False},
        'Liquidez_Corrente_mean'     : {'peso': 0.07, 'melhor_alto': True},
        'Receita_Liquida_slope_log'  : {'peso': 0.07, 'melhor_alto': True},
        'Lucro_Liquido_slope_log'    : {'peso': 0.07, 'melhor_alto': True},
        'Momentum_12m'               : {'peso': 0.08, 'melhor_alto': True},
    },
    "Bens Industriais": {
        'Margem_Operacional_mean'   : {'peso': 0.22, 'melhor_alto': True},
        'ROIC_mean'                 : {'peso': 0.22, 'melhor_alto': True},
        'Receita_Liquida_slope_log' : {'peso': 0.12, 'melhor_alto': True},
        'Liquidez_Corrente_mean'    : {'peso': 0.11, 'melhor_alto': True},
        'P/VP_mean'                 : {'peso': 0.09, 'melhor_alto': False},
        'Endividamento_Total_mean'  : {'peso': 0.09, 'melhor_alto': False},
        'Momentum_12m'              : {'peso': 0.15, 'melhor_alto': True},
    },
    "Utilidade Pública": {
        'Margem_Liquida_mean':         {'peso': 0.07/1.15, 'melhor_alto': True},
        'Margem_Operacional_mean':     {'peso': 0.10/1.15, 'melhor_alto': True},
        'ROE_mean':                    {'peso': 0.05/1.15, 'melhor_alto': True},
        'ROA_mean':                    {'peso': 0.03/1.15, 'melhor_alto': True},
        'ROIC_mean':                   {'peso': 0.05/1.15, 'melhor_alto': True},
        'P/VP_mean':                   {'peso': 0.05/1.15, 'melhor_alto': False},
        'DY_mean':                     {'peso': 0.20/1.15, 'melhor_alto': True},
        'Endividamento_Total_mean':    {'peso': 0.10/1.15, 'melhor_alto': False},
        'Alavancagem_Financeira_mean': {'peso': 0.08/1.15, 'melhor_alto': False},
        'Liquidez_Corrente_mean':      {'peso': 0.10/1.15, 'melhor_alto': True},
        'Receita_Liquida_slope_log':   {'peso': 0.03/1.15, 'melhor_alto': True},
        'Lucro_Liquido_slope_log':     {'peso': 0.05/1.15, 'melhor_alto': True},
        'Patrimonio_Liquido_slope_log':{'peso': 0.02/1.15, 'melhor_alto': True},
        'Divida_Liquida_slope_log':    {'peso': 0.04/1.15, 'melhor_alto': False},
        'Caixa_Liquido_slope_log':     {'peso': 0.03/1.15, 'melhor_alto': True},
        'Momentum_12m':                {'peso': 0.15/1.15, 'melhor_alto': True},                                            
    },
}

# ================================================
# PESOS POR SEGMENTO (sobreposição)
# ================================================
pesos_por_segmento: Dict[str, Dict[str, Dict[str, float | bool]]] = {
    "Bancos": {
        'ROE_mean'                 : {'peso': 0.25, 'melhor_alto': True},   # Retorno sobre patrimônio é crítico
        'P/VP_mean'                : {'peso': 0.12, 'melhor_alto': False},  # Valuation ajustado ao valor patrimonial
        'DY_mean'                  : {'peso': 0.22, 'melhor_alto': True},   # Dividend Yield relevante para bancos
        'Endividamento_Total_mean' : {'peso': 0.08, 'melhor_alto': False},  # Dívida total deve ser controlada
        'Liquidez_Corrente_mean'   : {'peso': 0.08, 'melhor_alto': True},   # Liquidez curta é importante para risco
        'Margem_Liquida_mean'      : {'peso': 0.10, 'melhor_alto': True},   # Eficiência operacional
        'Lucro_Liquido_slope_log'  : {'peso': 0.10, 'melhor_alto': True},   # Crescimento de lucro líquido
    },
    # adicionar outros segmentos conforme necessário
}

# ================================================
# DEFINIÇÃO DE INDICADORES E PESOS PARA SCORE GENÉRICO
# ================================================

indicadores_score = {
    'Margem_Liquida_mean': 0.15, 'Margem_Operacional_mean': 0.20, 'ROE_mean': 0.20,
    'ROA_mean': 0.20, 'ROIC_mean': 0.20, 'P/VP_mean': 0.10, 'DY_mean': 0.30,
    'Endividamento_Total_mean': 0.15, 'Alavancagem_Financeira_mean': 0.15,
    'Liquidez_Corrente_mean': 0.15, 'Receita_Liquida_slope_log': 0.15,
    'Lucro_Liquido_slope_log': 0.20, 'Patrimonio_Liquido_slope_log': 0.15,
    'Divida_Liquida_slope_log': 0.15, 'Caixa_Liquido_slope_log': 0.15,
    'Momentum_12m': 0.15,
}

# ================================================
# FUNÇÃO PARA OBTER PESOS COM BASE NO SETOR
# ================================================
def get_pesos(setor: str, segmento: str = None) -> Dict[str, Dict[str, float | bool]]:
    """
    Recupera os pesos para o segmento especificado.
    1) Se houver definição em pesos_por_segmento, retorna esses pesos.
    2) Senão, tenta pelo setor em pesos_por_setor.
    3) Senão, retorna genérico.
    """
    # 1) Checa sobreposição por segmento
    if segmento and segmento in pesos_por_segmento:
        return pesos_por_segmento[segmento]

    # 2) Checa pelo setor
    if setor in pesos_por_setor:
        return pesos_por_setor[setor]

    # 3) Genérico
    return {
        indicador: {'peso': peso, 'melhor_alto': True}
        for indicador, peso in indicadores_score.items()
    }

__all__ = [
    "pesos_por_setor",
    "pesos_por_segmento",
    "indicadores_score",
    "get_pesos",
]
