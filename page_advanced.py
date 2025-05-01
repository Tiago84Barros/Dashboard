"""page_advanced.py
~~~~~~~~~~~~~~~~~~~
Aba **Avançada** completa: filtros (Setor → Subsetor → Segmento → Idade), cálculo
integrado de *score*, carteiras, benchmark, gráficos comparativos e blocos de
resultado.

Copie‑e‑cole tal qual no seu projeto e chame `render()` dentro do seu app
Streamlit.

Dependências já modularizadas
-----------------------------
- **db_loader**  → `load_data_from_db`, `load_multiplos_from_db`, `load_multiplos_limitado_from_db`, `load_macro_summary`
- **helpers**    → `obter_setor_da_empresa`, `determinar_lideres`, `formatar_real`
- **scoring**    → `calcular_score_acumulado`, `_penalizar_plato`
- **portfolio**  → `gerir_carteira`, `gerir_carteira_todas_empresas`, `calcular_patrimonio_selic_macro`
- **weights**    → `PESOS_POR_SETOR`, `INDICADORES_SCORE`
- **yf_data**    → `baixar_precos`, `coletar_dividendos`
- **utils**      → `get_logo_url`

Outros: `pandas`, `numpy`, `matplotlib`, `plotly.express`, `streamlit`.
"""

from __future__ import annotations

import math
from typing import List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

from db_loader import (
    load_data_from_db,
    load_multiplos_from_db,
    load_macro_summary,
)
from helpers import (
    obter_setor_da_empresa,
    determinar_lideres,
    formatar_real,
)
from portfolio import (
    gerir_carteira,
    gerir_carteira_todas_empresas,
    calcular_patrimonio_selic_macro,
)
from scoring import calcular_score_acumulado, _penalizar_plato  # noqa: WPS450
from weights import PESOS_POR_SETOR as pesos_por_setor, INDICADORES_SCORE as indicadores_score
from yf_data import baixar_precos, coletar_dividendos
from utils import get_logo_url  # ajuste se estiver noutro módulo

# ---------------------------------------------------------------------------
# Helpers internos -----------------------------------------------------------
# ---------------------------------------------------------------------------

def _filtrar_por_idade(empresas_df: pd.DataFrame, opcao: str) -> pd.DataFrame:
    """Filtra empresas conforme <10 ou ≥10 anos de histórico."""
    selecionadas: List[pd.Series] = []
    for row in empresas_df.itertuples():
        tk_sa = f"{row.ticker}.SA"
        dre = load_data_from_db(tk_sa)
        if dre is None or dre.empty:
            continue
        anos_hist = pd.to_datetime(dre['Data'], errors='coerce').dt.year.nunique()
        if (
            opcao == 'Todas' or
            (opcao.startswith('Crescimento') and anos_hist < 10) or
            (opcao.startswith('Estabelecida') and anos_hist >= 10)
        ):
            selecionadas.append(pd.Series(row._asdict()))
    return pd.DataFrame(selecionadas)


def _plot_patrimonio(df: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(12, 6))
    for col in df.columns:
        style = {'linewidth': 2}
        if col == 'Patrimônio':
            style['color'] = 'red'
        elif col == 'Tesouro Selic':
            style.update(color='blue', linestyle='-.')
        else:
            style.update(color='gray', linewidth=1, linestyle='--', alpha=0.6)
        df[col].plot(ax=ax, label=col, **style)
    ax.set_title('Evolução do Patrimônio Acumulado')
    ax.set_xlabel('Data')
    ax.set_ylabel('Patrimônio (R$)')
    ax.legend()
    st.pyplot(fig)


# ---------------------------------------------------------------------------
# Página principal -----------------------------------------------------------
# ---------------------------------------------------------------------------

def render():
    if st.session_state.get('pagina') != 'Avançada':
        return

    st.markdown("""<h1 style='text-align:center;font-size:36px'>Análise Avançada de Ações</h1>""", unsafe_allow_html=True)

    dados_macro = load_macro_summary()
    setores_df: pd.DataFrame | None = st.session_state.get('setores_df')
    if setores_df is None or setores_df.empty:
        st.error('Tabela de setores não carregada.')
        return

    # ------------------------- Filtros hierárquicos -----------------------
    setor_sel = st.selectbox('Selecione o Setor:', sorted(setores_df['SETOR'].dropna().unique()))
    if not setor_sel:
        return
    subset_opts = setores_df.query('SETOR == @setor_sel')['SUBSETOR'].dropna().unique()
    subset_sel = st.selectbox('Selecione o Subsetor:', sorted(subset_opts))
    if not subset_sel:
        return
    seg_opts = setores_df.query('SETOR == @setor_sel and SUBSETOR == @subset_sel')['SEGMENTO'].dropna().unique()
    seg_sel = st.selectbox('Selecione o Segmento:', sorted(seg_opts))
    if not seg_sel:
        return

    emp_segmento = setores_df.query('SETOR == @setor_sel and SUBSETOR == @subset_sel and SEGMENTO == @seg_sel')
    if emp_segmento.empty:
        st.warning('Não há empresas nesse segmento.')
        return

    opcao_idade = st.selectbox('Tipo de Empresa:', ['Todas', 'Crescimento (< 10 anos)', 'Estabelecida (>= 10 anos)'])
    emp_filtradas = _filtrar_por_idade(emp_segmento, opcao_idade)
    if emp_filtradas.empty:
        st.warning('Nenhuma empresa atende ao filtro de idade.')
        return

    st.success(f'Total de empresas filtradas: {len(emp_filtradas)}')

    # ------------------------- Cards de empresas -------------------------
    cols_cards = st.columns(3)
    for idx, row in emp_filtradas.iterrows():
        with cols_cards[idx % 3]:
            st.markdown(
                f"""
                <div style='border:2px solid #ddd;border-radius:10px;padding:15px;margin:10px;background:#f9f9f9;text-align:center;'>
                    <img src='{get_logo_url(row.ticker)}' style='width:50px;height:50px;margin-bottom:10px;'>
                    <h4 style='color:#333'>{row.nome_empresa} ({row.ticker})</h4>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # ------------------------- Montagem lista_emp ------------------------
    lista_emp: list[dict] = []
    for row in emp_filtradas.itertuples():
        tk = row.ticker
        mult = load_multiplos_from_db(tk + '.SA')
        dre = load_data_from_db(tk + '.SA')
        if mult is None or mult.empty or dre is None or dre.empty:
            continue
        mult['Ano'] = pd.to_datetime(mult['Data'], errors='coerce').dt.year
        dre['Ano'] = pd.to_datetime(dre['Data'], errors='coerce').dt.year
        lista_emp.append({'ticker': tk, 'multiplos': mult, 'df_dre': dre})

    if not lista_emp:
        st.error('Dados financeiros ausentes para as empresas filtradas.')
        return

    setor_base = obter_setor_da_empresa(lista_emp[0]['ticker'], setores_df)
    pesos = pesos_por_setor.get(setor_base, indicadores_score)
    setores_emp = dict(zip(emp_filtradas['ticker'], emp_filtradas['SETOR']))

    precos = baixar_precos([tk + '.SA' for tk in emp_filtradas['ticker']])
    if precos is None or precos.empty:
        st.error('Falha ao baixar preços.')
        return

    precos_m = precos.resample('M').last()
    df_scores = calcular_score_acumulado(lista_emp, setores_emp, pesos, dados_macro, None, anos_minimos=4)
    df_scores = _penalizar_plato(df_scores, precos_m)
    lideres_ano = determinar_lideres(df_scores)

    dividendos = coletar_dividendos(df_scores['ticker'].unique())
    patrimonio_est, aportes = gerir_carteira(precos, df_scores, lideres_ano, dividendos)
    patrimonio_selic = calcular_patrimonio_selic_macro(dados_macro, aportes)
    patrimonio_emp = gerir_carteira_todas_empresas(precos, emp_filtradas['ticker'], aportes, dividendos)
    patrimonio_all = pd.concat([patrimonio_est, patrimonio_emp, patrimonio_selic], axis=1)

    st.markdown('---')
    if patrimonio_all.empty:
        st.warning('Dados insuficientes para o patrimônio.')
    else:
        _plot_patrimonio(patrimonio_all)

    # ----------------------- Blocos Patrimônio final ----------------------
    st.markdown('---')
    st.subheader('📊 Patrimônio Final para R$1.000/Mês')
    last_row = patrimonio_all.tail(1).rename_axis('Data').reset_index()
    df_pf = last_row.melt(id_vars='Data', var_name='Ticker', value_name='Valor').sort_values('Valor', ascending=False)

    cols_blk = st.columns(3)
    cont_lider = lideres_ano['ticker'].value_counts()
    for idx, row in df_pf.iterrows():
        tk = row['Ticker']
        val = row['Valor']
        if tk == 'Patrimônio':
            name, border, icon = 'Estratégia de Aporte', '#DAA520', 'https://cdn-icons-png.flaticon.com/512/1019/1019709.png'
        elif tk == 'Tesouro Selic':
            name, border, icon = 'Tesouro Selic', '#007bff', 'https://cdn-icons-png.flaticon.com/512/2331/2331949.png'
        else:
            name, border, icon = tk, '#d3d3d3', get_logo_url(tk)
        lider_txt = f"🏆 {cont_lider.get(tk, 0)}x Líder" if tk in cont_lider else ''
        with cols_blk[idx % 3]:
            st.markdown(
                f"""
                <div style='background:#fff;border:3px solid {border};border-radius:10px;padding:15px;margin:10px;text-align:center;box-shadow:2px 2px 5px rgba(0,0,0,.1);'>
                    <img src='{icon}' style='width:50px;height:auto;margin-bottom:5px;'>
                    <h3 style='margin:0;color:#4a4a4a'>{name}</h3>
                    <p style='font-size:18px;margin:5px 0;font-weight:bold;color:#2ecc71'>{formatar_real(val)}</p>
                    <p style='font-size:14px;color:#FFA500'>{lider_txt}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.markdown('---')
    st.markdown('<div style="margin:30px"></div>', unsafe_allow_html=True)

    # ------------------- Gráfico comparativo Múltiplos --------------------
    st.markdown('### Comparação de Indicadores (Múltiplos)')
    mult_map = {
        'Margem Líquida': 'Margem_Liquida',
        'Margem Operacional': 'Margem_Operacional',
        'ROE': 'ROE',
        'ROIC': 'ROIC',
        'P/L': 'P/L',
        'Liquidez Corrente': 'Liquidez_Corrente',
        'Alavancagem Financeira': 'Alavancagem_Financeira',
        'Endividamento Total': 'Endividamento_Total',
    }
    emp_nomes = emp_filtradas['nome_empresa'].tolist()
    emp_sel = st.multiselect('Empresas:', emp_nomes, default=emp_nomes)
    ind_display = st.selectbox('Indicador:', list(mult_map.keys()))
    ind_col = mult_map[ind_display]
    normalizar = st.checkbox('Normalizar (0‑1)', value=False)

    df_hist = []
    for row in emp_filtradas.itertuples():
        if row.nome_empresa not in emp_sel:
            continue
        mult = load_multiplos_from_db(row.ticker + '.SA')
        if mult is None or mult.empty or ind_col not in mult.columns:
            continue
        df_tmp = mult[['Data', ind_col]].copy()
        df_tmp['Ano'] = pd.to_datetime(df_tmp['Data']).dt.year
        df_tmp['Empresa'] = row.nome_empresa
        df_hist.append(df_tmp)
    if df_hist:
        df_hist = pd.concat(df_hist)
        df_hist = df_hist.dropna(subset=['Ano'])
        if normalizar:
            rng = df_hist[ind_col].agg(['min', 'max'])
            df_hist[ind_col] = (df_hist[ind_col] - rng['min']) / (rng['max'] - rng['min'])
        df_hist['Ano'] = df_hist['Ano'].astype(str)
        fig = px.bar(df
