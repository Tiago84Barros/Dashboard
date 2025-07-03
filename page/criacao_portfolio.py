from __future__ import annotations

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from core.db_loader import (
    load_setores_from_db,
    load_data_from_db,
    load_multiplos_from_db,
    load_macro_summary,
)
from core.helpers import (
    obter_setor_da_empresa,
    determinar_lideres,
    get_logo_url,
    formatar_real,
)
from core.scoring import (
    calcular_score_acumulado,
    penalizar_plato,
)
from core.portfolio import calcular_patrimonio_selic_macro, gerir_carteira, encontrar_proxima_data_valida, gerir_carteira_simples
from core.yf_data import baixar_precos, coletar_dividendos, baixar_precos_ano_corrente
from core.weights import get_pesos

def render():
    st.markdown("<h1 style='text-align: center;'>Criação de Portfólio</h1>", unsafe_allow_html=True)

    with st.sidebar:
        margem_input = st.text_input("% acima do Tesouro Selic para destacar (obrigatório):", value="")
        gerar = st.button("Gerar Portfólio")

    if not margem_input.strip():
        st.warning("Digite uma porcentagem no campo lateral e clique em 'Gerar Portfólio'.")
        return

    try:
        margem_superior = float(margem_input.strip())
    except ValueError:
        st.error("Porcentagem inválida. Digite apenas números.")
        return

    if not gerar:
        st.stop()

    setores_df = st.session_state.get("setores_df")
    if setores_df is None or setores_df.empty:
        setores_df = load_setores_from_db()
        st.session_state["setores_df"] = setores_df

    dados_macro = load_macro_summary()
    dados_macro.columns = dados_macro.columns.str.strip().str.replace('\ufeff', '')
    if 'Data' not in dados_macro.columns:
        st.error("A coluna 'Data' não foi encontrada nos dados macroeconômicos.")
        st.stop()
    dados_macro["Data"] = pd.to_datetime(dados_macro["Data"], errors='coerce')
    dados_macro = dados_macro.set_index("Data").sort_index()

    setores_unicos = setores_df[["SETOR", "SUBSETOR", "SEGMENTO"]].drop_duplicates()

    empresas_lideres_finais = []

    for _, seg in setores_unicos.iterrows():
        setor, subsetor, segmento = seg["SETOR"], seg["SUBSETOR"], seg["SEGMENTO"]

        empresas_segmento = setores_df[
            (setores_df["SETOR"] == setor) &
            (setores_df["SUBSETOR"] == subsetor) &
            (setores_df["SEGMENTO"] == segmento)
        ]

        empresas_validas = []
        for _, row in empresas_segmento.iterrows():
            dre = load_data_from_db(f"{row['ticker']}.SA")
            if dre is None or dre.empty:
                continue
            anos = pd.to_datetime(dre["Data"], errors="coerce").dt.year.nunique()
            if anos >= 10:
                empresas_validas.append(row)

        if not empresas_validas:
            continue

        empresas_validas = pd.DataFrame(empresas_validas)

        # Novo filtro: ignora segmentos com apenas uma empresa
        if len(empresas_validas) <= 1:
            continue

        def carregar_dados_empresa(row_dict):
            try:
                tk = f"{row_dict['ticker']}.SA"
                mult = load_multiplos_from_db(tk)
                dre = load_data_from_db(tk)
                if mult is None or dre is None or mult.empty or dre.empty:
                    return None
                mult['Ano'] = pd.to_datetime(mult['Data'], errors='coerce').dt.year
                dre['Ano'] = pd.to_datetime(dre['Data'], errors='coerce').dt.year
                return {
                    "ticker": row_dict["ticker"],
                    "nome": row_dict["nome_empresa"],
                    "multiplos": mult,
                    "dre": dre
                }
            except Exception:
                return None

        with ThreadPoolExecutor(max_workers=10) as executor:
            resultados = list(executor.map(carregar_dados_empresa, empresas_validas.to_dict("records")))

        lista_empresas = [r for r in resultados if r is not None]

        if not lista_empresas:
            continue

        setores_empresa = {e["ticker"]: obter_setor_da_empresa(e["ticker"], setores_df) for e in lista_empresas}
        pesos = get_pesos(setor)
        score = calcular_score_acumulado(lista_empresas, setores_empresa, pesos, dados_macro, anos_minimos=4)
        precos = baixar_precos([e['ticker'] + ".SA" for e in lista_empresas])
        precos_mensal = precos.resample('M').last()
        score = penalizar_plato(score, precos_mensal, meses=12, penal=0.30)

        if score.empty:
            continue

        tickers = score['ticker'].unique()
        dividendos = coletar_dividendos(tickers)
        lideres = determinar_lideres(score)
        patrimonio_empresas, datas_aportes = gerir_carteira(precos, score, lideres, dividendos)

        if patrimonio_empresas.empty:
            continue

        patrimonio_empresas = patrimonio_empresas.apply(pd.to_numeric, errors='coerce')
        final_empresas = patrimonio_empresas.iloc[-1].drop("Patrimônio", errors='ignore').sum()
        patrimonio_selic = calcular_patrimonio_selic_macro(dados_macro, datas_aportes)

        if patrimonio_selic.empty:
            continue

        final_selic = patrimonio_selic.iloc[-1]["Tesouro Selic"]
        diff = ((final_empresas / final_selic) - 1) * 100

        if diff < margem_superior:
            continue

        st.markdown(f"### {setor} > {subsetor} > {segmento}")
        st.markdown(f"**Valor final da estratégia:** R$ {final_empresas:,.2f} ({diff:.1f}% acima do Tesouro Selic)")

        empresas_estrategia = patrimonio_empresas.columns.drop("Patrimônio", errors='ignore')
        colunas_empresas = st.columns(min(3, len(empresas_estrategia)))

        for idx, ticker in enumerate(empresas_estrategia):
            col = colunas_empresas[idx % len(colunas_empresas)]
            logo_url = get_logo_url(ticker)
            nome = next((e['nome'] for e in lista_empresas if e['ticker'] == ticker), ticker)
            valor_final = patrimonio_empresas[ticker].iloc[-1]
            perc_part = (valor_final / final_empresas) * 100 if final_empresas != 0 else 0
            anos_lider = lideres[lideres['ticker'] == ticker]['Ano'].tolist()
            anos_lider_str = f"{len(anos_lider)}x Líder: {', '.join(map(str, anos_lider))}" if anos_lider else ""

            col.markdown(f"""
                <div style='border: 1px solid #ccc; border-radius: 8px; padding: 10px; margin-bottom: 10px; text-align: center;'>
                    <img src='{logo_url}' width='40' />
                    <p style='margin: 5px 0 0; font-weight: bold;'>{nome}</p>
                    <p style='margin: 0; color: #666; font-size: 12px;'>({ticker})</p>
                    <p style='font-size: 12px; color: #999;'>{anos_lider_str}</p>
                    <p style='font-size: 12px; color: #2c3e50;'>Participação: {perc_part:.1f}%</p>
                </div>
            """, unsafe_allow_html=True)

        ultimo_ano = score['Ano'].max()
        lideres_ano_anterior = lideres[lideres['Ano'] == ultimo_ano]

        for _, row in lideres_ano_anterior.iterrows():
            empresas_lideres_finais.append({
                'ticker': row['ticker'],
                'nome': next((e['nome'] for e in lista_empresas if e['ticker'] == row['ticker']), row['ticker']),
                'logo_url': get_logo_url(row['ticker']),
                'ano_lider': row['Ano'],
                'ano_compra': row['Ano'] + 1,
                'setor': setor
            })

    if empresas_lideres_finais:
        st.markdown("## \U0001F4D1 Empresas líderes para o próximo ano")
        colunas_lideres = st.columns(3)
        for idx, emp in enumerate(empresas_lideres_finais):
            col = colunas_lideres[idx % 3]
            col.markdown(f"""
                <div style='border: 2px solid #28a745; border-radius: 10px; padding: 12px; margin-bottom: 10px; background-color: #f0fff4; text-align: center;'>
                    <img src="{emp['logo_url']}" width="45" />
                    <h5 style="margin: 5px 0 0;">{emp['nome']}</h5>
                    <p style="margin: 0; color: #666; font-size: 13px;">({emp['ticker']})</p>
                    <p style="font-size: 12px; color: #333;">Líder em {emp['ano_lider']}<br>Para compra em {emp['ano_compra']}</p>
                </div>
            """, unsafe_allow_html=True)

        st.markdown("## \U0001F4CA Distribuição setorial do portfólio sugerido")
        setores_portfolio = pd.Series([e['setor'] for e in empresas_lideres_finais]).value_counts()
        fig, ax = plt.subplots()
        ax.pie(setores_portfolio.values, labels=setores_portfolio.index, autopct='%1.1f%%', startangle=90, textprops={'fontsize': 10})
        ax.axis('equal')
        st.pyplot(fig)

    # Etapa 4 - Desempenho parcial no ano corrente
    if empresas_lideres_finais:
        st.markdown("## \U0001F4CA Desempenho parcial das líderes (ano atual)")
    
        ano_corrente = datetime.now().year
    
        tickers_corrente = [e['ticker'] for e in empresas_lideres_finais if e['ano_compra'] == ano_corrente]
     
        if tickers_corrente:
            tickers_corrente_yf = [tk + ".SA" for tk in tickers_corrente]
            precos = baixar_precos_ano_corrente(tickers_corrente_yf)
            precos.index = pd.to_datetime(precos.index, errors='coerce')
            precos = precos.resample('B').last().ffill()
    
            if precos.empty:
                st.warning("⚠️ Dados de preço indisponíveis para as ações escolhidas no ano atual.")
                st.stop()
    
            carteira = {tk.replace(".SA", ""): 0 for tk in tickers_corrente_yf}
            tickers_limpos = [tk.replace(".SA", "") for tk in tickers_corrente_yf]
            dividendos_dict = coletar_dividendos(tickers_corrente_yf)
            
            datas_potenciais = pd.date_range(start=f"{ano_corrente}-01-01", end=f"{ano_corrente}-12-31", freq='MS')
            st.markdown("Datas Potenciais")
            st.dataframe(datas_potenciais)
            datas_aporte = []
            
            if not tickers_limpos:
                st.warning("⚠️ Nenhum ticker disponível para prever data de compra.")
            else:
                for data in datas_potenciais:
                    data_valida = encontrar_proxima_data_valida(data, precos)
                    if data_valida is not None and data_valida in precos.index:
                        datas_aporte.append(data_valida)
            st.markdown("Datas de Aporte")
            st.datframe(datas_aporte)
               
    
            patrimonio_aporte = gerir_carteira_simples(precos, tickers_limpos, datas_aporte, dividendos_dict=dividendos_dict)
            st.write("📊 Debug patrimônio_aporte (início):")
            st.line_chart(patrimonio_aporte.head(20))
          
            # Selic benchmark
            valor_selic = 0
            patrimonio_selic = []
            dados_macro.index = pd.to_datetime(dados_macro.index, errors='coerce')
            for data in datas_aporte:
                ano_ref = data.year
                try:
                    taxa_anual = dados_macro.loc[dados_macro.index.year == ano_ref, "Selic"].iloc[0] / 100
                except IndexError:
                    st.warning(f"[DEBUG] Taxa Selic não encontrada para o ano: {ano_ref}")
                    continue
    
                taxa_mensal = (1 + taxa_anual) ** (1 / 12) - 1
                valor_selic = (valor_selic + 1000) * (1 + taxa_mensal)
                patrimonio_selic.append((data, valor_selic))
    
            df_selic = pd.DataFrame(patrimonio_selic, columns=["Data", "Tesouro Selic"]).set_index("Data")
            df_selic = df_selic.reindex(patrimonio_aporte.index).ffill()
    
            df_final = pd.concat([
                patrimonio_aporte.rename("Estratégia de Aporte"),
                df_selic
            ], axis=1).dropna()
    
    #        st.write("DataFrame final consolidado:", df_final.head())
    
            if df_final.empty or df_final["Tesouro Selic"].isna().all():
                st.warning("⚠️ Não foi possível construir gráfico com os dados disponíveis.")
                st.stop()
    
            st.markdown(f"### Comparativo de desempenho parcial em {ano_corrente}")
            fig, ax = plt.subplots(figsize=(10, 5))
            df_final["Estratégia de Aporte"].plot(ax=ax, label="Estratégia de Aporte", color='red')
            df_final["Tesouro Selic"].plot(ax=ax, label="Tesouro Selic", color='blue')
            ax.set_ylabel("Valor acumulado (R$")
            ax.set_xlabel("Data")
            ax.legend()
            ax.grid(True, linestyle="--", alpha=0.5)
            st.pyplot(fig)
    
            # Bloco final com resumo do desempenho
            valor_estrategia_final = df_final["Estratégia de Aporte"].iloc[-1]
            valor_selic_final = df_final["Tesouro Selic"].iloc[-1]
            desempenho = ((valor_estrategia_final / valor_selic_final) - 1) * 100
            patrimonio_total_aplicado = 1000 * len(datas_aporte)
            retorno_estrategia = ((valor_estrategia_final / patrimonio_total_aplicado) - 1) * 100
    
            if desempenho > 0:
                cor = "green"
                mensagem = f"A estratégia de aportes nas empresas líderes superou o Tesouro Selic em {desempenho:.2f}% no ano de {ano_corrente}."
            else:
                cor = "red"
                mensagem = f"A estratégia de aportes nas empresas líderes ficou {abs(desempenho):.2f}% abaixo do Tesouro Selic no ano de {ano_corrente}."
    
            st.markdown(f"""
            <div style="margin-top: 20px; padding: 15px; border-radius: 8px; background-color: #f9f9f9; border-left: 5px solid {cor};">
                <h4 style="margin: 0;">📊 Resultado Comparativo</h4>
                <p style="font-size: 16px; color: #333;">{mensagem}</p>
                <p style="font-size: 14px; color: #666;">Retorno total da estratégia sobre o capital aportado no ano: <strong>{retorno_estrategia:.2f}%</strong></p>
                <p style="font-size: 14px; color: #999;">Baseado nas empresas líderes selecionadas com score fundamentalista ajustado.</p>
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown("<hr>", unsafe_allow_html=True)
