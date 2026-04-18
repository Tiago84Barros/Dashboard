# cvm_map_v2.py
# Camada de normalização CVM V2

import re
import pandas as pd
from core.db import get_engine

def fetch_raw():
    engine = get_engine()
    query = "SELECT * FROM cvm_financial_raw"
    return pd.read_sql(query, engine)

def fetch_mapping():
    engine = get_engine()
    query = "SELECT * FROM cvm_account_map WHERE ativo = TRUE ORDER BY prioridade"
    return pd.read_sql(query, engine)

def match_row(row, mappings):
    for _, m in mappings.iterrows():
        if m['cd_conta'] and row['cd_conta'] == m['cd_conta']:
            return m, 'exact'
        if m['ds_conta_pattern'] and re.search(m['ds_conta_pattern'], str(row['ds_conta'] or ''), re.IGNORECASE):
            return m, 'regex'
    return None, 'fallback'

def normalize():
    df = fetch_raw()
    mappings = fetch_mapping()
    results = []

    for _, row in df.iterrows():
        mapping, quality = match_row(row, mappings)
        if mapping is None:
            continue

        valor = row['vl_conta'] * mapping['sinal']

        results.append({
            "ticker": row['ticker'],
            "cd_cvm": row['cd_cvm'],
            "source_doc": row['source_doc'],
            "tipo_demo": row['tipo_demo'],
            "dt_refer": row['dt_refer'],
            "canonical_key": mapping['canonical_key'],
            "valor": valor,
            "unidade": "BRL",
            "qualidade_mapeamento": quality,
            "row_hash": row['row_hash']
        })

    return pd.DataFrame(results)

def save(df):
    engine = get_engine()
    df.to_sql("cvm_financial_normalized", engine, if_exists="append", index=False)

def main():
    df_norm = normalize()
    if not df_norm.empty:
        save(df_norm)
        print(f"Normalização concluída: {len(df_norm)} linhas inseridas.")
    else:
        print("Nenhum dado normalizado.")

if __name__ == "__main__":
    main()
