"""
pickup/fix_setores_faltantes.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Adiciona ao metadados.db (e Supabase) os tickers que têm dados de DRE
mas estão ausentes da tabela 'setores'.

42 tickers identificados em 02/05/2026:
  AESB3, ALSO3, APER3, ARZZ3, ATMP3, ATOM3, BKBR3, BPAC11, BRBI11,
  BRGE11, BRIT3, BRIV3, BRML3, C3, CARD3, CCRO3, CIEL3, CPLE11, CRDE3,
  CRIV3, EEEL3, ENAT3, ENBR3, ENGI11, FRTA3, GPIV33, INNT3, IVPR3B,
  K3, KLBN11, M3B, MEGA3, NINJ3, PRMN3B, PTCA11, RBNS11, RRRP3, SOMA3,
  STKF3, TEKA3, WIZS3, YBRA3B

Execute:
    python pickup/fix_setores_faltantes.py

Requer SUPABASE_DB_URL no ambiente.
"""
from __future__ import annotations

import os
import sqlite3

import pandas as pd
from sqlalchemy import create_engine, text

# ── Classificação correta (B3 SETOR / SUBSETOR / SEGMENTO) ──────────────────
# Fonte: classificação setorial B3 (https://www.b3.com.br/pt_br/produtos-e-servicos/
#         negociacao/renda-variavel/acoes/consultas/classificacao-setorial/)
FALTANTES = [
    # ticker        nome_empresa                    SETOR                           SUBSETOR                            SEGMENTO                        LISTAGEM
    ("AESB3",  "AES BRASIL",               "Utilidade Pública",         "Energia Elétrica",             "Energia Elétrica",             "NM"),
    ("ALSO3",  "ALLOS",                    "Financeiro",                "Exploração de Imóveis",        "Exploração de Imóveis",        "NM"),
    ("APER3",  "AMBIPAR",                  "Bens Industriais",          "Serviços",                     "Serviços Ambientais",          "NM"),
    ("ARZZ3",  "AREZZO CO",                "Consumo Cíclico",           "Tecidos, Vestuário e Calçados","Calçados",                     "NM"),
    ("ATMP3",  "ATMOSFERA",                "Saúde",                     "Serviços Médico-Hospitalares", "Serviços Médico-Hospitalares", "NM"),
    ("ATOM3",  "ATOM",                     "Financeiro",                "Serviços Financeiros Diversos","Serviços Financeiros Diversos", "N2"),
    ("BKBR3",  "BK BRASIL",                "Consumo Cíclico",           "Hotéis e Restaurantes",        "Restaurante e Similares",      "NM"),
    ("BPAC11", "BTG PACTUAL",              "Financeiro",                "Intermediários Financeiros",   "Bancos",                       "N2"),
    ("BRBI11", "BR BANKS",                 "Financeiro",                "Intermediários Financeiros",   "Bancos",                       "NM"),
    ("BRGE11", "BRADESPAR",                "Financeiro",                "Holdings Diversificadas",      "Holdings Diversificadas",      "N1"),
    ("BRIT3",  "BRITANIA",                 "Consumo Cíclico",           "Utilidades Domésticas",        "Eletrodomésticos",             "NM"),
    ("BRIV3",  "BRISANET",                 "Comunicações",              "Telecomunicações",             "Telecomunicações",             "NM"),
    ("BRML3",  "BR MALLS PAR",             "Financeiro",                "Exploração de Imóveis",        "Exploração de Imóveis",        "NM"),
    ("C3",     "C3",                       "Consumo Cíclico",           "Diversos",                     "Programas de Fidelização",     "NM"),
    ("CARD3",  "CSU CARDSYST",             "Financeiro",                "Serviços Financeiros Diversos","Serviços Financeiros Diversos", "NM"),
    ("CCRO3",  "CCR SA",                   "Bens Industriais",          "Transporte",                   "Exploração de Rodovias",       "NM"),
    ("CIEL3",  "CIELO",                    "Financeiro",                "Serviços Financeiros Diversos","Serviços Financeiros Diversos", "NM"),
    ("CPLE11", "COPEL",                    "Utilidade Pública",         "Energia Elétrica",             "Energia Elétrica",             "NM"),
    ("CRDE3",  "CREDITAS",                 "Financeiro",                "Serviços Financeiros Diversos","Serviços Financeiros Diversos", "NM"),
    ("CRIV3",  "CRUZEIRO SUL",             "Financeiro",                "Intermediários Financeiros",   "Bancos",                       "NM"),
    ("EEEL3",  "DESENVIX",                 "Utilidade Pública",         "Energia Elétrica",             "Energia Elétrica",             "NM"),
    ("ENAT3",  "ENAUTA PART",              "Petróleo, Gás e Biocombustíveis","Petróleo, Gás e Biocombustíveis","Exploração e Produção","NM"),
    ("ENBR3",  "EDP BRASIL",               "Utilidade Pública",         "Energia Elétrica",             "Energia Elétrica",             "NM"),
    ("ENGI11", "ENERGISA",                 "Utilidade Pública",         "Energia Elétrica",             "Energia Elétrica",             "NM"),
    ("FRTA3",  "INFRACOMMERCE",            "Tecnologia da Informação",  "Programas e Serviços",         "Programas e Serviços",         "NM"),
    ("GPIV33", "GP INVEST",                "Financeiro",                "Holdings Diversificadas",      "Holdings Diversificadas",      "BDR"),
    ("INNT3",  "INNOQ",                    "Tecnologia da Informação",  "Programas e Serviços",         "Programas e Serviços",         "NM"),
    ("IVPR3B", "IVAIPORÃ",                 "Utilidade Pública",         "Energia Elétrica",             "Energia Elétrica",             ""),
    ("K3",     "K3",                       "Tecnologia da Informação",  "Programas e Serviços",         "Programas e Serviços",         "NM"),
    ("KLBN11", "KLABIN SA",                "Materiais Básicos",         "Madeira e Papel",              "Papel e Celulose",             "NM"),
    ("M3B",    "M3B",                      "Financeiro",                "Serviços Financeiros Diversos","Serviços Financeiros Diversos", ""),
    ("MEGA3",  "METAL LEVE",               "Bens Industriais",          "Material de Transporte",       "Automóveis e Motos",           "NM"),
    ("NINJ3",  "NINJA",                    "Financeiro",                "Serviços Financeiros Diversos","Serviços Financeiros Diversos", "NM"),
    ("PRMN3B", "PRIMAV INFRA",             "Bens Industriais",          "Transporte",                   "Exploração de Rodovias",       ""),
    ("PTCA11", "PETROCASA",                "Petróleo, Gás e Biocombustíveis","Petróleo, Gás e Biocombustíveis","Refino, Gás e Petroquímicos",""),
    ("RBNS11", "RIBEIRÃO BONI",            "Financeiro",                "Exploração de Imóveis",        "Exploração de Imóveis",        ""),
    ("RRRP3",  "3R PETROLEUM",             "Petróleo, Gás e Biocombustíveis","Petróleo, Gás e Biocombustíveis","Exploração e Produção","NM"),
    ("SOMA3",  "GRUPO SOMA",               "Consumo Cíclico",           "Tecidos, Vestuário e Calçados","Vestuário",                    "NM"),
    ("STKF3",  "STOCK FOREST",             "Materiais Básicos",         "Madeira e Papel",              "Madeira",                      ""),
    ("TEKA3",  "TEKA",                     "Consumo Cíclico",           "Tecidos, Vestuário e Calçados","Fios e Tecidos",               ""),
    ("WIZS3",  "WIZE",                     "Financeiro",                "Previdência e Seguros",        "Seguradoras",                  "NM"),
    ("YBRA3B", "YBRA",                     "Financeiro",                "Serviços Financeiros Diversos","Serviços Financeiros Diversos", ""),
]


def _insert_sqlite(sqlite_path: str, rows: list[tuple]) -> int:
    """Insere/atualiza os registros no metadados.db (sem PK, usa DELETE+INSERT)."""
    conn = sqlite3.connect(sqlite_path)
    cur  = conn.cursor()
    tickers = [r[0] for r in rows]
    # Remove entradas antigas para os tickers que vamos inserir
    placeholders = ",".join("?" * len(tickers))
    cur.execute(f'DELETE FROM setores WHERE ticker IN ({placeholders})', tickers)
    sql = """
    INSERT INTO setores (ticker, nome_empresa, "SETOR", "SUBSETOR", "SEGMENTO", "LISTAGEM")
    VALUES (?, ?, ?, ?, ?, ?)
    """
    cur.executemany(sql, rows)
    n = len(rows)
    conn.commit()
    conn.close()
    return n


def _upsert_supabase(engine, rows: list[tuple]) -> int:
    """Upsert no Supabase."""
    sql = text("""
    INSERT INTO public.setores (ticker, nome_empresa, "SETOR", "SUBSETOR", "SEGMENTO", "LISTAGEM")
    VALUES (:ticker, :nome, :setor, :subsetor, :segmento, :listagem)
    ON CONFLICT (ticker) DO UPDATE SET
        nome_empresa = EXCLUDED.nome_empresa,
        "SETOR"      = EXCLUDED."SETOR",
        "SUBSETOR"   = EXCLUDED."SUBSETOR",
        "SEGMENTO"   = EXCLUDED."SEGMENTO",
        "LISTAGEM"   = EXCLUDED."LISTAGEM"
    """)
    params = [
        {"ticker": r[0], "nome": r[1], "setor": r[2],
         "subsetor": r[3], "segmento": r[4], "listagem": r[5]}
        for r in rows
    ]
    with engine.begin() as conn:
        conn.execute(sql, params)
    return len(rows)


def main():
    sqlite_path = os.getenv("SQLITE_METADADOS_PATH", "data/metadados.db")
    supabase_url = os.getenv("SUPABASE_DB_URL")

    if not os.path.exists(sqlite_path):
        print(f"[ERRO] SQLite não encontrado: {sqlite_path}")
        return

    rows = [(t, n, s, sub, seg, lst) for t, n, s, sub, seg, lst in FALTANTES]

    # 1. SQLite local
    n_sqlite = _insert_sqlite(sqlite_path, rows)
    print(f"[OK] SQLite: {len(rows)} registros inseridos/atualizados em '{sqlite_path}'")

    # 2. Supabase (opcional — só se SUPABASE_DB_URL estiver configurada)
    if supabase_url:
        engine = create_engine(supabase_url, pool_pre_ping=True)
        n_sup = _upsert_supabase(engine, rows)
        print(f"[OK] Supabase: {n_sup} registros inseridos/atualizados")
    else:
        print("[AVISO] SUPABASE_DB_URL não definida — pulando sync com Supabase.")
        print("         Execute depois: python pickup/dados_setores_b3.py")

    # Verificação final
    conn = sqlite3.connect(sqlite_path)
    total = pd.read_sql_query("SELECT COUNT(*) as n FROM setores", conn).iloc[0]["n"]
    conn.close()
    print(f"\n[TOTAL] setores agora tem {total} empresas no SQLite.")


if __name__ == "__main__":
    main()
