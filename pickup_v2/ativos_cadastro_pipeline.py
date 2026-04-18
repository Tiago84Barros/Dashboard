import psycopg2
from datetime import datetime

# 🔐 CONFIG (substitua pelos seus dados)
DB_URL = "postgresql://postgres:SENHA@HOST:PORT/postgres"

def get_connection():
    return psycopg2.connect(DB_URL)

def run_pipeline():
    conn = get_connection()
    cur = conn.cursor()

    print("🚀 Iniciando pipeline ativos_cadastro...")

    # EXEMPLO INICIAL (vamos expandir depois)
    ativos = [
        ("PETR3", "PETR3.SA", "9512", "33.000.167/0001-01", "BRPETRACNOR9", "Petrobras"),
        ("VALE3", "VALE3.SA", "4170", "33.592.510/0001-54", "BRVALEACNOR0", "Vale"),
        ("ITUB4", "ITUB4.SA", "19348", "60.701.190/0001-04", "BRITUBACNPR1", "Itaú"),
    ]

    for a in ativos:
        cur.execute("""
            insert into curated.ativos_cadastro (
                ticker, ticker_yahoo, codigo_cvm, cnpj, isin, nome_empresa, source, updated_at
            ) values (%s,%s,%s,%s,%s,%s,%s,%s)
            on conflict (ticker) do update set
                nome_empresa = excluded.nome_empresa,
                updated_at = now()
        """, (
            a[0], a[1], a[2], a[3], a[4], a[5],
            "manual_seed",
            datetime.utcnow()
        ))

    conn.commit()
    cur.close()
    conn.close()

    print("✅ Pipeline finalizado!")

if __name__ == "__main__":
    run_pipeline()
