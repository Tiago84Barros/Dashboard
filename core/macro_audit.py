from sqlalchemy import text
from sqlalchemy.engine import Engine


def audit_macro(engine: Engine) -> dict:
    results = {}

    with engine.connect() as conn:
        results["total_linhas"] = conn.execute(
            text("select count(*) from cvm.info_economica_mensal")
        ).scalar()

        results["datas_distintas"] = conn.execute(
            text("select count(distinct data) from cvm.info_economica_mensal")
        ).scalar()

        results["linhas_sem_cambio"] = conn.execute(
            text("""
                select count(*) 
                from cvm.info_economica_mensal 
                where cambio is null
            """)
        ).scalar()

        results["intervalo_datas"] = conn.execute(
            text("""
                select min(data) as inicio, max(data) as fim
                from cvm.info_economica_mensal
            """)
        ).mappings().first()

    return results
