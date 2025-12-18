from sqlalchemy import text

SYNC_TABLE_SQL = """
create schema if not exists cvm;

create table if not exists cvm.sync_state (
  key text primary key,
  value text,
  updated_at timestamptz not null default now()
);
"""

def ensure_sync_table(engine):
    with engine.begin() as conn:
        conn.execute(text(SYNC_TABLE_SQL))

def get_state(engine, key: str):
    with engine.begin() as conn:
        row = conn.execute(
            text("select value, updated_at from cvm.sync_state where key = :k"),
            {"k": key},
        ).fetchone()
        return None if row is None else {"value": row[0], "updated_at": row[1]}

def set_state(engine, key: str, value: str):
    with engine.begin() as conn:
        conn.execute(
            text("""
            insert into cvm.sync_state(key, value)
            values (:k, :v)
            on conflict (key) do update
              set value = excluded.value,
                  updated_at = now()
            """),
            {"k": key, "v": value},
        )
