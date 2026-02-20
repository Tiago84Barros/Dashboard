from __future__ import annotations

import hashlib
import json
import os
from typing import Any, Dict, List, Optional, Tuple

SNAP_TABLE = "portfolio_snapshots"
ITEMS_TABLE = "portfolio_snapshot_items"


def _get_env(name: str) -> Optional[str]:
    v = os.getenv(name)
    return v.strip() if isinstance(v, str) and v.strip() else None


def _get_supabase_url_and_key() -> Tuple[str, str]:
    url = _get_env("SUPABASE_URL") or _get_env("SUPABASE_PROJECT_URL")
    key = _get_env("SUPABASE_SERVICE_ROLE_KEY") or _get_env("SUPABASE_ANON_KEY")
    if not url or not key:
        raise RuntimeError(
            "Supabase não configurado: defina SUPABASE_URL (ou SUPABASE_PROJECT_URL) e "
            "SUPABASE_SERVICE_ROLE_KEY (ou SUPABASE_ANON_KEY) nas variáveis de ambiente."
        )
    return url.rstrip("/"), key


def _stable_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def compute_plan_hash(snapshot_header: Dict[str, Any], items: List[Dict[str, Any]]) -> str:
    """
    Hash determinístico (não inclui created_at).
    """
    payload = {
        "header": snapshot_header,
        "items": sorted(
            [{"ticker": i.get("ticker"), "peso": float(i.get("peso", 0))} for i in items],
            key=lambda x: (x["ticker"] or ""),
        ),
    }
    return hashlib.md5(_stable_json(payload).encode("utf-8")).hexdigest()


# ─────────────────────────────────────────────────────────────
# Cliente: tenta supabase-py; se não existir, usa REST PostgREST
# ─────────────────────────────────────────────────────────────

def _get_supabase_client():
    try:
        from supabase import create_client  # type: ignore
        url, key = _get_supabase_url_and_key()
        return create_client(url, key)
    except Exception:
        return None


def _rest_request(method: str, path: str, *, params: Optional[Dict[str, str]] = None, json_body: Any = None, extra_headers: Optional[Dict[str, str]] = None):
    import requests  # requests normalmente já existe no ambiente do Streamlit

    url, key = _get_supabase_url_and_key()
    full_url = f"{url}{path}"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Accept": "application/json",
    }
    if extra_headers:
        headers.update(extra_headers)

    resp = requests.request(method, full_url, params=params, json=json_body, headers=headers, timeout=30)
    if resp.status_code >= 400:
        raise RuntimeError(f"Supabase REST erro {resp.status_code}: {resp.text[:500]}")
    if resp.text.strip() == "":
        return None
    return resp.json()


def _select_latest_snapshot_rest() -> Optional[Dict[str, Any]]:
    rows = _rest_request(
        "GET",
        f"/rest/v1/{SNAP_TABLE}",
        params={
            "select": "*",
            "order": "created_at.desc",
            "limit": "1",
        },
    )
    if not rows:
        return None
    return rows[0]


def _select_items_rest(snapshot_id: str) -> List[Dict[str, Any]]:
    rows = _rest_request(
        "GET",
        f"/rest/v1/{ITEMS_TABLE}",
        params={
            "select": "*",
            "snapshot_id": f"eq.{snapshot_id}",
            "order": "id.asc",
        },
    )
    return list(rows or [])


def get_latest_snapshot() -> Optional[Dict[str, Any]]:
    """
    Retorna:
      {
        "snapshot": <row portfolio_snapshots>,
        "items": [ {ticker, peso, ...}, ... ]
      }
    """
    sb = _get_supabase_client()
    if sb is not None:
        snap_rows = (
            sb.table(SNAP_TABLE)
            .select("*")
            .order("created_at", desc=True)
            .limit(1)
            .execute()
            .data
        )
        if not snap_rows:
            return None
        snap = snap_rows[0]
        items = (
            sb.table(ITEMS_TABLE)
            .select("*")
            .eq("snapshot_id", snap["id"])
            .order("id", desc=False)
            .execute()
            .data
        ) or []
        return {"snapshot": snap, "items": items}

    # fallback REST (sem supabase-py)
    snap = _select_latest_snapshot_rest()
    if snap is None:
        return None
    items = _select_items_rest(str(snap["id"]))
    return {"snapshot": snap, "items": items}


def save_snapshot(
    *,
    tickers: List[Dict[str, Any]],
    selic_ref: Optional[float],
    margem_superior: Optional[float],
    tipo_empresa: Optional[str],
    filters_json: Optional[Dict[str, Any]] = None,
    notes: Optional[str] = None,
    status: str = "active",
) -> Dict[str, Any]:
    """
    Persiste snapshot e itens.
    - Faz UPSERT por plan_hash (índice único ux_portfolio_snapshots_plan_hash).
    - Sempre reescreve itens do snapshot retornado.
    """
    header = {
        "selic_ref": selic_ref,
        "margem_superior": margem_superior,
        "tipo_empresa": tipo_empresa,
        "filters_json": filters_json or {},
        "status": status,
        "notes": notes,
    }
    plan_hash = compute_plan_hash(header, tickers)
    header["plan_hash"] = plan_hash

    sb = _get_supabase_client()
    if sb is not None:
        up = sb.table(SNAP_TABLE).upsert(header, on_conflict="plan_hash").execute().data
        if not up:
            raise RuntimeError("Falha ao upsert em portfolio_snapshots.")
        snap = up[0]
        sid = snap["id"]

        # regrava itens
        sb.table(ITEMS_TABLE).delete().eq("snapshot_id", sid).execute()
        items_payload = [
            {"snapshot_id": sid, "ticker": i["ticker"], "peso": float(i.get("peso", 0))}
            for i in tickers
        ]
        if items_payload:
            sb.table(ITEMS_TABLE).insert(items_payload).execute()
        return {"snapshot": snap, "items": items_payload}

    # REST upsert
    snap_rows = _rest_request(
        "POST",
        f"/rest/v1/{SNAP_TABLE}",
        params={"on_conflict": "plan_hash"},
        json_body=header,
        extra_headers={
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=representation",
        },
    )
    if not snap_rows:
        raise RuntimeError("Falha ao upsert (REST) em portfolio_snapshots.")
    snap = snap_rows[0]
    sid = str(snap["id"])

    # delete antigos
    _rest_request("DELETE", f"/rest/v1/{ITEMS_TABLE}", params={"snapshot_id": f"eq.{sid}"})

    items_payload = [{"snapshot_id": sid, "ticker": i["ticker"], "peso": float(i.get("peso", 0))} for i in tickers]
    if items_payload:
        _rest_request(
            "POST",
            f"/rest/v1/{ITEMS_TABLE}",
            json_body=items_payload,
            extra_headers={
                "Content-Type": "application/json",
                "Prefer": "return=representation",
            },
        )

    return {"snapshot": snap, "items": items_payload}
