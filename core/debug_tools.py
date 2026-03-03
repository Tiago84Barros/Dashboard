
from __future__ import annotations

import traceback
from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd
import streamlit as st


@dataclass
class DebugSnapshot:
    name: str
    typ: str
    shape: Optional[tuple] = None
    dtype: Optional[str] = None
    length: Optional[int] = None
    na_count: Optional[int] = None


def dbg(name: str, obj: Any, max_rows: int = 5) -> DebugSnapshot:
    """Snapshot amigável no Streamlit para DataFrame/Series/objetos genéricos."""
    snap = DebugSnapshot(name=name, typ=str(type(obj)))

    try:
        if isinstance(obj, pd.DataFrame):
            snap.shape = obj.shape
            st.write(f"🧪 DBG `{name}`: DataFrame shape={obj.shape}")
            st.dataframe(obj.head(max_rows))
        elif isinstance(obj, pd.Series):
            snap.length = len(obj)
            snap.dtype = str(obj.dtype)
            snap.na_count = int(obj.isna().sum())
            st.write(
                f"🧪 DBG `{name}`: Series len={len(obj)} dtype={obj.dtype} "
                f"na={snap.na_count} notna_any={bool(obj.notna().any())}"
            )
            st.write(obj.head(max_rows))
        else:
            st.write(f"🧪 DBG `{name}`: {type(obj)} -> {obj}")
    except Exception:
        st.write(f"🧪 DBG `{name}`: falhou ao inspecionar ({type(obj)})")

    return snap


def show_trace(e: Exception, title: str = "Patch falhou") -> None:
    """Exibe stacktrace completo no Streamlit."""
    st.error(f"{title}: {type(e).__name__}: {e}")
    st.code(traceback.format_exc())
