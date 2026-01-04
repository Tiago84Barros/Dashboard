# pickup/dados_multiplos_itr.py
from __future__ import annotations

import os
from datetime import timezone
from typing import Dict, Optional, List

import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
import yfinance as yf

# =========================
# Config
# =========================
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
if not SUPABASE_DB_URL:
    raise RuntimeError("SUPABASE_DB_URL não definida")

ENGINE = sa.create_engine(SUPABASE_DB_URL)

ORIGEM_TRI = 'public."Demonstracoes_Financeiras_TRI"'
DEST_SCHEMA = "public"
DEST_TABLE = "multiplos_TRI"  # no DB: public."multiplos_TRI"

YF_BATCH_SIZE = int(os.getenv("YF_BATCH_SIZE", "50"))
# Opcional: limitar tickers para teste rápido
YF_MAX_TICKERS = int(os.getenv("YF_MAX_TICKERS", "0"))  # 0 = sem limite
# Opcional: pular preços (pipeline contábil puro)
SKIP_PRICE = os.getenv("SKIP_PRICE", "0") == "1"


def log(msg: str) -> None:
    print(msg, flush=True)


def to_utc_midnight_timestamptz(d: pd.Timestamp) -> pd.Timestamp:
    """
    origem TRI: date
    destino multiplos_TRI: timestamptz
    grava com
