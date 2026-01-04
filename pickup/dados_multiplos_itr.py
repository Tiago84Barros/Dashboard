# pickup/dados_multiplos_itr.py
from __future__ import annotations

import os
import re
import logging
from datetime import timezone
from typing import Dict, Optional, List

import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
import yfinance as yf

# =========================
# Silenciar ruído do yfinance
# =========================
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# =========================
# Config
# =========================
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
if not SUPABASE_DB_URL:
    raise RuntimeError("SUPABASE_DB_URL não definida")

ENGINE = sa.create_engine(SUPABASE_DB_URL)

ORIGEM_TRI = 'public."Demonstracoes_Financeiras_TRI"'
DEST_SCHEMA = "public"
DEST_TABLE = "multiplos_TRI"  # DB: public."multiplos_TRI"

YF_BATCH_SIZE = int(os.getenv("YF_BATCH_SIZE", "50"))
YF_MAX_TICKERS = int(os.getenv("
