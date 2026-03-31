from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
import hashlib
import re
import xml.etree.ElementTree as ET

import requests

from core.ticker_utils import normalize_ticker


# ─────────────────────────────────────────────────────────────
# MODELOS
# ─────────────────────────────────────────────────────────────

@dataclass
class NewsItem:
    ticker: str
    title: str
    link: str
    source: str
    published_at: Optional[datetime]
    snippet: str


# ─────────────────────────────────────────────────────────────
# HELPERS RSS
# ─────────────────────────────────────────────────────────────

def _google_news_rss_url(query: str, days: int = 60) -> str:
    q = query.strip()
    q = re.sub(r"\s+", " ", q)
    return (
        "https://news.google.com/rss/search?"
        f"q={requests.utils.quote(q + f' when:{days}d')}"
        "&hl=pt-BR&gl=BR&ceid=BR:pt-419"
    )


def _safe_text(x: Optional[str]) -> str:
    return (x or "").strip()


def _parse_rss_datetime(pub: str) -> Optional[datetime]:
    pub = (pub or "").strip()
    if not pub:
        return None
    for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S %z"):
        try:
            dt = datetime.strptime(pub, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            continue
    return None


def _dedup_key(title: str, link: str) -> str:
    raw = (title or "") + "||" + (link or "")
    return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()


# ─────────────────────────────────────────────────────────────
# COLETA RSS
# ─────────────────────────────────────────────────────────────

def fetch_google_news_rss(
    query: str,
    days: int = 60,
    timeout: int = 12,
) -> List[Tuple[str, str, str, str]]:
    """
    Retorna tuplas: (title, link, source, pubDate)
    """
    url = _google_news_rss_url(query, days=days)
    r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()

    root = ET.fromstring(r.text)
    channel = root.find("channel")
    if channel is None:
        return []

    out: List[Tuple[str, str, str, str]] = []
    for item in channel.findall("item"):
        title = _safe_text(item.findtext("title"))
        link = _safe_text(item.findtext("link"))
        pub = _safe_text(item.findtext("pubDate"))

        src_el = item.find("source")
        source = _safe_text(src_el.text if src_el is not None else "")

        if title and link:
            out.append((title, link, source, pub))
    return out


# ─────────────────────────────────────────────────────────────
# CONSTRUÇÃO POR TICKER
# ─────────────────────────────────────────────────────────────

def build_news_items_for_ticker(
    *,
    ticker: str,
    company_name: str,
    days: int = 60,
    max_items: int = 15,
) -> List[NewsItem]:
    tk = normalize_ticker(ticker)
    name = (company_name or tk).strip()

    query = f'{tk} OR "{name}"'

    rows = fetch_google_news_rss(query, days=days)

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days)

    seen = set()
    items: List[NewsItem] = []
    for title, link, source, pub in rows:
        key = _dedup_key(title, link)
        if key in seen:
            continue
        seen.add(key)

        dt = _parse_rss_datetime(pub)
        if dt is not None and dt < cutoff:
            continue

        items.append(
            NewsItem(
                ticker=tk,
                title=title,
                link=link,
                source=source or "Fonte não informada",
                published_at=dt,
                snippet="",
            )
        )

    items.sort(
        key=lambda x: x.published_at or datetime(1970, 1, 1, tzinfo=timezone.utc),
        reverse=True,
    )
    return items[:max_items]


def build_news_for_portfolio(
    *,
    tickers_and_names: List[Tuple[str, str]],
    days: int = 60,
    max_items_per_ticker: int = 15,
) -> Dict[str, List[NewsItem]]:
    out: Dict[str, List[NewsItem]] = {}
    for tk, nm in tickers_and_names:
        try:
            out[tk] = build_news_items_for_ticker(
                ticker=tk,
                company_name=nm,
                days=days,
                max_items=max_items_per_ticker,
            )
        except Exception:
            out[tk] = []
    return out


# ─────────────────────────────────────────────────────────────
# 🔹 NOVO — CONTEXTO PARA PATCH 7
# ─────────────────────────────────────────────────────────────

def build_news_context_for_tickers(
    *,
    tickers_and_names: List[Tuple[str, str]],
    days: int = 60,
    max_items_per_ticker: int = 10,
) -> Dict[str, List[Dict]]:
    """
    Retorna contexto simplificado por ticker, pronto para:
    - Patch 7 (validação de evidências)
    - Relatórios narrativos
    - Auditoria / explicabilidade

    NÃO chama LLM.
    """

    raw = build_news_for_portfolio(
        tickers_and_names=tickers_and_names,
        days=days,
        max_items_per_ticker=max_items_per_ticker,
    )

    context: Dict[str, List[Dict]] = {}

    for tk, items in raw.items():
        rows: List[Dict] = []
        for it in items:
            rows.append(
                {
                    "title": it.title,
                    "source": it.source,
                    "date": it.published_at.isoformat() if it.published_at else None,
                    "link": it.link,
                }
            )
        context[tk] = rows

    return context
