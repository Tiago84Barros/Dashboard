from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
import hashlib
import re
import xml.etree.ElementTree as ET

import requests


@dataclass
class NewsItem:
    ticker: str
    title: str
    link: str
    source: str
    published_at: Optional[datetime]
    snippet: str


def _google_news_rss_url(query: str, days: int = 60) -> str:
    # Google News RSS: ceid BR:pt-419, hl pt-BR, gl BR
    # "when:60d" costuma funcionar bem no Google News.
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
    # Ex: "Mon, 27 Jan 2026 19:22:00 GMT"
    pub = (pub or "").strip()
    if not pub:
        return None
    for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S %z"):
        try:
            dt = datetime.strptime(pub, fmt)
            # normaliza p/ timezone aware
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            continue
    return None


def _dedup_key(title: str, link: str) -> str:
    raw = (title or "") + "||" + (link or "")
    return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()


def fetch_google_news_rss(query: str, days: int = 60, timeout: int = 12) -> List[Tuple[str, str, str, str]]:
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

        # fonte no RSS do Google News normalmente vem em <source>
        src_el = item.find("source")
        source = _safe_text(src_el.text if src_el is not None else "")

        if title and link:
            out.append((title, link, source, pub))
    return out


def build_news_items_for_ticker(
    *,
    ticker: str,
    company_name: str,
    days: int = 60,
    max_items: int = 15,
) -> List[NewsItem]:
    """
    Coleta evidências (RSS) e retorna itens deduplicados e filtrados por janela.
    """
    tk = (ticker or "").upper().replace(".SA", "").strip()
    name = (company_name or tk).strip()

    # Query simples e robusta: ticker + nome
    # Você pode ajustar depois para incluir "B3" ou setor, se quiser.
    query = f'{tk} OR "{name}"'

    rows = fetch_google_news_rss(query, days=days)

    # Dedup + filtro de data (60 dias)
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
                snippet="",  # RSS não traz snippet consistente; deixamos vazio.
            )
        )

    # ordena por recência (desc) e corta
    items.sort(key=lambda x: x.published_at or datetime(1970, 1, 1, tzinfo=timezone.utc), reverse=True)
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
