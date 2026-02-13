import feedparser
import httpx
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import logging

logger = logging.getLogger(__name__)

YAHOO_RSS_URL = "https://finance.yahoo.com/rss/headline?s={ticker}"


async def fetch_news(ticker: str) -> list[dict]:
    url = YAHOO_RSS_URL.format(ticker=ticker)

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url)
            resp.raise_for_status()

        feed = feedparser.parse(resp.text)
        articles = []

        for entry in feed.entries:
            published_at = None
            if hasattr(entry, "published"):
                try:
                    published_at = parsedate_to_datetime(entry.published).astimezone(timezone.utc).isoformat()
                except Exception:
                    published_at = datetime.now(timezone.utc).isoformat()

            articles.append({
                "ticker": ticker,
                "title": entry.get("title", ""),
                "summary": entry.get("summary", ""),
                "url": entry.get("link", ""),
                "source": "Yahoo Finance",
                "published_at": published_at,
                "sentiment": None,
                "sentiment_reasoning": None,
            })

        return articles

    except Exception as e:
        logger.error(f"Yahoo RSS fetch failed for {ticker}: {e}")
        raise
