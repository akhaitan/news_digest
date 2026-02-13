import httpx
from datetime import datetime, timedelta, timezone
from app.config import MASSIVE_API_KEY, MASSIVE_BASE_URL
from app.sources.polygon_rate_limiter import limiter
import logging

logger = logging.getLogger(__name__)


async def fetch_news(ticker: str, days: int = 3) -> list[dict]:
    if not MASSIVE_API_KEY or MASSIVE_API_KEY == "your_api_key_here":
        logger.warning("MASSIVE_API_KEY not configured, skipping Massive fetch")
        return []

    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")

    url = f"{MASSIVE_BASE_URL}/v2/reference/news"
    params = {
        "ticker": ticker,
        "published_utc.gte": since,
        "limit": 50,
        "sort": "published_utc",
        "order": "desc",
        "apiKey": MASSIVE_API_KEY,
    }

    try:
        await limiter.acquire()
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, params=params)

            if resp.status_code == 429:
                logger.warning(f"Massive rate limit hit for {ticker}")
                raise RateLimitError(f"Rate limit hit for {ticker}")

            resp.raise_for_status()
            data = resp.json()

        articles = []
        for item in data.get("results", []):
            sentiment = None
            sentiment_reasoning = None

            for insight in item.get("insights", []):
                if insight.get("ticker") == ticker:
                    sentiment = insight.get("sentiment")
                    sentiment_reasoning = insight.get("sentiment_reasoning")
                    break

            articles.append({
                "ticker": ticker,
                "title": item.get("title", ""),
                "summary": item.get("description", ""),
                "url": item.get("article_url", ""),
                "source": item.get("publisher", {}).get("name", ""),
                "published_at": item.get("published_utc", ""),
                "sentiment": sentiment,
                "sentiment_reasoning": sentiment_reasoning,
            })

        return articles

    except RateLimitError:
        raise
    except Exception as e:
        logger.error(f"Polygon.io news fetch failed for {ticker}: {e}")
        raise


async def fetch_news_batch(tickers: list[str], days: int = 3) -> dict[str, list[dict]]:
    """Fetch news for multiple tickers in a single Polygon.io API call.

    Returns a dict mapping ticker -> list[article].
    """
    if not tickers:
        return {}
    if not MASSIVE_API_KEY or MASSIVE_API_KEY == "your_api_key_here":
        logger.warning("MASSIVE_API_KEY not configured, skipping batch fetch")
        return {t: [] for t in tickers}

    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    ticker_set = set(tickers)

    # Polygon.io requires repeated 'ticker' params (ticker=AAPL&ticker=MSFT)
    url = f"{MASSIVE_BASE_URL}/v2/reference/news"
    params: list[tuple[str, str]] = [("ticker", t) for t in tickers]
    params.extend([
        ("published_utc.gte", since),
        ("limit", str(min(50 * len(tickers), 1000))),
        ("sort", "published_utc"),
        ("order", "desc"),
        ("apiKey", MASSIVE_API_KEY),
    ])

    try:
        await limiter.acquire()
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(url, params=params)

            if resp.status_code == 429:
                logger.warning("Polygon.io rate limit hit for batch news request")
                raise RateLimitError("Rate limit hit for batch request")

            resp.raise_for_status()
            data = resp.json()

        # Group results by ticker
        result: dict[str, list[dict]] = {t: [] for t in tickers}

        for item in data.get("results", []):
            article_tickers = item.get("tickers", [])
            for t in article_tickers:
                if t not in ticker_set:
                    continue
                sentiment = None
                sentiment_reasoning = None
                for insight in item.get("insights", []):
                    if insight.get("ticker") == t:
                        sentiment = insight.get("sentiment")
                        sentiment_reasoning = insight.get("sentiment_reasoning")
                        break

                result[t].append({
                    "ticker": t,
                    "title": item.get("title", ""),
                    "summary": item.get("description", ""),
                    "url": item.get("article_url", ""),
                    "source": item.get("publisher", {}).get("name", ""),
                    "published_at": item.get("published_utc", ""),
                    "sentiment": sentiment,
                    "sentiment_reasoning": sentiment_reasoning,
                })

        logger.info(f"Batch news fetch: {len(data.get('results', []))} articles for {len(tickers)} tickers")
        return result

    except RateLimitError:
        raise
    except Exception as e:
        logger.error(f"Polygon.io batch news fetch failed: {e}")
        raise


class RateLimitError(Exception):
    pass
