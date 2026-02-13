"""Fetch all US stock tickers from Polygon.io reference data."""

import httpx
import asyncio
import logging
from urllib.parse import urlparse, parse_qs
from app.config import MASSIVE_API_KEY, MASSIVE_BASE_URL
from app.db import upsert_known_stocks
from app.sources.polygon_rate_limiter import limiter

logger = logging.getLogger(__name__)

# Major US exchanges
US_EXCHANGES = ["XNAS", "XNYS", "XASE"]  # NASDAQ, NYSE, AMEX
EXCHANGE_LABELS = {
    "XNAS": "NASDAQ",
    "XNYS": "NYSE",
    "XASE": "AMEX",
}

MAX_RETRIES_PER_PAGE = 3


async def fetch_all_us_tickers() -> list[dict]:
    """Fetch all active US stock tickers from Polygon.io with pagination.
    
    Saves partial results to DB after each exchange to survive interruptions.
    """
    if not MASSIVE_API_KEY or MASSIVE_API_KEY == "your_api_key_here":
        logger.warning("API key not configured, skipping ticker fetch")
        return []

    all_tickers = []

    for exchange in US_EXCHANGES:
        exchange_tickers = []
        cursor = None
        page = 0
        retries = 0

        while True:
            page += 1
            url = f"{MASSIVE_BASE_URL}/v3/reference/tickers"
            params = {
                "market": "stocks",
                "exchange": exchange,
                "active": "true",
                "limit": 1000,
                "order": "asc",
                "sort": "ticker",
                "apiKey": MASSIVE_API_KEY,
            }
            if cursor:
                params["cursor"] = cursor

            try:
                await limiter.acquire()
                async with httpx.AsyncClient(timeout=30) as client:
                    resp = await client.get(url, params=params)
                    if resp.status_code == 429:
                        retries += 1
                        if retries > MAX_RETRIES_PER_PAGE:
                            logger.warning(f"Max retries exceeded for {exchange} page {page}, moving on")
                            break
                        logger.warning(f"Rate limit hit for {exchange} page {page}, retry {retries}/{MAX_RETRIES_PER_PAGE}")
                        await asyncio.sleep(15)
                        page -= 1  # Don't increment page on retry
                        continue
                    resp.raise_for_status()
                    data = resp.json()

                retries = 0  # Reset retries on success
                results = data.get("results", [])
                for item in results:
                    ticker_type = item.get("type", "")
                    # Only common stocks and ETFs, skip warrants, rights, etc.
                    if ticker_type not in ("CS", "ETF", "ADRC"):
                        continue
                    exchange_tickers.append({
                        "ticker": item["ticker"],
                        "name": item.get("name", item["ticker"]),
                        "exchange": EXCHANGE_LABELS.get(exchange, exchange),
                    })

                next_url = data.get("next_url")
                if next_url:
                    parsed = urlparse(next_url)
                    qs = parse_qs(parsed.query)
                    cursor = qs.get("cursor", [None])[0]
                    if not cursor:
                        break
                else:
                    break

            except Exception as e:
                logger.error(f"Ticker fetch failed for {exchange} page {page}: {e}")
                break

        # Save partial results after each exchange
        if exchange_tickers:
            upsert_known_stocks(exchange_tickers)
            all_tickers.extend(exchange_tickers)
            logger.info(f"Saved {len(exchange_tickers)} tickers from {EXCHANGE_LABELS.get(exchange, exchange)}")

    logger.info(f"Total US tickers fetched: {len(all_tickers)}")
    return all_tickers
