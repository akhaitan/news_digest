"""Cron job: refresh news for all important stocks + user watchlists.

Combines the curated TOP_TICKERS list with every ticker in any user's
watchlist, deduplicates, and fetches news at 3 stocks/minute.

Usage:
    python jobs/refresh_all_news.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
import os

# Ensure the project root is on sys.path so `app.*` imports work
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db import init_db, get_all_watched_tickers
from app.services.news import refresh_stock
from app.sources.polygon_rate_limiter import limiter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("refresh_all_news")

# ── Curated ticker list ──────────────────────────────────────────────
TOP_TICKERS = [
    # Dow Jones 30
    "AAPL", "AMGN", "AXP", "BA", "CAT", "CRM", "CSCO", "CVX", "DIS", "DOW",
    "GS", "HD", "HON", "IBM", "INTC", "JNJ", "JPM", "KO", "MCD", "MMM",
    "MRK", "MSFT", "NKE", "PG", "TRV", "UNH", "V", "VZ", "WBA", "WMT",
    # Additional mega-caps
    "NVDA", "AMZN", "META", "GOOGL", "GOOG", "TSLA", "AVGO", "COST",
    "ADBE", "NFLX", "PEP", "AMD", "TMUS", "INTU", "TXN", "QCOM",
]


async def main():
    # Override the global rate limiter to 3 req/min
    limiter.max_requests = 3
    limiter.window_seconds = 60.0
    limiter._timestamps.clear()
    logger.info(
        f"Rate limiter set to {limiter.max_requests} req/{limiter.window_seconds:.0f}s"
    )

    init_db()

    # Merge top tickers + all user watchlist tickers, deduplicated
    user_tickers = get_all_watched_tickers()
    all_tickers = list(dict.fromkeys(TOP_TICKERS + user_tickers))  # preserves order, dedupes

    total = len(all_tickers)
    success = 0
    failed = 0
    all_errors: list[str] = []

    logger.info(
        f"Starting news refresh for {total} tickers "
        f"({len(TOP_TICKERS)} curated + {len(user_tickers)} from watchlists, "
        f"{total - len(TOP_TICKERS)} unique additions from users)"
    )

    for i, ticker in enumerate(all_tickers, 1):
        logger.info(f"[{i}/{total}] Refreshing {ticker}...")
        try:
            count, errors = await refresh_stock(ticker)
            if errors:
                all_errors.extend(errors)
                logger.warning(f"  {ticker}: {count} articles, errors: {errors}")
                failed += 1
            else:
                logger.info(f"  {ticker}: {count} articles")
                success += 1
        except Exception as e:
            logger.error(f"  {ticker}: unexpected error — {e}")
            all_errors.append(f"{ticker}: {e}")
            failed += 1

    logger.info(
        f"Done. {success}/{total} succeeded, {failed} had errors. "
        f"Total errors: {len(all_errors)}"
    )
    if all_errors:
        for err in all_errors:
            logger.error(f"  • {err}")


if __name__ == "__main__":
    asyncio.run(main())

