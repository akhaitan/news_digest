from __future__ import annotations

import logging
from app.sources import massive, yahoo_rss
from app.sources.massive import RateLimitError
from app.db import (
    upsert_articles, update_refresh_log, get_articles, get_all_recent_articles,
    purge_old_articles, get_all_watched_tickers, get_user_tickers, get_stock_name,
    delete_articles_for_ticker, get_stale_tickers,
)

logger = logging.getLogger(__name__)

SUMMARY_PREFIX_LEN = 200


def _short_error(e: Exception) -> str:
    """Return a concise, user-friendly error message."""
    msg = str(e)
    # Strip verbose HTTP boilerplate from httpx errors
    if "\nFor more information check:" in msg:
        msg = msg.split("\nFor more information check:")[0]
    # Shorten common patterns
    if "429" in msg:
        return "rate limited"
    if "timeout" in msg.lower() or "timed out" in msg.lower():
        return "request timed out"
    if "connect" in msg.lower():
        return "could not connect"
    # Cap length
    return msg[:120]


def _is_relevant(article: dict, ticker: str, company_name: str | None) -> bool:
    """Article must mention the company in the title, and ticker/name must appear in title or summary prefix."""
    title = (article.get("title") or "").lower()
    summary_prefix = (article.get("summary") or "")[:SUMMARY_PREFIX_LEN].lower()

    # Title must mention the ticker or company name
    title_match = ticker.lower() in title or (company_name and company_name.lower() in title)
    if not title_match:
        return False

    # Also check broader text for reinforcement
    text = title + " " + summary_prefix
    if ticker.lower() in text:
        return True
    if company_name and company_name.lower() in text:
        return True
    return False


async def refresh_stock(ticker: str) -> tuple[int, list[str]]:
    """Fetch news for a ticker from all sources.
    
    Returns (article_count, errors).
    Polygon.io is primary; Yahoo RSS is only used as fallback if Polygon fails.
    Only replaces cached articles if at least one source succeeds.
    """
    errors: list[str] = []
    any_source_ok = False
    polygon_articles: list[dict] = []
    yahoo_articles: list[dict] = []

    try:
        polygon_articles = await massive.fetch_news(ticker)
        any_source_ok = True
    except RateLimitError:
        errors.append(f"Polygon.io rate limited ({ticker})")
    except Exception as e:
        errors.append(f"Polygon.io error for {ticker}: {_short_error(e)}")

    # Only fall back to Yahoo RSS if Polygon failed
    if not any_source_ok:
        try:
            yahoo_articles = await yahoo_rss.fetch_news(ticker)
            any_source_ok = True
        except Exception as e:
            errors.append(f"Yahoo Finance error for {ticker}: {_short_error(e)}")

    # Merge articles (polygon first, then unique yahoo)
    articles = polygon_articles[:]
    existing_urls = {a["url"] for a in articles}
    for a in yahoo_articles:
        if a["url"] not in existing_urls:
            articles.append(a)

    company_name = get_stock_name(ticker)
    articles = [a for a in articles if _is_relevant(a, ticker, company_name)]

    if any_source_ok:
        # At least one source responded — safe to replace cache
        delete_articles_for_ticker(ticker)
        if articles:
            upsert_articles(articles)
        update_refresh_log(ticker)
    else:
        # All sources failed — keep existing cached articles
        logger.warning(f"All news sources failed for {ticker}, keeping cached data")

    return len(articles), errors


async def refresh_all_stocks() -> tuple[dict[str, int], list[str]]:
    tickers = get_all_watched_tickers()
    if not tickers:
        return {}, []

    all_errors: list[str] = []
    results = {}
    for ticker in tickers:
        count, errors = await refresh_stock(ticker)
        results[ticker] = count
        all_errors.extend(errors)

    purge_old_articles()
    return results, all_errors


async def refresh_user_stocks(
    username: str,
    stale_minutes: int | None = None,
) -> tuple[dict[str, int], list[str], int]:
    """Refresh news for a user's watchlist (per-ticker Polygon.io calls).

    Polygon.io's news API only supports one ticker per request, so we make
    individual calls per ticker (rate-limited to 5 req/min).

    If stale_minutes is set, only tickers whose last news refresh is older
    than that threshold (or never refreshed) are fetched.

    Returns (per_ticker_counts, errors, skipped_count).
    """
    if stale_minutes is not None:
        tickers = get_stale_tickers(username, stale_minutes, log_table="refresh_log")
        total_tickers = len(get_user_tickers(username))
        skipped = total_tickers - len(tickers)
    else:
        tickers = get_user_tickers(username)
        skipped = 0

    if not tickers:
        return {}, [], skipped

    all_errors: list[str] = []
    results = {}
    for ticker in tickers:
        count, errors = await refresh_stock(ticker)
        results[ticker] = count
        all_errors.extend(errors)
    return results, all_errors, skipped


def get_digest(ticker: str | None = None, hours: int = 24) -> list[dict]:
    if ticker:
        return get_articles(ticker, hours)
    return get_all_recent_articles(hours)
