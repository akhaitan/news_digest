from __future__ import annotations

import logging
from app.sources.polygon_events import fetch_ticker_events
from app.db import (
    upsert_events, delete_events_for_ticker, get_events_for_tickers,
    get_user_tickers, purge_old_events, get_all_watched_tickers,
    update_events_refresh_log, get_stale_tickers,
)

logger = logging.getLogger(__name__)


async def refresh_events_for_ticker(ticker: str) -> tuple[int, list[str]]:
    """Fetch and cache all events for a single ticker.
    
    Returns (event_count, errors).
    Only replaces cached events if at least partial data was fetched successfully.
    """
    events, errors = await fetch_ticker_events(ticker)

    if events:
        # Got some data — safe to replace cache
        delete_events_for_ticker(ticker)
        upsert_events(events)
        update_events_refresh_log(ticker)
        logger.info(f"Cached {len(events)} events for {ticker}")
    elif not errors:
        # No events and no errors — API returned empty, clear old cache
        delete_events_for_ticker(ticker)
        update_events_refresh_log(ticker)
        logger.info(f"No events found for {ticker}, cleared cache")
    else:
        # All fetches failed — keep existing cached events
        logger.warning(f"Events fetch failed for {ticker}, keeping cached data")

    return len(events), errors


async def refresh_events_for_user(
    username: str,
    stale_minutes: int | None = None,
) -> tuple[dict[str, int], list[str], int]:
    """Refresh events for a user's watchlist.

    If stale_minutes is set, only tickers whose last event refresh is older
    than that threshold (or never refreshed) are fetched.

    Returns (per_ticker_counts, errors, skipped_count).
    """
    if stale_minutes is not None:
        tickers = get_stale_tickers(username, stale_minutes, log_table="events_refresh_log")
        total_tickers = len(get_user_tickers(username))
        skipped = total_tickers - len(tickers)
    else:
        tickers = get_user_tickers(username)
        skipped = 0
    results = {}
    all_errors: list[str] = []
    for ticker in tickers:
        count, errors = await refresh_events_for_ticker(ticker)
        results[ticker] = count
        all_errors.extend(errors)
    return results, all_errors, skipped


async def refresh_all_events() -> tuple[dict[str, int], list[str]]:
    """Refresh events for all watched tickers across all users."""
    tickers = get_all_watched_tickers()
    results = {}
    all_errors: list[str] = []
    for ticker in tickers:
        count, errors = await refresh_events_for_ticker(ticker)
        results[ticker] = count
        all_errors.extend(errors)
    purge_old_events()
    return results, all_errors


def get_user_events(username: str, start_date: str | None = None, end_date: str | None = None) -> list[dict]:
    """Get all cached events for a user's watchlist tickers."""
    tickers = get_user_tickers(username)
    return get_events_for_tickers(tickers, start_date, end_date)

