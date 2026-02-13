import httpx
import json
from datetime import datetime, timedelta, timezone
from app.config import MASSIVE_API_KEY, MASSIVE_BASE_URL
from app.sources.polygon_rate_limiter import limiter
import logging

logger = logging.getLogger(__name__)


async def fetch_dividends(ticker: str) -> list[dict]:
    """Fetch upcoming and recent dividends from Polygon.io. Raises on failure."""
    if not MASSIVE_API_KEY or MASSIVE_API_KEY == "your_api_key_here":
        logger.warning("API key not configured, skipping dividend fetch")
        return []

    url = f"{MASSIVE_BASE_URL}/v3/reference/dividends"
    params = {
        "ticker": ticker,
        "limit": 20,
        "order": "desc",
        "sort": "ex_dividend_date",
        "apiKey": MASSIVE_API_KEY,
    }

    await limiter.acquire()
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(url, params=params)
        if resp.status_code == 429:
            raise Exception(f"Polygon.io rate limited (dividends for {ticker})")
        resp.raise_for_status()
        data = resp.json()

    events = []
    for item in data.get("results", []):
        ex_date = item.get("ex_dividend_date")
        pay_date = item.get("pay_date")
        cash_amount = item.get("cash_amount")
        frequency_name = {
            1: "Annual", 2: "Bi-Annual", 4: "Quarterly",
            12: "Monthly", 0: "One-Time",
        }.get(item.get("frequency", 0), "")

        description = f"${cash_amount:.4f}/share" if cash_amount else ""
        if frequency_name:
            description += f" ({frequency_name})"

        metadata = json.dumps({
            "cash_amount": cash_amount,
            "currency": item.get("currency", "USD"),
            "declaration_date": item.get("declaration_date"),
            "record_date": item.get("record_date"),
            "pay_date": pay_date,
            "frequency": item.get("frequency"),
            "dividend_type": item.get("dividend_type"),
        })

        if ex_date:
            events.append({
                "ticker": ticker,
                "event_type": "dividend_ex",
                "event_date": ex_date,
                "title": f"{ticker} Ex-Dividend Date",
                "description": description,
                "metadata": metadata,
            })

        if pay_date:
            events.append({
                "ticker": ticker,
                "event_type": "dividend_pay",
                "event_date": pay_date,
                "title": f"{ticker} Dividend Payment",
                "description": description,
                "metadata": metadata,
            })

    return events


async def fetch_splits(ticker: str) -> list[dict]:
    """Fetch stock splits from Polygon.io. Raises on failure."""
    if not MASSIVE_API_KEY or MASSIVE_API_KEY == "your_api_key_here":
        logger.warning("API key not configured, skipping splits fetch")
        return []

    url = f"{MASSIVE_BASE_URL}/v3/reference/splits"
    params = {
        "ticker": ticker,
        "limit": 20,
        "order": "desc",
        "sort": "execution_date",
        "apiKey": MASSIVE_API_KEY,
    }

    await limiter.acquire()
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(url, params=params)
        if resp.status_code == 429:
            raise Exception(f"Polygon.io rate limited (splits for {ticker})")
        resp.raise_for_status()
        data = resp.json()

    events = []
    for item in data.get("results", []):
        exec_date = item.get("execution_date")
        split_from = item.get("split_from", 1)
        split_to = item.get("split_to", 1)

        description = f"{split_to}-for-{split_from} split"
        metadata = json.dumps({
            "split_from": split_from,
            "split_to": split_to,
        })

        if exec_date:
            events.append({
                "ticker": ticker,
                "event_type": "split",
                "event_date": exec_date,
                "title": f"{ticker} Stock Split ({split_to}:{split_from})",
                "description": description,
                "metadata": metadata,
            })

    return events


async def fetch_ticker_events(ticker: str) -> tuple[list[dict], list[str]]:
    """Fetch all event types for a ticker.
    
    Returns (events, errors). Partial success is possible —
    e.g. dividends succeed but splits are rate-limited.
    """
    all_events = []
    errors = []

    try:
        dividends = await fetch_dividends(ticker)
        all_events.extend(dividends)
    except Exception as e:
        errors.append(str(e))

    try:
        splits = await fetch_splits(ticker)
        all_events.extend(splits)
    except Exception as e:
        errors.append(str(e))

    return all_events, errors

