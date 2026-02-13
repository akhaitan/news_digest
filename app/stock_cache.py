"""In-memory cache for known stocks — eliminates DB round-trips for autocomplete.

The cache is loaded once on startup and updated whenever stocks are upserted.
All search/resolve/lookup operations run against this in-memory list,
making autocomplete instant regardless of DB latency.
"""

from __future__ import annotations

import logging
import threading

logger = logging.getLogger(__name__)

# ── Cache storage ─────────────────────────────────────────────────────
_lock = threading.Lock()
_stocks: list[dict] = []  # [{"ticker": "AAPL", "name": "Apple Inc", "exchange": "XNAS"}, ...]
_ticker_index: dict[str, dict] = {}  # "AAPL" -> {"ticker": "AAPL", "name": "Apple Inc", "exchange": "XNAS"}
_name_index: dict[str, str] = {}  # "apple inc" -> "AAPL"


def load_cache_from_db():
    """Load all known stocks from the database into memory."""
    from app.db import get_db, _fetchall

    with get_db() as conn:
        rows = _fetchall(conn, "SELECT ticker, name, exchange FROM known_stocks ORDER BY ticker")

    with _lock:
        _stocks.clear()
        _ticker_index.clear()
        _name_index.clear()
        for r in rows:
            entry = {"ticker": r["ticker"], "name": r["name"], "exchange": r.get("exchange") or ""}
            _stocks.append(entry)
            _ticker_index[r["ticker"].upper()] = entry
            _name_index[r["name"].lower()] = r["ticker"]

    logger.info(f"Stock cache loaded: {len(_stocks)} entries")


def update_cache(stocks: list[dict]):
    """Add/update entries in the cache (called after upsert_known_stocks)."""
    with _lock:
        for s in stocks:
            ticker_upper = s["ticker"].upper()
            entry = {"ticker": s["ticker"], "name": s["name"], "exchange": s.get("exchange") or ""}

            if ticker_upper in _ticker_index:
                # Update existing entry in-place
                old = _ticker_index[ticker_upper]
                # Remove old name index if name changed
                if old["name"].lower() in _name_index:
                    del _name_index[old["name"].lower()]
                old.update(entry)
            else:
                _stocks.append(entry)
                _ticker_index[ticker_upper] = entry

            _name_index[s["name"].lower()] = s["ticker"]


def get_count() -> int:
    """Return number of cached stocks."""
    with _lock:
        return len(_stocks)


def search(query: str, limit: int = 15) -> list[dict]:
    """Search cached stocks by ticker or company name (case-insensitive).

    Results are ordered: exact ticker match → ticker prefix → name contains.
    """
    if not query or len(query) < 1:
        return []

    q_upper = query.upper()
    q_lower = query.lower()

    exact = []
    prefix = []
    contains = []

    with _lock:
        for s in _stocks:
            t_upper = s["ticker"].upper()
            n_lower = s["name"].lower()

            if t_upper == q_upper:
                exact.append(s)
            elif t_upper.startswith(q_upper):
                prefix.append(s)
            elif q_lower in n_lower:
                contains.append(s)

    # Sort each group alphabetically by ticker
    prefix.sort(key=lambda s: s["ticker"])
    contains.sort(key=lambda s: s["ticker"])

    results = exact + prefix + contains
    return [{"ticker": s["ticker"], "name": s["name"], "exchange": s["exchange"]} for s in results[:limit]]


def resolve_ticker(input_text: str) -> str | None:
    """Resolve user input to a valid ticker. Handles ticker or company name."""
    input_text = input_text.strip()
    if not input_text:
        return None

    with _lock:
        # Try exact ticker match
        ticker_upper = input_text.upper()
        if ticker_upper in _ticker_index:
            return _ticker_index[ticker_upper]["ticker"]

        # Try exact name match
        name_lower = input_text.lower()
        if name_lower in _name_index:
            return _name_index[name_lower]

    return None


def get_name(ticker: str) -> str | None:
    """Get company name from cache."""
    with _lock:
        entry = _ticker_index.get(ticker.upper())
        return entry["name"] if entry else None

