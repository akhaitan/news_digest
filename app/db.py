from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager
from app.config import DATABASE_URL, DB_PATH, DEFAULT_STOCKS, ARTICLE_RETENTION_DAYS

logger = logging.getLogger(__name__)

# ── Database backend detection ────────────────────────────────────────
USE_PG = bool(DATABASE_URL)

if USE_PG:
    import psycopg2
    import psycopg2.extras


@contextmanager
def get_db():
    """Return a database connection.

    Yields a (conn, is_pg) tuple if PostgreSQL, or a plain conn for SQLite.
    Consumers should use the module-level helper functions rather than
    calling this directly.
    """
    if USE_PG:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    else:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()


def _execute(conn, query: str, params=None):
    """Execute a query and return a cursor with results."""
    if params is None:
        params = ()
    if USE_PG:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(query, params)
        return cur
    else:
        # SQLite: conn.execute() returns a Cursor directly
        return conn.execute(query, params)


def _fetchall(conn, query: str, params=None) -> list[dict]:
    """Execute and fetch all rows as dicts."""
    cur = _execute(conn, query, params)
    rows = cur.fetchall()
    if USE_PG:
        return [dict(r) for r in rows]
    return [dict(r) for r in rows]


def _fetchone(conn, query: str, params=None) -> dict | None:
    """Execute and fetch one row as dict."""
    cur = _execute(conn, query, params)
    row = cur.fetchone()
    if row is None:
        return None
    return dict(row)


# ── Placeholder helper ────────────────────────────────────────────────
# SQLite uses ?, PostgreSQL uses %s
P = "%s" if USE_PG else "?"


def _ph(n: int) -> str:
    """Return n comma-separated placeholders."""
    return ",".join([P] * n)


# ── Schema initialisation ────────────────────────────────────────────

def init_db():
    with get_db() as conn:
        if USE_PG:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stocks (
                    ticker TEXT PRIMARY KEY,
                    name TEXT NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS articles (
                    id SERIAL PRIMARY KEY,
                    ticker TEXT NOT NULL REFERENCES stocks(ticker),
                    title TEXT NOT NULL,
                    summary TEXT,
                    url TEXT NOT NULL UNIQUE,
                    source TEXT,
                    published_at TEXT,
                    sentiment TEXT,
                    sentiment_reasoning TEXT,
                    created_at TEXT NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC')::TEXT
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_articles_ticker ON articles(ticker)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_at)")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS refresh_log (
                    ticker TEXT PRIMARY KEY,
                    last_refresh TEXT NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_watchlists (
                    username TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    added_at TEXT NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC')::TEXT,
                    PRIMARY KEY (username, ticker)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id SERIAL PRIMARY KEY,
                    ticker TEXT NOT NULL REFERENCES stocks(ticker),
                    event_type TEXT NOT NULL,
                    event_date TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT,
                    metadata TEXT,
                    created_at TEXT NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC')::TEXT,
                    UNIQUE(ticker, event_type, event_date)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_events_ticker ON events(ticker)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_events_date ON events(event_date)")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS known_stocks (
                    ticker TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    exchange TEXT,
                    updated_at TEXT NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC')::TEXT
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_known_stocks_name ON known_stocks(LOWER(name))")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS read_articles (
                    username TEXT NOT NULL,
                    article_id INTEGER NOT NULL REFERENCES articles(id),
                    read_at TEXT NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC')::TEXT,
                    PRIMARY KEY (username, article_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS events_refresh_log (
                    ticker TEXT PRIMARY KEY,
                    last_refresh TEXT NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS portfolio_digests (
                    username TEXT PRIMARY KEY,
                    digest_text TEXT NOT NULL,
                    generated_at TEXT NOT NULL,
                    article_count INTEGER DEFAULT 0,
                    model TEXT
                )
            """)
            cur.close()
        else:
            # SQLite schema
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS stocks (
                    ticker TEXT PRIMARY KEY,
                    name TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    title TEXT NOT NULL,
                    summary TEXT,
                    url TEXT NOT NULL UNIQUE,
                    source TEXT,
                    published_at TEXT,
                    sentiment TEXT,
                    sentiment_reasoning TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    FOREIGN KEY (ticker) REFERENCES stocks(ticker)
                );

                CREATE INDEX IF NOT EXISTS idx_articles_ticker ON articles(ticker);
                CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_at);

                CREATE TABLE IF NOT EXISTS refresh_log (
                    ticker TEXT PRIMARY KEY,
                    last_refresh TEXT NOT NULL,
                    FOREIGN KEY (ticker) REFERENCES stocks(ticker)
                );

                CREATE TABLE IF NOT EXISTS user_watchlists (
                    username TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    added_at TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (username, ticker)
                );

                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_date TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT,
                    metadata TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    FOREIGN KEY (ticker) REFERENCES stocks(ticker),
                    UNIQUE(ticker, event_type, event_date)
                );

                CREATE INDEX IF NOT EXISTS idx_events_ticker ON events(ticker);
                CREATE INDEX IF NOT EXISTS idx_events_date ON events(event_date);

                CREATE TABLE IF NOT EXISTS known_stocks (
                    ticker TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    exchange TEXT,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                );

                CREATE INDEX IF NOT EXISTS idx_known_stocks_name ON known_stocks(name COLLATE NOCASE);

                CREATE TABLE IF NOT EXISTS read_articles (
                    username TEXT NOT NULL,
                    article_id INTEGER NOT NULL,
                    read_at TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (username, article_id),
                    FOREIGN KEY (article_id) REFERENCES articles(id)
                );

                CREATE TABLE IF NOT EXISTS events_refresh_log (
                    ticker TEXT PRIMARY KEY,
                    last_refresh TEXT NOT NULL,
                    FOREIGN KEY (ticker) REFERENCES stocks(ticker)
                );

                CREATE TABLE IF NOT EXISTS portfolio_digests (
                    username TEXT PRIMARY KEY,
                    digest_text TEXT NOT NULL,
                    generated_at TEXT NOT NULL,
                    article_count INTEGER DEFAULT 0,
                    model TEXT
                );
            """)

        for ticker, name in DEFAULT_STOCKS.items():
            _execute(conn,
                f"INSERT INTO stocks (ticker, name) VALUES ({P}, {P}) ON CONFLICT(ticker) DO NOTHING",
                (ticker, name),
            )


# ── Stock helpers ─────────────────────────────────────────────────────

def ensure_stock_exists(ticker: str):
    with get_db() as conn:
        _execute(conn,
            f"INSERT INTO stocks (ticker, name) VALUES ({P}, {P}) ON CONFLICT(ticker) DO NOTHING",
            (ticker, ticker),
        )


def upsert_stock(ticker: str, name: str):
    """Insert or update a stock's name."""
    with get_db() as conn:
        _execute(conn,
            f"""INSERT INTO stocks (ticker, name) VALUES ({P}, {P})
                ON CONFLICT(ticker) DO UPDATE SET name = EXCLUDED.name""",
            (ticker, name),
        )


# ── Articles ──────────────────────────────────────────────────────────

def delete_articles_for_ticker(ticker: str):
    with get_db() as conn:
        _execute(conn, f"DELETE FROM articles WHERE ticker = {P}", (ticker,))


def upsert_articles(articles: list[dict]):
    with get_db() as conn:
        for a in articles:
            _execute(conn,
                f"""INSERT INTO articles
                    (ticker, title, summary, url, source, published_at, sentiment, sentiment_reasoning)
                    VALUES ({_ph(8)})
                    ON CONFLICT(url) DO NOTHING""",
                (
                    a["ticker"],
                    a["title"],
                    a.get("summary"),
                    a["url"],
                    a.get("source"),
                    a.get("published_at"),
                    a.get("sentiment"),
                    a.get("sentiment_reasoning"),
                ),
            )


def get_articles(ticker: str, hours: int = 24) -> list[dict]:
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    with get_db() as conn:
        return _fetchall(conn,
            f"""SELECT * FROM articles
                WHERE ticker = {P} AND published_at >= {P}
                ORDER BY published_at DESC""",
            (ticker, since),
        )


def get_all_recent_articles(hours: int = 24) -> list[dict]:
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    with get_db() as conn:
        return _fetchall(conn,
            f"""SELECT * FROM articles
                WHERE published_at >= {P}
                ORDER BY published_at DESC""",
            (since,),
        )


def get_stock_summary(hours: int = 24) -> list[dict]:
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    with get_db() as conn:
        return _fetchall(conn,
            f"""SELECT s.ticker, s.name,
                       COUNT(a.id) as article_count,
                       SUM(CASE WHEN a.sentiment = 'positive' THEN 1 ELSE 0 END) as positive,
                       SUM(CASE WHEN a.sentiment = 'negative' THEN 1 ELSE 0 END) as negative,
                       SUM(CASE WHEN a.sentiment = 'neutral' OR a.sentiment IS NULL THEN 1 ELSE 0 END) as neutral
                FROM stocks s
                LEFT JOIN articles a ON s.ticker = a.ticker AND a.published_at >= {P}
                GROUP BY s.ticker, s.name
                ORDER BY s.ticker""",
            (since,),
        )


def get_user_stock_summary(username: str, hours: int = 24) -> list[dict]:
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    with get_db() as conn:
        return _fetchall(conn,
            f"""SELECT s.ticker, s.name,
                       COUNT(a.id) as article_count,
                       SUM(CASE WHEN a.sentiment = 'positive' THEN 1 ELSE 0 END) as positive,
                       SUM(CASE WHEN a.sentiment = 'negative' THEN 1 ELSE 0 END) as negative,
                       SUM(CASE WHEN a.sentiment = 'neutral' OR a.sentiment IS NULL THEN 1 ELSE 0 END) as neutral
                FROM user_watchlists w
                JOIN stocks s ON w.ticker = s.ticker
                LEFT JOIN articles a ON s.ticker = a.ticker AND a.published_at >= {P}
                WHERE w.username = {P}
                GROUP BY s.ticker, s.name
                ORDER BY s.ticker""",
            (since, username),
        )


# ── User watchlists ──────────────────────────────────────────────────

def get_user_tickers(username: str) -> list[str]:
    with get_db() as conn:
        rows = _fetchall(conn,
            f"SELECT ticker FROM user_watchlists WHERE username = {P} ORDER BY ticker",
            (username,),
        )
        return [r["ticker"] for r in rows]


def add_user_ticker(username: str, ticker: str):
    ensure_stock_exists(ticker)
    with get_db() as conn:
        _execute(conn,
            f"INSERT INTO user_watchlists (username, ticker) VALUES ({P}, {P}) ON CONFLICT DO NOTHING",
            (username, ticker),
        )


def remove_user_ticker(username: str, ticker: str):
    with get_db() as conn:
        _execute(conn,
            f"DELETE FROM user_watchlists WHERE username = {P} AND ticker = {P}",
            (username, ticker),
        )


def get_all_watched_tickers() -> list[str]:
    with get_db() as conn:
        rows = _fetchall(conn,
            "SELECT DISTINCT ticker FROM user_watchlists ORDER BY ticker",
        )
        return [r["ticker"] for r in rows]


# ── Refresh log ───────────────────────────────────────────────────────

def get_last_refresh(ticker: str) -> str | None:
    with get_db() as conn:
        row = _fetchone(conn,
            f"SELECT last_refresh FROM refresh_log WHERE ticker = {P}", (ticker,),
        )
        return row["last_refresh"] if row else None


def update_refresh_log(ticker: str):
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        _execute(conn,
            f"""INSERT INTO refresh_log (ticker, last_refresh) VALUES ({P}, {P})
                ON CONFLICT(ticker) DO UPDATE SET last_refresh = EXCLUDED.last_refresh""",
            (ticker, now),
        )


def purge_old_articles():
    cutoff = (
        datetime.now(timezone.utc) - timedelta(days=ARTICLE_RETENTION_DAYS)
    ).isoformat()
    with get_db() as conn:
        _execute(conn, f"DELETE FROM articles WHERE published_at < {P}", (cutoff,))


def get_stock_name(ticker: str) -> str | None:
    with get_db() as conn:
        row = _fetchone(conn,
            f"SELECT name FROM stocks WHERE ticker = {P}", (ticker,),
        )
        return row["name"] if row else None


# ── Events ────────────────────────────────────────────────────────────

def upsert_events(events: list[dict]):
    with get_db() as conn:
        for e in events:
            if USE_PG:
                _execute(conn,
                    f"""INSERT INTO events
                        (ticker, event_type, event_date, title, description, metadata)
                        VALUES ({_ph(6)})
                        ON CONFLICT(ticker, event_type, event_date) DO UPDATE SET
                            title = EXCLUDED.title,
                            description = EXCLUDED.description,
                            metadata = EXCLUDED.metadata""",
                    (
                        e["ticker"], e["event_type"], e["event_date"],
                        e["title"], e.get("description"), e.get("metadata"),
                    ),
                )
            else:
                _execute(conn,
                    f"""INSERT OR REPLACE INTO events
                        (ticker, event_type, event_date, title, description, metadata)
                        VALUES ({_ph(6)})""",
                    (
                        e["ticker"], e["event_type"], e["event_date"],
                        e["title"], e.get("description"), e.get("metadata"),
                    ),
                )


def get_events_for_ticker(ticker: str) -> list[dict]:
    with get_db() as conn:
        return _fetchall(conn,
            f"SELECT * FROM events WHERE ticker = {P} ORDER BY event_date ASC",
            (ticker,),
        )


def get_events_for_tickers(tickers: list[str], start_date: str | None = None, end_date: str | None = None) -> list[dict]:
    if not tickers:
        return []
    placeholders = _ph(len(tickers))
    query = f"SELECT * FROM events WHERE ticker IN ({placeholders})"
    params: list = list(tickers)
    if start_date:
        query += f" AND event_date >= {P}"
        params.append(start_date)
    if end_date:
        query += f" AND event_date <= {P}"
        params.append(end_date)
    query += " ORDER BY event_date ASC"
    with get_db() as conn:
        return _fetchall(conn, query, tuple(params))


def delete_events_for_ticker(ticker: str):
    with get_db() as conn:
        _execute(conn, f"DELETE FROM events WHERE ticker = {P}", (ticker,))


def purge_old_events():
    cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%d")
    with get_db() as conn:
        _execute(conn, f"DELETE FROM events WHERE event_date < {P}", (cutoff,))


# ── Known stocks (autocomplete & normalization) ──────────────────────

def upsert_known_stocks(stocks: list[dict]):
    """Bulk insert/update known stocks."""
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        for s in stocks:
            _execute(conn,
                f"""INSERT INTO known_stocks (ticker, name, exchange, updated_at)
                    VALUES ({P}, {P}, {P}, {P})
                    ON CONFLICT(ticker) DO UPDATE SET
                        name = EXCLUDED.name,
                        exchange = EXCLUDED.exchange,
                        updated_at = EXCLUDED.updated_at""",
                (s["ticker"], s["name"], s.get("exchange"), now),
            )


def search_known_stocks(query: str, limit: int = 15) -> list[dict]:
    """Search known stocks by ticker or company name (case-insensitive).

    Results are ordered: exact ticker match → ticker prefix → name contains.
    """
    if not query or len(query) < 1:
        return []
    with get_db() as conn:
        if USE_PG:
            return _fetchall(conn,
                f"""SELECT ticker, name, exchange FROM (
                        SELECT ticker, name, exchange, 1 as priority FROM known_stocks
                        WHERE UPPER(ticker) = UPPER({P})
                        UNION ALL
                        SELECT ticker, name, exchange, 2 as priority FROM known_stocks
                        WHERE UPPER(ticker) LIKE UPPER({P}) AND UPPER(ticker) != UPPER({P})
                        UNION ALL
                        SELECT ticker, name, exchange, 3 as priority FROM known_stocks
                        WHERE UPPER(name) LIKE UPPER({P}) AND UPPER(ticker) NOT LIKE UPPER({P})
                    ) sub
                    ORDER BY priority, ticker
                    LIMIT {P}""",
                (query, f"{query}%", query, f"%{query}%", f"{query}%", limit),
            )
        else:
            return _fetchall(conn,
                f"""SELECT ticker, name, exchange FROM (
                        SELECT ticker, name, exchange, 1 as priority FROM known_stocks
                        WHERE ticker = {P} COLLATE NOCASE
                        UNION ALL
                        SELECT ticker, name, exchange, 2 as priority FROM known_stocks
                        WHERE ticker LIKE {P} COLLATE NOCASE AND ticker != {P} COLLATE NOCASE
                        UNION ALL
                        SELECT ticker, name, exchange, 3 as priority FROM known_stocks
                        WHERE name LIKE {P} COLLATE NOCASE AND ticker NOT LIKE {P} COLLATE NOCASE
                    )
                    ORDER BY priority, ticker
                    LIMIT {P}""",
                (query, f"{query}%", query, f"%{query}%", f"{query}%", limit),
            )


def resolve_ticker(input_text: str) -> str | None:
    """Resolve user input to a valid ticker. Handles ticker or company name."""
    input_text = input_text.strip()
    if not input_text:
        return None
    with get_db() as conn:
        if USE_PG:
            row = _fetchone(conn,
                f"SELECT ticker FROM known_stocks WHERE UPPER(ticker) = UPPER({P})",
                (input_text,),
            )
            if row:
                return row["ticker"]
            row = _fetchone(conn,
                f"SELECT ticker FROM known_stocks WHERE UPPER(name) = UPPER({P})",
                (input_text,),
            )
        else:
            row = _fetchone(conn,
                f"SELECT ticker FROM known_stocks WHERE ticker = {P} COLLATE NOCASE",
                (input_text,),
            )
            if row:
                return row["ticker"]
            row = _fetchone(conn,
                f"SELECT ticker FROM known_stocks WHERE name = {P} COLLATE NOCASE",
                (input_text,),
            )
        return row["ticker"] if row else None


def get_known_stock_count() -> int:
    with get_db() as conn:
        row = _fetchone(conn, "SELECT COUNT(*) as cnt FROM known_stocks")
        return row["cnt"] if row else 0


def get_known_stock_name(ticker: str) -> str | None:
    """Get company name from known_stocks table."""
    with get_db() as conn:
        if USE_PG:
            row = _fetchone(conn,
                f"SELECT name FROM known_stocks WHERE UPPER(ticker) = UPPER({P})",
                (ticker,),
            )
        else:
            row = _fetchone(conn,
                f"SELECT name FROM known_stocks WHERE ticker = {P} COLLATE NOCASE",
                (ticker,),
            )
        return row["name"] if row else None


# ── Read/unread article tracking ─────────────────────────────────────

def mark_article_read(username: str, article_id: int):
    """Mark an article as read for a user."""
    with get_db() as conn:
        _execute(conn,
            f"INSERT INTO read_articles (username, article_id) VALUES ({P}, {P}) ON CONFLICT DO NOTHING",
            (username, article_id),
        )


def mark_article_unread(username: str, article_id: int):
    """Mark an article as unread (remove read marker) for a user."""
    with get_db() as conn:
        _execute(conn,
            f"DELETE FROM read_articles WHERE username = {P} AND article_id = {P}",
            (username, article_id),
        )


def get_read_article_ids(username: str) -> set[int]:
    """Get all article IDs that a user has read."""
    with get_db() as conn:
        rows = _fetchall(conn,
            f"SELECT article_id FROM read_articles WHERE username = {P}",
            (username,),
        )
        return {r["article_id"] for r in rows}


# ── Events refresh tracking ──────────────────────────────────────────

def update_events_refresh_log(ticker: str):
    """Record that events for a ticker were just refreshed."""
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        _execute(conn,
            f"""INSERT INTO events_refresh_log (ticker, last_refresh) VALUES ({P}, {P})
                ON CONFLICT(ticker) DO UPDATE SET last_refresh = EXCLUDED.last_refresh""",
            (ticker, now),
        )


def get_user_refresh_timestamps(username: str) -> dict[str, dict]:
    """Get news and events refresh timestamps for all of a user's tickers."""
    tickers = get_user_tickers(username)
    if not tickers:
        return {}
    placeholders = _ph(len(tickers))
    result: dict[str, dict] = {t: {"news_refresh": None, "events_refresh": None} for t in tickers}
    with get_db() as conn:
        rows = _fetchall(conn,
            f"SELECT ticker, last_refresh FROM refresh_log WHERE ticker IN ({placeholders})",
            tuple(tickers),
        )
        for r in rows:
            result[r["ticker"]]["news_refresh"] = r["last_refresh"]
        rows = _fetchall(conn,
            f"SELECT ticker, last_refresh FROM events_refresh_log WHERE ticker IN ({placeholders})",
            tuple(tickers),
        )
        for r in rows:
            result[r["ticker"]]["events_refresh"] = r["last_refresh"]
    return result


def get_stale_tickers(username: str, stale_minutes: int, log_table: str = "refresh_log") -> list[str]:
    """Return user's tickers whose last refresh is older than stale_minutes (or never refreshed).

    Results are sorted most-stale first: never-refreshed tickers come first,
    then by oldest last_refresh ascending.
    """
    assert log_table in ("refresh_log", "events_refresh_log"), "invalid table"
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=stale_minutes)).isoformat()
    tickers = get_user_tickers(username)
    if not tickers:
        return []
    placeholders = _ph(len(tickers))
    with get_db() as conn:
        rows = _fetchall(conn,
            f"SELECT ticker, last_refresh FROM {log_table} WHERE ticker IN ({placeholders})",
            tuple(tickers),
        )
        refresh_map = {r["ticker"]: r["last_refresh"] for r in rows}

    stale = [t for t in tickers if refresh_map.get(t, "") <= cutoff]
    stale.sort(key=lambda t: refresh_map.get(t, ""))
    return stale


# ── Portfolio Digest ──────────────────────────────────────────────────

def get_cached_digest(username: str) -> dict | None:
    """Return the cached digest for a user, or None if no digest exists."""
    with get_db() as conn:
        row = _fetchone(conn,
            f"SELECT digest_text, generated_at, article_count, model FROM portfolio_digests WHERE username = {P}",
            (username,),
        )
        return row


def save_digest(username: str, digest_text: str, article_count: int, model: str):
    """Save or replace the cached digest for a user."""
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        _execute(conn,
            f"""INSERT INTO portfolio_digests (username, digest_text, generated_at, article_count, model)
                VALUES ({P}, {P}, {P}, {P}, {P})
                ON CONFLICT(username) DO UPDATE SET
                    digest_text = EXCLUDED.digest_text,
                    generated_at = EXCLUDED.generated_at,
                    article_count = EXCLUDED.article_count,
                    model = EXCLUDED.model""",
            (username, digest_text, now, article_count, model),
        )
