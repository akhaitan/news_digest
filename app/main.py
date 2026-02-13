import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Form, Request, Body
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

import asyncio
import json
from datetime import datetime, timezone
from app.config import DEFAULT_STOCKS, REFRESH_HOUR
from app.db import (
    init_db, get_user_stock_summary, get_articles, get_last_refresh,
    add_user_ticker, remove_user_ticker, get_user_tickers, get_stock_name,
    upsert_known_stocks, upsert_stock,
    mark_article_read, mark_article_unread, get_read_article_ids,
    get_user_refresh_timestamps, get_stale_tickers,
    get_cached_digest,
)
import app.stock_cache as stock_cache
from app.services.news import refresh_all_stocks, refresh_stock, refresh_user_stocks
from app.sources.polygon_tickers import fetch_all_us_tickers
from app.services.digest import generate_digest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    scheduler.add_job(
        refresh_all_stocks,
        trigger=CronTrigger(hour=REFRESH_HOUR),
        id="nightly_refresh",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(f"Scheduler started — nightly refresh at {REFRESH_HOUR}:00")

    # Load known stocks into in-memory cache
    stock_cache.load_cache_from_db()
    count = stock_cache.get_count()
    if count < 100:
        logger.info(f"Known stocks cache has {count} entries — fetching US tickers from Polygon.io in background...")
        asyncio.create_task(_populate_known_stocks())
    else:
        logger.info(f"Known stocks cache has {count} entries — skipping fetch")

    yield
    scheduler.shutdown()


async def _populate_known_stocks():
    """Background task to populate known stocks from Polygon.io."""
    try:
        tickers = await fetch_all_us_tickers()
        if not tickers:
            # Fallback: seed with DEFAULT_STOCKS if API fails
            fallback = [{"ticker": t, "name": n, "exchange": ""} for t, n in DEFAULT_STOCKS.items()]
            upsert_known_stocks(fallback)
            stock_cache.update_cache(fallback)
            logger.warning(f"API fetch failed, seeded {len(fallback)} default stocks")
        else:
            # Reload cache from DB (polygon_tickers.py already called upsert_known_stocks per exchange)
            stock_cache.load_cache_from_db()
            logger.info(f"Finished populating {len(tickers)} known stocks, cache refreshed")
    except Exception as e:
        logger.error(f"Failed to populate known stocks: {e}")


app = FastAPI(title="Stock News Digest", lifespan=lifespan)
templates = Jinja2Templates(directory="app/templates")


@app.get("/", response_class=HTMLResponse)
async def landing(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@app.get("/api/stocks/search")
async def api_stock_search(q: str = "", username: str = ""):
    """Autocomplete endpoint — search known stocks by ticker or company name.
    
    If username is provided, results include an 'in_watchlist' flag.
    """
    results = stock_cache.search(q.strip(), limit=15)
    if username:
        user_tickers = set(get_user_tickers(username))
        for r in results:
            r["in_watchlist"] = r["ticker"] in user_tickers
    return JSONResponse(results)


@app.post("/api/stocks/refresh-list")
async def api_refresh_stock_list():
    """Manually re-fetch the known stocks list from Polygon.io."""
    tickers = await fetch_all_us_tickers()
    if tickers:
        upsert_known_stocks(tickers)
        stock_cache.load_cache_from_db()
        return JSONResponse({"status": "ok", "count": len(tickers)})
    return JSONResponse({"status": "error", "message": "No tickers fetched"}, status_code=500)


def _format_ago(minutes: float) -> str:
    """Human-readable time-ago string."""
    if minutes < 1:
        return "just now"
    if minutes < 60:
        return f"{int(minutes)}m ago"
    hours = minutes / 60
    if hours < 24:
        return f"{int(hours)}h ago"
    return f"{int(hours / 24)}d ago"


@app.get("/{username}", response_class=HTMLResponse)
async def dashboard(request: Request, username: str):
    stocks = get_user_stock_summary(username, hours=72)
    tickers = get_user_tickers(username)
    all_articles = []
    for t in tickers:
        all_articles.extend(get_articles(t, hours=72))
    # Sort by recency (newest first)
    read_ids = get_read_article_ids(username)
    all_articles.sort(key=lambda a: a.get("published_at") or "", reverse=True)

    # Compute per-stock freshness indicators
    refresh_ts = get_user_refresh_timestamps(username)
    now = datetime.now(timezone.utc)
    for stock in stocks:
        ts = refresh_ts.get(stock["ticker"], {})
        nr = ts.get("news_refresh")

        if nr:
            diff = (now - datetime.fromisoformat(nr)).total_seconds() / 60
            stock["news_freshness"] = "green" if diff < 1440 else ("yellow" if diff < 4320 else "red")
            stock["news_ago"] = _format_ago(diff)
        else:
            stock["news_freshness"] = "gray"
            stock["news_ago"] = "Never"

    # Load cached digest (if any)
    digest = get_cached_digest(username)

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "username": username,
            "stocks": stocks,
            "articles": all_articles,
            "read_ids": read_ids,
            "digest": digest,
        },
    )


@app.get("/{username}/stock/{ticker}", response_class=HTMLResponse)
async def stock_detail(request: Request, username: str, ticker: str):
    ticker = ticker.upper()
    user_tickers = get_user_tickers(username)
    if ticker not in user_tickers:
        return HTMLResponse("Ticker not in your watchlist", status_code=404)

    articles = get_articles(ticker, hours=72)
    last_refresh = get_last_refresh(ticker)
    name = get_stock_name(ticker) or ticker

    return templates.TemplateResponse(
        "stock.html",
        {
            "request": request,
            "username": username,
            "ticker": ticker,
            "name": name,
            "articles": articles,
            "last_refresh": last_refresh,
        },
    )


@app.post("/{username}/add")
async def add_ticker(username: str, ticker: str = Form(...)):
    raw_input = ticker.strip()
    # Resolve input to a known ticker (handles both ticker symbols and company names)
    resolved = stock_cache.resolve_ticker(raw_input)
    if not resolved:
        # User entered something we don't recognize
        return RedirectResponse(f"/{username}?error=unknown_ticker&q={raw_input}", status_code=303)

    # Use the resolved ticker and update the stocks table with the proper name
    known_name = stock_cache.get_name(resolved)
    if known_name:
        upsert_stock(resolved, known_name)
    add_user_ticker(username, resolved)
    # Auto-fetch news if no cached articles for this ticker
    if not get_articles(resolved, hours=72):
        _count, _errors = await refresh_stock(resolved)
    # Auto-regenerate portfolio digest
    try:
        await generate_digest(username)
    except Exception as e:
        logger.warning(f"Auto-digest generation failed: {e}")
    return RedirectResponse(f"/{username}", status_code=303)


@app.post("/api/{username}/add-multiple")
async def add_multiple_tickers(username: str, tickers: list[str] = Body(...)):
    """Add multiple tickers at once. Expects a JSON body: ["AAPL", "Microsoft", ...]
    
    Each entry can be a ticker symbol or company name — they're resolved against the
    known stocks database. Returns per-line results so the UI can show what matched.
    """
    line_results = []
    added = []
    existing_tickers = set(get_user_tickers(username))

    for raw in tickers:
        raw = raw.strip()
        if not raw:
            continue
        resolved = stock_cache.resolve_ticker(raw)
        if not resolved:
            line_results.append({"input": raw, "status": "not_found", "ticker": None, "name": None})
            continue

        known_name = stock_cache.get_name(resolved) or resolved

        if resolved in existing_tickers:
            line_results.append({"input": raw, "status": "already_exists", "ticker": resolved, "name": known_name})
            continue

        # Update stocks table with proper name
        upsert_stock(resolved, known_name)
        add_user_ticker(username, resolved)
        added.append(resolved)
        existing_tickers.add(resolved)
        line_results.append({"input": raw, "status": "added", "ticker": resolved, "name": known_name})

    # Fetch news for newly added tickers
    async def fetch_if_needed(t):
        if not get_articles(t, hours=72):
            _count, _errors = await refresh_stock(t)

    if added:
        await asyncio.gather(*[fetch_if_needed(t) for t in added])

    return JSONResponse({
        "status": "ok",
        "results": line_results,
        "added_count": len(added),
    })


@app.post("/{username}/remove/{ticker}")
async def remove_ticker(username: str, ticker: str):
    ticker = ticker.upper()
    remove_user_ticker(username, ticker)
    return RedirectResponse(f"/{username}", status_code=303)



@app.post("/api/articles/{article_id}/read")
async def api_mark_read(article_id: int, username: str = Form(...)):
    mark_article_read(username, article_id)
    return JSONResponse({"status": "ok", "article_id": article_id, "is_read": True})


@app.post("/api/articles/{article_id}/unread")
async def api_mark_unread(article_id: int, username: str = Form(...)):
    mark_article_unread(username, article_id)
    return JSONResponse({"status": "ok", "article_id": article_id, "is_read": False})


@app.get("/api/digest/{username}")
async def api_get_digest(username: str):
    """Get the cached portfolio digest for a user."""
    digest = get_cached_digest(username)
    if digest:
        return JSONResponse(digest)
    return JSONResponse({"digest_text": None, "generated_at": None})


@app.post("/api/digest/{username}/generate")
async def api_generate_digest(username: str):
    """Generate a fresh portfolio digest using OpenAI."""
    try:
        result = await generate_digest(username)
        return JSONResponse({"status": "ok", **result})
    except ValueError as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=400)
    except Exception as e:
        logger.error(f"Digest generation failed for {username}: {e}")
        return JSONResponse(
            {"status": "error", "message": f"Failed to generate digest: {str(e)[:200]}"},
            status_code=500,
        )


@app.post("/api/refresh/{username}")
async def api_refresh_user(username: str):
    results, errors, skipped = await refresh_user_stocks(username, stale_minutes=1440)
    total = sum(results.values())
    response = {
        "status": "partial" if errors else "ok",
        "total_articles": total,
        "per_ticker": results,
        "skipped": skipped,
    }
    if errors:
        response["errors"] = errors
    return JSONResponse(response)


# --- Streaming refresh endpoints (SSE) ---
# NOTE: These must be registered BEFORE the /{ticker} catch-all route
# so that "/stream" is matched literally instead of as a ticker name.

@app.post("/api/refresh/{username}/stream")
async def api_refresh_user_stream(username: str):
    """Stream progress updates as news is refreshed for each ticker.

    Uses a background task + queue pattern with heartbeats so the SSE
    connection stays alive during long rate-limiter waits.
    """
    async def generate():
        all_tickers = get_user_tickers(username)
        stale = get_stale_tickers(username, stale_minutes=1440, log_table="refresh_log")
        stale_set = set(stale)
        skipped = [t for t in all_tickers if t not in stale_set]

        yield f"data: {json.dumps({'type': 'init', 'queue': stale, 'skipped': skipped})}\n\n"

        if not stale:
            yield f"data: {json.dumps({'type': 'complete', 'errors': []})}\n\n"
            return

        queue: asyncio.Queue = asyncio.Queue()

        async def worker():
            all_errors: list[str] = []
            for ticker in stale:
                await queue.put(json.dumps({'type': 'processing', 'ticker': ticker}))
                count, errors = await refresh_stock(ticker)
                all_errors.extend(errors)
                await queue.put(json.dumps({'type': 'ticker_done', 'ticker': ticker, 'count': count, 'errors': errors}))
            await queue.put(json.dumps({'type': 'complete', 'errors': all_errors}))

        task = asyncio.create_task(worker())
        while not task.done() or not queue.empty():
            try:
                msg = await asyncio.wait_for(queue.get(), timeout=5.0)
                yield f"data: {msg}\n\n"
                if '"type": "complete"' in msg or '"type":"complete"' in msg:
                    break
            except asyncio.TimeoutError:
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"
        await task  # Ensure any exceptions propagate

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/refresh/{username}/{ticker}")
async def api_refresh_ticker(username: str, ticker: str):
    ticker = ticker.upper()
    user_tickers = get_user_tickers(username)
    if ticker not in user_tickers:
        return JSONResponse({"error": "Ticker not in watchlist"}, status_code=404)

    count, errors = await refresh_stock(ticker)
    response = {
        "status": "partial" if errors else "ok",
        "ticker": ticker,
        "articles_fetched": count,
    }
    if errors:
        response["errors"] = errors
    return JSONResponse(response)


