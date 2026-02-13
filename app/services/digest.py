"""Portfolio digest generation using OpenAI."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from openai import AsyncOpenAI

from app.config import OPENAI_API_KEY
from app.db import (
    get_user_tickers,
    get_articles,
    get_cached_digest,
    save_digest,
)

logger = logging.getLogger(__name__)

MODEL = "gpt-4o-mini"

SYSTEM_PROMPT = """\
You are a concise financial news analyst. The user will give you a list of recent
news articles for stocks in their portfolio.

Your job: produce a **Portfolio Digest** — a scannable briefing. Follow these rules:

1. One bullet per ticker that has significant news.
2. If a ticker has multiple articles, synthesize them into that single bullet.
3. Lead each bullet with an emoji:
   📈 for positive / bullish news
   📉 for negative / bearish news
   ⚖️ for neutral or mixed
4. Mention the ticker symbol in **bold** for each bullet.
5. At the end, combine ALL tickers with no significant news
   into a single line (e.g. "⚖️ **XYZ, ABC, DEF**: No significant news.").
6. Do NOT repeat article titles verbatim — synthesize and summarize.
7. For EVERY bullet that has news, you MUST include at least one markdown link
   to the most relevant source article: [short phrase](url).
   Use the exact URLs provided in the data — NEVER invent or modify URLs.
   Keep link text to 2–4 words.
"""


def _build_context(
    articles: list[dict],
    tickers: list[str],
) -> str:
    """Build the user-message context from articles."""
    lines = [f"Portfolio tickers: {', '.join(tickers)}", ""]

    if articles:
        lines.append(f"--- Recent News ({len(articles)} articles, last 3 days) ---")
        for a in articles:
            sentiment = a.get("sentiment") or "unknown"
            reasoning = a.get("sentiment_reasoning") or ""
            summary_snippet = (a.get("summary") or "")[:200]
            ts = (a.get("published_at") or "")[:16]
            url = a.get("url") or ""
            lines.append(
                f"[{a['ticker']}] ({sentiment}) {a['title']}"
                + (f" | {reasoning}" if reasoning else "")
                + (f" | {summary_snippet}" if summary_snippet else "")
                + (f" | {ts}" if ts else "")
                + (f" | url: {url}" if url else "")
            )
        lines.append("")
    else:
        lines.append("No recent news articles found.")

    return "\n".join(lines)


async def generate_digest(username: str) -> dict:
    """Generate a portfolio digest for the user and cache it.

    Returns a dict with keys: digest_text, generated_at, article_count, model.
    """
    if not OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY is not configured")

    tickers = get_user_tickers(username)
    if not tickers:
        return {
            "digest_text": "Your watchlist is empty. Add some stocks to generate a digest.",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "article_count": 0,
            "model": MODEL,
        }

    # Gather articles (last 3 days)
    all_articles: list[dict] = []
    for t in tickers:
        all_articles.extend(get_articles(t, hours=72))
    # Sort by published_at descending
    all_articles.sort(key=lambda a: a.get("published_at") or "", reverse=True)

    context = _build_context(all_articles, tickers)

    logger.info(
        f"Generating digest for {username}: {len(all_articles)} articles, "
        f"~{len(context)} chars context"
    )

    client = AsyncOpenAI(api_key=OPENAI_API_KEY)
    response = await client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": context},
        ],
        max_tokens=1200,
        temperature=0.3,
    )

    digest_text = response.choices[0].message.content or "No digest generated."
    usage = response.usage
    logger.info(
        f"Digest generated for {username}: {usage.prompt_tokens} prompt + "
        f"{usage.completion_tokens} completion tokens"
    )

    # Cache it
    save_digest(username, digest_text, len(all_articles), MODEL)

    return {
        "digest_text": digest_text,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "article_count": len(all_articles),
        "model": MODEL,
    }

