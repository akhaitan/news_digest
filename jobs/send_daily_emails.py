"""Cron job: generate portfolio digests and send email reports for all users.

Designed to run as a GitHub Action (7 AM / 7 PM EST) or standalone.
For each user who has an email set, it:
  1. Generates a fresh portfolio digest from cached articles (via OpenAI).
  2. Sends the digest email via Resend.

Usage:
    python jobs/send_daily_emails.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import datetime, timezone

import httpx

# Ensure the project root is on sys.path so `app.*` imports work
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import APP_BASE_URL, RESEND_API_KEY, RECIPIENT_EMAIL
from app.db import init_db, get_users_with_emails, get_user_tickers, get_articles
from app.services.digest import generate_digest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("send_daily_emails")


def _format_digest_html(raw_text: str) -> str:
    """Convert the markdown-ish digest text to inline-styled HTML for email."""
    import re

    lines = [l for l in raw_text.split("\n") if l.strip()]
    html_parts: list[str] = []

    for line in lines:
        # Convert markdown links [text](url) to <a> tags
        line = re.sub(
            r"\[([^\]]+)\]\((https?://[^\)]+)\)",
            r'<a href="\2" style="color: #4f46e5; text-decoration: underline;">\1</a>',
            line,
        )
        # Convert **bold** to <strong>
        line = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", line)
        # Strip leading bullet chars
        stripped = line.strip()
        if stripped and stripped[0] in "-•*":
            stripped = stripped[1:].strip()
        html_parts.append(f"<p style=\"margin: 6px 0; line-height: 1.7;\">{stripped}</p>")

    return "\n".join(html_parts)


def _build_email_body(username: str, digest_text: str) -> str:
    """Build the full email HTML body."""
    dashboard_url = f"{APP_BASE_URL}/{username}"
    digest_html = _format_digest_html(digest_text)

    return (
        '<div style="font-family: -apple-system, BlinkMacSystemFont, \'Segoe UI\', '
        "Roboto, Helvetica, Arial, sans-serif; max-width: 600px; margin: 0 auto;\">"
        f'<p style="font-size: 16px; color: #1e293b; margin-bottom: 16px;">Hi {username},</p>'
        '<h2 style="font-size: 20px; font-weight: 700; color: #1e293b; margin-bottom: 16px;">'
        "📊 Portfolio Digest</h2>"
        '<div style="color: #374151; line-height: 1.7; font-size: 14px;">'
        f"{digest_html}"
        "</div>"
        '<hr style="border: none; border-top: 1px solid #e5e7eb; margin: 24px 0;">'
        '<p style="font-size: 13px; color: #6b7280;">'
        "View the full dashboard: "
        f'<a href="{dashboard_url}" style="color: #4f46e5; text-decoration: underline;">'
        f"{dashboard_url}</a>"
        "</p>"
        "</div>"
    )


async def send_email(client: httpx.AsyncClient, to: list[str], subject: str, html: str) -> bool:
    """Send an email via Resend. Returns True on success."""
    try:
        resp = await client.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "from": "Stock Digest <digest@vibeyvibey.com>",
                "to": to,
                "subject": subject,
                "html": html,
            },
            timeout=15.0,
        )
        if resp.status_code in (200, 201):
            data = resp.json()
            logger.info(f"  Email sent to {to}: id={data.get('id')}")
            return True
        else:
            logger.error(f"  Resend API error ({resp.status_code}): {resp.text[:300]}")
            return False
    except Exception as e:
        logger.error(f"  Email send failed: {e}")
        return False


async def main():
    if not RESEND_API_KEY:
        logger.error("RESEND_API_KEY is not set — aborting.")
        sys.exit(1)

    init_db()

    users = get_users_with_emails()
    if not users:
        logger.info("No users with email addresses found — nothing to do.")
        return

    logger.info(f"Found {len(users)} user(s) with email addresses")

    # Build email subject with today's date
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%b %d, %Y")  # e.g. "Feb 13, 2026"
    subject = f"{date_str} — Stock Report"

    sent = 0
    failed = 0

    async with httpx.AsyncClient() as client:
        for user in users:
            username = user["username"]
            user_email = user["email"]

            logger.info(f"Processing {username} ({user_email})...")

            # Check if user has tickers
            tickers = get_user_tickers(username)
            if not tickers:
                logger.info(f"  {username} has no tickers — skipping")
                continue

            # Check if there are any cached articles
            total_articles = 0
            for t in tickers:
                total_articles += len(get_articles(t, hours=72))

            if total_articles == 0:
                logger.info(f"  {username} has no recent articles — skipping")
                continue

            # Generate fresh digest
            try:
                result = await generate_digest(username)
                digest_text = result["digest_text"]
                logger.info(
                    f"  Digest generated: {result['article_count']} articles, "
                    f"model={result['model']}"
                )
            except Exception as e:
                logger.error(f"  Digest generation failed for {username}: {e}")
                failed += 1
                continue

            # Build and send email
            html_body = _build_email_body(username, digest_text)

            # Determine recipients: user + admin CC
            recipients = [user_email]
            if RECIPIENT_EMAIL and RECIPIENT_EMAIL.lower() != user_email.lower():
                recipients.append(RECIPIENT_EMAIL)

            success = await send_email(client, recipients, subject, html_body)
            if success:
                sent += 1
            else:
                failed += 1

    logger.info(f"Done. {sent} email(s) sent, {failed} failed out of {len(users)} users.")


if __name__ == "__main__":
    asyncio.run(main())

