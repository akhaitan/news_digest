"""Centralized rate limiter for Polygon.io API.

Free tier: 5 requests per minute. All Polygon source modules share
this single limiter so concurrent refreshes (news + events + tickers)
never exceed the limit.
"""

import asyncio
import time
import logging

logger = logging.getLogger(__name__)


class PolygonRateLimiter:
    def __init__(self, max_requests: int = 5, window_seconds: float = 60.0):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._timestamps: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self):
        """Wait until a request slot is available, then claim it."""
        while True:
            async with self._lock:
                now = time.monotonic()
                # Prune timestamps outside the sliding window
                self._timestamps = [
                    t for t in self._timestamps
                    if now - t < self.window_seconds
                ]

                if len(self._timestamps) < self.max_requests:
                    self._timestamps.append(now)
                    return  # Slot acquired

                # Need to wait — calculate how long until the oldest request expires
                wait_time = self.window_seconds - (now - self._timestamps[0]) + 0.5

            # Sleep outside the lock so other coroutines aren't blocked
            logger.info(
                f"Polygon rate limiter: {len(self._timestamps)}/{self.max_requests} "
                f"slots used, waiting {wait_time:.1f}s"
            )
            await asyncio.sleep(wait_time)


# Singleton — shared across all Polygon API modules
limiter = PolygonRateLimiter(max_requests=5, window_seconds=60.0)

