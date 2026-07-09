"""
backoff.py — Global 429 rate-limit backoff manager.

When Playo returns 429, the next poll cycle(s) are delayed using
exponential backoff: 1 min → 2 min → 5 min → 10 min → ...
After enough consecutive clean runs, the backoff level resets.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

from .models import BackoffConfig

logger = logging.getLogger("monitor.backoff")


@dataclass
class BackoffManager:
    """
    Tracks 429 rate-limit state across poll cycles.

    Usage:
        bm = BackoffManager(config)

        # At end of each run:
        bm.on_run_complete(got_429=bool, runs_since_last_429=int)

        # At start of next cycle (before sleeping for poll_interval):
        extra_delay = bm.maybe_extra_delay()
        # sleep poll_interval + extra_delay if in cooldown period
    """

    config: BackoffConfig
    _level: int = 0
    _next_allowed_ts: float = 0.0
    _consecutive_clean: int = 0
    _last_429_at: float = 0.0

    def record_429(self, retry_after: float | None = None) -> None:
        """Called when a 429 is received. Advances backoff level."""
        if not self.config.enabled:
            return

        now = time.monotonic()
        self._last_429_at = now

        # Use Retry-After header if provided, otherwise use configured delay
        delay = retry_after if retry_after is not None else self.current_delay()
        self._next_allowed_ts = now + delay
        self._level = min(self._level + 1, len(self.config.delays_seconds) - 1)
        self._consecutive_clean = 0

        logger.warning(
            "Rate-limited (429). Backoff level %d/%d, next allowed in %.0fs",
            self._level + 1,
            len(self.config.delays_seconds),
            delay,
        )

    def current_delay(self) -> float:
        """Return the current backoff delay for 429 recovery."""
        if self._level < len(self.config.delays_seconds):
            return self.config.delays_seconds[self._level]
        return self.config.delays_seconds[-1]

    def maybe_extra_delay(self) -> float:
        """
        Return extra delay in seconds to apply before the next poll cycle.
        Returns 0 if no backoff is active.
        """
        if not self.config.enabled:
            return 0.0

        now = time.monotonic()
        remaining = self._next_allowed_ts - now
        if remaining > 0:
            logger.info("In backoff cooldown, waiting additional %.0fs", remaining)
            return remaining
        return 0.0

    def on_run_complete(self, got_429: bool, runs_since_last_429: int) -> None:
        """
        Called after each poll cycle completes.

        If no 429 occurred and enough clean runs have passed, reset
        the backoff level.
        """
        if got_429:
            self._consecutive_clean = 0
            return

        self._consecutive_clean += 1
        if (
            self._consecutive_clean >= self.config.reset_after_runs
            and self._level > 0
        ):
            prev_level = self._level
            self._level = 0
            self._next_allowed_ts = 0.0
            logger.info(
                "Backoff reset after %d clean runs (was level %d)",
                self._consecutive_clean,
                prev_level,
            )
            self._consecutive_clean = 0

    def is_in_cooldown(self) -> bool:
        """True if we are currently in a rate-limit cooldown period."""
        if not self.config.enabled:
            return False
        return time.monotonic() < self._next_allowed_ts

    def cooldown_remaining(self) -> float:
        """Seconds remaining in current cooldown, or 0."""
        remaining = self._next_allowed_ts - time.monotonic()
        return max(0.0, remaining)

    def reset(self) -> None:
        """Manually reset backoff to no delay."""
        self._level = 0
        self._next_allowed_ts = 0.0
        self._consecutive_clean = 0
        logger.info("Backoff manually reset")