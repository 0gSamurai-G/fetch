"""
token_manager.py — Async token lifecycle management.

Loads the stored auth token, validates its age, and runs the
Playwright-based refresh script when needed or on 401/403.
"""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

from .models import TokenConfig

logger = logging.getLogger("monitor.token")


class TokenError(Exception):
    pass


class TokenManager:
    """
    Manages the Playo API auth token lifecycle.

    - load_token()      — read from disk
    - validate_age()    — check if token is too old
    - needs_refresh()   — True if token is missing or expired
    - refresh()         — run token_manager.py as subprocess to get a new token
    - get_token()      — returns valid token, auto-refreshing if needed
    """

    def __init__(self, config: TokenConfig, project_root: Path) -> None:
        self.config = config
        self._project_root = project_root
        self._token_file = project_root / config.file
        self._token: str | None = None
        self._token_fetched_at: float = 0.0
        self._refresh_count: int = 0

    # ── Public API ──────────────────────────────────────────────────────────────

    def load_token(self) -> str | None:
        """Load token from disk, or None if not present."""
        if not self._token_file.exists():
            return None
        try:
            token = self._token_file.read_text().strip()
            if token:
                self._token = token
                self._token_fetched_at = os.path.getmtime(self._token_file)
                return token
        except OSError as e:
            logger.warning("Could not read token file: %s", e)
        return None

    def get_token(self) -> str:
        """
        Return a valid token, auto-refreshing if missing or too old.
        Raises TokenError if refresh fails.
        """
        token = self.load_token()
        if token and not self._is_expired():
            return token

        if token and self._is_expired():
            logger.info("Token is %.0fs old (max %.0fs) - refreshing", self._age_seconds(), self.config.max_age_seconds)

        self.refresh()
        return self._get_and_validate_token()

    def token_age_seconds(self) -> float | None:
        """Return age of current token in seconds, or None if no token."""
        if self._token is None:
            return None
        return time.time() - self._token_fetched_at

    # ── Refresh ────────────────────────────────────────────────────────────────

    def needs_refresh(self) -> bool:
        """True if token file is missing or token is too old."""
        if not self._token_file.exists():
            return True
        token = self.load_token()
        if not token:
            return True
        return self._is_expired()

    def refresh(self) -> None:
        """
        Run scripts/token_manager.py via subprocess to capture a fresh
        Playo auth token via Playwright.

        Blocks until the script completes. Raises TokenError on failure.
        """
        script_path = self._project_root / self.config.refresh_script
        if not script_path.exists():
            raise TokenError(f"Token refresh script not found: {script_path}")

        cookies_path = os.path.expandvars(self.config.cookies_file)
        if not os.path.exists(cookies_path):
            raise TokenError(
                f"Cookies file not found: {cookies_path}\n"
                "Please save your Playo cookies and update config/config.json"
            )

        logger.info(
            "Refreshing Playo auth token via Playwright (cookies: %s)",
            cookies_path,
        )

        try:
            result = subprocess.run(
                [
                    sys.executable,
                    str(script_path),
                    "--cookies",
                    cookies_path,
                    "--refresh",
                ],
                capture_output=True,
                text=True,
                timeout=120,
                cwd=str(self._project_root),
            )
        except subprocess.TimeoutExpired:
            raise TokenError("Token refresh timed out after 120s")
        except OSError as e:
            raise TokenError(f"Failed to run refresh script: {e}")

        if result.returncode != 0:
            logger.error("Token refresh stderr: %s", result.stderr)
            raise TokenError(f"Token refresh script exited with code {result.returncode}")

        # Validate new token was written
        new_token = self.load_token()
        if not new_token:
            raise TokenError("Token refresh script did not write a new token")

        self._refresh_count += 1
        logger.info("Token refreshed successfully (refresh #%d)", self._refresh_count)

    def refresh_count(self) -> int:
        return self._refresh_count

    # ── Private ───────────────────────────────────────────────────────────────

    def _get_and_validate_token(self) -> str:
        token = self.load_token()
        if not token:
            raise TokenError("No token available after refresh")
        return token

    def _is_expired(self) -> bool:
        if self._token is None:
            return True
        return self._age_seconds() >= self.config.max_age_seconds

    def _age_seconds(self) -> float:
        if not self._token_file.exists():
            return float("inf")
        return time.time() - os.path.getmtime(self._token_file)