#!/usr/bin/env python3
"""
refresh_token.py
----------------
Silently refreshes the Playo bearer token using a saved browser storage state
(cookies + localStorage) from a previous interactive login session.

This script DOES NOT perform a new login. It loads the saved session state into
a headless browser, navigates to the booking page, and sniffs the authorization
header from the /booking-lab-public/availability/ network request — the same way
the interactive login does, but without any user interaction.

After every successful token capture, it also re-saves the storage_state.json
with the renewed session cookies. This supports Playo's sliding expiry window:
each successful authenticated visit extends the session, so as long as this
script runs regularly the session remains alive indefinitely.

Exit codes:
    0  — Success: token and storage state updated
    1  — Unexpected error (script bug, Playwright crash, etc.)
    2  — Session expired: storage_state.json exists but the session is no longer
         valid (redirected to login, availability request never fired). Manual
         re-login required: python scripts/token_manager.py --interactive-login

Usage:
    python scripts/refresh_token.py [--project-root PATH]
    python scripts/refresh_token.py --check-storage-expiry
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ── Project root resolution ────────────────────────────────────────────────────

_SCRIPT_DIR = Path(__file__).parent.resolve()
_DEFAULT_PROJECT_ROOT = _SCRIPT_DIR.parent  # scripts/ is one level under project root

# ── Paths (resolved at runtime from project root) ─────────────────────────────

def _paths(project_root: Path) -> tuple[Path, Path, Path]:
    """Return (storage_state_path, token_file_path, failure_marker_path)."""
    return (
        project_root / "tokens" / "storage_state.json",
        project_root / "tokens" / "current_token.txt",
        project_root / "tokens" / "refresh_failure.json",
    )

# ── Known venue to trigger availability API ────────────────────────────────────

_VENUE_ID = "ec7d2c4e-dc4a-434f-97ee-95cfd0f3c3a5"
_BOOKING_URL = f"https://playo.co/booking?venueId={_VENUE_ID}"
_AVAILABILITY_PATH = "/booking-lab-public/availability/"

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("refresh_token")


# ── Atomic write (mirrors runner.py pattern exactly) ──────────────────────────

def _atomic_write(path: Path, content: str) -> None:
    """Write content to a temp file next to path, then atomically rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


# ── Failure marker ────────────────────────────────────────────────────────────

def _write_failure_marker(failure_path: Path, reason: str) -> None:
    """Write a structured failure marker to tokens/refresh_failure.json."""
    marker = {
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "reason": reason,
        "action_required": (
            "Session cookies have expired. "
            "Run: python scripts/token_manager.py --interactive-login"
        ),
    }
    _atomic_write(failure_path, json.dumps(marker, indent=2))
    logger.error(
        "Playo session expired — run scripts/token_manager.py --interactive-login "
        "to reauthenticate. (Failure marker written to %s)",
        failure_path,
    )


# ── Core refresh logic ────────────────────────────────────────────────────────

def refresh(project_root: Path) -> int:
    """
    Perform one silent token refresh cycle.

    Returns exit code: 0=success, 1=unexpected error, 2=session dead.
    """
    storage_state_path, token_file_path, failure_path = _paths(project_root)

    # ── Precondition: storage state must exist ─────────────────────────────────
    if not storage_state_path.exists():
        logger.error(
            "No storage state found at %s. "
            "Run: python scripts/token_manager.py --interactive-login",
            storage_state_path,
        )
        _write_failure_marker(failure_path, "storage_state.json not found — initial login required")
        return 2

    logger.info("Loading storage state from %s", storage_state_path)

    try:
        storage_state_data = json.loads(storage_state_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.error("Failed to read storage state: %s", e)
        return 1

    # ── Launch headless Playwright with existing session ───────────────────────
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error(
            "playwright not installed. Run: pip install playwright && playwright install chromium"
        )
        return 1

    auth_token: str | None = None
    was_redirected_to_login = False

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(storage_state=storage_state_data)
            page = context.new_page()

            # ── Intercept availability requests to capture authorization header ──
            def on_response(response):
                nonlocal auth_token
                if _AVAILABILITY_PATH in response.url and response.request.method == "GET":
                    token = dict(response.request.headers).get("authorization", "")
                    if token:
                        auth_token = token
                        logger.debug("Authorization header captured from: %s", response.url)

            # ── Detect login redirect (session dead indicator) ─────────────────
            def on_request(request):
                nonlocal was_redirected_to_login
                if "login" in request.url and "playo.co" in request.url:
                    was_redirected_to_login = True
                    logger.debug("Login redirect detected: %s", request.url)

            page.on("response", on_response)
            page.on("request", on_request)

            logger.info("Navigating to booking page: %s", _BOOKING_URL)
            try:
                page.goto(_BOOKING_URL, wait_until="networkidle", timeout=30_000)
            except Exception as e:
                logger.warning("Navigation wait timed out or errored (%s), checking for token anyway", e)

            # Give late-firing XHR calls a few extra seconds
            page.wait_for_timeout(3000)

            # ── Evaluate outcome ───────────────────────────────────────────────
            if auth_token:
                logger.info("Token captured successfully. Saving...")

                # a) Save bearer token
                _atomic_write(token_file_path, auth_token.strip())
                logger.info("Bearer token saved → %s", token_file_path)

                # b) Re-save renewed storage state (supports sliding session expiry)
                #    context.storage_state() returns a plain dict; json.dumps() adapts
                #    it to the str that _atomic_write expects.
                renewed_state = context.storage_state()
                _atomic_write(storage_state_path, json.dumps(renewed_state, indent=2))
                logger.info("Storage state renewed → %s", storage_state_path)

                context.close()
                browser.close()
                return 0

            else:
                # Token never appeared — session is likely dead
                context.close()
                browser.close()

                if was_redirected_to_login:
                    reason = "Session redirected to login page — cookies have expired"
                else:
                    reason = (
                        "Navigated to booking page but availability request never fired "
                        "(possible redirect, auth wall, or page layout change)"
                    )

                logger.error(
                    "Silent refresh failed: %s. Current URL pattern: %s",
                    reason,
                    "login page detected" if was_redirected_to_login else "unknown",
                )
                _write_failure_marker(failure_path, reason)
                return 2

    except Exception as e:
        logger.exception("Unexpected error during refresh: %s", e)
        return 1


# ── Storage expiry inspector ──────────────────────────────────────────────────

def check_storage_expiry(project_root: Path) -> None:
    """Print cookie expiry info from the saved storage_state.json."""
    storage_state_path, _, _ = _paths(project_root)

    if not storage_state_path.exists():
        print(f"No storage state found at {storage_state_path}")
        print("Run: python scripts/token_manager.py --interactive-login")
        return

    state = json.loads(storage_state_path.read_text(encoding="utf-8"))
    cookies = state.get("cookies", [])

    if not cookies:
        print("No cookies in storage state.")
        return

    now = time.time()
    print(f"\nCookie expiry for {storage_state_path}")
    print(f"{'─'*70}")
    print(f"{'Name':<35} {'Domain':<20} {'Expires':<25} {'Status'}")
    print(f"{'─'*70}")

    sorted_cookies = sorted(
        [c for c in cookies if c.get("expires", -1) > 0],
        key=lambda c: c.get("expires", 0),
        reverse=True,
    )
    session_cookies = [c for c in cookies if c.get("expires", -1) <= 0]

    for c in sorted_cookies:
        exp = c.get("expires", 0)
        days_left = (exp - now) / 86400
        exp_str = datetime.fromtimestamp(exp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        name = c.get("name", "?")[:34]
        domain = c.get("domain", "?")[:19]

        if days_left > 1:
            status = f"✓ {days_left:.1f}d left"
        elif days_left > 0:
            status = f"⚠ {days_left * 24:.1f}h left"
        else:
            status = "✗ EXPIRED"

        print(f"{name:<35} {domain:<20} {exp_str:<25} {status}")

    if session_cookies:
        print(f"\n(+ {len(session_cookies)} session cookie(s) with no fixed expiry)")

    # Summary
    auth_names = {"__session", "firebase", "token", "auth", "playo", "session", "uid"}
    key_cookies = [
        c for c in sorted_cookies
        if any(n in c.get("name", "").lower() for n in auth_names)
    ]
    print(f"\n{'─'*70}")
    if key_cookies:
        min_days = min((c.get("expires", 0) - now) / 86400 for c in key_cookies)
        max_days = max((c.get("expires", 0) - now) / 86400 for c in key_cookies)
        print(f"Key auth cookies: {len(key_cookies)} found, "
              f"shortest expiry={min_days:.1f}d, longest={max_days:.1f}d")
        if min_days > 1:
            print("✓ Session looks LONG-LIVED — silent refresh should work.")
        else:
            print("⚠ Session cookies expiring soon — manual re-login needed soon.")
    else:
        print("Could not identify key auth cookies by name pattern.")
        print("Review the table above manually.")
    print()


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Silently refresh Playo bearer token using saved browser session state"
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=_DEFAULT_PROJECT_ROOT,
        help=f"Project root directory (default: {_DEFAULT_PROJECT_ROOT})",
    )
    parser.add_argument(
        "--check-storage-expiry",
        action="store_true",
        help="Print cookie expiry info from tokens/storage_state.json and exit",
    )
    args = parser.parse_args()

    project_root = args.project_root.resolve()

    if args.check_storage_expiry:
        check_storage_expiry(project_root)
        sys.exit(0)

    exit_code = refresh(project_root)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
