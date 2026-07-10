#!/usr/bin/env python3
"""
token_manager.py
----------------
Uses Playwright to open playo.co with saved session cookies, intercepts the
authorization header from any booking-lab API call, and writes it to
tokens/current_token.txt.

Usage:
    python scripts/token_manager.py --interactive-login
        Opens a real (non-headless) browser window. Log in manually (including
        OTP). Once the booking page loads and the API call fires, the script
        saves tokens/storage_state.json (full session) and tokens/current_token.txt
        (bearer token), then exits.

    python scripts/token_manager.py [--cookies <path>]
        Legacy: load a Netscape cookies file and sniff the token headlessly.

    python scripts/token_manager.py --refresh
        Force-refresh even if token file already exists (legacy cookies mode).

The authorization token expires after some unknown duration (TBD: observe 401s).
When fetch_availability.py gets a 401/403 it will call this script to refresh.
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

COOKIES_DEFAULT = os.path.expandvars(r"%USERPROFILE%\Downloads\playo.co_cookies.txt")
TOKEN_FILE = Path(__file__).parent.parent / "tokens" / "current_token.txt"
STORAGE_STATE_FILE = Path(__file__).parent.parent / "tokens" / "storage_state.json"

# Known venue to trigger the availability API call
_VENUE_ID = "ec7d2c4e-dc4a-434f-97ee-95cfd0f3c3a5"
_BOOKING_URL = f"https://playo.co/booking?venueId={_VENUE_ID}"

HEADERS_DEFAULT = {
    "accept": "application/json",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
    "referer": "https://playo.co/booking",
}


# ── Atomic write ───────────────────────────────────────────────────────────────

def _atomic_write(path: Path, content: str) -> None:
    """Write content to a temp file next to path, then atomically rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


# ── Legacy: Netscape cookie helpers ───────────────────────────────────────────

def _parse_netscape_cookies(path: str) -> list[dict]:
    """Parse a Netscape-format cookies file into Playwright-compatible dicts."""
    cookies = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) != 7:
                continue
            domain, _, path_str, secure, expiration, name, value = parts
            cookies.append(
                {
                    "domain": domain.lstrip("."),
                    "name": name,
                    "value": value,
                    "path": path_str,
                    "secure": secure == "TRUE",
                    "httpOnly": False,
                    "expires": int(expiration) if expiration != "-1" else -1,
                }
            )
    return cookies


def _fetch_token_headless(cookies_path: str) -> str | None:
    """Legacy: load Netscape cookies into a headless context and sniff the token."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("ERROR: playwright not installed. Run: pip install playwright && playwright install chromium")
        sys.exit(1)

    auth_token = None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        def on_response(response):
            nonlocal auth_token
            if "/booking-lab-public/availability/" in response.url and response.request.method == "GET":
                token = dict(response.request.headers).get("authorization", "")
                if token:
                    auth_token = token

        page.on("response", on_response)

        if cookies_path and os.path.exists(cookies_path):
            try:
                netscape_cookies = _parse_netscape_cookies(cookies_path)
                for c in netscape_cookies:
                    c.pop("expires", None)
                    c.pop("httpOnly", None)
                context.add_cookies(netscape_cookies)
            except Exception as e:
                print(f"WARN: Could not load cookies: {e}")

        page.goto(_BOOKING_URL, wait_until="networkidle")
        page.wait_for_timeout(2000)
        browser.close()

    return auth_token


# ── Interactive login ──────────────────────────────────────────────────────────

def _interactive_login(timeout_seconds: int = 300) -> None:
    """
    Open a non-headless Chromium window and wait for the user to log in
    (including OTP). Once the booking page loads and the availability API
    request fires, save:
        - tokens/storage_state.json  (full session: cookies + localStorage)
        - tokens/current_token.txt   (captured bearer token)
    Exits with code 1 if the timeout is reached without a successful capture.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("ERROR: playwright not installed. Run: pip install playwright && playwright install chromium")
        sys.exit(1)

    print(f"\n{'='*60}")
    print("  Playo Interactive Login")
    print(f"{'='*60}")
    print("  A browser window will open. Please:")
    print("    1. Log in to playo.co (including OTP)")
    print("    2. Wait for the booking page to load")
    print(f"  The script will auto-detect login and save your session.")
    print(f"  Timeout: {timeout_seconds}s")
    print(f"{'='*60}\n")

    auth_token = None
    login_confirmed = False
    deadline = time.monotonic() + timeout_seconds

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=0)
        context = browser.new_context()
        page = context.new_page()

        def on_response(response):
            nonlocal auth_token, login_confirmed
            if "/booking-lab-public/availability/" in response.url and response.request.method == "GET":
                token = dict(response.request.headers).get("authorization", "")
                if token:
                    auth_token = token
                    login_confirmed = True
                    print(f"\n  ✓ Auth token captured!")

        page.on("response", on_response)

        # Navigate to the booking page — if not logged in, Playo will redirect
        # to the login page, where the user logs in manually.
        page.goto(_BOOKING_URL, wait_until="domcontentloaded", timeout=30_000)
        print("  Browser open. Waiting for you to complete login...")
        print("  (If already logged in, the token may appear immediately.)\n")

        # Poll until token is captured or timeout is hit
        while not login_confirmed:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                browser.close()
                print("\nERROR: Timed out waiting for login.")
                print("Please run again and complete login within the time limit.")
                sys.exit(1)

            # Every 10s print a reminder
            if int(remaining) % 10 == 0:
                print(f"  Waiting... {int(remaining)}s remaining", end="\r", flush=True)

            page.wait_for_timeout(500)

            # If the user navigated to the booking page but token wasn't sniffed
            # from the initial load, trigger it by reloading once we're on the
            # right domain and seem authenticated (no /login in URL).
            current_url = page.url
            if (
                "playo.co" in current_url
                and "/login" not in current_url
                and "booking" not in current_url
                and not login_confirmed
            ):
                # Navigate to booking page to trigger the API call
                try:
                    page.goto(_BOOKING_URL, wait_until="networkidle", timeout=20_000)
                    page.wait_for_timeout(2000)
                except Exception:
                    pass  # keep waiting

        # ── Success: save storage state and token ──────────────────────────────
        print(f"\n  ✓ Login confirmed. Saving session state...")

        # Save full browser storage state (cookies + localStorage)
        state = context.storage_state()
        _atomic_write(STORAGE_STATE_FILE, json.dumps(state, indent=2))
        print(f"  ✓ Storage state saved → {STORAGE_STATE_FILE}")

        # Save captured bearer token
        _atomic_write(TOKEN_FILE, auth_token.strip())
        print(f"  ✓ Bearer token saved  → {TOKEN_FILE}")

        # Print cookie expiry information for the user to evaluate longevity
        _print_cookie_expiry(state)

        browser.close()

    print(f"\n{'='*60}")
    print("  Done! You can now run the monitor.")
    print(f"  Silent refresh will use: {STORAGE_STATE_FILE}")
    print(f"  Run this again if refresh_token.py reports 'cookies dead'.")
    print(f"{'='*60}\n")


def _print_cookie_expiry(state: dict) -> None:
    """
    Print cookie expiry timestamps from a storage_state dict so the user
    can judge whether the session is long-lived or short-lived.
    """
    cookies = state.get("cookies", [])
    if not cookies:
        print("\n  (No cookies found in storage state — unusual.)")
        return

    now = time.time()
    print(f"\n  {'─'*56}")
    print(f"  Cookie expiry report ({len(cookies)} cookies saved):")
    print(f"  {'─'*56}")

    # Show the longest-lived cookies (most relevant for session health)
    sorted_cookies = sorted(
        [c for c in cookies if c.get("expires", -1) > 0],
        key=lambda c: c.get("expires", 0),
        reverse=True,
    )
    session_cookies = [c for c in cookies if c.get("expires", -1) <= 0]

    for c in sorted_cookies[:10]:
        exp = c.get("expires", 0)
        days_left = (exp - now) / 86400
        exp_str = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(exp))
        name = c.get("name", "?")
        domain = c.get("domain", "?")
        if days_left > 1:
            status = f"✓ {days_left:.1f} days left"
        elif days_left > 0:
            status = f"⚠ {days_left * 24:.1f} hours left"
        else:
            status = "✗ EXPIRED"
        print(f"    {name:<30} {domain:<20} expires {exp_str} [{status}]")

    if session_cookies:
        print(f"    ... plus {len(session_cookies)} session cookie(s) (no fixed expiry)")

    # Highlight the likely auth cookies
    auth_names = {"__session", "firebase", "token", "auth", "playo", "session"}
    key_cookies = [
        c for c in sorted_cookies
        if any(n in c.get("name", "").lower() for n in auth_names)
    ]
    if key_cookies:
        print(f"\n  Key auth cookies:")
        for c in key_cookies:
            exp = c.get("expires", 0)
            days_left = (exp - time.time()) / 86400
            print(f"    ► {c.get('name')} — {days_left:.1f} days remaining")
        if all((c.get("expires", 0) - now) / 86400 > 1 for c in key_cookies):
            print(f"\n  ✓ Session looks LONG-LIVED — silent refresh should work.")
        else:
            print(f"\n  ⚠ Session cookies expire soon — silent refresh may not help long-term.")
    else:
        print(f"\n  ⚠  Could not identify key auth cookies by name. Check expiries above manually.")
    print(f"  {'─'*56}\n")


# ── Token save/load ────────────────────────────────────────────────────────────

def save_token(token: str) -> None:
    _atomic_write(TOKEN_FILE, token.strip())
    print(f"Token saved to {TOKEN_FILE}")


def load_token() -> str | None:
    if TOKEN_FILE.exists():
        return TOKEN_FILE.read_text().strip()
    return None


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch / refresh Playo auth token via Playwright")
    parser.add_argument("--cookies", default=COOKIES_DEFAULT, help="Path to Netscape cookies file (legacy)")
    parser.add_argument("--refresh", action="store_true", help="Force refresh even if token file exists (legacy)")
    parser.add_argument(
        "--interactive-login",
        action="store_true",
        help=(
            "Open a non-headless browser window so you can log in manually. "
            "Saves tokens/storage_state.json and tokens/current_token.txt on success."
        ),
    )
    parser.add_argument(
        "--login-timeout",
        type=int,
        default=300,
        help="Seconds to wait for manual login (default: 300)",
    )
    args = parser.parse_args()

    if args.interactive_login:
        _interactive_login(timeout_seconds=args.login_timeout)
        return

    # ── Legacy cookies-file mode ───────────────────────────────────────────────
    if not args.refresh:
        existing = load_token()
        if existing:
            print(f"Using cached token from {TOKEN_FILE} (use --refresh to force re-fetch)")
            print(f"Token: {existing[:20]}...")
            return

    cookies_path = args.cookies
    if not os.path.exists(cookies_path):
        print(f"ERROR: Cookies file not found: {cookies_path}")
        print("Please provide --cookies or save the file to the default location.")
        print("\nTIP: Use --interactive-login instead for a one-time headful login that")
        print("     saves a reusable storage state (no cookies export needed).")
        sys.exit(1)

    print(f"Loading cookies from: {cookies_path}")
    print("Launching Playwright to capture auth token...")

    import asyncio
    token = _fetch_token_headless(cookies_path)

    if token:
        save_token(token)
        print(f"Captured token: {token[:20]}...")
    else:
        print("ERROR: Could not capture auth token. The session cookies may be expired.")
        print("Please refresh your cookies in the browser and save a new export.")
        print("\nTIP: Use --interactive-login instead — it saves a reusable session state.")
        sys.exit(1)


if __name__ == "__main__":
    main()