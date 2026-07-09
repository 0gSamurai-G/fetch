#!/usr/bin/env python3
"""
token_manager.py
----------------
Uses Playwright to open playo.co with saved session cookies, intercepts the
authorization header from any booking-lab API call, and writes it to
tokens/current_token.txt.

Usage:
    python token_manager.py [--cookies <path>]
    python token_manager.py --refresh

The authorization token expires after some unknown duration (TBD: observe 401s).
When fetch_availability.py gets a 401/403 it will call this script to refresh.
"""

import argparse
import asyncio
import json
import os
import re
import sys
from pathlib import Path

COOKIES_DEFAULT = os.path.expandvars(r"%USERPROFILE%\Downloads\playo.co_cookies.txt")
TOKEN_FILE = Path(__file__).parent.parent / "tokens" / "current_token.txt"
HEADERS_DEFAULT = {
    "accept": "application/json",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
    "referer": "https://playo.co/booking",
}


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


def _netscape_to_session_cookies(path: str) -> list[dict]:
    """Convert Netscape cookies to a session-style list (no expires) for set_extra_http_headers."""
    cookies = _parse_netscape_cookies(path)
    session_cookies = []
    for c in cookies:
        session_cookies.append(
            {
                "name": c["name"],
                "value": c["value"],
                "domain": c["domain"],
                "path": c["path"],
            }
        )
    return session_cookies


async def _fetch_token_via_playwright(cookies_path: str) -> str | None:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("ERROR: playwright not installed. Run: pip install playwright && playwright install chromium")
        sys.exit(1)

    auth_token = None
    venue_id = "ec7d2c4e-dc4a-434f-97ee-95cfd0f3c3a5"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        # Intercept all XHR/fetch responses
        def on_response(response):
            nonlocal auth_token
            url = response.url
            if "/booking-lab-public/availability/" in url and response.request.method == "GET":
                headers = dict(response.request.headers)
                token = headers.get("authorization", "")
                if token:
                    auth_token = token

        page.on("response", on_response)

        # Try loading cookies first
        if cookies_path and os.path.exists(cookies_path):
            try:
                netscape_cookies = _parse_netscape_cookies(cookies_path)
                for c in netscape_cookies:
                    c.pop("expires", None)
                    c.pop("httpOnly", None)
                context.set_cookies(netscape_cookies)
            except Exception as e:
                print(f"WARN: Could not load cookies: {e}")

        # Navigate to trigger the availability API call
        page.goto(f"https://playo.co/booking?venueId={venue_id}", wait_until="networkidle")

        # Also intercept during any subsequent interactions (wait a bit for any lazy calls)
        page.wait_for_timeout(2000)

        browser.close()

    return auth_token


def save_token(token: str):
    TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_FILE.write_text(token.strip())
    print(f"Token saved to {TOKEN_FILE}")


def load_token() -> str | None:
    if TOKEN_FILE.exists():
        return TOKEN_FILE.read_text().strip()
    return None


def main():
    parser = argparse.ArgumentParser(description="Fetch / refresh Playo auth token via Playwright")
    parser.add_argument("--cookies", default=COOKIES_DEFAULT, help="Path to Netscape cookies file")
    parser.add_argument("--refresh", action="store_true", help="Force refresh even if token file exists")
    args = parser.parse_args()

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
        sys.exit(1)

    print(f"Loading cookies from: {cookies_path}")
    print("Launching Playwright to capture auth token...")

    token = asyncio.run(_fetch_token_via_playwright(cookies_path))

    if token:
        save_token(token)
        print(f"Captured token: {token[:20]}...")
    else:
        print("ERROR: Could not capture auth token. The session cookies may be expired.")
        print("Please refresh your cookies in the browser and save a new export.")
        sys.exit(1)


if __name__ == "__main__":
    main()