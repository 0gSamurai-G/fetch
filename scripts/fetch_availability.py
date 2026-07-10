#!/usr/bin/env python3
"""
fetch_availability.py
---------------------
Fetches slot availability for configured venues from Playo and outputs
normalised records + a diff against the previous run.

Usage:
    python fetch_availability.py
    python fetch_availability.py --venues config/venues.json --output output
    python fetch_availability.py --check              # only print diff, no new snapshot
"""

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

SCRIPT_DIR = Path(__file__).parent
CONFIG_DIR = SCRIPT_DIR.parent / "config"
TOKEN_FILE = SCRIPT_DIR.parent / "tokens" / "current_token.txt"


# ── Token management ──────────────────────────────────────────────────────────

def load_token() -> str:
    if not TOKEN_FILE.exists():
        print("ERROR: No auth token found. Run: python scripts/token_manager.py --cookies <path>")
        sys.exit(1)
    return TOKEN_FILE.read_text().strip()


def refresh_token(cookies_path: str) -> str:
    print("Attempting token refresh via Playwright...")
    from token_manager import _fetch_token_via_playwright
    token = asyncio.run(_fetch_token_via_playwright(cookies_path))
    if not token:
        raise RuntimeError(
            "Token refresh failed. Please run:\n"
            "  python scripts/token_manager.py --cookies <path_to_cookies.txt> --refresh"
        )
    from token_manager import save_token
    save_token(token)
    print("Token refreshed successfully.")
    return token


# ── Configuration ──────────────────────────────────────────────────────────────

def load_config() -> dict[str, Any]:
    cfg_path = os.environ.get(
        "FETCH_CONFIG",
        str(CONFIG_DIR / "config.json")
    )
    with open(cfg_path) as f:
        return json.load(f)


def load_venues(venues_path: str | None = None) -> list[dict[str, str]]:
    if venues_path is None:
        venues_path = os.environ.get(
            "FETCH_VENUES",
            str(CONFIG_DIR / "venues.json")
        )
    with open(venues_path) as f:
        return json.load(f)


# ── Date helpers ───────────────────────────────────────────────────────────────

def date_range(start: date, days: int) -> list[date]:
    return [start + timedelta(days=i) for i in range(days)]


# ── API client ─────────────────────────────────────────────────────────────────

REQUEST_HEADERS = {
    "accept": "application/json",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
    "referer": "https://playo.co/booking",
    # devicetype: 99 is sent by the web app; we omit it as it doesn't seem required for the raw API
}


def _build_client(token: str, cfg: dict) -> httpx.AsyncClient:
    transport = httpx.AsyncHTTPTransport(retries=0)
    return httpx.AsyncClient(
        transport=transport,
        headers={
            **REQUEST_HEADERS,
            "authorization": token,
        },
        timeout=30.0,
        limits=httpx.Limits(
            max_connections=cfg["fetch"]["max_concurrent"],
            max_keepalive_connections=cfg["fetch"]["max_concurrent"],
        ),
    )


async def _fetch_venue_sport(
    client: httpx.AsyncClient,
    venue_id: str,
    sport_code: str,
    target_date: date,
    cfg: dict,
    sem: asyncio.Semaphore,
) -> list[dict] | None:
    url = (
        f"{cfg['playo']['api_base']}"
        f"/booking-lab-public/availability/v1"
        f"/{venue_id}/{sport_code}/{target_date.isoformat()}"
    )
    delays = cfg["fetch"]["retry_backoff_base_seconds"]
    max_retries = cfg["fetch"]["max_retries"]

    for attempt in range(max_retries + 1):
        async with sem:
            try:
                resp = await client.get(url)
                if resp.status_code == 401 or resp.status_code == 403:
                    print(f"  AUTH FAILED (401/403) for {venue_id}/{sport_code}/{target_date}")
                    return None
                resp.raise_for_status()
                return _parse_response(resp.json(), target_date)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    wait = delays * (2 ** attempt)
                    print(f"  Rate-limited (429). Waiting {wait}s before retry...")
                    await asyncio.sleep(wait)
                    continue
                if e.response.status_code >= 500:
                    wait = delays * (2 ** attempt)
                    print(f"  Server error ({e.response.status_code}). Retry {attempt+1}/{max_retries} after {wait}s")
                    await asyncio.sleep(wait)
                    continue
                raise
            except httpx.RequestError as e:
                if attempt < max_retries:
                    wait = delays * (2 ** attempt)
                    print(f"  Network error: {e}. Retry {attempt+1}/{max_retries} after {wait}s")
                    await asyncio.sleep(wait)
                    continue
                raise

    return None


def _parse_response(data: dict, target_date: date) -> list[dict]:
    """
    Parse the Playo availability response into normalised slot records.

    Response shape (confirmed via DevTools):
      {
        "requestStatus": 1,
        "message": "success",
        "data": {
          "currency": "INR",
          "timezone": "Asia/Kolkata",
          "isAvailable": true,
          "type": "COURTBOOKING",
          "minSlotDuration": 30,
          "courtInfo": [
            {
              "courtName": "Outdoor Synthetic Court 1",
              "courtId": 36003,
              "activityId": 18918,
              "slotInfo": [
                {"status": 1, "price": 200.0, "time": "05:00:00"},
                {"status": 1, "price": 200.0, "time": "05:30:00"},
                ...
              ]
            },
            ...
          ]
        }
      }

    Normalised output fields:
      playz_turf_id   – from venue config
      venue_name      – from venue config
      playo_venue_id  – from venue config
      court_name      – courtInfo[].courtName
      sport_code      – passed in
      date            – target_date.isoformat()
      start_time      – slotInfo[].time  (HH:MM:SS)
      end_time        – start_time + 30 min (minSlotDuration from response, default 30)
      is_booked       – True if status == 0, False if status == 1
      fetched_at      – ISO timestamp of when we fetched this record
    """
    records = []
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    min_duration = data.get("data", {}).get("minSlotDuration", 30)

    for court in data.get("data", {}).get("courtInfo", []):
        court_name = court.get("courtName", "Unknown")
        for slot in court.get("slotInfo", []):
            start_hms = slot.get("time", "")
            if not start_hms:
                continue
            start_parts = start_hms.split(":")
            h, m, s = int(start_parts[0]), int(start_parts[1]), int(start_parts[2])
            end_minutes = h * 60 + m + min_duration
            end_h = (end_minutes // 60) % 24
            end_m = end_minutes % 60
            end_time = f"{end_h:02d}:{end_m:02d}:{s:02d}"

            records.append({
                "court_name": court_name,
                "date": target_date.isoformat(),
                "start_time": start_hms,
                "end_time": end_time,
                "is_booked": slot.get("status", 1) == 0,
                "fetched_at": fetched_at,
            })

    return records


# ── Diff logic ─────────────────────────────────────────────────────────────────

def compute_diff(old: list[dict], new: list[dict]) -> dict:
    """
    Diff two snapshot lists on the natural key (court_name, date, start_time).

    Returns:
      {
        "newly_booked":   slots that were free in old but are now booked
      , "newly_free":     slots that were booked in old but are now free
      , "unchanged":      slots with the same status in both
      }
    """
    def key(r): return (r["court_name"], r["date"], r["start_time"])

    old_index = {key(r): r for r in old}
    new_index = {key(r): r for r in new}

    all_keys = set(old_index.keys()) | set(new_index.keys())

    newly_booked = []
    newly_free = []
    unchanged = []

    for k in all_keys:
        old_rec = old_index.get(k)
        new_rec = new_index.get(k)

        was_booked = old_rec["is_booked"] if old_rec else None
        is_booked  = new_rec["is_booked"] if new_rec else None

        if old_rec is None:
            # New slot appeared (not in old snapshot) — treat as free info only
            if is_booked:
                newly_booked.append(new_rec)
            else:
                unchanged.append(new_rec)
        elif new_rec is None:
            # Slot disappeared from new snapshot — skip (may be date range change)
            pass
        elif was_booked != is_booked:
            if is_booked:
                newly_booked.append(new_rec)
            else:
                newly_free.append(new_rec)
        else:
            unchanged.append(new_rec)

    return {"newly_booked": newly_booked, "newly_free": newly_free, "unchanged": unchanged}


# ── Main ────────────────────────────────────────────────────────────────────────

async def fetch_all(cfg: dict, venues: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Fetch all venues for today + next (days_ahead-1) days.
    Returns (fresh_records, venue_config_per_record).
    """
    days = cfg["fetch"]["days_ahead"]
    kolkata_tz = timezone(timedelta(hours=5, minutes=30))
    today = datetime.now(kolkata_tz).date()
    dates = date_range(today, days)

    token = load_token()
    client = _build_client(token, cfg)
    sem = asyncio.Semaphore(cfg["fetch"]["max_concurrent"])

    token_invalid = False
    all_records: list[dict] = []
    delays = cfg["fetch"]["request_delay_seconds"]

    tasks = []
    task_meta: list[tuple] = []

    for venue in venues:
        for target_date in dates:
            task_meta.append((venue, target_date))
            task = _fetch_venue_sport(
                client,
                venue["playo_venue_id"],
                venue["sport_code"],
                target_date,
                cfg,
                sem,
            )
            tasks.append(task)
            # sequential delay per request to same host
            await asyncio.sleep(delays)

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for meta, result in zip(task_meta, results):
        venue, target_date = meta
        if isinstance(result, Exception):
            err_msg = str(result)
            if "401" in err_msg or "403" in err_msg:
                print(f"  AUTH ERROR: {venue['playo_venue_id']} — token may be expired.")
                token_invalid = True
            else:
                print(f"  ERROR for {venue['venue_name']} ({target_date}): {result}")
            continue
        if result is None:
            # Auth failure marker
            continue
        for rec in result:
            rec["playz_turf_id"]  = venue["playz_turf_id"]
            rec["venue_name"]     = venue["venue_name"]
            rec["playo_venue_id"] = venue["playo_venue_id"]
            rec["sport_code"]     = venue["sport_code"]
        all_records.extend(result)

    await client.aclose()

    return all_records, []


async def run():
    parser = argparse.ArgumentParser(description="Fetch Playo slot availability for configured venues")
    parser.add_argument("--venues", help="Path to venues.json (default: config/venues.json)")
    parser.add_argument("--output-dir", help="Output directory (default: output/)")
    parser.add_argument("--check", action="store_true", help="Only show diff, don't save new snapshot")
    parser.add_argument("--force-refresh-token", action="store_true", help="Force token refresh before fetching")
    args = parser.parse_args()

    cfg = load_config()
    venues = load_venues(args.venues)
    output_dir = Path(args.output_dir or cfg["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    cookies_path = os.path.expandvars(cfg["playo"]["cookies_file"])

    kolkata_tz = timezone(timedelta(hours=5, minutes=30))
    if args.force_refresh_token or not TOKEN_FILE.exists():
        token = refresh_token(cookies_path)
    else:
        token = load_token()
        # Quick sanity check
        try:
            async with httpx.AsyncClient(headers={**REQUEST_HEADERS, "authorization": token}, timeout=10) as c:
                r = await c.get(
                    f"{cfg['playo']['api_base']}/booking-lab-public/availability/v1"
                    f"/{venues[0]['playo_venue_id']}/{venues[0]['sport_code']}/{datetime.now(kolkata_tz).date().isoformat()}"
                )
                if r.status_code in (401, 403):
                    print("Token rejected by API. Refreshing...")
                    token = refresh_token(cookies_path)
        except Exception:
            pass

    # Fetch
    print(f"\nFetching availability for {len(venues)} venue(s), {cfg['fetch']['days_ahead']} day(s)...")
    records, _ = await fetch_all(cfg, venues)
    print(f"Fetched {len(records)} slot records.\n")

    if not records:
        print("WARNING: No records fetched. Check your auth token and venue IDs.")
        return

    snapshot_path = output_dir / f"snapshot_{datetime.now(kolkata_tz).date().isoformat()}_{uuid.uuid4().hex[:8]}.json"
    diff_path = output_dir / "diff_latest.json"

    if args.check:
        prev_snapshot = sorted(output_dir.glob("snapshot_*.json"))
        if prev_snapshot:
            with open(prev_snapshot[-1]) as f:
                prev_records = json.load(f)
            diff = compute_diff(prev_records, records)
            _print_diff(diff)
        else:
            print("No previous snapshot found — showing full record list.")
            for r in records:
                print(f"  {r['venue_name']} | {r['court_name']} | {r['date']} {r['start_time']} | {'BOOKED' if r['is_booked'] else 'AVAILABLE'}")
        return

    # Save snapshot
    with open(snapshot_path, "w") as f:
        json.dump(records, f, indent=2)
    print(f"Snapshot saved: {snapshot_path}")

    # Diff against previous
    prev_snapshots = sorted(output_dir.glob("snapshot_*.json"))
    if len(prev_snapshots) > 1:
        with open(prev_snapshots[-2]) as f:
            prev_records = json.load(f)
        diff = compute_diff(prev_records, records)
        with open(diff_path, "w") as f:
            json.dump(diff, f, indent=2)
        print(f"Diff saved: {diff_path}")
        _print_diff(diff)
    else:
        print("First run — no diff to compute. Snapshot saved.")


def _print_diff(diff: dict):
    nb = len(diff["newly_booked"])
    nf = len(diff["newly_free"])
    print(f"\n{'='*60}")
    print(f"DIFF SUMMARY: {nb} newly booked | {nf} newly freed")
    print(f"{'='*60}")
    if nb:
        print("\n  NEWLY BOOKED (→ push to PlayZ to block):")
        for r in diff["newly_booked"]:
            print(f"    {r['playz_turf_id']} | {r['court_name']} | {r['date']} {r['start_time']}–{r['end_time']}")
    if nf:
        print("\n  NEWLY FREE (→ push to PlayZ to unblock):")
        for r in diff["newly_free"]:
            print(f"    {r['playz_turf_id']} | {r['court_name']} | {r['date']} {r['start_time']}–{r['end_time']}")
    if not nb and not nf:
        print("\n  No changes in booked/free status.")


if __name__ == "__main__":
    asyncio.run(run())