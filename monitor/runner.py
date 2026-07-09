"""
runner.py — Async availability fetch runner.

Handles:
- Parallel fetching of all venues across N days via httpx/asyncio
- Automatic token refresh on 401/403
- Exponential backoff retry on network/5xx errors
- Concurrency limiting via semaphore
- Diff against previous snapshot (keyed by court_id for stability)
- Schema validation on Playo responses
- Periodic full-state reconciliation
- Atomic JSON writes (temp-then-rename)
- Daily rollup aggregation before history pruning
- Run log entry generation
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

from .models import (
    AlertEntry,
    Config,
    FetchConfig,
    RunLogEntry,
    RunResult,
    SlotDiff,
    SlotRecord,
    Venue,
)
from .token_manager import TokenError, TokenManager

logger = logging.getLogger("monitor.runner")

BASE_HEADERS = {
    "accept": "application/json",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
    "referer": "https://playo.co/booking",
}


# ── Schema validation ─────────────────────────────────────────────────────────

_SCHEMA_REQUIRED_KEYS = frozenset(["courtInfo"])


def _validate_playo_response(data: dict) -> tuple[bool, str]:
    """
    Validate the structure of a Playo availability response.

    Returns (is_valid, reason_string).
    """
    if not isinstance(data, dict):
        return False, "response is not a dict"

    for key in _SCHEMA_REQUIRED_KEYS:
        if key not in data:
            return False, f"missing required field '{key}'"

    court_info = data["courtInfo"]
    if not isinstance(court_info, list):
        return False, f"'courtInfo' is {type(court_info).__name__}, expected list"

    for i, court in enumerate(court_info):
        if not isinstance(court, dict):
            return False, f"courtInfo[{i}] is {type(court).__name__}, expected dict"
        if "courtName" not in court:
            return False, f"courtInfo[{i}] missing 'courtName'"

    return True, ""


# ── Response parsing ───────────────────────────────────────────────────────────

def _parse_playo_response(data: dict, target_date: date) -> list[SlotRecord]:
    """Parse Playo availability response into SlotRecord list."""
    records: list[SlotRecord] = []
    fetched_at = data.get("fetched_at", "") or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    min_duration = data.get("minSlotDuration", 30)

    for court in data.get("courtInfo") or []:
        court_id = court.get("courtId", 0) or 0
        court_name = court.get("courtName", "Unknown")
        for slot in (court.get("slotInfo") or []):
            start_hms = slot.get("time") or ""
            if not start_hms:
                continue
            hp = start_hms.split(":")
            h, m, s = int(hp[0]), int(hp[1]), int(hp[2])
            end_minutes = h * 60 + m + min_duration
            end_h = (end_minutes // 60) % 24
            end_m = end_minutes % 60
            end_time = f"{end_h:02d}:{end_m:02d}:{s:02d}"

            records.append(
                SlotRecord(
                    playz_turf_id="",
                    venue_name="",
                    playo_venue_id="",
                    court_id=court_id,
                    court_name=court_name,
                    sport_code="",
                    date=target_date.isoformat(),
                    start_time=start_hms,
                    end_time=end_time,
                    is_booked=slot.get("status", 1) == 0,
                    fetched_at=fetched_at,
                )
            )

    return records


# ── Diff ──────────────────────────────────────────────────────────────────────

def _compute_diff(old: list[SlotRecord], new: list[SlotRecord], reconciliation: bool = False) -> SlotDiff:
    """
    Diff two snapshot lists.

    Keyed by (court_id, date, start_time) — court_name is excluded to avoid
    false storms when Playo renames a court.

    When reconciliation=True, no previous snapshot exists, so all current
    slots go into 'unchanged' (treat full state as baseline).
    """
    if reconciliation:
        return SlotDiff(unchanged=new)

    old_index = {r.key(): r for r in old}
    new_index = {r.key(): r for r in new}
    all_keys = set(old_index.keys()) | set(new_index.keys())

    diff = SlotDiff()
    for k in all_keys:
        old_r = old_index.get(k)
        new_r = new_index.get(k)
        was = old_r.is_booked if old_r else None
        is_ = new_r.is_booked if new_r else None

        if old_r is None and new_r is not None:
            target = diff.newly_booked if is_ else diff.unchanged
            target.append(new_r)
        elif new_r is None:
            pass
        elif was != is_:
            if is_:
                diff.newly_booked.append(new_r)
            else:
                diff.newly_free.append(new_r)
        else:
            diff.unchanged.append(new_r)

    return diff


def _slot_to_dict(r: SlotRecord) -> dict[str, Any]:
    return {
        "playz_turf_id": r.playz_turf_id,
        "venue_name": r.venue_name,
        "playo_venue_id": r.playo_venue_id,
        "court_id": r.court_id,
        "court_name": r.court_name,
        "sport_code": r.sport_code,
        "date": r.date,
        "start_time": r.start_time,
        "end_time": r.end_time,
        "is_booked": r.is_booked,
        "fetched_at": r.fetched_at,
    }


# ── Atomic write ──────────────────────────────────────────────────────────────

def _atomic_write(path: Path, content: str) -> None:
    """Write content to a temp file next to path, then atomically rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


# ── Rollup ─────────────────────────────────────────────────────────────────────

def _generate_rollup(records: list[SlotRecord], run_id: str) -> dict[str, Any]:
    """Aggregate a set of slot records into a daily rollup summary."""
    by_turf: dict[str, dict[str, Any]] = {}
    for r in records:
        turf = r.playz_turf_id
        if turf not in by_turf:
            by_turf[turf] = {"venue_name": r.venue_name, "total_slots": 0, "booked": 0, "free": 0}
        by_turf[turf]["total_slots"] += 1
        if r.is_booked:
            by_turf[turf]["booked"] += 1
        else:
            by_turf[turf]["free"] += 1

    return {
        "rollup_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "venues": by_turf,
        "total_slots": len(records),
        "total_booked": sum(1 for r in records if r.is_booked),
        "total_free": sum(1 for r in records if not r.is_booked),
    }


@dataclass
class _VenueFetchTask:
    """A single venue x date fetch with retry logic."""

    client: httpx.AsyncClient
    venue: Venue
    target_date: date
    cfg: FetchConfig
    api_base: str
    token: str
    sem: asyncio.Semaphore
    on_auth_error: list[bool]
    result: list[SlotRecord] = field(default_factory=list)
    error: Exception | None = None
    got_429: bool = False
    retry_after_seconds: float | None = None
    retries: int = 0
    schema_validation_failures: int = 0

    async def run(self) -> None:
        url = (
            f"{self.api_base}/booking-lab-public/availability/v1"
            f"/{self.venue.playo_venue_id}/{self.venue.sport_code}/{self.target_date.isoformat()}"
        )
        headers = {**BASE_HEADERS, "authorization": self.token}
        delays = self.cfg.retry_backoff_base_seconds
        max_retries = self.cfg.max_retries

        async with self.sem:
            for attempt in range(max_retries + 1):
                try:
                    resp = await self.client.get(url, headers=headers)

                    if resp.status_code in (401, 403):
                        logger.warning(
                            "HTTP %d for %s %s - signalling token refresh",
                            resp.status_code,
                            self.venue.venue_name,
                            self.venue.sport_code,
                        )
                        self.on_auth_error.append(True)
                        return

                    if resp.status_code == 429:
                        self.got_429 = True
                        raw_retry = resp.headers.get("retry-after", "")
                        try:
                            self.retry_after_seconds = float(raw_retry)
                        except (ValueError, TypeError):
                            self.retry_after_seconds = None

                        logger.warning(
                            "429 for %s %s (attempt %d/%d, retry-after=%s)",
                            self.venue.venue_name,
                            self.venue.sport_code,
                            attempt + 1,
                            max_retries + 1,
                            raw_retry or "none",
                        )
                        if attempt < max_retries:
                            wait = self.retry_after_seconds or (delays * (2 ** attempt))
                            await asyncio.sleep(wait)
                            continue
                        return

                    if resp.status_code >= 500:
                        if attempt < max_retries:
                            wait = delays * (2 ** attempt)
                            logger.warning(
                                "HTTP %d for %s %s - retry %d/%d in %.1fs",
                                resp.status_code,
                                self.venue.venue_name,
                                self.venue.sport_code,
                                attempt + 1,
                                max_retries + 1,
                                wait,
                            )
                            await asyncio.sleep(wait)
                            continue
                        return

                    resp.raise_for_status()

                    raw = resp.json()
                    raw["fetched_at"] = resp.headers.get(
                        "date",
                        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    )

                    valid, reason = _validate_playo_response(raw)
                    if not valid:
                        self.schema_validation_failures += 1
                        logger.warning(
                            "Schema validation failed for %s %s: %s",
                            self.venue.venue_name,
                            self.venue.sport_code,
                            reason,
                        )
                        return

                    slots = _parse_playo_response(raw, self.target_date)
                    for slot in slots:
                        slot.playz_turf_id = self.venue.playz_turf_id
                        slot.venue_name = self.venue.venue_name
                        slot.playo_venue_id = self.venue.playo_venue_id
                        slot.sport_code = self.venue.sport_code

                    self.result = slots
                    return

                except (httpx.TimeoutException, httpx.ConnectError, httpx.RemoteProtocolError) as e:
                    self.error = e
                    if attempt < max_retries:
                        wait = delays * (2 ** attempt)
                        logger.warning(
                            "%s for %s %s - retry %d/%d in %.1fs",
                            type(e).__name__,
                            self.venue.venue_name,
                            self.venue.sport_code,
                            attempt + 1,
                            max_retries + 1,
                            wait,
                        )
                        await asyncio.sleep(wait)
                        self.retries += 1
                        continue
                    logger.error(
                        "Network error for %s %s after %d retries: %s",
                        self.venue.venue_name,
                        self.venue.sport_code,
                        max_retries,
                        e,
                    )
                    return

                except httpx.HTTPStatusError as e:
                    self.error = e
                    if attempt < max_retries:
                        wait = delays * (2 ** attempt)
                        logger.warning(
                            "HTTPStatusError %d for %s - retry %d/%d in %.1fs",
                            e.response.status_code,
                            self.venue.venue_name,
                            attempt + 1,
                            max_retries + 1,
                            wait,
                        )
                        await asyncio.sleep(wait)
                        self.retries += 1
                        continue
                    return


@dataclass
class Runner:
    """
    Async runner that fetches availability for all venues, computes diffs,
    and manages snapshot history.
    """

    config: Config
    venues: list[Venue]
    token_manager: TokenManager
    latest_snapshot_path: Path
    latest_diff_path: Path
    history_dir: Path
    rollup_dir: Path

    _token_refresh_count: int = 0

    async def run(
        self,
        reconciliation: bool = False,
        stale_venues: dict[str, int] | None = None,
    ) -> tuple[list[SlotRecord], SlotDiff, RunLogEntry]:
        """
        Execute one full poll cycle.

        When reconciliation=True, skips the diff against the previous snapshot
        (treats this run as establishing a fresh baseline) and logs it as a
        RECONCILIATION result.

        Returns (fresh_records, diff, run_log_entry).
        """
        run_id = uuid.uuid4().hex[:8]
        start_time = time.monotonic()
        auth_error_flag: list[bool] = []

        token = self._get_valid_token()

        kolkata_tz = timezone(timedelta(hours=5, minutes=30))
        today = datetime.now(kolkata_tz).date()
        dates = [today + timedelta(days=i) for i in range(self.config.fetch.days_ahead)]

        logger.info(
            "[run %s] Starting poll - %d venues x %d days (token age: %.0fs)%s",
            run_id,
            len(self.venues),
            len(dates),
            self.token_age_seconds() or 0,
            " [RECONCILIATION]" if reconciliation else "",
        )

        first_fetch = await self._fetch_all(token, dates, auth_error_flag)

        if auth_error_flag:
            logger.info("[run %s] Retrying after token refresh", run_id)
            self._do_token_refresh()
            self._token_refresh_count += 1
            token = self._get_valid_token()
            auth_error_flag.clear()
            first_fetch = await self._fetch_all(token, dates, auth_error_flag)

        tasks, total_retries, got_429, retry_after_val, schema_failures = first_fetch

        # Load previous snapshot to find carry-over records
        prev_records = self._load_previous_snapshot()
        prev_by_venue_date: dict[tuple[str, str], list[SlotRecord]] = {}
        for r in prev_records:
            prev_by_venue_date.setdefault((r.playo_venue_id, r.date), []).append(r)

        # Build all_records and handle carryover
        all_records: list[SlotRecord] = []
        current_stale_keys: set[str] = set()

        for t in tasks:
            v = t.venue
            date_str = t.target_date.isoformat()
            key_name = f"{v.venue_name} [{date_str}]"
            is_failed = (t.error is not None) or t.got_429 or (t.schema_validation_failures > 0)

            if is_failed:
                old_slots = prev_by_venue_date.get((v.playo_venue_id, date_str), [])
                if old_slots:
                    all_records.extend(old_slots)
                    logger.warning(
                        "Carrying over %d slot records for %s from previous snapshot due to fetch/validation failure.",
                        len(old_slots), key_name
                    )
                else:
                    logger.warning(
                        "Fetch failed for %s and no previous snapshot records exist to carry over.",
                        key_name
                    )
                current_stale_keys.add(key_name)
            else:
                all_records.extend(t.result)
                logger.info("Successfully fetched %d slot records for %s.", len(t.result), key_name)

        # Update stale_venues dictionary if provided
        if stale_venues is not None:
            # 1. Increment or add current stale keys
            for key in current_stale_keys:
                stale_venues[key] = stale_venues.get(key, 0) + 1
            # 2. Clear successfully fetched keys
            for t in tasks:
                key_name = f"{t.venue.venue_name} [{t.target_date.isoformat()}]"
                if key_name not in current_stale_keys and key_name in stale_venues:
                    del stale_venues[key_name]

        if not prev_records:
            diff = _compute_diff([], all_records, reconciliation=True)
        else:
            diff = _compute_diff(prev_records, all_records, reconciliation=False)

        self._save_snapshots(all_records, diff, run_id)
        self._maybe_rollup(all_records, run_id)

        duration = time.monotonic() - start_time
        changes = diff.summary()

        result = RunResult.RECONCILIATION.value if reconciliation else (
            RunResult.SUCCESS.value if all_records else RunResult.FAILED.value
        )

        log_entry = RunLogEntry(
            run_id=run_id,
            timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            duration_seconds=round(duration, 2),
            result=result,
            venues_queried=len(self.venues),
            slots_fetched=len(all_records),
            changes=changes,
            reconciliation_run=reconciliation,
            token_refreshed=self._token_refresh_count > 0,
            retry_count=total_retries,
            schema_validation_failures=schema_failures,
        )

        logger.info(
            "[run %s] Done%s in %.1fs | slots=%d booked=%d | diff: +booked=%d +free=%d",
            run_id,
            " [RECONCILIATION]" if reconciliation else "",
            duration,
            len(all_records),
            sum(1 for r in all_records if r.is_booked),
            changes["newly_booked"],
            changes["newly_free"],
        )

        return all_records, diff, log_entry

    async def _fetch_all(
        self,
        token: str,
        dates: list[date],
        auth_error_flag: list[bool],
    ) -> tuple[list[_VenueFetchTask], int, bool, float | None, int]:
        cfg = self.config.fetch
        sem = asyncio.Semaphore(cfg.max_concurrent)
        transport = httpx.AsyncHTTPTransport(retries=0)
        client = httpx.AsyncClient(
            transport=transport,
            headers={**BASE_HEADERS, "authorization": token},
            timeout=httpx.Timeout(cfg.timeout_seconds),
            limits=httpx.Limits(
                max_connections=cfg.max_concurrent,
                max_keepalive_connections=cfg.max_concurrent,
            ),
        )

        tasks: list[_VenueFetchTask] = []
        for venue in self.venues:
            for d in dates:
                tasks.append(
                    _VenueFetchTask(
                        client=client,
                        venue=venue,
                        target_date=d,
                        cfg=cfg,
                        api_base=self.config.token.api_base,
                        token=token,
                        sem=sem,
                        on_auth_error=auth_error_flag,
                    )
                )

        await asyncio.gather(*[t.run() for t in tasks])
        await client.aclose()

        total_retries = 0
        got_429 = False
        retry_after_val: float | None = None
        total_schema_failures = 0

        for t in tasks:
            total_retries += t.retries
            total_schema_failures += t.schema_validation_failures
            if t.got_429:
                got_429 = True
                retry_after_val = t.retry_after_seconds

        return tasks, total_retries, got_429, retry_after_val, total_schema_failures

    def token_age_seconds(self) -> float | None:
        return self.token_manager.token_age_seconds()

    def _get_valid_token(self) -> str:
        try:
            return self.token_manager.get_token()
        except TokenError as e:
            logger.error("Token acquisition failed: %s", e)
            raise

    def _do_token_refresh(self) -> None:
        try:
            self.token_manager.refresh()
            logger.info("Token refresh succeeded")
        except TokenError as e:
            logger.error("Token refresh failed: %s - proceeding with existing token", e)
            raise

    def _load_previous_snapshot(self) -> list[SlotRecord]:
        if not self.latest_snapshot_path.exists():
            return []
        raw = json.loads(self.latest_snapshot_path.read_text())
        return [SlotRecord.from_raw_dict(r) for r in raw]

    def _save_snapshots(self, records: list[SlotRecord], diff: SlotDiff, run_id: str) -> None:
        snapshot_content = json.dumps([_slot_to_dict(r) for r in records], indent=2)
        _atomic_write(self.latest_snapshot_path, snapshot_content)

        diff_content = json.dumps(
            {
                "newly_booked": [_slot_to_dict(r) for r in diff.newly_booked],
                "newly_free": [_slot_to_dict(r) for r in diff.newly_free],
                "unchanged": [_slot_to_dict(r) for r in diff.unchanged],
            },
            indent=2,
        )
        _atomic_write(self.latest_diff_path, diff_content)

        self.history_dir.mkdir(parents=True, exist_ok=True)
        history_path = self.history_dir / f"snapshot_{datetime.now(timezone.utc).strftime('%Y-%m-%d_%H%M%S')}.json"
        history_path.write_text(json.dumps([_slot_to_dict(r) for r in records], indent=2), encoding="utf-8")

        self._prune_history()

    def _maybe_rollup(self, records: list[SlotRecord], run_id: str) -> None:
        kolkata_tz = timezone(timedelta(hours=5, minutes=30))
        today_str = datetime.now(kolkata_tz).strftime("%Y-%m-%d")
        rollup_path = self.rollup_dir / f"{today_str}.json"

        existing: dict[str, Any] = {}
        if rollup_path.exists():
            try:
                existing = json.loads(rollup_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                existing = {}

        runs = existing.get("runs", [])
        runs.append(_generate_rollup(records, run_id))
        existing["runs"] = runs

        existing["last_updated"] = datetime.now(timezone.utc).isoformat()
        existing["total_runs"] = len(runs)

        _atomic_write(rollup_path, json.dumps(existing, indent=2))

    def _prune_history(self) -> None:
        max_count = self.config.snapshot.max_count
        keep_days = self.config.snapshot.keep_days
        cutoff = datetime.now(timezone.utc) - timedelta(days=keep_days)

        files = sorted(self.history_dir.glob("snapshot_*.json"), key=lambda p: p.stat().st_mtime)

        if len(files) > max_count:
            for f in files[: len(files) - max_count]:
                f.unlink(missing_ok=True)
                logger.debug("Pruned snapshot (count): %s", f.name)

        for f in files:
            try:
                mtime = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc)
                if mtime < cutoff:
                    f.unlink(missing_ok=True)
                    logger.debug("Pruned snapshot (age): %s", f.name)
            except OSError:
                pass

        self._prune_rollups(cutoff)

    def _prune_rollups(self, cutoff: datetime) -> None:
        retention = self.config.snapshot.rollup_retention_days
        if retention <= 0:
            return

        rollup_cutoff = datetime.now(timezone.utc) - timedelta(days=retention)
        for f in self.rollup_dir.glob("*.json"):
            try:
                mtime = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc)
                if mtime < rollup_cutoff:
                    f.unlink(missing_ok=True)
                    logger.debug("Pruned rollup (age): %s", f.name)
            except OSError:
                pass