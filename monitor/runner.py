"""
runner.py — Async availability fetch runner.

Handles:
- Parallel fetching of all venues across N days via httpx/asyncio
- Automatic token refresh on 401/403
- Exponential backoff retry on network/5xx errors
- Concurrency limiting via semaphore
- Diff against previous snapshot
- Snapshot history management
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
    Config,
    FetchConfig,
    RunLogEntry,
    RunResult,
    SlotDiff,
    SlotRecord,
    Venue,
)
from .token_manager import TokenManager

logger = logging.getLogger("monitor.runner")

BASE_HEADERS = {
    "accept": "application/json",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
    "referer": "https://playo.co/booking",
}


def _parse_playo_response(data: dict, target_date: date) -> list[SlotRecord]:
    """Parse Playo availability response into SlotRecord list."""
    records: list[SlotRecord] = []
    fetched_at = data.get("fetched_at", "") or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    min_duration = data.get("minSlotDuration", 30)

    for court in data.get("courtInfo", []):
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


def _compute_diff(old: list[SlotRecord], new: list[SlotRecord]) -> SlotDiff:
    """Diff two snapshot lists on (court_name, date, start_time) key."""
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
        "court_name": r.court_name,
        "sport_code": r.sport_code,
        "date": r.date,
        "start_time": r.start_time,
        "end_time": r.end_time,
        "is_booked": r.is_booked,
        "fetched_at": r.fetched_at,
    }


@dataclass
class _VenueFetchTask:
    """A single venue × date fetch with retry logic."""

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
                            "HTTP %d for %s %s — signalling token refresh",
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
                                "HTTP %d for %s %s — retry %d/%d in %.1fs",
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
                            "%s for %s %s — retry %d/%d in %.1fs",
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
                            "HTTPStatusError %d for %s — retry %d/%d in %.1fs",
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

    _token_refresh_count: int = 0

    async def run(self) -> tuple[list[SlotRecord], SlotDiff, RunLogEntry]:
        """
        Execute one full poll cycle.
        Returns (fresh_records, diff, run_log_entry).
        """
        run_id = uuid.uuid4().hex[:8]
        start_time = time.monotonic()
        auth_error_flag: list[bool] = []

        token = self._get_valid_token()

        today = date.today()
        dates = [today + timedelta(days=i) for i in range(self.config.fetch.days_ahead)]

        logger.info(
            "[run %s] Starting poll — %d venues × %d days (token age: %.0fs)",
            run_id,
            len(self.venues),
            len(dates),
            self.token_manager.token_age_seconds() or 0,
        )

        # First fetch attempt
        (
            all_records,
            total_retries,
            got_429,
            retry_after_val,
            _,
        ) = await self._fetch_all(token, dates, auth_error_flag)

        # Retry after token refresh if needed
        if auth_error_flag:
            logger.info("[run %s] Retrying after token refresh", run_id)
            self._do_token_refresh()
            self._token_refresh_count += 1
            token = self._get_valid_token()
            auth_error_flag.clear()

            (
                all_records,
                total_retries2,
                got_429_2,
                retry_after_val2,
                _,
            ) = await self._fetch_all(token, dates, auth_error_flag)
            total_retries += total_retries2
            got_429 = got_429 or got_429_2
            retry_after_val = retry_after_val or retry_after_val2

        # Diff against previous snapshot
        prev_records = self._load_previous_snapshot()
        diff = _compute_diff(prev_records, all_records) if prev_records else SlotDiff()

        # Save snapshots
        self._save_snapshots(all_records, diff)

        duration = time.monotonic() - start_time
        changes = diff.summary()

        log_entry = RunLogEntry(
            run_id=run_id,
            timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            duration_seconds=round(duration, 2),
            result=RunResult.SUCCESS.value if all_records else RunResult.FAILED.value,
            venues_queried=len(self.venues),
            slots_fetched=len(all_records),
            changes=changes,
            token_refreshed=self._token_refresh_count > 0,
            retry_count=total_retries,
        )

        logger.info(
            "[run %s] Done in %.1fs | slots=%d booked=%d | diff: +booked=%d +free=%d",
            run_id,
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
    ) -> tuple[list[SlotRecord], int, bool, float | None, bool]:
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

        all_records: list[SlotRecord] = []
        total_retries = 0
        got_429 = False
        retry_after_val: float | None = None

        for t in tasks:
            all_records.extend(t.result)
            total_retries += t.retries
            if t.got_429:
                got_429 = True
                retry_after_val = t.retry_after_seconds

        return all_records, total_retries, got_429, retry_after_val, False

    def _get_valid_token(self) -> str:
        try:
            return self.token_manager.get_token()
        except Exception as e:
            logger.error("Token acquisition failed: %s", e)
            raise

    def _do_token_refresh(self) -> None:
        try:
            self.token_manager.refresh()
            logger.info("Token refresh succeeded")
        except Exception as e:
            logger.error("Token refresh failed: %s - proceeding with existing token", e)
            raise

    def _load_previous_snapshot(self) -> list[SlotRecord]:
        if not self.latest_snapshot_path.exists():
            return []
        raw = json.loads(self.latest_snapshot_path.read_text())
        return [SlotRecord.from_raw_dict(r) for r in raw]

    def _save_snapshots(self, records: list[SlotRecord], diff: SlotDiff) -> None:
        self.latest_snapshot_path.parent.mkdir(parents=True, exist_ok=True)

        self.latest_snapshot_path.write_text(
            json.dumps([_slot_to_dict(r) for r in records], indent=2)
        )

        self.latest_diff_path.write_text(
            json.dumps(
                {
                    "newly_booked": [_slot_to_dict(r) for r in diff.newly_booked],
                    "newly_free": [_slot_to_dict(r) for r in diff.newly_free],
                    "unchanged": [_slot_to_dict(r) for r in diff.unchanged],
                },
                indent=2,
            )
        )

        self.history_dir.mkdir(parents=True, exist_ok=True)
        history_path = self.history_dir / f"snapshot_{datetime.now(timezone.utc).strftime('%Y-%m-%d_%H%M%S')}.json"
        history_path.write_text(json.dumps([_slot_to_dict(r) for r in records], indent=2))

        self._prune_history()

    def _prune_history(self) -> None:
        max_count = self.config.snapshot.max_count
        keep_days = self.config.snapshot.keep_days
        cutoff = datetime.now(timezone.utc) - timedelta(days=keep_days)

        files = sorted(self.history_dir.glob("snapshot_*.json"), key=lambda p: p.stat().st_mtime)

        # Prune by count
        if len(files) > max_count:
            for f in files[: len(files) - max_count]:
                f.unlink(missing_ok=True)
                logger.debug("Pruned snapshot (count): %s", f.name)

        # Prune by age
        for f in files:
            try:
                mtime = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc)
                if mtime < cutoff:
                    f.unlink(missing_ok=True)
                    logger.debug("Pruned snapshot (age): %s", f.name)
            except OSError:
                pass