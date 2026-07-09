"""
scheduler.py — Infinite poll scheduler with graceful shutdown.

Features:
- Runs the fetch cycle every `poll_interval_seconds`
- Checks global backoff before each cycle
- Handles SIGINT/SIGTERM for graceful shutdown
- Updates health status after every run
- Logs every execution
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from .backoff import BackoffManager
from .models import HealthStatus, RunLogEntry, RunResult, SlotDiff
from .runner import Runner

logger = logging.getLogger("monitor.scheduler")


class GracefulShutdown:
    """
    Tracks whether a graceful shutdown has been requested.
    Thread-safe across asyncio and signal handlers.
    """

    def __init__(self) -> None:
        self._requested = False
        self._lock = threading.Lock()
        self._shutdown_event = asyncio.Event()

    def request(self) -> None:
        with self._lock:
            self._requested = True
        logger.info("Shutdown requested - finishing current cycle then exiting")
        self._shutdown_event.set()

    @property
    def is_requested(self) -> bool:
        with self._lock:
            return self._requested

    def wait_for_shutdown(self) -> asyncio.Event:
        return self._shutdown_event


class Scheduler:
    """
    Infinite scheduler that runs ``Runner.run()`` every ``poll_interval_seconds``.

    Each cycle:
        1. Check shutdown flag (exit if set)
        2. Apply backoff cooldown delay if active
        3. Sleep remaining time until next poll time
        4. Execute runner.run()
        5. Update health status
        6. Update backoff state
        7. Log result and print diff summary
        8. Repeat
    """

    def __init__(
        self,
        runner: Runner,
        poll_interval_seconds: int,
        startup_delay_seconds: float,
        shutdown_timeout_seconds: float,
        backoff: BackoffManager,
        health_file_path: Path,
        shutdown: GracefulShutdown,
    ) -> None:
        self.runner = runner
        self.poll_interval = poll_interval_seconds
        self.startup_delay = startup_delay_seconds
        self.shutdown_timeout = shutdown_timeout_seconds
        self.backoff = backoff
        self.health_path = health_file_path
        self.shutdown = shutdown

        self._health = HealthStatus(
            start_time=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            current_status="starting",
        )
        self._recent_entries: list[RunLogEntry] = []

    def start(self) -> None:
        """
        Start the scheduler. Registers signal handlers, then runs the event loop
        until shutdown. Blocks the calling thread.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._on_signal, sig)

        logger.info(
            "Scheduler starting — poll_interval=%ds, startup_delay=%.1fs, shutdown_timeout=%.1fs",
            self.poll_interval,
            self.startup_delay,
            self.shutdown_timeout,
        )

        try:
            loop.run_until_complete(self._run_loop())
        finally:
            loop.close()
            logger.info("Scheduler loop exited")

    async def _run_loop(self) -> None:
        """Main scheduler loop — runs until shutdown is requested."""
        # Initial startup delay
        try:
            await asyncio.sleep(self.startup_delay)
        except asyncio.CancelledError:
            return

        # Absolute timestamp of next scheduled run
        next_run_ts = time.monotonic()
        self._health.current_status = "running"

        while not self.shutdown.is_requested:
            # 1. Check backoff cooldown and apply extra delay if needed
            extra_delay = self.backoff.maybe_extra_delay()
            if extra_delay > 0 and not self.shutdown.is_requested:
                logger.info(
                    "Rate-limit backoff active — sleeping extra %.0fs before next poll",
                    extra_delay,
                )
                end_backoff = time.monotonic() + extra_delay
                while time.monotonic() < end_backoff and not self.shutdown.is_requested:
                    await asyncio.sleep(min(5.0, end_backoff - time.monotonic()))

            # 2. Sleep until next scheduled poll time
            now = time.monotonic()
            sleep_duration = max(0.0, next_run_ts - now)
            if sleep_duration > 0 and not self.shutdown.is_requested:
                logger.debug("Sleeping %.1fs until next poll", sleep_duration)
                end_sleep = time.monotonic() + sleep_duration
                while time.monotonic() < end_sleep and not self.shutdown.is_requested:
                    await asyncio.sleep(min(5.0, end_sleep - time.monotonic()))

            if self.shutdown.is_requested:
                break

            # 3. Advance next run timestamp
            next_run_ts = time.monotonic() + self.poll_interval
            self._health.next_scheduled_run = datetime.fromtimestamp(
                time.time() + self.poll_interval, tz=timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%SZ")

            # 4. Execute the fetch cycle
            await self._execute_cycle()

            # 5. Check shutdown after cycle (graceful exit)
            if self.shutdown.is_requested:
                break

        # Graceful shutdown complete
        self._health.current_status = "stopped"
        self._health.next_scheduled_run = None
        self._save_health()

    async def _execute_cycle(self) -> None:
        """Run one fetch + diff cycle and update health/logs."""
        run_ok = False

        try:
            records, diff, entry = await self.runner.run()
            run_ok = True

            # Update health
            self._health.total_runs += 1
            self._health.last_successful_run = entry.timestamp
            self._health.last_run_result = RunResult.SUCCESS.value
            self._health.consecutive_failures = 0
            self._health.token_refresh_count = self.runner._token_refresh_count

            # Token age
            try:
                from token_manager import TokenManager
                # token_manager accessed via runner.token_manager
                age = self.runner.token_manager.token_age_seconds()
                self._health.token_age_seconds = age
            except Exception:
                pass

            # Update backoff (clean run — resets if enough consecutive)
            self.backoff.on_run_complete(got_429=False, runs_since_last_429=0)

            # Log
            self._log_entry(entry)
            self._save_health()

            # Print summary
            self._print_diff_summary(diff)

        except Exception as exc:
            self._health.total_runs += 1
            self._health.consecutive_failures += 1
            self._health.last_failed_run = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            self._health.last_run_result = RunResult.FAILED.value
            self._save_health()

            logger.error(
                "Poll cycle failed (consecutive_failures=%d): %s",
                self._health.consecutive_failures,
                exc,
                exc_info=True,
            )

    def _print_diff_summary(self, diff: SlotDiff) -> None:
        """Print a human-readable summary of booking changes."""
        if not diff.has_changes():
            return

        nb = len(diff.newly_booked)
        nf = len(diff.newly_free)

        print(f"\n{'-'*60}")
        print(f"BOOKING CHANGES: {nb} newly blocked | {nf} newly available")
        print(f"{'-'*60}")

        if nb:
            print("\n  [BLOCK in PlayZ]")
            for r in diff.newly_booked:
                print(f"    {r.playz_turf_id} | {r.court_name} | {r.date} {r.start_time}-{r.end_time}")

        if nf:
            print("\n  [UNBLOCK in PlayZ]")
            for r in diff.newly_free:
                print(f"    {r.playz_turf_id} | {r.court_name} | {r.date} {r.start_time}-{r.end_time}")

        print(f"\n  Total: {nb} blocked, {nf} unblocked")
        print(f"  Payload: monitor_data/latest_diff.json (ready to POST to PlayZ)")

    def _log_entry(self, entry: RunLogEntry) -> None:
        """Add run entry to in-memory buffer and log to logger."""
        self._recent_entries.append(entry)
        if len(self._recent_entries) > 200:
            self._recent_entries = self._recent_entries[-200:]

        level = logging.INFO
        if entry.result == RunResult.FAILED.value:
            level = logging.ERROR
        elif entry.token_refreshed:
            level = logging.WARNING

        logger.log(
            level,
            "[run %s] result=%-12s venues=%d slots=%d duration=%.1fs "
            "retries=%d token_refreshed=%s changes=%s",
            entry.run_id,
            entry.result,
            entry.venues_queried,
            entry.slots_fetched,
            entry.duration_seconds,
            entry.retry_count,
            entry.token_refreshed,
            entry.changes,
        )

    def _save_health(self) -> None:
        """Atomically write health status to disk."""
        self.health_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.health_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._health.to_dict(), indent=2))
        tmp.replace(self.health_path)  # Atomic on POSIX, close enough on Windows

    def _on_signal(self, sig: signal.Signals) -> None:
        logger.info("Received %s - initiating graceful shutdown", sig.name)
        self.shutdown.request()