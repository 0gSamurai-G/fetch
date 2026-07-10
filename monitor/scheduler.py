"""
scheduler.py — Infinite poll scheduler with graceful shutdown.

Features:
- Runs the fetch cycle every `poll_interval_seconds` with optional jitter
- Periodic full-state reconciliation every N cycles
- Checks global backoff before each cycle
- Handles SIGINT/SIGTERM for graceful shutdown
- Updates health status after every run
- Writes alert entries when failure/schema thresholds are breached
- Logs every execution
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from .backoff import BackoffManager
from .models import (
    AlertEntry,
    HealthStatus,
    MonitorConfig,
    RunLogEntry,
    RunResult,
    SlotDiff,
)
from .runner import Runner

logger = logging.getLogger("monitor.scheduler")

# Exit code returned by scripts/refresh_token.py when session is dead
_REFRESH_EXIT_COOKIES_DEAD = 2


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
        3. Sleep remaining time until next poll time (with optional jitter)
        4. Execute runner.run() (reconciliation every N cycles)
        5. Update health status and alert hooks
        6. Update backoff state
        7. Log result and print diff summary
        8. Repeat
    """

    def __init__(
        self,
        runner: Runner,
        poll_interval_seconds: int,
        poll_interval_jitter_fraction: float,
        startup_delay_seconds: float,
        shutdown_timeout_seconds: float,
        reconciliation_interval_cycles: int,
        consecutive_failures_alert_threshold: int,
        schema_validation_alert_threshold: int,
        backoff: BackoffManager,
        health_file_path: Path,
        alerts_file_path: Path,
        shutdown: GracefulShutdown,
        token_refresh_interval_hours: float = 6.0,
        project_root: Path | None = None,
    ) -> None:
        self.runner = runner
        self.poll_interval = poll_interval_seconds
        self.poll_interval_jitter_fraction = poll_interval_jitter_fraction
        self.startup_delay = startup_delay_seconds
        self.shutdown_timeout = shutdown_timeout_seconds
        self.reconciliation_interval = reconciliation_interval_cycles
        self.consecutive_failures_alert_threshold = consecutive_failures_alert_threshold
        self.schema_validation_alert_threshold = schema_validation_alert_threshold
        self.backoff = backoff
        self.health_path = health_file_path
        self.alerts_path = alerts_file_path
        self.shutdown = shutdown
        self.token_refresh_interval_seconds = token_refresh_interval_hours * 3600
        self.project_root = project_root or health_file_path.parent.parent

        self._cycle_count = 0
        self._health = HealthStatus(
            start_time=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            current_status="starting",
        )
        self._recent_entries: list[RunLogEntry] = []

    def _jittered_interval(self) -> float:
        """Return poll interval with random jitter applied."""
        jitter = self.poll_interval * self.poll_interval_jitter_fraction
        return self.poll_interval + random.uniform(-jitter, jitter)

    def start(self) -> None:
        """
        Start the scheduler. Registers signal handlers, then runs the event loop
        until shutdown. Blocks the calling thread.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            # Unix only — raises NotImplementedError on Windows
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, self._on_signal, sig)
        except NotImplementedError:
            # Windows fallback: use signal.signal() on the main thread
            def _make_handler(s: signal.Signals):
                def _h(signum: int, frame: object) -> None:
                    self._on_signal(s)
                return _h

            signal.signal(signal.SIGINT, _make_handler(signal.SIGINT))
            if hasattr(signal, "SIGTERM"):
                try:
                    signal.signal(signal.SIGTERM, _make_handler(signal.SIGTERM))
                except (OSError, ValueError):
                    pass  # SIGTERM not reliably available on Windows

        logger.info(
            "Scheduler starting - poll_interval=%ds jitter=%.1f%% startup_delay=%.1fs "
            "reconciliation_every=%d cycles token_refresh_every=%.1fh",
            self.poll_interval,
            self.poll_interval_jitter_fraction * 100,
            self.startup_delay,
            self.reconciliation_interval,
            self.token_refresh_interval_seconds / 3600,
        )

        try:
            loop.run_until_complete(self._run_all())
        finally:
            loop.close()
            logger.info("Scheduler loop exited")

    async def _run_all(self) -> None:
        """Run the main fetch loop and the background token refresh loop concurrently."""
        await asyncio.gather(
            self._run_loop(),
            self._token_refresh_loop(),
            return_exceptions=True,
        )

    async def _run_loop(self) -> None:
        """Main scheduler loop - runs until shutdown is requested."""
        try:
            await asyncio.sleep(self.startup_delay)
        except asyncio.CancelledError:
            return

        next_run_ts = time.monotonic()
        self._health.current_status = "running"

        while not self.shutdown.is_requested:
            extra_delay = self.backoff.maybe_extra_delay()
            if extra_delay > 0 and not self.shutdown.is_requested:
                logger.info(
                    "Rate-limit backoff active - sleeping extra %.0fs before next poll",
                    extra_delay,
                )
                end_backoff = time.monotonic() + extra_delay
                while time.monotonic() < end_backoff and not self.shutdown.is_requested:
                    await asyncio.sleep(min(5.0, end_backoff - time.monotonic()))

            now = time.monotonic()
            sleep_duration = max(0.0, next_run_ts - now)
            if sleep_duration > 0 and not self.shutdown.is_requested:
                logger.debug("Sleeping %.1fs until next poll", sleep_duration)
                end_sleep = time.monotonic() + sleep_duration
                while time.monotonic() < end_sleep and not self.shutdown.is_requested:
                    await asyncio.sleep(min(5.0, end_sleep - time.monotonic()))

            if self.shutdown.is_requested:
                break

            self._cycle_count += 1

            is_reconciliation = (
                self._cycle_count == 1
                or (self._cycle_count % self.reconciliation_interval) == 0
            )
            if is_reconciliation:
                if self._cycle_count > 1:
                    logger.info(
                        "Scheduled reconciliation run at cycle %d (every %d cycles)",
                        self._cycle_count,
                        self.reconciliation_interval,
                    )
                logger.info("Reconciliation run - performing proactive cookie verification...")
                if not self.runner.token_manager.verify_cookies():
                    logger.warning("Proactive cookie verification failed. Attempting token refresh...")
                    try:
                        self.runner.token_manager.refresh()
                        logger.info("Token refreshed successfully proactively.")
                    except Exception as exc:
                        logger.error("Proactive token refresh failed: %s", exc)

            interval = self._jittered_interval()
            next_run_ts = time.monotonic() + interval
            self._health.next_scheduled_run = datetime.fromtimestamp(
                time.time() + interval, tz=timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%SZ")

            await self._execute_cycle(reconciliation=is_reconciliation)

            if self.shutdown.is_requested:
                break

        self._health.current_status = "stopped"
        self._health.next_scheduled_run = None
        self._save_health()

    async def _execute_cycle(self, reconciliation: bool = False) -> None:
        """Run one fetch + diff cycle and update health/logs."""
        run_ok = False

        try:
            records, diff, entry = await self.runner.run(
                reconciliation=reconciliation,
                stale_venues=self._health.stale_venues,
            )
            run_ok = True

            self._health.total_runs += 1
            self._health.last_successful_run = entry.timestamp
            self._health.last_run_result = entry.result
            self._health.consecutive_failures = 0
            self._health.token_refresh_count = self.runner._token_refresh_count
            self._health.schema_validation_failures = entry.schema_validation_failures

            age = self.runner.token_age_seconds()
            if age is not None:
                self._health.token_age_seconds = age

            self.backoff.on_run_complete(got_429=False, runs_since_last_429=0)

            self._log_entry(entry)
            self._save_health()
            self._check_alerts(entry)

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

            self._check_alerts_on_error(exc)

    def _check_alerts(self, entry: RunLogEntry) -> None:
        """Evaluate alert thresholds and write alert entries if triggered."""
        should_alert = False
        reason = ""

        if self._health.consecutive_failures >= self.consecutive_failures_alert_threshold:
            should_alert = True
            reason = (
                f"Consecutive failures ({self._health.consecutive_failures}) "
                f"crossed threshold ({self.consecutive_failures_alert_threshold})"
            )
        elif entry.schema_validation_failures >= self.schema_validation_alert_threshold:
            should_alert = True
            reason = (
                f"Schema validation failures ({entry.schema_validation_failures}) "
                f"crossed threshold ({self.schema_validation_alert_threshold})"
            )

        if self.runner.token_manager.is_cookies_dead():
            should_alert = True
            reason = (
                f"Cookies appear dead (server rejected token). "
                f"Last error: {self.runner.token_manager.last_verify_error() or 'unknown'}"
            )

        # Check for stale/carried-over venue-dates
        max_stale = self.runner.config.monitor.max_stale_cycles
        stale_list = []
        for key, count in self._health.stale_venues.items():
            if count > max_stale:
                stale_list.append(f"{key} ({count} consecutive cycles)")

        if stale_list:
            should_alert = True
            reason = (
                f"The following venue/date pairs have been stale/carried-over for "
                f"more than {max_stale} consecutive cycles: {', '.join(stale_list)}"
            )

        if should_alert:
            alert_entry = AlertEntry(
                timestamp=datetime.now(timezone.utc).isoformat(),
                reason=reason,
                severity="error" if "dead" in reason else "warning",
                run_id=entry.run_id,
                consecutive_failures=self._health.consecutive_failures,
                schema_validation_failures=entry.schema_validation_failures,
                current_status=self._health.current_status,
            )
            self._append_alert(alert_entry)

    def _check_alerts_on_error(self, exc: Exception) -> None:
        """Write an alert entry for an exception that caused a cycle to fail."""
        reason = f"Run failed: {type(exc).__name__}: {exc}"
        alert_entry = AlertEntry(
            timestamp=datetime.now(timezone.utc).isoformat(),
            reason=reason,
            severity="error",
            run_id="",
            consecutive_failures=self._health.consecutive_failures,
            current_status=self._health.current_status,
        )
        self._append_alert(alert_entry)

    def _append_alert(self, entry: AlertEntry) -> None:
        """Append an AlertEntry to alerts.json (atomic write)."""
        try:
            existing: list[dict] = []
            if self.alerts_path.exists():
                try:
                    existing = json.loads(self.alerts_path.read_text(encoding="utf-8"))
                except (json.JSONDecodeError, OSError):
                    existing = []

            existing.append(entry.to_dict())

            # Cap alerts log to the configured maximum count
            limit = self.runner.config.monitor.alerts_file_max_count
            if len(existing) > limit:
                existing = existing[-limit:]

            content = json.dumps(existing, indent=2)
            tmp = self.alerts_path.with_suffix(".tmp")
            tmp.write_text(content, encoding="utf-8")
            tmp.replace(self.alerts_path)

            logger.warning(
                "ALERT written to %s: [%s] %s",
                self.alerts_path,
                entry.severity,
                entry.reason,
            )
        except Exception as e:
            logger.error("Failed to write alert entry: %s", e)

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
            "retries=%d token_refreshed=%s changes=%s reconciliation=%s",
            entry.run_id,
            entry.result,
            entry.venues_queried,
            entry.slots_fetched,
            entry.duration_seconds,
            entry.retry_count,
            entry.token_refreshed,
            entry.changes,
            entry.reconciliation_run,
        )

    def _save_health(self) -> None:
        """Atomically write health status to disk."""
        self.health_path.parent.mkdir(parents=True, exist_ok=True)
        content = json.dumps(self._health.to_dict(), indent=2)
        tmp = self.health_path.with_suffix(".tmp")
        tmp.write_text(content, encoding="utf-8")
        tmp.replace(self.health_path)

    def _on_signal(self, sig: signal.Signals) -> None:
        logger.info("Received %s - initiating graceful shutdown", sig.name)
        self.shutdown.request()

    # ── Background token refresh loop ─────────────────────────────────────────

    async def _token_refresh_loop(self) -> None:
        """
        Run scripts/refresh_token.py every token_refresh_interval_seconds,
        independently of the main slot-fetch cycle.

        On success: update health fields last_silent_token_refresh_at / _result.
        On cookies-dead (exit code 2): write AlertEntry to alerts.json and log
        at ERROR level.
        """
        refresh_script = self.project_root / "scripts" / "refresh_token.py"

        if not refresh_script.exists():
            logger.warning(
                "Silent refresh script not found at %s — background token refresh disabled.",
                refresh_script,
            )
            return

        # Run the first refresh shortly after startup (15s delay), then every N hours.
        # This ensures the token is fresh right from the start without waiting N hours.
        first_run_delay = 15.0
        logger.info(
            "Token refresh loop starting — first run in %.0fs, then every %.1fh",
            first_run_delay,
            self.token_refresh_interval_seconds / 3600,
        )

        await self._interruptible_sleep(first_run_delay)

        while not self.shutdown.is_requested:
            await self._run_one_token_refresh(refresh_script)
            await self._interruptible_sleep(self.token_refresh_interval_seconds)

    async def _interruptible_sleep(self, total_seconds: float) -> None:
        """Sleep for total_seconds but wake up every 5s to check shutdown flag."""
        end = time.monotonic() + total_seconds
        while time.monotonic() < end and not self.shutdown.is_requested:
            await asyncio.sleep(min(5.0, end - time.monotonic()))

    async def _run_one_token_refresh(self, refresh_script: Path) -> None:
        """Invoke refresh_token.py as an async subprocess and handle its exit code."""
        logger.info("Running silent token refresh: %s", refresh_script)
        now_iso = datetime.now(timezone.utc).isoformat()

        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable,
                str(refresh_script),
                "--project-root", str(self.project_root),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(self.project_root),
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
        except asyncio.TimeoutError:
            logger.error("Silent token refresh timed out after 120s")
            self._health.last_silent_token_refresh_result = "error"
            self._save_health()
            return
        except Exception as exc:
            logger.error("Failed to launch refresh script: %s", exc)
            self._health.last_silent_token_refresh_result = "error"
            self._save_health()
            return

        # Log subprocess output at debug level
        if stdout:
            for line in stdout.decode(errors="replace").splitlines():
                logger.debug("[refresh_token] %s", line)
        if stderr:
            for line in stderr.decode(errors="replace").splitlines():
                logger.debug("[refresh_token stderr] %s", line)

        exit_code = proc.returncode

        if exit_code == 0:
            logger.info("Silent token refresh succeeded")
            self._health.last_silent_token_refresh_at = now_iso
            self._health.last_silent_token_refresh_result = "success"
            self._save_health()

        elif exit_code == _REFRESH_EXIT_COOKIES_DEAD:
            msg = (
                "Playo session expired — run "
                "scripts/token_manager.py --interactive-login to reauthenticate."
            )
            logger.error(msg)
            self._health.last_silent_token_refresh_result = "cookies_dead"
            self._save_health()

            alert = AlertEntry(
                timestamp=now_iso,
                reason=msg,
                severity="error",
                run_id="token_refresh",
                consecutive_failures=self._health.consecutive_failures,
                current_status=self._health.current_status,
            )
            self._append_alert(alert)

        else:
            logger.error(
                "Silent token refresh exited with unexpected code %d", exit_code
            )
            self._health.last_silent_token_refresh_result = "error"
            self._save_health()