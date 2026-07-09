#!/usr/bin/env python3
"""
monitor.py — Production availability monitoring daemon.

Usage:
    python monitor.py
    python monitor.py --config config/config.json
    python monitor.py --check-health
    python monitor.py --force-token-refresh
"""

from __future__ import annotations

import argparse
import json
import logging
import logging.handlers
import os
import signal
import sys
import time
from pathlib import Path

_PROJECT_ROOT = Path(__file__).parent.resolve()
_monitor_pkg  = _PROJECT_ROOT / "monitor"

if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from monitor.backoff import BackoffManager
from monitor.config import load_config, load_venues
from monitor.models import HealthStatus, LogLevel, Paths
from monitor.runner import Runner
from monitor.scheduler import GracefulShutdown, Scheduler
from monitor.token_manager import TokenManager


# ── Logging ────────────────────────────────────────────────────────────────────

def _setup_logging(level: LogLevel, log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)

    file_fmt  = "%(asctime)s %(levelname)-8s %(name)-20s %(message)s"
    console_fmt = "%(asctime)s %(levelname)-8s %(message)s"
    date_fmt  = "%Y-%m-%dT%H:%M:%S"

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.value))

    fh = logging.handlers.RotatingFileHandler(
        log_dir / "monitor.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=7,
        encoding="utf-8",
    )
    fh.setFormatter(logging.Formatter(file_fmt,  datefmt=date_fmt))
    fh.setLevel(getattr(logging, level.value))

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter(console_fmt, datefmt="%H:%M:%S"))
    ch.setLevel(getattr(logging, level.value))

    root.addHandler(fh)
    root.addHandler(ch)

    for noisy in ("httpx", "httpcore", "playwright"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


# ── Health ────────────────────────────────────────────────────────────────────

def _print_health(path: Path) -> None:
    if not path.exists():
        print("No health file found - monitor not running?")
        return
    h = HealthStatus.from_dict(json.loads(path.read_text()))
    print("\n=== Monitor Health ===")
    fields = [
        ("current_status",       h.current_status),
        ("last_successful_run",  h.last_successful_run  or "never"),
        ("last_failed_run",      h.last_failed_run       or "never"),
        ("last_run_result",      h.last_run_result       or "unknown"),
        ("total_runs",           h.total_runs),
        ("consecutive_failures", h.consecutive_failures),
        ("token_refresh_count",  h.token_refresh_count),
        ("token_age_seconds",    f"{h.token_age_seconds:.0f}s" if h.token_age_seconds else "?"),
        ("next_scheduled_run",   h.next_scheduled_run    or "not scheduled"),
        ("uptime",               h.start_time            or "?"),
    ]
    for k, v in fields:
        print(f"  {k:<26} {v}")
    print()


# ── Signal handling ────────────────────────────────────────────────────────────

_shutdown = GracefulShutdown()


def _sigint_handler(sig, frame):
    del sig, frame
    _shutdown.request()


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    args = _parse_args()

    try:
        cfg = load_config(args.config or _PROJECT_ROOT / "config" / "config.json")
    except FileNotFoundError as e:
        print(f"ERROR: Config not found: {e}", file=sys.stderr)
        sys.exit(1)

    log_dir = _PROJECT_ROOT / cfg.monitor.log_dir
    _setup_logging(cfg.monitor.log_level, log_dir)
    logger = logging.getLogger("monitor")

    paths = Paths(_PROJECT_ROOT)

    # --check-health shortcut
    if args.check_health:
        _print_health(paths.status_file(cfg.monitor))
        sys.exit(0)

    # Venues
    try:
        venues = load_venues(args.venues or None)
    except FileNotFoundError as e:
        print(f"ERROR: venues.json not found: {e}", file=sys.stderr)
        sys.exit(1)

    if not venues:
        print("ERROR: No venues in config - add at least one venue to venues.json", file=sys.stderr)
        sys.exit(1)

    logger.info("Monitor starting - %d venue(s) loaded", len(venues))

    # Token manager
    tm = TokenManager(cfg.token, _PROJECT_ROOT)

    if args.force_token_refresh:
        logger.info("Forcing token refresh before start")
        try:
            tm.refresh()
        except Exception as exc:
            print(f"ERROR: Token refresh failed: {exc}", file=sys.stderr)
            sys.exit(1)

    try:
        token = tm.get_token()
        age = tm.token_age_seconds()
        logger.info("Token ready (age: %.0fs)", age or 0)
    except Exception as exc:
        print(
            f"ERROR: No valid Playo auth token.\n"
            f"  Run first: python scripts/token_manager.py\n"
            f"  Error: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)

    # Backoff
    backoff = BackoffManager(cfg.backoff)

    # Paths
    health_path     = paths.status_file(cfg.monitor)
    latest_snapshot = paths.latest_snapshot(cfg.monitor)
    latest_diff     = paths.latest_diff(cfg.monitor)
    history_dir     = paths.history_dir(cfg.snapshot)

    # Runner
    runner = Runner(
        config=cfg,
        venues=venues,
        token_manager=tm,
        latest_snapshot_path=latest_snapshot,
        latest_diff_path=latest_diff,
        history_dir=history_dir,
    )

    # Write initial health
    health_path.parent.mkdir(parents=True, exist_ok=True)
    health_path.write_text(json.dumps(HealthStatus(
        start_time=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        current_status="running",
        token_age_seconds=age,
    ).to_dict(), indent=2))

    # Register signal handlers
    signal.signal(signal.SIGINT,  _sigint_handler)
    signal.signal(signal.SIGTERM, _sigint_handler)

    # Scheduler
    scheduler = Scheduler(
        runner=runner,
        poll_interval_seconds=cfg.monitor.poll_interval_seconds,
        startup_delay_seconds=cfg.monitor.startup_delay_seconds,
        shutdown_timeout_seconds=cfg.monitor.shutdown_timeout_seconds,
        backoff=backoff,
        health_file_path=health_path,
        shutdown=_shutdown,
    )

    print(
        f"\n"
        f"  Playo -> PlayZ Availability Monitor\n"
        f"  {'-'*42}\n"
        f"  Venues          {len(venues)}\n"
        f"  Poll interval   {cfg.monitor.poll_interval_seconds}s\n"
        f"  Logs            {log_dir}\n"
        f"  Health file     {health_path}\n"
        f"  History         {history_dir}\n"
        f"  {'-'*42}\n"
        f"  Press Ctrl+C to stop gracefully.\n"
    )

    logger.info(
        "Monitor initialised — venues=%d poll_interval=%ds",
        len(venues),
        cfg.monitor.poll_interval_seconds,
    )

    # Run until shutdown
    scheduler.start()

    logger.info("Monitor stopped cleanly")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Playo -> PlayZ availability monitor")
    p.add_argument("--config",              type=Path, default=None)
    p.add_argument("--venues",             type=Path, default=None)
    p.add_argument("--check-health",        action="store_true")
    p.add_argument("--force-token-refresh", action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    main()