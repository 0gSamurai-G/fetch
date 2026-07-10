"""
config.py — Loads and validates configuration from config/config.json.
Supports env-var overrides (e.g. FETCH_POLL_INTERVAL_SECONDS=120).
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from .models import (
    BackoffConfig,
    Config,
    FetchConfig,
    LogLevel,
    MonitorConfig,
    SnapshotConfig,
    TokenConfig,
    Venue,
)


DEFAULT_CONFIG_DIR = Path(__file__).parent.parent / "config"


def _get_env_int(key: str, default: int) -> int:
    val = os.environ.get(key, "")
    try:
        return int(val) if val else default
    except ValueError:
        return default


def _get_env_float(key: str, default: float) -> float:
    val = os.environ.get(key, "")
    try:
        return float(val) if val else default
    except ValueError:
        return default


def _get_env_str(key: str, default: str) -> str:
    return os.environ.get(key, default)


def load_monitor_config(raw: dict) -> MonitorConfig:
    m = raw.get("monitor", {})
    return MonitorConfig(
        poll_interval_seconds=_get_env_int("FETCH_POLL_INTERVAL_SECONDS", m.get("poll_interval_seconds", 300)),
        poll_interval_jitter_fraction=_get_env_float("FETCH_POLL_JITTER", m.get("poll_interval_jitter_fraction", 0.1)),
        startup_delay_seconds=_get_env_float("FETCH_STARTUP_DELAY_SECONDS", m.get("startup_delay_seconds", 2.0)),
        shutdown_timeout_seconds=_get_env_float("FETCH_SHUTDOWN_TIMEOUT_SECONDS", m.get("shutdown_timeout_seconds", 60.0)),
        reconciliation_interval_cycles=_get_env_int("FETCH_RECONCILIATION_CYCLES", m.get("reconciliation_interval_cycles", 12)),
        log_level=LogLevel(m.get("log_level", "INFO").upper()),
        log_dir=_get_env_str("FETCH_LOG_DIR", m.get("log_dir", "logs")),
        status_file=_get_env_str("FETCH_STATUS_FILE", m.get("status_file", "monitor_data/status.json")),
        latest_snapshot_file=_get_env_str("FETCH_LATEST_SNAPSHOT", m.get("latest_snapshot_file", "monitor_data/latest.json")),
        latest_diff_file=_get_env_str("FETCH_LATEST_DIFF", m.get("latest_diff_file", "monitor_data/latest_diff.json")),
        alerts_file=_get_env_str("FETCH_ALERTS_FILE", m.get("alerts_file", "monitor_data/alerts.json")),
        consecutive_failures_alert_threshold=_get_env_int("FETCH_ALERT_FAILURES", m.get("consecutive_failures_alert_threshold", 5)),
        schema_validation_alert_threshold=_get_env_int("FETCH_ALERT_SCHEMA", m.get("schema_validation_alert_threshold", 3)),
        alerts_file_max_count=_get_env_int("FETCH_ALERTS_MAX_COUNT", m.get("alerts_file_max_count", 100)),
        max_stale_cycles=_get_env_int("FETCH_MAX_STALE_CYCLES", m.get("max_stale_cycles", 5)),
    )


def load_token_config(raw: dict) -> TokenConfig:
    t = raw.get("token", {})
    return TokenConfig(
        file=_get_env_str("FETCH_TOKEN_FILE", t.get("file", "tokens/current_token.txt")),
        max_age_seconds=_get_env_float("FETCH_TOKEN_MAX_AGE", t.get("max_age_seconds", 14400.0)),
        refresh_script=_get_env_str("FETCH_TOKEN_REFRESH_SCRIPT", t.get("refresh_script", "scripts/token_manager.py")),
        cookies_file=os.path.expandvars(
            _get_env_str("FETCH_COOKIES_FILE", t.get("cookies_file", "%USERPROFILE%\\Downloads\\playo.co_cookies.txt"))
        ),
        api_base=_get_env_str("PLAYO_API_BASE", t.get("api_base", "https://api.playo.io")),
    )


def load_fetch_config(raw: dict) -> FetchConfig:
    f = raw.get("fetch", {})
    return FetchConfig(
        days_ahead=_get_env_int("FETCH_DAYS_AHEAD", f.get("days_ahead", 3)),
        max_concurrent=_get_env_int("FETCH_MAX_CONCURRENT", f.get("max_concurrent", 3)),
        request_delay_seconds=_get_env_float("FETCH_REQUEST_DELAY", f.get("request_delay_seconds", 0.8)),
        max_retries=_get_env_int("FETCH_MAX_RETRIES", f.get("max_retries", 3)),
        retry_backoff_base_seconds=_get_env_float("FETCH_BACKOFF_BASE", f.get("retry_backoff_base_seconds", 2.0)),
        timeout_seconds=_get_env_float("FETCH_TIMEOUT", f.get("timeout_seconds", 30.0)),
    )


def load_backoff_config(raw: dict) -> BackoffConfig:
    b = raw.get("backoff", {})
    delays_raw = b.get("delays_seconds", [60, 120, 300, 600])
    return BackoffConfig(
        enabled=b.get("enabled", True),
        delays_seconds=tuple(float(d) for d in delays_raw),
        reset_after_runs=_get_env_int("FETCH_BACKOFF_RESET_AFTER", b.get("reset_after_runs", 3)),
    )


def load_snapshot_config(raw: dict) -> SnapshotConfig:
    s = raw.get("snapshot", {})
    return SnapshotConfig(
        keep_days=_get_env_int("FETCH_SNAPSHOT_KEEP_DAYS", s.get("keep_days", 30)),
        history_dir=_get_env_str("FETCH_HISTORY_DIR", s.get("history_dir", "monitor_data/history")),
        max_count=_get_env_int("FETCH_HISTORY_MAX_COUNT", s.get("max_count", 1000)),
        rollup_dir=_get_env_str("FETCH_ROLLUP_DIR", s.get("rollup_dir", "monitor_data/rollups")),
        rollup_retention_days=_get_env_int("FETCH_ROLLUP_RETENTION_DAYS", s.get("rollup_retention_days", 0)),
    )


def validate_config(cfg: Config) -> None:
    # MonitorConfig validation
    if cfg.monitor.poll_interval_seconds <= 0:
        raise ValueError(f"poll_interval_seconds must be > 0, got {cfg.monitor.poll_interval_seconds}")
    if not (0.0 <= cfg.monitor.poll_interval_jitter_fraction <= 1.0):
        raise ValueError(f"poll_interval_jitter_fraction must be in [0.0, 1.0], got {cfg.monitor.poll_interval_jitter_fraction}")
    if cfg.monitor.startup_delay_seconds < 0:
        raise ValueError(f"startup_delay_seconds must be >= 0, got {cfg.monitor.startup_delay_seconds}")
    if cfg.monitor.shutdown_timeout_seconds < 0:
        raise ValueError(f"shutdown_timeout_seconds must be >= 0, got {cfg.monitor.shutdown_timeout_seconds}")
    if cfg.monitor.reconciliation_interval_cycles < 0:
        raise ValueError(f"reconciliation_interval_cycles must be >= 0, got {cfg.monitor.reconciliation_interval_cycles}")
    if cfg.monitor.consecutive_failures_alert_threshold < 0:
        raise ValueError(f"consecutive_failures_alert_threshold must be >= 0, got {cfg.monitor.consecutive_failures_alert_threshold}")
    if cfg.monitor.schema_validation_alert_threshold < 0:
        raise ValueError(f"schema_validation_alert_threshold must be >= 0, got {cfg.monitor.schema_validation_alert_threshold}")
    if cfg.monitor.alerts_file_max_count <= 0:
        raise ValueError(f"alerts_file_max_count must be > 0, got {cfg.monitor.alerts_file_max_count}")
    if cfg.monitor.max_stale_cycles < 0:
        raise ValueError(f"max_stale_cycles must be >= 0, got {cfg.monitor.max_stale_cycles}")

    # TokenConfig validation
    if cfg.token.max_age_seconds <= 0:
        raise ValueError(f"token max_age_seconds must be > 0, got {cfg.token.max_age_seconds}")

    # FetchConfig validation
    if cfg.fetch.days_ahead <= 0:
        raise ValueError(f"days_ahead must be > 0, got {cfg.fetch.days_ahead}")
    if cfg.fetch.max_concurrent <= 0:
        raise ValueError(f"max_concurrent must be > 0, got {cfg.fetch.max_concurrent}")
    if cfg.fetch.request_delay_seconds < 0:
        raise ValueError(f"request_delay_seconds must be >= 0, got {cfg.fetch.request_delay_seconds}")
    if cfg.fetch.max_retries < 0:
        raise ValueError(f"max_retries must be >= 0, got {cfg.fetch.max_retries}")
    if cfg.fetch.retry_backoff_base_seconds < 0:
        raise ValueError(f"retry_backoff_base_seconds must be >= 0, got {cfg.fetch.retry_backoff_base_seconds}")
    if cfg.fetch.timeout_seconds <= 0:
        raise ValueError(f"timeout_seconds must be > 0, got {cfg.fetch.timeout_seconds}")

    # BackoffConfig validation
    if any(d <= 0 for d in cfg.backoff.delays_seconds):
        raise ValueError(f"backoff delays_seconds must all be > 0, got {cfg.backoff.delays_seconds}")
    if cfg.backoff.reset_after_runs <= 0:
        raise ValueError(f"backoff reset_after_runs must be > 0, got {cfg.backoff.reset_after_runs}")

    # SnapshotConfig validation
    if cfg.snapshot.keep_days < 0:
        raise ValueError(f"snapshot keep_days must be >= 0, got {cfg.snapshot.keep_days}")
    if cfg.snapshot.max_count < 0:
        raise ValueError(f"snapshot max_count must be >= 0, got {cfg.snapshot.max_count}")
    if cfg.snapshot.rollup_retention_days < 0:
        raise ValueError(f"snapshot rollup_retention_days must be >= 0, got {cfg.snapshot.rollup_retention_days}")


def load_config(config_path: Path | None = None) -> Config:
    if config_path is None:
        config_path = DEFAULT_CONFIG_DIR / "config.json"

    with open(config_path) as f:
        raw = json.load(f)

    cfg = Config(
        monitor=load_monitor_config(raw),
        token=load_token_config(raw),
        fetch=load_fetch_config(raw),
        backoff=load_backoff_config(raw),
        snapshot=load_snapshot_config(raw),
    )
    validate_config(cfg)
    return cfg


def load_venues(venues_path: Path | None = None) -> list[Venue]:
    """
    Load venues from the venues.json file.

    Args:
        venues_path: Path to a venues.json file. If None, uses
            config_dir / "venues.json" (config_dir is the package's config/ dir).
            Can also be set via FETCH_VENUES env var.
    """
    if venues_path is None:
        env_path = os.environ.get("FETCH_VENUES", "")
        if env_path:
            venues_path = Path(env_path)
        else:
            venues_path = DEFAULT_CONFIG_DIR / "venues.json"
    with open(venues_path) as f:
        return [Venue.from_dict(v) for v in json.load(f)]