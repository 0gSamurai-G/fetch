"""
models.py — All dataclasses and type definitions for the monitor service.
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any


# ── Enums ──────────────────────────────────────────────────────────────────────

class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class RunResult(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    RATE_LIMITED = "rate_limited"
    TOKEN_EXPIRED = "token_expired"
    SKIPPED = "skipped"
    RECONCILIATION = "reconciliation"


# ── Configuration dataclasses ──────────────────────────────────────────────────

@dataclass(frozen=True)
class FetchConfig:
    days_ahead: int = 3
    max_concurrent: int = 3
    request_delay_seconds: float = 0.8
    max_retries: int = 3
    retry_backoff_base_seconds: float = 2.0
    timeout_seconds: float = 30.0


@dataclass(frozen=True)
class BackoffConfig:
    enabled: bool = True
    delays_seconds: tuple[float, ...] = (60.0, 120.0, 300.0, 600.0)
    reset_after_runs: int = 3


@dataclass(frozen=True)
class SnapshotConfig:
    keep_days: int = 30
    history_dir: str = "monitor_data/history"
    max_count: int = 1000
    rollup_dir: str = "monitor_data/rollups"
    rollup_retention_days: int = 0  # 0 = keep forever


@dataclass(frozen=True)
class MonitorConfig:
    poll_interval_seconds: int = 300
    poll_interval_jitter_fraction: float = 0.1  # ±10% by default
    startup_delay_seconds: float = 2.0
    shutdown_timeout_seconds: float = 60.0
    reconciliation_interval_cycles: int = 12  # every 12 cycles = hourly at 5-min intervals
    log_level: LogLevel = LogLevel.INFO
    log_dir: str = "logs"
    status_file: str = "monitor_data/status.json"
    latest_snapshot_file: str = "monitor_data/latest.json"
    latest_diff_file: str = "monitor_data/latest_diff.json"
    alerts_file: str = "monitor_data/alerts.json"
    consecutive_failures_alert_threshold: int = 5
    schema_validation_alert_threshold: int = 3


# ── Alert dataclasses ──────────────────────────────────────────────────────────

@dataclass
class AlertEntry:
    """An alert written to monitor_data/alerts.json when attention is needed."""

    timestamp: str          # ISO 8601
    reason: str             # Human-readable reason
    severity: str           # "warning" | "error"
    run_id: str             # Which run triggered it
    consecutive_failures: int | None = None
    schema_validation_failures: int | None = None
    current_status: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> AlertEntry:
        return cls(**d)


# ── Paths helper ───────────────────────────────────────────────────────────────

class Paths:
    """Resolves output/input paths relative to a project root."""

    def __init__(self, project_root: Path) -> None:
        self._root = project_root.resolve()

    @property
    def log_dir(self) -> Path:
        return self._root / "logs"

    def status_file(self, monitor_cfg: MonitorConfig) -> Path:
        return self._root / monitor_cfg.status_file

    def latest_snapshot(self, monitor_cfg: MonitorConfig) -> Path:
        return self._root / monitor_cfg.latest_snapshot_file

    def latest_diff(self, monitor_cfg: MonitorConfig) -> Path:
        return self._root / monitor_cfg.latest_diff_file

    def alerts_file(self, monitor_cfg: MonitorConfig) -> Path:
        return self._root / monitor_cfg.alerts_file

    def history_dir(self, snapshot_cfg: SnapshotConfig) -> Path:
        return self._root / snapshot_cfg.history_dir

    def rollup_dir(self, snapshot_cfg: SnapshotConfig) -> Path:
        return self._root / snapshot_cfg.rollup_dir

    def token_file(self, token_cfg: TokenConfig) -> Path:
        return self._root / token_cfg.file

    def project_root(self) -> Path:
        return self._root


# ── Token config ───────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class TokenConfig:
    file: str = "tokens/current_token.txt"
    max_age_seconds: float = 14400.0
    refresh_script: str = "scripts/token_manager.py"
    cookies_file: str = "%USERPROFILE%\\Downloads\\playo.co_cookies.txt"
    api_base: str = "https://api.playo.io"


# ── Top-level Config ───────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Config:
    monitor: MonitorConfig
    token: TokenConfig
    fetch: FetchConfig
    backoff: BackoffConfig
    snapshot: SnapshotConfig


# ── Venue ─────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Venue:
    playz_turf_id: str
    playo_venue_id: str
    sport_code: str
    venue_name: str
    sport_name: str = ""

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Venue:
        return cls(
            playz_turf_id=d["playz_turf_id"],
            playo_venue_id=d["playo_venue_id"],
            sport_code=d["sport_code"],
            venue_name=d["venue_name"],
            sport_name=d.get("sport_name", ""),
        )


# ── Slot record ───────────────────────────────────────────────────────────────

@dataclass
class SlotRecord:
    """
    Normalised slot from the Playo availability API.

    Diff key uses (court_id, date, start_time) — NOT court_name — to avoid
    false block/unblock storms when Playo renames a court.
    """

    playz_turf_id: str
    venue_name: str
    playo_venue_id: str
    court_id: int       # Playo's internal court ID (used as diff key)
    court_name: str     # Display name only (not used for diff keying)
    sport_code: str
    date: str            # YYYY-MM-DD
    start_time: str     # HH:MM:SS
    end_time: str        # HH:MM:SS
    is_booked: bool
    fetched_at: str       # ISO 8601

    def key(self) -> tuple[int, str, str]:
        """Diff key: (court_id, date, start_time). court_name is excluded."""
        return (self.court_id, self.date, self.start_time)

    @classmethod
    def from_raw_dict(cls, d: dict[str, Any]) -> SlotRecord:
        return cls(
            playz_turf_id=d["playz_turf_id"],
            venue_name=d["venue_name"],
            playo_venue_id=d["playo_venue_id"],
            court_id=d["court_id"],
            court_name=d["court_name"],
            sport_code=d["sport_code"],
            date=d["date"],
            start_time=d["start_time"],
            end_time=d["end_time"],
            is_booked=d["is_booked"],
            fetched_at=d["fetched_at"],
        )


# ── Diff ──────────────────────────────────────────────────────────────────────

@dataclass
class SlotDiff:
    newly_booked: list[SlotRecord] = field(default_factory=list)
    newly_free: list[SlotRecord] = field(default_factory=list)
    unchanged: list[SlotRecord] = field(default_factory=list)

    def has_changes(self) -> bool:
        return bool(self.newly_booked or self.newly_free)

    def summary(self) -> dict[str, int]:
        return {
            "newly_booked": len(self.newly_booked),
            "newly_free": len(self.newly_free),
            "unchanged": len(self.unchanged),
        }

    def to_playz_payload(self) -> dict[str, Any]:
        return {
            "changes": [
                {
                    "action": "block",
                    "playz_turf_id": s.playz_turf_id,
                    "court_id": s.court_id,
                    "court_name": s.court_name,
                    "date": s.date,
                    "start_time": s.start_time,
                    "end_time": s.end_time,
                    "fetched_at": s.fetched_at,
                }
                for s in self.newly_booked
            ] + [
                {
                    "action": "unblock",
                    "playz_turf_id": s.playz_turf_id,
                    "court_id": s.court_id,
                    "court_name": s.court_name,
                    "date": s.date,
                    "start_time": s.start_time,
                    "end_time": s.end_time,
                    "fetched_at": s.fetched_at,
                }
                for s in self.newly_free
            ],
            "summary": self.summary(),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }


# ── Run log entry ──────────────────────────────────────────────────────────────

@dataclass
class RunLogEntry:
    run_id: str
    timestamp: str
    duration_seconds: float
    result: str
    venues_queried: int
    slots_fetched: int
    changes: dict[str, int]
    reconciliation_run: bool = False
    error_message: str = ""
    token_refreshed: bool = False
    retry_count: int = 0
    schema_validation_failures: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ── Health / Status ────────────────────────────────────────────────────────────

@dataclass
class HealthStatus:
    last_successful_run: str | None = None
    last_failed_run: str | None = None
    last_run_result: str | None = None
    token_age_seconds: float | None = None
    token_refresh_count: int = 0
    consecutive_failures: int = 0
    schema_validation_failures: int = 0
    total_runs: int = 0
    start_time: str = ""
    next_scheduled_run: str | None = None
    current_status: str = "starting"
    config_version: str = "2.0"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> HealthStatus:
        d2 = dict(d)
        d2.pop("config_version", None)
        return cls(**d2)


# ── Rate limit event ───────────────────────────────────────────────────────────

@dataclass
class RateLimitEvent:
    retry_after_seconds: float
    hit_at: str
    run_id: str