"""
monitor — Production availability monitoring package.

This __init__.py re-exports the public API for convenient access:
    from monitor import Runner, Scheduler, TokenManager, HealthStatus, ...

For type-checkers and IDE auto-complete, individual module imports are preferred:
    from monitor.runner import Runner
    from monitor.scheduler import Scheduler
"""

from __future__ import annotations

from monitor.backoff import BackoffManager
from monitor.config import load_config, load_venues
from monitor.models import (
    BackoffConfig,
    Config,
    FetchConfig,
    HealthStatus,
    LogLevel,
    MonitorConfig,
    Paths,
    RunLogEntry,
    RunResult,
    SlotDiff,
    SlotRecord,
    SnapshotConfig,
    TokenConfig,
    Venue,
)

__all__ = [
    "BackoffManager",
    "BackoffConfig",
    "Config",
    "FetchConfig",
    "HealthStatus",
    "load_config",
    "load_venues",
    "LogLevel",
    "MonitorConfig",
    "Paths",
    "RunLogEntry",
    "RunResult",
    "SlotDiff",
    "SlotRecord",
    "SnapshotConfig",
    "TokenConfig",
    "Venue",
]

__version__ = "1.0.0"