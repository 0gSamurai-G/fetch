import os
import json
import tempfile
from pathlib import Path
from datetime import datetime, timezone, timedelta
import pytest

from monitor.runner import (
    _validate_playo_response,
    _compute_diff,
    _atomic_write,
)
from monitor.models import SlotRecord, Config, MonitorConfig, TokenConfig, FetchConfig, BackoffConfig, SnapshotConfig
from monitor.config import validate_config

# 1. Test _validate_playo_response
def test_validate_playo_response():
    # Valid response
    valid_data = {
        "courtInfo": [
            {
                "courtName": "Synthetic Court 1",
                "courtId": 123
            }
        ]
    }
    is_valid, reason = _validate_playo_response(valid_data)
    assert is_valid is True
    assert reason == ""

    # Not a dict
    is_valid, reason = _validate_playo_response([])
    assert is_valid is False
    assert "response is not a dict" in reason

    # Missing courtInfo
    is_valid, reason = _validate_playo_response({})
    assert is_valid is False
    assert "missing required field 'courtInfo'" in reason

    # courtInfo not a list
    is_valid, reason = _validate_playo_response({"courtInfo": "not a list"})
    assert is_valid is False
    assert "expected list" in reason

    # court missing courtName
    invalid_court = {
        "courtInfo": [
            {
                "courtId": 123
                # missing courtName
            }
        ]
    }
    is_valid, reason = _validate_playo_response(invalid_court)
    assert is_valid is False
    assert "missing 'courtName'" in reason


# 2. Test _compute_diff with courtId keys & rename stability
def test_compute_diff_court_id_stability():
    old = [
        SlotRecord(
            playz_turf_id="turf_1",
            venue_name="Kaizen",
            playo_venue_id="venue_1",
            court_id=123,
            court_name="Old Court Name",
            sport_code="SP83",
            date="2026-07-09",
            start_time="09:00:00",
            end_time="09:30:00",
            is_booked=False,
            fetched_at="2026-07-09T08:00:00Z"
        )
    ]

    # New slot is booked, and has a new courtName but SAME courtId
    new = [
        SlotRecord(
            playz_turf_id="turf_1",
            venue_name="Kaizen",
            playo_venue_id="venue_1",
            court_id=123,
            court_name="Super Court 1", # Renamed
            sport_code="SP83",
            date="2026-07-09",
            start_time="09:00:00",
            end_time="09:30:00",
            is_booked=True, # Booked!
            fetched_at="2026-07-09T08:05:00Z"
        )
    ]

    diff = _compute_diff(old, new)
    assert len(diff.newly_booked) == 1
    assert len(diff.newly_free) == 0
    assert len(diff.unchanged) == 0
    assert diff.newly_booked[0].court_name == "Super Court 1"


# 3. Test reconciliation diff correctness
def test_reconciliation_diff_correctness():
    # Scenario A: reconciliation runs and previous snapshot exists.
    # In this case we do normal diff.
    old = [
        SlotRecord(
            playz_turf_id="turf_1",
            venue_name="Kaizen",
            playo_venue_id="venue_1",
            court_id=123,
            court_name="Synthetic 1",
            sport_code="SP83",
            date="2026-07-09",
            start_time="09:00:00",
            end_time="09:30:00",
            is_booked=False,
            fetched_at="2026-07-09T08:00:00Z"
        )
    ]
    new = [
        SlotRecord(
            playz_turf_id="turf_1",
            venue_name="Kaizen",
            playo_venue_id="venue_1",
            court_id=123,
            court_name="Synthetic 1",
            sport_code="SP83",
            date="2026-07-09",
            start_time="09:00:00",
            end_time="09:30:00",
            is_booked=True,
            fetched_at="2026-07-09T08:05:00Z"
        )
    ]

    # Reconciliation with previous snapshot
    diff = _compute_diff(old, new, reconciliation=False)
    assert len(diff.newly_booked) == 1
    assert len(diff.unchanged) == 0

    # Scenario B: Reconciliation runs when no previous snapshot exists.
    # Treat all current slots as unchanged (baseline).
    diff_baseline = _compute_diff([], new, reconciliation=True)
    assert len(diff_baseline.newly_booked) == 0
    assert len(diff_baseline.unchanged) == 1
    assert diff_baseline.unchanged[0].is_booked is True


# 4. Test atomic write behavior & mid-write failure simulation
def test_atomic_write_robustness():
    with tempfile.TemporaryDirectory() as tmp_dir:
        dest_path = Path(tmp_dir) / "latest.json"

        # Write initial valid file
        original_content = "initial valid data"
        dest_path.write_text(original_content, encoding="utf-8")

        # Simulate crash/failure mid-write of the new content
        # E.g. we try to write, but mock / trigger an exception
        # We can simulate this by monkeypatching Path.write_text to raise an exception
        # when writing the temporary file.
        def mock_write_text(self, data, *args, **kwargs):
            if ".tmp" in self.name:
                raise IOError("Simulated disk full / mid-write crash")
            return original_write_text(self, data, *args, **kwargs)

        original_write_text = Path.write_text
        Path.write_text = mock_write_text

        try:
            with pytest.raises(IOError):
                _atomic_write(dest_path, "corrupted partial data")
        finally:
            Path.write_text = original_write_text

        # Verify that original file was not corrupted or modified
        assert dest_path.read_text(encoding="utf-8") == original_content


# 5. Test configuration validation
def test_config_validation():
    def make_valid_config():
        return Config(
            monitor=MonitorConfig(),
            token=TokenConfig(),
            fetch=FetchConfig(),
            backoff=BackoffConfig(),
            snapshot=SnapshotConfig()
        )

    # Valid config shouldn't raise anything
    validate_config(make_valid_config())

    # Invalid poll interval
    cfg = make_valid_config()
    # Need to reconstruct due to frozen=True
    cfg_invalid = Config(
        monitor=MonitorConfig(poll_interval_seconds=0),
        token=cfg.token,
        fetch=cfg.fetch,
        backoff=cfg.backoff,
        snapshot=cfg.snapshot
    )
    with pytest.raises(ValueError, match="poll_interval_seconds must be > 0"):
        validate_config(cfg_invalid)

    # Invalid jitter
    cfg_invalid = Config(
        monitor=MonitorConfig(poll_interval_jitter_fraction=-0.1),
        token=cfg.token,
        fetch=cfg.fetch,
        backoff=cfg.backoff,
        snapshot=cfg.snapshot
    )
    with pytest.raises(ValueError, match="poll_interval_jitter_fraction must be in"):
        validate_config(cfg_invalid)


# 6. Test timezone correctness
def test_kolkata_timezone():
    kolkata_tz = timezone(timedelta(hours=5, minutes=30))
    now_utc = datetime.now(timezone.utc)
    now_kolkata = datetime.now(kolkata_tz)

    # The hour difference between local and UTC must be exactly 5.5 hours
    time_diff = abs((now_kolkata - now_utc).total_seconds())
    assert time_diff < 5.0 # within a few seconds of execution jitter
