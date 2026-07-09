# Playo → PlayZ Availability Sync

Fetches real-time slot availability from Playo for your own venues and outputs
a diff of changed slots, ready to be pushed into PlayZ to prevent double-booking.

---

## Two ways to run

| Mode | Command | Use when |
|------|---------|----------|
| **Manual** | `python scripts/fetch_availability.py` | Testing, one-off runs |
| **Daemon** | `python monitor.py` | Production, runs 24/7 |

Both share the same config files and token management.

---

## Quick start (daemon)

```bash
# 1. Install dependencies
pip install httpx playwright
playwright install chromium

# 2. Save Playo cookies (Chrome DevTools → Application → Cookies → playo.co → Export as Netscape format)
#    Save to %USERPROFILE%\Downloads\playo.co_cookies.txt

# 3. Get initial auth token
python scripts/token_manager.py --cookies "%USERPROFILE%\Downloads\playo.co_cookies.txt"

# 4. Start the production monitor (runs forever, Ctrl+C to stop)
python monitor.py
```

---

## File layout

```
Fetch - Bookings/
├── monitor.py                  ← Production daemon entry point
├── monitor/
│   ├── __init__.py             Public API re-exports
│   ├── __main__.py             python -m monitor
│   ├── models.py               Dataclasses: Config, Venue, SlotRecord, SlotDiff, HealthStatus, etc.
│   ├── config.py               Config loader with env-var overrides
│   ├── backoff.py              429 rate-limit backoff manager
│   ├── token_manager.py        Token lifecycle (load / auto-refresh / age tracking)
│   ├── runner.py               Async fetch + diff + snapshot history
│   └── scheduler.py            Infinite poll loop + graceful shutdown
├── scripts/
│   ├── fetch_availability.py   Manual / scheduled script (standalone)
│   └── token_manager.py        Playwright-based token extractor
├── config/
│   ├── config.json             All settings (daemon + fetch + backoff + snapshot)
│   └── venues.json             Your venue list
├── logs/                       Rotating log files (monitor.log + 7 backups)
├── monitor_data/
│   ├── status.json              Health status (last run, token age, failures, uptime)
│   ├── latest.json              Latest full snapshot
│   ├── latest_diff.json         Latest diff (newly_booked / newly_free)
│   └── history/                 Historical snapshots (auto-pruned by age + count)
├── tokens/
│   └── current_token.txt        Playo auth token (auto-refreshed on 401/403)
└── output/                      Output from manual fetch_availability.py script
```

---

## monitor.py — Production daemon

### Starting

```bash
python monitor.py
```

The daemon:
- Runs forever, polling every **5 minutes** (configurable)
- Automatically refreshes the auth token on 401/403
- Recovers from network failures with exponential backoff
- Backs off on 429 with escalating delays
- Logs everything to `logs/monitor.log`
- Updates `monitor_data/status.json` after every run

### Checking status

```bash
python monitor.py --check-health
```

Sample output:
```
=== Monitor Health ===
  current_status         running
  last_successful_run    2026-07-09T15:49:01Z
  last_failed_run        never
  last_run_result        success
  total_runs             42
  consecutive_failures    0
  token_refresh_count     1
  token_age_seconds       3600s
  next_scheduled_run      2026-07-09T16:00:00Z
  uptime                 2026-07-09T14:00:00Z
```

### Forcing a token refresh

```bash
python monitor.py --force-token-refresh
```

### Stopping gracefully

Press **Ctrl+C**. The daemon:
1. Finishes the current fetch cycle
2. Flushes logs
3. Saves health status
4. Exits cleanly

Never kill -9 / SIGKILL, or the current cycle state may be lost.

### Starting as a module

```bash
python -m monitor
```

---

## Configuration — config/config.json

Everything is configurable. Environment variables override file values.

```json
{
  "monitor": {
    "poll_interval_seconds":   300,
    "startup_delay_seconds":    2.0,
    "shutdown_timeout_seconds": 60.0,
    "log_level":               "INFO",
    "log_dir":                 "logs",
    "status_file":             "monitor_data/status.json",
    "latest_snapshot_file":     "monitor_data/latest.json",
    "latest_diff_file":         "monitor_data/latest_diff.json"
  },
  "token": {
    "file":             "tokens/current_token.txt",
    "max_age_seconds":  14400,
    "refresh_script":    "scripts/token_manager.py",
    "cookies_file":     "%USERPROFILE%\\Downloads\\playo.co_cookies.txt",
    "api_base":         "https://api.playo.io"
  },
  "fetch": {
    "days_ahead":               3,
    "max_concurrent":           3,
    "request_delay_seconds":    0.8,
    "max_retries":              3,
    "retry_backoff_base_seconds": 2.0,
    "timeout_seconds":          30.0
  },
  "backoff": {
    "enabled":         true,
    "delays_seconds":  [60, 120, 300, 600],
    "reset_after_runs": 3
  },
  "snapshot": {
    "keep_days":   30,
    "history_dir": "monitor_data/history",
    "max_count":   1000
  }
}
```

### Environment variable overrides

| Env var | Type | Description |
|---------|------|-------------|
| `FETCH_POLL_INTERVAL_SECONDS` | int | Poll interval (default: 300) |
| `FETCH_TOKEN_FILE` | str | Token file path |
| `FETCH_COOKIES_FILE` | str | Cookies file path |
| `FETCH_LOG_DIR` | str | Logs directory |
| `FETCH_DAYS_AHEAD` | int | Days to fetch per poll |
| `FETCH_LOG_LEVEL` | str | DEBUG\|INFO\|WARNING\|ERROR |
| `FETCH_MAX_CONCURRENT` | int | Concurrent API requests |
| `FETCH_BACKOFF_RESET_AFTER` | int | Clean runs before backoff resets |

Example:
```bash
FETCH_POLL_INTERVAL_SECONDS=120 FETCH_LOG_LEVEL=DEBUG python monitor.py
```

---

## How it works

### Startup sequence

```
monitor.py starts
  → load config.json + venues.json
  → setup rotating file + console logging
  → load / validate auth token
  → register SIGINT/SIGTERM handlers
  → write initial status.json (current_status=starting)
  → start infinite poll loop
```

### Each poll cycle

```
1. Sleep until next 5-minute boundary
   (checks backoff cooldown and shutdown flag every ~5s)
2. Run fetch cycle:
   a. For each venue × day: call Playo availability API
      - Max 3 concurrent requests
      - 0.8s delay between requests
      - Up to 3 retries with exponential backoff (2s base)
   b. On HTTP 401/403:
      → run token_manager.py --refresh
      → retry all failed requests with new token
   c. On HTTP 429:
      → record backoff (1→2→5→10 min escalating delays)
      → skip this cycle, retry at next poll
3. Diff against previous snapshot
4. Save:
   - monitor_data/latest.json           (full snapshot)
   - monitor_data/latest_diff.json      (newly_booked / newly_free)
   - monitor_data/history/snapshot_*.json (historical, pruned by age + count)
5. Print human-readable summary if changes found
6. Update monitor_data/status.json
7. Log to logs/monitor.log
8. Return to step 1
```

### Backoff on 429

When Playo returns 429, the next poll cycle is delayed:

| 429 occurrence | Delay before next poll |
|----------------|------------------------|
| 1st | 60s (1 min) |
| 2nd | 120s (2 min) |
| 3rd | 300s (5 min) |
| 4th+ | 600s (10 min) |

After 3 consecutive clean runs (no 429), backoff resets to 0.

### Automatic token refresh

```
API call returns 401/403
  → signal token refresh
  → run: python scripts/token_manager.py --refresh
  → capture new authorization header from Playwright
  → overwrite tokens/current_token.txt
  → retry all failed requests with new token
  → log token refresh event
```

Manual refresh: `python monitor.py --force-token-refresh`

### Error recovery

| Error | Behaviour |
|-------|-----------|
| DNS failure | Retry 3× with exponential backoff (2s base) |
| Connection reset | Retry 3× with exponential backoff |
| Timeout | Retry 3× with exponential backoff |
| HTTP 5xx | Retry 3× with exponential backoff |
| HTTP 429 | Skip cycle, apply backoff delay |
| HTTP 401/403 | Auto-refresh token + retry |
| Fatal (config missing, no venues) | Exit with error code 1 |

The daemon never exits due to transient errors. Only fatal configuration problems cause exit.

---

## Snapshot and diff output

### `monitor_data/latest.json` — full snapshot

```json
[
  {
    "playz_turf_id":  "playz_karve_nagar_pickleball",
    "venue_name":     "Kaizen Sports - Karve Nagar",
    "playo_venue_id": "ec7d2c4e-dc4a-434f-97ee-95cfd0f3c3a5",
    "sport_code":     "SP83",
    "court_name":     "Outdoor Synthetic Court 1",
    "date":           "2026-07-09",
    "start_time":     "19:30:00",
    "end_time":       "20:00:00",
    "is_booked":       true,
    "fetched_at":      "2026-07-09T15:49:01Z"
  }
]
```

### `monitor_data/latest_diff.json` — changes

```json
{
  "newly_booked": [ { slot record... } ],
  "newly_free":   [ { slot record... } ],
  "unchanged":    [ { slot record... } ]
}
```

To POST changes to PlayZ:

```python
import httpx, json

diff = json.load(open("monitor_data/latest_diff.json"))

for slot in diff["newly_booked"]:
    httpx.post(
        "https://your-playz-instance.com/api/internal/block-slot",
        json={
            "playz_turf_id": slot["playz_turf_id"],
            "court_name":    slot["court_name"],
            "date":          slot["date"],
            "start_time":    slot["start_time"],
            "end_time":      slot["end_time"],
        },
        headers={"Authorization": "Bearer YOUR_PLAYZ_API_KEY"}
    )
```

---

## Venue configuration — config/venues.json

```json
[
  {
    "playz_turf_id":  "playz_karve_nagar_pickleball",
    "playo_venue_id": "ec7d2c4e-dc4a-434f-97ee-95cfd0f3c3a5",
    "sport_code":     "SP83",
    "venue_name":     "Kaizen Sports - Karve Nagar",
    "sport_name":     "Pickleball"
  }
]
```

To find `sport_code` for a new venue: navigate to the venue's booking page
and look at the network request URL:
`/booking-lab-public/availability/v1/{venue_id}/**SP83**/2026-07-09`

---

## Logging

- **File**: `logs/monitor.log` (rotates at 10 MB, keeps 7 backups)
- **Console**: stdout with timestamps
- **Level**: configured via `config.json` or `FETCH_LOG_LEVEL` env var

Example log entry:
```
2026-07-09T15:50:01 INFO     monitor.runner         [run abc12345] result=success     venues=3 slots=432 duration=12.34s retries=0 token_refreshed=False changes={'newly_booked': 2, 'newly_free': 1, 'unchanged': 429}
2026-07-09T16:00:01 WARNING  monitor.backoff         Rate-limited (429). Backoff level 2/4, next allowed in 120s
2026-07-09T16:05:01 WARNING  monitor.runner         [run def67890] result=success     venues=3 slots=432 duration=11.89s retries=0 token_refreshed=True changes={...}
```

---

## Health status — monitor_data/status.json

Written after every poll cycle:

```json
{
  "last_successful_run":   "2026-07-09T15:50:01Z",
  "last_failed_run":        null,
  "last_run_result":        "success",
  "token_age_seconds":      3612.4,
  "token_refresh_count":    1,
  "consecutive_failures":   0,
  "total_runs":             43,
  "start_time":             "2026-07-09T14:00:00Z",
  "next_scheduled_run":     "2026-07-09T16:05:00Z",
  "current_status":          "running"
}
```

---

## Snapshot history

- Stored in `monitor_data/history/snapshot_YYYY-MM-DD_HHMMSS.json`
- Auto-pruned: removes files older than 30 days **and** keeps max 1000 files
- `monitor_data/latest.json` always has the most recent full snapshot

---

## Recovery behavior

| Failure | Recovery |
|---------|----------|
| Token expires | Auto-refresh via Playwright on next 401/403 |
| Network blip | 3 retries with exponential backoff |
| Playo 429 | Backoff delays (1→2→5→10 min), then continues |
| Playo 5xx | Retry 3× with backoff, skip cycle if persistent |
| All venues fail | Log error, save failed status, continue next cycle |
| Config file missing | Exit with error |
| No venues configured | Exit with error |
| Cookies file missing | Exit with error |

---

## Deployment options

### Windows

```bat
# Shortcut in Startup folder, or run as a Windows Service via NSSM:
nssm install PlayoMonitor python monitor.py
nssm start PlayoMonitor
```

### Linux (systemd)

```ini
[Unit]
Description=Playo → PlayZ Availability Monitor
After=network.target

[Service]
Type=simple
User=youruser
WorkingDirectory=/opt/playo-monitor
ExecStart=/usr/bin/python3 monitor.py
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

### Docker

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY . /app
RUN pip install httpx playwright && playwright install chromium
CMD ["python", "monitor.py"]
```

---

## Manual script (one-off runs)

```bash
# Normal run
python scripts/fetch_availability.py

# Check diff only (no new snapshot)
python scripts/fetch_availability.py --check

# Force token refresh first
python scripts/fetch_availability.py --force-refresh-token
```

---

## Confirmed Playo API details

| Item | Value |
|------|-------|
| Endpoint | `GET https://api.playo.io/booking-lab-public/availability/v1/{venue_id}/{sport_code}/{date}` |
| Auth | Custom `authorization` header (token from browser session) |
| Session cookies | `sid` cookie (httpOnly, server-side session) — from Netscape cookie file |
| Sport codes | Venue-specific; determine via Playwright network inspection |
| Response field | `status: 1` = available, `status: 0` = booked |
| Min slot duration | 30 minutes (from `minSlotDuration` in response) |

---

## Known sport codes (Kaizen Sports, Pune)

| Sport | Code |
|-------|------|
| Pickleball | SP83 |
| (unknown) | SP56 |
| (unknown) | SP2 |

---

## Terms of Service

Automated scraping of Playo's API is **not** covered by an official partner
agreement. The `authorization` header is session-gated. Before long-term
production use, contact Playo about an official Venue Partner API. This
service is for your own venues only.