# USPTO Latest File Downloader

This directory contains only the code, dependency definitions, and minimal operating instructions required for production runtime. The fixed entrypoint is the CLI:

```bash
python3 run_download_latest_once.py
```

## Installation

```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
./.venv/bin/playwright install chromium
```

If you want to set optional environment variables:

```bash
cp .env.example .env
set -a
source .env
set +a
```

## Environment Variables

- `USPTO_ROOT_DIR`: Unified runtime root directory. If the download and runtime directories are not explicitly set, the service uses `downloads/` and `runtime/` under this root by default.
- `USPTO_DOWNLOADS_DIR`: Override the ZIP download directory.
- `USPTO_RUNTIME_DIR`: Override the directory for `app.db`, lock files, and runtime cache.
- `USPTO_COOKIE_CACHE_TTL_SECONDS`: Defaults to `0`, which disables persistence of third-party cookies. Cookies are written to `runtime/app.db` only when this value is greater than `0`.
- `USPTO_RETRY_JITTER_RATIO`: Jitter ratio used for retry backoff.
- `USPTO_FAILURE_COOLDOWN_SECONDS`: Cooldown period after repeated failures. Set to `0` to disable cooldown.

Notes:

- Relative paths are resolved against `USPTO_ROOT_DIR`. If `USPTO_ROOT_DIR` is not set, the project root is used as the base directory.
- If none of the three path variables are set, the default directories are used.
- Persistent cookie mode is only suitable for a single-user, trusted host.

## Run

Run a single sync:

```bash
./.venv/bin/python run_download_latest_once.py
```

Minimal production verification:

```bash
./.venv/bin/python -m py_compile run_download_latest_once.py core/*.py sync/*.py storage/*.py
./.venv/bin/playwright install chromium
./.venv/bin/python run_download_latest_once.py
```

## Scheduled Sync

Example `cron` entry:

```cron
0 */6 * * * cd /opt/uspto_latest_downloader && /bin/zsh -lc 'set -a; [ -f .env ] && source .env; set +a; ./.venv/bin/python run_download_latest_once.py' >> runtime/cron.log 2>&1
```

Example `systemd` service:

```ini
[Unit]
Description=USPTO latest downloader sync job
Wants=network-online.target
After=network-online.target

[Service]
Type=oneshot
WorkingDirectory=/opt/uspto_latest_downloader
EnvironmentFile=/opt/uspto_latest_downloader/.env
ExecStart=/opt/uspto_latest_downloader/.venv/bin/python /opt/uspto_latest_downloader/run_download_latest_once.py
```

Example `systemd` timer:

```ini
[Unit]
Description=Run USPTO latest downloader every 6 hours

[Timer]
OnBootSec=10min
OnUnitActiveSec=6h
Persistent=true

[Install]
WantedBy=timers.target
```

## Directory Layout

- `core/`: Shared contracts, constants, and logging utilities
- `sync/`: Main synchronization pipeline, including scheduling, upstream access, and ZIP handling
- `storage/`: SQLite persistence layer
- `run_download_latest_once.py`: CLI synchronization entrypoint
- `requirements.txt`: Production dependencies
- `.env.example`: Example deployment environment variables
- `downloads/`: Stored ZIP files
- `runtime/`: Database, lock files, and runtime cache

## Runtime Characteristics

- Download jobs perform limited retries and backoff for browser sessions, official metadata fetches, and ZIP downloads.
- Cookies obtained through the browser are not persisted by default. They are written to `runtime/app.db` only when `USPTO_COOKIE_CACHE_TTL_SECONDS > 0`.
- Retry backoff includes random jitter by default to avoid a fixed request rhythm.
- Repeated failures trigger a short cooldown window to avoid continuously hammering USPTO.
- Upstream `fileDownloadURI` values are validated against an `https://data.uspto.gov/...` allowlist.
- Existing local ZIP files are verified not only by file size, but also by ZIP structure integrity.
- Runtime logs use structured JSON logging and are written to `stderr`.
- Each CLI execution resumes synchronization based on SQLite state and local disk state.
- At runtime, permissions on `runtime/` are tightened to `0700`, and `runtime/app.db`, its `-wal/-shm` files, and `.download.lock` are tightened to `0600`.
