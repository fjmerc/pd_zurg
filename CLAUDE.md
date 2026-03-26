# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pd_zurg is a Docker container that orchestrates three services into a unified media streaming solution:
- **Zurg** — debrid WebDAV server (Real-Debrid, AllDebrid, TorBox)
- **rclone** — FUSE mount for the WebDAV endpoint
- **plex_debrid** — watchlist automation for Plex/Jellyfin

It enables streaming debrid libraries through media servers without local storage. Two workflow modes: watchlist automation (plex_debrid monitors Plex/Trakt/Overseerr) and blackhole mode (Sonarr/Radarr drop .torrent/.magnet files → debrid → symlinks).

## Build & Run

```bash
# Build the Docker image
docker build -t pd_zurg .

# Run with docker-compose (configure .env first from .env.example)
docker-compose up -d
```

The Dockerfile is a multi-stage Alpine build: copies rclone binary from `rclone:1.73.2`, installs Python 3.11 with venv, and runs `main.py` as entrypoint. Targets amd64, arm64, arm/v7.

## Testing

```bash
# Run all tests
pytest

# Run a single test file
pytest tests/test_blackhole.py

# Run with coverage
pytest --cov=utils --cov=base --cov-report=term-missing

# Run a specific test
pytest tests/test_blackhole.py::test_function_name -v
```

Test dependencies: `pip install -r requirements-dev.txt` (pytest, pytest-cov, pytest-mock).

Tests use three shared fixtures from `tests/conftest.py`: `tmp_dir` (temp directory), `env_vars` (set env vars via monkeypatch), `clean_env` (remove all pd_zurg env vars).

## Architecture

**Entry point:** `main.py` — signal handling (SIGTERM/SIGINT/SIGHUP/SIGCHLD), config validation, sequential service startup, blocking `signal.pause()` loop. Version is defined as a string literal in `main()`.

**Configuration:** `base/__init__.py` — `Config` class loads env vars with Docker secrets fallback (`/run/secrets/`). Exports ~61 module-level variables used throughout via `from base import *`. Supports SIGHUP reload via `refresh_globals()`.

**Process lifecycle:** `utils/processes.py` — Global process registry with auto-restart (exponential backoff: 5s→300s, max 5 retries, 1hr stability reset). Ordered shutdown (LIFO) with per-service timeouts.

**Service modules:**
- `zurg/setup.py` + `zurg/update.py` — Zurg config patching (token, port, creds in YAML) and GitHub release auto-update
- `rclone/rclone.py` — Generates rclone WebDAV config, manages FUSE mounts, supports dual-provider (RD + AD)
- `plex_debrid_/setup.py` + `plex_debrid_/update.py` — plex_debrid settings.json initialization and auto-update

**Key utilities (in `utils/`):**
- `blackhole.py` (largest module) — Watches `/watch` folder, submits to debrid API, polls status, creates symlinks in `/completed` when content appears on rclone mount. Includes release name parsing and local library dedup.
- `status_server.py` + `settings_api.py` + `settings_page.py` — Built-in HTTP dashboard (no framework) with process health, mount status, system metrics, and a browser-based settings editor with OAuth flows
- `config_validator.py` — Startup validation of API keys, URLs, feature conflicts
- `config_reload.py` — SIGHUP live reload handler
- `ffprobe_monitor.py` — Detects and kills stuck ffprobe processes on debrid mounts
- `notifications.py` — Apprise wrapper for 90+ notification services
- `file_utils.py` — Atomic write (write-to-temp-then-rename)

## Key Patterns

- Config values are accessed as module-level globals imported from `base` (e.g., `from base import RDAPIKEY, ZURG`). Boolean configs are stored as strings and compared with `str(VAR).lower() == 'true'`.
- Subprocess management uses `utils/processes.py` wrappers, not raw `subprocess.Popen`. All processes are registered for coordinated shutdown.
- File writes use `utils/file_utils.py` atomic write to prevent corruption.
- The status server is a raw `http.server.HTTPServer` — no Flask/Django.
- `plex_debrid/` is a git submodule (upstream code); `plex_debrid_/` contains the project's setup/update wrappers.
- **Sonarr and Radarr are parallel systems.** Any feature, fix, or API integration that applies to Sonarr (TV shows) almost certainly needs an equivalent for Radarr (movies). Always consider both when working on arr client code, symlink creation, library scanning, pending state, download/remove endpoints, and rescan triggers. If you implement something for one, implement it for the other in the same change.

## CI/CD

GitHub Actions (`.github/workflows/docker-image.yml`): triggers on push to master, daily cron, and manual dispatch. Builds multi-platform Docker images, pushes to Docker Hub + GHCR, creates GitHub releases from CHANGELOG.md, and posts Discord announcements.
