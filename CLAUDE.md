# CLAUDE.md

## Project Overview

pd_zurg is a Docker container orchestrating Zurg (debrid WebDAV), rclone (FUSE mount), and plex_debrid (watchlist automation) into a unified media streaming solution — no local storage needed.

Two workflow modes: **watchlist** (plex_debrid monitors Plex/Trakt/Overseerr) and **blackhole** (Sonarr/Radarr drop .torrent/.magnet files → debrid → symlinks).

## Commands

```bash
docker build -t pd_zurg .                  # Build image
docker-compose up -d                       # Run (configure .env from .env.example first)

.venv/bin/pytest                           # Run all tests (MUST use .venv — system Python lacks deps)
.venv/bin/pytest tests/test_blackhole.py   # Single file
.venv/bin/pytest tests/test_blackhole.py::test_name -v  # Single test
.venv/bin/pytest --cov=utils --cov=base --cov-report=term-missing  # Coverage
.venv/bin/pip install -r requirements-dev.txt  # Test deps (pytest, pytest-cov, pytest-mock)
```

Test fixtures in `tests/conftest.py`: `tmp_dir`, `env_vars` (monkeypatch), `clean_env` (strips all pd_zurg env vars).

## Architecture

- **Entry point:** `main.py` — signals (SIGTERM/SIGINT/SIGHUP/SIGCHLD), config validation, sequential service startup, blocking `signal.pause()` loop. Version is a string literal in `main()`.
- **Config:** `base/__init__.py` — `Config` class loads env vars with Docker secrets fallback (`/run/secrets/`). Exports module-level globals used everywhere via `from base import *`. SIGHUP reload via `refresh_globals()`.
- **Processes:** `utils/processes.py` — global registry with auto-restart (exponential backoff 5s→300s, max 5 retries, 1hr stability reset). LIFO shutdown with per-service timeouts.
- **Services:** `zurg/`, `rclone/`, `plex_debrid_/` each have setup, update, and download modules.
- **HTTP dashboard:** `utils/status_server.py` + `settings_api.py` + `settings_page.py` — raw `http.server.HTTPServer`, no framework.
- **Library scanner:** `utils/library.py` — `LibraryScanner` with split `_scan_read()` (read-only enumeration) and `_scan_effects()` (preference enforcement, arr searches, symlinks). `refresh()` updates the cache after the read phase so the UI gets data in seconds, then runs effects in the background. Tries WebDAV PROPFIND to Zurg directly (`utils/webdav.py`) before falling back to FUSE mount scanning.

## Gotchas and Key Patterns

- **Boolean configs are strings.** Always compare with `str(VAR).lower() == 'true'`, never use truthiness.
- **`plex_debrid/` vs `plex_debrid_/`:** The former is a git submodule (upstream code). The latter (with trailing underscore) contains this project's wrappers. Don't confuse them.
- **NEVER use raw `subprocess.Popen`.** Always use `utils/processes.py` wrappers so processes are registered for coordinated shutdown.
- **ALWAYS use `utils/file_utils.py` atomic write** for file writes to prevent corruption.
- **Sonarr and Radarr are parallel systems.** Any feature, fix, or API integration for one MUST have an equivalent for the other. This applies to arr client code, symlink creation, library scanning, pending state, download/remove endpoints, and rescan triggers. Always implement both in the same change.
- **`arr_client.py` and `webdav.py` use urllib only** — no requests/httpx dependency. Keep it that way.
- **Two separate symlink systems exist.** Blackhole completed-dir symlinks (`blackhole.py:_create_symlinks`) use original torrent folder names for arr import. Library debrid symlinks (`library.py:_create_debrid_symlinks`) use the arr's canonical folder name from API. Changes to symlink target construction, path translation, or verification must be checked against BOTH systems.
- **Title matching uses a 3-level cascade: exact → `_norm_for_matching` → TMDB ID fallback.** Most real titles require the TMDB ID fallback (e.g., torrent "F1 The Movie" → Radarr "F1"). This cascade appears in 4 places that must stay symmetric: movie dir selection, show dir selection, movie rescan trigger, show rescan trigger. Never add or change a level in one without updating all four.
- **`_normalize_title` ≠ `_norm_for_matching`.** `_normalize_title` is the TMDB cache key (lowercase + strip trailing year). `_norm_for_matching` is the arr fuzzy-match key (transliterate + strip punctuation + hyphens→spaces). They are NOT interchangeable. Changing either affects all downstream consumers.
- **Symlink targets use `BLACKHOLE_SYMLINK_TARGET_BASE`** (exists in arr/Plex containers, NOT in pd_zurg). Verification code in `verify_symlinks` and `_cleanup_symlinks` must translate targets back to `BLACKHOLE_RCLONE_MOUNT` before checking existence. These are inverse operations — if creation logic changes, verification must change identically.
- **`MEDIA_EXTENSIONS` is defined in three files** (`library.py`, `blackhole.py`, `scheduled_tasks.py`). These sets MUST be identical. A mismatch causes files visible to creation but invisible to verification or vice versa.

## Commit Checklist

- **ALWAYS update `CHANGELOG.md`** before committing. Add entries under the current unreleased version for any user-visible additions, changes, or fixes. Use the existing format: `- **Bold title**: Description`.

## CI/CD

GitHub Actions (`.github/workflows/docker-image.yml`): push to master, daily cron, manual dispatch. Multi-platform Docker images → Docker Hub + GHCR, GitHub releases from CHANGELOG.md, Discord announcements.
