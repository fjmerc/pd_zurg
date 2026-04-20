# Architecture Guide

Developer reference for pd_zurg internals. Complements [CLAUDE.md](CLAUDE.md) (rules, gotchas, commands) and [README.md](README.md) (user setup, env var reference). Use this document when implementing features to understand how the pieces connect, what data flows will be affected, and what side effects to expect.

---

## 1. System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│  pd_zurg container                                              │
│                                                                 │
│  main.py (PID 1)                                                │
│  ├── Zurg RD          (child process — WebDAV server)           │
│  ├── Zurg AD          (child process — WebDAV server, optional) │
│  ├── rclone           (child process — FUSE mount)              │
│  ├── plex_debrid      (child process — watchlist automation)    │
│  │                                                              │
│  ├── [thread] Process Monitor    (restart crashed children)     │
│  ├── [thread] Task Scheduler     (periodic tasks every 10s)     │
│  ├── [thread] Blackhole Watcher  (poll /watch for torrents)     │
│  ├── [thread] Settings Watcher   (poll settings.json)           │
│  ├── [thread] Status HTTP Server (dashboard + API)              │
│  ├── [thread] ffprobe Monitor    (kill stuck ffprobe)           │
│  └── [thread] Duplicate Cleanup  (Plex dedup, optional)         │
│                                                                 │
│  /data/{mount}/ ← rclone FUSE mount (debrid content)            │
│  /config/       ← persistent state files                        │
│  /watch/        ← blackhole input (torrent/magnet files)        │
│  /completed/    ← blackhole output (symlinks for arr import)    │
└─────────────────────────────────────────────────────────────────┘
```

### Startup Sequence (main.py)

```
1. Load config         base/__init__.py — env vars + Docker secrets
2. Run validation      config_validator.py — check required vars
3. Start status server status_server.py — HTTP dashboard + API
4. Init subsystems     history, blocklist, notifications
5. Zurg setup          zurg/setup.py — generate config, start process
6. rclone setup        rclone/rclone.py — generate config, wait for Zurg, mount
7. Duplicate cleanup   duplicate_cleanup.py — Plex dedup (if enabled)
8. plex_debrid setup   plex_debrid_/setup.py — configure, start process
9. Blackhole setup     blackhole.py — start watcher thread (if enabled)
10. ffprobe monitor    ffprobe_monitor.py — start monitor thread
11. Process monitor    processes.py — watch child processes for crashes
12. Settings watcher   settings_watcher.py — poll settings.json for changes
13. Register tasks     scheduled_tasks.py — register all periodic tasks
14. Start scheduler    task_scheduler.py — begin periodic execution
15. while True:        Block in signal.pause() loop, handle signals
      signal.pause()
```

### Shutdown Sequence

```
1. SIGTERM/SIGINT received → shutdown()
2. Stop task scheduler
3. Stop child processes in LIFO order (reverse of startup):
   plex_debrid (15s timeout) → rclone (10s) → Zurg (10s)
   SIGTERM first, SIGKILL after timeout
4. Unmount FUSE mounts under /data/
5. Send shutdown notification (5s timeout)
6. sys.exit(0)
```

---

## 2. Module Layers

```
┌─────────────────────────────────────────────────────────────┐
│  UI Layer                                                    │
│  status_server  settings_page  library_page  activity_page   │
│  system_page    settings_api   ui_common                     │
├─────────────────────────────────────────────────────────────┤
│  Integration Layer                                           │
│  arr_client (Sonarr/Radarr)   tmdb        search (Torrentio)│
│  debrid_client                 webdav      duplicate_cleanup │
├─────────────────────────────────────────────────────────────┤
│  Core Services                                               │
│  blackhole       library       library_prefs                 │
│  scheduled_tasks task_scheduler config_reload                │
├─────────────────────────────────────────────────────────────┤
│  Infrastructure                                              │
│  processes    notifications   history    blocklist            │
│  api_metrics  file_utils      quality_parser                 │
│  ffprobe_monitor  auto_update  network   config_validator    │
├─────────────────────────────────────────────────────────────┤
│  Foundation                                                  │
│  base/__init__.py (Config singleton, env vars, all imports)  │
│  utils/logger.py  (get_logger, SubprocessLogger)             │
├─────────────────────────────────────────────────────────────┤
│  External Processes                                          │
│  zurg/    (setup, update, download)                          │
│  rclone/  (setup, config generation)                         │
│  plex_debrid_/  (setup, update, download — project wrappers) │
│  plex_debrid/   (git submodule — upstream code, don't edit)  │
└─────────────────────────────────────────────────────────────┘
```

### Key Module Responsibilities

| Module | Owns | Key Exports |
|--------|------|-------------|
| `base/__init__.py` | Config singleton, env vars, global imports | `Config`, all `UPPERCASE` config vars, `refresh_globals()` |
| `utils/processes.py` | Child process lifecycle | `ProcessHandler`, `register_process()`, `shutdown_all_processes()` |
| `utils/blackhole.py` | Torrent submission + symlink creation | `setup()`, `stop()`, `BlackholeWatcher` class |
| `utils/library.py` | Mount + local library scanning | `get_scanner()`, `LibraryScanner` class |
| `utils/library_prefs.py` | Per-title source preferences and pending state | `set_preference()`, `get_all_preferences()`, `update_pending_error()`, `set_pending_warned()` |
| `utils/arr_client.py` | Sonarr/Radarr/Overseerr API | `SonarrClient`, `RadarrClient`, `OverseerrClient` |
| `utils/debrid_client.py` | Debrid provider APIs (RD/AD/TB) | `RealDebridClient`, `AllDebridClient`, `TorBoxClient` |
| `utils/search.py` | Torrentio search + debrid add | `search_torrents()`, `add_torrent_to_debrid()` |
| `utils/tmdb.py` | TMDB metadata + episode data | `search_show()`, `search_movie()`, `get_episodes()` |
| `utils/scheduled_tasks.py` | All periodic task implementations | `register_all()`, individual task functions |
| `utils/task_scheduler.py` | Task scheduling engine | `TaskScheduler` singleton (`scheduler`) |
| `utils/config_reload.py` | SIGHUP config reload | `handle_sighup()` |
| `utils/notifications.py` | Apprise notification dispatch | `init()`, `notify(event, title, body, level)` |
| `utils/history.py` | JSONL event audit log | `init()`, `log_event()`, `query()` |
| `utils/blocklist.py` | Torrent hash rejection | `init()`, `add()`, `is_blocked()`, `query()` |
| `utils/status_server.py` | HTTP server + status data | `setup()`, `status_data` singleton |
| `utils/settings_api.py` | Settings editor API + schema | `ENV_SCHEMA`, env read/write functions |
| `utils/webdav.py` | Direct WebDAV PROPFIND to Zurg | `webdav_list()` — used by library scanner before FUSE fallback |
| `utils/file_utils.py` | Atomic file writes | `atomic_write()` context manager |
| `utils/api_metrics.py` | Debrid API health tracking | `api_metrics` singleton, `tracked_request()` |

---

## 3. Process & Thread Model

### Child Processes (managed by `utils/processes.py`)

| Process | Binary | Restart Policy | Shutdown Timeout |
|---------|--------|----------------|------------------|
| Zurg RD | `/zurg/zurg` | 5 retries, backoff 5s→300s, 1hr stability reset | 10s |
| Zurg AD | `/zurg/zurg` | Same | 10s |
| rclone | `rclone mount` | Same | 10s |
| plex_debrid | `python plex_debrid` | Same | 15s |

### Threads

| Thread | Module | Daemon | Start | Stop |
|--------|--------|--------|-------|------|
| Process Monitor | `processes.py` | Yes | `start_process_monitor()` | Shutdown signal |
| Task Scheduler | `task_scheduler.py` | Yes | `scheduler.start()` | `scheduler.stop()` |
| Task Workers (N) | `task_scheduler.py` | Yes | Per-task execution | Task completion |
| Blackhole Watcher | `blackhole.py` | Yes | `setup()` | `stop()` |
| Settings Watcher | `settings_watcher.py` | Yes | `start()` | Shutdown |
| Status HTTP Server | `status_server.py` | Yes | `setup()` | Shutdown |
| ffprobe Monitor | `ffprobe_monitor.py` | Yes | `setup()` | Shutdown |
| Duplicate Cleanup | `duplicate_cleanup.py` | Yes | `setup()` | Shutdown |
| Notification Dispatch | `notifications.py` | — | Synchronous in caller's thread | — |
| Config Reload | `config_reload.py` | Yes | Per SIGHUP | Single-shot |

### Signal Handling

| Signal | Handler | Effect |
|--------|---------|--------|
| `SIGTERM` | `shutdown()` | Graceful shutdown (stop scheduler, LIFO process kill, unmount) |
| `SIGINT` | `shutdown()` | Same as SIGTERM |
| `SIGHUP` | `handle_sighup()` | Reload .env, diff changes, restart affected services |
| `SIGCHLD` | `SIG_IGN` | Auto-reap zombie children (no handler conflicts with Popen) |

### Authentication & Health

- **HTTP dashboard**: Protected by `STATUS_UI_AUTH` (Basic Auth, `user:password` format). Settings editor requires auth; read-only dashboard endpoints are accessible without it.
- **Docker HEALTHCHECK**: `Dockerfile` defines a `HEALTHCHECK` running `healthcheck.py` every 60s. Checks child process liveness (regex on cmdline), FUSE mount health (`os.path.ismount`), and status server responsiveness.

---

## 4. Data Flows

### 4.1 Blackhole Pipeline

```
Sonarr/Radarr drops .torrent/.magnet into /watch           (flat mode)
Sonarr drops into /watch/sonarr/, Radarr into /watch/radarr/ (labeled mode)
         │
         ▼
BlackholeWatcher._scan()
  ├─ Root files → label=None (flat mode)
  └─ Files in validated subdir → label=<subdir_name>  (see _is_valid_label)
         │
         ▼
BlackholeWatcher._process_file(file, label=…)
  ├─ Parse torrent: extract info_hash, name, file list
  ├─ Check blocklist: blocklist.is_blocked(hash) → skip if blocked
  ├─ Dedup check: scan local library if BLACKHOLE_DEDUP_ENABLED
  ├─ Submit to debrid API: _add_to_realdebrid() / _add_to_alldebrid() / _add_to_torbox()
  ├─ Record: history.log_event('grabbed', title)
  └─ Start pending monitor (label persisted in pending_monitors.json)
         │
         ▼
BlackholeWatcher._check_*_status() — poll loop
  ├─ Status: queued → downloading → downloaded
  ├─ Terminal errors: magnet_error, virus, dead → blocklist + notify
  └─ On "downloaded" → trigger symlink creation
         │
         ▼
BlackholeWatcher._create_symlinks(label=…)
  ├─ Poll mount: wait for files at /data/{mount}/category/release/
  │   (up to BLACKHOLE_MOUNT_POLL_TIMEOUT, default 300s)
  ├─ flat:    {COMPLETED_DIR}/release/file.mkv          → SYMLINK_TARGET_BASE/…
  ├─ labeled: {COMPLETED_DIR}/<label>/release/file.mkv  → SYMLINK_TARGET_BASE/…
  │   NOTE: Target path resolves in arr/Plex containers, NOT in pd_zurg
  ├─ Record: history.log_event('symlink_created')
  └─ Notify: notifications.notify('symlink_created')
         │
         ▼
Sonarr imports from /completed/sonarr/ (labeled) or /completed/ (flat)
Radarr imports from /completed/radarr/ (labeled) — each arr sees only its own items
  ├─ Moves/hardlinks to library folder
  ├─ Triggers: Plex library scan
  └─ Updates: episode/movie status in database
```

**Label routing invariant.** When the host directory layout includes label subdirs under `BLACKHOLE_DIR` (e.g. `sonarr/`, `radarr/`), pd_zurg routes each completed symlink into the same-named subdir under `BLACKHOLE_COMPLETED_DIR`. Each arr mounts only its own label subdir. Flat layout (no subdirs) remains fully supported. Any new consumer of `BLACKHOLE_COMPLETED_DIR` must use `utils.blackhole.iter_release_dirs()` rather than assuming top-level entries are release dirs.

### 4.2 Library Scan Cycle

```
Trigger: scheduled_tasks.library_scan() (default: every 1 hour)
   or:   Manual via WebUI "Scan Now" button
         │
         ▼
LibraryScanner.scan() — two-phase design
  │
  ├─ Phase 1: _scan_read()  [read-only, ~5-10 seconds]
  │   ├─ Try WebDAV PROPFIND to Zurg directly (fast, no FUSE overhead)
  │   ├─ Fallback: Walk FUSE mount /data/{mount}/
  │   ├─ Walk local library dirs (if configured)
  │   ├─ Parse folder names → titles, years, seasons, episodes
  │   ├─ Cross-reference: match debrid ↔ local by title
  │   ├─ Enrich: TMDB metadata (posters, episode counts, IMDb IDs)
  │   └─ Build: unified item list with source='debrid'|'local'|'both'
  │         │
  │         ▼
  │   refresh() updates cache here → UI gets data immediately
  │
  └─ Phase 2: _scan_effects()  [side effects, ~30-60 seconds]
      ├─ Enforce preferences: execute pending prefer-local/prefer-debrid transitions
      ├─ Search missing: trigger Sonarr/Radarr searches for missing episodes
      │   (records last_error, retry_count, next_retry_at on pending entries)
      ├─ Recover local fallback: re-route completed local-fallback downloads
      ├─ Clear resolved: remove pending entries whose target source arrived
      ├─ Escalate stuck: mark to-debrid → debrid-unavailable after threshold
      │   (DEBRID_UNAVAILABLE_THRESHOLD_DAYS, default 3)
      ├─ Warn stalled: send pending_warning notification for items pending 24h+
      │   (PENDING_WARNING_HOURS, default 24; set 0 to disable)
      ├─ Create debrid symlinks: organized symlinks in local library dirs
      │   (uses arr's canonical folder name, NOT torrent folder name)
      └─ Trigger rescans: Sonarr rescan_series() / Radarr rescan_movie()
              │
              ▼
         Cache updated → WebUI /api/library serves fresh data
         Arr rescans → Plex discovers new content
```

### 4.3 Quality Compromise State Machine

When `QUALITY_COMPROMISE_ENABLED=true`, the blackhole alt-retry loop runs a per-file state machine that escalates to a lower quality tier (within the arr's profile) after a dwell window at the preferred tier. State lives in the `<file>.meta` sidecar under a `tier_state` object; legacy sidecars without `tier_state` load as `None` and the engine short-circuits with `('stay', 'legacy_no_tier_state')` — fully backward compatible.

```
          ┌─────────────┐
          │   QUEUED    │  .torrent/.magnet landed, no attempt yet
          └──────┬──────┘
                 │ first attempt
                 ▼
          ┌─────────────┐
          │ PREFERRED   │  trying tier 0 (user's top preference from profile)
          │   TIER      │  RetryMeta.tier_state seeded: current_tier_index=0,
          └──────┬──────┘  first_attempted_at=now
                 │
    ┌────────────┼────────────┐
    │            │            │
    │ cached     │ uncached   │ dwell elapsed AND
    │ grab OK    │ alt list   │ all tier-0 alts failed
    │            │ exhausted  │ AND cached option
    │            │ (normal    │ exists at tier 1
    │            │  retry)    │ AND current_tier_index < max_tier_drop
    ▼            ▼            ▼
┌────────┐  ┌─────────┐  ┌──────────────┐
│ GRABBED│  │ WAITING │  │ COMPROMISED  │  tier_index=1
│ (tier 0)│  │ (tier 0)│  │ tier 1 cached│
└────────┘  └────┬────┘  │  grab push   │
                 │       └──────┬───────┘
                 │ dwell        │
                 │ keeps        ▼
                 │ counting┌─────────┐
                 └────────▶│ GRABBED │ (or: WAITING tier 1, then tier 2...)
                           │ (tier N)│
                           └─────────┘

Terminal states:
  GRABBED   — push_release succeeded; Sonarr's normal upgrade logic handles
              future replacement with the preferred tier (compromises are
              never permanent — critical semantic)
  FAILED    — all profile-permitted tiers exhausted, or max_tier_drop cap
              hit before a cached candidate appeared; notify + move to failed/
```

**Invariants** (enforced across `utils/quality_compromise.py`, `utils/blackhole.py::_try_compromise`, `utils/arr_client.py::get_tier_order`):

- **I1 — Profile is the ceiling.** `get_tier_order(profile_id)` reads the arr's allowed qualities from `/api/v3/qualityprofile/{id}`; the engine never grabs a tier absent from that list. `_filter_candidates` double-checks the returned tier label.
- **I2 — Monotonic downward movement.** `RetryMeta.advance_tier` refuses decrement or stay. `first_attempted_at` is never reset — alt-exhaustion writes go through `mark_alt_exhausted` which preserves `tier_state`.
- **I3 — Dwell before compromise.** `should_compromise` returns `('stay', 'dwell_not_elapsed')` until `now - first_attempted_at >= dwell_seconds`.
- **I4 — Cache-aware or don't compromise.** `only_cached=True` (default) treats `cached=None` as not cached. RD users post-Nov-2024 never compromise under strict mode (documented in `utils/search.py::check_debrid_cache`).
- **I5 — Sonarr/Radarr parity.** `get_quality_profile` / `get_tier_order` live on `_ArrClientBase`; `_try_compromise` handles both series and movie contexts in one call site.
- **I6 — Opt-in.** `QUALITY_COMPROMISE_ENABLED=false` (default) — `_try_compromise` returns False without running any decision logic.
- **I7 — Observable.** Every compromise writes a `compromise_grabbed` history event, annotates `pending_monitors.json`, and is queryable via `GET /api/blackhole/compromises` — independent of `QUALITY_COMPROMISE_NOTIFY`.

### `RetryMeta` v2 schema

The blackhole retry sidecar (`<file>.meta`) now carries an optional nested `tier_state` object. Legacy v1 entries (`{retries, last_attempt, alt_exhausted}` with no `tier_state`) are fully supported — `RetryMeta.read_tier_state` returns `None`, and the compromise decision loop degrades to `('stay', 'legacy_no_tier_state')`.

```json
{
  "retries": 2,
  "last_attempt": 1713664800.0,
  "alt_exhausted": true,

  "tier_state": {
    "schema_version": 2,
    "arr_service": "sonarr",
    "arr_url_hash": "a9f2c4",
    "profile_id": 4,
    "tier_order": ["2160p", "1080p", "720p"],
    "current_tier_index": 0,
    "first_attempted_at": 1713580000.0,
    "tier_attempts": [
      {
        "tier": "2160p",
        "tier_index": 0,
        "first_tried_at": 1713580000.0,
        "last_tried_at": 1713664800.0,
        "attempts": 3,
        "cached_hits_found": 0,
        "uncached_hits_found": 5,
        "outcome": "no_cached_alts_exhausted"
      }
    ],
    "compromise_fired_at": null,
    "last_advance_reason": null,
    "season_pack_attempted": false
  }
}
```

`arr_url_hash` is SHA-256 of the arr's base URL truncated to 6 hex chars — disambiguates state for users running multiple arr instances (e.g. `sonarr-4k` + `sonarr-hd`) without logging raw URLs. Writes route through `utils.file_utils.atomic_write` and a module-level `threading.RLock` serializes load-modify-save so the blackhole worker and alt-retry thread can't drop a tier advance.

### `pending_monitors.json` compromise annotation

Post-compromise entries carry four extra fields so the dashboard and library detail view can surface the event:

```json
{
  "torrent_id": "ABC123",
  "filename": "Show.S01E03.torrent",
  "service": "realdebrid",
  "timestamp": 1713664800.0,
  "label": "sonarr",
  "compromised": true,
  "preferred_tier": "2160p",
  "grabbed_tier": "1080p",
  "compromise_reason": "dwell_elapsed",
  "compromise_strategy": "tier_drop"
}
```

Legacy entries without these fields load with `compromised=False`.

### 4.4 Config Reload

```
docker kill -s HUP pd_zurg
  or: Settings editor "Save & Reload"
         │
         ▼
handle_sighup() → spawns background thread
         │
         ▼
_reload_env()
  ├─ Read /config/.env via dotenv_values()
  ├─ Diff against current os.environ
  ├─ Update os.environ with new values
  └─ Detect removed keys
         │
         ▼
_determine_restarts(changed_vars)
  ├─ Check SERVICE_DEPENDENCIES map (which vars affect which services)
  ├─ SOFT_RELOAD vars: only update in-memory values, no restart
  │   (log levels, notification settings, cleanup toggles)
  ├─ Dependency chain: zurg change → also restart rclone → also restart plex_debrid
  └─ Return: set of services needing restart
         │
         ▼
Restart affected services:
  ├─ Stop order:  plex_debrid → rclone → zurg (reverse dependency)
  ├─ Regenerate configs (zurg YAML, rclone config)
  ├─ Start order: zurg → rclone → plex_debrid (forward dependency)
  ├─ Non-process services: notifications.init(), blackhole.stop()/setup()
  └─ Notify: 'Config Reloaded' notification
```

### 4.5 Symlink Lifecycle

```
CREATION (two separate systems):

  Blackhole symlinks (blackhole.py:_create_symlinks)
  ├─ Target: original torrent/release folder name
  ├─ Purpose: arr import (Sonarr/Radarr pick up from /completed)
  ├─ Location (flat):    {COMPLETED_DIR}/release-name/file.mkv
  ├─ Location (labeled): {COMPLETED_DIR}/<label>/release-name/file.mkv
  │   Label scopes symlinks to one arr; enumerate via iter_release_dirs()
  └─ Cleanup: BLACKHOLE_SYMLINK_MAX_AGE (default 72h)

  Library debrid symlinks (library.py:_create_debrid_symlinks)
  ├─ Target: arr's canonical folder name from API
  ├─ Purpose: organized library structure for Plex
  ├─ Location: {LOCAL_LIBRARY_TV}/Show Name/Season XX/file.mkv
  └─ Cleanup: via verify_symlinks scheduled task

         │
         ▼
VERIFICATION (scheduled_tasks.verify_symlinks — default: every 6 hours)
  ├─ Walk library dirs for symlinks pointing to debrid mount
  ├─ Translate: BLACKHOLE_SYMLINK_TARGET_BASE → BLACKHOLE_RCLONE_MOUNT
  │   (symlink targets resolve in arr containers, not pd_zurg)
  ├─ Check: does the translated target exist on the FUSE mount?
  ├─ Mount health guard: abort if mount missing, empty, or categories empty
  │
  ├─ If broken → attempt REPAIR:
  │   ├─ Search mount for the release name under different categories
  │   ├─ If found: atomic symlink replacement (create tmp → rename)
  │   └─ If not found: DELETE symlink
  │       ├─ Clean up empty parent dirs (_cleanup_empty_parents)
  │       │   (prevents phantom local classification that blocks recreation)
  │       └─ If SYMLINK_REPAIR_AUTO_SEARCH=true: trigger arr re-search
  │
  └─ Result: history event + notification if repairs/deletions occurred

         │
         ▼
BLACKHOLE CLEANUP (blackhole.py:_cleanup_symlinks — runs periodically)
  ├─ Walk {COMPLETED_DIR} for release directories
  ├─ Remove broken symlinks within each release dir
  ├─ Remove entire dir if: no valid files remain OR aged out (>SYMLINK_MAX_AGE)
  └─ Log: "[blackhole] Cleaned up completed dir: {entry}"

         │
         ▼
HOUSEKEEPING (scheduled_tasks.housekeeping — default: every 24 hours)
  ├─ Clean stale pending state (library_pending.json entries older than N days)
  ├─ Remove empty directories in BLACKHOLE_COMPLETED_DIR
  └─ Clean old .meta.json retry files (>7 days)
```

---

## 5. Cross-Container Path Model

This is the most common source of confusion. Symlinks are created inside pd_zurg but must resolve inside other containers.

```
┌─────────────────────────────────────┐
│  pd_zurg container                  │
│                                     │
│  rclone FUSE mount:                 │
│  /data/pd_zurg/                     │
│    ├── movies/                      │
│    │   └── Movie.Name/file.mkv      │  ← actual file (via WebDAV → debrid CDN)
│    └── shows/                       │
│        └── Show.Name/S01/ep.mkv     │
│                                     │
│  Symlink created here:              │
│  /completed/Movie.Name/file.mkv     │
│    → /mnt/debrid/movies/Movie.Name/ │  ← points to TARGET_BASE path
│      file.mkv                       │     (does NOT resolve here!)
│                                     │
│  BLACKHOLE_RCLONE_MOUNT=/data/pd_zurg │
│  BLACKHOLE_SYMLINK_TARGET_BASE=     │
│    /mnt/debrid                      │
└─────────────────────────────────────┘
         │ Docker volume: ./mnt:/data:shared
         │ Docker volume: ./mnt:/rclone (Plex)
         │ Docker volume: ./mnt:/mnt/debrid (Sonarr/Radarr)
         ▼
┌─────────────────────────────────────┐
│  Sonarr / Radarr container          │
│                                     │
│  /mnt/debrid/pd_zurg/               │  ← same mount, different path
│    ├── movies/                      │
│    │   └── Movie.Name/file.mkv      │  ← symlink resolves HERE
│    └── shows/                       │
│                                     │
│  /completed/Movie.Name/file.mkv     │  ← reads symlink, follows to
│    → /mnt/debrid/movies/...         │     /mnt/debrid (exists!)
└─────────────────────────────────────┘

┌─────────────────────────────────────┐
│  Plex container                     │
│                                     │
│  /rclone/pd_zurg/                   │  ← same mount, yet another path
│    ├── movies/                      │
│    └── shows/                       │
│                                     │
│  Library points to: /rclone/pd_zurg │
│  Plex sees media files directly     │
└─────────────────────────────────────┘
```

### Path Translation in Code

When **creating** symlinks (blackhole.py, library.py):
```
target = BLACKHOLE_SYMLINK_TARGET_BASE + "/movies/Movie.Name/file.mkv"
       = /mnt/debrid/movies/Movie.Name/file.mkv
```

When **verifying** symlinks exist (scheduled_tasks.py, blackhole.py):
```
# Symlink target: /mnt/debrid/movies/Movie.Name/file.mkv
# Can't check that path — it doesn't exist in pd_zurg container!
# Translate back using anchored prefix check (NOT .replace()):
symlink_target_real = os.path.realpath(BLACKHOLE_SYMLINK_TARGET_BASE) + '/'
if target.startswith(symlink_target_real):
    check_path = BLACKHOLE_RCLONE_MOUNT + '/' + target[len(symlink_target_real):]
# = /data/pd_zurg/movies/Movie.Name/file.mkv
# Now check: os.path.exists(check_path)
```

**Key rule:** Creation and verification are inverse operations. If you change how targets are constructed, you must update verification to match.

---

## 6. State & Persistence

### Persistent Files (survive container restart)

| File | Format | Module | Purpose |
|------|--------|--------|---------|
| `/config/.env` | dotenv | `base`, `config_reload`, `settings_api` | All configuration |
| `/config/settings.json` | JSON | `plex_debrid_`, `settings_api`, `settings_watcher` | plex_debrid settings |
| `/config/rclone.config` | INI | `rclone/rclone.py` | rclone WebDAV mount endpoints |
| `/config/history.jsonl` | JSONL | `utils/history.py` | Event audit log (append-only) |
| `/config/blocklist.json` | JSON | `utils/blocklist.py` | Blocked torrent hashes + titles |
| `/config/library_prefs.json` | JSON | `utils/library_prefs.py` | Per-title prefer-local/prefer-debrid |
| `/config/library_pending.json` | JSON | `utils/library_prefs.py` | Pending preference transitions (direction, created, last_searched, episodes, last_error, retry_count, next_retry_at, warned_at) |
| `/config/tmdb_cache.json` | JSON | `utils/tmdb.py` | TMDB metadata cache (7-day TTL) |
| `/config/backups/{date}/` | Mixed | `scheduled_tasks.py` | Daily config backups (keep last 7) |
| `/zurg/RD/config.yml` | YAML | `zurg/setup.py` | Zurg RealDebrid config |
| `/zurg/AD/config.yml` | YAML | `zurg/setup.py` | Zurg AllDebrid config |
| `{COMPLETED_DIR}/pending_monitors.json` | JSON | `utils/blackhole.py` | Active torrent monitors (survives restart). Entries: `{torrent_id, filename, service, timestamp, label?}` — `label` is present when the torrent was submitted via a per-arr label subdir; legacy entries without `label` are treated as flat-mode |
| `{BLACKHOLE_DIR}/*.meta.json` | JSON | `utils/blackhole.py` | Per-torrent retry metadata |

### In-Memory Singletons (lost on restart)

| Singleton | Module | Thread-Safe | Purpose |
|-----------|--------|-------------|---------|
| `config` | `base/__init__.py` | Read-mostly | Config values from env |
| `status_data` | `utils/status_server.py` | Lock | Event buffer, service status, process health |
| `_process_registry` | `utils/processes.py` | Lock | All tracked child processes |
| `scanner._cache` | `utils/library.py` | Lock | Last scan results (powers /api/library) |
| `api_metrics` | `utils/api_metrics.py` | Lock | Per-provider API call stats |
| `scheduler._tasks` | `utils/task_scheduler.py` | Lock | Registered tasks and execution state |
| `_notifier` | `utils/notifications.py` | Lock | Apprise instance + config |

---

## 7. External Service Map

| Service | Module(s) | Protocol | Credentials | Failure Behavior |
|---------|-----------|----------|-------------|-----------------|
| **Real-Debrid** | `blackhole`, `debrid_client`, `search` | HTTPS REST | `RD_API_KEY` | Retry with backoff; blocklist on terminal errors |
| **AllDebrid** | `blackhole`, `debrid_client`, `search` | HTTPS REST | `AD_API_KEY` | Same as RD |
| **TorBox** | `blackhole`, `debrid_client`, `search` | HTTPS REST | `TORBOX_API_KEY` | Same as RD |
| **Sonarr** | `arr_client`, `scheduled_tasks` | HTTP REST (v3) | `SONARR_URL` + `SONARR_API_KEY` | Skip operations; log warnings |
| **Radarr** | `arr_client`, `scheduled_tasks` | HTTP REST (v3) | `RADARR_URL` + `RADARR_API_KEY` | Skip operations; log warnings |
| **Overseerr** | `arr_client` | HTTP REST (v1) | `SEERR_ADDRESS` + `SEERR_API_KEY` | Fallback source; non-critical |
| **Plex** | `duplicate_cleanup`, `plex_debrid` | HTTP (PlexAPI) | `PLEX_ADDRESS` + `PLEX_TOKEN` | Dedup disabled; plex_debrid crashes/restarts |
| **Jellyfin** | `plex_debrid` (submodule) | HTTP REST | `JF_ADDRESS` + `JF_API_KEY` | plex_debrid crashes/restarts |
| **TMDB** | `utils/tmdb.py` | HTTPS REST (v3) | `TMDB_API_KEY` | Posters/metadata unavailable; library scan still works |
| **Torrentio** | `utils/search.py` | HTTPS REST | `TORRENTIO_URL` (no key) | Search unavailable; non-critical |
| **GitHub** | `utils/download.py`, `zurg/download.py` | HTTPS REST | `GITHUB_TOKEN` (optional) | Auto-update disabled |
| **Apprise targets** | `utils/notifications.py` | Various (90+ protocols) | `NOTIFICATION_URL` | Notifications silently fail |
| **Zurg WebDAV** | `rclone/rclone.py`, `utils/webdav.py` | HTTP WebDAV | `ZURG_USER` + `ZURG_PASS` (optional) | Library scanner falls back to FUSE mount |

### Credential Safety

- Docker secrets supported: mount file at `/run/secrets/{name}`, omit env var
- Logs mask sensitive values: `_safe_log_url()` in `search.py` strips query params
- Config reload masks: keys containing `KEY`, `TOKEN`, `PASS`, `SECRET`
- **Never** log full URLs containing debrid API keys

---

## 8. Scheduled Tasks

All tasks registered in `scheduled_tasks.register_all()`, executed by `task_scheduler.TaskScheduler`.

| Task | Default Interval | Initial Delay | Prerequisites | Side Effects |
|------|-----------------|---------------|---------------|-------------|
| `audit_download_routing` | 6h | 5min | Blackhole + (Sonarr or Radarr) | Modifies arr download client tags, indexer routing |
| `clean_stale_queue` | 15min | 2min | Blackhole + (Sonarr or Radarr) | Deletes stale queue items from arr |
| `detect_stale_grabs` | 15min | 10min | Blackhole + (Sonarr or Radarr) | Re-triggers searches for silently failed grabs |
| `library_scan` | 1h | 2min | Status UI enabled | Enforces preferences, escalates stuck pending, warns stalled (24h+), creates symlinks, triggers arr rescans, TMDB API calls |
| `verify_symlinks` | 6h | 10min | Blackhole symlinks enabled | Deletes/repairs broken symlinks, cleans empty dirs, optionally triggers arr re-search |
| `enforce_preferences` | 6h | 6h | Status UI + `LIBRARY_PREFERENCE_AUTO_ENFORCE` | Deletes debrid torrents or local files based on preferences |
| `housekeeping` | 24h | 1h | Always | Cleans stale pending state, empty dirs, old .meta files, rotates history.jsonl |
| `config_backup` | 24h | 5min | Always | Copies .env, settings, preferences, blocklist, history to /config/backups/, prunes >7 |
| `mount_liveness` | 1min | 1min | rclone configured | Probes FUSE mount; logs warnings on slow response; alerts on local library mount drops |
| `notification_digest` | 24h | Until configured time | Digest enabled + notification URL | Sends daily summary notification |

### Task Interactions

```
library_scan
  ├─ triggers → arr rescan (Sonarr/Radarr API)
  ├─ creates → debrid symlinks (library dir)
  └─ reads → TMDB cache (may trigger API calls if stale)

verify_symlinks
  ├─ deletes → broken symlinks
  ├─ calls → _cleanup_empty_parents (prevents phantom local classification)
  └─ optionally triggers → arr re-search (SYMLINK_REPAIR_AUTO_SEARCH)

enforce_preferences
  ├─ reads → library_prefs.json
  ├─ calls → debrid_client.delete_torrent() (prefer-local)
  └─ calls → arr_client for downloads (prefer-debrid)

housekeeping
  ├─ cleans → library_pending.json stale entries
  ├─ cleans → empty dirs in COMPLETED_DIR
  ├─ cleans → old .meta.json in BLACKHOLE_DIR
  └─ rotates → history.jsonl (prunes events older than HISTORY_RETENTION_DAYS)
```

---

## 9. Error Recovery Patterns

### Process Auto-Restart

```
Child process crashes → Process Monitor detects (poll loop)
  ├─ Restart attempt 1: wait 5s
  ├─ Restart attempt 2: wait 15s
  ├─ Restart attempt 3: wait 45s
  ├─ Restart attempt 4: wait 120s
  ├─ Restart attempt 5: wait 300s
  └─ Max retries exhausted → notify('health_error'), stop retrying
      After 1 hour of stability → retry counter resets to 0
```

### Blackhole Retry Policy

```
Torrent submission fails → retry with schedule:
  ├─ Retry 1: 5 minutes
  ├─ Retry 2: 15 minutes
  ├─ Retry 3: 1 hour
  └─ Max retries (3) → give up, log error
      Retry state persisted in .meta.json sidecar files
```

### Symlink Mount Health Guard

```
verify_symlinks() and _cleanup_broken_debrid_symlinks() guard against
mass deletion by checking mount health before processing:
  1. Mount directory must exist (os.path.isdir)
  2. Mount must be non-empty (os.listdir returns entries)
  3. At least one category dir must have content (deeper check)
  If any check fails → ABORT, log warning, return error status
  Purpose: prevent nuking entire library on temporary mount failure
```

### Blocklist Auto-Add

```
Debrid returns terminal error (magnet_error, virus, dead, etc.):
  If BLOCKLIST_AUTO_ADD=true (default):
    → blocklist.add(info_hash, title, reason)
    → Future submissions with same hash are silently skipped
    → Visible in WebUI and via /api/blocklist
```

### Pending Item Escalation and Warnings

```
_escalate_stuck_pending() runs every library_scan (default: 1h):
  ├─ Check: to-debrid entries older than DEBRID_UNAVAILABLE_THRESHOLD_DAYS (default: 3)
  ├─ Escalate: direction → 'debrid-unavailable' (stops automatic retries)
  ├─ Notify: 'debrid_unavailable' event with list of escalated titles
  └─ History: log event per title

_warn_stalled_pending() runs after escalation in same scan:
  ├─ Check: to-debrid entries older than PENDING_WARNING_HOURS (default: 24; 0=disabled)
  ├─ Guard: skip if warned_at already set (one notification per item)
  ├─ Notify: 'pending_warning' event with last_error context
  └─ Track: set warned_at timestamp to prevent repeats
```

### Blackhole Alt-Exhaustion

```
_try_alternative_release() on all alternatives failed:
  ├─ Move original file → failed/ directory
  ├─ Notify: 'download_error' — "All Alternatives Failed"
  └─ History: log 'failed' event with detail
```

### Mount Liveness Probe

```
mount_liveness_probe() runs every 60s:
  ├─ Check: os.path.ismount(BLACKHOLE_RCLONE_MOUNT)
  ├─ Check: os.listdir() completes within 5s
  ├─ If slow (>5s): log warning, report to status_data
  ├─ If unresponsive: log error, report to status_data
  └─ Local library health: detect NFS/SMB mount drops
      Alert once per incident, reset on recovery
```

---

## 10. Adding a New Feature — Checklist

When implementing a new feature, use this checklist alongside the [CLAUDE.md gotchas](CLAUDE.md#gotchas-and-key-patterns):

1. **Identify affected layers** — Which modules in Section 2 will you touch?
2. **Trace data flows** — Which flows in Section 4 are affected? What side effects?
3. **Check path model** — If touching symlinks or file paths, review Section 5
4. **State files** — Adding persistent state? Use `file_utils.atomic_write()` and add to Section 6
5. **External APIs** — New API dependency? Add to Section 7 with failure behavior
6. **Scheduled tasks** — New periodic operation? Register in `scheduled_tasks.register_all()`
7. **Sonarr/Radarr symmetry** — Anything for one arr must have an equivalent for the other
8. **Notifications** — New event? Add to `ALL_EVENTS` in `notifications.py` AND `NOTIFICATION_EVENTS` help in `settings_api.py`
9. **Thread safety** — Accessing shared state? Use the appropriate lock (see Section 6)
10. **Boolean configs** — Compare with `str(VAR).lower() == 'true'`, never truthiness
