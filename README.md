# Zurgarr

Stream your Real-Debrid library through Plex or Jellyfin — one container, zero local storage.

![Build Status](https://img.shields.io/github/actions/workflow/status/fjmerc/zurgarr/docker-image.yml)

## What is Zurgarr?

Zurgarr packages three tools into a single Docker container: **[Zurg](https://github.com/debridmediamanager/zurg-testing)** (connects to your debrid account and serves files via WebDAV), **[rclone](https://github.com/rclone/rclone)** (mounts those files as a local directory), and optionally **[plex_debrid](https://github.com/itsToggle/plex_debrid)** (automates content discovery from your watchlists). Your media server sees the debrid library as local files and streams them on demand — no downloading, no local storage needed.

~150MB Alpine image. 3 services. That's it.

## Why This Project?

> [!NOTE]
> Zurgarr started life as a fork of [pd_zurg](https://github.com/I-am-PUID-0/pd_zurg) by [I-am-PUID-0](https://github.com/I-am-PUID-0), which was deprecated in favor of [DUMB](https://github.com/I-am-PUID-0/DUMB). After substantial divergence (new subsystems, ~22k lines of tests, full architecture docs, and a different feature direction) the project was renamed to Zurgarr to fit the *arr ecosystem naming convention and signal that it's its own thing now. The MIT-licensed lineage is preserved with full attribution.

**What Zurgarr adds over the original pd_zurg:**

- **Process auto-restart** — crashed services restart with exponential backoff (5s → 300s), resets after 1 hour of stability
- **Blackhole watch folder** — Sonarr/Radarr drop `.torrent`/`.magnet` files, Zurgarr sends them to debrid and creates symlinks when ready. Supports **per-arr label routing** so Sonarr and Radarr never see each other's items. See the [Blackhole Symlink Guide](BLACKHOLE_SYMLINK_GUIDE.md)
- **Local library dedup** — checks your existing library before submitting to debrid to avoid duplicates
- **Notifications** — 90+ services via [Apprise](https://github.com/caronc/apprise) (Discord, Telegram, Slack, email, etc.)
- **Status dashboard** — process health, mount status, system resources, and a browser-based settings editor
- **Library browser** — browse your combined debrid + local library with TMDB metadata, source preference management, episode-level download/switch controls, and interactive debrid torrent search with cache status
- **Auto debrid symlinks** — automatically creates organized symlinks in your local library for debrid-only content so Sonarr/Radarr can discover it, with automatic rescan triggers
- **ffprobe recovery** — detects and kills stuck ffprobe processes on debrid mounts
- **MDBList integration** — subscribe to curated lists that auto-feed plex_debrid
- **Atomic config writes** and **ordered shutdown** for reliability

## How It Works

Zurgarr supports two workflows. You can use either or both.

**Watchlist Flow** — plex_debrid monitors your watchlists automatically:

```
Watchlist (Plex / Trakt / Overseerr)
  → plex_debrid (search & match)
    → Real-Debrid (cloud cache)
      → Zurg (WebDAV) → rclone (/data mount)
        → Plex / Jellyfin (stream)
```

**Arr + Blackhole Flow** — Sonarr/Radarr with tag-based routing:

```
Overseerr (requests) → Sonarr / Radarr (tag-based routing)
  │
  ├─ Local path (no debrid tag):
  │    VPN → qBittorrent / Usenet → Local Disk → Plex
  │
  └─ Debrid path (tag: debrid — no VPN needed):
       Blackhole (/watch)
         → Zurgarr (submit to Real-Debrid)
           → Zurg / rclone (mount)
             → Symlinks (/completed)
               → Sonarr / Radarr (import) → Plex (stream)
```

## Quick Start

### Prerequisites

- A **Linux Docker host** (not Docker Desktop — it lacks [mount propagation](https://docs.docker.com/storage/bind-mounts/#configure-bind-propagation) support. See the [upstream pd_zurg wiki](https://github.com/I-am-PUID-0/pd_zurg/wiki/Setup-Guides) for WSL2 alternatives on Windows — most setup notes still apply)
- A [Real-Debrid](https://real-debrid.com/apitoken), [AllDebrid](https://alldebrid.com/apikeys/), or [TorBox](https://torbox.app/settings) account with an API key
- FUSE support on the host (`/dev/fuse`)

### 1. Build the image

```bash
docker build -t zurgarr https://github.com/fjmerc/zurgarr.git
```

### 2. Configure

```bash
# Download the example config and compose file
wget https://raw.githubusercontent.com/fjmerc/zurgarr/master/.env.example -O .env
wget https://raw.githubusercontent.com/fjmerc/zurgarr/master/docker-compose.yml

# Edit — at minimum set these:
#   RD_API_KEY        (your debrid API key)
#   STATUS_UI_AUTH    (e.g., admin:yourpassword)
nano .env
```

The [`.env.example`](.env.example) is fully commented — every setting is documented inline.

### 3. Create directories

```bash
mkdir -p config log cache mnt RD
```

### 4. Start

```bash
docker compose up -d
```

### 5. Verify

- Open the status dashboard at `http://your-host:8080/status`
- Check that Zurg and rclone show as **Running**
- Verify the mount: `ls mnt/zurgarr/` should show your debrid library categories

## Choose Your Workflow

### Option A: Watchlist Mode (plex_debrid)

Best if you want fully automated content from Plex watchlists, Trakt lists, or Overseerr requests — no Sonarr/Radarr needed.

**Enable in `.env`:**

```bash
PD_ENABLED=true
PLEX_USER=your_plex_username
PLEX_TOKEN=your_plex_token
PLEX_ADDRESS=http://192.168.1.100:32400

# Optional: auto-refresh Plex library when new content appears
PLEX_REFRESH=true
PLEX_MOUNT_DIR=/zurgarr

# Optional: Overseerr integration
SEERR_API_KEY=your_key
SEERR_ADDRESS=http://overseerr:5055
```

For **Jellyfin/Emby**, use `JF_ADDRESS` and `JF_API_KEY` instead of the Plex variables. Note: Jellyfin requires [additional plex_debrid setup](https://github.com/itsToggle/plex_debrid#open_file_folder-library-collection-service) for Trakt Collections.

### Option B: Arr + Blackhole Mode (Sonarr/Radarr)

Best if you already use Sonarr/Radarr and want debrid as a download client alongside (or instead of) qBittorrent/Usenet.

**Enable in `.env`:**

```bash
PD_ENABLED=false                # Not needed — Sonarr/Radarr handle discovery
BLACKHOLE_ENABLED=true
BLACKHOLE_SYMLINK_ENABLED=true
BLACKHOLE_RCLONE_MOUNT=/data/zurgarr
BLACKHOLE_SYMLINK_TARGET_BASE=/mnt/debrid   # Path as seen by Plex/Sonarr/Radarr host(s)

# Optional: skip content you already have
BLACKHOLE_DEDUP_ENABLED=true
BLACKHOLE_LOCAL_LIBRARY_TV=/data/media/tv
BLACKHOLE_LOCAL_LIBRARY_MOVIES=/data/media/movies
```

**Add volumes** in `docker-compose.yml`:

```yaml
volumes:
  - /opt/blackhole:/watch            # Sonarr/Radarr drop .torrent files here
  - /opt/completed:/completed        # Zurgarr creates symlinks here
  # Local library (read-write — needed for auto debrid symlinks):
  - /path/to/library/tv:/data/media/tv
  - /path/to/library/movies:/data/media/movies
```

See the **[Blackhole Symlink Guide](BLACKHOLE_SYMLINK_GUIDE.md)** for complete setup including Sonarr/Radarr download client configuration, multi-host NFS, verification steps, and troubleshooting.

#### Quality compromise & season-pack fallback

When a strict Sonarr/Radarr profile (e.g. "2160p REMUX only") turns up no cached release on your debrid provider, Zurgarr's default behavior is to cycle through the arr's alternatives at the same tier and eventually move the torrent to `failed/`. The quality-compromise engine is an **opt-in** escalation path that, after a configurable dwell window, probes one tier below the preferred tier within the same profile and grabs the best cached release there — so an uncached 2160p with a perfectly cached 1080p available doesn't leave the episode missing. The arr's profile is always the ceiling: Zurgarr never grabs a tier the profile doesn't permit, even after dwell, even if cached. When the preferred tier later appears on debrid, Sonarr/Radarr's normal upgrade logic reclaims it — compromises are not permanent.

Flow: set `QUALITY_COMPROMISE_ENABLED=true`, wait `QUALITY_COMPROMISE_DWELL_DAYS` (default 3) at the preferred tier, then the engine probes the next allowed tier with `QUALITY_COMPROMISE_ONLY_CACHED=true` (default, safest — unknown/not-cached releases are refused) and grabs the best cached candidate. Every compromise fires a `compromise_grabbed` history event, annotates the dashboard, and is surfaced via `GET /api/blackhole/compromises`. Setting `QUALITY_COMPROMISE_NOTIFY=false` silences Apprise but keeps the dashboard trail. Opt-in season-pack fallback (`SEASON_PACK_FALLBACK_ENABLED=true`) probes a cached pack at the **preferred** tier before any tier drop for shows with many holes. See the [Blackhole Symlink Guide](BLACKHOLE_SYMLINK_GUIDE.md#smart-quality-compromise) for full setup, the rollback path, and debrid-provider caveats (Real-Debrid's deprecated cache endpoint).

### Option C: Both

You can run plex_debrid and blackhole simultaneously. Set `PD_ENABLED=true` and `BLACKHOLE_ENABLED=true`. Use tags in Sonarr/Radarr to route specific content through the blackhole while plex_debrid handles watchlist items.

## Docker Compose

> [!NOTE]
> These examples are starting points. Adjust paths to match your environment.

### Base Setup

```yaml
services:
  zurgarr:
    container_name: zurgarr
    image: zurgarr:latest
    stdin_open: true
    tty: true
    env_file: .env
    volumes:
      - ./config:/config
      - ./log:/log
      - ./cache:/cache
      - ./RD:/zurg/RD
      # - ./AD:/zurg/AD             # Uncomment for AllDebrid
      - ./mnt:/data:shared
      ## Uncomment for blackhole mode:
      # - /opt/blackhole:/watch
      # - /opt/completed:/completed
      ## Uncomment for local library (read-write for auto debrid symlinks):
      # - /path/to/library/tv:/data/media/tv
      # - /path/to/library/movies:/data/media/movies
    ports:
      - "8080:8080"                  # Status UI
    devices:
      - /dev/fuse:/dev/fuse:rwm
    cap_add:
      - SYS_ADMIN
    security_opt:
      - apparmor:unconfined
      - no-new-privileges
```

### Plex Companion

The Plex container should wait for Zurgarr's mount to be ready:

```yaml
  plex:
    image: plexinc/pms-docker:latest
    container_name: plex
    devices:
      - /dev/dri:/dev/dri
    volumes:
      - /path/to/plex/config:/config
      - /path/to/plex/transcode:/transcode
      - ./mnt:/rclone               # rclone mount from Zurgarr — add to Plex library
    environment:
      - TZ=America/New_York
    ports:
      - "32400:32400"
    depends_on:
      zurgarr:
        condition: service_healthy
```

## Features

| Feature | Description |
|---------|-------------|
| **Process auto-restart** | Crashed processes restart with exponential backoff (5s → 300s). Resets after 1 hour of stability. Max 5 retries. |
| **Blackhole + symlinks** | Sonarr/Radarr integration via watch folder. Creates symlinks to debrid content — zero-copy, no local storage. [Guide](BLACKHOLE_SYMLINK_GUIDE.md) |
| **Local library dedup** | Checks your existing TV/movie library before sending torrents to debrid. Avoids duplicate downloads. |
| **Notifications** | 90+ services via [Apprise](https://github.com/caronc/apprise). Events: startup, shutdown, mount, cleanup, errors. |
| **Status dashboard** | Process health, mount status, system resources at `/status`. Auto-refreshes. JSON API at `/api/status`. |
| **Settings editor** | Browser-based config at `/settings`. Edit env vars, plex_debrid settings, run OAuth flows — no SSH needed. |
| **MDBList** | Subscribe to curated lists (IMDB Top 250, trending, genre lists) that auto-feed plex_debrid. |
| **ffprobe recovery** | Detects stuck ffprobe processes on debrid mounts. Recovers or kills after 3 failed attempts. |
| **Cross-machine setup** | Expose Zurg's WebDAV port and mount from any machine via rclone. Simpler than NFS. |
| **Atomic config writes** | Write-to-temp-then-rename prevents corruption on mid-write container kills. |
| **Ordered shutdown** | Per-process timeouts with elapsed time logging. |
| **Duplicate cleanup** | Automated Plex duplicate detection with configurable keep policy (local vs Zurg copy). |

## Web UI & Settings Editor

The **status dashboard** at `/status` shows:
- Process health (Zurg, rclone, plex_debrid) with uptime
- Mount status and disk usage
- System resources (cgroup-aware for containers)
- Recent events and filtered log viewer

The **settings editor** at `/settings` provides:
- **Zurgarr tab** — edit all environment variables with toggles, dropdowns, password fields, inline validation, and SIGHUP reload (no restart needed)
- **plex_debrid tab** — edit settings.json with multi-select pickers, list editors, and quality profile JSON editor
- **OAuth tab** — connect Trakt, Debrid Link, Put.io, and Orionoid via device code flow
- **Import/Export** — download or upload settings for backup and migration

Requires `STATUS_UI_AUTH` (e.g., `admin:changeme`). The settings editor is not accessible without authentication.

## Configuration Reference

All settings are documented in [`.env.example`](.env.example) with inline comments. The grouped tables below provide additional context.

<details>
<summary><strong>Core — Zurg & rclone (always required)</strong></summary>

| Variable | Description | Default |
|----------|-------------|---------|
| `TZ` | [Timezone](http://en.wikipedia.org/wiki/List_of_tz_database_time_zones) | |
| `ZURG_ENABLED` | Enable Zurg | `false` |
| `RD_API_KEY` | [Real-Debrid API key](https://real-debrid.com/apitoken) | |
| `AD_API_KEY` | [AllDebrid API key](https://alldebrid.com/apikeys/) (alternative to RD) | |
| `TORBOX_API_KEY` | [TorBox API key](https://torbox.app/settings) (alternative to RD) | |
| `RCLONE_MOUNT_NAME` | Name for the rclone mount | |
| `RCLONE_LOG_LEVEL` | [Log level](https://rclone.org/docs/#log-level-level) for rclone. Set to `OFF` to suppress | `NOTICE` |
| `RCLONE_DIR_CACHE_TIME` | [Directory cache duration](https://rclone.org/commands/rclone_mount/#vfs-directory-cache) | `10s` |
| `RCLONE_CACHE_DIR` | [Cache directory](https://rclone.org/docs/#cache-dir-dir) | |
| `RCLONE_VFS_CACHE_MODE` | [VFS cache mode](https://rclone.org/commands/rclone_mount/#vfs-file-caching) | `off` (FUSE) / `full` (NFS) |
| `RCLONE_VFS_CACHE_MAX_SIZE` | Max VFS cache size | |
| `RCLONE_VFS_CACHE_MAX_AGE` | Max VFS cache age | |
| `RCLONE_VFS_READ_CHUNK_SIZE` | Initial read chunk size | |
| `RCLONE_VFS_READ_CHUNK_SIZE_LIMIT` | Max read chunk size | |
| `RCLONE_BUFFER_SIZE` | Buffer size for transfers | |
| `RCLONE_TRANSFERS` | Number of parallel transfers | |
| `ZURG_VERSION` | Pin Zurg version (e.g., `v0.9.2-hotfix.4`) or `nightly` (requires `GITHUB_TOKEN`) | `latest` |
| `ZURG_UPDATE` | Auto-update Zurg on startup | `false` |
| `ZURG_LOG_LEVEL` | Zurg log level. Set to `OFF` to suppress | `INFO` |
| `ZURG_USER` | WebDAV basic auth username | |
| `ZURG_PASS` | WebDAV basic auth password | |
| `ZURG_PORT` | WebDAV port. **Set a fixed port** if exposing to other machines | random |
| `NFS_ENABLED` | Enable rclone NFS server. **Warning:** does NOT create a local mount — use FUSE mode if Plex is on the same machine | `false` |
| `NFS_PORT` | NFS server port | random |

</details>

<details>
<summary><strong>plex_debrid — Watchlist automation</strong></summary>

| Variable | Description | Default |
|----------|-------------|---------|
| `PD_ENABLED` | Enable plex_debrid | `false` |
| `PLEX_USER` | [Plex username](https://app.plex.tv/desktop/#!/settings/account) | |
| `PLEX_TOKEN` | [Plex token](https://support.plex.tv/articles/204059436-finding-an-authentication-token-x-plex-token/) | |
| `PLEX_ADDRESS` | Plex server URL (e.g., `http://192.168.1.100:32400`). Must include `http://` or `https://`, no trailing `/` | |
| `JF_ADDRESS` | Jellyfin/Emby URL (alternative to Plex) | |
| `JF_API_KEY` | Jellyfin/Emby API key | |
| `SEERR_API_KEY` | Overseerr/Jellyseerr API key | |
| `SEERR_ADDRESS` | Overseerr/Jellyseerr URL | |
| `SHOW_MENU` | Show plex_debrid interactive menu on startup | `true` |
| `PD_UPDATE` | Auto-update plex_debrid. Requires `PD_REPO` | `false` |
| `PD_REPO` | Update repository: `user,repo,branch` (e.g., `itsToggle,plex_debrid,main`) | |
| `PD_LOG_LEVEL` | Log level (`DEBUG`, `INFO`, or `OFF`) | `INFO` |
| `TRAKT_CLIENT_ID` | Trakt API client ID (uses itsToggle's default if unset) | |
| `TRAKT_CLIENT_SECRET` | Trakt API client secret | |
| `FLARESOLVERR_URL` | [FlareSolverr](https://github.com/FlareSolverr/FlareSolverr) URL for Cloudflare-protected indexers | |

</details>

<details>
<summary><strong>Plex Library Management</strong></summary>

| Variable | Description | Default |
|----------|-------------|---------|
| `PLEX_REFRESH` | Auto-refresh Plex libraries after mount changes | `false` |
| `PLEX_MOUNT_DIR` | Mount path as Plex sees it (for library refresh) | |
| `DUPLICATE_CLEANUP` | Automated Plex duplicate detection and cleanup | `false` |
| `CLEANUP_INTERVAL` | Hours between duplicate cleanup runs | `24` |
| `DUPLICATE_CLEANUP_KEEP` | Which copy to keep: `local` (logs Zurg dupes) or `zurg` (deletes local copy) | `local` |
| `AUTO_UPDATE_INTERVAL` | Hours between auto-update checks | `24` |

</details>

<details>
<summary><strong>Blackhole — Sonarr/Radarr integration</strong></summary>

| Variable | Description | Default |
|----------|-------------|---------|
| `BLACKHOLE_ENABLED` | Enable blackhole watch folder | `false` |
| `BLACKHOLE_DIR` | Watch directory for `.torrent`/`.magnet` files. Supports **per-arr label subdirs** (e.g. `BLACKHOLE_DIR/sonarr/…`, `BLACKHOLE_DIR/radarr/…`) so each arr sees only its own items — see the [Blackhole Symlink Guide](BLACKHOLE_SYMLINK_GUIDE.md) | `/watch` |
| `BLACKHOLE_POLL_INTERVAL` | Seconds between folder scans | `5` |
| `BLACKHOLE_DEBRID` | Debrid service: `realdebrid`, `alldebrid`, `torbox`. Auto-detected if not set | auto |
| `BLACKHOLE_SYMLINK_ENABLED` | Enable symlink creation after download. See [Blackhole Symlink Guide](BLACKHOLE_SYMLINK_GUIDE.md) | `false` |
| `BLACKHOLE_COMPLETED_DIR` | Directory for completed symlinks. Under the per-arr label layout, symlinks are nested one level deeper (`BLACKHOLE_COMPLETED_DIR/sonarr/…`). Flat layout still works when no label subdirs are present | `/completed` |
| `BLACKHOLE_RCLONE_MOUNT` | rclone mount path inside container. Append mount name if set (e.g., `/data/zurgarr`) | `/data` |
| `BLACKHOLE_SYMLINK_TARGET_BASE` | Mount path as seen by Plex/Sonarr/Radarr host(s). Must resolve on every host that reads symlinks. **Required** for symlink mode | |
| `BLACKHOLE_MOUNT_POLL_TIMEOUT` | Max seconds to wait for content on mount | `300` |
| `BLACKHOLE_MOUNT_POLL_INTERVAL` | Seconds between mount checks | `10` |
| `BLACKHOLE_SYMLINK_MAX_AGE` | Hours before old symlinks are cleaned up | `72` |
| `SYMLINK_REPAIR_AUTO_SEARCH` | When broken symlinks can't be repaired from mount, trigger Sonarr/Radarr re-search | `false` |
| `BLACKHOLE_DEDUP_ENABLED` | Check local library before submitting to debrid | `false` |
| `BLACKHOLE_LOCAL_LIBRARY_TV` | Container path to TV library for dedup. Required when dedup enabled | |
| `BLACKHOLE_LOCAL_LIBRARY_MOVIES` | Container path to movie library for dedup. Required when dedup enabled | |
| `QUALITY_COMPROMISE_ENABLED` | Opt-in master toggle for cache-aware tier escalation within the arr's profile after a dwell window. All `QUALITY_COMPROMISE_*` / `SEASON_PACK_FALLBACK_*` vars below are inert while OFF. See [Blackhole Symlink Guide](BLACKHOLE_SYMLINK_GUIDE.md#smart-quality-compromise) | `false` |
| `QUALITY_COMPROMISE_DWELL_DAYS` | Days at the preferred tier before the first compromise may fire (range 1–30) | `3` |
| `QUALITY_COMPROMISE_MIN_SEEDERS` | Seeder floor for compromise candidates (range 0–1000) | `3` |
| `QUALITY_COMPROMISE_ONLY_CACHED` | Require the compromise candidate to be cached on your debrid provider. Real-Debrid users will effectively never compromise under strict mode (RD deprecated instant-availability Nov 2024) — flip OFF for aggressive escalation, or use AllDebrid/TorBox | `true` |
| `QUALITY_COMPROMISE_MAX_TIER_DROP` | Cap on how far below the preferred tier the engine may descend (range 1–10; `1`=one drop only, `10`≈unlimited — profile ceiling still applies) | `2` |
| `QUALITY_COMPROMISE_NOTIFY` | Send an Apprise notification on each compromise grab. OFF silences Apprise only — history events + dashboard trail still fire | `true` |
| `SEASON_PACK_FALLBACK_ENABLED` | TV-only: probe a cached pack at the preferred tier before any tier drop. Requires `QUALITY_COMPROMISE_ENABLED=true` | `false` |
| `SEASON_PACK_FALLBACK_MIN_MISSING` | Minimum missing-episode count in the target season before a pack probe (range 1–100) | `4` |
| `SEASON_PACK_FALLBACK_MIN_RATIO` | Minimum missing/total ratio for the target season (range 0.0–1.0; `0.0`=disable ratio gate, rely on MIN_MISSING alone) | `0.4` |

</details>

<details>
<summary><strong>Status UI & Monitoring</strong></summary>

| Variable | Description | Default |
|----------|-------------|---------|
| `STATUS_UI_ENABLED` | Enable the status web dashboard | `false` |
| `STATUS_UI_PORT` | Dashboard port | `8080` |
| `STATUS_UI_AUTH` | Basic auth in `user:password` format. **Required** for settings editor | |
| `FFPROBE_MONITOR_ENABLED` | Enable stuck ffprobe detection | `true` |
| `FFPROBE_STUCK_TIMEOUT` | Seconds before a stuck ffprobe triggers recovery | `300` |
| `FFPROBE_POLL_INTERVAL` | Seconds between ffprobe monitor scans | `30` |

</details>

<details>
<summary><strong>Notifications</strong></summary>

| Variable | Description | Default |
|----------|-------------|---------|
| `NOTIFICATION_URL` | [Apprise](https://github.com/caronc/apprise) URL(s), comma-separated (e.g., `discord://webhook_id/webhook_token`) | |
| `NOTIFICATION_EVENTS` | Comma-separated events to subscribe to: `startup`, `shutdown`, `download_complete`, `download_error`, `library_refresh`, `symlink_created`, `symlink_failed`, `debrid_unavailable`, `local_fallback_triggered`, `blocklist_added`, `health_error`, `symlink_repaired`, `daily_digest`, `debrid_add_success`, `debrid_add_failed`. Leave empty for all | all |
| `NOTIFICATION_LEVEL` | Minimum severity: `info`, `warning`, `error` | `info` |
| `NOTIFICATION_DIGEST_ENABLED` | Send a daily summary instead of individual notifications | `false` |
| `NOTIFICATION_DIGEST_TIME` | When to send the daily digest (24h format) | `08:00` |

</details>

<details>
<summary><strong>Media Services — Sonarr/Radarr</strong></summary>

| Variable | Description | Default |
|----------|-------------|---------|
| `SONARR_URL` | Sonarr base URL (e.g., `http://sonarr:8989`). Used for downloads, rescans, and folder naming | |
| `SONARR_API_KEY` | Sonarr API key (Settings > General in Sonarr) | |
| `RADARR_URL` | Radarr base URL (e.g., `http://radarr:7878`). Used for downloads, rescans, and folder naming | |
| `RADARR_API_KEY` | Radarr API key (Settings > General in Radarr) | |

</details>

<details>
<summary><strong>Library Metadata & Debrid Search</strong></summary>

| Variable | Description | Default |
|----------|-------------|---------|
| `TMDB_API_KEY` | [TMDB](https://www.themoviedb.org/) API key (free). Enables posters, episode titles, missing episode detection, and IMDb ID resolution for debrid search | |
| `TORRENTIO_URL` | Torrentio API base URL (e.g., `https://torrentio.strem.fun`). Enables interactive torrent search in the Library detail view with debrid cache status and one-click add | |
| `HISTORY_RETENTION_DAYS` | Days to keep activity history events | `30` |
| `BLOCKLIST_AUTO_ADD` | Auto-blocklist torrents that hit terminal debrid errors | `true` |
| `BLOCKLIST_EXPIRY_DAYS` | Auto-expire auto-added blocklist entries after N days (0=never). Manual entries are kept forever | `0` |
| `LIBRARY_PREFERENCE_AUTO_ENFORCE` | Automatically switch sources when content arrives matching a stored preference (prefer-local/prefer-debrid) | `false` |
| `DEBRID_UNAVAILABLE_THRESHOLD_DAYS` | Days of failed debrid searches before marking content as debrid-unavailable and stopping retries | `3` |
| `PENDING_WARNING_HOURS` | Hours before sending a `pending_warning` notification for items stuck searching. Set to `0` to disable | `24` |

</details>

<details>
<summary><strong>Logging & Advanced</strong></summary>

| Variable | Description | Default |
|----------|-------------|---------|
| `ZURGARR_LOG_LEVEL` | Zurgarr [log level](https://docs.python.org/3/library/logging.html#logging-levels). Legacy `PDZURG_LOG_LEVEL` is still accepted through 2.20.0 and fires a one-shot deprecation warning at startup. | `INFO` |
| `ZURGARR_LOG_COUNT` | Number of rotated log files to retain. Legacy `PDZURG_LOG_COUNT` still accepted through 2.20.0. | `2` |
| `ZURGARR_LOG_SIZE` | Max log file size before rotation (`K`/`M`/`G`). Legacy `PDZURG_LOG_SIZE` still accepted through 2.20.0. | `10M` |
| `COLOR_LOG_ENABLED` | Enable colored console output | `false` |
| `GITHUB_TOKEN` | [GitHub token](https://github.com/settings/tokens) for Zurg private repo / nightly builds | |
| `SKIP_VALIDATION` | Skip startup config validation | `false` |

</details>

## Data Volumes

| Container path | Permissions | Description |
|----------------|:-----------:|-------------|
| `/config` | rw | rclone.conf, plex_debrid settings.json, persistent state. **Note:** rclone.conf is overwritten on start — don't share with other rclone instances |
| `/log` | rw | Log files |
| `/cache` | rw | rclone VFS cache (when `RCLONE_VFS_CACHE_MODE` is set) |
| `/data` | rshared | rclone mount point. Not needed if only using plex_debrid |
| `/zurg/RD` | rw | Zurg Real-Debrid state |
| `/zurg/AD` | rw | Zurg AllDebrid state |
| `/watch` | rw | Blackhole watch folder (only when `BLACKHOLE_ENABLED=true`) |
| `/completed` | rw | Completed symlinks (only when `BLACKHOLE_SYMLINK_ENABLED=true`) |

<details>
<summary><strong>Docker Secrets</strong></summary>

Zurgarr supports Docker secrets for sensitive values. Create files containing each secret and reference them in your compose:

**Supported:** `github_token`, `rd_api_key`, `ad_api_key`, `torbox_api_key`, `plex_user`, `plex_token`, `plex_address`, `jf_api_key`, `jf_address`, `seerr_api_key`, `seerr_address`

```yaml
services:
  zurgarr:
    image: zurgarr:latest
    secrets:
      - rd_api_key
      - plex_token

secrets:
  rd_api_key:
    file: ./secrets/rd_api_key.txt
  plex_token:
    file: ./secrets/plex_token.txt
```

Remove the corresponding environment variables when using secrets.

</details>

## Guides

- **[Blackhole Symlink Guide](BLACKHOLE_SYMLINK_GUIDE.md)** — complete Sonarr/Radarr setup including symlink mode, multi-host NFS, dedup, and troubleshooting
- **[Changelog](CHANGELOG.md)** — version history and release notes
- **[Brand assets](assets/README.md)** — icon SVG, social-preview SVG, and instructions for regenerating PNG exports for Docker Hub or GitHub social previews

## Troubleshooting

**Mount not available / empty `/data` directory**
- Ensure `/dev/fuse` is mapped and `SYS_ADMIN` capability is set in your compose file
- Check rclone logs: `docker logs zurgarr 2>&1 | grep rclone`
- Verify your debrid API key is valid and the account is active

**Docker Desktop: mount propagation error**
- Docker Desktop does not support `rshared` mount propagation required by rclone
- Use a Linux VM, WSL2, or bare-metal Docker instead
- See the [upstream pd_zurg wiki](https://github.com/I-am-PUID-0/pd_zurg/wiki/Setup-Guides) for WSL2 setup instructions (most steps still apply)

**Plex not seeing debrid content**
- The Plex library must point to the rclone mount shared from Zurgarr
- If using `depends_on: service_healthy`, ensure Zurgarr's healthcheck passes first
- Try `PLEX_REFRESH=true` with `PLEX_MOUNT_DIR` set to the mount path as Plex sees it

**Blackhole: symlinks created but broken**
- `BLACKHOLE_SYMLINK_TARGET_BASE` must match the mount path on every host that reads symlinks (Plex, Sonarr, Radarr), not inside the Zurgarr container
- If services run on different hosts with different mount paths, create a symlink (e.g., `ln -s /actual/mount/path /mnt/debrid`) so the path resolves everywhere
- Verify the rclone/WebDAV mount is accessible from where Plex/Sonarr/Radarr run
- See the [Blackhole Symlink Guide](BLACKHOLE_SYMLINK_GUIDE.md#troubleshooting) for detailed diagnostics

**Stuck ffprobe processes**
- Normal when Plex scans expired debrid links — the monitor handles it automatically
- Increase `FFPROBE_STUCK_TIMEOUT` if you see false positives during large library scans

**Migrating from pd_zurg**
- Container/image renamed `pd_zurg` → `zurgarr`. Update `container_name`, `image`, and the service key in your `docker-compose.yml`. Old Docker Hub images under `fjmerc/pd_zurg` remain accessible but new pushes go to `fjmerc/zurgarr`.
- Default rclone mount name is now `zurgarr` (was `pd_zurg`). If you set `RCLONE_MOUNT_NAME` explicitly in `.env`, nothing changes. If you relied on the default, your mount path becomes `/data/zurgarr` instead of `/data/pd_zurg` — update `BLACKHOLE_RCLONE_MOUNT` and `PLEX_MOUNT_DIR` accordingly.
- Env var keys and Prometheus metric names are being renamed across a two-release window. **2.19.0** reads BOTH the new `ZURGARR_LOG_*` and legacy `PDZURG_LOG_*` env vars (new name wins when both are set; a one-shot deprecation warning fires when a legacy name is read) and emits metrics under BOTH the `pd_zurg_*` and `zurgarr_*` prefixes. Your existing `.env` files and Grafana dashboards keep working. **2.20.0** drops the legacy names — before upgrading to 2.20.0, rename your `.env` entries to `ZURGARR_LOG_*` and update Grafana dashboard queries from `pd_zurg_*` to `zurgarr_*`.
- Browser-saved Basic Auth credentials for `/settings` may need to be re-saved (the auth realm changed from `pd_zurg` to `Zurgarr`).

## Community

- **Bug reports & feature requests:** [GitHub Issues](https://github.com/fjmerc/zurgarr/issues)
- **Upstream pd_zurg (archived):** [Discussions](https://github.com/I-am-PUID-0/pd_zurg/discussions) | [Discord](https://discord.gg/EPSWqmeeXM)
- **plex_debrid:** [Discussions](https://github.com/itsToggle/plex_debrid/discussions) | [Discord](https://discord.gg/u3vTDGjeKE)

## Credits

Zurgarr builds on the work of:

- **[itsToggle](https://github.com/itsToggle)** — plex_debrid ([affiliate](http://real-debrid.com/?id=5708990) | [PayPal](https://www.paypal.com/paypalme/oidulibbe))
- **[yowmamasita](https://github.com/yowmamasita)** — Zurg ([sponsor](https://github.com/sponsors/debridmediamanager))
- **[ncw](https://github.com/ncw)** — rclone ([sponsor](https://rclone.org/sponsor/))
- **[I-am-PUID-0](https://github.com/I-am-PUID-0)** — original pd_zurg, the project Zurgarr was forked from

## Licensing

Code authored for the Zurgarr project (and its pd_zurg lineage in this fork) is released under the MIT License — see [LICENSE](LICENSE).

This repository also redistributes or vendors third-party components, each governed by its own upstream terms:

- **rclone** — MIT, see [LICENSES/rclone.LICENSE](LICENSES/rclone.LICENSE)
- **Zurg** — no license declared upstream; downloaded as a binary at image build time
- **plex_debrid** — no license declared upstream; vendored under [plex_debrid/](plex_debrid/) with notes in [plex_debrid/ATTRIBUTION.md](plex_debrid/ATTRIBUTION.md)

See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for the full inventory.
