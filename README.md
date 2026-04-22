# Zurgarr

Local media and debrid content, managed as one library — for Plex, Jellyfin, Sonarr, and Radarr.

![Build Status](https://img.shields.io/github/actions/workflow/status/fjmerc/zurgarr/docker-image.yml)

## What is Zurgarr?

Zurgarr bridges your local media library and a Real-Debrid / AllDebrid /
TorBox account so your *arr stack and media server see them as one
unified library instead of two parallel systems.

At its base it packages
**[Zurg](https://github.com/debridmediamanager/zurg-testing)** (WebDAV
for your debrid account), **[rclone](https://github.com/rclone/rclone)**
(mounts it as a local directory), and optionally
**[plex_debrid](https://github.com/itsToggle/plex_debrid)** (watchlist
automation) into a single ~150 MB Alpine container — your media server
sees debrid content as local files, no downloading, no local storage.

On top of that foundation it adds an integration layer: a source-aware
Library browser that combines local + debrid content with
per-item preferences, automatic debrid symlinks so Sonarr/Radarr can
discover debrid-only content, TMDB-driven gap-fill across both backends,
a Sonarr/Radarr blackhole with cache-aware quality compromise, a
self-healing routing audit, and a browser-based settings editor.
Mixed setups feel like one library, not two.

## Why this project?

> [!NOTE]
> Zurgarr started as a fork of
> [pd_zurg](https://github.com/I-am-PUID-0/pd_zurg) by
> [I-am-PUID-0](https://github.com/I-am-PUID-0), which was deprecated in
> favor of [DUMB](https://github.com/I-am-PUID-0/DUMB). After substantial
> divergence the project was renamed to Zurgarr to fit the *arr ecosystem
> naming convention and signal that it's its own thing now. The
> MIT-licensed lineage is preserved with full attribution.

pd_zurg's contribution was packaging three services into one container.
Zurgarr keeps that packaging and adds an integration layer that treats
local files and debrid content as one library to manage, reconcile, and
observe. Capabilities grouped by intent:

**Unified library — make two sources feel like one**

- Source-aware **Library browser** combining debrid + local content with
  TMDB metadata, per-item source preferences (prefer-local /
  prefer-debrid / to-any), and episode-level switch controls
- **Auto debrid symlinks** — debrid-only content appears in your local
  library paths so Sonarr/Radarr can discover and manage it alongside
  local files
- **Interactive Torrentio search** with per-provider cache annotations
  and one-click add-to-debrid, straight from the Library detail view
- **Cross-machine setup** — expose Zurg's WebDAV and mount the debrid
  library from any host, not just the Zurgarr container

**Sonarr/Radarr integration — blackhole + smart reconciliation**

- **Blackhole watch folder** with per-arr label routing (each arr sees
  only its own items) and symlink creation after the debrid download
  resolves — see the [Blackhole Symlink Guide](BLACKHOLE_SYMLINK_GUIDE.md)
- **Quality compromise engine** — when a strict profile turns up no
  cached releases, cache-aware tier escalation finds the best cached
  alternative within the arr's profile; Sonarr's normal upgrade path
  reclaims the preferred tier later
- **Season-pack fallback** probes cached packs at the preferred tier
  before any tier drop for shows with many holes
- **TMDB gap-fill** — for every monitored show, diffs the aired-episode
  list against what exists across debrid + local and searches
  Sonarr/Radarr for the missing pieces, regardless of source preference
- **Debrid-account dedup + require-cached gates** stop uncached junk and
  duplicate hashes from landing in your account in the first place

**Self-healing — fewer manual interventions**

- **Routing audit auto-tag** applies the debrid tag to monitored
  Sonarr/Radarr media missing a routing tag (self-heals Overseerr
  requests that arrive with empty tags)
- **Symlink verify + repair** detects broken symlinks and triggers arr
  re-search so RD cache evictions close automatically
- **Hash blocklist** with configurable auto-expiry prevents re-grab
  loops on terminally failed torrents
- **ffprobe recovery** kills stuck Plex scans on expired debrid links
- **Process auto-restart** with exponential backoff (5 s → 300 s, resets
  after 1 h stable)

**Observability & control**

- Browser-based **status dashboard + Settings editor** with SIGHUP
  reload — edit most env vars without restarting the container
- **Activity history log** — every grab, compromise, symlink event, and
  debrid add, filterable by type and time
- **Apprise notifications** across 90+ services (Discord, Telegram,
  Slack, email, etc.) with optional daily digest mode
- **Prometheus metrics endpoint** for Grafana / Alertmanager
- **OAuth device-code flows** for Trakt, Debrid Link, Put.io, Orionoid;
  **MDBList** subscriptions that auto-feed plex_debrid

**Reliability & non-disruption**

- **Atomic config writes** — temp-rename pattern for `.env`,
  `settings.json`, tier-state sidecars, and preference store prevents
  corruption on mid-write container kills
- **Ordered shutdown** with per-service timeouts and elapsed-time logging
- **Docker secrets** support for API keys and tokens
- **Drops into your existing *arr stack** — the debrid path is a
  sidecar alongside qBittorrent/Usenet, not a replacement

## How it works

Two workflows. Use either or both.

**Watchlist flow** — plex_debrid monitors your watchlists automatically:

```
Watchlist (Plex / Trakt / Overseerr)
  → plex_debrid (search & match)
    → Real-Debrid (cloud cache)
      → Zurg (WebDAV) → rclone (/data mount)
        → Plex / Jellyfin (stream)
```

**Arr + blackhole flow** — Sonarr/Radarr with tag-based routing:

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

- A **Linux Docker host** (not Docker Desktop — it lacks
  [mount propagation](https://docs.docker.com/storage/bind-mounts/#configure-bind-propagation)
  support)
- A [Real-Debrid](https://real-debrid.com/apitoken),
  [AllDebrid](https://alldebrid.com/apikeys/), or
  [TorBox](https://torbox.app/settings) account with an API key
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

# Edit — at minimum set:
#   RD_API_KEY        (or AD_API_KEY / TORBOX_API_KEY)
#   STATUS_UI_AUTH    (e.g., admin:yourpassword)
nano .env
```

The [`.env.example`](.env.example) is commented inline. Full reference
lives in [CONFIGURATION.md](CONFIGURATION.md).

### 3. Create directories

```bash
mkdir -p config log cache mnt RD
```

### 4. Start

```bash
docker compose up -d
```

### 5. Verify

- Status dashboard at `http://your-host:8080/status`
- Zurg and rclone should show as **Running**
- Mount check: `ls mnt/zurgarr/` should show your debrid library
  categories

## Choose your workflow

### Option A: Watchlist mode (plex_debrid)

Best if you want fully automated content from Plex watchlists, Trakt, or
Overseerr — no Sonarr/Radarr needed.

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

For **Jellyfin/Emby**, use `JF_ADDRESS` and `JF_API_KEY` instead. Note:
Jellyfin requires
[additional plex_debrid setup](https://github.com/itsToggle/plex_debrid#open_file_folder-library-collection-service)
for Trakt Collections.

### Option B: Arr + blackhole mode (Sonarr/Radarr)

Best if you already use Sonarr/Radarr and want debrid as a download
client alongside (or instead of) qBittorrent/Usenet.

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

See the **[Blackhole Symlink Guide](BLACKHOLE_SYMLINK_GUIDE.md)** for
Sonarr/Radarr download-client configuration, multi-host NFS,
verification steps, and troubleshooting.

### Option C: Both

Run plex_debrid and blackhole simultaneously. Set `PD_ENABLED=true` and
`BLACKHOLE_ENABLED=true`, then use Sonarr/Radarr tags to route specific
content through the blackhole while plex_debrid handles watchlist items.

## Recommended settings per debrid provider

Uncached junk (0%/0-seed entries in DMM) and duplicate hashes are
common pain points. Defaults handle dedup automatically; the
cache-required gates need one flip depending on your provider:

| Your provider | Flip ON in the Settings UI |
|---|---|
| **Real-Debrid** | `PD_ENFORCE_CACHED_VERSIONS` |
| **AllDebrid or TorBox** | `BLACKHOLE_REQUIRE_CACHED` and `SEARCH_REQUIRE_CACHED`, plus `PD_ENFORCE_CACHED_VERSIONS` if using plex_debrid |

Background and more symptom-based fixes in
[TROUBLESHOOTING.md](TROUBLESHOOTING.md).

## Docker Compose

> [!NOTE]
> These examples are starting points. Adjust paths to match your environment.

### Base setup

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

### Plex companion

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

## Web UI & Settings editor

The **status dashboard** at `/status` shows process health (Zurg, rclone,
plex_debrid), mount status, system resources, recent events, and a
filtered log viewer.

The **settings editor** at `/settings` provides:

- **Zurgarr tab** — every env var with toggles, dropdowns, password
  fields, inline validation, and SIGHUP reload (no restart needed)
- **plex_debrid tab** — edit `settings.json` with multi-select pickers,
  list editors, and a quality-profile JSON editor
- **OAuth tab** — connect Trakt, Debrid Link, Put.io, and Orionoid via
  device-code flow
- **Import/Export** — download or upload settings for backup/migration

Requires `STATUS_UI_AUTH` (e.g., `admin:changeme`). Not accessible
without authentication.

## Docs

- **[CONFIGURATION.md](CONFIGURATION.md)** — every env var, grouped by
  feature, with defaults
- **[TROUBLESHOOTING.md](TROUBLESHOOTING.md)** — symptom-first fixes for
  the common problems
- **[BLACKHOLE_SYMLINK_GUIDE.md](BLACKHOLE_SYMLINK_GUIDE.md)** — complete
  Sonarr/Radarr blackhole setup including symlink mode, multi-host NFS,
  dedup, and diagnostics
- **[ARCHITECTURE.md](ARCHITECTURE.md)** — how the internals fit
  together (for contributors)
- **[CHANGELOG.md](CHANGELOG.md)** — version history and release notes

## Community

- **Bug reports & feature requests:** [GitHub Issues](https://github.com/fjmerc/zurgarr/issues)
- **Upstream pd_zurg (archived):** [Discussions](https://github.com/I-am-PUID-0/pd_zurg/discussions) | [Discord](https://discord.gg/EPSWqmeeXM)
- **plex_debrid:** [Discussions](https://github.com/itsToggle/plex_debrid/discussions) | [Discord](https://discord.gg/u3vTDGjeKE)

## Credits

Zurgarr builds on the work of:

- **[itsToggle](https://github.com/itsToggle)** — plex_debrid ([affiliate](http://real-debrid.com/?id=5708990) | [PayPal](https://www.paypal.com/paypalme/oidulibbe))
- **[yowmamasita](https://github.com/yowmamasita)** — Zurg ([sponsor](https://github.com/sponsors/debridmediamanager))
- **[ncw](https://github.com/ncw)** — rclone ([sponsor](https://rclone.org/sponsor/))
- **[I-am-PUID-0](https://github.com/I-am-PUID-0)** — original pd_zurg

## Licensing

Code authored for the Zurgarr project (and its pd_zurg lineage in this
fork) is released under the MIT License — see [LICENSE](LICENSE).

This repository also redistributes or vendors third-party components,
each governed by its own upstream terms:

- **rclone** — MIT, see [LICENSES/rclone.LICENSE](LICENSES/rclone.LICENSE)
- **Zurg** — no license declared upstream; downloaded as a binary at
  image build time
- **plex_debrid** — no license declared upstream; vendored under
  [plex_debrid/](plex_debrid/) with notes in
  [plex_debrid/ATTRIBUTION.md](plex_debrid/ATTRIBUTION.md)

See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for the full inventory.
