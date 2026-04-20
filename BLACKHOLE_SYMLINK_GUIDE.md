# Blackhole Symlink Guide — Sonarr/Radarr Integration with Real-Debrid

This guide explains how to use pd_zurg's blackhole feature with symlink mode to integrate Sonarr and Radarr with Real-Debrid (or AllDebrid/TorBox). This enables zero-copy, automated media management where Sonarr/Radarr handle content discovery and tracking while debrid provides the actual media files.

## Table of Contents

- [Overview](#overview)
- [How It Works](#how-it-works)
- [Auto-Symlinks for Debrid-Only Content](#auto-symlinks-for-debrid-only-content)
- [Prerequisites](#prerequisites)
- [Directory Layout: Labeled vs Flat](#directory-layout-labeled-vs-flat)
- [Architecture](#architecture)
  - [Single-Host Setup (Labeled)](#single-host-setup-labeled)
  - [Single-Arr Quick Start (Flat)](#single-arr-quick-start-flat)
  - [Multi-Host Setup](#multi-host-setup)
- [Configuration](#configuration)
  - [pd_zurg Environment Variables](#pd_zurg-environment-variables)
  - [Docker Compose](#docker-compose)
  - [Sonarr Setup](#sonarr-setup)
  - [Radarr Setup](#radarr-setup)
- [Migration from Flat Layout](#migration-from-flat-layout)
- [Smart quality compromise](#smart-quality-compromise)
- [Verification](#verification)
- [Troubleshooting](#troubleshooting)
- [FAQ](#faq)

---

## Overview

The blackhole symlink feature bridges the gap between Sonarr/Radarr and debrid services. Without it, Sonarr/Radarr can send torrents to Real-Debrid via the blackhole, but they never know when the download completes — so they can't track episodes, auto-grab new releases, or manage your library.

With symlink mode enabled, pd_zurg:

1. Accepts `.torrent` and `.magnet` files from Sonarr/Radarr
2. Submits them to your debrid service
3. Monitors the torrent until it's ready
4. Waits for the content to appear on the Zurg/rclone mount
5. Creates symlinks in a "completed" directory pointing to the actual files on the mount

Sonarr/Radarr see the symlinks as completed downloads and import them into their library. The symlinks are just pointers (a few bytes each) — **no files are copied and no extra disk space is used**. Your media server (Plex, Jellyfin, Emby) follows the symlinks to stream directly from the debrid mount.

## How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                        The Symlink Pipeline                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Sonarr/Radarr finds a release                               │
│          │                                                       │
│          ▼                                                       │
│  2. Drops .torrent/.magnet in the blackhole watch directory      │
│          │                                                       │
│          ▼                                                       │
│  3. pd_zurg picks up the file, submits to Real-Debrid API       │
│          │                                                       │
│          ▼                                                       │
│  4. pd_zurg polls RD API until torrent status = "downloaded"     │
│     (cached torrents are instant, uncached may take minutes)     │
│          │                                                       │
│          ▼                                                       │
│  5. pd_zurg waits for content to appear on the rclone mount      │
│     (Zurg detects the new torrent and serves it via WebDAV)      │
│          │                                                       │
│          ▼                                                       │
│  6. pd_zurg creates symlinks in the completed directory          │
│     /completed/Release.Name/episode.mkv                          │
│       → /mnt/debrid/shows/Release.Name/episode.mkv              │
│          │                                                       │
│          ▼                                                       │
│  7. Sonarr/Radarr scans the completed directory (Watch Folder)   │
│     finds the symlinked files, and imports them into its library  │
│          │                                                       │
│          ▼                                                       │
│  8. Plex/Jellyfin follows the symlinks to stream from debrid     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Prerequisites

- **pd_zurg** running with Zurg + rclone (the debrid mount must be working)
- **Sonarr** and/or **Radarr** installed
- A **debrid API key** (Real-Debrid, AllDebrid, or TorBox)
- Torrent **indexers** configured in Sonarr/Radarr (Jackett, Prowlarr, or direct Torznab)

## Directory Layout: Labeled vs Flat

pd_zurg supports two directory layouts. **Labeled mode is the recommended pattern** whenever you run more than one arr against pd_zurg. Flat mode is retained for simple single-arr installs.

### Labeled mode (recommended)

Each arr gets its own subdirectory under `BLACKHOLE_DIR` and `BLACKHOLE_COMPLETED_DIR`. pd_zurg auto-detects the layout — no new environment variable is required. You just reshape your folders and mount each arr's subdir into its container.

```
/opt/blackhole/                ← mounted as /watch inside pd_zurg
├── sonarr/                    ← Sonarr writes here; pd_zurg picks up with label="sonarr"
├── radarr/                    ← Radarr writes here; pd_zurg picks up with label="radarr"
├── failed/                    ← shared retry staging (managed by pd_zurg)
└── .alt_pending/              ← shared alt-retry staging (managed by pd_zurg)

/opt/completed/                ← mounted as /completed inside pd_zurg
├── sonarr/                    ← Sonarr Watch Folder; contains only Sonarr's releases
├── radarr/                    ← Radarr Watch Folder; contains only Radarr's releases
└── pending_monitors.json      ← internal state; safe to leave alone
```

pd_zurg mounts the **parents** (`/opt/blackhole`, `/opt/completed`). Each arr container mounts only its **own label subdir**, so Sonarr physically cannot see Radarr's items (and vice versa). This eliminates the "Directory not empty" orphan warnings that appear when two arrs share the same folder.

Label name rules: alphanumeric plus `-` / `_`, max 64 characters. The names `failed` and `.alt_pending` are reserved for staging. `sonarr`, `radarr`, `readarr`, `lidarr`, `sonarr-4k`, `sonarr-hd`, etc. are all fine.

### Flat mode (single-arr installs)

If you run only one arr against pd_zurg, you can drop `.torrent` / `.magnet` files directly in the root of `BLACKHOLE_DIR` and symlinks land directly in `BLACKHOLE_COMPLETED_DIR`. This is the original behavior and continues to work unchanged:

```
/opt/blackhole/Release.torrent         → picked up by pd_zurg
/opt/completed/Release.Name/file.mkv   → symlink created here
```

Mixed mode also works: loose files in the root are treated as unlabeled, while subdirs are treated as labels. This is useful during migration from flat to labeled layout.

## Architecture

### Single-Host Setup (Labeled)

If pd_zurg, Sonarr, and Radarr all run on the **same Docker host**, the setup is straightforward — all containers share directories via Docker bind mounts, and each arr sees only its own label subdir.

```
Docker Host
├── pd_zurg container
│   ├── /watch       ← /opt/blackhole  (parent — sees all labels)
│   ├── /completed   ← /opt/completed  (parent — writes each label subdir)
│   └── /data        ← rclone mount (Zurg WebDAV)
│
├── Sonarr container
│   ├── /watch       ← /opt/blackhole/sonarr   (label subdir only)
│   ├── /completed   ← /opt/completed/sonarr   (label subdir only)
│   └── /mnt/debrid  ← rclone mount (needed so symlink targets resolve)
│
└── Radarr container
    ├── /watch       ← /opt/blackhole/radarr   (label subdir only)
    ├── /completed   ← /opt/completed/radarr   (label subdir only)
    └── /mnt/debrid  ← rclone mount
```

**Host directory layout:**
```bash
/opt/blackhole/sonarr/     # Sonarr's outbound blackhole
/opt/blackhole/radarr/     # Radarr's outbound blackhole
/opt/completed/sonarr/     # Sonarr's Watch Folder
/opt/completed/radarr/     # Radarr's Watch Folder
/mnt/debrid/               # rclone FUSE mount to Zurg WebDAV
```

### Single-Arr Quick Start (Flat)

If you're only wiring up one arr (e.g., just Sonarr), you can skip the label subdirs and share one directory:

```bash
/opt/blackhole/    # shared watch dir
/opt/completed/    # shared completed dir
/mnt/debrid/       # rclone mount
```

This is identical to the pre-label-routing behavior. Each container mounts the same host path into `/watch` and `/completed`. If you later want to add a second arr, follow [Migration from Flat Layout](#migration-from-flat-layout).

### Multi-Host Setup

If pd_zurg runs on a different host than Sonarr/Radarr, you need to share the blackhole and completed directories between hosts. NFS is the simplest approach.

```
Host A (Sonarr/Radarr)                    Host B (pd_zurg)
├── /opt/blackhole/ ──NFS export──────────→ /mnt/blackhole/
├── /opt/completed/ ──NFS export──────────→ /mnt/completed/
└── /mnt/debrid/    ──rclone WebDAV──────→ Zurg on Host B
```

**Important:** The Sonarr/Radarr host exports the directories, and the pd_zurg host mounts them. This is because:
- Sonarr/Radarr need fast local access to the completed directory for imports
- Symlink targets must resolve on the Sonarr/Radarr host (where `/mnt/debrid` exists)

**NFS setup on Host A (Sonarr/Radarr host):**
```bash
# Create directories
sudo mkdir -p /opt/blackhole /opt/completed
sudo chmod 777 /opt/blackhole /opt/completed

# Export via NFS (replace 10.0.0.2 with your pd_zurg host's IP)
echo "/opt/blackhole 10.0.0.2(rw,sync,no_subtree_check,no_root_squash)" | sudo tee -a /etc/exports
echo "/opt/completed 10.0.0.2(rw,sync,no_subtree_check,no_root_squash)" | sudo tee -a /etc/exports
sudo exportfs -ra
```

**NFS setup on Host B (pd_zurg host):**
```bash
# Create mount points
sudo mkdir -p /mnt/blackhole /mnt/completed

# Mount (replace 10.0.0.1 with your Sonarr/Radarr host's IP)
sudo mount -t nfs 10.0.0.1:/opt/blackhole /mnt/blackhole
sudo mount -t nfs 10.0.0.1:/opt/completed /mnt/completed

# Add to fstab for persistence
echo "10.0.0.1:/opt/blackhole /mnt/blackhole nfs defaults,_netdev 0 0" | sudo tee -a /etc/fstab
echo "10.0.0.1:/opt/completed /mnt/completed nfs defaults,_netdev 0 0" | sudo tee -a /etc/fstab
```

## Configuration

### pd_zurg Environment Variables

Add these to your `.env` file or set them directly in `docker-compose.yml`:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `BLACKHOLE_ENABLED` | Yes | `false` | Enable the blackhole watcher |
| `BLACKHOLE_DIR` | No | `/watch` | Directory to watch for `.torrent`/`.magnet` files |
| `BLACKHOLE_POLL_INTERVAL` | No | `5` | Seconds between scans of the watch directory |
| `BLACKHOLE_DEBRID` | No | auto-detect | Debrid service: `realdebrid`, `alldebrid`, or `torbox`. Auto-detected from API keys if not set. |
| `BLACKHOLE_SYMLINK_ENABLED` | Yes | `false` | Enable symlink creation after debrid download |
| `BLACKHOLE_COMPLETED_DIR` | No | `/completed` | Directory where symlinks are created |
| `BLACKHOLE_RCLONE_MOUNT` | No | `/data` | Path to the rclone mount inside the container. If `RCLONE_MOUNT_NAME` is set (e.g., `pd_zurg`), use `/data/<mount_name>` (e.g., `/data/pd_zurg`). |
| `BLACKHOLE_SYMLINK_TARGET_BASE` | Yes* | _(empty)_ | **How the rclone mount path looks on the Sonarr/Radarr host.** This is critical for cross-host setups. See [Understanding BLACKHOLE_SYMLINK_TARGET_BASE](#understanding-blackhole_symlink_target_base). |
| `BLACKHOLE_MOUNT_POLL_TIMEOUT` | No | `300` | Seconds to wait for debrid to process the torrent AND for content to appear on the mount |
| `BLACKHOLE_MOUNT_POLL_INTERVAL` | No | `10` | Seconds between status/mount checks |
| `BLACKHOLE_SYMLINK_MAX_AGE` | No | `72` | Hours before completed symlink directories are cleaned up |
| `SYMLINK_REPAIR_AUTO_SEARCH` | No | `false` | When the verify task finds broken symlinks that can't be repaired from the mount, trigger Sonarr/Radarr to re-search. Uses a 2-hour cooldown per item. |
| `BLACKHOLE_DEDUP_ENABLED` | No | `false` | Enable local library duplicate checking before sending torrents to debrid. When enabled, pd_zurg compares incoming torrents against your existing TV and movie libraries to avoid re-downloading content you already have. |
| `BLACKHOLE_LOCAL_LIBRARY_TV` | Yes* | _(empty)_ | Path to your local TV library inside the container for dedup checking (e.g., `/data/media/tv`). Must be mounted read-only in the pd_zurg container. |
| `BLACKHOLE_LOCAL_LIBRARY_MOVIES` | Yes* | _(empty)_ | Path to your local movies library inside the container for dedup checking (e.g., `/data/media/movies`). Must be mounted read-only in the pd_zurg container. |

\* Required when symlink mode is enabled.

\* Required when `BLACKHOLE_DEDUP_ENABLED=true`.

#### Understanding BLACKHOLE_SYMLINK_TARGET_BASE

This is the most important setting. Symlinks are created inside the pd_zurg container, but they must resolve on **every host that reads them** — Plex, Sonarr, Radarr, and any other service that accesses your media library.

**Inside pd_zurg**, a file might be at:
```
/data/pd_zurg/shows/Release.Name/episode.mkv
```

**On the host**, the same file is accessible at:
```
/mnt/debrid/shows/Release.Name/episode.mkv
```

So you set `BLACKHOLE_SYMLINK_TARGET_BASE=/mnt/debrid` — this replaces the container-internal mount path with the host-visible path in the symlink target.

**Single-host example:** If your rclone mount is at `/mnt/debrid` on the host and mounted into both pd_zurg (`/data`) and Sonarr (`/mnt/debrid`):
```
BLACKHOLE_SYMLINK_TARGET_BASE=/mnt/debrid
```

**Multi-host example:** If Plex and Sonarr/Radarr run on different hosts with different mount paths, the `BLACKHOLE_SYMLINK_TARGET_BASE` path must resolve on **all** of them. If the rclone mount has different paths on each host, create a symlink on hosts where the path doesn't match:
```bash
# On a host where the mount is at /mnt/remote/realdebrid/pd_zurg but symlinks use /mnt/debrid:
sudo ln -s /mnt/remote/realdebrid/pd_zurg /mnt/debrid
```
```
BLACKHOLE_SYMLINK_TARGET_BASE=/mnt/debrid
```

#### Example .env

```bash
# Blackhole Settings
BLACKHOLE_ENABLED=true
BLACKHOLE_DIR=/watch

# Blackhole Symlink Settings
BLACKHOLE_SYMLINK_ENABLED=true
BLACKHOLE_COMPLETED_DIR=/completed
BLACKHOLE_RCLONE_MOUNT=/data/pd_zurg
BLACKHOLE_SYMLINK_TARGET_BASE=/mnt/debrid
BLACKHOLE_MOUNT_POLL_TIMEOUT=300
BLACKHOLE_MOUNT_POLL_INTERVAL=10
BLACKHOLE_SYMLINK_MAX_AGE=72

# Blackhole Dedup Settings (optional — checks local library before submitting to debrid)
BLACKHOLE_DEDUP_ENABLED=true
BLACKHOLE_LOCAL_LIBRARY_TV=/data/media/tv
BLACKHOLE_LOCAL_LIBRARY_MOVIES=/data/media/movies
```

### Docker Compose

These snippets show the **labeled layout** (recommended). For flat-mode single-arr installs, drop the label subdirs from the volume paths — everything else is identical.

#### pd_zurg

```yaml
services:
  pd_zurg:
    image: pd_zurg:latest
    container_name: pd_zurg
    volumes:
      - ./zurg_config.yml:/zurg/config.yml
      - config:/config
      - log:/log
      - rd:/zurg/RD
      - /mnt/remote/realdebrid:/data:shared      # rclone mount
      - /opt/blackhole:/watch                      # PARENT dir — pd_zurg sees all labels
      - /opt/completed:/completed                  # PARENT dir — pd_zurg writes all labels
      # Local library for dedup (read-only)
      - /mnt/truenas/data/media/tv:/data/media/tv:ro
      - /mnt/truenas/data/media/movies:/data/media/movies:ro
    environment:
      - BLACKHOLE_ENABLED=true
      - BLACKHOLE_DIR=/watch
      - BLACKHOLE_SYMLINK_ENABLED=true
      - BLACKHOLE_COMPLETED_DIR=/completed
      - BLACKHOLE_RCLONE_MOUNT=/data/pd_zurg       # adjust to match your RCLONE_MOUNT_NAME
      - BLACKHOLE_SYMLINK_TARGET_BASE=/mnt/debrid   # path as seen by Sonarr/Radarr
      # ... other pd_zurg settings ...
    devices:
      - /dev/fuse:/dev/fuse:rwm
    cap_add:
      - SYS_ADMIN
    security_opt:
      - apparmor:unconfined
      - no-new-privileges
```

#### Sonarr

```yaml
  sonarr:
    image: lscr.io/linuxserver/sonarr
    container_name: sonarr
    volumes:
      - sonarr_config:/config
      - /mnt/truenas/data/media/tv:/data/media/tv   # your local media library
      - /mnt/debrid:/mnt/debrid:rslave               # debrid mount (rslave for FUSE propagation)
      - /opt/blackhole/sonarr:/watch                 # label SUBDIR — Sonarr sees only its own items
      - /opt/completed/sonarr:/completed             # label SUBDIR — Sonarr imports only its own items
    environment:
      - PUID=1000
      - PGID=1000
      - TZ=Your/Timezone
```

> **Note:** Use `:rslave` mount propagation for the debrid FUSE mount. This ensures the container sees mount changes if rclone reconnects. Without it, the mount may appear empty after a brief disconnection.

#### Radarr

```yaml
  radarr:
    image: lscr.io/linuxserver/radarr
    container_name: radarr
    volumes:
      - radarr_config:/config
      - /mnt/truenas/data/media/movies:/data/media/movies
      - /mnt/debrid:/mnt/debrid:rslave
      - /opt/blackhole/radarr:/watch                 # label SUBDIR
      - /opt/completed/radarr:/completed             # label SUBDIR
    environment:
      - PUID=1000
      - PGID=1000
      - TZ=Your/Timezone
```

Create the host directories ahead of time so the bind mounts succeed on first start:

```bash
sudo mkdir -p /opt/blackhole/sonarr /opt/blackhole/radarr
sudo mkdir -p /opt/completed/sonarr /opt/completed/radarr
sudo chmod 777 /opt/blackhole /opt/completed  # adjust to your PUID/PGID
```

### Sonarr Setup

1. Go to **Settings → Download Clients → Add → Torrent Blackhole**

2. Configure the download client (paths are container-internal — the labeled layout means Sonarr still sees them as `/watch` and `/completed`):

   | Setting | Value | Notes |
   |---------|-------|-------|
   | Name | `Real-Debrid (Blackhole)` | Or any name you prefer |
   | Torrent Folder | `/watch/` | Where Sonarr saves `.torrent` files |
   | Watch Folder | `/completed/` | Where Sonarr looks for completed downloads |
   | Save Magnet Files | `Yes` | Many indexers only provide magnets |
   | Save Magnet Files Extension | `.magnet` | Default is fine |
   | Read Only | `Yes` | Tells Sonarr to copy (symlinks) instead of move |
   | Remove Completed Downloads | `No` | Don't try to delete from the debrid mount |
   | Remove Failed Downloads | `No` | Same reason |

3. Click **Test** — should pass with no errors.

4. **(Optional) Use tags** to route specific series to the blackhole client:
   - Create a tag (e.g., `debrid`) in **Settings → Tags**
   - Assign the tag to the blackhole download client
   - Assign the same tag to series you want to download via debrid
   - Series without the tag will use your other download clients (qBittorrent, NZBGet, etc.)

### Radarr Setup

Same as Sonarr — go to **Settings → Download Clients → Add → Torrent Blackhole** and use the same settings. The label routing is entirely a function of the host directory layout; the arr itself doesn't need to know.

## Migration from Flat Layout

If you're upgrading from a pre-label-routing setup where Sonarr and Radarr shared `/opt/blackhole` and `/opt/completed`, follow these steps once. Existing in-flight torrents and symlinks keep working throughout.

1. **Stop pd_zurg and all arrs** so nothing writes new files during the reshape.
   ```bash
   docker stop pd_zurg sonarr radarr
   ```

2. **Create label subdirs on the host**:
   ```bash
   sudo mkdir -p /opt/blackhole/sonarr /opt/blackhole/radarr
   sudo mkdir -p /opt/completed/sonarr /opt/completed/radarr
   ```

3. **(Optional) Move any in-flight files** into the correct label dir. Anything left at the root of `/opt/blackhole` keeps working in flat mode — only new drops from the arrs need to land in the label subdirs.

4. **Update each arr's docker-compose volume mounts** to point at the label subdir (see the Sonarr/Radarr snippets above). pd_zurg keeps mounting the parents.

5. **Start pd_zurg first, then the arrs**:
   ```bash
   docker start pd_zurg
   docker start sonarr radarr
   ```

Existing release folders under `/opt/completed/` root (from before the migration) are still valid — pd_zurg's cleanup task will expire them on the normal schedule (`BLACKHOLE_SYMLINK_MAX_AGE`, default 72h). You don't need to move them.

If you prefer to stay on flat layout with a single arr, no change is required. The code treats both layouts as first-class.

## Smart quality compromise

### What it does

If you run a strict Sonarr/Radarr profile — say "2160p REMUX only" or "1080p BluRay only" — and the debrid service has no cached copy at that tier, the blackhole will normally cycle through the arr's alternatives at the same tier and eventually move the file to `failed/`. The episode stays missing until you manually relax the profile or pick a different release, even when a perfectly good cached copy one tier down is sitting right there.

Smart quality compromise is an opt-in safety net for this case. After a waiting period at the preferred tier, pd_zurg probes one tier below within the same profile, checks your debrid cache, and grabs the best cached release at that lower tier. **The arr's quality profile is always the ceiling** — pd_zurg never grabs a tier your profile doesn't permit, no matter how long it waits. When the preferred tier later appears on debrid, Sonarr's/Radarr's normal upgrade logic reclaims it automatically — compromises are temporary placeholders, not permanent decisions.

### How to enable

The feature ships OFF. Flip it on with the master toggle:

```bash
QUALITY_COMPROMISE_ENABLED=true
```

Sensible defaults are set for the rest; you typically don't need to touch them:

```bash
QUALITY_COMPROMISE_DWELL_DAYS=3          # days at preferred tier before compromise fires
QUALITY_COMPROMISE_MIN_SEEDERS=3         # candidate seeder floor
QUALITY_COMPROMISE_ONLY_CACHED=true      # refuse to compromise to an uncached release
QUALITY_COMPROMISE_MAX_TIER_DROP=2       # how far below preferred to allow (1=one drop only)
QUALITY_COMPROMISE_NOTIFY=true           # Apprise notification on each compromise
```

Apply via SIGHUP (no restart needed):

```bash
docker kill -s HUP pd_zurg
```

Or use the **Settings → Quality Compromise** section of the web UI.

#### Decide on `ONLY_CACHED` for your debrid provider

- **Default (`true`, recommended):** pd_zurg only compromises to a release the debrid service already has cached. A compromise to an uncached release is worse than no compromise — you'd trade quality and still wait on the debrid download.
- **Real-Debrid caveat:** Real-Debrid deprecated their cache-availability endpoint in November 2024, so pd_zurg cannot tell whether a Real-Debrid release is cached. Under the default `ONLY_CACHED=true`, RD users will effectively never see a compromise fire (every candidate is "unknown" and treated as not cached). If you're on RD and want compromises anyway, set `QUALITY_COMPROMISE_ONLY_CACHED=false` — this is aggressive (the compromise candidate may need to download from peers) but will let the engine escalate.
- **AllDebrid and TorBox:** Their cache endpoints still work, so strict mode behaves as expected. No change needed.

#### Season-pack fallback (opt-in on top)

For shows with many missing episodes in a single season (e.g. 5/10 holes), the engine can probe a cached **season pack at the preferred tier** before considering any tier drop. This backfills the holes in one grab without any quality compromise. It's opt-in on top of the master toggle:

```bash
SEASON_PACK_FALLBACK_ENABLED=true       # requires QUALITY_COMPROMISE_ENABLED=true
SEASON_PACK_FALLBACK_MIN_MISSING=4      # absolute floor: at least this many missing
SEASON_PACK_FALLBACK_MIN_RATIO=0.4      # AND at least 40% of the season missing
```

The ratio gate (belt-and-suspenders with `MIN_MISSING`) prevents a 40-episode season with 4 holes (only 10%) from grabbing a whole-season pack when just a few episodes are legitimately missing. Set `SEASON_PACK_FALLBACK_MIN_RATIO=0.0` to disable the ratio gate and rely on `MIN_MISSING` alone.

### How to read the compromise trail

Every compromise grab is recorded in three places:

1. **Activity history** — filter the Activity page for event type `compromise_grabbed`. Each entry shows the preferred tier, the grabbed tier, the reason (`dwell_elapsed` or `season_pack_before_tier_drop`), how long the item waited at the preferred tier, and how many cached/uncached candidates existed at the preferred tier.

2. **Library detail page** — titles with a recent compromise grab show a small `↓ <tier>` pill next to the normal quality badge (e.g. `↓ 1080p` when the preferred was 2160p). Hover for the full tooltip: "Compromised from 2160p — reason=dwell_elapsed".

3. **API endpoint** — `GET /api/blackhole/compromises` returns the latest 50 compromise events as structured JSON (title, episode, preferred_tier, grabbed_tier, reason, strategy, timestamp, dwell_days, cached/uncached candidate counts). Useful for dashboards or post-mortem analysis.

All three surfaces fire regardless of `QUALITY_COMPROMISE_NOTIFY` — if you silence the Apprise notification, you don't lose the audit trail.

### How to roll back

Flip the master toggle off:

```bash
QUALITY_COMPROMISE_ENABLED=false
docker kill -s HUP pd_zurg
```

The blackhole returns to pre-feature behavior immediately: no tier escalation, no season-pack probes, no new `compromise_grabbed` events. **No data migration is required** — existing `.meta` sidecars with `tier_state` are simply ignored by the decision loop, and they keep working if you flip the toggle back on later. Past `compromise_grabbed` history events stay in the log (they're audit records, not operational state); the `↓ <tier>` badge on the library detail page fades out naturally as those events age past the history retention window.

## Verification

### Step 1: Check pd_zurg logs at startup

```
[blackhole] Watching /watch (poll: 5s, service: realdebrid)
[blackhole] Symlink mode enabled: completed=/completed, mount=/data/pd_zurg, target_base=/mnt/debrid, timeout=300s, interval=10s, max_age=72h
```

If you see this, the blackhole with symlink mode is running.

### Step 2: Test with a known cached torrent

Search for a popular, fully-released show in Sonarr. Trigger a manual search and grab a release. Or, for a quick test, drop a `.magnet` file directly into the blackhole:

```bash
echo 'magnet:?xt=urn:btih:YOUR_HASH_HERE&dn=Test' > /opt/blackhole/test.magnet
```

### Step 3: Watch the logs

You should see this sequence:
```
[blackhole] Processing: test.magnet
[blackhole] Added to realdebrid: test.magnet
[blackhole] Monitoring torrent XXXXX for test.magnet
[blackhole] Torrent ready: test.magnet (release: Release.Name)
[blackhole] Found on mount: /data/pd_zurg/shows/Release.Name (category: shows)
[blackhole] Symlink: episode.mkv -> /mnt/debrid/shows/Release.Name/episode.mkv
[blackhole] Created N symlink(s) for Release.Name
```

### Step 4: Verify symlinks resolve

On the Sonarr/Radarr host:
```bash
ls -la /opt/completed/
# Should show release folders

ls -la /opt/completed/Release.Name/
# Should show symlinks pointing to /mnt/debrid/...

stat -L /opt/completed/Release.Name/episode.mkv
# Should show the actual file size (not "No such file")
```

### Step 5: Verify Sonarr can see them

In Sonarr, go to **Activity → Queue**. You should see the download appear as "completed" and Sonarr should begin importing it.

## Troubleshooting

### Symlinks not being created

**Check the logs for "Waiting for ... on mount":**
This means pd_zurg submitted the torrent to debrid and it's ready, but the content hasn't appeared on the rclone mount yet. Possible causes:
- `BLACKHOLE_RCLONE_MOUNT` is wrong. If `RCLONE_MOUNT_NAME=pd_zurg`, the mount is at `/data/pd_zurg`, not `/data`.
- Zurg hasn't detected the new torrent yet. Zurg checks for changes every N seconds (configured via `check_for_changes_every_secs` in zurg config). Wait or reduce this interval.
- The release name from the debrid API doesn't match the folder name on the mount. Check what Zurg created vs what the API returned.

**Check the logs for "Timeout waiting for debrid":**
The torrent isn't finishing on the debrid side. Possible causes:
- The torrent is not cached on the debrid service (needs to download from peers — can take a long time)
- The debrid API key is invalid or the account has issues
- Increase `BLACKHOLE_MOUNT_POLL_TIMEOUT` for uncached torrents

### Symlinks are created but broken (dangling)

**Check the symlink target path:**
```bash
readlink /opt/completed/Release.Name/episode.mkv
```

The target should point to a path that exists on **every host that reads symlinks** (Plex, Sonarr, Radarr). Common issues:
- `BLACKHOLE_SYMLINK_TARGET_BASE` doesn't match the actual mount path on the host — verify with `ls /mnt/debrid/` (or whatever your base is)
- Multi-host setup: the path resolves on one host but not another. Create a symlink on the missing host: `sudo ln -s /actual/mount/path /mnt/debrid`
- The rclone/WebDAV mount is down
- The debrid content was removed (expired, manually deleted)

### Sonarr says "Watch Folder is not writable"

The `/completed` directory (as seen inside the Sonarr container) needs to be writable by Sonarr's user (PUID/PGID). Fix:
```bash
sudo chmod 777 /opt/completed
```

### Sonarr finds releases but "0 reports downloaded"

Check that:
- The download client has the correct tags matching the series
- The indexers return torrent results (not just usenet)
- Releases aren't blocklisted (check **Activity → Blocklist**)
- The download client is enabled and passes the test

### Content on debrid mount but not found by pd_zurg

The release name from the debrid API may not match the folder name Zurg creates. Check:
```bash
# What the API returned (in pd_zurg logs):
# "release: Something.S01E01.1080p.WEB.mkv"

# What Zurg created:
docker exec pd_zurg ls /data/pd_zurg/shows/ | grep -i "something"
# "Something.S01E01.1080p.WEB"  ← Zurg stripped the .mkv extension
```

pd_zurg handles this automatically by trying both the original name and the extension-stripped name. If you're still having issues, check if there are other naming differences.

### Multi-host: NFS "access denied"

Make sure:
- The NFS server host has exported the directory: `sudo exportfs -v`
- The NFS client host IP is in the export list
- NFS is running: `sudo systemctl status nfs-server`
- After changing exports: `sudo exportfs -ra`

## Auto-Symlinks for Debrid-Only Content

In addition to the blackhole pipeline (where Sonarr/Radarr submit torrents), pd_zurg can automatically create symlinks for content that was added to debrid through other means — direct uploads, other tools, or shared debrid accounts.

### How it works

When `BLACKHOLE_SYMLINK_ENABLED=true` and local library paths are configured (`BLACKHOLE_LOCAL_LIBRARY_TV` / `BLACKHOLE_LOCAL_LIBRARY_MOVIES`), the library scanner runs after each scan and:

1. Identifies shows/movies that exist on the debrid mount but have no local presence (source = debrid only)
2. Creates organized symlink structures in the local library:
   - **TV**: `{local_tv}/Show Name (Year)/Season XX/filename.mkv` → debrid mount
   - **Movies**: `{local_movies}/Movie Name (Year)/filename.mkv` → debrid mount
3. Uses Sonarr/Radarr's canonical folder names when the show/movie is already tracked by the arr
4. Triggers a Sonarr `RescanSeries` / Radarr `RescanMovie` command so the arr picks up the new files

### Requirements

All of these must be set:

| Variable | Purpose | Example |
|----------|---------|---------|
| `BLACKHOLE_SYMLINK_ENABLED` | Enable symlink features | `true` |
| `BLACKHOLE_RCLONE_MOUNT` | rclone mount path inside pd_zurg container | `/data/pd_zurg` |
| `BLACKHOLE_SYMLINK_TARGET_BASE` | Same mount as seen by Sonarr/Radarr | `/mnt/debrid` |
| `BLACKHOLE_LOCAL_LIBRARY_TV` | Local TV library (must be **read-write**) | `/data/media/tv` |
| `BLACKHOLE_LOCAL_LIBRARY_MOVIES` | Local movie library (must be **read-write**) | `/data/media/movies` |

For the Sonarr/Radarr rescan trigger, also configure `SONARR_URL`/`SONARR_API_KEY` and/or `RADARR_URL`/`RADARR_API_KEY`.

### Important: read-write mounts

The local library paths **must be mounted read-write** in the pd_zurg container. If they are read-only (`:ro`), symlink creation will fail with `Read-only file system` errors. In your pd_zurg `docker-compose.yml`:

```yaml
volumes:
  - /path/to/tv:/data/media/tv        # NO :ro — must be writable
  - /path/to/movies:/data/media/movies # NO :ro — must be writable
```

### What about shows/movies not in Sonarr/Radarr?

Content that isn't tracked by Sonarr/Radarr will still get symlinks created (using the parsed torrent title). These will appear as "unmapped folders" in the arr's Library Import section, where you can review and import them.

## FAQ

### Does this use any local disk space?

Virtually none. Symlinks are a few bytes each. The actual media files live on the debrid service and are streamed via the Zurg/rclone mount.

### What happens when a debrid torrent expires or is deleted?

The symlinks become broken (dangling). The cleanup task runs every 5 minutes and removes broken symlinks automatically. Sonarr/Radarr will see the episode as "missing" and can re-grab it.

### Can I use this alongside qBittorrent/NZBGet?

Yes. Use tags in Sonarr/Radarr to route specific series/movies to the blackhole client and others to your traditional download clients. Series without the debrid tag will use your other clients as normal.

### What about plex_debrid — do I still need it?

No. The blackhole symlink feature replaces plex_debrid's content acquisition role. You can disable it with `PD_ENABLED=false`. The blackhole handles the debrid submission, and Sonarr/Radarr handle the content discovery and library management.

### Does this work with Jellyfin/Emby?

Yes. Any media server that follows symlinks will work. Point your library at the directory where Sonarr/Radarr import the symlinks (your media root folder, e.g., `/data/media/tv`).

### How fast is it for cached torrents?

Nearly instant. Cached torrents on Real-Debrid are available in 1-2 seconds. The full pipeline (submit → ready → mount scan → symlink creation) typically completes in under 10 seconds.

### What if I only run pd_zurg and Sonarr on the same host?

The setup is simpler — you don't need NFS. Just use the same host directory for both containers:
```yaml
# pd_zurg
volumes:
  - /opt/blackhole:/watch
  - /opt/completed:/completed

# Sonarr
volumes:
  - /opt/blackhole:/watch
  - /opt/completed:/completed
```

### What about Bazarr (subtitles)?

Bazarr pulls root folder paths from Sonarr/Radarr. If Sonarr/Radarr have a debrid root folder (e.g., `/mnt/debrid/shows`), Bazarr will show a health warning because it can't access that path. This is cosmetic — Bazarr works normally for content that Sonarr/Radarr have already imported to your local media library (e.g., on TrueNAS or local disk). Subtitles cannot be written directly to the debrid mount since it's read-only.
