# Blackhole Symlink Guide — Sonarr/Radarr Integration with Real-Debrid

This guide explains how to use pd_zurg's blackhole feature with symlink mode to integrate Sonarr and Radarr with Real-Debrid (or AllDebrid/TorBox). This enables zero-copy, automated media management where Sonarr/Radarr handle content discovery and tracking while debrid provides the actual media files.

## Table of Contents

- [Overview](#overview)
- [How It Works](#how-it-works)
- [Prerequisites](#prerequisites)
- [Architecture](#architecture)
  - [Single-Host Setup](#single-host-setup)
  - [Multi-Host Setup](#multi-host-setup)
- [Configuration](#configuration)
  - [pd_zurg Environment Variables](#pd_zurg-environment-variables)
  - [Docker Compose](#docker-compose)
  - [Sonarr Setup](#sonarr-setup)
  - [Radarr Setup](#radarr-setup)
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

## Architecture

### Single-Host Setup

If pd_zurg, Sonarr, and Radarr all run on the **same Docker host**, the setup is straightforward — all containers share directories via Docker bind mounts.

```
Docker Host
├── pd_zurg container
│   ├── /watch       ← blackhole input (shared with Sonarr/Radarr)
│   ├── /completed   ← symlink output (shared with Sonarr/Radarr)
│   └── /data        ← rclone mount (Zurg WebDAV)
│
├── Sonarr container
│   ├── /watch       ← same directory, Sonarr writes .torrent files here
│   ├── /completed   ← same directory, Sonarr reads symlinks from here
│   └── /mnt/debrid  ← rclone mount (needed so symlink targets resolve)
│
└── Radarr container
    ├── /watch       ← same directory
    ├── /completed   ← same directory
    └── /mnt/debrid  ← rclone mount
```

**Host directory layout:**
```bash
/opt/blackhole/    # Shared blackhole watch directory
/opt/completed/    # Shared completed symlinks directory
/mnt/debrid/       # rclone FUSE mount to Zurg WebDAV
```

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

\* Required when symlink mode is enabled.

#### Understanding BLACKHOLE_SYMLINK_TARGET_BASE

This is the most important setting. Symlinks are created inside the pd_zurg container, but they must resolve on the Sonarr/Radarr host where the content is accessed.

**Inside pd_zurg**, a file might be at:
```
/data/pd_zurg/shows/Release.Name/episode.mkv
```

**On the Sonarr/Radarr host**, the same file is accessible at:
```
/mnt/debrid/shows/Release.Name/episode.mkv
```

So you set `BLACKHOLE_SYMLINK_TARGET_BASE=/mnt/debrid` — this replaces the container-internal mount path with the host-visible path in the symlink target.

**Single-host example:** If your rclone mount is at `/mnt/debrid` on the host and mounted into both pd_zurg (`/data`) and Sonarr (`/mnt/debrid`):
```
BLACKHOLE_SYMLINK_TARGET_BASE=/mnt/debrid
```

**Multi-host example:** Same concept — use the path as it appears on the Sonarr/Radarr host:
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
```

### Docker Compose

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
      - /opt/blackhole:/watch                      # blackhole input (or NFS mount)
      - /opt/completed:/completed                  # symlink output (or NFS mount)
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
      - /opt/blackhole:/watch                         # blackhole — Sonarr writes .torrent here
      - /opt/completed:/completed                     # completed — Sonarr reads symlinks from here
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
      - /opt/blackhole:/watch
      - /opt/completed:/completed
    environment:
      - PUID=1000
      - PGID=1000
      - TZ=Your/Timezone
```

### Sonarr Setup

1. Go to **Settings → Download Clients → Add → Torrent Blackhole**

2. Configure the download client:

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

Same as Sonarr — go to **Settings → Download Clients → Add → Torrent Blackhole** and use the same settings.

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

The target should point to a path that exists on the Sonarr/Radarr host. Common issues:
- `BLACKHOLE_SYMLINK_TARGET_BASE` doesn't match the actual mount path on the Sonarr/Radarr host
- The rclone/WebDAV mount is down on the Sonarr/Radarr host
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
