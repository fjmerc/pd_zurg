# Configuration Reference

Every environment variable Zurgarr reads, grouped by feature. This is the
long reference — for a first-time walkthrough see the
[README](README.md) or the
[Blackhole Symlink Guide](BLACKHOLE_SYMLINK_GUIDE.md).

## How to set a variable

Three options, any of them work:

1. **`.env` file** next to `docker-compose.yml` (recommended). The commented
   [`.env.example`](.env.example) is a ready-to-copy template.
2. **`environment:`** block in `docker-compose.yml` — useful for values
   you share across containers.
3. **Web UI** at `http://your-host:8080/settings` after you've set
   `STATUS_UI_AUTH`. Saves to `.env` and applies most changes without a
   restart (SIGHUP reload).

## Minimum required to start

| Variable | Why |
|---|---|
| `RD_API_KEY` (or `AD_API_KEY` or `TORBOX_API_KEY`) | Zurg can't connect without a debrid account |
| `STATUS_UI_AUTH` | Unlocks the web UI at `/settings`. Format: `user:password` |

Everything else has a sensible default or is opt-in.

---

## Debrid & mount (always required)

| Variable | Description | Default |
|---|---|---|
| `TZ` | [Timezone](http://en.wikipedia.org/wiki/List_of_tz_database_time_zones) | |
| `ZURG_ENABLED` | Enable Zurg | `false` |
| `RD_API_KEY` | [Real-Debrid API key](https://real-debrid.com/apitoken) | |
| `AD_API_KEY` | [AllDebrid API key](https://alldebrid.com/apikeys/) | |
| `TORBOX_API_KEY` | [TorBox API key](https://torbox.app/settings) | |
| `RCLONE_MOUNT_NAME` | Name for the rclone mount | |
| `RCLONE_LOG_LEVEL` | [rclone log level](https://rclone.org/docs/#log-level-level). `OFF` to suppress | `NOTICE` |
| `RCLONE_DIR_CACHE_TIME` | [Directory cache duration](https://rclone.org/commands/rclone_mount/#vfs-directory-cache) | `10s` |
| `RCLONE_CACHE_DIR` | [Cache directory](https://rclone.org/docs/#cache-dir-dir) | |
| `RCLONE_VFS_CACHE_MODE` | [VFS cache mode](https://rclone.org/commands/rclone_mount/#vfs-file-caching) | `off` (FUSE) / `full` (NFS) |
| `RCLONE_VFS_CACHE_MAX_SIZE` | Max VFS cache size | |
| `RCLONE_VFS_CACHE_MAX_AGE` | Max VFS cache age | |
| `RCLONE_VFS_READ_CHUNK_SIZE` | Initial read chunk size | |
| `RCLONE_VFS_READ_CHUNK_SIZE_LIMIT` | Max read chunk size | |
| `RCLONE_BUFFER_SIZE` | Buffer size for transfers | |
| `RCLONE_TRANSFERS` | Parallel transfers | |
| `ZURG_VERSION` | Pin Zurg version or `nightly` (requires `GITHUB_TOKEN`) | `latest` |
| `ZURG_UPDATE` | Auto-update Zurg on startup | `false` |
| `ZURG_LOG_LEVEL` | Zurg log level. `OFF` to suppress | `INFO` |
| `ZURG_USER` | WebDAV basic auth username | |
| `ZURG_PASS` | WebDAV basic auth password | |
| `ZURG_PORT` | WebDAV port. Set a fixed value if exposing to other machines | random |
| `NFS_ENABLED` | Enable rclone NFS server (does NOT create a local mount — use FUSE if Plex is on the same host) | `false` |
| `NFS_PORT` | NFS server port | random |

---

## plex_debrid (watchlist automation)

| Variable | Description | Default |
|---|---|---|
| `PD_ENABLED` | Enable plex_debrid | `false` |
| `PLEX_USER` | Plex username | |
| `PLEX_TOKEN` | Plex token | |
| `PLEX_ADDRESS` | Plex server URL. Must include scheme, no trailing `/` | |
| `JF_ADDRESS` | Jellyfin/Emby URL (alternative to Plex) | |
| `JF_API_KEY` | Jellyfin/Emby API key | |
| `SEERR_API_KEY` | Overseerr/Jellyseerr API key | |
| `SEERR_ADDRESS` | Overseerr/Jellyseerr URL | |
| `SHOW_MENU` | Show plex_debrid interactive menu on startup | `true` |
| `PD_UPDATE` | Auto-update plex_debrid. Requires `PD_REPO` | `false` |
| `PD_REPO` | Update repo in `user,repo,branch` form | |
| `PD_LOG_LEVEL` | Log level (`DEBUG`/`INFO`/`OFF`) | `INFO` |
| `PD_LOGFILE` | Path for plex_debrid log output | |
| `TRAKT_CLIENT_ID` | Trakt API client ID (uses upstream default if unset) | |
| `TRAKT_CLIENT_SECRET` | Trakt API client secret | |
| `FLARESOLVERR_URL` | [FlareSolverr](https://github.com/FlareSolverr/FlareSolverr) URL for Cloudflare-protected indexers | |
| `PD_ENFORCE_CACHED_VERSIONS` | On startup, add a "cache-required" rule to every plex_debrid content version missing it. Stops uncached fallback grabs. Idempotent. **Recommended ON for RD users** — see [TROUBLESHOOTING](TROUBLESHOOTING.md#dmm-shows-torrents-at-0--with-no-seeds) | `false` |

---

## Plex library management

| Variable | Description | Default |
|---|---|---|
| `PLEX_REFRESH` | Auto-refresh Plex libraries after mount changes | `false` |
| `PLEX_MOUNT_DIR` | Mount path as Plex sees it (for library refresh) | |
| `DUPLICATE_CLEANUP` | Automated Plex duplicate detection + cleanup | `false` |
| `CLEANUP_INTERVAL` | Hours between duplicate cleanup runs | `24` |
| `DUPLICATE_CLEANUP_KEEP` | `local` (logs Zurg dupes) or `zurg` (deletes local copy) | `local` |
| `AUTO_UPDATE_INTERVAL` | Hours between auto-update checks | `24` |

---

## Blackhole (Sonarr/Radarr integration)

See the [Blackhole Symlink Guide](BLACKHOLE_SYMLINK_GUIDE.md) for full setup.

| Variable | Description | Default |
|---|---|---|
| `BLACKHOLE_ENABLED` | Enable blackhole watch folder | `false` |
| `BLACKHOLE_DIR` | Watch dir for `.torrent`/`.magnet`. Supports per-arr label subdirs (`sonarr/`, `radarr/`) — see the [Blackhole Guide](BLACKHOLE_SYMLINK_GUIDE.md) | `/watch` |
| `BLACKHOLE_POLL_INTERVAL` | Seconds between folder scans | `5` |
| `BLACKHOLE_DEBRID` | Debrid service: `realdebrid`, `alldebrid`, `torbox`. Auto-detected if unset | auto |
| `BLACKHOLE_SYMLINK_ENABLED` | Enable symlink creation after download | `false` |
| `BLACKHOLE_COMPLETED_DIR` | Staging directory for completed symlinks. Under per-arr label layout, symlinks are nested (`.../sonarr/`, `.../radarr/`). Flat layout works when no label subdirs | `/completed` |
| `BLACKHOLE_RCLONE_MOUNT` | rclone mount path inside the container. Append mount name (e.g. `/data/zurgarr`) | `/data` |
| `BLACKHOLE_SYMLINK_TARGET_BASE` | Mount path as seen by Plex/Sonarr/Radarr hosts. **Required** for symlink mode | |
| `BLACKHOLE_MOUNT_POLL_TIMEOUT` | Max seconds to wait for content on mount | `300` |
| `BLACKHOLE_MOUNT_POLL_INTERVAL` | Seconds between mount checks | `10` |
| `BLACKHOLE_SYMLINK_MAX_AGE` | Hours before old symlinks are cleaned up | `72` |
| `SYMLINK_REPAIR_AUTO_SEARCH` | Trigger arr re-search when a broken symlink can't be repaired from mount | `false` |

### Local-library dedup (skip content you already have)

Compares the torrent name against files already in a local library path.

| Variable | Description | Default |
|---|---|---|
| `BLACKHOLE_DEDUP_ENABLED` | Skip torrents that match content in the local library | `false` |
| `BLACKHOLE_LOCAL_LIBRARY_TV` | Container path to TV library | |
| `BLACKHOLE_LOCAL_LIBRARY_MOVIES` | Container path to movie library | |

### Debrid-account dedup + cache gate

Different from local-library dedup — these gate the add on the debrid
account itself. See
[TROUBLESHOOTING](TROUBLESHOOTING.md#dmm-shows-torrents-at-0--with-no-seeds)
for recommended settings per provider.

| Variable | Description | Default |
|---|---|---|
| `BLACKHOLE_DEBRID_DEDUP_ENABLED` | Skip if the hash is already on the debrid account. Stops Sonarr/Radarr re-grabs from producing duplicate entries in DMM | `true` |
| `BLACKHOLE_REQUIRE_CACHED` | Refuse `.torrent`/`.magnet` drops that aren't confirmed cached. **RD users leave OFF** (RD deprecated its cache probe Nov 2024); AD/TB users can turn this ON | `false` |
| `BLACKHOLE_DELETE_UNCACHED_ON_TIMEOUT` | When the blackhole gives up waiting for debrid to cache a torrent (`BLACKHOLE_MOUNT_POLL_TIMEOUT`), actively delete it from the debrid account instead of leaving it as a 0%/0-seed entry. **Recommended ON for RD users** — see [TROUBLESHOOTING](TROUBLESHOOTING.md#uncached-torrents-pile-up-on-my-debrid-account-from-the-blackhole) | `false` |

### Quality compromise + season-pack fallback (opt-in)

Cache-aware tier escalation for strict Sonarr/Radarr profiles.
`QUALITY_COMPROMISE_ENABLED` is the master switch — every other variable
here is inert while it's OFF.

| Variable | Description | Default |
|---|---|---|
| `QUALITY_COMPROMISE_ENABLED` | Master toggle — enables the rest of this section | `false` |
| `QUALITY_COMPROMISE_DWELL_DAYS` | Days at the preferred tier before the first compromise may fire (1–30) | `3` |
| `QUALITY_COMPROMISE_MIN_SEEDERS` | Seeder floor for compromise candidates (0–1000) | `3` |
| `QUALITY_COMPROMISE_ONLY_CACHED` | Require cached on debrid. RD users under strict mode will never compromise — flip OFF for aggressive escalation, or use AD/TB | `true` |
| `QUALITY_COMPROMISE_MAX_TIER_DROP` | Max tiers below preferred the engine may descend (1–10; 10≈unlimited — profile still the ceiling) | `2` |
| `QUALITY_COMPROMISE_NOTIFY` | Apprise notification on each compromise grab. OFF silences Apprise only — dashboard + history still fire | `true` |
| `SEASON_PACK_FALLBACK_ENABLED` | TV-only: probe a cached pack at the preferred tier before any tier drop | `false` |
| `SEASON_PACK_FALLBACK_MIN_MISSING` | Min missing-episode count before a pack probe (1–100) | `4` |
| `SEASON_PACK_FALLBACK_MIN_RATIO` | Min missing/total ratio (0.0–1.0; 0.0 disables) | `0.4` |

---

## Debrid search UI (interactive torrent search)

| Variable | Description | Default |
|---|---|---|
| `TORRENTIO_URL` | Torrentio API base URL (e.g. `https://torrentio.strem.fun`). Enables interactive search in the Library detail view with cache annotations and one-click add | |
| `SEARCH_DEDUP_ENABLED` | Before submitting an "Add" click, check the account and refuse duplicates | `true` |
| `SEARCH_REQUIRE_CACHED` | Refuse the Add button when the hash isn't confirmed cached. Same RD caveat as `BLACKHOLE_REQUIRE_CACHED` — leave OFF on RD | `false` |

---

## Library browser, preferences, gap-fill

| Variable | Description | Default |
|---|---|---|
| `TMDB_API_KEY` | [TMDB](https://www.themoviedb.org/) API key (free). Enables posters, episode titles, missing-episode detection, IMDb ID resolution | |
| `HISTORY_RETENTION_DAYS` | Days to keep activity history | `30` |
| `BLOCKLIST_AUTO_ADD` | Auto-blocklist torrents that hit terminal debrid errors, disc-rip rejection, or uncached-timeout failures | `true` |
| `BLOCKLIST_EXPIRY_DAYS` | Auto-expire auto-added blocklist entries after N days (0=never). Manual entries kept forever | `0` |
| `LIBRARY_PREFERENCE_AUTO_ENFORCE` | Auto-switch sources when content arrives matching a stored preference | `false` |
| `DEBRID_UNAVAILABLE_THRESHOLD_DAYS` | Days of failed searches before marking content debrid-unavailable | `3` |
| `PENDING_WARNING_HOURS` | Hours before `pending_warning` notification for stuck items (0 disables) | `24` |
| `GAP_FILL_ENABLED` | Reconcile monitored shows against TMDB and search Sonarr/Radarr for aired episodes missing from both sources. Also auto-enables `verify_symlinks` re-search on broken symlinks | `true` |

---

## Media services (Sonarr/Radarr)

| Variable | Description | Default |
|---|---|---|
| `SONARR_URL` | Sonarr base URL (e.g. `http://sonarr:8989`) | |
| `SONARR_API_KEY` | Sonarr API key | |
| `RADARR_URL` | Radarr base URL (e.g. `http://radarr:7878`) | |
| `RADARR_API_KEY` | Radarr API key | |
| `ROUTING_AUTO_TAG_UNTAGGED` | During the routing audit, auto-apply the debrid tag to monitored Sonarr series / Radarr movies with no routing tag — self-heals Overseerr requests that arrive with empty tags | `true` |

---

## Status UI & monitoring

| Variable | Description | Default |
|---|---|---|
| `STATUS_UI_ENABLED` | Enable the status web dashboard | `false` |
| `STATUS_UI_PORT` | Dashboard port | `8080` |
| `STATUS_UI_AUTH` | `user:password`. **Required** for the settings editor | |
| `FFPROBE_MONITOR_ENABLED` | Detect stuck ffprobe processes on debrid mounts | `true` |
| `FFPROBE_STUCK_TIMEOUT` | Seconds before ffprobe is considered stuck | `300` |
| `FFPROBE_POLL_INTERVAL` | Seconds between ffprobe monitor scans | `30` |

---

## Notifications

| Variable | Description | Default |
|---|---|---|
| `NOTIFICATION_URL` | [Apprise](https://github.com/caronc/apprise) URL(s), comma-separated | |
| `NOTIFICATION_EVENTS` | Comma-separated event subscription (leave empty for all). Known events: `startup`, `shutdown`, `download_complete`, `download_error`, `library_refresh`, `symlink_created`, `symlink_failed`, `debrid_unavailable`, `local_fallback_triggered`, `blocklist_added`, `health_error`, `symlink_repaired`, `daily_digest`, `debrid_add_success`, `debrid_add_failed` | all |
| `NOTIFICATION_LEVEL` | Minimum severity: `info`, `warning`, `error` | `info` |
| `NOTIFICATION_DIGEST_ENABLED` | Daily summary instead of individual notifications | `false` |
| `NOTIFICATION_DIGEST_TIME` | When to send the digest (24h format) | `08:00` |

---

## Scheduled task intervals (advanced)

All intervals are in minutes unless noted. Defaults are tuned for
homelab-scale installs — most users don't need to touch these.

| Variable | Description | Default |
|---|---|---|
| `ROUTING_AUDIT_INTERVAL` | Minutes between Sonarr/Radarr routing audits (debrid-tag self-heal) | `360` (6h) |
| `QUEUE_CLEANUP_INTERVAL` | Minutes between Sonarr/Radarr queue cleanup passes | `60` |
| `LIBRARY_SCAN_INTERVAL` | Minutes between library scans | `60` |
| `SYMLINK_VERIFY_INTERVAL` | Minutes between symlink verification sweeps | `360` (6h) |
| `PREFERENCE_ENFORCE_INTERVAL` | Minutes between preference-enforcement passes | `60` |
| `HOUSEKEEPING_INTERVAL` | Minutes between housekeeping (history prune, cache rotation) | `1440` (24h) |
| `CONFIG_BACKUP_INTERVAL` | Seconds between scheduled config backups (archive of `.env`, `settings.json`, `library_prefs.json`, `blocklist.json`). `0` disables scheduled backups — manual backup/restore in the Settings UI still work | `86400` (24h) |
| `CONFIG_BACKUP_RETENTION` | Number of scheduled backup archives to retain. Older ones are pruned after each run | `7` |
| `CONFIG_BACKUP_DIR` | Directory that receives scheduled backup archives. Pre-restore snapshots also land here (under `pre-restore-<timestamp>/` subdirs) | `/config/backups` |
| `MOUNT_LIVENESS_INTERVAL` | Minutes between rclone mount liveness probes | `5` |

---

## Logging & advanced

| Variable | Description | Default |
|---|---|---|
| `ZURGARR_LOG_LEVEL` | [Zurgarr log level](https://docs.python.org/3/library/logging.html#logging-levels) | `INFO` |
| `ZURGARR_LOG_COUNT` | Rotated log files to retain | `2` |
| `ZURGARR_LOG_SIZE` | Max log file size before rotation (`K`/`M`/`G`) | `10M` |
| `COLOR_LOG_ENABLED` | Colored console output | `false` |
| `GITHUB_TOKEN` | [GitHub token](https://github.com/settings/tokens) — avoids rate limits, required for Zurg nightly | |
| `SKIP_VALIDATION` | Skip startup config validation | `false` |

---

## Docker secrets

Zurgarr reads sensitive values from `/run/secrets/<name>` when the env
var isn't set. Supported names: `github_token`, `rd_api_key`,
`ad_api_key`, `torbox_api_key`, `plex_user`, `plex_token`,
`plex_address`, `jf_api_key`, `jf_address`, `seerr_api_key`,
`seerr_address`.

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

Remove the corresponding env vars from `.env` when using secrets.

---

## Data volumes

| Container path | Permissions | Description |
|---|:---:|---|
| `/config` | rw | rclone.conf, plex_debrid settings.json, persistent state. `rclone.conf` is overwritten on start — don't share with other rclone instances |
| `/log` | rw | Log files |
| `/cache` | rw | rclone VFS cache (when `RCLONE_VFS_CACHE_MODE` is set) |
| `/data` | rshared | rclone mount point. Not needed if only using plex_debrid |
| `/zurg/RD` | rw | Zurg Real-Debrid state |
| `/zurg/AD` | rw | Zurg AllDebrid state |
| `/watch` | rw | Blackhole watch folder (only when `BLACKHOLE_ENABLED=true`) |
| `/completed` | rw | Completed symlinks (only when `BLACKHOLE_SYMLINK_ENABLED=true`) |
