# Troubleshooting

Symptom-first. Find what you're seeing, follow the fix. If your symptom
isn't here, open a [GitHub issue](https://github.com/fjmerc/zurgarr/issues).

## Contents

- [DMM shows torrents at 0% with no seeds](#dmm-shows-torrents-at-0--with-no-seeds)
- [Duplicate torrents in my debrid account](#duplicate-torrents-in-my-debrid-account)
- [Sonarr/Radarr keeps re-grabbing the same failed torrent](#sonarrradarr-keeps-re-grabbing-the-same-failed-torrent)
- [Mount not available / empty `/data` directory](#mount-not-available--empty-data-directory)
- [Docker Desktop: mount propagation error](#docker-desktop-mount-propagation-error)
- [Plex not seeing debrid content](#plex-not-seeing-debrid-content)
- [Blackhole: symlinks created but broken](#blackhole-symlinks-created-but-broken)
- [Stuck ffprobe processes](#stuck-ffprobe-processes)
- [Migrating from pd_zurg](#migrating-from-pd_zurg)

---

## DMM shows torrents at 0% with no seeds

Those are torrents your debrid provider accepted but can't actually
stream — uncached releases that will never download. Two paths can put
them in your account: the manual "Add" button in the Library search UI
and the Sonarr/Radarr blackhole watcher, plus the automated
`plex_debrid` flow if you use it.

**Fix, by provider:**

| Your provider | What to flip ON in Settings |
|---|---|
| **Real-Debrid** | `PD_ENFORCE_CACHED_VERSIONS`. RD has no working pre-add cache probe, so this is the only gate that works on RD — it configures plex_debrid to reject uncached releases after they're probed post-add. |
| **AllDebrid or TorBox** | `BLACKHOLE_REQUIRE_CACHED` and `SEARCH_REQUIRE_CACHED`. These use AD/TB's working cache-probe endpoints to refuse uncached releases before they're submitted. You can also flip `PD_ENFORCE_CACHED_VERSIONS` if you use plex_debrid. |

After flipping the setting, hit Save & Apply in the UI. For
`PD_ENFORCE_CACHED_VERSIONS` a plex_debrid restart is needed — the UI
will prompt you.

Already-added uncached torrents have to be deleted manually from DMM /
the RD web UI — the gates only prevent new ones.

## Duplicate torrents in my debrid account

Handled automatically as of v2.21. The `BLACKHOLE_DEBRID_DEDUP_ENABLED`
and `SEARCH_DEDUP_ENABLED` settings both default to **ON** — before any
add, Zurgarr checks whether the hash is already on your account and
skips if it is.

If you're still seeing duplicates:

- Confirm both settings are ON in the Settings UI (they should be unless
  you explicitly turned them off).
- Remember the existing duplicates won't clean themselves up — only new
  adds are gated. Clear the backlog from DMM / the RD web UI once.
- Torrents added **before** v2.21 won't be caught retroactively — the
  feature only filters new submissions.

## Sonarr/Radarr keeps re-grabbing the same failed torrent

The blocklist should prevent this. It auto-blocks torrents that hit
terminal debrid errors (`BLOCKLIST_AUTO_ADD` defaults to **ON**).

- Check `http://your-host:8080/blocklist` — the offending hash should
  be there.
- If `BLOCKLIST_EXPIRY_DAYS` is non-zero, auto-added entries expire
  after that many days. Set it to `0` to keep them forever.
- Manual entries in `/blocklist` are never expired.

If the re-grabs keep happening for different hashes of the same release
(quality variants), that's Sonarr/Radarr's normal retry behavior after
import failures — check your arr's Activity → Queue tab for stuck items.

## Mount not available / empty `/data` directory

- Ensure `/dev/fuse` is mapped and the container has `SYS_ADMIN`
  capability. Docker Compose:
  ```yaml
  devices:
    - /dev/fuse:/dev/fuse:rwm
  cap_add:
    - SYS_ADMIN
  security_opt:
    - apparmor:unconfined
    - no-new-privileges
  ```
- Check rclone logs: `docker logs zurgarr 2>&1 | grep rclone`
- Verify your debrid API key is valid and the account is active.

## Docker Desktop: mount propagation error

Docker Desktop doesn't support the `rshared` mount propagation rclone
needs. Options:

- Use a Linux VM, WSL2, or bare-metal Docker.
- The [upstream pd_zurg wiki](https://github.com/I-am-PUID-0/pd_zurg/wiki/Setup-Guides)
  has WSL2 setup instructions — most steps still apply.

## Plex not seeing debrid content

- The Plex library must point to the rclone mount shared from Zurgarr
  (the `./mnt:/data:shared` bind in `docker-compose.yml`).
- If your Plex container uses `depends_on: service_healthy`, make sure
  Zurgarr's healthcheck is passing first.
- Try `PLEX_REFRESH=true` with `PLEX_MOUNT_DIR` set to the mount path
  **as Plex sees it** (not the path inside the Zurgarr container).

## Blackhole: symlinks created but broken

The symlinks are absolute paths rooted at `BLACKHOLE_SYMLINK_TARGET_BASE`.
That path must resolve on every host reading the symlinks — Plex,
Sonarr, Radarr — not just inside the Zurgarr container.

- Set `BLACKHOLE_SYMLINK_TARGET_BASE` to the mount path used by your
  media-server hosts (e.g. `/mnt/debrid`).
- If hosts use different mount paths, create a symlink on each:
  `ln -s /actual/mount/path /mnt/debrid`
- Verify the rclone/WebDAV mount is accessible from the host running
  Plex/Sonarr/Radarr — try `ls /mnt/debrid/` from there.

See the [Blackhole Symlink Guide](BLACKHOLE_SYMLINK_GUIDE.md#troubleshooting)
for detailed diagnostics.

## Stuck ffprobe processes

Normal when Plex scans expired debrid links — the monitor handles it
automatically. If you see false positives during large library scans,
increase `FFPROBE_STUCK_TIMEOUT` (default 300s).

## Migrating from pd_zurg

### Compose file

- Rename `container_name`, `image`, and the service key:
  `pd_zurg` → `zurgarr`. Old Docker Hub images under `fjmerc/pd_zurg`
  remain accessible; new pushes go to `fjmerc/zurgarr`.

### Mount path

- The default rclone mount name changed from `pd_zurg` to `zurgarr`. If
  you set `RCLONE_MOUNT_NAME` explicitly, nothing changes. If you
  relied on the default, your mount path becomes `/data/zurgarr` —
  update `BLACKHOLE_RCLONE_MOUNT` and `PLEX_MOUNT_DIR` accordingly.

### Env vars (2.20.0 hard break)

Env var keys, Prometheus metric names, localStorage keys, on-disk
sidecar extensions, and the internal logger channel / log filename have
all completed their rename to the `zurgarr` / `ZURGARR` namespace as of
**2.20.0**.

Upgrading directly from pd_zurg (pre-2.19) requires user action before
first start:

- Rename any `PDZURG_LOG_*` entries in `.env` to `ZURGARR_LOG_*`.
- Rewrite Grafana / Alertmanager / recording-rule queries from
  `pd_zurg_*` to `zurgarr_*`.
- Update any external log shipper or tail pipeline keyed on the
  `PDZURG-YYYY-MM-DD.log` filename pattern to `ZURGARR-YYYY-MM-DD.log`.

The 2.19.0 release provided a dual-read / dual-emit deprecation window
for the env var and metric surfaces; 2.20.0 removed it. Users
upgrading from 2.19.x who already migrated during that window need no
further action.

Stale `/log/PDZURG-*.log` files from pre-2.20 aren't rotated or
auto-cleaned by `ZURGARR_LOG_COUNT` on the new filename — delete them
manually if disk footprint matters.

### Browser auth

Browser-saved Basic Auth credentials for `/settings` may need to be
re-saved (the auth realm changed from `pd_zurg` to `Zurgarr`).
