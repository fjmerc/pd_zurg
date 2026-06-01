"""Task implementations for the centralized task scheduler.

Each function follows the convention: returns a dict with 'status',
optional 'message', and optional 'items' count for result tracking.
"""

import os
import re
import shutil
import threading
import time
from datetime import datetime, timedelta, timezone
from utils.logger import get_logger

logger = get_logger()

try:
    from utils import history as _history
except ImportError:
    _history = None

from utils.activity_format import fmt_duration_ms


# ---------------------------------------------------------------------------
# Default intervals (seconds)
# ---------------------------------------------------------------------------

_DEFAULTS = {
    'ROUTING_AUDIT_INTERVAL': 6 * 3600,       # 6 hours
    'QUEUE_CLEANUP_INTERVAL': 15 * 60,         # 15 minutes
    'STALE_GRAB_INTERVAL': 15 * 60,            # 15 minutes
    'LIBRARY_SCAN_INTERVAL': 3600,             # 1 hour
    'SYMLINK_VERIFY_INTERVAL': 6 * 3600,       # 6 hours
    'PREFERENCE_ENFORCE_INTERVAL': 6 * 3600,   # 6 hours
    'HOUSEKEEPING_INTERVAL': 24 * 3600,        # 24 hours
    'CONFIG_BACKUP_INTERVAL': 24 * 3600,       # 24 hours
    'MOUNT_LIVENESS_INTERVAL': 60,             # 1 minute
    # Debrid health reconciler (plan 38). 12h matches a healthy 5000-torrent
    # library producing ~5000 probes per sweep at the module's 60/min limit
    # (≈85 min) — well clear of 12h, leaving headroom for other RD traffic.
    # Power-user override via env var; not surfaced in the Settings UI.
    'DEBRID_HEALTH_INTERVAL': 12 * 3600,       # 12 hours
}


def _get_interval(env_var):
    """Read interval from env, fall back to default. Value is in seconds."""
    val = os.environ.get(env_var)
    if val:
        try:
            return int(val)
        except ValueError:
            logger.warning(f"[scheduler] Invalid {env_var}={val}, using default")
    return _DEFAULTS.get(env_var, 3600)


# ---------------------------------------------------------------------------
# Task: Audit Download Routing (Priority 1)
# ---------------------------------------------------------------------------

def audit_download_routing():
    """Verify and fix download client/indexer tag routing in Sonarr and Radarr.

    Re-discovers routing tags, auto-tags untagged clients, fixes indexer
    routing, and tags usenet indexers to prevent debrid queue pollution.
    """
    from utils.arr_client import SonarrClient, RadarrClient

    services_checked = 0
    for ClientClass, name in [(SonarrClient, 'sonarr'), (RadarrClient, 'radarr')]:
        client = ClientClass()
        if not client.configured:
            continue
        try:
            client.audit_routing()
            services_checked += 1
            logger.info(f"[scheduler] Download routing audit complete for {name}")
        except Exception as e:
            logger.error(f"[scheduler] Routing audit failed for {name}: {e}")

    if services_checked == 0:
        return {'status': 'success', 'message': 'No arr services configured'}
    return {'status': 'success', 'message': f'Audited {services_checked} service(s)', 'items': services_checked}


# ---------------------------------------------------------------------------
# Task: Clean Stale Queue Items (Priority 1)
# ---------------------------------------------------------------------------

def clean_stale_queue_items():
    """Remove downloadClientUnavailable queue items older than 2 minutes."""
    from utils.arr_client import SonarrClient, RadarrClient

    total_removed = 0
    for ClientClass, name in [(SonarrClient, 'sonarr'), (RadarrClient, 'radarr')]:
        client = ClientClass()
        if not client.configured:
            continue
        try:
            removed = client.clean_all_stale_queue_items(max_age_seconds=120)
            total_removed += removed
            if removed:
                logger.info(f"[scheduler] Cleaned {removed} stale queue items from {name}")
        except Exception as e:
            logger.error(f"[scheduler] Queue cleanup failed for {name}: {e}")

    return {'status': 'success', 'message': f'Removed {total_removed} stale items', 'items': total_removed}


# ---------------------------------------------------------------------------
# Task: Library Scan (Priority 1)
# ---------------------------------------------------------------------------

def library_scan():
    """Scan debrid mount and local library, auto-create symlinks, trigger rescans."""
    from utils.library import get_scanner

    scanner = get_scanner()
    if scanner is None:
        return {'status': 'error', 'message': 'Library scanner not initialized'}

    data = scanner.scan()

    # Update the scanner cache so WebUI reflects latest data
    import threading
    with scanner._lock:
        scanner._cache = data
        scanner._cache_time = time.monotonic()

    movies = len(data.get('movies', []))
    shows = len(data.get('shows', []))
    duration_ms = data.get('scan_duration_ms', 0)

    dur_str = fmt_duration_ms(duration_ms) or '0ms'

    if _history:
        # Symlinks-created count isn't returned by scan() today; the scanner
        # logs per-title symlink_created events separately, so leave it 0
        # here rather than fabricate.
        _history.log_event('task_completed', 'Library Scan', source='scheduler',
                           detail=f'{movies} movies, {shows} shows ({dur_str})',
                           meta={'cause': 'task_library_scan',
                                 'movies': movies, 'shows': shows,
                                 'duration_ms': duration_ms})

    # Record a daily recovery snapshot (upsert-by-day) for the TB-viability
    # time series. Best-effort: snapshotting must never break the scan.
    try:
        from utils import recovery as _recovery
        _recovery.record_snapshot(data)
    except Exception as e:
        logger.debug(f"[scheduler] Recovery snapshot failed: {e}")

    return {
        'status': 'success',
        'message': f'{movies} movies, {shows} shows ({dur_str})',
        'items': movies + shows,
    }


# ---------------------------------------------------------------------------
# Task: Verify Symlinks (Priority 1)
# ---------------------------------------------------------------------------

MEDIA_EXTENSIONS = {'.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.ts', '.m4v', '.webm'}

# Track recently re-triggered arr search IDs to prevent search storms.
# Shared by verify_symlinks (repair), detect_stale_grabs, library
# symlink cleanup, and debrid_health remediation — each of those runs in
# its own daemon thread off the task scheduler, so every read/write of
# _retrigger_history must hold _retrigger_history_lock. The lock is held
# only for the in-memory ops (membership check + insert + delete); arr
# API calls happen OUTSIDE the lock to avoid serialising network I/O
# across unrelated tasks.
# Key: ('sonarr', ep_id) or ('radarr', movie_id), Value: epoch time of last trigger.
_retrigger_history = {}
_retrigger_history_lock = threading.Lock()
_RETRIGGER_COOLDOWN = 7200  # 2 hours — don't re-trigger the same item within this window

# Single-episode release pattern (S##E## with NO trailing E/digit/-, so
# multi-ep releases like S01E04E05 or S01E04-05 don't match and fall
# back to season-wide search). Used by force_episodes path to restrict
# the search to the exact ep that was filter-blocked, instead of
# triggering an N-episode search storm for a 200-ep anime season when
# only one ep was actually affected.
_SINGLE_EP_RE = re.compile(r'[.\s]S\d{1,2}E(\d{1,4})(?![E\d-])', re.IGNORECASE)


def _prune_retrigger_history():
    """Remove expired entries from the retrigger cooldown dict."""
    now = time.time()
    with _retrigger_history_lock:
        stale = [k for k, v in _retrigger_history.items() if now - v > _RETRIGGER_COOLDOWN]
        for k in stale:
            del _retrigger_history[k]

# Local library mount health tracking
_local_library_baselines = {}   # {label: True} — had real files on previous check
_local_library_alerted = {}     # {label: True} — alert already sent for this incident


def _cleanup_empty_parents(deleted_path, stop_at):
    """Remove parent directories that contain no media files, up to *stop_at*.

    After a symlink is deleted, its parent dir (e.g. "Movie Name (2025)/")
    may still contain Radarr/Sonarr metadata (.nfo, .jpg) but no video files.
    If left behind, the library scanner misclassifies it as local content and
    blocks symlink recreation.  Walk upward, removing dirs that lack media
    files, until we hit the library root.
    """
    parent = os.path.dirname(deleted_path)
    while parent and parent != stop_at and parent.startswith(stop_at + '/'):
        try:
            has_media = False
            for entry in os.scandir(parent):
                if os.path.splitext(entry.name)[1].lower() in MEDIA_EXTENSIONS:
                    has_media = True
                    break
            if has_media:
                break
            shutil.rmtree(parent, ignore_errors=True)
            logger.debug(f"[scheduler] Cleaned up empty dir: {parent}")
            parent = os.path.dirname(parent)
        except OSError:
            break


def _extract_release_info(target, debrid_prefixes):
    """Extract release name, relative file path, and category from a symlink target.

    Two layouts are recognised:
    - Zurg (RD/AD): ``<base>/<category>/<release>/<rel_file>`` →
      returns ``('<release>', '<rel_file>', '<category>')``.
    - TorBox flat: ``<base>/<release>/<rel_file>`` →
      returns ``('<release>', '<rel_file>', '')`` (empty category).

    Returns ``(None, None, None)`` if the target can't be parsed.
    """
    remainder = None
    for prefix in debrid_prefixes:
        if target.startswith(prefix):
            remainder = target[len(prefix):]
            break
    if not remainder:
        return None, None, None

    parts = remainder.split('/')
    # Path traversal rejected up-front so neither layout branch has to
    # repeat the check.  An empty path component (``//`` in the target,
    # rare but possible) collapses into the wrong release slot too —
    # reject those.
    if any(seg in ('..', '') for seg in parts):
        return None, None, None

    if len(parts) >= 3:
        # Zurg layout: <category>/<release>/<rel_file>
        return parts[1], '/'.join(parts[2:]), parts[0]
    if len(parts) == 2:
        # TorBox flat layout: <release>/<rel_file>
        return parts[0], parts[1], ''
    return None, None, None


def _find_release_on_mount(release_name, rclone_mount):
    """Search the rclone mount for a release folder.

    Returns ``(full_path, category)`` or ``(None, None)``.

    Probes categorized Zurg dirs first (``shows/movies/anime`` then
    ``__all__`` fallback) and lastly the bare mount root — the latter
    serves TorBox's flat WebDAV mount layout, where releases live
    directly under the mount root with no category subdivision.
    """
    from utils.blackhole import MOUNT_CATEGORIES

    for category in MOUNT_CATEGORIES:
        path = os.path.join(rclone_mount, category, release_name)
        if os.path.isdir(path):
            return path, category
    path = os.path.join(rclone_mount, '__all__', release_name)
    if os.path.isdir(path):
        return path, '__all__'
    # Flat-layout fallback (TorBox).
    path = os.path.join(rclone_mount, release_name)
    if os.path.isdir(path):
        return path, ''
    return None, None


def _attempt_arr_research(release_name, force_episodes=False):
    """Trigger Sonarr/Radarr search for a lost release.

    Uses ``parse_release_name`` to identify the content, then looks it up in
    the arr library and triggers a search.  Respects the shared retrigger
    cooldown to prevent search storms.

    ``force_episodes`` skips the ``hasFile`` gate on the TV branch so the
    caller can queue a search even when Sonarr's last scan still believes
    the episode is present.  Used by ``debrid_health._remediate`` because
    the just-issued ``delete_torrent`` won't drop out of Zurg's WebDAV
    listing for ~15-30 s — Sonarr's view lags the truth, and we'd
    otherwise skip every episode in the affected release.  When the
    release name carries a single-ep pattern (``S##E##``), the search is
    further narrowed to that one episode so a single blocked ep on a
    200-ep anime season doesn't fan out to 200 search jobs; multi-ep
    and season-pack releases fall through to the season-wide scope.

    On API failure the cooldown reservation is rolled back so the next
    sweep can retry instead of being silently muzzled for 2 h.

    Returns True if a search was actually triggered.
    """
    from utils.blackhole import parse_release_name
    from utils.arr_client import SonarrClient, RadarrClient

    name, season, is_tv = parse_release_name(release_name)
    if not name:
        return False

    _prune_retrigger_history()
    now_epoch = time.time()

    if is_tv:
        client = SonarrClient()
        if not client.configured:
            return False
        series = client.find_series_in_library(title=name)
        if not series:
            logger.debug(f"[scheduler] Repair: series '{name}' not found in Sonarr")
            return False

        episodes = client.get_episodes(series['id'])
        if not episodes:
            return False

        target_episode = None
        if force_episodes:
            ep_match = _SINGLE_EP_RE.search(release_name)
            if ep_match:
                target_episode = int(ep_match.group(1))

        target_eps = []
        with _retrigger_history_lock:
            for ep in episodes:
                if season is not None and ep.get('seasonNumber') != season:
                    continue
                if target_episode is not None and ep.get('episodeNumber') != target_episode:
                    continue
                if not force_episodes and ep.get('hasFile'):
                    continue
                ep_id = ep.get('id')
                if not ep_id:
                    continue
                item_key = ('sonarr', ep_id)
                if item_key in _retrigger_history:
                    continue
                target_eps.append(ep_id)
                _retrigger_history[item_key] = now_epoch

        if not target_eps:
            return False

        try:
            client.search_episodes(target_eps, media_title=name,
                                   cause='symlink_repair_research')
        except Exception:
            with _retrigger_history_lock:
                for eid in target_eps:
                    _retrigger_history.pop(('sonarr', eid), None)
            raise

        s_label = f'S{season:02d}' if season is not None else 'all'
        ep_label = f'E{target_episode:02d}' if target_episode is not None else ''
        logger.info(
            f"[scheduler] Repair: triggered Sonarr search for '{name}' "
            f"{s_label}{ep_label} ({len(target_eps)} episodes)"
        )
        return True
    else:
        client = RadarrClient()
        if not client.configured:
            return False
        movie = client.find_movie_in_library(title=name)
        if not movie:
            logger.debug(f"[scheduler] Repair: movie '{name}' not found in Radarr")
            return False

        item_key = ('radarr', movie['id'])
        with _retrigger_history_lock:
            if item_key in _retrigger_history:
                return False
            _retrigger_history[item_key] = now_epoch

        try:
            client.search_movie(movie['id'], media_title=name,
                                cause='symlink_repair_research')
        except Exception:
            with _retrigger_history_lock:
                _retrigger_history.pop(item_key, None)
            raise

        logger.info(f"[scheduler] Repair: triggered Radarr search for '{name}'")
        return True


def verify_symlinks():
    """Walk completed dir and local library for debrid-pointing symlinks, remove broken ones."""
    completed_dir = os.environ.get('BLACKHOLE_COMPLETED_DIR', '/completed')
    local_tv = os.environ.get('BLACKHOLE_LOCAL_LIBRARY_TV', '').strip()
    local_movies = os.environ.get('BLACKHOLE_LOCAL_LIBRARY_MOVIES', '').strip()
    rclone_mount = os.path.realpath(os.environ.get('BLACKHOLE_RCLONE_MOUNT', '/data'))
    symlink_target = os.environ.get('BLACKHOLE_SYMLINK_TARGET_BASE', '').strip()

    # Per-debrid (symlink_target_base, rclone_mount) pairs so that a broken
    # symlink can be translated back to the right mount for existence-checks
    # and repair.  RD/AD symlinks point at BLACKHOLE_SYMLINK_TARGET_BASE and
    # resolve to BLACKHOLE_RCLONE_MOUNT; TB symlinks point at
    # BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX (or the derived ``<RD base>_torbox``
    # fallback) and resolve to the TB rclone mount.  Without this, every TB
    # symlink looks like an unrecognised target prefix → skipped from
    # verification entirely (latent before plan 39, now load-bearing).
    from utils.blackhole import MOUNT_CATEGORIES
    from utils.debrid_routing import (
        TORBOX, mount_for_debrid, symlink_target_base_for_debrid,
    )

    target_mount_pairs = []  # list of (symlink_target_real, mount_real)
    debrid_prefixes = [rclone_mount + '/']
    if symlink_target:
        rd_target_real = os.path.realpath(symlink_target) + '/'
        debrid_prefixes.append(rd_target_real)
        target_mount_pairs.append((rd_target_real, rclone_mount))

    # TorBox pair.  ``mount_for_debrid`` needs the parent of the per-debrid
    # mount-name suffix — same convention used by BlackholeWatcher._mount_for.
    tb_target = symlink_target_base_for_debrid(TORBOX)
    tb_mount_real = ''
    if tb_target:
        tb_target_real = os.path.realpath(tb_target) + '/'
        # Resolve TB mount path from RCLONE_MOUNT_NAME parent + TORBOX_MOUNT_NAME.
        rclonemn = os.environ.get('RCLONE_MOUNT_NAME') or ''
        base = rclone_mount.rstrip('/')
        if rclonemn and os.path.basename(base) == rclonemn:
            parent = os.path.dirname(base)
            if parent:
                base = parent
        tb_mount = mount_for_debrid(TORBOX, rclone_mount_base=base) or ''
        if tb_mount and os.path.isdir(tb_mount):
            tb_mount_real = os.path.realpath(tb_mount)
            debrid_prefixes.append(tb_target_real)
            debrid_prefixes.append(tb_mount_real + '/')
            target_mount_pairs.append((tb_target_real, tb_mount_real))

    scan_dirs = []
    if os.path.isdir(completed_dir):
        scan_dirs.append(completed_dir)
    if local_tv and os.path.isdir(local_tv):
        scan_dirs.append(local_tv)
    if local_movies and os.path.isdir(local_movies):
        scan_dirs.append(local_movies)

    if not scan_dirs:
        return {'status': 'success', 'message': 'No directories to check'}

    # Guard: verify at least one configured rclone mount exists, is responsive,
    # and has content.  A missing or stalled FUSE mount makes os.path.exists
    # return False for everything, which would cause mass deletion of all
    # symlinks.  Zurg category stubs (movies/, shows/) can exist even when
    # all content is gone, so check that at least one category dir is
    # non-empty; the TB mount is flat (no categories) so we check that its
    # top-level listing is non-empty instead.  Either being healthy is
    # enough to proceed — the prefix→mount-pair routing below ensures we
    # only act on symlinks for mounts we can actually see.
    try:
        zurg_has_content = False
        if os.path.isdir(rclone_mount) and os.listdir(rclone_mount):
            zurg_has_content = any(
                os.path.isdir(os.path.join(rclone_mount, cat))
                and os.listdir(os.path.join(rclone_mount, cat))
                for cat in MOUNT_CATEGORIES
            )
        tb_has_content = False
        if tb_mount_real and os.path.isdir(tb_mount_real):
            tb_has_content = bool(os.listdir(tb_mount_real))
        if not (zurg_has_content or tb_has_content):
            logger.warning(
                f"[scheduler] No mount has content (zurg={rclone_mount!r}, "
                f"tb={tb_mount_real!r}) — aborting symlink verify to prevent mass deletion"
            )
            return {'status': 'error', 'message': 'Mounts empty, aborted'}
    except OSError as e:
        logger.error(f"[scheduler] Mount unresponsive — aborting symlink verify to prevent mass deletion: {e}")
        return {'status': 'error', 'message': f'Mount unresponsive, aborted: {e}'}

    # Phase 1: Identify broken symlinks (don't delete yet)
    to_delete = []
    checked = 0

    for scan_dir in scan_dirs:
        for root, dirs, files in os.walk(scan_dir):
            for fname in files:
                fpath = os.path.join(root, fname)
                if not os.path.islink(fpath):
                    continue

                target = os.readlink(fpath)
                # Resolve relative symlinks to absolute paths
                if not os.path.isabs(target):
                    target = os.path.realpath(os.path.join(os.path.dirname(fpath), target))
                # Only check symlinks pointing to the debrid mount or symlink target
                if not any(target.startswith(p) or target.rstrip('/') == p.rstrip('/')
                           for p in debrid_prefixes):
                    continue

                checked += 1
                # Translate the symlink target back to a local mount path
                # before checking existence.  Each (target_base, mount)
                # pair maps a host-visible Plex/arr path to the local
                # rclone-mount equivalent.  TB symlinks point at the TB
                # base and resolve to the TB mount; RD/AD symlinks point
                # at the RD base and resolve to the RD mount.  Without
                # the per-pair routing, a TB symlink translated against
                # the RD pair lands at /<RD-mount>/<TB-release> which
                # never exists → mass deletion.
                check_target, matched_mount = target, rclone_mount
                for tgt_real, mnt_real in target_mount_pairs:
                    if target.startswith(tgt_real):
                        check_target = mnt_real + '/' + target[len(tgt_real):]
                        matched_mount = mnt_real
                        break
                if not os.path.exists(check_target):
                    to_delete.append((fpath, target, scan_dir, matched_mount))

    # Phase 2: Attempt repair, then delete confirmed broken symlinks.
    # Auto-search on deletion is enabled by either the legacy opt-in flag
    # or GAP_FILL_ENABLED (default true), since re-searching disappeared
    # content is part of the "available anywhere" reconcile story.
    from utils.library import gap_fill_enabled
    auto_search = (
        os.environ.get('SYMLINK_REPAIR_AUTO_SEARCH', 'false').lower() == 'true'
        or gap_fill_enabled()
    )
    repaired = 0
    searched = 0
    deleted = 0

    for fpath, target, scan_dir, matched_mount in to_delete:
        # Step 1: Try to re-find the release on the mount it came from.
        release_name, rel_file, old_cat = _extract_release_info(target, debrid_prefixes)
        if release_name and rel_file:
            new_path, new_cat = _find_release_on_mount(release_name, matched_mount)
            if new_path and os.path.exists(os.path.join(new_path, rel_file)):
                # Rebuild the symlink target on the same target base it
                # came from — preserves the RD-vs-TB split that Plex
                # libraries depend on.
                rebuild_base = None
                for tgt_real, mnt_real in target_mount_pairs:
                    if mnt_real == matched_mount:
                        rebuild_base = tgt_real.rstrip('/')
                        break
                if rebuild_base is None:
                    rebuild_base = matched_mount
                new_target = os.path.join(rebuild_base, new_cat, release_name, rel_file)
                try:
                    tmp_link = fpath + '.repair_tmp'
                    os.symlink(new_target, tmp_link)
                    os.rename(tmp_link, fpath)
                    repaired += 1
                    logger.info(
                        f"[scheduler] Repaired symlink: {fpath} "
                        f"({old_cat!r} -> {new_cat!r})"
                    )
                    continue
                except OSError as e:
                    try:
                        os.remove(fpath + '.repair_tmp')
                    except OSError:
                        pass
                    logger.warning(f"[scheduler] Failed to repair symlink {fpath}: {e}")

        # Step 2: Content truly gone — delete
        try:
            os.remove(fpath)
            deleted += 1
            logger.info(f"[scheduler] Removed broken symlink: {fpath} -> {target}")
            if scan_dir in (local_tv, local_movies) and scan_dir:
                _cleanup_empty_parents(fpath, scan_dir)
        except OSError as e:
            logger.warning(f"[scheduler] Failed to remove broken symlink {fpath}: {e}")
            continue

        # Step 3: Optionally trigger arr re-search
        if auto_search and release_name:
            try:
                if _attempt_arr_research(release_name):
                    searched += 1
            except Exception as e:
                logger.warning(f"[scheduler] Repair re-search failed for '{release_name}': {e}")

    # Build result message
    parts = [f'Checked {checked}']
    if repaired:
        parts.append(f'repaired {repaired}')
    if searched:
        parts.append(f're-searched {searched}')
    if deleted:
        parts.append(f'removed {deleted}')
    msg = ', '.join(parts)

    if repaired or searched or deleted:
        if _history:
            _history.log_event('repair' if repaired or searched else 'cleanup',
                               'Symlink Verify', source='scheduler', detail=msg,
                               meta={'cause': 'task_verify_symlinks',
                                     'repaired': repaired,
                                     'searched': searched,
                                     'deleted': deleted,
                                     'checked': checked})
        if repaired or searched:
            try:
                from utils.notifications import notify
                notify('symlink_repaired', 'Symlink Repair',
                       msg, level='info')
            except ImportError:
                pass

    return {'status': 'success', 'message': msg, 'items': repaired + searched + deleted}


# ---------------------------------------------------------------------------
# Task: Enforce Source Preferences (Priority 2)
# ---------------------------------------------------------------------------

def enforce_source_preferences():
    """Enforce prefer-debrid/prefer-local preferences across the library."""
    from utils.library import get_scanner

    scanner = get_scanner()
    if scanner is None:
        return {'status': 'error', 'message': 'Library scanner not initialized'}

    # Run a scan with forced preference enforcement (no env var mutation)
    data = scanner.scan(force_enforce=True)
    with scanner._lock:
        scanner._cache = data
        scanner._cache_time = time.monotonic()

    movies = len(data.get('movies', []))
    shows = len(data.get('shows', []))
    return {
        'status': 'success',
        'message': f'Enforced preferences across {movies} movies, {shows} shows',
        'items': movies + shows,
    }


# ---------------------------------------------------------------------------
# Task: Housekeeping (Priority 2)
# ---------------------------------------------------------------------------

def housekeeping():
    """Clean stale state: pending badges, old retry metadata, empty dirs."""
    cleaned = 0

    # 1. Clean stale pending state
    # Normal entries (to-debrid, to-local, to-local-fallback): 7 days
    # debrid-unavailable entries: 30 days (persist until user acts or expires)
    try:
        from utils.library_prefs import get_all_pending, clear_pending
        pending = get_all_pending()
        stale_titles = []
        for title, data in pending.items():
            created = data.get('created')
            if not created:
                continue
            try:
                created_dt = datetime.fromisoformat(created)
                age_days = (datetime.now(timezone.utc) - created_dt.replace(
                    tzinfo=timezone.utc if created_dt.tzinfo is None else created_dt.tzinfo
                )).days
                max_age = 30 if data.get('direction') == 'debrid-unavailable' else 7
                if age_days > max_age:
                    stale_titles.append(title)
            except (ValueError, TypeError):
                pass
        for title in stale_titles:
            clear_pending(title)
            cleaned += 1
            logger.info(f"[scheduler] Cleared stale pending state for '{title}'")
    except Exception as e:
        logger.error(f"[scheduler] Error cleaning pending state: {e}")

    # 2. Clean empty directories in completed folder
    # Under labeled mode, the tree is completed_dir/<label>/<release>/... so
    # we walk bottom-up and re-check emptiness at each level (os.walk's
    # `dirs` list goes stale once we remove children). The top-level
    # completed_dir itself is never removed.
    completed_dir = os.environ.get('BLACKHOLE_COMPLETED_DIR', '/completed')
    try:
        if os.path.isdir(completed_dir):
            for root, _dirs, _files in os.walk(completed_dir, topdown=False):
                if root == completed_dir:
                    continue
                try:
                    if not os.listdir(root):
                        os.rmdir(root)
                        cleaned += 1
                        logger.debug(f"[scheduler] Removed empty directory: {root}")
                except OSError:
                    pass
    except Exception as e:
        logger.error(f"[scheduler] Error cleaning empty dirs: {e}")

    # 3. Clean old blackhole retry payloads + metadata in failed/.  The
    # failed/ tree holds the original .torrent/.magnet payloads alongside
    # their .meta sidecars.  ``BlackholeWatcher._retry_failed`` DOES poll
    # this tree and will retry items up to ``MAX_RETRIES`` (3) with the
    # ``RETRY_SCHEDULE`` backoff ([5m, 15m, 1h]) — so the maximum live
    # retry window for any file is ~80 minutes from the moment it lands
    # in failed/ (plus the time between scheduler ticks).  After that
    # window, the retry loop skips the file on every pass (retries >=
    # MAX_RETRIES or alt_exhausted=True), and nothing else touches it.
    # A 7-day-old payload has been terminal for >>160× the max retry
    # window — safely abandoned.  Without this sweep the payload class
    # would accumulate indefinitely while only the .meta sidecars ever
    # got cleaned, since the retry loop bumps sidecar mtime on every
    # poll.  Supports both flat (watch_dir/failed/<file>) and labeled
    # (watch_dir/failed/<label>/<file>) layouts via the recursive walk.
    # Non-retry file types are preserved so a misplaced file can be
    # inspected manually.
    now = time.time()
    watch_dir = os.environ.get('BLACKHOLE_DIR', '/watch')
    try:
        failed_root = os.path.join(watch_dir, 'failed')
        if os.path.isdir(failed_root):
            # followlinks=False (default, but explicit for defensive clarity):
            # a symlinked subdir under failed/ must not let the sweep escape
            # the tree.  Symlinked files inside failed/ are still listed under
            # *files* and their os.remove() unlinks the link rather than the
            # target, so there's no escape that way either.
            for root, _dirs, files in os.walk(failed_root, followlinks=False):
                for fname in files:
                    if not (fname.endswith('.meta')
                            or fname.endswith('.magnet')
                            or fname.endswith('.torrent')):
                        continue
                    fpath = os.path.join(root, fname)
                    try:
                        age_days = (now - os.path.getmtime(fpath)) / 86400
                        if age_days > 7:
                            os.remove(fpath)
                            cleaned += 1
                            # INFO rather than DEBUG so operators auditing
                            # "what did housekeeping delete?" don't need to
                            # flip log levels — destructive actions deserve
                            # visible logging by default.
                            logger.info(f"[scheduler] Removed stale failed/ item: {fpath}")
                    except OSError:
                        pass
    except Exception as e:
        logger.error(f"[scheduler] Error cleaning failed/ items: {e}")

    # 4. Rotate history log
    try:
        if _history:
            _history.rotate()
    except Exception as e:
        logger.error(f"[scheduler] Error rotating history: {e}")

    # 5. Expire old auto-added blocklist entries
    try:
        from utils import blocklist as _blocklist_mod
        expired = _blocklist_mod.expire()
        if expired:
            cleaned += expired
    except Exception as e:
        logger.error(f"[scheduler] Error expiring blocklist entries: {e}")

    if cleaned and _history:
        _history.log_event('task_completed', 'Housekeeping', source='scheduler',
                           detail=f'Cleaned {cleaned} item(s)',
                           meta={'cause': 'task_housekeeping',
                                 'cleaned': cleaned})

    return {'status': 'success', 'message': f'Cleaned {cleaned} items', 'items': cleaned}


# ---------------------------------------------------------------------------
# Task: Detect Stale Grabs (Priority 1)
# ---------------------------------------------------------------------------

def detect_stale_grabs():
    """Detect Sonarr/Radarr grabs that silently failed to reach the blackhole.

    Compares recent 'grabbed' history events against live episode/movie state
    (not the snapshot in history). If a grab is older than 10 minutes but the
    content still has no file, re-triggers a search. Each item is only
    re-triggered once per 2-hour window to prevent search storms.
    """
    import datetime as dt
    from utils.arr_client import SonarrClient, RadarrClient

    stale_found = 0
    searches_triggered = 0
    now_epoch = time.time()

    _prune_retrigger_history()

    for ClientClass, name in [
        (SonarrClient, 'sonarr'),
        (RadarrClient, 'radarr'),
    ]:
        client = ClientClass()
        if not client.configured:
            continue

        grabs = client.get_recent_grabs(page_size=200)
        if not grabs:
            continue

        now = dt.datetime.now(dt.timezone.utc)
        for record in grabs:
            # Only check grabs older than 10 minutes
            date_str = record.get('date', '')
            try:
                grab_time = dt.datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                age_minutes = (now - grab_time).total_seconds() / 60
                if age_minutes < 10:
                    continue
                # Only check grabs from last 2 hours
                if age_minutes > 120:
                    continue
            except (ValueError, TypeError):
                continue

            # Only act on blackhole grabs
            data = record.get('data', {})
            dl_client = data.get('downloadClient', '')
            if 'blackhole' not in dl_client.lower():
                continue

            # Fetch LIVE state (history embeds a snapshot, not current hasFile)
            if name == 'sonarr':
                ep_data = record.get('episode', {})
                ep_id = ep_data.get('id')
                if not ep_id:
                    continue
                live = client._get(f'/api/v3/episode/{ep_id}')
                if live and live.get('hasFile'):
                    continue
                item_key = ('sonarr', ep_id)
            else:
                movie_data = record.get('movie', {})
                movie_id = movie_data.get('id')
                if not movie_id:
                    continue
                live = client._get(f'/api/v3/movie/{movie_id}')
                if live and live.get('hasFile'):
                    continue
                item_key = ('radarr', movie_id)

            source_title = record.get('sourceTitle', '?')[:60]
            stale_found += 1

            # Dedup: skip if already re-triggered recently. Lock held
            # only for the membership check; the API call below releases
            # before re-acquiring on the set side.
            with _retrigger_history_lock:
                if item_key in _retrigger_history:
                    continue

            # Re-trigger search
            if name == 'sonarr':
                sn = ep_data.get('seasonNumber', 0)
                en = ep_data.get('episodeNumber', 0)
                logger.info(
                    f"[scheduler] Stale grab detected: {source_title} "
                    f"(S{sn:02d}E{en:02d}, grabbed {int(age_minutes)}m ago) — re-triggering search"
                )
                client.search_episodes([ep_id], media_title=source_title,
                                       cause='stale_grab_retry')
            else:
                logger.info(
                    f"[scheduler] Stale grab detected: {source_title} "
                    f"(grabbed {int(age_minutes)}m ago) — re-triggering search"
                )
                client.search_movie(movie_id, media_title=source_title,
                                    cause='stale_grab_retry')

            with _retrigger_history_lock:
                _retrigger_history[item_key] = now_epoch
            searches_triggered += 1

    msg = f'Found {stale_found} stale grabs'
    if searches_triggered:
        msg += f', re-triggered {searches_triggered} searches'
        if _history:
            _history.log_event('task_completed', 'Stale Grab Detection', source='scheduler',
                               detail=msg,
                               meta={'cause': 'task_stale_grab_detection',
                                     'stale_found': stale_found,
                                     'searches_triggered': searches_triggered})
    return {'status': 'success', 'message': msg, 'items': stale_found}


# ---------------------------------------------------------------------------
# Task: Config Backup (Priority 3)
# ---------------------------------------------------------------------------

def config_backup():
    """Write a tar.gz backup archive to /config/backups/ and prune old ones.

    Archive bundles .env, settings.json, library_prefs.json, blocklist.json
    with a manifest for restore.  Retention is controlled by
    ``CONFIG_BACKUP_RETENTION`` (default 7).  Pruning only touches files
    matching the archive filename pattern — pre-restore snapshot dirs
    under the same /config/backups/ are left alone.
    """
    from utils import backup as _backup

    backup_dir = os.environ.get('CONFIG_BACKUP_DIR', _backup.DEFAULT_BACKUP_DIR)
    try:
        keep = max(1, int(os.environ.get('CONFIG_BACKUP_RETENTION', '7')))
    except ValueError:
        keep = 7

    try:
        archive = _backup.create_backup_file(
            config_dir=_backup.DEFAULT_CONFIG_DIR, backup_dir=backup_dir
        )
    except Exception as e:
        logger.error(f"[scheduler] Config backup failed: {e}")
        return {'status': 'error', 'message': str(e)}

    pruned = 0
    try:
        pruned = _backup.prune_old_backups(backup_dir, keep=keep)
    except Exception as e:
        logger.warning(f"[scheduler] Error pruning old backups: {e}")

    size = archive.stat().st_size if archive.exists() else 0
    msg = f'Wrote {archive.name} ({size} bytes)'
    if pruned:
        msg += f', pruned {pruned}'
    return {'status': 'success', 'message': msg, 'items': 1}


# ---------------------------------------------------------------------------
# Task: Mount Liveness Probe (Priority 3)
# ---------------------------------------------------------------------------

def _has_real_media_files(path, sample_limit=10):
    """Quick-sample a library directory for real (non-symlink) media files.

    Checks up to *sample_limit* top-level subdirectories for at least one
    non-symlink media file.  Descends into Season subdirectories for TV
    libraries (Show/Season XX/episode.mkv).  Returns True as soon as one
    is found.
    """
    checked = 0
    try:
        with os.scandir(path) as it:
            for entry in it:
                if not entry.is_dir(follow_symlinks=False):
                    continue
                if checked >= sample_limit:
                    break
                checked += 1
                try:
                    with os.scandir(entry.path) as sub:
                        for f in sub:
                            ext = os.path.splitext(f.name)[1].lower()
                            if ext in MEDIA_EXTENSIONS and f.is_file(follow_symlinks=False):
                                return True
                            # Descend into Season subdirs for TV libraries
                            if f.is_dir(follow_symlinks=False):
                                try:
                                    with os.scandir(f.path) as deep:
                                        for g in deep:
                                            if (os.path.splitext(g.name)[1].lower() in MEDIA_EXTENSIONS
                                                    and g.is_file(follow_symlinks=False)):
                                                return True
                                except OSError:
                                    pass
                except OSError:
                    continue
    except OSError:
        pass
    return False


def _check_local_library_health():
    """Quick check that local library paths still have real (non-symlink) files.

    When a network mount (NFS/SMB) drops silently, the bind-mounted path
    inside the container still exists but only contains debrid symlinks
    that Zurgarr created locally.  Detecting the absence of real files
    catches this early and sends a notification.
    """
    local_movies = os.environ.get('BLACKHOLE_LOCAL_LIBRARY_MOVIES', '').strip()
    local_tv = os.environ.get('BLACKHOLE_LOCAL_LIBRARY_TV', '').strip()

    for label, path in [('movies', local_movies), ('tv', local_tv)]:
        if not path or not os.path.isdir(path):
            continue
        has_real = _has_real_media_files(path)
        prev = _local_library_baselines.get(label)

        if prev is True and not has_real and not _local_library_alerted.get(label):
            logger.error(
                f"[scheduler] Local {label} library has no real files — "
                f"network mount may have dropped: {path}"
            )
            try:
                from utils.notifications import notify
                notify('health_error', f'Local Library Down: {label}',
                       f'Local {label} library at {path} has no real media files. '
                       f'A network mount may have dropped.',
                       level='error')
            except Exception as exc:
                logger.debug(f"[scheduler] Failed to send mount-drop notification: {exc}")
            _local_library_alerted[label] = True
        elif has_real:
            if _local_library_alerted.get(label):
                logger.info(f"[scheduler] Local {label} library recovered: {path}")
            _local_library_baselines[label] = True
            _local_library_alerted[label] = False


# Mount-probe thresholds.  Surfaced as module constants so the test
# suite can monkeypatch them and so a future operator can tune via
# code review rather than spelunking through magic numbers.
_SLOW_MOUNT_THRESHOLD_SEC = 5      # listdir slower than this → log WARN
_LISTDIR_TIMEOUT_SEC = 15          # hard cap on listdir before we give up
                                    # (a wedged FUSE that responds-but-slow
                                    # would otherwise hang the scheduler
                                    # thread forever; this is the only
                                    # tier of defense)


def _listdir_with_timeout(path, timeout):
    """``os.listdir(path)`` with a wall-clock timeout.

    A wedged FUSE driver (rclone busy with vfs-refresh, blocked on a
    debrid 5xx, or genuinely stuck) doesn't raise — it just never
    returns.  ``signal.alarm`` only works from the main thread, and the
    scheduler runs in a worker, so we use a daemon thread with a
    bounded join.  On timeout the worker is *leaked* (no portable way
    to cancel a blocking syscall in another thread) but the caller is
    no longer wedged, and the daemon flag keeps the leak from
    preventing interpreter shutdown.

    Returns the listing on success.  Raises ``TimeoutError`` on
    timeout — the caller maps that to ``'error'``.  Raises the
    underlying ``OSError`` if the worker raised.
    """
    import threading
    result = {}

    def _run():
        try:
            result['entries'] = os.listdir(path)
        except BaseException as exc:  # noqa: BLE001 - propagate to caller
            result['exc'] = exc

    t = threading.Thread(target=_run, name=f'mount-probe-{os.path.basename(path)}', daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        raise TimeoutError(f'listdir({path!r}) did not return within {timeout}s')
    if 'exc' in result:
        raise result['exc']
    return result['entries']


def _probe_mount(mount_path, tolerate_timeout=False):
    """Probe a single FUSE mount.

    Returns ``(status, message, items)`` where status is one of
    ``'success'`` / ``'error'`` / ``'absent'``.  ``'absent'`` signals
    the path is not a mount at all (caller decides whether that's a
    real error or just "this mount isn't configured").  Distinguishing
    error vs absent matters for the dual-mount summary: a missing TB
    mount on a single-debrid install shouldn't degrade RD-only health.

    A wedged-but-not-erroring FUSE (rclone hung mid-operation) is
    bounded to ``_LISTDIR_TIMEOUT_SEC`` so one stuck mount can't block
    the entire scheduler thread (which also runs verify_symlinks,
    library_scan, debrid_health sweeps, etc).

    ``tolerate_timeout`` softens *only* the timeout case to a slow-but-
    alive ``'success'`` (used for TorBox, whose tight rate limit makes a
    full FUSE walk routinely 429-throttle past the deadline — a slow
    walk there is expected, not a dead mount).  A genuinely dead mount
    still raises ENOTCONN, which is handled as ``'error'`` regardless.
    """
    if not os.path.isdir(mount_path):
        return 'absent', f'Mount path does not exist: {mount_path}', 0
    if not os.path.ismount(mount_path):
        return 'absent', f'Not a mount point: {mount_path}', 0
    try:
        start = time.time()
        entries = _listdir_with_timeout(mount_path, _LISTDIR_TIMEOUT_SEC)
        elapsed = time.time() - start
        if elapsed > _SLOW_MOUNT_THRESHOLD_SEC:
            logger.warning(f"[scheduler] Mount {mount_path} is slow: listdir took {elapsed:.1f}s")
            return 'success', f'Mount responsive but slow ({elapsed:.1f}s)', len(entries)
        return 'success', f'{len(entries)} entries, {elapsed:.2f}s', len(entries)
    except TimeoutError as e:
        if tolerate_timeout:
            # A rate-limited mount (TorBox 429-throttling the FUSE walk)
            # is alive, just slow — reporting 'error' would flap the
            # System page red on every TB walk.  Only the *timeout* case
            # is softened; a dead mount raises ENOTCONN (handled below).
            logger.warning(f"[scheduler] Mount {mount_path} slow/rate-limited "
                           f"(no listing within {_LISTDIR_TIMEOUT_SEC}s): {e}")
            return 'success', f'Mount responsive but rate-limited ({e})', 0
        logger.error(f"[scheduler] Mount {mount_path} hung past {_LISTDIR_TIMEOUT_SEC}s: {e}")
        return 'error', f'Mount hung: {e}', 0
    except Exception as e:  # noqa: BLE001 — FUSE bindings raise non-OSError too
        # Half-stuck FUSE (mount table entry persists, rclone process
        # died) returns ENOTCONN here — the canonical failure mode this
        # probe needs to surface to operators.  Widened beyond OSError
        # because libfuse bindings can surface decode/parse errors on
        # corrupted dentry blocks too.
        logger.error(f"[scheduler] Mount {mount_path} is unresponsive: {e}")
        return 'error', f'Mount unresponsive: {e}', 0


def mount_liveness_probe():
    """Verify rclone FUSE mounts (RD/AD + TB) and local library mounts are healthy.

    Probes the primary mount (``BLACKHOLE_RCLONE_MOUNT``) and the TB
    mount when configured.  Either being dead degrades the overall
    status — neither mount being silently dead is the failure mode this
    probe was added to catch.  Plan 39 dual-debrid: pre-fix this only
    probed the RD mount, so a dead TB mount left the System page green
    while every TB grab silently timed out at 300s.
    """
    rclone_mount = os.environ.get('BLACKHOLE_RCLONE_MOUNT', '/data')

    # Primary (RD/AD) — always checked.  This is the load-bearing mount;
    # an absent or unresponsive primary is always an error.
    primary_status, primary_msg, primary_items = _probe_mount(rclone_mount)
    if primary_status == 'absent':
        # Primary missing is a real error (existing behavior).
        primary_status = 'error'

    # Optional TB mount.  Gated on the same three env vars
    # ``rclone/rclone.py::_torbox_mount_configured`` checks, so we don't
    # waste a probe on TB-unconfigured installs.  ``mount_for_debrid(TORBOX)``
    # always synthesises a path even without credentials (it falls back
    # to ``/data/torbox``), so a pure-discovery probe would falsely
    # report 'absent' on every RD-only install — which would be a
    # constant 'not a mount point' / 'path does not exist' message in
    # the System UI for users who never enabled TB.  Worse, if a future
    # change made ``BLACKHOLE_RCLONE_MOUNT`` point to ``/data/torbox``
    # (an unusual but valid config), the bare-discovery code would
    # probe the same mount twice.  Env-gate keeps the probe scoped.
    tb_configured = bool(
        os.environ.get('TORBOX_API_KEY')
        and os.environ.get('TORBOX_WEBDAV_USER')
        and os.environ.get('TORBOX_WEBDAV_PASS')
    )
    tb_mount_path = None
    tb_status, tb_msg, tb_items = None, None, 0
    if tb_configured:
        try:
            from utils.debrid_routing import TORBOX, mount_for_debrid
            # ``mount_for_debrid`` needs the parent of the per-debrid mount-name
            # suffix — same convention as BlackholeWatcher._mount_for and
            # verify_symlinks.
            rclonemn = os.environ.get('RCLONE_MOUNT_NAME') or ''
            base = rclone_mount.rstrip('/')
            if rclonemn and os.path.basename(base) == rclonemn:
                parent = os.path.dirname(base)
                if parent:
                    base = parent
            tb_mount_path = mount_for_debrid(TORBOX, rclone_mount_base=base)
        except Exception as e:
            logger.debug(f"[scheduler] TB mount discovery failed: {e}")

    if tb_mount_path:
        tb_status, tb_msg, tb_items = _probe_mount(tb_mount_path, tolerate_timeout=True)

    # Combine.  TB 'absent' means TB not configured — don't degrade.
    # TB 'error' means TB configured but dead — degrade to error.
    if primary_status == 'error':
        overall = 'error'
        message = f'RD: {primary_msg}'
        if tb_status:
            message += f' | TB ({tb_mount_path}): {tb_msg}'
    elif tb_status == 'error':
        overall = 'error'
        message = f'TB ({tb_mount_path}): {tb_msg} | RD: {primary_msg}'
    else:
        overall = 'success'
        message = primary_msg
        if tb_status == 'success':
            message += f' | TB: {tb_msg}'

    result = {
        'status': overall,
        'message': message,
        'items': primary_items,
    }
    if tb_status is not None:
        result['tb_items'] = tb_items
        result['tb_mount'] = tb_mount_path
        result['tb_status'] = tb_status

    # Check local library paths for real files (detects NFS/SMB mount drops).
    # Runs after the rclone check so a stale NFS mount doesn't block
    # rclone health reporting.
    _check_local_library_health()

    return result


# ---------------------------------------------------------------------------
# Task: Notification Digest (Daily summary)
# ---------------------------------------------------------------------------

def notification_digest():
    """Send a daily summary notification of the last 24 hours of events."""
    if not _history:
        return {'status': 'skipped', 'message': 'History module not available'}

    now = datetime.now(timezone.utc)
    start_iso = (now - timedelta(hours=24)).isoformat(timespec='seconds')

    result = _history.query(start=start_iso, limit=200)
    events = result.get('events', [])
    if not events:
        return {'status': 'success', 'message': 'No events today, digest skipped'}

    # Tally by event type
    counts = {}
    for ev in events:
        t = ev.get('type', 'unknown')
        counts[t] = counts.get(t, 0) + 1

    # Build human-readable summary
    labels = {
        'grabbed': 'torrents grabbed',
        'cached': 'cached on debrid',
        'symlink_created': 'symlinks created',
        'symlink_failed': 'symlink failures',
        'failed': 'failures',
        'debrid_unavailable': 'marked unavailable',
        'local_fallback_triggered': 'local fallback downloads',
        'blocklist_added': 'blocklisted',
        'cleanup': 'cleanups',
        'source_switch': 'source switches',
        'search': 'searches triggered',
        'rescan': 'rescans triggered',
    }

    parts = []
    for event_type, count in sorted(counts.items(), key=lambda x: -x[1]):
        label = labels.get(event_type, event_type.replace('_', ' '))
        parts.append(f'{count} {label}')

    body = 'Today: ' + ', '.join(parts)

    try:
        from utils.notifications import notify
        notify('daily_digest', 'Zurgarr Daily Summary', body)
    except Exception as e:
        logger.error(f"[scheduler] Digest notification failed: {e}")
        return {'status': 'error', 'message': str(e)}

    return {'status': 'success', 'message': body, 'items': len(events)}


def _compute_digest_delay():
    """Compute seconds until next NOTIFICATION_DIGEST_TIME (local wall clock)."""
    time_str = os.environ.get('NOTIFICATION_DIGEST_TIME', '08:00').strip()
    try:
        parts = time_str.split(':')
        hour = int(parts[0])
        minute = int(parts[1]) if len(parts) > 1 else 0
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError(f"out of range: {hour}:{minute}")
    except (ValueError, IndexError):
        logger.warning(f"[scheduler] Invalid NOTIFICATION_DIGEST_TIME='{time_str}', using 08:00")
        hour, minute = 8, 0

    now = datetime.now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def register_all():
    """Register all scheduled tasks with the central scheduler.

    Called from main.py after all services are initialized.
    Tasks that depend on optional features check their own prerequisites
    and skip registration if not applicable.
    """
    from utils.task_scheduler import scheduler

    # Priority 1 — High
    blackhole_enabled = os.environ.get('BLACKHOLE_ENABLED', 'false').lower() == 'true'

    # Audit Download Routing — only if Sonarr or Radarr is configured
    sonarr_url = os.environ.get('SONARR_URL', '')
    radarr_url = os.environ.get('RADARR_URL', '')
    if blackhole_enabled and (sonarr_url or radarr_url):
        scheduler.register(
            'audit_download_routing',
            audit_download_routing,
            interval_seconds=_get_interval('ROUTING_AUDIT_INTERVAL'),
            description='Verify download client/indexer tag routing in Sonarr/Radarr',
            initial_delay=300,  # 5 min after startup (let arrs settle)
        )

        scheduler.register(
            'clean_stale_queue',
            clean_stale_queue_items,
            interval_seconds=_get_interval('QUEUE_CLEANUP_INTERVAL'),
            description='Remove stale downloadClientUnavailable queue items',
            initial_delay=120,  # 2 min after startup
        )

        scheduler.register(
            'detect_stale_grabs',
            detect_stale_grabs,
            interval_seconds=_get_interval('STALE_GRAB_INTERVAL'),
            description='Detect grabs that silently failed and re-trigger searches',
            initial_delay=600,  # 10 min after startup
        )

    # Library Scan — only if status UI is enabled (scanner depends on it)
    status_ui = os.environ.get('STATUS_UI_ENABLED', 'false').lower() == 'true'
    if status_ui:
        scheduler.register(
            'library_scan',
            library_scan,
            interval_seconds=_get_interval('LIBRARY_SCAN_INTERVAL'),
            description='Scan debrid mount and local library, auto-create symlinks',
            initial_delay=120,  # 2 min
        )

    # Verify Symlinks — only if blackhole symlinks are enabled
    symlinks_enabled = os.environ.get('BLACKHOLE_SYMLINK_ENABLED', 'false').lower() == 'true'
    if symlinks_enabled:
        scheduler.register(
            'verify_symlinks',
            verify_symlinks,
            interval_seconds=_get_interval('SYMLINK_VERIFY_INTERVAL'),
            description='Check debrid symlinks and remove broken ones',
            initial_delay=600,  # 10 min
        )

    # Priority 2 — Medium

    # Enforce Source Preferences — only if preferences exist
    if status_ui:
        scheduler.register(
            'enforce_preferences',
            enforce_source_preferences,
            interval_seconds=_get_interval('PREFERENCE_ENFORCE_INTERVAL'),
            description='Enforce prefer-debrid/prefer-local source preferences',
            initial_delay=_get_interval('PREFERENCE_ENFORCE_INTERVAL'),
            enabled=os.environ.get('LIBRARY_PREFERENCE_AUTO_ENFORCE', 'false').lower() == 'true',
        )

    # Housekeeping — always enabled
    scheduler.register(
        'housekeeping',
        housekeeping,
        interval_seconds=_get_interval('HOUSEKEEPING_INTERVAL'),
        description='Clean stale pending state, empty dirs, old metadata',
        initial_delay=3600,  # 1 hour after startup
    )

    # Priority 3 — Nice to Have

    # CONFIG_BACKUP_INTERVAL=0 disables scheduled backups (manual
    # backup/restore via the Settings UI still work).  Any other value is
    # seconds; empty falls back to the _DEFAULTS entry (24h).
    backup_interval = _get_interval('CONFIG_BACKUP_INTERVAL')
    if backup_interval > 0:
        scheduler.register(
            'config_backup',
            config_backup,
            interval_seconds=backup_interval,
            description='Archive .env, settings.json, library_prefs.json, blocklist.json to /config/backups/',
            initial_delay=300,  # 5 min after startup
        )
    else:
        logger.info('[scheduler] Scheduled config backups disabled (CONFIG_BACKUP_INTERVAL=0)')

    # Mount liveness — register if rclone is configured (mount may not exist yet at startup)
    rclone_configured = os.environ.get('RCLONE_MOUNT_NAME', '') or os.environ.get('BLACKHOLE_RCLONE_MOUNT', '')
    if rclone_configured:
        scheduler.register(
            'mount_liveness',
            mount_liveness_probe,
            interval_seconds=_get_interval('MOUNT_LIVENESS_INTERVAL'),
            description='Verify rclone FUSE mount is responsive',
            initial_delay=60,
        )

    # Debrid Health Reconciler (plan 38) — register whenever any RD
    # credential is present in the environment, so the task surfaces
    # in the scheduler UI even when DEBRID_HEALTH_ENABLED is OFF.
    # The sweep itself is a no-op when disabled; this preserves the
    # user's ability to manually trigger from the System page without
    # restarting the container after toggling the env var ON.
    rd_configured = bool(os.environ.get('RD_API_KEY') or os.path.isfile('/run/secrets/rd_api_key'))
    if rd_configured:
        from utils.debrid_health import run_sweep as _debrid_health_run
        scheduler.register(
            'debrid_health_reconcile',
            _debrid_health_run,
            interval_seconds=_get_interval('DEBRID_HEALTH_INTERVAL'),
            description='Probe RD torrents for May 2026 keyword-filter blocks (infringing_file / error 35)',
            initial_delay=900,  # 15 min after startup
        )

    # Notification Digest — daily summary if enabled
    digest_enabled = os.environ.get('NOTIFICATION_DIGEST_ENABLED', 'false').lower() == 'true'
    if digest_enabled and os.environ.get('NOTIFICATION_URL'):
        scheduler.register(
            'notification_digest',
            notification_digest,
            interval_seconds=24 * 3600,  # once per day
            description='Send daily summary of pipeline events',
            initial_delay=_compute_digest_delay(),
        )

    logger.info(f"[scheduler] Registered {len(scheduler.get_status())} total tasks")
