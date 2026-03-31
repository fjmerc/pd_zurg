"""Task implementations for the centralized task scheduler.

Each function follows the convention: returns a dict with 'status',
optional 'message', and optional 'items' count for result tracking.
"""

import os
import shutil
import time
from datetime import datetime, timezone
from utils.logger import get_logger

logger = get_logger()

try:
    from utils import history as _history
except ImportError:
    _history = None


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

    if _history:
        _history.log_event('task_completed', 'Library Scan', source='scheduler',
                           detail=f'{movies} movies, {shows} shows ({duration_ms}ms)')

    return {
        'status': 'success',
        'message': f'{movies} movies, {shows} shows ({duration_ms}ms)',
        'items': movies + shows,
    }


# ---------------------------------------------------------------------------
# Task: Verify Symlinks (Priority 1)
# ---------------------------------------------------------------------------

_SYMLINK_DELETE_THRESHOLD = 50

_MEDIA_EXTENSIONS = {'.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.ts', '.m4v', '.webm'}


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
                if os.path.splitext(entry.name)[1].lower() in _MEDIA_EXTENSIONS:
                    has_media = True
                    break
            if has_media:
                break
            shutil.rmtree(parent, ignore_errors=True)
            logger.debug(f"[scheduler] Cleaned up empty dir: {parent}")
            parent = os.path.dirname(parent)
        except OSError:
            break


def verify_symlinks():
    """Walk completed dir and local library for debrid-pointing symlinks, remove broken ones."""
    completed_dir = os.environ.get('BLACKHOLE_COMPLETED_DIR', '/completed')
    local_tv = os.environ.get('BLACKHOLE_LOCAL_LIBRARY_TV', '').strip()
    local_movies = os.environ.get('BLACKHOLE_LOCAL_LIBRARY_MOVIES', '').strip()
    rclone_mount = os.path.realpath(os.environ.get('BLACKHOLE_RCLONE_MOUNT', '/data'))
    symlink_target = os.environ.get('BLACKHOLE_SYMLINK_TARGET_BASE', '').strip()
    # Check symlinks pointing to either the rclone mount or the symlink target base
    debrid_prefixes = [rclone_mount + '/']
    symlink_target_real = ''
    if symlink_target:
        symlink_target_real = os.path.realpath(symlink_target) + '/'
        debrid_prefixes.append(symlink_target_real)

    scan_dirs = []
    if os.path.isdir(completed_dir):
        scan_dirs.append(completed_dir)
    if local_tv and os.path.isdir(local_tv):
        scan_dirs.append(local_tv)
    if local_movies and os.path.isdir(local_movies):
        scan_dirs.append(local_movies)

    if not scan_dirs:
        return {'status': 'success', 'message': 'No directories to check'}

    # Guard: verify the rclone mount is responsive before scanning.
    # A stalled FUSE mount makes os.path.exists return False for everything,
    # which would cause mass deletion of all symlinks.
    if os.path.isdir(rclone_mount):
        try:
            os.listdir(rclone_mount)
        except OSError as e:
            logger.error(f"[scheduler] Mount {rclone_mount} unresponsive — aborting symlink verify to prevent mass deletion: {e}")
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
                # When BLACKHOLE_SYMLINK_TARGET_BASE differs from the rclone
                # mount, symlinks intentionally point to a path that only
                # exists inside Radarr/Sonarr's container (e.g. /mnt/debrid).
                # Translate the target to the local rclone mount path before
                # checking existence.
                check_target = target
                if symlink_target_real and target.startswith(symlink_target_real):
                    check_target = rclone_mount + '/' + target[len(symlink_target_real):]
                if not os.path.exists(check_target):
                    to_delete.append((fpath, target, scan_dir))

    # Phase 2: Safety threshold — refuse mass deletion
    broken = len(to_delete)
    if broken > _SYMLINK_DELETE_THRESHOLD and checked > 0 and broken / checked > 0.5:
        logger.error(
            f"[scheduler] Refusing to delete {broken}/{checked} symlinks — "
            f"threshold exceeded (>{_SYMLINK_DELETE_THRESHOLD} and >50%). "
            f"Check mount health or debrid subscription."
        )
        return {
            'status': 'error',
            'message': f'Mass deletion blocked: {broken}/{checked} symlinks appear broken',
            'items': 0,
        }

    # Phase 3: Delete confirmed broken symlinks and clean up empty dirs
    deleted = 0
    for fpath, target, scan_dir in to_delete:
        try:
            os.remove(fpath)
            deleted += 1
            logger.info(f"[scheduler] Removed broken symlink: {fpath} -> {target}")
            # Clean up empty parent dirs in local library paths (up to the
            # library root) so the scanner doesn't classify them as local
            if scan_dir in (local_tv, local_movies) and scan_dir:
                _cleanup_empty_parents(fpath, scan_dir)
        except OSError as e:
            logger.warning(f"[scheduler] Failed to remove broken symlink {fpath}: {e}")

    msg = f'Checked {checked} symlinks'
    if deleted:
        msg += f', removed {deleted} broken'
        if _history:
            _history.log_event('cleanup', 'Symlink Verify', source='scheduler',
                               detail=f'Removed {deleted} broken symlink(s) of {checked} checked')

    return {'status': 'success', 'message': msg, 'items': broken}


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
    completed_dir = os.environ.get('BLACKHOLE_COMPLETED_DIR', '/completed')
    try:
        if os.path.isdir(completed_dir):
            for root, dirs, files in os.walk(completed_dir, topdown=False):
                if root == completed_dir:
                    continue
                if not files and not dirs:
                    try:
                        os.rmdir(root)
                        cleaned += 1
                        logger.debug(f"[scheduler] Removed empty directory: {root}")
                    except OSError:
                        pass
    except Exception as e:
        logger.error(f"[scheduler] Error cleaning empty dirs: {e}")

    # 3. Clean old blackhole retry metadata (.meta.json files older than 7 days)
    now = time.time()
    watch_dir = os.environ.get('BLACKHOLE_DIR', '/watch')
    try:
        if os.path.isdir(watch_dir):
            for fname in os.listdir(watch_dir):
                if not fname.endswith('.meta.json'):
                    continue
                fpath = os.path.join(watch_dir, fname)
                try:
                    age_days = (now - os.path.getmtime(fpath)) / 86400
                    if age_days > 7:
                        os.remove(fpath)
                        cleaned += 1
                        logger.debug(f"[scheduler] Removed stale metadata: {fname}")
                except OSError:
                    pass
    except Exception as e:
        logger.error(f"[scheduler] Error cleaning metadata: {e}")

    # 4. Rotate history log
    try:
        if _history:
            _history.rotate()
    except Exception as e:
        logger.error(f"[scheduler] Error rotating history: {e}")

    if cleaned and _history:
        _history.log_event('task_completed', 'Housekeeping', source='scheduler',
                           detail=f'Cleaned {cleaned} item(s)')

    return {'status': 'success', 'message': f'Cleaned {cleaned} items', 'items': cleaned}


# ---------------------------------------------------------------------------
# Task: Detect Stale Grabs (Priority 1)
# ---------------------------------------------------------------------------

# Track recently re-triggered IDs to prevent search storms.
# Key: ('sonarr', ep_id) or ('radarr', movie_id), Value: epoch time of last trigger.
_retrigger_history = {}
_RETRIGGER_COOLDOWN = 7200  # 2 hours — don't re-trigger the same item within this window


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

    # Prune old entries from retrigger history
    stale_keys = [k for k, v in _retrigger_history.items() if now_epoch - v > _RETRIGGER_COOLDOWN]
    for k in stale_keys:
        del _retrigger_history[k]

    for ClientClass, name in [
        (SonarrClient, 'sonarr'),
        (RadarrClient, 'radarr'),
    ]:
        client = ClientClass()
        if not client.configured:
            continue

        grabs = client.get_recent_grabs(page_size=30)
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

            # Dedup: skip if already re-triggered recently
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
                client.search_episodes([ep_id])
            else:
                logger.info(
                    f"[scheduler] Stale grab detected: {source_title} "
                    f"(grabbed {int(age_minutes)}m ago) — re-triggering search"
                )
                client.search_movie(movie_id)

            _retrigger_history[item_key] = now_epoch
            searches_triggered += 1

    msg = f'Found {stale_found} stale grabs'
    if searches_triggered:
        msg += f', re-triggered {searches_triggered} searches'
        if _history:
            _history.log_event('task_completed', 'Stale Grab Detection', source='scheduler',
                               detail=msg)
    return {'status': 'success', 'message': msg, 'items': stale_found}


# ---------------------------------------------------------------------------
# Task: Config Backup (Priority 3)
# ---------------------------------------------------------------------------

def config_backup():
    """Backup .env and settings files to a timestamped directory."""
    backup_root = os.environ.get('CONFIG_BACKUP_DIR', '/config/backups')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_dir = os.path.join(backup_root, timestamp)

    files_to_backup = [
        ('/config/.env', '.env'),
        ('/config/settings.json', 'settings.json'),
        ('/config/preferences.json', 'preferences.json'),
    ]

    backed_up = 0
    try:
        os.makedirs(backup_dir, exist_ok=True)
        for src, dst_name in files_to_backup:
            if os.path.isfile(src):
                shutil.copy2(src, os.path.join(backup_dir, dst_name))
                backed_up += 1
    except Exception as e:
        logger.error(f"[scheduler] Config backup failed: {e}")
        return {'status': 'error', 'message': str(e)}

    # Prune old backups (keep last 7)
    try:
        if os.path.isdir(backup_root):
            backups = sorted(
                e for e in os.listdir(backup_root)
                if os.path.isdir(os.path.join(backup_root, e))
            )
            while len(backups) > 7:
                old = backups.pop(0)
                old_path = os.path.join(backup_root, old)
                if os.path.isdir(old_path):
                    shutil.rmtree(old_path, ignore_errors=True)
                    logger.debug(f"[scheduler] Pruned old backup: {old}")
    except Exception as e:
        logger.warning(f"[scheduler] Error pruning old backups: {e}")

    return {'status': 'success', 'message': f'Backed up {backed_up} files', 'items': backed_up}


# ---------------------------------------------------------------------------
# Task: Mount Liveness Probe (Priority 3)
# ---------------------------------------------------------------------------

def mount_liveness_probe():
    """Verify rclone FUSE mount is responsive, not just alive."""
    rclone_mount = os.environ.get('BLACKHOLE_RCLONE_MOUNT', '/data')

    if not os.path.isdir(rclone_mount):
        return {'status': 'error', 'message': f'Mount path does not exist: {rclone_mount}'}

    if not os.path.ismount(rclone_mount):
        return {'status': 'error', 'message': f'Not a mount point: {rclone_mount}'}

    # Try to list the mount directory (tests filesystem responsiveness)
    try:
        start = time.time()
        entries = os.listdir(rclone_mount)
        elapsed = time.time() - start
        if elapsed > 5:
            logger.warning(f"[scheduler] Mount {rclone_mount} is slow: listdir took {elapsed:.1f}s")
            return {
                'status': 'success',
                'message': f'Mount responsive but slow ({elapsed:.1f}s)',
                'items': len(entries),
            }
        return {
            'status': 'success',
            'message': f'{len(entries)} entries, {elapsed:.2f}s',
            'items': len(entries),
        }
    except OSError as e:
        logger.error(f"[scheduler] Mount {rclone_mount} is unresponsive: {e}")
        return {'status': 'error', 'message': f'Mount unresponsive: {e}'}


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

    scheduler.register(
        'config_backup',
        config_backup,
        interval_seconds=_get_interval('CONFIG_BACKUP_INTERVAL'),
        description='Backup .env and settings files',
        initial_delay=300,  # 5 min after startup
    )

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

    logger.info(f"[scheduler] Registered {len(scheduler.get_status())} total tasks")
