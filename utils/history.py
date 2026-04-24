"""Structured event history for the debrid pipeline.

Logs pipeline events (grabs, symlinks, failures, etc.) to a JSONL file
for querying via API and display in the dashboard Activity tab.

Callers pass a stable ``meta['cause']`` slug from the CAUSE_* constants
below; the UI builds the human-readable detail from meta. The free-form
``detail`` string is only kept for backward-compat with events written
before the cause vocabulary existed.
"""

import json
import os
import threading
import uuid
from datetime import datetime, timezone, timedelta
from utils.file_utils import atomic_write
from utils.logger import get_logger

logger = get_logger()

# Module-level state
_file_path = None
_lock = threading.Lock()
_retention_days = 30


# ---------------------------------------------------------------------------
# Cause vocabulary — stable slugs attached to events via ``meta['cause']``.
# The UI translates these to human strings; never rename an existing slug
# without also updating utils/activity_format.py and the JS mirror.
# ---------------------------------------------------------------------------

# Acquisition
CAUSE_BLACKHOLE_NEW_IMPORT = 'blackhole_new_import'
CAUSE_BLACKHOLE_CACHE_HIT = 'blackhole_cache_hit'
CAUSE_BLACKHOLE_GRAB_SUBMITTED = 'blackhole_grab_submitted'
CAUSE_LIBRARY_NEW_IMPORT = 'library_new_import'
CAUSE_LIBRARY_UPGRADE_REPLACED = 'library_upgrade_replaced'
CAUSE_LIBRARY_STATE_INIT = 'library_state_init'
CAUSE_COMPROMISE_GRAB = 'compromise_grab'

# Failure
CAUSE_DEBRID_ADD_FAILED = 'debrid_add_failed'
CAUSE_SYMLINK_CREATE_FAILED = 'symlink_create_failed'
CAUSE_DISC_RIP_REJECTED = 'disc_rip_rejected'
CAUSE_TERMINAL_ERROR = 'terminal_error'
CAUSE_UNCACHED_TIMEOUT = 'uncached_timeout'
CAUSE_UNCACHED_REJECTED = 'uncached_rejected'
CAUSE_INCOMPLETE_RELEASE = 'incomplete_release'
CAUSE_ALTS_EXHAUSTED = 'alts_exhausted'
CAUSE_DUPLICATE_SKIPPED = 'duplicate_skipped'
CAUSE_BLOCKLISTED_HASH = 'blocklisted_hash'
CAUSE_DEBRID_UNAVAILABLE_MARKED = 'debrid_unavailable_marked'
CAUSE_DEBRID_ADD_VIA_SEARCH = 'debrid_add_via_search'

# Action
CAUSE_POST_SYMLINK_RESCAN = 'post_symlink_rescan'
CAUSE_POST_GRAB_RESCAN = 'post_grab_rescan'
CAUSE_USER_TRIGGERED_RESCAN = 'user_triggered_rescan'
CAUSE_USER_TRIGGERED_SEARCH = 'user_triggered_search'
CAUSE_ROUTING_AUDIT_RETRY = 'routing_audit_retry'
CAUSE_STALE_GRAB_RETRY = 'stale_grab_retry'
CAUSE_SYMLINK_REPAIR_RESEARCH = 'symlink_repair_research'
CAUSE_PREFERENCE_ENFORCE_SEARCH = 'preference_enforce_search'
CAUSE_LOCAL_FALLBACK_GRAB = 'local_fallback_grab'

# Management
CAUSE_PREFERENCE_SOURCE_SWITCH = 'preference_source_switch'
CAUSE_ROUTING_REPAIRED = 'routing_repaired'
CAUSE_ARR_DELETED_USER = 'arr_deleted_user'
CAUSE_ARR_DELETED_CLEANUP = 'arr_deleted_cleanup'
CAUSE_AUTO_BLOCKLIST_ADDED = 'auto_blocklist_added'

# Scheduler / tasks
CAUSE_TASK_LIBRARY_SCAN = 'task_library_scan'
CAUSE_TASK_HOUSEKEEPING = 'task_housekeeping'
CAUSE_TASK_STALE_GRAB_DETECTION = 'task_stale_grab_detection'
CAUSE_TASK_ROUTING_AUDIT = 'task_routing_audit'
CAUSE_TASK_VERIFY_SYMLINKS = 'task_verify_symlinks'


def init(config_dir='/config'):
    """Initialize the history module. Call once at startup."""
    global _file_path, _retention_days
    _file_path = os.path.join(config_dir, 'history.jsonl')
    try:
        _retention_days = int(os.environ.get('HISTORY_RETENTION_DAYS') or 30)
    except (ValueError, TypeError):
        _retention_days = 30
        logger.warning("[history] Invalid HISTORY_RETENTION_DAYS, using default 30")
    logger.info(f"[history] Initialized — {_file_path} (retention: {_retention_days} days)")


def log_event(type, title, episode=None, detail='', source='', meta=None, media_title=None):
    """Append a single event to the history JSONL file.

    Args:
        type: Event type (grabbed, cached, failed, symlink_created, cleanup, etc.)
        title: Media title or technical identifier (e.g. torrent filename)
        episode: Episode identifier (e.g. "S01E05") or None for movies
        detail: Human-readable detail string (backward-compat fallback;
            prefer ``meta['cause']`` from the CAUSE_* vocab above — the UI
            builds the rendered detail from meta when present)
        source: Origin of the event (blackhole, library, arr, scheduler)
        meta: Optional dict of extra structured data. Canonical keys:
            cause, file, quality, size_bytes, cycle_n, cycle_first_ts,
            linked_to, replaces, command_id, arr_service, prior_event_id,
            provider, info_hash, age_days, search_attempts.
        media_title: Canonical show/movie name for matching on detail pages

    Returns:
        The event id (uuid string) on success, or None if history is
        uninitialised or the write failed.
    """
    if _file_path is None:
        return None

    event = {
        'id': str(uuid.uuid4()),
        'ts': datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'type': type,
        'title': title,
    }
    if episode:
        event['episode'] = episode
    if detail:
        event['detail'] = detail
    if source:
        event['source'] = source
    if meta:
        event['meta'] = meta
    if media_title:
        event['media_title'] = media_title

    line = json.dumps(event, separators=(',', ':')) + '\n'

    with _lock:
        try:
            with open(_file_path, 'a', encoding='utf-8') as f:
                f.write(line)
        except OSError as e:
            logger.error(f"[history] Failed to write event: {e}")
            return None
    return event['id']


def query(type=None, title=None, start=None, end=None, page=1, limit=50):
    """Query history events with optional filters, newest first.

    Args:
        type: Filter by event type
        title: Filter by title (case-insensitive substring match)
        start: ISO datetime string — only events at or after this time
        end: ISO datetime string — only events at or before this time
        page: Page number (1-based)
        limit: Events per page (max 200)

    Returns:
        dict with 'events', 'total', 'page', 'pages'
    """
    if _file_path is None:
        return {'events': [], 'total': 0, 'page': page, 'pages': 0}

    limit = max(1, min(limit, 200))
    page = max(1, page)
    events = _read_all_events()
    events.reverse()  # newest first

    # Apply filters
    if type:
        events = [e for e in events if e.get('type') == type]
    if title:
        title_lower = title.lower()
        events = [e for e in events if title_lower in e.get('title', '').lower() or title_lower in e.get('media_title', '').lower()]
    if start:
        events = [e for e in events if e.get('ts', '') >= start]
    if end:
        events = [e for e in events if e.get('ts', '') <= end]

    total = len(events)
    pages = (total + limit - 1) // limit
    offset = (page - 1) * limit
    page_events = events[offset:offset + limit]

    return {
        'events': page_events,
        'total': total,
        'page': page,
        'pages': pages,
    }


def query_by_show(title, limit=20):
    """Return last N events for a specific show title (case-insensitive exact match).

    Args:
        title: Show title to match
        limit: Max events to return

    Returns:
        list of event dicts, newest first
    """
    if _file_path is None:
        return []

    title_lower = title.lower()
    events = _read_all_events()
    events.reverse()  # newest first

    matched = []
    for e in events:
        if e.get('title', '').lower() == title_lower or e.get('media_title', '').lower() == title_lower:
            matched.append(e)
            if len(matched) >= limit:
                break
    return matched


def clear():
    """Truncate the history file."""
    if _file_path is None:
        return
    with _lock:
        try:
            with open(_file_path, 'w', encoding='utf-8') as f:
                pass  # truncate
            logger.info("[history] History cleared")
        except OSError as e:
            logger.error(f"[history] Failed to clear history: {e}")


def rotate():
    """Remove events older than HISTORY_RETENTION_DAYS.

    Reads all events, keeps those within retention window, rewrites the file
    atomically using file_utils.atomic_write.
    """
    if _file_path is None or not os.path.isfile(_file_path):
        return

    cutoff = (datetime.now(timezone.utc) - timedelta(days=_retention_days)).isoformat(timespec='seconds')

    with _lock:
        events = _read_all_events_unlocked()
        kept = [e for e in events if e.get('ts', '') >= cutoff]
        removed = len(events) - len(kept)

        if removed == 0:
            return

        try:
            with atomic_write(_file_path) as f:
                for event in kept:
                    f.write(json.dumps(event, separators=(',', ':')) + '\n')
            logger.info(f"[history] Rotated: removed {removed} events older than {_retention_days} days, kept {len(kept)}")
        except (OSError, json.JSONDecodeError) as e:
            logger.error(f"[history] Rotation failed: {e}")


def _read_all_events():
    """Read all events from the JSONL file. Thread-safe."""
    with _lock:
        return _read_all_events_unlocked()


def _read_all_events_unlocked():
    """Read all events from the JSONL file. Caller must hold _lock."""
    events = []
    try:
        with open(_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    continue  # skip corrupted lines
    except FileNotFoundError:
        pass
    except OSError as e:
        logger.error(f"[history] Failed to read history: {e}")
    return events
