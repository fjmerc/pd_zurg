"""Structured event history for the debrid pipeline.

Logs pipeline events (grabs, symlinks, failures, etc.) to a JSONL file
for querying via API and display in the dashboard Activity tab.
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
        detail: Human-readable detail string
        source: Origin of the event (blackhole, library, arr, scheduler)
        meta: Optional dict of extra structured data
        media_title: Canonical show/movie name for matching on detail pages
    """
    if _file_path is None:
        return

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
