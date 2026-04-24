"""In-memory retry counter for cosmetic "search #N, first attempt Xd ago"
annotations on activity events.

Keyed by ``(service, media_id)`` — e.g. ``('radarr', 608)`` or
``('sonarr-episode', 42017)``. Each bump returns the new count and the
ISO timestamp of the first attempt, which callers stash into event meta
so ``utils.activity_format`` can render it.

No disk persistence. Container restart resets the counters — this is
acceptable because the numbers are informational only, never a source
of truth. Call ``reset`` on success (grab landed, symlink created, user
deleted the item) to drop the entry.
"""

import threading
from datetime import datetime, timezone

_state = {}
_lock = threading.Lock()


def bump(service, media_id):
    """Increment the counter for (service, media_id).

    Returns:
        (count, first_ts_iso) — count is the new attempt number (1 on
        first call), first_ts_iso is the ISO timestamp of that first
        attempt.
    """
    key = (service, media_id)
    now_iso = datetime.now(timezone.utc).isoformat(timespec='seconds')
    with _lock:
        entry = _state.get(key)
        if entry is None:
            entry = {'count': 1, 'first_ts': now_iso}
            _state[key] = entry
        else:
            entry['count'] += 1
        return entry['count'], entry['first_ts']


def get(service, media_id):
    """Return (count, first_ts_iso) or (0, None) if no entry."""
    with _lock:
        entry = _state.get((service, media_id))
        if not entry:
            return 0, None
        return entry['count'], entry['first_ts']


def reset(service, media_id):
    """Drop the counter for (service, media_id). Safe if missing."""
    with _lock:
        _state.pop((service, media_id), None)


def reset_all():
    """Clear every counter. Test hook."""
    with _lock:
        _state.clear()


def size():
    """Return the number of tracked keys. Test/debug hook."""
    with _lock:
        return len(_state)


__all__ = ['bump', 'get', 'reset', 'reset_all', 'size']
