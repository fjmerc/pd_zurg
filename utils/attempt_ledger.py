"""Persistent grab-attempt counter — disk-backed, restart-survivable.

Counts how many times we've *force-acted* on a specific media unit so the
retry loops can give up after N tries instead of re-poking debrid forever.

Why this exists: two independent loops (the library force-grab path in
``utils.library`` and the TorBox cached-alternative path in
``utils.blackhole``) re-grab the same stuck, never-completing titles on
every scan.  Each add re-arms TorBox's abuse cooldown, which starves the
genuine Wanted→TB recovery.  The loops *thought* they had a give-up gate,
but it keyed on the wall-clock age of a ``created`` timestamp that flapping
resets — so the count never actually climbed.  This module is the missing
persistent count: it survives container restarts (the loops span days) and
is keyed independently of any flap-prone state.

Distinct from ``utils.retry_counter``, which is explicitly in-memory and
cosmetic (drives "search #N" UI annotations and resets on restart).  That
one can't gate a give-up decision; this one can.

Persistence is a single JSON file (``/config/grab_attempts.json``) holding
a flat ``{key: {count, first_ts, last_ts}}`` map, written atomically.  Keys
are opaque caller-chosen strings (e.g. ``fg:the-show:s3`` or
``tbalt:tt1234567:s3``) so the two call sites can't collide.

Safe before ``init()``: ``get`` returns 0, ``bump`` returns 0, ``reset``
and ``prune`` are no-ops.  Snapshotting must never break a scan.
"""

import json
import os
import threading
from datetime import datetime, timezone

from utils.file_utils import atomic_write
from utils.logger import get_logger

logger = get_logger()

_file_path = None
_lock = threading.Lock()
_state = {}

SCHEMA_VERSION = 1


def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def init(config_dir='/config'):
    """Load the ledger from disk. Call once at startup."""
    global _file_path, _state
    _file_path = os.path.join(config_dir, 'grab_attempts.json')
    with _lock:
        _state = _read_file()
    logger.info(f"[attempt_ledger] Initialized — {_file_path} ({len(_state)} keys)")


def _read_file():
    """Load the ledger map from disk, tolerating a missing/corrupt file."""
    if _file_path is None or not os.path.isfile(_file_path):
        return {}
    try:
        with open(_file_path, encoding='utf-8') as f:
            payload = json.load(f)
    except (OSError, ValueError) as e:
        logger.warning(f"[attempt_ledger] Could not read ledger ({e}); starting fresh")
        return {}
    if isinstance(payload, dict):
        entries = payload.get('entries')
        if isinstance(entries, dict):
            return {k: v for k, v in entries.items() if isinstance(v, dict)}
    return {}


def _write_locked():
    """Persist the in-memory state. Caller must hold ``_lock``."""
    if _file_path is None:
        return
    try:
        with atomic_write(_file_path) as f:
            json.dump({'version': SCHEMA_VERSION, 'entries': _state}, f,
                      separators=(',', ':'))
    except OSError as e:
        logger.error(f"[attempt_ledger] Failed to write ledger: {e}")


def bump(key):
    """Increment the attempt count for ``key`` and persist.

    Returns the new count (1 on first call), or 0 if the module hasn't been
    initialized yet (in which case nothing is recorded).
    """
    if _file_path is None:
        return 0
    now = _now_iso()
    with _lock:
        entry = _state.get(key)
        if not isinstance(entry, dict):
            entry = {'count': 0, 'first_ts': now}
            _state[key] = entry
        entry['count'] = int(entry.get('count', 0)) + 1
        entry['last_ts'] = now
        _write_locked()
        return entry['count']


def get(key):
    """Return the current attempt count for ``key`` (0 if unknown)."""
    with _lock:
        entry = _state.get(key)
        if not isinstance(entry, dict):
            return 0
        return int(entry.get('count', 0))


def last_seen_epoch(key):
    """Return ``key``'s ``last_ts`` as a Unix epoch float, or None.

    Lets callers use the ledger as a restart-survivable "when did this
    last happen" store (e.g. the TB-alt sibling-grab dedup TTL), not just
    a counter.  None means unknown/never — callers must not treat it as 0
    (epoch 0 would look infinitely old and pass any TTL check).
    """
    with _lock:
        entry = _state.get(key)
        ts = (entry.get('last_ts') or entry.get('first_ts')) if isinstance(entry, dict) else None
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts).timestamp()
    except (ValueError, TypeError):
        return None


def reset(key):
    """Drop the counter for ``key`` and persist. No-op if absent."""
    if _file_path is None:
        return
    with _lock:
        if key in _state:
            del _state[key]
            _write_locked()


def prune(max_age_seconds):
    """Drop entries whose ``last_ts`` is older than ``max_age_seconds``.

    Time-decay safety valve: a title abandoned long ago should get a fresh
    chance if it ever flows back through a loop, rather than staying capped
    forever.  Persists only when something was actually removed.
    """
    if _file_path is None:
        return
    cutoff = datetime.now(timezone.utc).timestamp() - max(0, max_age_seconds)
    removed = 0
    with _lock:
        if not _state:
            return
        for key in list(_state.keys()):
            entry = _state.get(key)
            ts = entry.get('last_ts') or entry.get('first_ts') if isinstance(entry, dict) else None
            try:
                age_ok = ts and datetime.fromisoformat(ts).timestamp() >= cutoff
            except (ValueError, TypeError):
                age_ok = False
            if not age_ok:
                del _state[key]
                removed += 1
        if removed:
            _write_locked()
    if removed:
        logger.debug(f"[attempt_ledger] Pruned {removed} stale key(s)")


def snapshot():
    """Return a point-in-time copy of the ledger map.

    ``{key: {count, first_ts, last_ts}}`` with per-entry dict copies so
    callers (the /api/stuck collector) can read cross-thread without
    racing ``bump``/``prune``.  Empty dict before ``init()``.
    """
    with _lock:
        return {k: dict(v) for k, v in _state.items()}


def size():
    """Return the number of tracked keys. Test/debug hook."""
    with _lock:
        return len(_state)


def reset_all():
    """Clear every counter and persist. Test hook."""
    with _lock:
        _state.clear()
        _write_locked()


__all__ = ['init', 'bump', 'get', 'reset', 'prune', 'size', 'reset_all',
           'snapshot', 'last_seen_epoch']
