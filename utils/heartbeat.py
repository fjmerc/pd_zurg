"""Worker-thread heartbeat registry — liveness for the container healthcheck.

The Docker healthcheck (``healthcheck.py``) runs as a SEPARATE short-lived
process: it can see OS processes and FUSE mounts, but not the long-lived
worker threads inside the main process (blackhole watcher, task scheduler,
process monitor).  A dead scheduler loop silently stops ALL periodic work —
health sweeps, scheduled scans, backups, ledger prunes — while the
container stays "healthy" forever.

Each worker loop registers here and beats once per iteration.  The registry
flushes a throttled, atomic JSON snapshot to ``/healthcheck/heartbeats.json``
which ``healthcheck.py`` evaluates: an entry whose last beat is older than
its per-thread ceiling means the thread is dead or permanently wedged — a
Docker restart is the correct remedy for both.

Ceilings are deliberately generous (minutes): the goal is catching
permanent death, not transient slowness.  A long legitimate blockage
(TorBox cooldown chunk, slow FUSE walk) must never flap the container —
the same slow-vs-dead posture the mount probes already take.

Every public function is exception-safe by design: heartbeat plumbing
must never become a NEW kill path for the loop it protects.

Clock: timestamps are ``time.monotonic()``, NOT wall clock.  On Linux
CLOCK_MONOTONIC is system-wide (seconds since boot), so the healthcheck
process compares against the same clock as the beating workers — and an
NTP step (e.g. a homelab RTC drifting 20 min, corrected once the peer is
reachable) can't instantly age every live worker past its ceiling the
way wall-clock timestamps would.  After a HOST reboot a leftover file
holds timestamps from the previous boot epoch; those read as age 0
(never stale) until ``reset()`` clears them at startup — fail-safe in
the direction that matters.
"""

import json
import os
import threading
import time

from utils.logger import get_logger
from utils.file_utils import atomic_write

logger = get_logger()

# Module constant, not an env var — this is internal plumbing between the
# main process and healthcheck.py, not user-facing configuration.  Tests
# monkeypatch it.
HEARTBEAT_FILE = '/healthcheck/heartbeats.json'

# Minimum seconds between file writes.  Beats land in memory on every
# call; the file only needs to be fresh at healthcheck granularity (60s).
_FLUSH_INTERVAL = 5

_lock = threading.Lock()
_entries = {}  # name -> {'stale_after': int, 'last_beat': float}
_last_flush = 0.0


def reset():
    """Clear all entries and flush an empty snapshot.

    Called once at main-process startup.  The heartbeat file survives a
    ``docker restart`` (same container filesystem), so without a reset a
    worker that legitimately does not start this boot (feature disabled,
    setup validation failure) would leave a stale ghost entry from the
    previous run — flagging the container unhealthy in a restart loop.
    """
    try:
        with _lock:
            _entries.clear()
        _flush(force=True)
    except Exception as e:
        logger.warning(f"[heartbeat] reset failed: {type(e).__name__}: {e}")


def register(name, stale_after):
    """Register a worker thread under ``name``.

    ``stale_after`` (seconds) is the ceiling past which a missing beat
    marks the container unhealthy.  Re-registering an existing name
    resets its beat (a restarted worker starts a fresh window).
    """
    try:
        with _lock:
            _entries[name] = {'stale_after': int(stale_after),
                              'last_beat': time.monotonic()}
        _flush(force=True)
    except Exception as e:
        logger.warning(f"[heartbeat] register({name!r}) failed: "
                       f"{type(e).__name__}: {e}")


def unregister(name):
    """Remove a worker from liveness tracking.

    Must be called when a worker is stopped ON PURPOSE (feature disabled
    via config reload, orderly shutdown) — otherwise its entry goes stale
    and flags a healthy container.
    """
    try:
        with _lock:
            _entries.pop(name, None)
        _flush(force=True)
    except Exception as e:
        logger.warning(f"[heartbeat] unregister({name!r}) failed: "
                       f"{type(e).__name__}: {e}")


def beat(name):
    """Record a liveness beat for ``name``.  No-op if not registered
    (avoids resurrecting an entry a concurrent unregister just removed).
    """
    try:
        with _lock:
            entry = _entries.get(name)
            if entry is None:
                return
            entry['last_beat'] = time.monotonic()
        _flush()
    except Exception as e:
        logger.debug(f"[heartbeat] beat({name!r}) failed: "
                     f"{type(e).__name__}: {e}")


def _flush(force=False):
    global _last_flush
    now = time.time()
    # The write happens UNDER the lock: two concurrent flushes writing
    # outside it race on os.replace ordering, and a beat's snapshot
    # landing after an unregister's empty snapshot would resurrect the
    # removed entry on disk — a stale ghost that false-flags a healthy
    # container.  The dump is tiny; brief blocking of beat() is fine.
    with _lock:
        if not force and now - _last_flush < _FLUSH_INTERVAL:
            return
        _last_flush = now
        snapshot = {name: dict(entry) for name, entry in _entries.items()}
        try:
            directory = os.path.dirname(HEARTBEAT_FILE)
            if directory:
                os.makedirs(directory, exist_ok=True)
            with atomic_write(HEARTBEAT_FILE) as f:
                json.dump(snapshot, f)
        except Exception as e:
            logger.debug(f"[heartbeat] flush failed: {type(e).__name__}: {e}")


def stale_entries(path=None, now=None):
    """Return ``[(name, age_seconds, stale_after), ...]`` for every entry
    whose last beat is older than its ceiling.  Consumed by healthcheck.py
    (a separate process — this reads the file, not module state).

    Missing or unreadable/corrupt file → ``[]``: the feature degrades to
    the pre-heartbeat healthcheck rather than failing the container on
    its own plumbing.  Entries with malformed fields are skipped for the
    same reason.  A ``last_beat`` in the future reads as age 0 — never
    stale (fail-safe for cross-boot leftovers, see clock note above).
    """
    if path is None:
        path = HEARTBEAT_FILE
    if now is None:
        now = time.monotonic()
    try:
        with open(path, encoding='utf-8') as f:
            data = json.load(f)
    except (OSError, ValueError):
        return []
    if not isinstance(data, dict):
        return []
    stale = []
    for name, entry in data.items():
        if not isinstance(entry, dict):
            continue
        try:
            last_beat = float(entry['last_beat'])
            stale_after = float(entry['stale_after'])
        except (KeyError, TypeError, ValueError):
            continue
        if stale_after <= 0:
            continue
        age = max(0.0, now - last_beat)
        if age > stale_after:
            stale.append((str(name), age, stale_after))
    return stale
