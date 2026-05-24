"""Debrid health reconciler — detect RD-side filter blocks on the
existing torrent set.

Closes pd_zurg's blind spot to Real-Debrid's May 2026 keyword filter:
RD's ``/torrents`` listing still advertises blocked torrents as healthy,
so Zurg's WebDAV view and ``os.path.exists`` on the FUSE mount both
report them as present. The only way to detect a block is to probe
``/unrestrict/link`` on a sample file per torrent — which is exactly
what ``RealDebridClient.probe_file`` does (plan 38 phase 1). This
module schedules and persists that probing.

Phase 2 scope: detection + state persistence only. No remediation.
Auto-deletion + arr re-search land in phase 4.
"""

import json
import os
import threading
import time

from utils.debrid_client import get_debrid_client
from utils.file_utils import atomic_write
from utils.logger import get_logger

logger = get_logger()


# ---------------------------------------------------------------------------
# Module constants (intentionally not env vars — see plan 38 for rationale)
# ---------------------------------------------------------------------------

_STATE_PATH = '/config/debrid_health.json'
_STATE_VERSION = 1
_STATE_MAX_BYTES = 10 * 1024 * 1024     # 10 MiB hard ceiling on the state file
_PROBE_TTL = 7 * 24 * 3600              # don't re-probe healthy torrents for 7d
_RATE_LIMIT_PER_MIN = 60                # under RD's 250/min user quota
_MAX_PER_SWEEP = 2000                   # probe cap per sweep
_REMEDIATE_MAX_PER_SWEEP = 100          # delete cap per sweep — defense against mass-delete on first auto-remediate enable
_PERSIST_EVERY = 25                     # incremental save every N probes
_VALID_STATUSES = ('healthy', 'blocked', 'unknown')

# Future-dated tolerance: a state ts more than 24h ahead of now is rejected
# as clock skew or tampering. Matches _load_webdav_capability in library.py.
_FUTURE_TOLERANCE = 86400


# ---------------------------------------------------------------------------
# Module state — lazy-loaded singleton, guarded by _lock
# ---------------------------------------------------------------------------

_lock = threading.Lock()
_state = None


def _enabled():
    """Master toggle. Honours runtime env-var changes (SIGHUP / UI edits)
    without needing a restart, so the scheduled task can be turned off
    on a live container."""
    return str(os.environ.get('DEBRID_HEALTH_ENABLED', 'true')).lower() == 'true'


def _auto_remediate_enabled():
    """Whether the sweep should delete blocked torrents from RD + trigger
    arr re-search. Off by default — mutating debrid state is opt-in.
    Honours runtime env-var changes for the same reason as ``_enabled``."""
    return str(os.environ.get('DEBRID_HEALTH_AUTO_REMEDIATE', 'false')).lower() == 'true'


def _empty_state():
    return {'version': _STATE_VERSION, 'probed': {}}


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------

def _load_state():
    """Read ``_STATE_PATH`` with strict validation.

    Returns an empty state on any failure (missing file, corruption,
    schema mismatch, oversized file, future-dated entries). Never
    raises — worst case is one extra sweep of re-probes. Posture
    mirrors ``library.LibraryScanner._load_webdav_capability``: size
    cap via ``os.fstat`` on the open fd to close the TOCTOU window
    between sizing and reading, strict-bool / strict-status checks,
    future-dated rejection, and a single ``OSError | ValueError``
    catch that absorbs corruption without blowing up the caller.
    """
    if not os.path.isfile(_STATE_PATH):
        return _empty_state()
    try:
        with open(_STATE_PATH, 'r', encoding='utf-8') as fh:
            size = os.fstat(fh.fileno()).st_size
            if size > _STATE_MAX_BYTES:
                logger.warning(
                    f"[debrid_health] state file exceeds {_STATE_MAX_BYTES} "
                    f"bytes ({size}), starting fresh"
                )
                return _empty_state()
            raw = json.load(fh)
    except (OSError, ValueError) as e:
        logger.warning(f"[debrid_health] cannot load state, starting fresh: {e}")
        return _empty_state()

    if not isinstance(raw, dict) or raw.get('version') != _STATE_VERSION:
        logger.warning("[debrid_health] state schema mismatch, starting fresh")
        return _empty_state()

    probed = raw.get('probed')
    if not isinstance(probed, dict):
        return _empty_state()

    now = time.time()
    cleaned = {}
    dropped = 0
    for k, v in probed.items():
        if not isinstance(k, str) or not isinstance(v, dict):
            dropped += 1
            continue
        ts = v.get('ts')
        if not isinstance(ts, (int, float)) or ts > now + _FUTURE_TOLERANCE:
            dropped += 1
            continue
        if v.get('status') not in _VALID_STATUSES:
            dropped += 1
            continue
        cleaned[k] = v
    if dropped:
        logger.info(f"[debrid_health] dropped {dropped} malformed state entries")
    return {'version': _STATE_VERSION, 'probed': cleaned}


def _save_state(state):
    """Atomic write of state. Best-effort — failures log and continue
    so a read-only ``/config`` can't take the scheduler offline."""
    try:
        with atomic_write(_STATE_PATH) as fh:
            json.dump(state, fh, separators=(',', ':'))
    except (OSError, ValueError, TypeError) as e:
        logger.warning(f"[debrid_health] cannot persist state: {e}")


def _get_state():
    """Lazy-load + return the singleton state dict. Holds ``_lock``
    only while assigning the singleton."""
    global _state
    with _lock:
        if _state is None:
            _state = _load_state()
        return _state


# ---------------------------------------------------------------------------
# Public read API (used by library enrichment in phase 3)
# ---------------------------------------------------------------------------

def get_blocked_hashes():
    """Return the set of uppercased infohashes flagged as blocked.

    Callers (e.g. ``library._enrich_with_tmdb_cache``) MUST uppercase
    their lookup hash for consistency with the state file's keying.
    """
    state = _get_state()
    with _lock:
        return {
            h for h, v in state['probed'].items()
            if isinstance(v, dict) and v.get('status') == 'blocked'
        }


# ---------------------------------------------------------------------------
# Sweep
# ---------------------------------------------------------------------------

def _should_skip(torrent_hash, state, now):
    """TTL skip rule. Only ``healthy`` entries within ``_PROBE_TTL``
    are skipped — ``blocked`` re-probes so we notice if RD reverses
    the filter; ``unknown`` re-probes ASAP since we don't have a verdict."""
    entry = state['probed'].get(torrent_hash)
    if not isinstance(entry, dict) or entry.get('status') != 'healthy':
        return False
    ts = entry.get('ts', 0)
    return isinstance(ts, (int, float)) and (now - ts) < _PROBE_TTL


def _remediate(client, torrent_id, torrent_hash, filename, probe_result):
    """Run the three-step remediation pipeline for a confirmed-blocked torrent.

    Order is deliberate:
      1. **Blocklist first.** Even if the RD delete fails, the next time
         Sonarr/Radarr tries to grab the same release, the blackhole's
         pre-submit blocklist check (blackhole.py) rejects it — breaks the
         grab → filter → re-grab loop.
      2. **Delete from RD.** Removes the entry from ``/torrents`` so Zurg
         drops it from the WebDAV listing on next sync, which in turn lets
         pd_zurg's existing ``_cleanup_broken_debrid_symlinks`` reap the
         dangling symlinks.
      3. **Trigger arr re-search.** Sonarr/Radarr look for a replacement
         release. Shares the 2 h ``_retrigger_history`` cooldown with the
         broken-symlink and stale-grab paths so a single release isn't
         re-searched twice within the window even if multiple subsystems
         fire on it at the same time.

    Each step is independently try/except'd so a failure in one doesn't
    abort the others. Returns a dict of booleans describing what
    succeeded — caller stores these on the state entry and the history
    event so a partial failure is visible in the activity feed.
    """
    actions = {'blocklisted': False, 'deleted': False, 'researched': False}

    # Step 1: blocklist (idempotent — re-adding an existing hash is a no-op)
    try:
        from utils import blocklist as _blocklist
        reason = probe_result.get('reason') or 'unknown'
        entry_id = _blocklist.add(
            torrent_hash, filename,
            reason=f'RD filter ({reason})',
            source='auto',
        )
        actions['blocklisted'] = bool(entry_id)
    except Exception as e:
        logger.warning(
            f"[debrid_health] blocklist add failed for {torrent_hash}: {e}"
        )

    # Step 2: delete from RD
    try:
        actions['deleted'] = bool(client.delete_torrent(torrent_id))
    except Exception as e:
        logger.warning(
            f"[debrid_health] delete_torrent failed for {torrent_id}: {e}"
        )

    # Step 3: arr re-search. Release name = filename without media
    # extension; matches the shape ``_attempt_arr_research`` expects
    # (same source format used by blackhole and verify_symlinks).
    # force_episodes=True because Zurg's WebDAV listing keeps the just-
    # deleted file visible for ~15-30 s, so Sonarr's ``hasFile`` is still
    # True at this instant — without the override, every TV episode in
    # the release would be skipped (see 2026-05-24 live sweep regression).
    release_name = os.path.splitext(filename)[0] if filename else ''
    if release_name:
        try:
            from utils.scheduled_tasks import _attempt_arr_research
            actions['researched'] = bool(
                _attempt_arr_research(release_name, force_episodes=True)
            )
        except Exception as e:
            logger.warning(
                f"[debrid_health] arr re-search failed for {release_name!r}: {e}"
            )

    # Step 4: history event — emitted regardless of which steps succeeded
    # so a partial failure is auditable in the activity feed.
    try:
        from utils import history as _history
        _history.log_event(
            type='debrid',
            title=filename or torrent_id,
            source='debrid_health',
            detail='Filter-blocked on debrid — auto-remediated',
            meta={
                'cause': _history.CAUSE_DEBRID_FILTERED,
                'reason': probe_result.get('reason') or 'infringing_file',
                'http': probe_result.get('http'),
                'info_hash': torrent_hash,
                'torrent_id': torrent_id,
                **actions,
            },
        )
    except Exception as e:
        logger.debug(f"[debrid_health] history log failed: {e}")

    return actions


def run_sweep():
    """Probe up to ``_MAX_PER_SWEEP`` Real-Debrid torrents and persist state.

    Returns a result dict compatible with the scheduler's expectations
    (``status``, ``message``, optional ``items``). The sweep is a no-op
    when ``DEBRID_HEALTH_ENABLED`` is false or no RD client is configured.

    Rate-limited to ``_RATE_LIMIT_PER_MIN`` probes/minute with an
    incremental persist every ``_PERSIST_EVERY`` probes so an
    interrupted run doesn't lose all work.
    """
    if not _enabled():
        logger.debug("[debrid_health] disabled, skipping sweep")
        return {'status': 'success', 'message': 'disabled', 'items': 0}

    client, _ = get_debrid_client(service='realdebrid')
    if client is None or not client.configured:
        logger.debug("[debrid_health] RD not configured, skipping sweep")
        return {'status': 'success', 'message': 'no RD client', 'items': 0}

    try:
        torrents = client.list_torrents()
    except Exception as e:
        logger.warning(f"[debrid_health] list_torrents failed: {e}")
        return {'status': 'error', 'message': f'list_torrents failed: {e}'}

    state = _get_state()
    now = time.time()
    counts = {'probed': 0, 'healthy': 0, 'blocked': 0,
              'unknown': 0, 'skipped': 0, 'remediated': 0}
    remediate_on = _auto_remediate_enabled()

    # Two-pass: filter eligibility first so the rate-limit accounting
    # below knows the true probe count (skipped torrents don't consume
    # API calls and shouldn't impose sleep delays).
    eligible = []
    for t in torrents:
        if not isinstance(t, dict):
            continue
        tid = t.get('id')
        h = (t.get('hash') or '').upper()
        if not tid or not h:
            continue
        if _should_skip(h, state, now):
            counts['skipped'] += 1
            continue
        eligible.append((tid, h, t.get('filename', '')))

    capped = eligible[:_MAX_PER_SWEEP]
    sleep_seconds = 60.0 / _RATE_LIMIT_PER_MIN

    for i, (tid, torrent_hash, filename) in enumerate(capped):
        result = client.probe_file(tid)
        status = result.get('status', 'unknown')
        if status not in _VALID_STATUSES:
            status = 'unknown'
        counts['probed'] += 1
        counts[status] += 1

        entry = {
            'status': status,
            'reason': result.get('reason') or result.get('error', ''),
            'http': result.get('http'),
            'ts': time.time(),
            'torrent_id': tid,
            'filename': filename,
        }

        # Auto-remediate the blocked entry if enabled and under the
        # per-sweep cap. Cap exists separately from _MAX_PER_SWEEP so
        # a first-run enable of AUTO_REMEDIATE on a large library can't
        # mass-delete in one shot — remaining blocked entries stay
        # flagged and remediate on subsequent sweeps.
        if (status == 'blocked'
                and remediate_on
                and counts['remediated'] < _REMEDIATE_MAX_PER_SWEEP):
            actions = _remediate(client, tid, torrent_hash, filename, result)
            entry['remediated'] = True
            entry['remediation_actions'] = actions
            counts['remediated'] += 1

        with _lock:
            state['probed'][torrent_hash] = entry

        if counts['probed'] % _PERSIST_EVERY == 0:
            with _lock:
                _save_state(state)

        # No sleep after the last probe — wasted wall time on the
        # scheduled-task UI's "currently running" indicator.
        if i < len(capped) - 1:
            time.sleep(sleep_seconds)

    with _lock:
        _save_state(state)

    msg_parts = [
        f"probed {counts['probed']}",
        f"healthy {counts['healthy']}",
        f"blocked {counts['blocked']}",
        f"unknown {counts['unknown']}",
        f"skipped {counts['skipped']}",
    ]
    if remediate_on:
        msg_parts.append(f"remediated {counts['remediated']}")
    msg = ', '.join(msg_parts)
    logger.info(f"[debrid_health] sweep complete: {msg}")

    # One summary notification per sweep when anything was remediated.
    # Per-item events still flow to history.py for the activity feed —
    # this saves the user from N apprise messages on a large initial
    # cleanup. Skipped entirely when remediation is off or zero deletes.
    if remediate_on and counts['remediated']:
        try:
            from utils.notifications import notify
            notify(
                'debrid_filtered',
                f'Debrid health: {counts["remediated"]} filter-blocked torrent(s) removed',
                f"Removed from debrid: {counts['remediated']}. "
                f"Total blocked detected this sweep: {counts['blocked']}. "
                f"Arr re-search triggered per release where applicable.",
                level='warning',
            )
        except Exception as e:
            logger.debug(f"[debrid_health] sweep summary notify failed: {e}")

    return {'status': 'success', 'message': msg, 'items': counts['probed']}


# ---------------------------------------------------------------------------
# Test hook
# ---------------------------------------------------------------------------

def _reset_for_testing():
    """Clear the lazy-loaded singleton. Tests call this between cases
    so each test gets a fresh state load from the (test-monkeypatched)
    ``_STATE_PATH``. Production code never calls this."""
    global _state
    with _lock:
        _state = None
