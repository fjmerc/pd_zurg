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

from utils.debrid_client import get_debrid_client, RD_LIST_LIMIT
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
_RESCUE_READY_TIMEOUT = 60              # max wall-clock seconds to wait for a TB rescue add to reach a ready state ('cached'/'completed'/'uploading')
_RESCUE_POLL_INTERVAL = 3               # seconds between TB status polls during rescue
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

# Shutdown coordination: rescue polling and per-probe rate-limit sleeps
# wait on this event so a SIGTERM / scheduler.stop() doesn't get stalled
# behind a 60s × N-rescue blocking sleep loop.  Set by ``request_stop()``
# (called from main.py's shutdown handler) and consumed by ``run_sweep``.
_stop_event = threading.Event()


def request_stop():
    """Signal an in-flight sweep to abort early.

    Called from the process-level shutdown path so SIGTERM doesn't get
    stalled behind rescue-polling.  Safe to call repeatedly; safe to
    call when no sweep is running.  Cleared automatically at the start
    of the next ``run_sweep`` so subsequent sweeps aren't muzzled.
    """
    _stop_event.set()


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


_TRUTHY_VALUES = ('true', '1', 'yes')
_FALSY_VALUES = ('false', '0', 'no')


def _cross_rescue_enabled():
    """Whether to attempt cross-debrid rescue (plan 39 phase 3) when RD
    filter-blocks a torrent that TorBox has cached.

    Default-resolved: ON when both ``RD_API_KEY`` and ``TORBOX_API_KEY``
    are set, OFF otherwise.  Explicit ``DEBRID_HEALTH_CROSS_RESCUE``
    env var overrides the default — set to a truthy value
    (``true``/``1``/``yes``) to force-attempt rescue when only one is
    set (no-op in practice — the rescue helper short-circuits when the
    alt isn't configured), or a falsy value (``false``/``0``/``no``)
    to disable rescue even with both keys present (e.g. while
    debugging the remediation path).  Same vocabulary as
    config_validator._is_truthy so operator muscle-memory carries
    across env vars.
    """
    explicit = (os.environ.get('DEBRID_HEALTH_CROSS_RESCUE') or '').lower()
    if explicit in _TRUTHY_VALUES:
        return True
    if explicit in _FALSY_VALUES:
        return False
    return bool(os.environ.get('RD_API_KEY')
                and os.environ.get('TORBOX_API_KEY'))


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


def restore_state_bytes(data):
    """Replace ``_STATE_PATH`` on disk AND the live singleton (backup restore).

    The file write and the in-memory refresh happen under a single
    ``_lock`` hold so an in-flight sweep (which persists via
    ``with _lock: _save_state(state)``) can't interleave and clobber the
    restored file with pre-restore memory.

    Mutates the existing dict in place rather than reassigning, so an
    in-flight sweep holding a reference from ``_get_state`` keeps
    operating on the live object — its next ``_save_state`` persists
    the restored data instead of an orphaned copy.
    """
    global _state
    with _lock:
        with atomic_write(_STATE_PATH, mode='wb') as fh:
            fh.write(data)
        fresh = _load_state()
        if _state is None:
            _state = fresh
        else:
            _state.clear()
            _state.update(fresh)


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
# Summary API (used by the System page mini-dashboard, plan 38 phase 5)
# ---------------------------------------------------------------------------

def get_summary():
    """Snapshot of reconciler state for the System page UI.

    Returns a dict with:
      - ``rd_configured``: bool — false when no RD credential is present.
        Legacy compat for the original single-card UI.
      - ``enabled``: bool — current ``DEBRID_HEALTH_ENABLED`` value.
      - ``auto_remediate``: bool — current ``DEBRID_HEALTH_AUTO_REMEDIATE``.
      - ``last_sweep_ts``: epoch seconds of the most recent probe in
        state (None if no sweep has run). Approximates the sweep end time
        — probes within a sweep are spread across the rate-limit window
        but the max is close enough for "how stale is this data".
      - ``counts``: {healthy, blocked, unknown, total}.
      - ``remediated_24h``: count of ``debrid_filtered`` history events
        in the last 24 h. Best-effort — silently 0 on any history error.
      - ``providers``: per-debrid block — list of card dicts with
        {service, configured, label, counts}.  Plan 39 phase 5: the UI
        renders two cards side-by-side when both RD + TB are configured,
        collapses to one when only one is.

    No state file schema change.  TB-side counts are derived from the
    TB rescue-cache section of the state file, NOT a separate state file.
    Per-provider ``last_probe_age`` is computed on-the-fly from per-entry
    timestamps so existing state files don't need a migration.
    """
    rd_configured = bool(
        os.environ.get('RD_API_KEY')
        or os.path.isfile('/run/secrets/rd_api_key')
    )
    tb_configured = bool(os.environ.get('TORBOX_API_KEY'))

    # Plan 39 phase 5: per-provider snapshots so the UI can render side-
    # by-side cards.  Computed from the same state file; the reconciler
    # itself is still RD-side only (plan 38 scope) but the dashboard
    # surfaces TB cache-rescue + configured-vs-not signals so users see
    # both providers' status at a glance.
    rescued_total, rescued_24h, remediated_24h = _summary_history_counts()

    providers = []
    if rd_configured:
        providers.append(_provider_card('realdebrid'))
    if tb_configured:
        providers.append(_provider_card('torbox', tb_rescued_total=rescued_total))

    if not rd_configured:
        # Preserve the original empty response so the existing UI's
        # ``!s.rd_configured → hide`` check stays valid.  ``providers``
        # is exposed unconditionally so the dual-card view works even
        # when only TB is configured.
        return {'rd_configured': False, 'providers': providers,
                'rescued_24h': rescued_24h}

    counts = {'healthy': 0, 'blocked': 0, 'unknown': 0, 'total': 0}
    last_sweep_ts = None
    state = _get_state()
    with _lock:
        for entry in state['probed'].values():
            if not isinstance(entry, dict):
                continue
            status = entry.get('status')
            if status in counts:
                counts[status] += 1
            counts['total'] += 1
            ts = entry.get('ts')
            if isinstance(ts, (int, float)):
                if last_sweep_ts is None or ts > last_sweep_ts:
                    last_sweep_ts = ts

    return {
        'rd_configured': True,
        'enabled': _enabled(),
        'auto_remediate': _auto_remediate_enabled(),
        'cross_rescue': _cross_rescue_enabled(),
        'last_sweep_ts': last_sweep_ts,
        'counts': counts,
        'remediated_24h': remediated_24h,
        'rescued_24h': rescued_24h,
        'providers': providers,
    }


_PROVIDER_LABELS = {
    'realdebrid': 'Real-Debrid',
    'alldebrid': 'AllDebrid',
    'torbox': 'TorBox',
}


def _summary_history_counts():
    """(rescued_total, rescued_24h, remediated_24h) from one history pass.

    Rescues span ALL rescue paths — sweep cross-rescues, blackhole
    cached-alternative grabs, and Wanted-backlog recoveries.  The
    sweep-state ``rescued=True`` flag structurally undercounts: it only
    sees sweep-time rescues (blackhole/library paths rescue blocked
    content upstream, before a sweep ever probes it), and a re-probe
    rebuilds the state entry without the flag.  History is the source of
    truth; totals are bounded by the history retention window (default
    30 days).  Best-effort — zeros on any history error.
    """
    try:
        from datetime import datetime, timedelta, timezone
        from utils import history as _history
        rescue_causes = (
            _history.CAUSE_DEBRID_RESCUED,
            _history.CAUSE_TB_CACHED_ALT_GRABBED,
            _history.CAUSE_WANTED_TB_RECOVERED,
        )
        start = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(timespec='seconds')
        total, recent = _history.count_by_cause_windows(
            rescue_causes + (_history.CAUSE_DEBRID_FILTERED,), start=start,
        )
        rescued_total = sum(total[c] for c in rescue_causes)
        rescued_24h = sum(recent[c] for c in rescue_causes)
        return rescued_total, rescued_24h, recent[_history.CAUSE_DEBRID_FILTERED]
    except Exception as e:
        logger.warning(f"[debrid_health] summary history counts failed: {e}")
        return 0, 0, 0


def _provider_card(service, tb_rescued_total=0):
    """Per-provider card data for the dashboard.

    Today only RD has a real probed-state set; TB's card reflects the
    history-derived rescue count (``tb_rescued_total``, spanning the
    retention window) plus the configured signal.  AD support is
    structurally present but reports an empty counts block until a
    probe path lands.
    """
    label = _PROVIDER_LABELS.get(service, service)
    if service == 'realdebrid':
        counts = {'healthy': 0, 'blocked': 0, 'unknown': 0, 'total': 0}
        last_probe = None
        state = _get_state()
        with _lock:
            for entry in state['probed'].values():
                if not isinstance(entry, dict):
                    continue
                status = entry.get('status')
                if status in counts:
                    counts[status] += 1
                counts['total'] += 1
                ts = entry.get('ts')
                if isinstance(ts, (int, float)):
                    if last_probe is None or ts > last_probe:
                        last_probe = ts
        return {
            'service': service, 'label': label, 'configured': True,
            'counts': counts, 'last_probe_ts': last_probe,
        }
    if service == 'torbox':
        # TB doesn't have a probed-state set — the rescue count comes
        # from history (all rescue paths, retention-window-bounded).
        # The state loop survives only to surface the most recent
        # sweep-rescue timestamp.
        last_probe = None
        state = _get_state()
        with _lock:
            for entry in state['probed'].values():
                if not isinstance(entry, dict):
                    continue
                if entry.get('rescued') is True:
                    ts = entry.get('ts')
                    if isinstance(ts, (int, float)):
                        if last_probe is None or ts > last_probe:
                            last_probe = ts
        return {
            'service': service, 'label': label, 'configured': True,
            'counts': {'rescued': tb_rescued_total},
            'last_probe_ts': last_probe,
        }
    return {
        'service': service, 'label': label, 'configured': True,
        'counts': {}, 'last_probe_ts': None,
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


def _release_anchor_candidates(filename, src_client=None, src_torrent_id=None):
    """Derive the set of mount folder names that identify THIS torrent's
    symlink targets.

    The sweep's ``filename`` comes from the source debrid's list entry,
    which for multi-file RD torrents can be an INNER file path
    (``Show/S01/ep.mkv``) rather than the torrent's top-level folder.
    Anchoring the retarget on that basename stem either matches nothing
    (silent rescue no-op) or — worse — matches a DIFFERENT torrent whose
    folder name equals the inner file's stem (e.g. a single-episode
    release of the same episode a season pack contains), retargeting an
    unrelated torrent's symlinks to a mount that doesn't have its
    content.

    When a source client is available, pull authoritative names from
    the torrent-info endpoint: ``filename`` / ``original_filename``
    (the torrent names Zurg derives the mount folder from) and the
    first path component of every multi-file ``files[].path``.  Folder
    names are added verbatim — NO extension stripping, since
    ``splitext`` on a dotted release name chops the ``.WEB-DL`` tail
    and would widen the match.

    Only when no authoritative name is derivable does the legacy
    behavior apply: the list-entry filename's basename stem.  Returns
    an empty set when nothing is derivable at all — callers must
    decline the retarget rather than guess.
    """
    candidates = set()

    def _add_top(name):
        if not isinstance(name, str):
            return
        name = name.strip().strip('/')
        top = name.split('/')[0]
        # ``.`` / ``..`` are never real release folders and would let a
        # retarget escape the intended tree — reject them.
        if top and top not in ('.', '..'):
            candidates.add(top)

    info = None
    if src_client is not None and src_torrent_id:
        try:
            info_fn = getattr(src_client, 'torrent_info', None)
            if callable(info_fn):
                info = info_fn(src_torrent_id)
        except Exception as e:
            logger.debug(
                f"[debrid_health] torrent_info fetch for anchor derivation "
                f"failed: {type(e).__name__}"
            )
    if isinstance(info, dict):
        _add_top(info.get('filename'))
        _add_top(info.get('original_filename'))
        if not candidates:
            # Only fall back to ``files[].path`` when the torrent carries
            # no authoritative name.  Per-file first components are unsafe
            # to trust individually: a wrapper-less torrent gives generic
            # folders (``Season 02``, ``Sample``, ``CD1``) that collide
            # with unrelated releases.  Only accept a first component that
            # is UNANIMOUS across every multi-file path — that unanimity
            # is what makes it the torrent's own top folder rather than a
            # per-file subdir.
            files = info.get('files')
            if isinstance(files, list):
                first_components = set()
                have_multifile = False
                for f in files:
                    if not isinstance(f, dict):
                        continue
                    p = f.get('path')
                    if isinstance(p, str) and '/' in p.strip('/'):
                        have_multifile = True
                        first_components.add(p.strip().strip('/').split('/')[0])
                if have_multifile and len(first_components) == 1:
                    _add_top(next(iter(first_components)))

    if not candidates:
        raw = (filename or '').strip().strip('/')
        if '/' in raw:
            # Inner file path: only the first component can be a top
            # folder.  The basename stem is exactly the dangerous
            # needle — never add it.
            _add_top(raw)
        elif raw:
            candidates.add(raw)
            stem = os.path.splitext(raw)[0]
            if stem:
                candidates.add(stem)

    candidates.discard('')
    return candidates


def _attempt_cross_rescue(torrent_hash, filename, source_debrid='realdebrid',
                          src_client=None, src_torrent_id=None):
    """Try to re-host a filter-blocked torrent on the alt debrid.

    Plan 39 phase 3 — short-circuit the existing blocklist+delete+research
    pipeline when the alt debrid (typically TorBox when source is RD) has
    the same content cached.  Symlinks in the arr library are retargeted
    to the alt mount so the file plays without an arr re-import.

    Returns a dict describing the rescue outcome:

        {'rescued': True,  'to': 'torbox', 'tb_torrent_id': '...',
         'retargeted': N, 'outcome': 'symlinks_retargeted' | 'no_symlinks_found'}
        {'rescued': False, 'reason': 'not_cached_on_alt' | 'add_failed'
                                    | 'never_ready' | 'no_alt_configured'
                                    | 'no_alt_mount' | 'error'}

    On rescued=False the caller falls through to the existing
    ``_remediate`` pipeline.  On rescued=True the caller skips
    blocklist+delete+research and emits the ``debrid_rescued`` history
    event with this dict's meta fields.

    Failure modes are all soft — never raises, never partially mutates.

    Plan 41 phase A — the cache-probe + add + wait-for-ready core is
    delegated to ``utils.debrid_routing.attempt_add_rescue`` so the
    sweep-driven and blackhole-add-time rescues share one implementation.
    Plex-active-session guard and symlink retargeting stay inline since
    they are debrid_health-specific (add-time rescues have no existing
    symlinks to disrupt).
    """
    from utils.debrid_routing import (attempt_add_rescue, pick_alt_debrid,
                                       make_preexisting_check)
    from utils.blackhole import TB_READY_STATES
    import time as _time

    # Pre-resolve the alt-debrid client in debrid_health's namespace so
    # ``get_debrid_client`` is the locally-imported symbol — that's the
    # one the rescue test suite patches via ``_patch_clients``.  Passing
    # the resolved client into the shared helper avoids the helper
    # re-importing get_debrid_client from a namespace tests don't patch.
    alt = pick_alt_debrid(source_debrid)
    if not alt:
        # Distinguish unsupported source from "user only has one debrid"
        # to match the pre-refactor reason vocabulary.
        from utils.debrid_routing import configured_debrids
        configured = configured_debrids()
        reason = 'unsupported_source' if source_debrid not in configured else 'no_alt_configured'
        return {'rescued': False, 'reason': reason}

    alt_key = os.environ.get(f'{alt.upper()}_API_KEY')
    if not alt_key:
        return {'rescued': False, 'reason': 'no_alt_configured'}

    try:
        alt_client, _svc = get_debrid_client(service=alt, api_key=alt_key)
    except Exception as e:
        logger.warning(
            f"[debrid_health] rescue get_debrid_client failed: "
            f"{type(e).__name__}"
        )
        return {'rescued': False, 'reason': 'no_alt_client'}
    if alt_client is None or not getattr(alt_client, 'configured', False):
        return {'rescued': False, 'reason': 'no_alt_client'}

    # Guard the shared helper's cleanup deletes: the alt add hash-dedups,
    # so a rescue that can't reach ready must never delete an entry the
    # user already had on the alt account (TB ``created_at`` / RD ``added``
    # predating our probe ⇒ pre-existing, skip the delete).
    _probe_start = _time.time()
    core = attempt_add_rescue(
        torrent_hash, source_debrid,
        alt_debrid=alt,
        alt_client=alt_client,
        ready_states=TB_READY_STATES,
        stop_event=_stop_event,
        ready_timeout=_RESCUE_READY_TIMEOUT,
        poll_interval=_RESCUE_POLL_INTERVAL,
        preexisting_check=make_preexisting_check(_probe_start),
        logger_prefix='debrid_health',
    )
    if not core.get('rescued'):
        # Pass through the failure reason.  Tests + callers read
        # ``tb_torrent_id`` (the legacy field name) for cleanup hints
        # when the rescue allocated an alt entry but couldn't reach
        # ready — rename for back-compat.
        result = {'rescued': False, 'reason': core.get('reason', 'error')}
        if core.get('alt_torrent_id'):
            result['tb_torrent_id'] = core['alt_torrent_id']
        return result

    alt = core['to']
    alt_tid = core['alt_torrent_id']
    alt_client = core['alt_client']

    # Anchor derivation — the set of mount folder names that identify
    # this torrent's symlink targets.  Both the Plex guard and the
    # retarget walk key off these; deriving once keeps them consistent
    # (the guard must protect exactly the symlinks the walk would touch).
    candidates = _release_anchor_candidates(filename, src_client, src_torrent_id)

    # Plex active-session guard — never swap a symlink that's mid-stream.
    # FUSE caches the open file descriptor so playback usually continues
    # from the original target, but new seeks fail after the rewrite.
    # Defer this rescue to the next sweep when Plex is actively playing
    # the file.  Same posture as duplicate_cleanup's read-only guard.
    #
    # Lives in debrid_health (not the shared helper) because add-time
    # rescues have no existing symlinks to disrupt — Plex can't be
    # mid-stream on a file the arr hasn't imported yet.
    if any(_plex_session_active_for_release(name) for name in candidates):
        logger.info(
            f"[debrid_health] rescue deferred — Plex is actively streaming "
            f"one of {sorted(candidates)!r}; will retry next sweep"
        )
        # Clean up the TB add so we don't accumulate dead entries on the
        # alt debrid while we wait for Plex to stop streaming.
        try:
            alt_client.delete_torrent(alt_tid)
        except Exception:
            pass
        return {'rescued': False, 'reason': 'plex_session_active',
                'tb_torrent_id': alt_tid}

    # File is now accessible via the alt mount.  Retarget any existing
    # in-library symlinks pointing at the RD base so they resolve to
    # the alt base instead.  Zero symlinks found ≠ failure — newly
    # blocked content might not have been imported by the arr yet, and
    # the rescue still made the file accessible going forward.
    if candidates:
        retargeted = _retarget_symlinks_to_alt(
            torrent_hash, filename,
            src_debrid=source_debrid, dst_debrid=alt,
            candidates=candidates,
            alt_client=alt_client, alt_torrent_id=alt_tid,
        )
        outcome = 'symlinks_retargeted' if retargeted else 'no_symlinks_found'
    else:
        # No anchor derivable → we cannot scope the walk to this torrent.
        # Declining beats guessing: a mis-anchored retarget rewrites an
        # UNRELATED torrent's symlinks to a mount that lacks its content.
        logger.warning(
            f"[debrid_health] rescue retarget declined — no release anchor "
            f"derivable for hash {torrent_hash[:8]}… (filename={filename!r}); "
            f"content is reachable via {alt} but existing symlinks were left "
            f"untouched"
        )
        retargeted = 0
        outcome = 'anchor_underivable'

    return {
        'rescued': True,
        'to': alt,
        'tb_torrent_id': alt_tid,
        'retargeted': retargeted,
        'outcome': outcome,
    }


def _plex_session_active_for_release(release_name):
    """Return True if Plex reports any active playback whose file path
    contains ``/<release_name>/`` — i.e. the symlink we'd retarget is
    currently being streamed.

    Best-effort: any error talking to Plex (no creds, network down,
    plexapi missing, parsing failure) returns False — better to risk
    a retarget collision than to defer rescues indefinitely because
    Plex is offline.  Result is NOT cached; callers gate one rescue
    per call so the cost is at most one /status/sessions request per
    rescue (cheap; runs every 12h by default).
    """
    if not release_name:
        return False
    try:
        plex_addr = os.environ.get('PLEX_ADDRESS') or os.environ.get('PLEXADD')
        plex_token = os.environ.get('PLEX_TOKEN') or os.environ.get('PLEXTOKEN')
        if not (plex_addr and plex_token):
            return False
        from plexapi.server import PlexServer
        server = PlexServer(plex_addr, plex_token, timeout=5)
        needle = '/' + release_name + '/'
        for session in server.sessions() or ():
            try:
                for media in getattr(session, 'media', None) or ():
                    for part in getattr(media, 'parts', None) or ():
                        file_path = getattr(part, 'file', '') or ''
                        if needle in file_path:
                            return True
            except Exception:
                continue
        return False
    except Exception as e:
        logger.debug(f"[debrid_health] Plex sessions probe failed: {type(e).__name__} — assuming no active stream")
        return False


def _retarget_symlinks_to_alt(torrent_hash, filename, src_debrid='realdebrid', dst_debrid='torbox',
                              candidates=None, alt_client=None, alt_torrent_id=None):
    """Find symlinks in the arr libraries pointing at the source debrid's
    mount FOR THIS SPECIFIC RELEASE, and atomic-retarget each one to the
    equivalent path under the alt debrid's mount.

    Returns the number of symlinks retargeted.  Errors per-symlink are
    logged and counted as skipped — never raise; rescue is best-effort.

    Strategy: walk ``BLACKHOLE_LOCAL_LIBRARY_TV`` and
    ``BLACKHOLE_LOCAL_LIBRARY_MOVIES`` (the arr-side library roots), and
    for each symlink whose target points into the source debrid's
    ``BLACKHOLE_SYMLINK_TARGET_BASE`` **AND** belongs to the rescued
    release (a directory component of the target — the torrent-folder
    name blackhole.py builds targets around — exactly equals one of the
    anchor *candidates*), swap the path prefix to the alt debrid's base
    and replace the symlink atomically via ``os.symlink + os.replace``.

    *candidates* is the anchor set from ``_release_anchor_candidates``;
    when ``None`` it is derived from *filename* alone (legacy direct
    callers).  When *alt_client*/*alt_torrent_id* are given, the alt
    debrid's REAL on-disk folder is derived from its torrent info and
    substituted for the matched component — the alt provider may
    sanitize the folder name (TB mylist gotcha), so a same-name swap
    can produce a dangling link even though the content is there.

    The release-anchor filter is what keeps rescue scoped to ONE torrent:
    without it, the walker would rewrite every RD symlink to TB on the
    first rescue, silently breaking the entire library since the alt
    debrid only has THIS torrent's content cached.
    """
    from utils.debrid_routing import symlink_target_base_for_debrid

    src_base = (symlink_target_base_for_debrid(src_debrid) or '').rstrip('/')
    dst_base = (symlink_target_base_for_debrid(dst_debrid) or '').rstrip('/')
    if not src_base or not dst_base:
        logger.debug(
            f"[debrid_health] rescue retarget skipped — src_base={src_base!r}, "
            f"dst_base={dst_base!r}"
        )
        return 0

    if candidates is None:
        candidates = _release_anchor_candidates(filename)
    if not candidates:
        logger.warning(
            f"[debrid_health] rescue retarget skipped — no release anchor "
            f"derivable for hash {torrent_hash[:8]}…"
        )
        return 0

    # Real on-disk folder on the alt debrid, when derivable.  TB's
    # mylist ``name`` is a sanitized display string — the actual mount
    # folder is the first component of ``files[].name``.  ``None``
    # falls back to the same-name swap (correct whenever the alt
    # provider preserves the torrent folder name).
    alt_folder = None
    if alt_client is not None and alt_torrent_id:
        try:
            info_fn = getattr(alt_client, 'torrent_info', None)
            alt_info = info_fn(alt_torrent_id) if callable(info_fn) else None
            if isinstance(alt_info, dict):
                files = alt_info.get('files')
                if isinstance(files, list) and files and isinstance(files[0], dict):
                    first = files[0].get('name')
                    if isinstance(first, str) and '/' in first.strip('/'):
                        alt_folder = first.strip('/').split('/')[0]
        except Exception as e:
            logger.debug(
                f"[debrid_health] alt torrent_info fetch for folder remap "
                f"failed: {type(e).__name__}"
            )

    library_roots = [
        r for r in (
            os.environ.get('BLACKHOLE_LOCAL_LIBRARY_TV'),
            os.environ.get('BLACKHOLE_LOCAL_LIBRARY_MOVIES'),
        ) if r and os.path.isdir(r)
    ]
    if not library_roots:
        return 0

    src_prefix = src_base + '/'
    count = 0
    for root in library_roots:
        for dirpath, _dirs, files in os.walk(root, followlinks=False):
            for name in files:
                path = os.path.join(dirpath, name)
                if not os.path.islink(path):
                    continue
                try:
                    current_target = os.readlink(path)
                except OSError:
                    continue
                if not current_target.startswith(src_prefix):
                    continue
                # Per-release filter — only rewrite symlinks that
                # belong to THIS rescued torrent: a DIRECTORY component
                # of the target tail must exactly equal an anchor
                # candidate (the leaf is a file, never the torrent
                # folder).  Symlinks for other torrents stay untouched;
                # the alt debrid doesn't have their content cached.
                parts = current_target[len(src_prefix):].split('/')
                matched_idx = next(
                    (i for i, p in enumerate(parts[:-1]) if p in candidates),
                    None,
                )
                if matched_idx is None:
                    continue
                if alt_folder and alt_folder != parts[matched_idx]:
                    parts = (parts[:matched_idx] + [alt_folder]
                             + parts[matched_idx + 1:])
                new_target = dst_base + '/' + '/'.join(parts)
                try:
                    # Atomic-replace: write the new symlink at a temp
                    # path in the same directory, then ``os.replace``
                    # it over the old one.  No window where the symlink
                    # is missing — important because Plex might be
                    # mid-stat at this exact moment.
                    tmp = path + '.rescue-tmp'
                    try:
                        os.remove(tmp)
                    except FileNotFoundError:
                        pass
                    os.symlink(new_target, tmp)
                    os.replace(tmp, path)
                    count += 1
                    logger.info(
                        f"[debrid_health] rescue retargeted: {path} "
                        f"→ {new_target}"
                    )
                except OSError as e:
                    logger.warning(
                        f"[debrid_health] rescue retarget failed for {path}: "
                        f"{e}"
                    )
                    # Best-effort: clean up the temp if it survives a partial
                    # failure so the next rescue isn't blocked by orphan files.
                    try:
                        os.remove(path + '.rescue-tmp')
                    except FileNotFoundError:
                        pass

    return count


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
    # ``basename`` strips any path component an uploader may have packed
    # into the RD ``filename`` field (e.g. ``Show/S01/ep.mkv``) — without
    # it the parsed release name would carry slashes and could fuzzy-
    # match the wrong series in Sonarr, which under ``force_episodes=True``
    # would queue searches for the wrong show.
    # force_episodes=True because Zurg's WebDAV listing keeps the just-
    # deleted file visible for ~15-30 s, so Sonarr's ``hasFile`` is still
    # True at this instant — without the override, every TV episode in
    # the release would be skipped (see 2026-05-24 live sweep regression).
    release_name = os.path.splitext(os.path.basename(filename))[0] if filename else ''
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


# Re-entry guard for run_sweep.  The task scheduler already serializes
# scheduled + manual (run_now) invocations via its per-task `running`
# flag, but the `_stop_event.clear()` at sweep start would unmuzzle a
# stopping sweep if any future caller bypassed the scheduler — so the
# guard lives here with the hazard rather than relying on every caller.
_sweep_guard = threading.Lock()


def run_sweep():
    """Probe up to ``_MAX_PER_SWEEP`` Real-Debrid torrents and persist state.

    Returns a result dict compatible with the scheduler's expectations
    (``status``, ``message``, optional ``items``). The sweep is a no-op
    when ``DEBRID_HEALTH_ENABLED`` is false or no RD client is configured.
    Never re-entered: a second concurrent call returns immediately.

    Rate-limited to ``_RATE_LIMIT_PER_MIN`` probes/minute with an
    incremental persist every ``_PERSIST_EVERY`` probes so an
    interrupted run doesn't lose all work.
    """
    if not _sweep_guard.acquire(blocking=False):
        logger.info("[debrid_health] sweep already running, skipping")
        return {'status': 'success', 'message': 'sweep already running', 'items': 0}
    try:
        return _run_sweep()
    finally:
        _sweep_guard.release()


def _run_sweep():
    if not _enabled():
        logger.debug("[debrid_health] disabled, skipping sweep")
        return {'status': 'success', 'message': 'disabled', 'items': 0}

    # Clear any stale stop signal from a prior aborted run so this
    # invocation isn't muzzled before it starts.
    _stop_event.clear()

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

    # Prune entries for torrents no longer on RD. Remediation deletes
    # the torrent but the state entry lived on forever (the probe loop
    # only iterates list_torrents(), so a deleted hash is never
    # re-visited) — deleted-while-blocked ghosts inflated the
    # dashboard's blocked count and get_blocked_hashes() indefinitely.
    # Only prune from a provably-complete list: an empty response can't
    # be distinguished from a soft failure (and a truly empty account
    # has nothing worth pruning), and a response at the client's page
    # limit may be truncated — treating page-2 torrents as deleted
    # would mass-wipe valid state.
    if torrents and len(torrents) < RD_LIST_LIMIT:
        current_hashes = {
            (t.get('hash') or '').upper()
            for t in torrents if isinstance(t, dict)
        }
        with _lock:
            stale = [h for h in state['probed'] if h not in current_hashes]
            for h in stale:
                del state['probed'][h]
            if stale:
                _save_state(state)
        if stale:
            logger.info(
                f"[debrid_health] pruned {len(stale)} state entries for "
                f"torrents no longer on RD"
            )

    now = time.time()
    counts = {'probed': 0, 'healthy': 0, 'blocked': 0,
              'unknown': 0, 'skipped': 0, 'remediated': 0, 'rescued': 0}
    remediate_on = _auto_remediate_enabled()
    rescue_on = _cross_rescue_enabled()

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

    # Reasons we've already WARNed about in this sweep — keeps the
    # unhandled-reason logger from spamming when an RD response-shape
    # drift hits N torrents in the same sweep.  Per-sweep scope (not
    # module-level) so a future sweep can re-surface the same drift if
    # nothing's been fixed.
    unhandled_reasons_seen = set()

    for i, (tid, torrent_hash, filename) in enumerate(capped):
        if _stop_event.is_set():
            logger.info("[debrid_health] stop signal received — aborting sweep early")
            break
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

        # Only act on confirmed filter-blocks ('infringing_file', the
        # RD May-2026 keyword filter return).  HTTP 404 from
        # /unrestrict/link is also mapped to status='blocked' with
        # reason='not_found' — a transient CDN miss, a file being
        # re-processed by RD, or a stale link from /torrents/info.
        # Triggering rescue or destructive remediation on a transient
        # 404 would mass-delete healthy torrents and pollute TB with
        # speculative adds for files that may reappear on RD a sweep
        # later.  Re-probe on next sweep; don't touch state.
        #
        # Reason-predicate lives in ``utils.debrid_routing`` so the
        # filter-block vocabulary is shared with the blackhole add-time
        # rescue gate (plan 41 phase A) — a future RD return-string
        # change updates both sites at once.
        from utils.debrid_routing import is_filter_block_reason
        is_filter_block = (
            status == 'blocked'
            and is_filter_block_reason(result.get('reason'))
        )
        # Fail-closed defense: if RD ever ships a new blocked-reason
        # (e.g. 'dmca_takedown', 'regional_restriction'), the gate
        # above won't recognise it and no action will run.  Counts
        # accumulate silently on the dashboard.  Surface drift via a
        # WARN log so an operator notices before the counts get large.
        # Throttled per-reason-per-sweep so a 1000-torrent sweep with
        # the same new reason logs once, not 1000 times.
        if status == 'blocked' and not is_filter_block:
            reason = result.get('reason') or '<missing>'
            if reason != 'not_found' and reason not in unhandled_reasons_seen:
                # 'not_found' is the documented transient (HTTP 404);
                # any other unknown reason is RD response-shape drift.
                logger.warning(
                    f"[debrid_health] blocked torrent {tid} has "
                    f"unhandled reason={reason!r} — no action taken. "
                    f"If RD's response shape has changed, update "
                    f"probe_file + is_filter_block gate."
                )
                unhandled_reasons_seen.add(reason)

        # Plan 39 phase 3: cross-debrid rescue BEFORE the existing
        # remediation pipeline.  When RD filter-blocks a torrent and TB
        # has it cached, re-host on TB and retarget arr-library
        # symlinks — no blocklist, no RD delete, no arr re-search.  On
        # any rescue failure (TB not cached / add failed / never ready),
        # fall through to the existing remediate path.
        if is_filter_block and rescue_on:
            rescue = _attempt_cross_rescue(torrent_hash, filename,
                                           source_debrid='realdebrid',
                                           src_client=client,
                                           src_torrent_id=tid)
            if rescue.get('rescued'):
                entry['rescued'] = True
                entry['rescue_meta'] = {
                    'to': rescue.get('to'),
                    'tb_torrent_id': rescue.get('tb_torrent_id'),
                    'retargeted': rescue.get('retargeted', 0),
                    'outcome': rescue.get('outcome'),
                }
                counts['rescued'] += 1
                # History event — replaces the debrid_filtered event the
                # remediate path would have emitted.  Same filename /
                # source so the UI groups them correctly; distinct cause
                # so the user sees the recovery story rather than a
                # delete + re-search loop.
                try:
                    from utils import history as _history
                    _history.log_event(
                        type='debrid',
                        title=filename or tid,
                        source='debrid_health',
                        detail='Filter-blocked on RD — rescued via TorBox',
                        meta={
                            'cause': _history.CAUSE_DEBRID_RESCUED,
                            'rescue_stage': 'sweep',
                            'from': 'realdebrid',
                            'to': rescue.get('to'),
                            'info_hash': torrent_hash,
                            'torrent_id': tid,
                            'tb_torrent_id': rescue.get('tb_torrent_id'),
                            'retargeted': rescue.get('retargeted', 0),
                            'rescue_outcome': rescue.get('outcome'),
                            'reason': result.get('reason') or 'infringing_file',
                            'http': result.get('http'),
                        },
                    )
                except Exception as e:
                    logger.debug(f"[debrid_health] rescue history log failed: {e}")
                try:
                    from utils.notifications import notify as _notify_fn
                    _notify_fn(
                        'debrid_rescued',
                        f'Debrid rescue: {filename[:60]}',
                        f'RD filter-blocked, rescued via TorBox '
                        f'({rescue.get("retargeted", 0)} symlink(s) retargeted)',
                    )
                except Exception as e:
                    logger.debug(f"[debrid_health] rescue notify failed: {e}")
                # Update probe-cache entry so the rescued torrent doesn't
                # immediately re-trigger on the next sweep.  Treat as
                # 'unknown' rather than 'healthy' — the RD-side block is
                # still real; we just have an alt source for the file.
                entry['status'] = 'unknown'
                entry['reason'] = 'rescued_via_alt_debrid'
                with _lock:
                    state['probed'][torrent_hash] = entry
                # Skip the rest of the per-torrent loop — rescued, no
                # further action needed.  No sleep between rescue and the
                # next eligible iteration because rescue is rate-limited
                # by add_magnet's own latency.
                continue

        # Auto-remediate the blocked entry if enabled and under the
        # per-sweep cap. Cap exists separately from _MAX_PER_SWEEP so
        # a first-run enable of AUTO_REMEDIATE on a large library can't
        # mass-delete in one shot — remaining blocked entries stay
        # flagged and remediate on subsequent sweeps.  Gated on
        # ``is_filter_block`` (NOT plain ``status == 'blocked'``) so
        # transient 404 'not_found' responses don't trigger destructive
        # blocklist + delete + arr re-search.
        if (is_filter_block
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
        # ``_stop_event.wait`` (instead of plain ``time.sleep``) lets a
        # SIGTERM mid-sweep interrupt the rate-limit pause cleanly.
        if i < len(capped) - 1:
            if _stop_event.wait(sleep_seconds):
                logger.info("[debrid_health] stop signal received during rate-limit pause — aborting")
                break

    with _lock:
        _save_state(state)

    msg_parts = [
        f"probed {counts['probed']}",
        f"healthy {counts['healthy']}",
        f"blocked {counts['blocked']}",
        f"unknown {counts['unknown']}",
        f"skipped {counts['skipped']}",
    ]
    if rescue_on and counts['rescued']:
        msg_parts.append(f"rescued {counts['rescued']}")
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
    _stop_event.clear()
