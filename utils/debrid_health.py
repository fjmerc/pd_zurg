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
    providers = []
    if rd_configured:
        providers.append(_provider_card('realdebrid'))
    if tb_configured:
        providers.append(_provider_card('torbox'))

    if not rd_configured:
        # Preserve the original empty response so the existing UI's
        # ``!s.rd_configured → hide`` check stays valid.  ``providers``
        # is exposed unconditionally so the dual-card view works even
        # when only TB is configured.
        return {'rd_configured': False, 'providers': providers}

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

    remediated_24h = 0
    rescued_24h = 0
    try:
        from datetime import datetime, timedelta, timezone
        from utils import history as _history
        start = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(timespec='seconds')
        # type='debrid' is the only event type the reconciler emits;
        # filter by cause client-side because history.query has no
        # native meta-filter parameter.
        result = _history.query(type='debrid', start=start, limit=500)
        for ev in result.get('events', []):
            meta = ev.get('meta') or {}
            cause = meta.get('cause')
            if cause == 'debrid_filtered':
                remediated_24h += 1
            elif cause == 'debrid_rescued':
                rescued_24h += 1
    except Exception as e:
        logger.debug(f"[debrid_health] summary history query failed: {e}")

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


def _provider_card(service):
    """Per-provider card data for the dashboard.

    Today only RD has a real probed-state set; TB's card reflects the
    rescue-side state (entries in the state file flagged
    ``rescued=True`` for the TB side) plus the configured signal.
    AD support is structurally present but reports an empty counts
    block until a probe path lands.
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
        # TB doesn't have a probed-state set yet — surface the rescue
        # cache count (entries written to RD state with rescued=True).
        rescued = 0
        last_probe = None
        state = _get_state()
        with _lock:
            for entry in state['probed'].values():
                if not isinstance(entry, dict):
                    continue
                if entry.get('rescued') is True:
                    rescued += 1
                    ts = entry.get('ts')
                    if isinstance(ts, (int, float)):
                        if last_probe is None or ts > last_probe:
                            last_probe = ts
        return {
            'service': service, 'label': label, 'configured': True,
            'counts': {'rescued': rescued},
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


def _attempt_cross_rescue(torrent_hash, filename, source_debrid='realdebrid'):
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
    """
    # Today's only rescue direction is RD → TB.  AD → TB or TB → RD will
    # come later if the user enables those combinations; for now keep
    # the check explicit so future combinations land as code additions,
    # not silent fallthroughs.
    if source_debrid != 'realdebrid':
        return {'rescued': False, 'reason': 'unsupported_source'}

    alt = 'torbox'
    alt_key = os.environ.get('TORBOX_API_KEY')
    if not alt_key:
        return {'rescued': False, 'reason': 'no_alt_configured'}

    # Cache-check first — adding an uncached hash would start a TB
    # download we don't want.  Rescue is a HIT-CACHED operation.
    try:
        from utils.search import check_debrid_cache
        cache_map = check_debrid_cache(
            [torrent_hash.lower()], service=alt, api_key=alt_key,
        )
        cached = cache_map.get(torrent_hash.lower())
    except Exception as e:
        logger.warning(
            f"[debrid_health] rescue cache probe failed for "
            f"{torrent_hash[:8]}…: {type(e).__name__}"
        )
        return {'rescued': False, 'reason': 'cache_probe_error'}

    if cached is not True:
        return {'rescued': False, 'reason': 'not_cached_on_alt'}

    # Add to the alt debrid.  TB accepts a hash-only magnet via the
    # standard /torrents/createtorrent endpoint we already use for the
    # blackhole add path.
    alt_client, _svc = get_debrid_client(service=alt, api_key=alt_key)
    if alt_client is None or not alt_client.configured:
        return {'rescued': False, 'reason': 'no_alt_client'}

    try:
        alt_tid = alt_client.add_magnet(torrent_hash)
    except Exception as e:
        logger.warning(
            f"[debrid_health] rescue add_magnet failed for "
            f"{torrent_hash[:8]}…: {type(e).__name__}"
        )
        return {'rescued': False, 'reason': 'add_error'}
    if not alt_tid:
        return {'rescued': False, 'reason': 'add_failed'}

    # Honour a stop signal received between ``add_magnet`` and the poll
    # loop — don't burn an outbound API call we know we'll discard.
    # Clean up the in-flight TB add so we don't leave a stale entry.
    if _stop_event.is_set():
        try:
            alt_client.delete_torrent(alt_tid)
        except Exception:
            pass
        return {'rescued': False, 'reason': 'stop_requested',
                'tb_torrent_id': alt_tid}

    # Poll for ready.  TB caches resolve in seconds for hit-cached adds
    # — the ``_RESCUE_READY_TIMEOUT`` module constant is the upper bound
    # covering the slowest observed case during plan 39 smoke testing.
    # Uncached on alt (post-add) is treated as miss; we don't want to
    # leave a stale "downloading on TB" entry around if the cache probe
    # lied.  Tests monkeypatch the constants directly to avoid touching
    # ``time.time`` globally (which would break pytest's own timing).
    #
    # The interval sleep uses ``_stop_event.wait()`` (not ``time.sleep()``)
    # so a SIGTERM mid-rescue aborts within one poll interval rather than
    # stalling the scheduler's 15s join window.
    deadline = time.time() + _RESCUE_READY_TIMEOUT
    is_ready = False
    while time.time() < deadline:
        if _stop_event.is_set():
            break
        try:
            state_str = alt_client.torrent_status(alt_tid)
        except Exception:
            state_str = ''
        # Strip + lower in case the provider returns whitespace or
        # capital-cased status strings (TB docs are inconsistent across
        # endpoints; defensive normalisation).  Accept TB's full ready
        # set: ``cached`` (instant cache hit — the dominant rescue case
        # since cross-rescue is gated on TB-cached cache probe), plus
        # ``completed`` / ``uploading`` for full-BT-cycle torrents.
        # Imported from blackhole's TB_READY_STATES so both code paths
        # stay in lock-step — pre-fix this checked only 'completed' and
        # silently timed out on every cached-hit rescue.
        from utils.blackhole import TB_READY_STATES
        if (state_str or '').strip().lower() in TB_READY_STATES:
            is_ready = True
            break
        if _stop_event.wait(_RESCUE_POLL_INTERVAL):
            break

    if not is_ready:
        # The hash was reportedly cached on TB but the add didn't reach
        # a ready state (cached / completed / uploading) within budget.
        # Clean up the TB entry so a future rescue attempt can re-add cleanly.
        try:
            alt_client.delete_torrent(alt_tid)
        except Exception:
            pass
        return {'rescued': False, 'reason': 'never_ready',
                'tb_torrent_id': alt_tid}

    # Plex active-session guard — never swap a symlink that's mid-stream.
    # FUSE caches the open file descriptor so playback usually continues
    # from the original target, but new seeks fail after the rewrite.
    # Defer this rescue to the next sweep when Plex is actively playing
    # the file.  Same posture as duplicate_cleanup's read-only guard.
    release_name_guard = os.path.splitext(os.path.basename(filename or ''))[0]
    if release_name_guard and _plex_session_active_for_release(release_name_guard):
        logger.info(
            f"[debrid_health] rescue deferred — Plex is actively streaming "
            f"{release_name_guard!r}; will retry next sweep"
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
    retargeted = _retarget_symlinks_to_alt(torrent_hash, filename)

    return {
        'rescued': True,
        'to': alt,
        'tb_torrent_id': alt_tid,
        'retargeted': retargeted,
        'outcome': 'symlinks_retargeted' if retargeted else 'no_symlinks_found',
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


def _retarget_symlinks_to_alt(torrent_hash, filename, src_debrid='realdebrid', dst_debrid='torbox'):
    """Find symlinks in the arr libraries pointing at the source debrid's
    mount FOR THIS SPECIFIC RELEASE, and atomic-retarget each one to the
    equivalent path under the alt debrid's mount.

    Returns the number of symlinks retargeted.  Errors per-symlink are
    logged and counted as skipped — never raise; rescue is best-effort.

    Strategy: walk ``BLACKHOLE_LOCAL_LIBRARY_TV`` and
    ``BLACKHOLE_LOCAL_LIBRARY_MOVIES`` (the arr-side library roots), and
    for each symlink whose target points into the source debrid's
    ``BLACKHOLE_SYMLINK_TARGET_BASE`` **AND** belongs to the rescued
    release (matched by the ``/<release_name>/`` segment that
    blackhole.py uses as the torrent-folder name), swap the path prefix
    to the alt debrid's base and replace the symlink atomically via
    ``os.symlink + os.replace``.

    The release-name filter is what keeps rescue scoped to ONE torrent:
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

    # Release name = filename without media extension.  Matches how
    # blackhole.py names the torrent folder under the rclone mount and
    # therefore the ``/<release_name>/`` segment of every symlink target
    # for this torrent.  Without a non-empty release filter we'd
    # mass-retarget every RD symlink on a single rescue.
    release_name = os.path.splitext(os.path.basename(filename or ''))[0]
    if not release_name:
        logger.warning(
            f"[debrid_health] rescue retarget skipped — empty release name "
            f"for hash {torrent_hash[:8]}…"
        )
        return 0
    release_segment = '/' + release_name + '/'

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
                # belong to THIS rescued torrent (matched by the
                # ``/<release_name>/`` path segment).  Symlinks for
                # other RD torrents stay untouched; the alt debrid
                # doesn't have their content cached.
                if release_segment not in current_target:
                    continue
                new_target = dst_base + '/' + current_target[len(src_prefix):]
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
        is_filter_block = (
            status == 'blocked'
            and (result.get('reason') or '') == 'infringing_file'
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
                                           source_debrid='realdebrid')
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
