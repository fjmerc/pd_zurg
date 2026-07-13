"""Stuck-content aggregator — the data layer behind ``/api/stuck``.

Joins the retry/failure state that already exists across four stores into
title-level records an operator can act on:

* **in-flight** — ``pending_monitors.json`` entries older than
  ``PENDING_WARNING_HOURS`` (a grab handed to a debrid that never
  completed the monitor loop);
* **wanted** — library "Wanted" ghosts the recovery legs have memoized
  against (Torrentio empty, RD probe missed, TB cooled down), whose
  give-up caps (``fg:``/``tbalt:`` attempt-ledger keys) are exhausted, or
  which the recovery legs have terminally given up on (``wantedblock:``
  strike — filter-blocked on RD and uncached on TB across enough passes);
* **history** — titles whose recent event stream is an unbroken failure
  streak (N+ failure-cause events with no progress-cause event after
  them, spanning at least a day).

History streaks are then reconciled against the scanner's merged library:
a streak-only record is dropped when the library shows the title now
satisfied (a movie on disk, or a show with zero missing episodes), since
the event log alone can't see a title that was filled by a path that
logged no progress-cause event.

Everything here is read-only against the source stores; the only mutation
this module owns is the 7-day operator dismissal, persisted as
``stuckdismiss:<key>`` attempt-ledger entries so no new store is needed.

Results are cached for ``_CACHE_TTL`` seconds — the joins walk the full
history retention window (30 days) and must not run per page-poll.
"""

import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone

from utils.logger import get_logger

logger = get_logger()

_CACHE_TTL = 60
_cache_lock = threading.Lock()
_cache_result = None
_cache_time = 0.0

# A history failure streak qualifies as "stuck" once it has this many
# failure events with no progress event after them, and the streak has
# been running at least this long (young streaks are normal retry churn).
_STREAK_MIN_EVENTS = 3
_STREAK_MIN_AGE_SECONDS = 24 * 3600

_HISTORY_WINDOW_DAYS = 30

DISMISS_TTL_SECONDS = 7 * 24 * 3600
_DISMISS_PREFIX = 'stuckdismiss:'

# Cause slugs that mean "this attempt failed / went nowhere".
_FAILURE_CAUSES = frozenset((
    'debrid_add_failed', 'symlink_create_failed', 'disc_rip_rejected',
    'terminal_error', 'uncached_timeout', 'uncached_rejected',
    'incomplete_release', 'alts_exhausted', 'blocklisted_hash',
    'debrid_unavailable_marked', 'debrid_filtered', 'wanted_rd_uncached',
    'wanted_filter_giveup',
))

# Cause slugs that mean "the title made real progress" — they reset a
# failure streak.  Grab submission counts: a fresh grab in flight is the
# pipeline actively working the title, not a stall.
_PROGRESS_CAUSES = frozenset((
    'blackhole_new_import', 'blackhole_cache_hit',
    'blackhole_grab_submitted', 'blackhole_mount_handoff',
    'library_new_import', 'library_upgrade_replaced', 'compromise_grab',
    'debrid_add_via_search', 'wanted_tb_recovered', 'wanted_rd_recovered',
    'debrid_rescued', 'tb_cached_alt_grabbed', 'routing_repaired',
))

# Human-readable labels for the reason chips the UI renders.
REASON_LABELS = {
    'no_results': 'Torrentio has no releases',
    'rd_miss': 'RD probe uncached/blocked',
    'tb_cooldown': 'TB probe cooled down',
    'grab_capped': 'Force-grab give-up cap hit',
    'alt_capped': 'TB-alt give-up cap hit',
    'filter_giveup': 'Filter-blocked everywhere — recovery gave up',
    'failure_streak': 'Repeated failures, no progress',
    'inflight_stale': 'Grab pending too long',
    'compromised': 'Compromise-quality grab',
    'filter_blocked': 'RD keyword-filter blocked',
    'blocklisted': 'On local blocklist',
}


def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def _pending_monitor_files():
    """Candidate ``pending_monitors.json`` paths, mirroring the watcher's
    completed-dir-when-symlinking / watch-dir-otherwise placement.  Both
    are returned (dedup'd) so a mode flip doesn't hide the old file."""
    paths = []
    completed = (os.getenv('BLACKHOLE_COMPLETED_DIR') or '').strip()
    watch = (os.getenv('BLACKHOLE_DIR') or '').strip()
    symlinking = os.environ.get('BLACKHOLE_SYMLINK_ENABLED', '').lower() == 'true'
    ordered = [completed, watch] if symlinking else [watch, completed]
    for d in ordered:
        if d:
            p = os.path.join(d, 'pending_monitors.json')
            if p not in paths:
                paths.append(p)
    return paths


def _load_pending_entries():
    entries = []
    seen = set()
    for path in _pending_monitor_files():
        if not os.path.isfile(path):
            continue
        try:
            with open(path, encoding='utf-8') as f:
                data = json.load(f)
        except (OSError, ValueError):
            continue
        if not isinstance(data, list):
            continue
        for e in data:
            if not isinstance(e, dict):
                continue
            tid = e.get('torrent_id')
            if tid is not None:
                # Only dedup on real ids — collapsing all id-less entries
                # onto a shared None key would drop legitimate ones.
                if tid in seen:
                    continue
                seen.add(tid)
            entries.append(e)
    return entries


def _pending_stuck_threshold_seconds():
    try:
        hours = int(os.environ.get('PENDING_WARNING_HOURS', '24'))
    except (ValueError, TypeError):
        hours = 24
    if hours <= 0:
        hours = 24  # warnings disabled ≠ the surface should hide stalls
    return hours * 3600


def _norm_key(title):
    try:
        from utils.library import normalize_title
        return normalize_title(title or '') or (title or '').strip().lower()
    except Exception:
        return (title or '').strip().lower()


def _record(key, title, media_type):
    return {
        'key': key,
        'title': title,
        'media_type': media_type,
        'kinds': [],
        'reasons': [],
        'since': None,
        'attempts': 0,
        'imdb_id': None,
        'info_hash': None,
        'provider': None,
        'torrent_id': None,
        'blocklisted': False,
        'last_event': None,
    }


def _merge_since(rec, iso_ts):
    if iso_ts and (rec['since'] is None or iso_ts < rec['since']):
        rec['since'] = iso_ts


def _add_reason(rec, reason):
    if reason and reason not in rec['reasons']:
        rec['reasons'].append(reason)


def _collect_inflight(records):
    threshold = _pending_stuck_threshold_seconds()
    now = time.time()
    for entry in _load_pending_entries():
        try:
            age = now - float(entry.get('timestamp') or 0)
        except (TypeError, ValueError):
            continue
        if age < threshold:
            continue
        filename = str(entry.get('filename') or entry.get('torrent_id') or '?')
        key = 'inflight:' + _norm_key(filename)
        rec = records.get(key)
        if rec is None:
            rec = records[key] = _record(key, filename, 'release')
        rec['kinds'].append('inflight')
        _add_reason(rec, 'inflight_stale')
        rec['provider'] = entry.get('debrid') or entry.get('service')
        rec['torrent_id'] = entry.get('torrent_id')
        if entry.get('compromised'):
            _add_reason(rec, 'compromised')
        ts = entry.get('timestamp')
        if ts:
            _merge_since(rec, datetime.fromtimestamp(
                float(ts), tz=timezone.utc).isoformat(timespec='seconds'))


def _collect_wanted(records, ledger):
    try:
        from utils.library import get_scanner
        scanner = get_scanner()
    except Exception:
        scanner = None
    if scanner is None:
        return
    data = None
    try:
        data = scanner.peek_data()
    except Exception:
        pass
    if not data:
        return
    try:
        memos = scanner.wanted_recovery_snapshot()
    except Exception:
        memos = {'no_results': {}, 'rd_miss': {}, 'tb_cooldown': {}}

    try:
        fg_cap = int(os.environ.get('FORCE_GRAB_MAX_ATTEMPTS', '12'))
    except (ValueError, TypeError):
        fg_cap = 12
    try:
        alt_cap = int(os.environ.get('BLACKHOLE_TB_ALT_MAX_ATTEMPTS', '12'))
    except (ValueError, TypeError):
        alt_cap = 12
    try:
        from utils.library import WANTED_FILTER_GIVEUP_STRIKES as _giveup
    except Exception:
        _giveup = 3

    now_iso = _now_iso()

    def _memo_hit(memo, imdb):
        """Oldest age (seconds) of any memo entry for this imdb, or None."""
        best = None
        prefix = f"{imdb}:"
        for k, age in memo.items():
            if k == imdb or k.startswith(prefix):
                if best is None or age > best:
                    best = age
        return best

    ghosts = []
    for m in data.get('movies') or []:
        if isinstance(m, dict) and m.get('source') == 'wanted':
            ghosts.append(('movie', m))
    for s in data.get('shows') or []:
        if isinstance(s, dict) and s.get('source') == 'wanted':
            ghosts.append(('show', s))

    for media_type, item in ghosts:
        title = item.get('title') or ''
        imdb = item.get('imdb_id')
        norm = _norm_key(title)
        reasons = []
        oldest_age = None
        if imdb:
            for memo_name, reason in (('no_results', 'no_results'),
                                      ('rd_miss', 'rd_miss'),
                                      ('tb_cooldown', 'tb_cooldown')):
                age = _memo_hit(memos.get(memo_name) or {}, imdb)
                if age is not None:
                    reasons.append(reason)
                    if oldest_age is None or age > oldest_age:
                        oldest_age = age
        attempts = 0
        fg_prefix = f"fg:{norm}"
        # wantedblock strikes are per-probe: ``wantedblock:<imdb>`` for movies
        # but ``wantedblock:<imdb>:<season>:<episode>`` for shows, so match the
        # bare key OR any episode-scoped suffix under it.
        wb_prefix = f"wantedblock:{imdb}" if imdb else None
        for k, entry in ledger.items():
            count = int(entry.get('count', 0)) if isinstance(entry, dict) else 0
            if k == fg_prefix or k.startswith(fg_prefix + ':'):
                attempts = max(attempts, count)
                if count >= fg_cap:
                    reasons.append('grab_capped')
            elif imdb and k.startswith(f"tbalt:{imdb}:"):
                attempts = max(attempts, count)
                if count >= alt_cap:
                    reasons.append('alt_capped')
            elif wb_prefix and (k == wb_prefix or k.startswith(wb_prefix + ':')):
                if count >= _giveup:
                    if 'filter_giveup' not in reasons:
                        reasons.append('filter_giveup')
                    attempts = max(attempts, count)
        if not reasons:
            continue  # merely wanted — the queue will get to it

        key = 'title:' + norm
        rec = records.get(key)
        if rec is None:
            rec = records[key] = _record(key, title, media_type)
        rec['kinds'].append('wanted')
        for r in reasons:
            _add_reason(rec, r)
        rec['imdb_id'] = imdb
        rec['attempts'] = max(rec['attempts'], attempts)
        if oldest_age is not None:
            since = (datetime.now(timezone.utc)
                     - timedelta(seconds=oldest_age))
            _merge_since(rec, since.isoformat(timespec='seconds'))
        else:
            _merge_since(rec, now_iso)


def _collect_history(records):
    try:
        from utils import history
        start = (datetime.now(timezone.utc)
                 - timedelta(days=_HISTORY_WINDOW_DAYS)
                 ).isoformat(timespec='seconds')
        events = history.events_since(start)
    except Exception:
        return

    # Per-title failure streak: walk oldest→newest; a progress cause wipes
    # the streak, a failure cause extends it.
    streaks = {}  # norm -> {title, first_ts, count, last_event}
    for e in events:
        if not isinstance(e, dict):
            continue
        meta = e.get('meta') or {}
        cause = meta.get('cause')
        if cause not in _FAILURE_CAUSES and cause not in _PROGRESS_CAUSES:
            continue
        title = e.get('media_title') or e.get('title') or ''
        norm = _norm_key(title)
        if not norm:
            continue
        if cause in _PROGRESS_CAUSES:
            streaks.pop(norm, None)
            continue
        st = streaks.get(norm)
        if st is None:
            st = streaks[norm] = {'title': title, 'first_ts': e.get('ts'),
                                  'count': 0, 'last_event': None,
                                  'has_episode': False}
        st['count'] += 1
        st['last_event'] = e
        if e.get('episode'):
            st['has_episode'] = True

    now = datetime.now(timezone.utc)
    for norm, st in streaks.items():
        if st['count'] < _STREAK_MIN_EVENTS:
            continue
        try:
            first = datetime.fromisoformat(st['first_ts'])
            if first.tzinfo is None:
                first = first.replace(tzinfo=timezone.utc)
            if (now - first).total_seconds() < _STREAK_MIN_AGE_SECONDS:
                continue
        except (TypeError, ValueError):
            continue
        last = st['last_event'] or {}
        meta = last.get('meta') or {}
        key = 'title:' + norm
        rec = records.get(key)
        if rec is None:
            media_type = 'show' if st.get('has_episode') else 'movie'
            rec = records[key] = _record(key, st['title'], media_type)
        rec['kinds'].append('history')
        _add_reason(rec, 'failure_streak')
        rec['attempts'] = max(rec['attempts'], st['count'])
        rec['info_hash'] = meta.get('info_hash') or rec['info_hash']
        rec['provider'] = rec['provider'] or meta.get('service')
        rec['last_event'] = {
            'ts': last.get('ts'),
            'type': last.get('type'),
            'cause': meta.get('cause'),
            'detail': last.get('detail', ''),
            'meta': meta,
        }
        _merge_since(rec, st['first_ts'])


def _annotate(records):
    """Blocklist / filter-block flags on the joined records."""
    blocked_hashes = set()
    try:
        from utils import debrid_health
        blocked_hashes = {h.lower() for h in debrid_health.get_blocked_hashes()}
    except Exception:
        pass
    try:
        from utils import blocklist
        is_blocked = blocklist.is_blocked
        is_blocked_title = blocklist.is_blocked_title
    except Exception:
        is_blocked = is_blocked_title = None

    for rec in records.values():
        ih = (rec.get('info_hash') or '').lower()
        if ih and ih in blocked_hashes:
            _add_reason(rec, 'filter_blocked')
        try:
            if is_blocked is not None and ih and is_blocked(ih):
                rec['blocklisted'] = True
            elif is_blocked_title is not None and is_blocked_title(rec.get('title') or ''):
                rec['blocklisted'] = True
        except Exception:
            pass
        if rec['blocklisted']:
            _add_reason(rec, 'blocklisted')


def _reconcile_satisfied(records):
    """Drop history-only failure streaks the library now shows satisfied.

    ``_collect_history`` reconstructs streaks from the event log alone and
    has no view of current library state, so a title that was eventually
    filled by a path that logged no progress-cause event (a quality
    upgrade, a different release, an out-of-window grab) keeps showing its
    stale streak until it ages out of the 30-day window.  Cross-check
    against the scanner's merged library — the same ``peek_data`` snapshot
    ``_collect_wanted`` already reads — and drop a record whose *only*
    justification is a history streak when the library confirms that title
    satisfied (a movie present on disk; a show with zero missing episodes).

    History events carry no year or imdb_id (``history.log_event``), so the
    join is by normalized title, which is weak: ``normalize_title`` strips
    the trailing ``(YYYY)`` and years live in a separate field, so a
    remake/reboot collapses onto its original.  To keep the reconcile from
    ever *hiding* a genuinely-stuck title (the dangerous direction), a
    match must be unambiguous on the two axes the streak does carry:

    * **media_type** — a movie streak is only suppressed by a satisfied
      *movie*, a show streak only by a satisfied *show*.  (A ``Daredevil``
      release parsed as a movie can't be cleared by a satisfied show of
      the same name, and vice-versa.)  The streak's type is derived from
      *any* episode-bearing event in it (``_collect_history``), so a
      season-pack failure can't mistype a show as a movie.
    * **single identity** — a ``(norm, media_type)`` that maps to more than
      one library entry is ambiguous and never suppressed; only a lone,
      fully-satisfied entry with a *known* year clears a streak.  A
      ``year`` of ``None`` can't establish a distinct identity, so such an
      entry is never confirmable — a satisfied remake with an unknown year
      can't hide a stuck original of the same name.

    Records also backed by a live in-flight or wanted kind are never
    touched, and a show whose missing-episode count is unknown (``None``)
    is treated as unsatisfied so a genuine gap can never be hidden.  The
    one irreducible residual: a stuck title present *only* in the history
    log (e.g. a watchlist grab absent from any arr) that shares a name with
    a lone satisfied library entry of a different year — no available datum
    distinguishes them.
    """
    try:
        from utils.library import get_scanner
        scanner = get_scanner()
        data = scanner.peek_data() if scanner is not None else None
    except Exception:
        data = None
    if not data:
        return

    # (norm, media_type) -> list of (year, satisfied).  A streak is cleared
    # only when its identity maps to exactly ONE satisfied library entry
    # with a known year; any duplicate, ambiguity, or unknown year blocks
    # suppression so a genuinely-stuck title can never be hidden.
    ident = {}

    def _mark(norm, mtype, year, sat):
        if not norm:
            return
        ident.setdefault((norm, mtype), []).append((year, sat))

    for m in data.get('movies') or []:
        if isinstance(m, dict):
            _mark(_norm_key(m.get('title') or ''), 'movie',
                  m.get('year'), m.get('source') != 'wanted')
    for s in data.get('shows') or []:
        if not isinstance(s, dict):
            continue
        miss = s.get('missing_episodes')
        done = (s.get('source') != 'wanted'
                and isinstance(miss, int) and miss == 0)
        _mark(_norm_key(s.get('title') or ''), 'show', s.get('year'), done)

    confirmed = set()
    for k, entries in ident.items():
        if len(entries) != 1:
            continue
        year, sat = entries[0]
        if sat and year is not None:
            confirmed.add(k)
    if not confirmed:
        return

    suppressed = []
    for key, rec in list(records.items()):
        if set(rec.get('kinds') or ()) != {'history'}:
            continue
        norm = (key[len('title:'):] if key.startswith('title:')
                else _norm_key(rec.get('title') or ''))
        if (norm, rec.get('media_type')) in confirmed:
            suppressed.append(rec.get('title') or norm)
            records.pop(key, None)
    if suppressed:
        logger.info("[stuck] suppressed %d satisfied history streak(s): %s",
                    len(suppressed),
                    ', '.join(sorted(suppressed))[:200])


def _dismissed_keys(ledger):
    """Set of record keys whose dismissal is still inside its 7-day TTL."""
    now = time.time()
    out = set()
    for k, entry in ledger.items():
        if not k.startswith(_DISMISS_PREFIX) or not isinstance(entry, dict):
            continue
        ts = entry.get('last_ts') or entry.get('first_ts')
        try:
            epoch = datetime.fromisoformat(ts).timestamp() if ts else None
        except (ValueError, TypeError):
            epoch = None
        if epoch is not None and now - epoch < DISMISS_TTL_SECONDS:
            out.add(k[len(_DISMISS_PREFIX):])
    return out


def dismiss(key):
    """Snooze ``key`` off the stuck surface for DISMISS_TTL_SECONDS."""
    from utils import attempt_ledger
    attempt_ledger.bump(_DISMISS_PREFIX + key)
    invalidate_cache()


def clear_retry_state(key, title=None, imdb_id=None):
    """Reset the give-up state that suppresses retries for a stuck record.

    Clears the scanner's Wanted-recovery memos for ``imdb_id``, resets the
    ``fg:``/``tbalt:``/``wantedblock:`` attempt-ledger counters, and drops any dismissal —
    the next scan (and any operator-triggered arr search) gets a clean
    slate.  Returns a summary dict of what was cleared.
    """
    from utils import attempt_ledger
    cleared = {'memos': 0, 'ledger_keys': 0}
    if imdb_id:
        try:
            from utils.library import get_scanner
            scanner = get_scanner()
            if scanner is not None:
                cleared['memos'] = scanner.clear_wanted_memos(imdb_id)
        except Exception:
            logger.warning('[stuck] Could not clear scanner memos', exc_info=True)
    prefixes = []
    if title:
        norm = _norm_key(title)
        if norm:
            prefixes.append(f"fg:{norm}")
    if imdb_id:
        prefixes.append(f"tbalt:{imdb_id}:")
        prefixes.append(f"wantedblock:{imdb_id}")
    if prefixes:
        for k in attempt_ledger.snapshot():
            for p in prefixes:
                if k == p or k.startswith(p if p.endswith(':') else p + ':'):
                    attempt_ledger.reset(k)
                    cleared['ledger_keys'] += 1
                    break
    attempt_ledger.reset(_DISMISS_PREFIX + key)
    invalidate_cache()
    return cleared


def invalidate_cache():
    global _cache_result, _cache_time
    with _cache_lock:
        _cache_result = None
        _cache_time = 0.0


def collect(max_items=200, force=False):
    """Return the joined stuck-content payload (cached for _CACHE_TTL).

    The lock is held across the whole computation so concurrent uncached
    callers wait for one collection instead of stampeding the 30-day
    history walk in parallel.
    """
    global _cache_result, _cache_time
    with _cache_lock:
        if (not force and _cache_result is not None
                and time.monotonic() - _cache_time < _CACHE_TTL):
            return _cache_result

        from utils import attempt_ledger
        ledger = attempt_ledger.snapshot()

        records = {}
        for step in (lambda: _collect_inflight(records),
                     lambda: _collect_wanted(records, ledger),
                     lambda: _collect_history(records)):
            try:
                step()
            except Exception:
                logger.warning('[stuck] Collector step failed', exc_info=True)
        _annotate(records)
        try:
            _reconcile_satisfied(records)
        except Exception:
            logger.warning('[stuck] Satisfied-reconcile step failed',
                           exc_info=True)

        dismissed = _dismissed_keys(ledger)
        items = []
        dismissed_count = 0
        for key, rec in records.items():
            if key in dismissed:
                dismissed_count += 1
                continue
            rec['reason_labels'] = [REASON_LABELS.get(r, r)
                                    for r in rec['reasons']]
            items.append(rec)

        items.sort(key=lambda r: (-r['attempts'], r['since'] or '9999'))
        payload = {
            'generated_at': _now_iso(),
            'total': len(items),
            'dismissed': dismissed_count,
            'items': items[:max_items],
        }
        _cache_result = payload
        _cache_time = time.monotonic()
        return payload


__all__ = ['collect', 'dismiss', 'clear_retry_state', 'invalidate_cache',
           'REASON_LABELS', 'DISMISS_TTL_SECONDS']
