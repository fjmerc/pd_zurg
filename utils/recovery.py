"""Media recovery tracking — quantitative '% recovered' time series.

Records a daily snapshot of how much of the user's media is currently
playable via the debrid mount versus on-disk-only (local fallback) versus
not-yet-acquired (wanted).  Built for the filter-recovery-era TB-viability
experiment: it answers "is TorBox actually restoring my library?" with
longitudinal data instead of a gut feeling.

Two framings live side by side in each snapshot:

  - **Library health** (``recovery`` block): the trustworthy headline.
    ``pct_debrid`` = playable-on-debrid units / total wanted units.
  - **Filter-gate signals** (``filter_gate`` block): the decision-relevant
    secondary view — daily counts of RD filter-blocks, alt-rescues, and the
    currently-blocked hash population.  Its denominator (everything RD ever
    broke) is fuzzy and partly bounded by history retention, so we record
    the raw daily deltas and leave ratio-building to the consumer.

The unit of measurement is a *media unit*: each movie counts as 1 and each
episode counts as 1, so a half-recovered show is reflected honestly rather
than counting whole shows.

Persistence is a single JSON file (``/config/recovery_snapshots.json``)
holding a date-keyed list, upserted once per UTC calendar day regardless of
scan frequency, capped at ``RECOVERY_SNAPSHOT_RETENTION_DAYS`` (default 365).
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
_retention_days = 365

SCHEMA_VERSION = 1


def init(config_dir='/config'):
    """Initialize the recovery module. Call once at startup."""
    global _file_path, _retention_days
    _file_path = os.path.join(config_dir, 'recovery_snapshots.json')
    try:
        _retention_days = max(1, int(os.environ.get('RECOVERY_SNAPSHOT_RETENTION_DAYS') or 365))
    except (ValueError, TypeError):
        _retention_days = 365
        logger.warning("[recovery] Invalid RECOVERY_SNAPSHOT_RETENTION_DAYS, using default 365")
    logger.info(f"[recovery] Initialized — {_file_path} (retention: {_retention_days} days)")


def _pct(num, denom):
    """Percentage rounded to one decimal, 0.0 when the denominator is 0."""
    if not denom:
        return 0.0
    return round(num / denom * 100, 1)


def _count_wanted(data):
    """Count not-yet-acquired but *releasable* media units.

    The denominator should reflect media you could actually have today, not
    announced/unaired future content — otherwise the recovery percentage is
    diluted by titles that don't exist yet.

    Wanted movies are the Radarr ghost entries (``missing=True`` /
    ``source='wanted'``); we skip any stamped ``is_available=False`` (Radarr
    hasn't reached the movie's minimum-availability date). Entries without
    the flag (legacy snapshots, non-Radarr libraries) count as before.

    Wanted episodes come from the pre-computed ``missing_episodes`` per show.
    Where Sonarr matched the show, that count is already aired-monitored-only
    (``_apply_sonarr_monitored_filter`` rebases it on Sonarr's aired
    ``episodeCount``), so unaired episodes are already excluded here. Shows
    that fell back to the TMDB total-episode count can still include a few
    unaired episodes — an accepted residual for unmatched shows.

    Fully-absent monitored series (zero episodes on disk) are injected as
    ghost shows by ``_apply_sonarr_wanted_shows`` carrying the same
    aired-monitored ``missing_episodes``, so they're counted here too — the
    TV mirror of the Radarr ghost-movie path. Without that injection a show
    you've downloaded nothing of would never reach ``data['shows']`` and the
    TV side of the denominator would read low.
    """
    wanted = 0
    for movie in data.get('movies', []) or []:
        if movie.get('missing') or movie.get('source') == 'wanted':
            if movie.get('is_available') is False:
                continue
            wanted += 1
    for show in data.get('shows', []) or []:
        me = show.get('missing_episodes')
        if isinstance(me, int) and me > 0:
            wanted += me
    return wanted


def _filter_gate_signals():
    """Pull filter-gate deltas from the debrid health reconciler.

    Returns ``None`` when RD isn't configured or the health module is
    unavailable — the caller records that as "no filter-gate data" rather
    than a misleading zero.
    """
    try:
        from utils.debrid_health import get_summary
    except ImportError:
        return None
    try:
        summary = get_summary()
    except Exception as e:
        # Don't let a debrid_health regression sink the whole snapshot —
        # the library-health framing doesn't depend on these signals. Log
        # at warning (not debug) so a real schema break is visible rather
        # than masquerading as "RD not configured".
        logger.warning(f"[recovery] filter-gate signal fetch failed: {e}")
        return None

    if not isinstance(summary, dict) or not summary.get('rd_configured'):
        return None

    counts = summary.get('counts') or {}
    return {
        'blocked': counts.get('blocked', 0),
        'filtered_24h': summary.get('remediated_24h', 0),
        'rescued_24h': summary.get('rescued_24h', 0),
    }


def compute_snapshot(data, now=None):
    """Build a recovery snapshot dict from a library scan ``data`` payload.

    Pure computation — does not touch disk.  ``now`` is injectable for tests.
    """
    from utils.library import compute_library_stats

    now = now or datetime.now(timezone.utc)
    stats = compute_library_stats(data)

    movies = stats['movies']['by_source']
    episodes = stats['shows']['episodes']['by_source']

    movies_debrid = movies.get('debrid', 0) + movies.get('both', 0)
    movies_local = movies.get('local', 0)
    eps_debrid = episodes.get('debrid', 0) + episodes.get('both', 0)
    eps_local = episodes.get('local', 0)

    available_debrid = movies_debrid + eps_debrid
    available_local = movies_local + eps_local
    on_disk = available_debrid + available_local
    wanted = _count_wanted(data)
    total = on_disk + wanted

    return {
        'date': now.strftime('%Y-%m-%d'),
        'ts': now.isoformat(timespec='seconds'),
        'units': {
            'movies': {
                'total': stats['movies']['total'],
                'debrid': movies.get('debrid', 0),
                'local': movies.get('local', 0),
                'both': movies.get('both', 0),
            },
            'episodes': {
                'total': stats['shows']['episodes']['total'],
                'debrid': episodes.get('debrid', 0),
                'local': episodes.get('local', 0),
                'both': episodes.get('both', 0),
            },
        },
        'recovery': {
            'available_debrid': available_debrid,
            'available_local': available_local,
            'on_disk': on_disk,
            'wanted': wanted,
            'total': total,
            'pct_debrid': _pct(available_debrid, total),
            'pct_on_disk': _pct(on_disk, total),
        },
        'filter_gate': _filter_gate_signals(),
    }


def _read_file():
    """Load the snapshots list from disk, tolerating a missing/corrupt file."""
    if _file_path is None or not os.path.isfile(_file_path):
        return []
    try:
        with open(_file_path, encoding='utf-8') as f:
            payload = json.load(f)
    except (OSError, ValueError) as e:
        logger.warning(f"[recovery] Could not read snapshots ({e}); starting fresh")
        return []
    if isinstance(payload, dict):
        snaps = payload.get('snapshots', [])
        return snaps if isinstance(snaps, list) else []
    return payload if isinstance(payload, list) else []


def record_snapshot(data, now=None):
    """Compute today's snapshot and upsert it into the persisted series.

    Upsert is keyed by UTC calendar day: a second scan on the same day
    overwrites that day's entry rather than appending, so the series stays
    at one point per day regardless of how often the scanner runs.  The
    series is trimmed to the retention window and written atomically.

    Best-effort — returns the snapshot on success, ``None`` on any failure
    (callers must never let snapshotting break the scan).

    Scans whose arr enrichment was degraded (``data['arr_degraded']``
    non-empty — a configured Sonarr/Radarr bulk-list fetch failed) are
    refused: a Sonarr failure inflates ``wanted`` by falling back to
    TMDB-only missing math, a Radarr/ghost failure deflates it by
    skipping injection.  Either way the daily point would be fiction.
    Better a gap (or yesterday's honest value via the per-day upsert)
    than a poisoned metric; the next healthy scan records the day.
    """
    if _file_path is None:
        return None
    degraded = data.get('arr_degraded')
    if degraded:
        logger.warning(
            f"[recovery] Skipping snapshot — arr enrichment degraded this "
            f"scan ({', '.join(map(str, degraded))}); wanted counts "
            f"unreliable. The next healthy scan will record today's point.")
        return None
    try:
        snapshot = compute_snapshot(data, now=now)
    except Exception as e:
        logger.error(f"[recovery] Failed to compute snapshot: {e}")
        return None

    with _lock:
        snaps = _read_file()
        snaps = [s for s in snaps if s.get('date') != snapshot['date']]
        snaps.append(snapshot)
        snaps.sort(key=lambda s: s.get('date', ''))
        if len(snaps) > _retention_days:
            snaps = snaps[-_retention_days:]
        try:
            with atomic_write(_file_path) as f:
                json.dump({'version': SCHEMA_VERSION, 'snapshots': snaps}, f,
                          separators=(',', ':'))
        except OSError as e:
            logger.error(f"[recovery] Failed to write snapshots: {e}")
            return None

    return snapshot


def restore_bytes(data):
    """Replace the on-disk snapshot series atomically (backup restore).

    Holding ``_lock`` across the write guarantees an in-flight
    ``record_snapshot`` read-modify-write can't persist pre-restore
    content over the restored file.
    """
    path = _file_path or '/config/recovery_snapshots.json'
    with _lock:
        with atomic_write(path, mode='wb') as f:
            f.write(data)


def load_snapshots(limit=None):
    """Return the recorded snapshots oldest-first, optionally last *limit*.

    No lock: the file is only ever published via ``atomic_write``'s
    ``os.replace``, so a concurrent read sees either the whole old file or
    the whole new one — never a torn write. Skipping the lock keeps a
    ``GET /api/recovery`` on the HTTP thread from blocking behind a scan
    write on the scheduler thread.
    """
    snaps = _read_file()
    snaps.sort(key=lambda s: s.get('date', ''))
    if limit and limit > 0:
        return snaps[-limit:]
    return snaps


def get_latest():
    """Return the most recent snapshot, or ``None`` if none recorded yet."""
    snaps = load_snapshots()
    return snaps[-1] if snaps else None
