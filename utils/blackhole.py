"""Blackhole watch folder for .torrent and .magnet files.

Monitors a directory for torrent/magnet files, submits them to the
configured debrid service, and removes the file after processing.
Compatible with Sonarr/Radarr blackhole download client configuration.

When symlink mode is enabled, monitors submitted torrents until content
appears on the rclone mount, then creates symlinks in a completed
directory for Sonarr/Radarr to import.
"""

import hashlib
import json
import os
import re
import shutil
import tempfile
import time
import threading
import uuid
from datetime import datetime
import requests
from utils.file_utils import atomic_write
from utils.logger import get_logger
from utils import attempt_ledger

logger = get_logger()

try:
    from utils.notifications import notify as _notify
except ImportError:
    _notify = None

try:
    from utils import history as _history
except ImportError:
    _history = None

try:
    from utils import blocklist as _blocklist
except ImportError:
    _blocklist = None

from utils.api_metrics import tracked_request

_watcher = None

# Retry configuration for failed torrent submissions
RETRY_SCHEDULE = [300, 900, 3600]  # 5 min, 15 min, 1 hour
MAX_RETRIES = 3

# Max same-tier alternatives to probe against TorBox when recovering an
# uncached grab (_try_torbox_cached_alternative).  TorBox's cache probe is
# per-hash and synchronous on the serial blackhole thread, so this bounds
# worst-case added latency to roughly _TB_ALT_MAX_PROBES * _CACHE_PROBE_TIMEOUT.
# Probed candidates are pre-ranked by seeders, so the best releases are seen
# first even when a popular title returns far more streams than this.
_TB_ALT_MAX_PROBES = 12

# Suppress redundant cached-alternative recovery grabs for the same
# (imdb_id, season) within this window.  When a whole season is
# uncached-but-Wanted, the arr drops every episode's .magnet at once; without
# this guard each episode independently grabs a cached alternative and — before
# the first pack lands (~300s) and gets symlinked — siblings pick *different*
# season packs, grabbing 3+ full-season packs for one season and multiplying the
# rclone VFS download load into a TorBox 429 storm.  One cached pack recovers the
# whole season, so the first grab wins and the rest defer.  Sized to cover the
# grab→land→hourly-scan→symlink window; after content lands, the local-dedup
# check skips re-drops anyway, and a still-missing episode self-heals via the
# arr re-drop once this expires.
_TB_ALT_DEDUP_TTL = 21600  # 6 hours

# Plan 41 phase A — rescue-staging filename layout.  When the add-time
# cross-rescue stages ``file_path`` to ``.alt_pending/`` before the
# 60s wait_ready loop, it prefixes with ``.rescue-<uuid8>-`` so the
# unique-name collision with the alt-release path's own same-directory
# staging is impossible.  The recovery path on next startup
# (``_recover_alt_pending``) MUST strip the prefix before moving to
# ``failed/`` — otherwise the mangled name doesn't match what
# Sonarr/Radarr expects on the next retry cycle and the file silently
# rots.  The single-source-of-truth regex is below.
_RESCUE_STAGED_PREFIX_RE = re.compile(r'^\.rescue-[0-9a-f]{8}-')
# Cap the staged filename portion so the resulting basename
# (``.rescue-<8 hex>-<filename>`` = 17 byte overhead) stays under
# POSIX ``NAME_MAX`` (255 bytes) even for long multi-byte names from
# non-English trackers.  220 + 17 = 237 bytes worst-case.
_RESCUE_STAGED_FILENAME_MAX = 220


def _restore_rescue_basename(name):
    """Reverse the ``.rescue-<uuid8>-`` prefix added by the rescue staging.

    Returns the original filename (or *name* unchanged when no prefix
    is present).  Used by ``_recover_alt_pending`` on container restart
    so a rescue-orphan that survived an SIGKILL gets moved to
    ``failed/`` under its original name — Sonarr/Radarr's blackhole
    import recognises that name on the next retry cycle.
    """
    if not name:
        return name
    return _RESCUE_STAGED_PREFIX_RE.sub('', name, count=1)

# Per-provider rate-limit gating.  When a debrid API returns HTTP 429 or a
# body containing "rate limit", the corresponding provider is marked
# back-off-until ``now + _RATE_LIMIT_BACKOFF``.  Subsequent ``_add_to_*``
# calls for the SAME provider sleep until that window expires before
# hitting the API again — turns a self-DoS during a Sonarr search storm
# (e.g. MissingEpisodeSearch firing all S01 episodes of the same release
# group within the same second) into a single bounded wait per episode
# rather than dozens of immediate-fail-then-retry-in-5min loops that
# all hit the rate-limit again on retry.
#
# Cross-provider isolation: a 429 on RD does not throttle TB calls.
# That means cache_aware routing can still keep moving even when one
# debrid is under pressure.
_RATE_LIMIT_BACKOFF = 60  # seconds; one rate-limit token-bucket window
_rate_limit_lock = threading.Lock()
_rate_limit_until = {}    # provider -> unix timestamp until which adds wait

# TorBox-specific: the per-tier daily/monthly download quota can put the
# whole account into a ``cooldown_until`` window that is NOT a standard
# 429 — TB returns HTTP 400 + ``{"error":"DOWNLOAD_SERVER_ERROR",...}``
# on createtorrent while the cooldown is active.  ``_check_torbox_cooldown``
# fetches the cooldown timestamp from /v1/api/user/me and exposes it as
# "seconds until the cooldown expires", so a failed add can be converted
# into a precise rate-limit window via ``_mark_rate_limited('torbox', seconds=N)``.
# A short module-level cache keeps a retry storm from hammering /user/me.
_TB_COOLDOWN_CACHE_TTL = 30  # seconds — short enough to react to manual lift
_tb_cooldown_cache = {'checked_at': 0.0, 'seconds_until': 0.0}

# Coalesced root re-list.  Phase 2 of ``_process_torrent`` kicks rclone to
# re-list the mount root via RC ``vfs/refresh`` so a freshly-cached torrent
# surfaces without waiting for the dir-cache to expire.  On a FLAT TorBox
# mount that refresh is a full-root PROPFIND — expensive.  When a season
# pack lands, its N episode monitors each reach Phase 2 within the same
# second and every one fires its own root refresh, producing an N-way burst
# of full-root PROPFINDs that trips TorBox's WebDAV listing rate-limit
# (HTTP 429 "rate limit exceeded" on ``webdav.torbox.app``).  Coalesce them:
# a refresh fires at most once per ``_ROOT_REFRESH_COALESCE_S`` window across
# ALL monitor threads, so the season-pack burst collapses to a single
# re-list that surfaces every sibling episode at once.
_ROOT_REFRESH_COALESCE_S = 15  # seconds; one TorBox WebDAV listing window
_root_refresh_lock = threading.Lock()
_root_refresh_ts = 0.0  # monotonic ts of the last fired root refresh


def _coalesced_root_refresh(*, _now=None):
    """Fire an rclone root re-list at most once per coalesce window.

    Returns ``True`` when this call actually triggered the refresh, ``False``
    when it was suppressed because another monitor thread already refreshed
    within ``_ROOT_REFRESH_COALESCE_S``.  The window is claimed *before* the
    (best-effort) refresh runs so two concurrent callers can't both fire;
    any refresh failure is swallowed — the readiness probe degrades to
    polling the existing dir-cache.  Uses a monotonic clock: this is a pure
    elapsed-time window (unlike ``_check_torbox_cooldown``, which compares
    against TB's externally-sourced wall-clock ``cooldown_until``), so a
    backward NTP/VM-resume step can't over-suppress refreshes.  ``_now`` is
    a test seam.
    """
    now = _now if _now is not None else time.monotonic()
    global _root_refresh_ts
    with _root_refresh_lock:
        if now - _root_refresh_ts < _ROOT_REFRESH_COALESCE_S:
            return False
        _root_refresh_ts = now
    try:
        from utils.rclone_rc import refresh_dir
        refresh_dir('')
    except Exception:
        pass
    return True


def _check_torbox_cooldown(api_key, *, _now=None):
    """Return seconds remaining on TB's account-level ``cooldown_until``.

    Reads ``/v1/api/user/me`` and parses the ISO-8601 ``cooldown_until``
    field.  Returns ``0.0`` when there is no active cooldown, the response
    cannot be parsed, or the network call fails — every failure mode
    degrades to "treat as no cooldown" so a transient /user/me hiccup
    cannot wedge the entire TB add pipeline.

    The success-path result is cached for ``_TB_COOLDOWN_CACHE_TTL``
    seconds (with elapsed-time decay so callers see a monotonically
    non-increasing snapshot) so a retry storm only triggers one
    /user/me call per window.  **Failure paths do NOT write the cache**:
    a transient /user/me 5xx during a real cooldown must not mask the
    cooldown for the full TTL — the next call will re-probe and pick
    it up once the API recovers.  ``_now`` is a test seam.
    """
    now = _now if _now is not None else time.time()
    with _rate_limit_lock:
        cached_at = _tb_cooldown_cache.get('checked_at', 0.0)
        cached_seconds = _tb_cooldown_cache.get('seconds_until', 0.0)
    if now - cached_at < _TB_COOLDOWN_CACHE_TTL:
        elapsed = now - cached_at
        return max(0.0, cached_seconds - elapsed)
    if not api_key:
        return 0.0
    try:
        headers = {'Authorization': f'Bearer {api_key}'}
        resp = requests.get(
            'https://api.torbox.app/v1/api/user/me',
            headers=headers, timeout=10,
        )
        if resp.status_code != 200:
            return 0.0  # transient failure — do NOT cache
        payload = resp.json() or {}
        cooldown_until_str = (payload.get('data') or {}).get('cooldown_until')
        if not cooldown_until_str:
            seconds_until = 0.0
        else:
            # TB returns RFC 3339 UTC like ``2026-05-28T08:40:58Z``.
            # ``fromisoformat`` parses the ``Z`` suffix only from
            # Python 3.11+; normalise to ``+00:00`` for compatibility.
            # Also guard against a future TB change that drops the
            # timezone marker entirely — assume UTC in that case so
            # ``.timestamp()`` doesn't silently interpret as local time.
            normalised = cooldown_until_str.replace('Z', '+00:00')
            dt = datetime.fromisoformat(normalised)
            if dt.tzinfo is None:
                from datetime import timezone
                dt = dt.replace(tzinfo=timezone.utc)
            expires = dt.timestamp()
            seconds_until = max(0.0, expires - now)
    except Exception as exc:  # network, JSON, ISO parse — all degrade safely
        logger.debug(f"[blackhole] TB cooldown probe failed: {exc}")
        return 0.0  # transient failure — do NOT cache
    with _rate_limit_lock:
        _tb_cooldown_cache['checked_at'] = now
        _tb_cooldown_cache['seconds_until'] = seconds_until
    return seconds_until


_RATE_LIMIT_SLEEP_CHUNK = 300  # seconds — cap per-call sleep so a long
                                # cooldown (e.g. TB daily-quota 22h) can be
                                # preempted by SIGTERM / manual window
                                # reset without wedging the worker thread.


def _check_rate_limit(provider, *, _max_chunk=None):
    """Block until any active rate-limit window for *provider* expires.

    Cheap when no window is active (one lock acquire + dict lookup).
    For long windows (e.g. TB account cooldown can be tens of thousands
    of seconds), the sleep is chunked at ``_RATE_LIMIT_SLEEP_CHUNK`` so
    a SIGTERM or a manually cleared window can interrupt the wait
    without leaving the worker stuck on a 22-hour ``time.sleep``.
    """
    max_chunk = _max_chunk if _max_chunk is not None else _RATE_LIMIT_SLEEP_CHUNK
    logged = False
    while True:
        with _rate_limit_lock:
            until = _rate_limit_until.get(provider, 0)
        now = time.time()
        remaining = until - now
        if remaining <= 0:
            return
        wait = min(remaining, max_chunk)
        if not logged:
            logger.warning(
                f"[blackhole] {provider}: rate-limit window active, "
                f"sleeping up to {remaining:.1f}s before next add"
            )
            logged = True
        time.sleep(wait)
        # If we slept the full remaining duration in one chunk we're done;
        # otherwise loop to wait the rest (re-reading ``until`` so a
        # manual reset can wake us up early).
        if wait < max_chunk:
            return


def _mark_rate_limited(provider, seconds=None):
    """Mark *provider* as rate-limited for *seconds* (default _RATE_LIMIT_BACKOFF)."""
    seconds = seconds if seconds is not None else _RATE_LIMIT_BACKOFF
    with _rate_limit_lock:
        _rate_limit_until[provider] = time.time() + seconds


def _is_rate_limit_response(response):
    """Return True iff *response* indicates the debrid API rate-limited us.

    Recognises HTTP 429 (standard) and an HTTP-200/4xx body containing
    the phrase ``rate limit`` (RD's text response on its custom 429-ish
    error path, as well as TB's).  Case-insensitive substring match —
    no full-text parse, just enough to distinguish from real failures.
    """
    try:
        if response.status_code == 429:
            return True
        body = (response.text or '').lower()
        return 'rate limit' in body or 'rate_limit' in body
    except Exception:
        return False

# Media file extensions for symlink creation
MEDIA_EXTENSIONS = {'.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.ts', '.m4v', '.webm'}

# Zurg mount category directories (checked in order; __all__ is fallback)
MOUNT_CATEGORIES = ['shows', 'movies', 'anime']

# Leading ``[indexer.to] `` prefix that TorBox sometimes adds to its API
# ``data.name`` field via the indexer scraper. The folder TorBox writes
# to its WebDAV mount has the bare torrent folder name without that
# prefix, so the mount lookup needs to try the stripped form too.
#
# Implementation lives in ``utils.debrid_routing`` (moved during plan
# 41 phase-B reviewer fix-up to eliminate a cross-module private-name
# dependency).  The alias below keeps the existing call sites in this
# module working without per-call-site churn.


def _strip_indexer_prefix(name):
    """Strip a leading ``[indexer.to] `` block from a release name.

    Thin alias for ``utils.debrid_routing.strip_indexer_prefix`` — kept
    here so existing in-module callers (``_find_on_torbox_mount``) don't
    grow a top-level import that would re-form the
    ``blackhole ↔ debrid_routing`` cycle.
    """
    from utils.debrid_routing import strip_indexer_prefix
    return strip_indexer_prefix(name)


def _is_safe_mount_name(name):
    """Reject release-name candidates that could escape the mount root.

    The release name flows in from a debrid API field (TB's ``data.name``,
    RD's ``filename``, AD's ``magnets.filename``) which is uploader-controlled
    at submission time.  ``os.path.join('/mnt/tb', '/etc')`` collapses to
    ``'/etc'`` — an absolute path on the right side wins — and a name
    containing ``..`` lets the caller climb out of the mount.  Either would
    make ``_find_on_*_mount`` happily return an ``os.path.isdir`` hit
    pointing OUTSIDE the intended rclone mount, which then becomes the
    ``os.walk`` root for symlink creation.

    Returns True iff *name* is a non-empty, single-segment, non-traversing
    string with no NUL byte and no path separator.  Mount listdir entries
    are also passed through this helper as defense-in-depth — well-behaved
    FUSE drivers never return multi-segment names, but TB's WebDAV stack
    has been observed sanitising names in surprising ways.
    """
    if not name or name in ('.', '..'):
        return False
    if '/' in name or '\\' in name or '\x00' in name:
        return False
    if os.path.isabs(name):
        return False
    return True

# Label routing: subdir names in watch_dir that are NOT labels
# (retry staging and alt-retry staging — handled by dedicated logic)
_RESERVED_LABELS = {'failed', '.alt_pending'}

# Label validation: alphanumeric plus hyphen/underscore, max 64 chars
_LABEL_MAX_LEN = 64
_LABEL_RE = re.compile(r'^[A-Za-z0-9_-]+$')


def _is_valid_label(name):
    """Return True if *name* is a valid per-arr routing label.

    Labels must be alphanumeric plus hyphen/underscore, max 64 chars,
    and cannot match any reserved name (case-insensitive).
    """
    if not name or not isinstance(name, str):
        return False
    if len(name) > _LABEL_MAX_LEN:
        return False
    if name.lower() in _RESERVED_LABELS:
        return False
    if not _LABEL_RE.match(name):
        return False
    return True


def iter_release_dirs(completed_dir):
    """Yield ``(label, release_name, release_path)`` for each release dir under *completed_dir*.

    Handles three layouts:
      - Flat: ``completed_dir/<release_name>/`` (contains files) → label=None
      - Labeled: ``completed_dir/<label>/<release_name>/`` → label=<label>
      - Mixed: both coexist (users mid-migration)

    Heuristic for distinguishing a label parent dir from a flat-mode release
    dir:
      1. The name must match the label whitelist (``_is_valid_label``).
      2. The dir is EITHER empty (a label subdir awaiting its first release)
         OR contains only subdirectories (no loose files, which would imply
         a release dir that happens to have a label-compatible name).
    Anything else is treated as a flat-mode release dir with label=None.

    Known caveat: a flat-mode release dir whose name matches the label
    whitelist and whose contents are exclusively subdirectories (e.g. a
    `Season 01/` subdir containing files) is misclassified as a label
    parent. In practice release names almost always include dots/spaces
    or bracket tags, so the whitelist rejects them. If a user runs into
    this, rename the release dir or switch to the `strict`/`off` modes
    (planned follow-up).

    Consumers of ``BLACKHOLE_COMPLETED_DIR`` (cleanup, empty-dir sweep,
    symlink verification, title removal) should use this helper instead
    of ``os.listdir(completed_dir)``.
    """
    if not completed_dir or not os.path.isdir(completed_dir):
        return

    try:
        top_entries = os.listdir(completed_dir)
    except OSError:
        return

    for entry in top_entries:
        entry_path = os.path.join(completed_dir, entry)
        if not os.path.isdir(entry_path):
            continue

        # Decide whether this is a label dir or a flat-mode release dir.
        # A label dir has a valid label name AND either:
        #   - is empty (the user just created /completed/sonarr/), OR
        #   - contains at least one subdirectory (a release).
        # Stray loose files inside a label dir (e.g. .DS_Store, Thumbs.db,
        # arr lockfiles) are ignored rather than demoting the whole dir to
        # flat-mode — demotion would cause _cleanup_symlinks to wipe the
        # entire label tree when it aged out.
        is_label = False
        if _is_valid_label(entry):
            try:
                children = list(os.scandir(entry_path))
            except OSError:
                children = []
            has_subdir = any(c.is_dir(follow_symlinks=False) for c in children)
            if has_subdir or not children:
                is_label = True

        if is_label:
            try:
                for sub in os.listdir(entry_path):
                    sub_path = os.path.join(entry_path, sub)
                    if os.path.isdir(sub_path):
                        yield (entry, sub, sub_path)
            except OSError:
                continue
        else:
            yield (None, entry, entry_path)

# Terminal debrid statuses that mean the torrent will never complete
RD_TERMINAL_ERRORS = {'magnet_error', 'error', 'virus', 'dead'}
AD_TERMINAL_ERRORS = {'Error'}
TB_TERMINAL_ERRORS = {'error', 'failed'}

# TorBox download_state values that mean the file is on TB storage and
# reachable via WebDAV — i.e. the blackhole should stop polling and
# proceed to symlink creation.  TB uses ``cached`` for instant-cache
# hits (the dominant case under plan 39 cache_aware routing, since
# every TB-routed grab is cache-positive by construction), ``completed``
# for torrents that went through a full BT download, and ``uploading``
# for torrents in the post-download seed phase where the file is still
# present.  Checking only ``completed`` (the pre-fix behaviour) caused
# every cached TB grab to time out at ``mount_poll_timeout`` (default
# 300s) and get auto-blocklisted as "Uncached on debrid (timed out)",
# even though the file had been ready since the moment the add returned.
TB_READY_STATES = {'completed', 'cached', 'uploading'}


def _bencode_end(data, start):
    """Find the end offset of a bencoded value starting at `start`.

    Supports dicts (d...e), lists (l...e), integers (iNe), and byte strings (N:...).
    Returns the offset ONE PAST the last byte, or None on parse error.
    """
    if start >= len(data):
        return None
    ch = data[start:start + 1]
    if ch == b'd' or ch == b'l':
        pos = start + 1
        while pos < len(data) and data[pos:pos + 1] != b'e':
            pos = _bencode_end(data, pos)
            if pos is None:
                return None
            # Dicts have key-value pairs; after key we need the value
            if ch == b'd':
                pos = _bencode_end(data, pos)
                if pos is None:
                    return None
        return pos + 1 if pos < len(data) else None
    elif ch == b'i':
        end = data.find(b'e', start + 1)
        return end + 1 if end != -1 else None
    elif ch and ch[0:1].isdigit():
        colon = data.find(b':', start)
        if colon == -1:
            return None
        try:
            length = int(data[start:colon])
        except ValueError:
            return None
        return colon + 1 + length
    return None


def _parse_episodes(filename):
    """Extract episode numbers from a release filename.

    Returns a set of episode ints, or empty set for season packs.
    Handles S01E04, S01E04E05, S01E04-E06, etc.
    """
    name = re.sub(r'\.(torrent|magnet)$', '', filename, flags=re.IGNORECASE)
    # Match S01E04, S01E04E05, S01E04-E06, etc.
    m = re.search(r'S\d+(E\d+(?:[E\-]E?\d+)*)', name, re.IGNORECASE)
    if not m:
        return set()
    ep_str = m.group(1)
    nums = [int(x) for x in re.findall(r'\d+', ep_str)]
    if len(nums) == 2 and '-' in ep_str:
        lo, hi = nums
        if lo <= hi and (hi - lo) < 100:
            return set(range(lo, hi + 1))
        return {lo, hi}
    return set(nums)


_BARE_YEAR_RE = re.compile(r'\s*\(\d{4}\)\s*$')


def _safe_entry_title(entry):
    """Extract the canonical title from a TMDB cache entry, defending
    against malformed entries where ``title`` is non-string (dict, list,
    int) — calling ``.strip()`` on those would raise AttributeError and
    crash the whole resolution path.  Returns a stripped str, or ''.
    """
    t = entry.get('title')
    return t.strip() if isinstance(t, str) else ''


def _extract_entry_year(entry):
    """Pull a 4-digit year from a TMDB cache entry. Movies use ``release_date``,
    shows use ``first_air_date``. Returns int or None.
    """
    for key in ('release_date', 'first_air_date'):
        date = entry.get(key, '') or ''
        if isinstance(date, str) and len(date) >= 4 and date[:4].isdigit():
            try:
                return int(date[:4])
            except ValueError:
                pass
    return None


def _lookup_canonical_in_tmdb(title, year, is_tv):
    """Look up a canonical media title in the local TMDB cache.

    Tries (1) direct year-qualified key match, then (2) token-aligned prefix
    match against cache keys to recover from parsers that left actor names
    or genre tags appended to the title (e.g.  "Gattaca Ethan Hawke Sci Fi"
    → "Gattaca").  Year, when available on both sides, must match.

    Returns the canonical TMDB title (str) on hit, None otherwise.  Safe
    when the cache file is missing or the tmdb module fails to import.
    """
    try:
        from utils import tmdb as _tmdb
        from utils.library import normalize_title, norm_for_matching
    except Exception as e:
        # Module-level failure (genuine import bug, not a missing cache
        # file) — log so silent grab-event degradation is discoverable.
        logger.debug("[blackhole] canonical-title lookup imports failed: %s", e)
        return None

    try:
        with _tmdb._cache_lock:
            cache = _tmdb._load_cache()
    except Exception as e:
        logger.debug("[blackhole] canonical-title cache load failed: %s", e)
        return None

    section_key = 'shows' if is_tv else 'movies'
    section = cache.get(section_key, {})
    if not section or not isinstance(section, dict):
        return None

    norm = normalize_title(title or '')
    if not norm:
        return None

    # (1) Direct year-qualified lookup — fastest path
    try:
        entry = _tmdb._cache_lookup(section, norm, year)
    except Exception:
        entry = None
    if isinstance(entry, dict):
        canonical = _safe_entry_title(entry)
        if canonical:
            return canonical

    # (2) Token-aligned prefix match.  The robust parser may leave
    # extraneous words (actor name, genre tag) appended to the title; we
    # pick the longest cache entry whose token sequence is a strict
    # prefix of the parsed token sequence.
    parsed_tokens = norm_for_matching(title or '').split()
    if not parsed_tokens:
        return None

    best_canonical = None
    best_token_count = 0

    for cache_key, entry in section.items():
        try:
            if not isinstance(entry, dict) or not isinstance(cache_key, str):
                continue
            bare_key = _BARE_YEAR_RE.sub('', cache_key)
            candidate_tokens = norm_for_matching(bare_key).split()
            if not candidate_tokens:
                continue
            if len(candidate_tokens) > len(parsed_tokens):
                continue
            if parsed_tokens[:len(candidate_tokens)] != candidate_tokens:
                continue
            # Single-word cache title prefixing a multi-word parse is a
            # high false-positive risk — e.g. cache entry "The" (the 2017
            # film) would prefix-match every release whose name starts
            # with "The".  Demand year confirmation in this narrow case;
            # the multi-word matches below are inherently more specific.
            if len(candidate_tokens) == 1 and len(parsed_tokens) > 1:
                if year is None:
                    continue
                entry_year = _extract_entry_year(entry)
                if entry_year != year:  # also rejects None (fail-closed here)
                    continue
            elif year is not None:
                # Multi-token candidate: keep prior fail-open behavior so
                # legacy entries lacking release_date/first_air_date can
                # still resolve (the candidate's specificity already
                # protects against false positives).
                entry_year = _extract_entry_year(entry)
                if entry_year is not None and entry_year != year:
                    continue
            # Longest = most specific.  Ties go to first-seen.
            if len(candidate_tokens) > best_token_count:
                canonical = _safe_entry_title(entry)
                if canonical:
                    best_canonical = canonical
                    best_token_count = len(candidate_tokens)
        except Exception as e:
            # Defensive: a single malformed entry must not poison the
            # whole scan — drop it and keep going.
            logger.debug("[blackhole] skipping malformed cache entry %r: %s",
                         cache_key, e)
            continue

    return best_canonical


def _resolve_canonical_title(filename, fallback_name, is_tv):
    """Refine a release filename's media title via the library's robust
    parser plus an optional TMDB-cache canonical lookup.

    Returns the best available title — preferring TMDB-canonical, then
    the library parser's output, then ``fallback_name`` (typically the
    naive parse_release_name output).  Never returns the empty string
    or None when ``fallback_name`` is truthy; on any error path the
    caller still gets the original behavior.
    """
    if not filename:
        return fallback_name

    base = re.sub(r'\.(torrent|magnet)$', '', filename, flags=re.IGNORECASE)
    if not base:
        return fallback_name

    # Lazy import — avoids circular-import risk and keeps blackhole
    # module load cheap when the library module isn't needed.
    try:
        from utils.library import parse_folder_name
    except Exception:
        return fallback_name

    try:
        robust_title, robust_year = parse_folder_name(base)
    except Exception:
        return fallback_name

    if not robust_title:
        return fallback_name

    canonical = _lookup_canonical_in_tmdb(robust_title, robust_year, is_tv)
    if canonical:
        return canonical
    return robust_title


def _enrich_for_history(filename):
    """Extract media_title and episode string from a torrent filename for history logging.

    The media_title is resolved via a 3-step cascade: (1) naive parser
    establishes is_tv + season/episode for the ep_str, (2) the library's
    robust parser refines the title (handles bracketed/parenthesized
    years, dash separators, edition tags), (3) when the title still has
    extra junk (actor names, genre tags injected mid-name), a
    token-aligned prefix lookup against the TMDB cache resolves it to
    the canonical media title.  Each step falls back safely.
    """
    name, season, is_tv = parse_release_name(filename)
    eps = _parse_episodes(filename)
    ep_str = None
    if is_tv and season is not None and eps:
        ep_str = f"S{season:02d}" + "".join(f"E{e:02d}" for e in sorted(eps))
    elif is_tv and season is not None:
        ep_str = f"S{season:02d}"

    refined = _resolve_canonical_title(filename, name, is_tv)
    return (refined or None), ep_str


def _is_resolving_video(path):
    """True iff path is a video file (by extension) that actually resolves.

    Broken symlinks (dangling debrid targets) return False so they never count
    as local content.
    """
    if os.path.splitext(path)[1].lower() not in MEDIA_EXTENSIONS:
        return False
    return os.path.exists(path)


def _dir_has_video(path, recursive=False):
    """True iff the directory contains at least one resolving video file.

    Subtitles (.srt), .nfo, artwork, and broken symlinks do NOT count — only a
    real, resolving media file marks the content as present locally. This is
    what prevents an orphan subtitle folder from permanently blocking a grab.

    ``recursive`` descends exactly one level (``show/Season NN/episode.mkv`` is
    the deepest real arr/Plex layout) — a bounded scan, NOT an unbounded
    ``os.walk``, so a no-video show folder full of subtitle/metadata files on a
    throttled FUSE mount can't trigger a full-subtree stat storm.
    """
    try:
        for f in os.listdir(path):
            child = os.path.join(path, f)
            if _is_resolving_video(child):
                return True
            if recursive and os.path.isdir(child):
                try:
                    for sub in os.listdir(child):
                        if _is_resolving_video(os.path.join(child, sub)):
                            return True
                except OSError:
                    continue
    except OSError:
        pass
    return False


def _local_episodes(season_dir):
    """Extract episode numbers from resolving video files in a local season dir.

    Only real video files (MEDIA_EXTENSIONS, not broken symlinks) contribute an
    episode number — a stray ``ShowName.S01E03.srt`` must not make episode 3
    look present locally.
    """
    eps = set()
    try:
        for f in os.listdir(season_dir):
            if not _is_resolving_video(os.path.join(season_dir, f)):
                continue
            for m in re.finditer(r'(?<![a-zA-Z])[Ee](\d+)', f):
                eps.add(int(m.group(1)))
    except OSError:
        pass
    return eps


def parse_release_name(filename):
    """Extract show/movie name and season from a release filename.

    Returns (name, season_number_or_None, is_tv).
    """
    # Remove file extension
    name = re.sub(r'\.(torrent|magnet)$', '', filename, flags=re.IGNORECASE)

    # Try to find season pattern (S01E01, S01, Season 1)
    season_match = re.search(
        r'[.\s]S(\d{1,2})[E.\s]|[.\s]S(\d{1,2})[.\s]|[.\s]S(\d{1,2})$|Season[.\s](\d{1,2})',
        name, re.IGNORECASE,
    )

    if season_match:
        season = int(next(g for g in season_match.groups() if g is not None))
        # Everything before the season marker is the show name
        show_name = name[:season_match.start()]
        show_name = re.sub(r'[.\-_]', ' ', show_name).strip()
        show_name = re.sub(r'\s*\(?\d{4}\)?\s*$', '', show_name).strip()
        return show_name, season, True

    # No season pattern — likely a movie
    year_match = re.search(r'[.\s](\d{4})[.\s]', name)
    if year_match:
        movie_name = name[:year_match.start()]
    else:
        quality_match = re.search(
            r'[.\s](1080p|720p|2160p|4K|WEB|BluRay|BDRip|HDTV|REMUX)',
            name, re.IGNORECASE,
        )
        movie_name = name[:quality_match.start()] if quality_match else name

    movie_name = re.sub(r'[.\-_]', ' ', movie_name).strip()
    return movie_name, None, False


def _is_multi_season_pack(release_name):
    """Detect if a release name indicates a multi-season pack.

    Returns (is_multi, season_start, season_end).
    For 'Complete Series/Collection' patterns returns (True, None, None)
    since the range isn't known from the name alone.
    """
    # 1. S01E01-S05E10 (cross-season episode range)
    m = re.search(r'S(\d{1,2})E\d+\s*[-–]\s*S(\d{1,2})E\d+', release_name, re.IGNORECASE)
    if m:
        s1, s2 = int(m.group(1)), int(m.group(2))
        if s1 != s2:
            return True, min(s1, s2), max(s1, s2)

    # 2. S01-S05 (both prefixed with S)
    m = re.search(r'S(\d{1,2})\s*[-–]\s*S(\d{1,2})', release_name, re.IGNORECASE)
    if m:
        s1, s2 = int(m.group(1)), int(m.group(2))
        if s1 != s2:
            return True, min(s1, s2), max(s1, s2)

    # 3. S01-05 (first prefixed, second bare number)
    # S\d{1,2} immediately followed by dash then digits — no E between S## and dash.
    # (?![a-zA-Z\d]) prevents matching encoding markers like S05-10bit or S02-3D.
    m = re.search(r'S(\d{1,2})[-–](\d{1,2})(?![a-zA-Z\d])', release_name, re.IGNORECASE)
    if m:
        s1, s2 = int(m.group(1)), int(m.group(2))
        if s1 != s2:
            return True, min(s1, s2), max(s1, s2)

    # 4. Season(s) 1-5 / Seasons 1 & 2 / Seasons 1 and 2
    m = re.search(r'Seasons?[.\s]*(\d{1,2})[.\s]*(?:[-–&+]|and)[.\s]*(\d{1,2})', release_name, re.IGNORECASE)
    if m:
        s1, s2 = int(m.group(1)), int(m.group(2))
        if s1 != s2:
            return True, min(s1, s2), max(s1, s2)

    # 5. Series 1-3
    m = re.search(r'Series[.\s]*(\d{1,2})[.\s]*[-–][.\s]*(\d{1,2})', release_name, re.IGNORECASE)
    if m:
        s1, s2 = int(m.group(1)), int(m.group(2))
        if s1 != s2:
            return True, min(s1, s2), max(s1, s2)

    # 6. Complete Series / Complete Collection
    if re.search(r'Complete[.\s](?:Series|Collection)', release_name, re.IGNORECASE):
        return True, None, None

    return False, None, None


def _extract_file_season(filepath):
    """Extract season number from a media file path within a release.

    filepath is relative to the release root, e.g. 'Season 02/Show.S02E05.mkv'.
    Returns season number as int, or None if unparseable.
    """
    parts = filepath.replace('\\', '/').split('/')
    filename = parts[-1]

    # Check filename for SxxExx pattern (most reliable)
    m = re.search(r'[Ss](\d{1,2})[Ee]\d+', filename)
    if m:
        return int(m.group(1))

    # Fallback: Sxx without Exx (e.g., S03.Special.mkv) — must not re-match SxxExx
    m = re.search(r'[Ss](\d{1,2})(?=[.\s\-_]|$)(?![Ee]\d)', filename)
    if m:
        return int(m.group(1))

    # Check parent directories for season indicators
    for part in parts[:-1]:
        m = re.search(r'[Ss]eason[.\s]*(\d{1,2})', part, re.IGNORECASE)
        if m:
            return int(m.group(1))
        m = re.match(r'^S(\d{1,2})$', part, re.IGNORECASE)
        if m:
            return int(m.group(1))

    return None


def _build_season_release_name(original_name, season_num):
    """Construct a per-season release name from a multi-season pack name.

    Replaces the multi-season indicator with a single-season S{XX} pattern.
    Example: 'Breaking.Bad.S01-S05.1080p.BluRay-GROUP'
           → 'Breaking.Bad.S03.1080p.BluRay-GROUP'
    """
    sxx = f'S{season_num:02d}'

    # Try each multi-season pattern and replace with single season
    patterns = [
        r'S\d{1,2}E\d+\s*[-–]\s*S\d{1,2}E\d+',       # S01E01-S05E10
        r'S\d{1,2}\s*[-–]\s*S\d{1,2}',                  # S01-S05
        r'S\d{1,2}[-–]\d{1,2}',                          # S01-05
        r'Seasons?[.\s]*\d{1,2}[.\s]*(?:[-–&+]|and)[.\s]*\d{1,2}',  # Seasons 1-5
        r'Series[.\s]*\d{1,2}[.\s]*[-–][.\s]*\d{1,2}',  # Series 1-3
        r'Complete[.\s](?:Series|Collection)',             # Complete Series
    ]
    for pattern in patterns:
        result = re.sub(pattern, sxx, original_name, count=1, flags=re.IGNORECASE)
        if result != original_name:
            # Clean up double dots from replacement
            result = re.sub(r'\.{2,}', '.', result)
            return result.strip('.')

    # Fallback: append season
    result = f'{original_name}.{sxx}'
    result = re.sub(r'\.{2,}', '.', result)
    return result.strip('.')


# Serializes all RetryMeta load-modify-save operations across threads so a
# concurrent read-then-write cannot lose a tier advance or reset
# first_attempted_at.  Sidecar writes are low-frequency (once per blackhole
# decision) so a single module-level lock is cheap; per-file locking would
# add bookkeeping without meaningful gain.  RLock so helpers that call
# other helpers (future Phase 5 wiring) don't self-deadlock.
_retry_meta_lock = threading.RLock()


class RetryMeta:
    """Tracks retry state for failed blackhole files via JSON sidecar files.

    State survives container restarts since it's persisted to disk.

    V2 schema (plan 33) adds a nested ``tier_state`` object for the
    quality-compromise state machine.  Legacy files without ``tier_state``
    load correctly — ``read_tier_state()`` returns ``None`` and the
    compromise engine treats that as "not yet in the compromise flow".
    Top-level keys (``retries``, ``last_attempt``, ``alt_exhausted``)
    retain their v1 semantics; ``write()`` now preserves unrelated keys
    so a retry-count bump does not wipe compromise state.

    All load-modify-save helpers serialize through ``_retry_meta_lock``
    so concurrent callers (blackhole worker + alt-retry thread) cannot
    interleave reads and writes in a way that drops an advance or
    re-seeds the dwell clock.
    """

    # Bump whenever the nested tier_state shape or semantics change so
    # upgrades can migrate forward.  A reader encountering a HIGHER version
    # than it knows falls back to "no tier state" (re-seeded fresh) rather
    # than operating on unknown fields.  A reader encountering a LOWER
    # version than the minimum it trusts ALSO falls back to re-seed — see
    # ``_validate_tier_state`` and ``_MIN_TRUSTED_TIER_STATE_VERSION`` below.
    #
    # Version history:
    #   1 — initial schema (plan 33 Phase 2).  Had an inverted ``tier_order``
    #       semantic: ``get_tier_order`` preserved Sonarr's ASCENDING API
    #       order but consumers treated ``tier_order[0]`` as preferred.
    #       Any v1 sidecar seeded under the buggy code is unreliable.
    #   2 — ``tier_order`` is guaranteed preferred-first (post-fix for the
    #       inverted-ordering bug).  Shape is otherwise unchanged from v1.
    TIER_STATE_SCHEMA_VERSION = 2
    _MIN_TRUSTED_TIER_STATE_VERSION = 2

    @staticmethod
    def meta_path(file_path):
        return file_path + '.meta'

    # -- Low-level I/O helpers (internal) ---------------------------------

    @staticmethod
    def _load_raw(file_path):
        """Return the full meta dict; ``{}`` on missing or corrupt file."""
        meta = RetryMeta.meta_path(file_path)
        if not os.path.exists(meta):
            return {}
        try:
            with open(meta, 'r') as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
        except (json.JSONDecodeError, IOError):
            pass
        return {}

    @staticmethod
    def _save_raw(file_path, data):
        """Atomic save of the full meta dict.  Returns True on success.

        Uses ``atomic_write`` so a torn write during a crash leaves the
        existing sidecar intact — critical once ``tier_state`` drives
        compromise decisions, because a corrupt sidecar would either
        re-seed from scratch (resetting the dwell clock) or be ignored
        entirely (losing the per-tier attempt history).

        Catches ``TypeError``/``ValueError`` alongside the usual I/O
        errors because v2 serializes user-supplied strings (reason,
        outcome, tier labels) and a malformed value must not bubble up
        and kill the watcher poll cycle.
        """
        meta = RetryMeta.meta_path(file_path)
        try:
            with atomic_write(meta) as f:
                json.dump(data, f)
            return True
        except (IOError, OSError, TypeError, ValueError) as e:
            logger.warning(f"[blackhole] Could not write retry meta for {file_path}: {e}")
            return False

    @staticmethod
    def _validate_tier_state(ts):
        """Return *ts* if it passes v1 shape checks, else ``None``.

        A hand-edited or future-schema sidecar could land a dict with
        unexpected types in ``tier_order``/``tier_attempts`` /
        ``current_tier_index`` — subscripting those in
        ``record_tier_attempt`` or ``advance_tier`` would crash the
        decision loop.  We reject the whole tier_state rather than
        partially trust it; the caller treats ``None`` as "legacy /
        absent" and re-seeds fresh.
        """
        if not isinstance(ts, dict):
            return None
        version = ts.get('schema_version', 1)
        if not isinstance(version, int) or isinstance(version, bool):
            return None
        if version > RetryMeta.TIER_STATE_SCHEMA_VERSION:
            # Forward-compat guard: a downgrade from a future writer must
            # not silently act on fields this code doesn't understand.
            logger.warning(
                f"[blackhole] Ignoring tier_state with schema_version={version} "
                f"(this code supports up to {RetryMeta.TIER_STATE_SCHEMA_VERSION})"
            )
            return None
        if version < RetryMeta._MIN_TRUSTED_TIER_STATE_VERSION:
            # Backward-compat guard: v1 sidecars were written under the
            # inverted-tier_order bug; the stored ``tier_order`` may have
            # ``tier_order[0]`` as the LOWEST-quality tier.  Treating it as
            # preferred would send compromise upward in quality — the
            # opposite of user intent.  Return None so ``init_tier_state``
            # re-seeds fresh with the corrected (preferred-first) order on
            # the next retry pass, at the cost of resetting that item's
            # dwell clock (acceptable: the alternative is permanently wrong
            # decisions for items seeded pre-fix).
            logger.info(
                f"[blackhole] Re-seeding tier_state with schema_version={version} "
                f"(minimum trusted: {RetryMeta._MIN_TRUSTED_TIER_STATE_VERSION} "
                f"— pre-fix v1 sidecars had inverted tier_order)"
            )
            return None
        if not isinstance(ts.get('tier_order', []), list):
            return None
        if not isinstance(ts.get('tier_attempts', []), list):
            return None
        current = ts.get('current_tier_index', 0)
        if not isinstance(current, int) or isinstance(current, bool) or current < 0:
            return None
        return ts

    @staticmethod
    def read(file_path):
        """Read retry count and last attempt time. Returns (retries, last_attempt)."""
        data = RetryMeta._load_raw(file_path)
        return data.get('retries', 0), data.get('last_attempt', 0)

    @staticmethod
    def write(file_path, retries):
        """Write retry count and current timestamp.

        Preserves unrelated keys (``alt_exhausted``, ``tier_state``, etc.)
        so the compromise state survives a retry-count bump — without
        this, the first retry after tier_state is seeded would silently
        wipe the dwell timer and reset the state machine.

        A persistent I/O failure used to be a debug-level log; it's now
        a warning so operators see the cause if retry counts appear
        stuck at zero (a read-only sidecar dir would otherwise retry
        forever without surfacing).
        """
        with _retry_meta_lock:
            data = RetryMeta._load_raw(file_path)
            data['retries'] = retries
            data['last_attempt'] = time.time()
            if not RetryMeta._save_raw(file_path, data):
                logger.warning(
                    f"[blackhole] Retry count for {file_path} may not be persisted; "
                    f"check sidecar directory permissions and free space"
                )

    @staticmethod
    def remove(file_path):
        """Clean up sidecar meta file."""
        with _retry_meta_lock:
            meta = RetryMeta.meta_path(file_path)
            try:
                if os.path.exists(meta):
                    os.remove(meta)
            except OSError:
                pass

    @staticmethod
    def mark_alt_exhausted(file_path):
        """Flag this sidecar so the retry loop skips alt-release re-search.

        Centralises the two call sites that previously wrote the sidecar
        by hand with plain ``open()`` + ``json.dump`` — those bypassed
        ``_save_raw`` and would wipe any tier_state already seeded by
        ``init_tier_state``.  Using this helper preserves tier_state AND
        gets the atomic-write crash safety.
        """
        with _retry_meta_lock:
            data = RetryMeta._load_raw(file_path)
            # Preserve tier_state and any other fields; only bump the
            # three v1 fields that the legacy writer ever set.
            data['retries'] = 1
            data['last_attempt'] = time.time()
            data['alt_exhausted'] = True
            return RetryMeta._save_raw(file_path, data)

    @staticmethod
    def is_alt_exhausted(file_path):
        """Return True if alt-release search has already been exhausted."""
        return bool(RetryMeta._load_raw(file_path).get('alt_exhausted', False))

    # -- V2 tier-state helpers (plan 33) ----------------------------------

    @staticmethod
    def arr_url_hash(arr_url):
        """SHA-256 of the arr base URL, truncated to 6 hex chars.

        Disambiguates per-arr-instance compromise state without logging
        the raw URL.  A user running ``sonarr-4k`` and ``sonarr-hd`` gets
        independent decisions for the same release name because the
        stored hash differs.  Six hex chars is ~1-in-16M collision risk,
        acceptable because collisions only cross-contaminate state
        between two distinct arrs serving the same filename — a rare
        edge case where the fallout is a tier choice computed from the
        wrong profile, not data loss.
        """
        if not arr_url:
            return ''
        return hashlib.sha256(arr_url.encode('utf-8')).hexdigest()[:6]

    @staticmethod
    def read_tier_state(file_path):
        """Return the ``tier_state`` dict, or ``None`` for legacy entries.

        Legacy sidecars (v1 schema without nested tier_state) yield
        ``None`` so callers can seed fresh via ``init_tier_state`` —
        this is the backward-compatibility hinge described in the plan.
        Malformed or future-schema tier_state also yields ``None`` so
        the decision loop degrades gracefully rather than crashing.
        """
        data = RetryMeta._load_raw(file_path)
        return RetryMeta._validate_tier_state(data.get('tier_state'))

    @staticmethod
    def init_tier_state(file_path, arr_service, arr_url, profile_id,
                        tier_order, now=None):
        """Seed ``tier_state`` on the first attempt.  Idempotent.

        Returns the persisted (or pre-existing) tier_state dict.  If
        tier_state already exists AND passes shape validation, it is
        returned unchanged — overwriting ``first_attempted_at`` would
        let retries game the dwell timer (I3: dwell is measured from
        the first preferred-tier attempt, not the most recent one).
        A malformed pre-existing tier_state is replaced rather than
        trusted.
        """
        with _retry_meta_lock:
            data = RetryMeta._load_raw(file_path)
            existing = RetryMeta._validate_tier_state(data.get('tier_state'))
            if existing is not None:
                return existing
            if now is None:
                now = time.time()
            tier_state = {
                'schema_version': RetryMeta.TIER_STATE_SCHEMA_VERSION,
                'arr_service': arr_service,
                'arr_url_hash': RetryMeta.arr_url_hash(arr_url),
                'profile_id': profile_id,
                'tier_order': list(tier_order or []),
                'current_tier_index': 0,
                'first_attempted_at': now,
                'tier_attempts': [],
                'compromise_fired_at': None,
                'last_advance_reason': None,
                'season_pack_attempted': False,
            }
            data['tier_state'] = tier_state
            RetryMeta._save_raw(file_path, data)
            return tier_state

    @staticmethod
    def record_tier_attempt(file_path, tier_index, cached_hits, uncached_hits,
                            outcome, now=None):
        """Upsert a tier_attempts entry for *tier_index*.

        Existing entry for the same index: bump ``last_tried_at``,
        increment ``attempts``, refresh hit counts and outcome.
        No existing entry: append a fresh one with ``attempts=1``.

        Returns True if persisted, False if ``tier_state`` is missing
        (caller must have already called ``init_tier_state``) or if
        ``tier_index`` is out of the profile's tier range (I1: never
        record an attempt at a tier the profile doesn't allow).  Bool
        tier_index rejected (bool is-a int in Python) to defend against
        accidental truthy use.
        """
        if not isinstance(tier_index, int) or isinstance(tier_index, bool):
            return False
        if tier_index < 0:
            return False
        with _retry_meta_lock:
            data = RetryMeta._load_raw(file_path)
            ts = RetryMeta._validate_tier_state(data.get('tier_state'))
            if ts is None:
                return False
            if now is None:
                now = time.time()
            order = ts.get('tier_order') or []
            if tier_index >= len(order):
                return False
            tier_label = order[tier_index]
            attempts = ts.setdefault('tier_attempts', [])
            existing = None
            for entry in attempts:
                if (isinstance(entry, dict)
                        and isinstance(entry.get('tier_index'), int)
                        and not isinstance(entry.get('tier_index'), bool)
                        and entry.get('tier_index') == tier_index):
                    existing = entry
                    break
            cached_count = max(0, int(cached_hits or 0))
            uncached_count = max(0, int(uncached_hits or 0))
            if existing is None:
                attempts.append({
                    'tier': tier_label,
                    'tier_index': tier_index,
                    'first_tried_at': now,
                    'last_tried_at': now,
                    'attempts': 1,
                    'cached_hits_found': cached_count,
                    'uncached_hits_found': uncached_count,
                    'outcome': outcome,
                })
            else:
                prev_attempts = existing.get('attempts', 0)
                if not isinstance(prev_attempts, int) or isinstance(prev_attempts, bool):
                    prev_attempts = 0
                existing['last_tried_at'] = now
                existing['attempts'] = max(0, prev_attempts) + 1
                existing['cached_hits_found'] = cached_count
                existing['uncached_hits_found'] = uncached_count
                existing['outcome'] = outcome
            return RetryMeta._save_raw(file_path, data)

    @staticmethod
    def advance_tier(file_path, new_tier_index, reason, now=None):
        """Advance ``current_tier_index`` downward (strictly increasing).

        I2 — monotonic downward movement: refuses to stay at or move
        above the current index.  Out-of-range indices are refused so
        the compromise engine never lands outside the profile's allowed
        tier list (I1: profile is the ceiling).  Sets
        ``compromise_fired_at`` on the first advance only so history
        records the initial compromise timestamp, not the most recent.

        Returns True if persisted.  False means: tier_state missing,
        new_tier_index invalid, or the advance would violate I1/I2.
        """
        if not isinstance(new_tier_index, int) or isinstance(new_tier_index, bool):
            return False
        with _retry_meta_lock:
            data = RetryMeta._load_raw(file_path)
            ts = RetryMeta._validate_tier_state(data.get('tier_state'))
            if ts is None:
                return False
            current = ts.get('current_tier_index', 0)
            if not isinstance(current, int) or isinstance(current, bool):
                current = 0
            if new_tier_index <= current:
                return False
            order = ts.get('tier_order') or []
            if new_tier_index >= len(order):
                return False
            ts['current_tier_index'] = new_tier_index
            if ts.get('compromise_fired_at') is None:
                if now is None:
                    now = time.time()
                ts['compromise_fired_at'] = now
            ts['last_advance_reason'] = reason
            return RetryMeta._save_raw(file_path, data)

    @staticmethod
    def mark_season_pack_attempted(file_path):
        """Flip ``season_pack_attempted`` so the pack probe fires only once.

        Returns True if persisted, False if ``tier_state`` is missing.
        """
        with _retry_meta_lock:
            data = RetryMeta._load_raw(file_path)
            ts = RetryMeta._validate_tier_state(data.get('tier_state'))
            if ts is None:
                return False
            ts['season_pack_attempted'] = True
            return RetryMeta._save_raw(file_path, data)


class BlackholeWatcher:
    SUPPORTED_EXTENSIONS = {'.torrent', '.magnet'}

    def __init__(self, watch_dir, debrid_api_key, debrid_service='realdebrid',
                 poll_interval=5, symlink_enabled=False, completed_dir='/completed',
                 rclone_mount='/data', symlink_target_base='', mount_poll_timeout=300,
                 mount_poll_interval=10, symlink_max_age=72,
                 dedup_enabled=False, local_library_tv='', local_library_movies='',
                 debrid_api_keys=None):
        self.watch_dir = watch_dir
        self.debrid_api_key = debrid_api_key
        self.debrid_service = debrid_service
        # Per-grab routing (plan 39 phase 2).  When ``debrid_api_keys`` is
        # provided (typically by the factory at startup_blackhole_watcher),
        # the watcher knows about *all* configured debrids and routes each
        # grab independently.  When None (legacy callers / single-debrid
        # tests), falls back to the single ``debrid_service``+``debrid_api_key``
        # pair — behavior matches pre-plan-39 pd_zurg exactly.
        if debrid_api_keys:
            self.debrid_api_keys = dict(debrid_api_keys)
        else:
            self.debrid_api_keys = {debrid_service: debrid_api_key} if debrid_api_key else {}
        self.poll_interval = poll_interval
        self._stop_event = threading.Event()

        # Local library dedup configuration
        self.dedup_enabled = dedup_enabled
        self.local_library_tv = local_library_tv
        self.local_library_movies = local_library_movies

        # Symlink configuration
        self.symlink_enabled = symlink_enabled
        self.completed_dir = completed_dir
        self.rclone_mount = rclone_mount
        self.symlink_target_base = symlink_target_base
        self.mount_poll_timeout = mount_poll_timeout
        self.mount_poll_interval = mount_poll_interval
        self.symlink_max_age = symlink_max_age

        # Active monitor tracking (prevents duplicate monitors)
        self._active_monitors = set()
        self._monitors_lock = threading.RLock()

        # Audit-driven re-search cooldown so a flaky indexer handing out
        # broken releases for the same (show, season) doesn't produce an
        # unbounded grab-blocklist-regrab loop on Sonarr.
        self._audit_retrigger = {}  # {(title_lower, season): epoch}
        self._audit_retrigger_lock = threading.Lock()
        self._AUDIT_RETRIGGER_COOLDOWN = 7200  # 2 hours
        self._AUDIT_RETRIGGER_MAX_PER_WINDOW = 3  # caps retries within window

        # Dedup for TorBox cached-alternative recovery: one grabbed pack
        # recovers a whole season, so the first grab for an (imdb_id, season)
        # suppresses sibling episode grabs within _TB_ALT_DEDUP_TTL.
        # Mirrored into attempt_ledger (``tbaltdedup:`` keys) so the TTL
        # survives a container restart mid-backfill.
        self._tb_alt_recent_grabs = {}  # {(imdb_id, season): epoch}
        self._tb_alt_grabs_lock = threading.Lock()
        # Persistent give-up cap: after this many cached-alternative grabs for
        # one (imdb_id, season) across scans/restarts, stop re-grabbing (each
        # grab re-arms TorBox's abuse cooldown). Survives restart via the
        # attempt_ledger; the in-memory dedup above only bounds a single burst.
        try:
            self._tb_alt_max_attempts = int(os.environ.get('BLACKHOLE_TB_ALT_MAX_ATTEMPTS', '12'))
        except (ValueError, TypeError):
            self._tb_alt_max_attempts = 12
        if symlink_enabled:
            self._pending_file = os.path.join(completed_dir, 'pending_monitors.json')
        else:
            self._pending_file = os.path.join(watch_dir, 'pending_monitors.json')
        self._last_cleanup = 0

    # ── Per-debrid resolution helpers (plan 39 phase 2) ───────────────

    def _api_key_for(self, debrid):
        """Return the API key for ``debrid``, falling back to instance default.

        Multi-debrid setups populate ``self.debrid_api_keys``; single-debrid
        setups fall through to ``self.debrid_api_key``.  Callers MUST pass
        the resolved key to every provider method instead of touching
        ``self.debrid_api_key`` directly — that's how the single-instance
        watcher manages to host concurrent grabs against different debrids.

        ``getattr(self, 'debrid_api_keys', None)`` is defensive: some tests
        bypass ``__init__`` via ``__new__`` and set only the legacy
        single-debrid attributes, so the new dict may be absent.
        """
        keys = getattr(self, 'debrid_api_keys', None)
        if keys and debrid in keys:
            return keys[debrid]
        # Legacy fallback — only safe when debrid == self.debrid_service.
        if debrid == self.debrid_service:
            return self.debrid_api_key
        return None

    def _mount_for(self, debrid):
        """Return the rclone mount path for ``debrid``.

        Defers to ``utils.debrid_routing.mount_for_debrid`` so the per-debrid
        path contract is in one place (used here, by debrid_health.py
        phase 3, and library.py phase 4).  Falls back to the instance-level
        ``self.rclone_mount`` only when the helper can't resolve a path
        (e.g. RCLONE_MOUNT_NAME unset in a unit test) — that preserves the
        pre-plan-39 contract that the watcher always has a usable mount.
        """
        from utils.debrid_routing import mount_for_debrid
        # ``self.rclone_mount`` can be the bare parent (``/data`` —
        # default; tests set this way) OR the auto-detected leaf
        # (``/data/zurgarr`` — production once RCLONE_MOUNT_NAME +
        # __all__ are present).  ``mount_for_debrid`` wants the PARENT
        # so it can append the per-debrid mount name (torbox / RD/AD
        # suffixed leaves).  Trim the leaf when ``self.rclone_mount``
        # ends with the configured RCLONE_MOUNT_NAME; otherwise use
        # it directly.  Without the trim, TB would land at
        # ``/data/zurgarr/torbox`` — a phantom subdir under the RD
        # mount instead of its own top-level mount.
        base = self.rclone_mount.rstrip('/')
        rclonemn = os.environ.get('RCLONE_MOUNT_NAME') or ''
        if rclonemn and os.path.basename(base) == rclonemn:
            parent = os.path.dirname(base)
            if parent:
                base = parent
        resolved = mount_for_debrid(debrid, rclone_mount_base=base)
        return resolved or self.rclone_mount

    def _symlink_target_base_for(self, debrid):
        """Return the host-side symlink target base for ``debrid``."""
        from utils.debrid_routing import symlink_target_base_for_debrid
        base = symlink_target_base_for_debrid(debrid)
        # Legacy fallback for tests / single-debrid setups where the helper
        # returns '' (e.g. RD only, env vars not loaded).
        return base or self.symlink_target_base

    # 60s TTL on (svc, hash) cache-probe results.  Heavy blackhole
    # traffic (e.g. a Sonarr backfill drop) can otherwise burn through
    # TB's rate budget by re-probing the same hash for every
    # back-to-back grab from the same indexer push.  Mirrors the
    # ``_existing_hashes_cache`` TTL pattern in search.py.
    _PROBE_CACHE_TTL = 60.0

    def _ensure_probe_cache(self):
        """Lazy-init the per-instance probe cache.  Instance-scoped (not
        class-scoped) so test cases and multi-watcher scenarios don't
        bleed cached results across each other.
        """
        if not hasattr(self, '_probe_cache'):
            self._probe_cache = {}
            self._probe_cache_lock = threading.Lock()

    def _route_grab(self, info_hash):
        """Pick the debrid service to host this grab.

        Defers to ``utils.debrid_routing.pick_debrid_for_grab``.  In
        ``cache_aware`` mode, probes each configured debrid via
        ``utils.search.check_debrid_cache`` and prefers a confirmed cache
        hit; single-debrid setups always return that one debrid.  The
        helper handles fallback to the primary when probes are unavailable
        or inconclusive.

        Returns the chosen debrid service name.  Defaults to
        ``self.debrid_service`` when routing yields nothing (defensive —
        ``pick_debrid_for_grab`` returns ``None`` only when no debrid is
        configured at all, which is a startup error elsewhere).
        """
        from utils.debrid_routing import pick_debrid_for_grab

        self._ensure_probe_cache()

        def _probe(svc, h):
            key = self._api_key_for(svc)
            if not key:
                return None
            cache_key = (svc, h.lower())
            now = time.time()
            with self._probe_cache_lock:
                hit = self._probe_cache.get(cache_key)
                if hit and hit[0] > now:
                    return hit[1]
            try:
                from utils.search import check_debrid_cache
                # Use the per-debrid api_key — passing the wrong one would
                # silently return None (auth failure) and bias the routing
                # toward the primary, masking the cache lookup.
                result = check_debrid_cache([h.lower()], service=svc, api_key=key)
                outcome = result.get(h.lower()) if isinstance(result, dict) else None
            except Exception:
                outcome = None
            with self._probe_cache_lock:
                # Opportunistic prune to keep the cache bounded — drop any
                # entries that have expired alongside our insert.  At one
                # probe per grab this stays well under 1000 entries.
                self._probe_cache = {
                    k: v for k, v in self._probe_cache.items() if v[0] > now
                }
                self._probe_cache[cache_key] = (now + self._PROBE_CACHE_TTL, outcome)
            return outcome

        chosen = pick_debrid_for_grab(info_hash, cache_probe=_probe)
        return chosen or self.debrid_service

    # ── Debrid submission methods ────────────────────────────────────

    def _add_to_realdebrid(self, file_path, api_key=None):
        """Add a torrent/magnet to Real-Debrid.

        ``api_key`` defaults to ``self.debrid_api_key`` for back-compat
        with single-debrid callers; phase-2 multi-debrid callers pass the
        resolved per-debrid key explicitly via ``self._api_key_for('realdebrid')``.
        """
        _check_rate_limit('realdebrid')
        api_key = api_key or self.debrid_api_key
        ext = os.path.splitext(file_path)[1].lower()
        headers = {'Authorization': f'Bearer {api_key}'}

        if ext == '.magnet':
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                magnet_link = f.read().strip()
            url = 'https://api.real-debrid.com/rest/1.0/torrents/addMagnet'
            response = tracked_request('realdebrid', requests.post, url, headers=headers, data={'magnet': magnet_link}, timeout=30)
        elif ext == '.torrent':
            url = 'https://api.real-debrid.com/rest/1.0/torrents/addTorrent'
            with open(file_path, 'rb') as f:
                response = tracked_request('realdebrid', requests.put, url,
                                           headers={**headers, 'Content-Type': 'application/x-bittorrent'},
                                           data=f.read(), timeout=30)
        else:
            return False, f'Unsupported extension: {ext}'

        if response.status_code in (200, 201):
            torrent_id = response.json().get('id')
            if not torrent_id:
                return False, 'Real-Debrid response missing torrent id'
            select_url = f'https://api.real-debrid.com/rest/1.0/torrents/selectFiles/{torrent_id}'
            select_resp = tracked_request('realdebrid', requests.post, select_url, headers=headers, data={'files': 'all'}, timeout=30)
            if select_resp.status_code not in (200, 202, 204):
                logger.warning(f"[blackhole] selectFiles failed for {torrent_id}: HTTP {select_resp.status_code}")
            return True, torrent_id
        if _is_rate_limit_response(response):
            _mark_rate_limited('realdebrid')
            return False, 'rate limit exceeded'
        return False, response.text[:200]

    def _add_to_alldebrid(self, file_path, api_key=None):
        """Add a torrent/magnet to AllDebrid."""
        _check_rate_limit('alldebrid')
        api_key = api_key or self.debrid_api_key
        ext = os.path.splitext(file_path)[1].lower()
        params = {'agent': 'zurgarr', 'apikey': api_key}

        if ext == '.magnet':
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                magnet_link = f.read().strip()
            url = 'https://api.alldebrid.com/v4/magnet/upload'
            response = tracked_request('alldebrid', requests.post, url, params=params, data={'magnets[]': magnet_link}, timeout=30)
        elif ext == '.torrent':
            url = 'https://api.alldebrid.com/v4/magnet/upload/file'
            with open(file_path, 'rb') as f:
                response = tracked_request('alldebrid', requests.post, url, params=params, files={'files[]': f}, timeout=30)
        else:
            return False, f'Unsupported extension: {ext}'

        if response.status_code == 200:
            return True, response.json()
        if _is_rate_limit_response(response):
            _mark_rate_limited('alldebrid')
            return False, 'rate limit exceeded'
        return False, response.text[:200]

    def _add_to_torbox(self, file_path, api_key=None):
        """Add a torrent/magnet to TorBox."""
        _check_rate_limit('torbox')
        api_key = api_key or self.debrid_api_key
        ext = os.path.splitext(file_path)[1].lower()
        headers = {'Authorization': f'Bearer {api_key}'}
        url = 'https://api.torbox.app/v1/api/torrents/createtorrent'

        if ext == '.magnet':
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                magnet_link = f.read().strip()
            response = tracked_request('torbox', requests.post, url, headers=headers, data={'magnet': magnet_link}, timeout=30)
        elif ext == '.torrent':
            # TB rejects with BOZO_TORRENT unless the file part carries Content-Type: application/x-bittorrent.
            with open(file_path, 'rb') as f:
                files = {'file': (os.path.basename(file_path), f, 'application/x-bittorrent')}
                response = tracked_request('torbox', requests.post, url, headers=headers, files=files, timeout=30)
        else:
            return False, f'Unsupported extension: {ext}'

        if response.status_code in (200, 201):
            return True, response.json()
        if _is_rate_limit_response(response):
            _mark_rate_limited('torbox')
            return False, 'rate limit exceeded'
        # TB does not surface its account-level cooldown via 429 — instead
        # it returns HTTP 400 ``DOWNLOAD_SERVER_ERROR`` while ``cooldown_until``
        # on /user/me is set.  Probe the cooldown so a quota-exhausted
        # account converts into a precise rate-limit window rather than
        # an open retry loop that wastes every subsequent createtorrent
        # call until the cooldown lifts.
        cooldown_seconds = _check_torbox_cooldown(api_key)
        if cooldown_seconds > 0:
            _mark_rate_limited('torbox', seconds=cooldown_seconds)
            return False, (
                f'TorBox account cooldown active for '
                f'{int(cooldown_seconds)}s — gating subsequent adds'
            )
        return False, response.text[:200]

    # ── Torrent ID extraction ────────────────────────────────────────

    def _extract_torrent_id(self, result, debrid=None):
        """Extract a normalized torrent ID string from the debrid submission result.

        ``debrid`` defaults to ``self.debrid_service`` for back-compat; phase-2
        multi-debrid callers pass the chosen service explicitly so the parsing
        path matches the provider that actually accepted the add.
        """
        debrid = debrid or self.debrid_service
        try:
            if debrid == 'realdebrid':
                return str(result)
            elif debrid == 'alldebrid':
                return str(result['data']['magnets'][0]['id'])
            elif debrid == 'torbox':
                data = result.get('data', {})
                return str(data.get('torrent_id') or data.get('id', ''))
        except (KeyError, IndexError, TypeError) as e:
            logger.warning(f"[blackhole] Could not extract torrent ID from {debrid} response: {e}")
        return None

    # ── Debrid status check methods ──────────────────────────────────

    def _check_realdebrid_status(self, torrent_id, api_key=None):
        """Check torrent status on Real-Debrid. Returns (status, info_dict)."""
        api_key = api_key or self.debrid_api_key
        headers = {'Authorization': f'Bearer {api_key}'}
        url = f'https://api.real-debrid.com/rest/1.0/torrents/info/{torrent_id}'
        response = tracked_request('realdebrid', requests.get, url, headers=headers, timeout=30)
        if response.status_code == 200:
            info = response.json()
            return info.get('status', 'unknown'), info
        if response.status_code == 404:
            logger.warning(f"[blackhole] RD torrent {torrent_id} no longer exists (404)")
            return 'dead', {}  # Treat as terminal so monitor stops immediately
        logger.warning(f"[blackhole] RD status check failed for {torrent_id}: HTTP {response.status_code}")
        return 'api_error', {}

    def _check_alldebrid_status(self, torrent_id, api_key=None):
        """Check torrent status on AllDebrid. Returns (status, info_dict)."""
        api_key = api_key or self.debrid_api_key
        params = {'agent': 'zurgarr', 'apikey': api_key, 'id': torrent_id}
        url = 'https://api.alldebrid.com/v4/magnet/status'
        response = tracked_request('alldebrid', requests.get, url, params=params, timeout=30)
        if response.status_code == 200:
            info = response.json()
            if info.get('status') != 'success':
                logger.warning(f"[blackhole] AD API error for {torrent_id}: {info.get('status')}")
                return 'api_error', info
            try:
                magnet = info['data']['magnets']
                if not isinstance(magnet, dict):
                    return 'unknown', info
                return magnet.get('status', 'unknown'), info
            except (KeyError, TypeError):
                return 'unknown', info
        logger.warning(f"[blackhole] AD status check failed for {torrent_id}: HTTP {response.status_code}")
        return 'api_error', {}

    def _check_torbox_status(self, torrent_id, api_key=None):
        """Check torrent status on TorBox. Returns (status, info_dict)."""
        api_key = api_key or self.debrid_api_key
        headers = {'Authorization': f'Bearer {api_key}'}
        url = 'https://api.torbox.app/v1/api/torrents/mylist'
        params = {'id': torrent_id}
        response = tracked_request('torbox', requests.get, url, headers=headers, params=params, timeout=30)
        if response.status_code == 200:
            info = response.json()
            data = info.get('data')
            if not isinstance(data, dict):
                return 'unknown', info
            return data.get('download_state', 'unknown'), info
        logger.warning(f"[blackhole] TorBox status check failed for {torrent_id}: HTTP {response.status_code}")
        return 'api_error', {}

    def _is_torrent_ready(self, status, debrid=None):
        """Check if the debrid status indicates the torrent is fully downloaded."""
        debrid = debrid or self.debrid_service
        if debrid == 'realdebrid':
            return status == 'downloaded'
        elif debrid == 'alldebrid':
            return status == 'Ready'
        elif debrid == 'torbox':
            return status in TB_READY_STATES
        return False

    def _is_terminal_error(self, status, debrid=None):
        """Check if the debrid status indicates a terminal (unrecoverable) error."""
        debrid = debrid or self.debrid_service
        if debrid == 'realdebrid':
            return status in RD_TERMINAL_ERRORS
        elif debrid == 'alldebrid':
            return status in AD_TERMINAL_ERRORS
        elif debrid == 'torbox':
            return status in TB_TERMINAL_ERRORS
        return False

    def _extract_release_name(self, info, debrid=None):
        """Extract the release/folder name from the debrid torrent info response."""
        debrid = debrid or self.debrid_service
        try:
            if debrid == 'realdebrid':
                return info.get('filename', '')
            elif debrid == 'alldebrid':
                return info['data']['magnets'].get('filename', '')
            elif debrid == 'torbox':
                return info['data'].get('name', '')
        except (KeyError, TypeError):
            pass
        return ''

    def _extract_hash_from_info(self, info, debrid=None):
        """Extract the info hash from a debrid torrent info response."""
        debrid = debrid or self.debrid_service
        try:
            if debrid == 'realdebrid':
                return (info.get('hash') or '').upper()
            elif debrid == 'alldebrid':
                return (info['data']['magnets'].get('hash') or '').upper()
            elif debrid == 'torbox':
                return (info['data'].get('hash') or '').upper()
        except (KeyError, TypeError):
            pass
        return ''

    def _has_usable_media_files(self, info, debrid=None):
        """Check if the debrid torrent contains any files with recognized media extensions.

        Returns True if at least one file matches MEDIA_EXTENSIONS.
        Returns True (assume usable) if file info is unavailable — never reject
        what we can't verify.
        """
        try:
            filenames = self._extract_filenames_from_info(info, debrid=debrid)
        except Exception:
            return True  # Can't verify — assume usable
        if not filenames:
            return True  # No file info available — assume usable
        return any(
            os.path.splitext(f)[1].lower() in MEDIA_EXTENSIONS
            for f in filenames
        )

    def _extract_filenames_from_info(self, info, debrid=None):
        """Extract flat list of filenames from a debrid torrent info response.

        Provider-specific extraction; returns empty list if structure is unexpected.
        """
        debrid = debrid or self.debrid_service
        if debrid == 'realdebrid':
            files = info.get('files')
            if not isinstance(files, list):
                return []
            return [
                os.path.basename(f['path'])
                for f in files
                if f.get('selected') == 1 and f.get('path')
            ]
        elif debrid == 'alldebrid':
            try:
                files = info['data']['magnets']['files']
            except (KeyError, TypeError):
                return []
            if not isinstance(files, list):
                return []
            # AD uses nested structure: 'n' = name, 'e' = children
            result = []
            stack = list(files)
            while stack:
                node = stack.pop()
                if not isinstance(node, dict):
                    continue
                children = node.get('e')
                if isinstance(children, list):
                    stack.extend(children)
                elif node.get('n'):
                    result.append(node['n'])
            return result
        elif debrid == 'torbox':
            try:
                files = info['data']['files']
            except (KeyError, TypeError):
                return []
            if not isinstance(files, list):
                return []
            return [f['name'] for f in files if f.get('name')]
        return []

    # ── Mount scanning ───────────────────────────────────────────────

    def _find_on_mount(self, release_name, debrid=None, file_names=None):
        """Search the rclone mount for a release folder.

        Returns (full_path, category, matched_name) or (None, None, None) if not found.

        Zurg-backed mounts (RD/AD) are categorized: probes
        ``shows/movies/anime`` first, then ``__all__`` as fallback.
        TorBox's WebDAV mount is flat — releases land directly under the
        mount root with no category subdivision — so for TB the returned
        ``category`` is ``''`` (passed through to ``_create_symlinks``
        which builds host-side targets as ``<TB target base>/<release>/``
        with no category segment).

        Also tries stripping a trailing media file extension (Zurg strips
        it from single-file torrent folder names), and for TB additionally
        tries stripping a leading ``[indexer.to] `` prefix that the
        scraper sometimes adds to TB's API ``data.name`` while the
        actual mount folder has the bare name. A final listdir-walk
        fuzzy match using ``norm_for_matching`` catches further
        normalization drift between TB's API and its WebDAV layout.

        ``debrid`` (plan 39 phase 2) selects which mount to search.  Defaults
        to ``self.debrid_service`` for back-compat with single-debrid callers.
        TorBox content lives at the TB mount; RD/AD content lives at the
        Zurg mount.  The two are NEVER cross-searched — if a torrent was
        added to TB, only the TB mount can possibly have it.
        """
        debrid = debrid or self.debrid_service
        mount_path = self._mount_for(debrid)

        if debrid == 'torbox':
            # Plan 41 phase B.3 — single source of truth for TB folder-name
            # candidates lives in ``utils.debrid_routing``.  ``file_names``
            # bridges the indexer-display-title vs WebDAV-folder gap for
            # non-English trackers (the API name is the indexer's native-
            # language title; TB stores under the .torrent's info.name,
            # which surfaces as the first path segment of each file in
            # the API's data.files[].name list).
            from utils.debrid_routing import build_tb_lookup_candidates
            candidates = build_tb_lookup_candidates(release_name, file_names=file_names)
            return self._find_on_torbox_mount(mount_path, release_name, candidates)

        # Try both the original name and with video extension stripped
        candidates = [release_name]
        base, ext = os.path.splitext(release_name)
        if ext.lower() in MEDIA_EXTENSIONS and base:
            candidates.append(base)

        for name in candidates:
            if not _is_safe_mount_name(name):
                logger.warning(f"[blackhole] Rejecting unsafe mount candidate: {name!r}")
                continue
            for category in MOUNT_CATEGORIES:
                path = os.path.join(mount_path, category, name)
                if os.path.isdir(path):
                    return path, category, name
            # Fallback to __all__
            path = os.path.join(mount_path, '__all__', name)
            if os.path.isdir(path):
                return path, '__all__', name
        return None, None, None

    def _find_on_torbox_mount(self, mount_path, release_name, candidates):
        """Locate *release_name* on the flat TorBox WebDAV mount.

        Returns ``(full_path, '', matched_name)`` on hit, otherwise
        ``(None, None, None)``. ``category`` is the empty string because
        the TB mount has no category subdivision — see ``_find_on_mount``.

        Two-step search:
        1. Exact-path probes for each candidate (cheapest path; covers
           the common case where TB's API name matches the folder name).
        2. ``os.listdir`` + fuzzy compare via ``norm_for_matching`` —
           catches indexer-tag variations that the leading-bracket
           regex doesn't (trailing tags, embedded brackets) and other
           drift between TB's API ``data.name`` and the actual folder
           it wrote to WebDAV. Only returns a hit when exactly one
           folder matches — refuses to guess between duplicates.
        """
        for name in candidates:
            if not _is_safe_mount_name(name):
                logger.warning(f"[blackhole] Rejecting unsafe TB mount candidate: {name!r}")
                continue
            path = os.path.join(mount_path, name)
            if os.path.isdir(path):
                return path, '', name

        try:
            from utils.library import norm_for_matching
        except Exception:
            return None, None, None

        target_fuzzy = norm_for_matching(_strip_indexer_prefix(release_name) or release_name)
        if not target_fuzzy:
            return None, None, None

        try:
            entries = os.listdir(mount_path)
        except OSError as e:
            logger.debug(f"[blackhole] Cannot list TB mount {mount_path}: {e}")
            return None, None, None

        matches = []
        for entry in entries:
            if not _is_safe_mount_name(entry):
                # Well-behaved FUSE never returns this; defense-in-depth.
                continue
            entry_path = os.path.join(mount_path, entry)
            if not os.path.isdir(entry_path):
                continue
            entry_fuzzy = norm_for_matching(_strip_indexer_prefix(entry) or entry)
            if entry_fuzzy == target_fuzzy:
                matches.append((entry_path, entry))

        if len(matches) == 1:
            path, name = matches[0]
            logger.info(
                f"[blackhole] TB fuzzy mount match: API name {release_name!r} "
                f"→ folder {name!r}"
            )
            return path, '', name
        if len(matches) > 1:
            logger.warning(
                f"[blackhole] Multiple TB mount candidates for {release_name!r}: "
                f"{[m[1] for m in matches]} — refusing to guess"
            )
        return None, None, None

    # ── Symlink creation ─────────────────────────────────────────────

    def _completed_base(self, label):
        """Return the base output directory, prefixed by *label* when set.

        With label="sonarr" → /completed/sonarr
        With label=None     → /completed  (flat-mode, backward compatible)
        """
        if label:
            return os.path.join(self.completed_dir, label)
        return self.completed_dir

    def _failed_dir(self, label):
        """Return the failed/ staging dir for *label* (or flat failed/ if None)."""
        base = os.path.join(self.watch_dir, 'failed')
        if label:
            return os.path.join(base, label)
        return base

    def _alt_pending_dir(self, label):
        """Return the .alt_pending/ staging dir for *label* (or flat if None)."""
        base = os.path.join(self.watch_dir, '.alt_pending')
        if label:
            return os.path.join(base, label)
        return base

    def _create_symlinks(self, release_name, category, mount_path, label=None, debrid=None):
        """Create symlinks in the completed directory for media files.

        Symlink targets use the per-debrid base resolved via
        ``_symlink_target_base_for`` so RD content points at
        ``BLACKHOLE_SYMLINK_TARGET_BASE`` and TorBox content points at
        ``BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX`` — Plex sees them as
        separate libraries (plan 39 Q1).

        For multi-season packs, splits files into per-season directories
        with constructed release names that Sonarr can parse individually.

        When *label* is set, output is nested under ``completed_dir/<label>/``
        so each arr only sees its own items (see ``.plans/31-blackhole-per-arr-routing.md``).

        Returns the number of symlinks created.
        """
        debrid = debrid or self.debrid_service
        if not _is_safe_mount_name(release_name):
            logger.error(f"[blackhole] Refusing symlink creation for unsafe release_name: {release_name!r}")
            return 0
        is_multi, _, _ = _is_multi_season_pack(release_name)

        if is_multi:
            split_count = self._create_split_season_symlinks(release_name, category, mount_path, label=label, debrid=debrid)
            if split_count is not None:
                return split_count
            logger.debug(f"[blackhole] Could not split {release_name} by season, using single dir")

        # Single-dir logic (original behavior, now label-aware)
        completed_base = self._completed_base(label)
        completed_release_dir = os.path.normpath(os.path.join(completed_base, release_name))
        completed_real = os.path.normpath(completed_base)
        # Defense-in-depth: even with _is_safe_mount_name above, normpath the
        # full join and verify the result stays under completed_base. Matches
        # the guard the split-season branch has at the season_dir level.
        if not completed_release_dir.startswith(completed_real + os.sep) and completed_release_dir != completed_real:
            logger.error(f"[blackhole] Refusing path-traversal release dir: {completed_release_dir}")
            return 0
        os.makedirs(completed_release_dir, exist_ok=True)
        symlink_target_base = self._symlink_target_base_for(debrid)
        count = 0

        for root, _dirs, files in os.walk(mount_path):
            for f in files:
                ext = os.path.splitext(f)[1].lower()
                if ext not in MEDIA_EXTENSIONS:
                    continue
                if 'sample' in f.lower():
                    continue

                rel = os.path.relpath(os.path.join(root, f), mount_path)
                symlink_path = os.path.normpath(os.path.join(completed_release_dir, rel))
                target = os.path.join(symlink_target_base, category, release_name, rel)

                # Guard against path traversal from adversarial release names
                if not symlink_path.startswith(completed_release_dir + os.sep):
                    logger.warning(f"[blackhole] Skipping path traversal attempt: {rel}")
                    continue

                os.makedirs(os.path.dirname(symlink_path), exist_ok=True)

                if os.path.islink(symlink_path) or os.path.exists(symlink_path):
                    logger.debug(f"[blackhole] Symlink already exists: {symlink_path}")
                    continue

                try:
                    os.symlink(target, symlink_path)
                    logger.info(f"[blackhole] Symlink: {rel} -> {target}")
                    count += 1
                except OSError as e:
                    logger.error(f"[blackhole] Failed to create symlink {symlink_path}: {e}")

        return count

    def _create_split_season_symlinks(self, release_name, category, mount_path, label=None, debrid=None):
        """Split a multi-season pack into per-season symlink directories.

        Groups media files by season, creates a separate completed directory
        for each season with a constructed release name, and returns the
        total number of symlinks created. Returns None if fewer than 2 seasons
        are detected (caller should fall back to single-dir).

        When *label* is set, season dirs are nested under ``completed_dir/<label>/``.
        ``debrid`` (plan 39 phase 2) selects the symlink target base.
        """
        debrid = debrid or self.debrid_service
        symlink_target_base = self._symlink_target_base_for(debrid)
        season_files = {}

        for root, _dirs, files in os.walk(mount_path):
            for f in files:
                ext = os.path.splitext(f)[1].lower()
                if ext not in MEDIA_EXTENSIONS:
                    continue
                if 'sample' in f.lower():
                    continue

                rel = os.path.relpath(os.path.join(root, f), mount_path)
                season = _extract_file_season(rel)
                if season is None:
                    logger.warning(f"[blackhole] Cannot determine season for '{f}' in multi-season pack {release_name}, skipping")
                    continue

                season_files.setdefault(season, []).append(rel)

        if len(season_files) < 2:
            return None

        count = 0
        completed_base = self._completed_base(label)
        completed_real = os.path.normpath(completed_base)
        logger.info(f"[blackhole] Multi-season pack: {release_name} → splitting into {len(season_files)} seasons")

        for season_num, rel_list in sorted(season_files.items()):
            season_name = _build_season_release_name(release_name, season_num)
            season_dir = os.path.normpath(os.path.join(completed_base, season_name))

            # Guard against path traversal in the constructed season dir name
            if not season_dir.startswith(completed_real + os.sep):
                logger.warning(f"[blackhole] Skipping path traversal in season name: {season_name}")
                continue

            os.makedirs(season_dir, exist_ok=True)

            for rel in rel_list:
                symlink_path = os.path.normpath(os.path.join(season_dir, rel))
                target = os.path.join(symlink_target_base, category, release_name, rel)

                if not symlink_path.startswith(season_dir + os.sep):
                    logger.warning(f"[blackhole] Skipping path traversal attempt: {rel}")
                    continue

                os.makedirs(os.path.dirname(symlink_path), exist_ok=True)

                if os.path.islink(symlink_path) or os.path.exists(symlink_path):
                    logger.debug(f"[blackhole] Symlink already exists: {symlink_path}")
                    continue

                try:
                    os.symlink(target, symlink_path)
                    logger.info(f"[blackhole] Symlink (S{season_num:02d}): {rel} -> {target}")
                    count += 1
                except OSError as e:
                    logger.error(f"[blackhole] Failed to create symlink {symlink_path}: {e}")

            logger.info(f"[blackhole]   Season {season_num:02d}: {len(rel_list)} file(s) → {season_name}")

        return count

    # ── Post-grab completeness audit ─────────────────────────────────

    def _audit_retrigger_recent(self, title, season):
        """Return True when an audit-driven re-search for ``(title, season)``
        has fired more than ``_AUDIT_RETRIGGER_MAX_PER_WINDOW`` times within
        ``_AUDIT_RETRIGGER_COOLDOWN`` seconds.  Bounds the grab-blocklist-regrab
        loop a flaky indexer could otherwise sustain indefinitely.
        """
        key = (title.lower(), int(season))
        now = time.time()
        with self._audit_retrigger_lock:
            entries = self._audit_retrigger.get(key, [])
            entries = [t for t in entries if now - t < self._AUDIT_RETRIGGER_COOLDOWN]
            self._audit_retrigger[key] = entries
            if len(entries) >= self._AUDIT_RETRIGGER_MAX_PER_WINDOW:
                logger.info(
                    f"[blackhole] Audit retry limit reached for {title} S{season:02d} "
                    f"({len(entries)} attempts in last "
                    f"{self._AUDIT_RETRIGGER_COOLDOWN//60}m) — backing off"
                )
                return True
            return False

    def _audit_retrigger_mark(self, title, season):
        """Record a successful audit-driven re-search trigger against
        ``(title, season)`` so ``_audit_retrigger_recent`` can enforce the
        per-window cap on subsequent attempts."""
        key = (title.lower(), int(season))
        with self._audit_retrigger_lock:
            self._audit_retrigger.setdefault(key, []).append(time.time())

    def _audit_release_completeness(self, filename, release_name, mount_path, info):
        """Verify a TV release actually delivered every claimed episode.

        Only audits episode-level releases — ``_parse_episodes`` returns an
        empty set for season packs, and we have no TMDB ground truth at this
        layer to diff against.  The library-scan reconcile catches pack gaps
        on the next cycle.

        On short delivery: blocklist the release hash so the same bad release
        isn't re-grabbed, log a ``release_incomplete`` history event, and
        trigger a force-grab re-search for the still-missing episodes.

        Best-effort: any failure in the re-search path is swallowed (the
        blocklist + history are what matters).  Partially-delivered episodes
        are NOT un-symlinked — partial playback beats nothing.
        """
        claimed = _parse_episodes(filename)
        if not claimed:
            return  # pack or unparseable — rely on library reconcile

        delivered = set()
        try:
            for root, _dirs, files in os.walk(mount_path):
                for f in files:
                    ext = os.path.splitext(f)[1].lower()
                    if ext not in MEDIA_EXTENSIONS:
                        continue
                    if 'sample' in f.lower():
                        continue
                    delivered |= _parse_episodes(f)
        except OSError as e:
            logger.debug(f"[blackhole] Completeness audit: walk failed for {release_name}: {e}")
            return

        missing = sorted(claimed - delivered)
        if not missing:
            return

        info_hash = ''
        try:
            info_hash = self._extract_hash_from_info(info) or ''
        except Exception:
            pass

        logger.warning(
            f"[blackhole] Incomplete release {release_name}: claimed "
            f"{sorted(claimed)}, delivered {sorted(delivered)}, missing {missing}"
        )

        if _blocklist and info_hash and str(os.environ.get('BLOCKLIST_AUTO_ADD', 'true')).lower() == 'true':
            try:
                _blocklist.add(
                    info_hash, filename,
                    reason=f'incomplete release: missing episodes {missing}',
                    source='auto',
                )
            except Exception as e:
                logger.debug(f"[blackhole] Blocklist add failed: {e}")

        if _history:
            try:
                mt, ep = _enrich_for_history(filename)
                _history.log_event(
                    'release_incomplete', filename, episode=ep, source='blackhole',
                    detail=f'Missing episodes: {missing}',
                    meta={
                        'cause': 'incomplete_release',
                        'info_hash': info_hash,
                        'claimed': sorted(claimed),
                        'delivered': sorted(delivered),
                        'missing': missing,
                    },
                    media_title=mt,
                )
            except Exception as e:
                logger.debug(f"[blackhole] History log failed: {e}")

        try:
            title, season, is_tv = parse_release_name(filename)
            if is_tv and season is not None and title:
                # Audit-driven re-search uses a find-only path: if Sonarr
                # doesn't already track the series, we do NOT add it here.
                # The filename-parsed title drops the disambiguation year
                # (parse_release_name strips it), so handing `tmdb_id=None`
                # straight into ensure_and_search can miss a year-qualified
                # series ("Lucky Hank (2023)") and fall through to add_series,
                # creating a duplicate.  Resolve TMDB first, and only proceed
                # when the series is already in the library.
                if self._audit_retrigger_recent(title, season):
                    return
                from utils.arr_client import get_download_service
                from utils.tmdb import search_show as tmdb_search_show
                client, svc = get_download_service('show')
                if not client or svc != 'sonarr':
                    return
                tmdb_id = None
                try:
                    hit = tmdb_search_show(title)
                    if hit:
                        tmdb_id = hit.get('tmdb_id')
                except Exception:
                    pass
                if not client.find_series_in_library(tmdb_id=tmdb_id, title=title):
                    logger.info(
                        f"[blackhole] Audit: series {title!r} not in Sonarr — "
                        f"skipping re-search to avoid adding a duplicate"
                    )
                    return
                self._audit_retrigger_mark(title, season)
                client.ensure_and_search(
                    title, tmdb_id, season, missing,
                    prefer_debrid=True, respect_monitored=True,
                )
        except Exception as e:
            logger.warning(
                f"[blackhole] Re-search failed for missing episodes of {release_name}: {e}"
            )

    # ── Symlink cleanup ──────────────────────────────────────────────

    def _cleanup_symlinks(self):
        """Remove broken symlinks and aged-out directories from the completed dir.

        Handles both flat (``completed_dir/<release>``) and labeled
        (``completed_dir/<label>/<release>``) layouts via ``iter_release_dirs``.
        Empty label dirs left behind after all their releases are cleaned up
        are removed as well, but the top-level ``completed_dir`` itself is
        never removed.

        Multi-debrid (plan 39 phase 2): symlinks for TB-routed grabs point
        at a different ``BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX`` and resolve
        through a different rclone mount.  The translation table is built
        from every configured debrid; without it, a TB-target symlink
        would skip the prefix match and fall through to the raw-path
        ``os.path.exists`` check (which always succeeds inside the
        container — the host's TB mount isn't bound to the pd_zurg
        container), so broken TB symlinks would accumulate as ghosts.
        """
        if not self.symlink_enabled or not self.completed_dir:
            return
        if not os.path.exists(self.completed_dir):
            return

        now = time.time()
        max_age_secs = self.symlink_max_age * 3600

        # Build (target_base_real, rclone_real) translation table for
        # every configured debrid.  Each iteration of the loop tries
        # the prefix match in order; first hit wins.  Falls back to the
        # instance defaults when the routing helpers don't surface a
        # path (unit tests / single-debrid setups).
        translations = []  # [(target_base_real_with_slash, rclone_real)]
        try:
            seen_bases = set()
            # ``debrid_api_keys`` is always a dict (set in __init__); empty
            # is normal for symlink-disabled tests and the for-loop handles
            # that fine — the instance-default pair below is the safety net.
            for svc in self.debrid_api_keys:
                base = self._symlink_target_base_for(svc)
                if not base or base in seen_bases:
                    continue
                seen_bases.add(base)
                try:
                    base_real = os.path.realpath(base) + '/'
                    mount = self._mount_for(svc)
                    mount_real = os.path.realpath(mount) if mount else os.path.realpath(self.rclone_mount)
                except OSError:
                    continue
                translations.append((base_real, mount_real))
            # Always include the instance-default pair as a fallback so
            # legacy single-debrid callers (no debrid_api_keys populated)
            # still match.
            if self.symlink_target_base:
                try:
                    default_base_real = os.path.realpath(self.symlink_target_base) + '/'
                    default_mount_real = os.path.realpath(self.rclone_mount)
                    if default_base_real not in {t[0] for t in translations}:
                        translations.append((default_base_real, default_mount_real))
                except OSError:
                    pass
        except Exception as e:
            logger.debug(f"[blackhole] cleanup translation table failed: {e}")
            translations = []
            if self.symlink_target_base:
                try:
                    translations.append((
                        os.path.realpath(self.symlink_target_base) + '/',
                        os.path.realpath(self.rclone_mount),
                    ))
                except OSError:
                    pass

        cleaned_label_parents = set()

        for label, release_name, entry_path in iter_release_dirs(self.completed_dir):
            # Remove broken symlinks within this release dir.
            # Symlinks point to SYMLINK_TARGET_BASE which only exists in
            # Sonarr/Radarr's container — translate to the rclone mount
            # before checking existence.
            has_valid = False
            for root, _dirs, files in os.walk(entry_path):
                for f in files:
                    fp = os.path.join(root, f)
                    if os.path.islink(fp):
                        target = os.readlink(fp)
                        if not os.path.isabs(target):
                            target = os.path.realpath(os.path.join(os.path.dirname(fp), target))
                        check_target = fp
                        for base_real, mount_real in translations:
                            if target.startswith(base_real):
                                check_target = mount_real + '/' + target[len(base_real):]
                                break
                        if not os.path.exists(check_target):
                            try:
                                os.unlink(fp)
                                logger.debug(f"[blackhole] Removed broken symlink: {fp}")
                            except OSError:
                                pass
                        else:
                            has_valid = True

            # Remove dir if no valid files remain or if aged out
            try:
                mtime = os.path.getmtime(entry_path)
            except OSError:
                continue

            should_remove = not has_valid
            if max_age_secs > 0 and (now - mtime) > max_age_secs:
                should_remove = True

            if should_remove:
                try:
                    shutil.rmtree(entry_path, ignore_errors=True)
                    display = f"{label}/{release_name}" if label else release_name
                    logger.info(f"[blackhole] Cleaned up completed dir: {display}")
                    if label:
                        cleaned_label_parents.add(os.path.join(self.completed_dir, label))
                except Exception as e:
                    logger.debug(f"[blackhole] Failed to clean up {entry_path}: {e}")

        # Remove now-empty label dirs. The top-level completed_dir is never
        # in cleaned_label_parents by construction.
        for parent in cleaned_label_parents:
            try:
                if os.path.isdir(parent) and not os.listdir(parent):
                    os.rmdir(parent)
                    logger.debug(f"[blackhole] Removed empty label dir: {parent}")
            except OSError:
                pass

    # ── Pending monitor persistence ──────────────────────────────────

    def _load_pending(self):
        """Load pending monitor entries from disk."""
        if not os.path.exists(self._pending_file):
            return []
        try:
            with open(self._pending_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return []

    def _save_pending(self, entries):
        """Save pending monitor entries to disk atomically."""
        try:
            with atomic_write(self._pending_file) as f:
                json.dump(entries, f)
        except (IOError, OSError) as e:
            logger.debug(f"[blackhole] Could not write pending monitors: {e}")

    def _add_pending(self, torrent_id, filename, label=None, compromise=None, debrid=None):
        """Add a torrent to the pending monitors file.

        *compromise* is an optional dict annotating this grab as a
        quality-compromise result: ``{preferred_tier, grabbed_tier,
        reason, strategy}`` where strategy is ``'tier_drop'`` or
        ``'season_pack'``.  Legacy entries without this field load as
        uncompromised per the plan-33 schema additions.

        *debrid* (plan 39 phase 2) records which debrid hosts this
        torrent so the resume path on restart can re-bind the monitor
        to the correct provider + mount.  Defaults to the instance
        debrid for legacy callers; multi-debrid grabs pass it explicitly.
        Legacy on-disk entries without a ``debrid`` key load as
        ``realdebrid`` (the only pre-plan-39 option for new grabs).
        """
        with self._monitors_lock:
            entries = self._load_pending()
            if any(e['torrent_id'] == torrent_id for e in entries):
                return
            entry_debrid = debrid or self.debrid_service
            entry = {
                'torrent_id': torrent_id,
                'filename': filename,
                'service': entry_debrid,
                'debrid': entry_debrid,
                'timestamp': time.time(),
            }
            # Persist label alongside the torrent so restart/resume keeps routing
            if label is not None:
                entry['label'] = label
            if compromise:
                entry['compromised'] = True
                entry['preferred_tier'] = compromise.get('preferred_tier')
                entry['grabbed_tier'] = compromise.get('grabbed_tier')
                entry['compromise_reason'] = compromise.get('reason')
                entry['compromise_strategy'] = compromise.get('strategy')
            entries.append(entry)
            self._save_pending(entries)

    def _remove_pending(self, torrent_id):
        """Remove a torrent from the pending monitors file."""
        with self._monitors_lock:
            entries = self._load_pending()
            entries = [e for e in entries if e['torrent_id'] != torrent_id]
            self._save_pending(entries)
            self._active_monitors.discard(torrent_id)

    # ── Monitor orchestration ────────────────────────────────────────

    def _start_monitor(self, torrent_id, filename, label=None, compromise=None, debrid=None):
        """Spawn a background thread to monitor a torrent and create symlinks.

        *compromise* is an optional dict forwarded to ``_add_pending`` so
        the on-disk pending entry records the compromise lineage; see
        ``_add_pending`` for the expected keys.

        *debrid* (plan 39 phase 2) binds the monitor thread to a specific
        provider for its lifetime.  Defaults to the instance debrid for
        single-debrid setups + legacy callers.
        """
        debrid = debrid or self.debrid_service
        with self._monitors_lock:
            if torrent_id in self._active_monitors:
                logger.debug(f"[blackhole] Already monitoring torrent {torrent_id}")
                return
            self._active_monitors.add(torrent_id)

        self._add_pending(torrent_id, filename, label=label, compromise=compromise, debrid=debrid)
        t = threading.Thread(
            target=self._monitor_and_symlink,
            args=(torrent_id, filename, label, debrid),
            daemon=True,
        )
        t.start()
        tag = f" [label={label}]" if label else ""
        provider_tag = f" via {debrid}" if debrid != self.debrid_service else ""
        logger.info(f"[blackhole] Monitoring torrent {torrent_id} for {filename}{tag}{provider_tag}")

    def _register_scanner_handoff(self, filename):
        """Hand a confirmed-ready grab off to the library scanner.

        Called when a torrent was confirmed added + ready on the debrid but
        the content did not surface on the rclone mount within
        ``mount_poll_timeout`` (common under TorBox 429 rate-limiting — the
        torrent IS permanently in the account and surfaces shortly after).
        Rather than treating that as a hard failure, we record a 'to-debrid'
        pending entry keyed by the canonical title so the library scanner
        resolves it on a later pass: ``_create_debrid_symlinks`` creates the
        symlink once the content surfaces, ``_clear_resolved_pending`` clears
        the entry, and ``_escalate_stuck_pending`` marks it debrid-unavailable
        if it never does.

        Returns True iff a pending entry was registered. Packs whose episode
        list can't be represented as a single ``(season, episodes)`` entry
        return False — single-season packs (no parseable episodes), multi- and
        cross-season packs (``S01-S05`` / ``S01E01-S02E10`` / ``Complete
        Series``), and unresolvable titles. A partial or sentinel episode list
        would never match the scanner's source map and would falsely escalate
        to debrid-unavailable even after the symlinks are created; the
        scanner's unconditional symlinking still recovers that content, just
        without the escalation safety-net.
        """
        try:
            from utils.library import normalize_title
            from utils.library_prefs import set_pending
        except Exception as e:
            logger.debug(f"[blackhole] scanner hand-off imports failed: {e}")
            return False

        media_title, _ep = _enrich_for_history(filename)
        if not media_title:
            logger.debug(f"[blackhole] Hand-off skipped — unresolved title: {filename}")
            return False
        norm = normalize_title(media_title)
        if not norm:
            logger.debug(f"[blackhole] Hand-off skipped — empty norm title: {filename}")
            return False

        _name, season, is_tv = parse_release_name(filename)

        # Multi- and cross-season packs (S01-S05, S01E01-S02E10, Complete
        # Series) can't be represented as one (season, episodes) entry, and a
        # partial/sentinel list would falsely escalate to debrid-unavailable
        # even after the scanner symlinks the content. Note: a multi-season
        # pack with no single-season marker parses as is_tv=False, so this
        # check MUST run before the movie branch — otherwise it would register
        # a bogus movie (0,0) entry under a show's title. Detection runs on the
        # de-extensioned filename (the season range lives in the full release
        # name, which parse_release_name strips out of _name).
        base = re.sub(r'\.(torrent|magnet)$', '', filename, flags=re.IGNORECASE)
        is_multi_pack, _ss, _se = _is_multi_season_pack(base)
        if is_multi_pack:
            logger.debug(f"[blackhole] Hand-off skipped — multi/cross-season pack: {filename}")
            return False

        if is_tv:
            if season is None:
                logger.debug(f"[blackhole] Hand-off skipped — TV with no season: {filename}")
                return False
            eps = _parse_episodes(filename)
            if not eps:
                logger.debug(f"[blackhole] Hand-off skipped — season pack: {filename}")
                return False  # season pack / unparseable — see docstring
            episodes = [{'season': season, 'episode': e} for e in sorted(eps)]
        else:
            episodes = [{'season': 0, 'episode': 0}]

        try:
            set_pending(norm, episodes, direction='to-debrid')
        except Exception as e:
            logger.warning(
                f"[blackhole] Could not register scanner hand-off for {filename}: {e}"
            )
            return False
        return True

    def _monitor_and_symlink(self, torrent_id, filename, label=None, debrid=None):
        """Background thread: poll debrid status, wait for mount, create symlinks.

        This method runs in its own thread and must not block the main scan loop.
        *label* is the per-arr routing label (e.g. "sonarr"); None means flat mode.

        *debrid* (plan 39 phase 2) binds the thread to a specific provider for
        its lifetime — used by every status check, info extraction, and
        symlink-creation call below.  Defaults to the instance debrid for
        single-debrid setups and legacy callers.
        """
        debrid = debrid or self.debrid_service
        api_key = self._api_key_for(debrid)
        status_dispatch = {
            'realdebrid': self._check_realdebrid_status,
            'alldebrid': self._check_alldebrid_status,
            'torbox': self._check_torbox_status,
        }
        check_status = status_dispatch.get(debrid)
        if not check_status:
            logger.error(f"[blackhole] No status checker for {debrid}")
            self._remove_pending(torrent_id)
            return

        # Phase 1: Wait for debrid to finish downloading
        start_time = time.time()
        release_name = None
        info = {}

        while not self._stop_event.is_set():
            elapsed = time.time() - start_time
            if elapsed > self.mount_poll_timeout:
                logger.warning(f"[blackhole] Timeout waiting for debrid to process {filename} "
                               f"(torrent {torrent_id}, {elapsed:.0f}s)")
                try:
                    from utils.metrics import metrics
                    metrics.inc('blackhole_torrent_timeout')
                except Exception:
                    pass
                # Opt-in cleanup: on timeout, actively delete the still-uncached
                # torrent from the debrid account so it doesn't sit there as
                # a 0%/0-seed entry accumulating over time.  Mirrors the disc
                # rip cleanup pattern below — same per-service client lookup,
                # same guards.
                deleted_from_debrid = False
                if str(os.environ.get('BLACKHOLE_DELETE_UNCACHED_ON_TIMEOUT',
                                       'false')).lower() == 'true':
                    try:
                        from utils.debrid_client import get_debrid_client
                        # Route through the grab's debrid — NOT the priority
                        # default — so an AD/TB torrent ID never leaks into an
                        # RD client (shared-hex ID collision would silently
                        # delete an unrelated torrent).
                        client, _svc = get_debrid_client(
                            service=debrid,
                            api_key=api_key,
                        )
                        if client:
                            client.delete_torrent(str(torrent_id))
                            deleted_from_debrid = True
                            logger.info(
                                f"[blackhole] Deleted uncached torrent {torrent_id} "
                                f"from debrid ({debrid}) on timeout ({filename})"
                            )
                    except Exception as e:
                        msg = str(e)
                        if api_key and api_key in msg:
                            msg = msg.replace(api_key, '***')
                        logger.debug(
                            f"[blackhole] Failed to delete timed-out torrent "
                            f"{torrent_id} from debrid: {msg}"
                        )
                # History fires regardless of cleanup opt-in — the timeout
                # happened either way and the audit trail must reflect it.
                # ``meta.deleted`` distinguishes the two outcomes (cleanup
                # ran vs. user opted out) for post-hoc analysis.  Enrich
                # unconditionally to mirror the disc-rip path below; the
                # ``_history``/``_blocklist`` consumers below each have
                # their own truthy guard.
                _mt, _ep = _enrich_for_history(filename)
                if _history:
                    _detail = ('Timed out uncached — removed from debrid'
                               if deleted_from_debrid
                               else 'Timed out uncached — debrid cleanup skipped')
                    _history.log_event(
                        'failed', filename, episode=_ep, source='blackhole',
                        detail=_detail,
                        meta={'cause': 'uncached_timeout',
                              'torrent_id': str(torrent_id),
                              'deleted': deleted_from_debrid,
                              'provider': debrid},
                        media_title=_mt,
                    )
                # Auto-blocklist the hash so the same dead-swarm release
                # isn't re-grabbed on every wanted-search cycle.  Mirrors
                # the terminal_error path below — same env gate, same
                # source, same history shape.  Hash comes from the most
                # recent successful check_status; empty hash (every poll
                # raised before timeout) silently skips the add.
                if _blocklist and str(os.environ.get('BLOCKLIST_AUTO_ADD', 'true')).lower() == 'true':
                    bl_hash = self._extract_hash_from_info(info, debrid=debrid)
                    if bl_hash:
                        _blocklist.add(bl_hash, filename,
                                       reason='Uncached on debrid (timed out)',
                                       source='auto')
                        if _history:
                            _history.log_event('blocklist_added', filename, episode=_ep,
                                               source='blackhole',
                                               detail='Auto-blocklisted: uncached timeout',
                                               meta={'cause': 'auto_blocklist_added',
                                                     'blocklist_reason': 'uncached_timeout',
                                                     'info_hash': bl_hash},
                                               media_title=_mt)
                if _notify:
                    _notify('download_error', 'Blackhole: Torrent Timeout',
                            f'{filename} timed out waiting for debrid processing',
                            level='warning')
                self._remove_pending(torrent_id)
                return

            try:
                status, info = check_status(torrent_id, api_key=api_key)
            except Exception as e:
                logger.warning(f"[blackhole] Error checking status for {torrent_id}: {e}")
                self._stop_event.wait(self.mount_poll_interval)
                continue

            if self._is_torrent_ready(status, debrid=debrid):
                release_name = self._extract_release_name(info, debrid=debrid)
                logger.info(f"[blackhole] Torrent ready: {filename} (release: {release_name}, via {debrid})")
                # Disc rip detection: check debrid file list before mount wait
                if not self._has_usable_media_files(info, debrid=debrid):
                    logger.warning(f"[blackhole] No recognized media files in {filename} — "
                                   f"auto-blocklisting and removing from debrid.")
                    _mt, _ep = _enrich_for_history(filename) if _history else (None, None)
                    if _blocklist and str(os.environ.get('BLOCKLIST_AUTO_ADD', 'true')).lower() == 'true':
                        bl_hash = self._extract_hash_from_info(info, debrid=debrid)
                        if bl_hash:
                            _blocklist.add(bl_hash, filename, reason='disc rip (no usable media files)', source='auto')
                            if _history:
                                _history.log_event('blocklist_added', filename, episode=_ep, source='blackhole',
                                                   detail='Auto-blocklisted: disc rip',
                                                   meta={'cause': 'auto_blocklist_added',
                                                         'blocklist_reason': 'disc rip',
                                                         'info_hash': bl_hash},
                                                   media_title=_mt)
                    try:
                        from utils.debrid_client import get_debrid_client
                        # Route through the grab's debrid — see the
                        # timeout-delete block above for the cross-provider
                        # hazard this avoids.
                        client, _svc = get_debrid_client(
                            service=debrid,
                            api_key=api_key,
                        )
                        if client:
                            client.delete_torrent(str(torrent_id))
                    except Exception as e:
                        msg = str(e)
                        if api_key and api_key in msg:
                            msg = msg.replace(api_key, '***')
                        logger.debug(f"[blackhole] Failed to delete disc rip from debrid: {msg}")
                    if _history:
                        _history.log_event('failed', filename, episode=_ep, source='blackhole',
                                           detail='Rejected: no usable media files',
                                           meta={'cause': 'disc_rip_rejected',
                                                 'provider': debrid,
                                                 'torrent_id': torrent_id},
                                           media_title=_mt)
                    try:
                        from utils.metrics import metrics
                        metrics.inc('blackhole_disc_rip_rejected')
                    except Exception:
                        pass
                    if _notify:
                        _notify('download_error', 'Blackhole: No Media Files',
                                f'{filename} contains no recognized media files. '
                                f'Auto-blocklisted and removed from debrid.',
                                level='warning')
                    self._remove_pending(torrent_id)
                    return
                if _history:
                    _mt, _ep = _enrich_for_history(filename)
                    _history.log_event('cached', filename, episode=_ep, source='blackhole',
                                       detail=f'Ready on {debrid}',
                                       meta={'cause': 'blackhole_cache_hit',
                                             'provider': debrid,
                                             'torrent_id': torrent_id},
                                       media_title=_mt)
                break

            if self._is_terminal_error(status, debrid=debrid):
                logger.error(f"[blackhole] Torrent {torrent_id} hit terminal error: {status}")
                _mt, _ep = _enrich_for_history(filename) if _history else (None, None)
                if _history:
                    # Scrub potential credential echoes from the provider
                    # status string before persisting it to history.jsonl.
                    import re as _re_bh
                    _status_safe = _re_bh.sub(
                        r'(apikey|api_key|token|key|bearer)=[^&\s]+',
                        r'\1=***', str(status), flags=_re_bh.IGNORECASE)
                    _history.log_event('failed', filename, episode=_ep, source='blackhole',
                                       detail=f'Terminal error: {_status_safe}',
                                       meta={'cause': 'terminal_error',
                                             'status': _status_safe,
                                             'provider': debrid,
                                             'torrent_id': torrent_id},
                                       media_title=_mt)
                # Auto-blocklist on terminal failure
                if _blocklist and str(os.environ.get('BLOCKLIST_AUTO_ADD', 'true')).lower() == 'true':
                    bl_hash = self._extract_hash_from_info(info, debrid=debrid)
                    if bl_hash:
                        _blocklist.add(bl_hash, filename, reason=f'Terminal error: {status}', source='auto')
                        if _history:
                            _history.log_event('blocklist_added', filename, episode=_ep, source='blackhole',
                                               detail=f'Auto-blocklisted: {status}',
                                               meta={'cause': 'auto_blocklist_added',
                                                     'blocklist_reason': f'terminal error: {status}',
                                                     'info_hash': bl_hash},
                                               media_title=_mt)
                try:
                    from utils.metrics import metrics
                    metrics.inc('blackhole_symlink_failed')
                except Exception:
                    pass
                if _notify:
                    _notify('download_error', 'Blackhole: Torrent Error',
                            f'{filename} failed with debrid status: {status}',
                            level='error')
                self._remove_pending(torrent_id)
                return

            logger.debug(f"[blackhole] Torrent {torrent_id} status: {status} ({elapsed:.0f}s)")
            self._stop_event.wait(self.mount_poll_interval)

        if self._stop_event.is_set():
            return

        if not release_name:
            logger.error(f"[blackhole] Could not determine release name for {filename}")
            self._remove_pending(torrent_id)
            return

        # Phase 2: Wait for content to appear on the rclone mount
        # Uses its own timeout budget separate from the debrid polling phase
        mount_start = time.time()
        mount_path = None
        category = None

        # Plan 41 phase B.3 — extract the per-file list from the last
        # status response so ``_find_on_mount`` (TB branch) can derive
        # WebDAV folder-name candidates from the file paths.  Critical
        # for non-English trackers where the API ``data.name`` is the
        # indexer's display title but TB stores under the .torrent's
        # ``info.name``.  Computed once before the poll loop — TB's
        # file list is stable post-cache-hit; no need to re-extract
        # every iteration.
        try:
            file_names = self._extract_filenames_from_info(info, debrid=debrid)
        except Exception:
            file_names = None

        # Kick rclone to re-list the mount root immediately so we don't have
        # to wait for the dir-cache to expire (2h on TorBox WebDAV, which has
        # no working ChangeNotify polling).  Coalesced across monitor threads:
        # when a season pack lands, its N episode monitors all reach here at
        # once — without coalescing each would fire a full-root PROPFIND and
        # the burst trips TorBox's WebDAV listing rate-limit.  One re-list per
        # window surfaces every sibling episode, so the suppressed monitors
        # still find their content on the very next poll.
        _coalesced_root_refresh()

        while not self._stop_event.is_set():
            elapsed_mount = time.time() - mount_start
            if elapsed_mount > self.mount_poll_timeout:
                # The torrent was confirmed added + ready on the debrid before
                # this branch is reachable (Phase 1 broke out via
                # _is_torrent_ready and passed the disc-rip check), so a slow
                # mount is NOT a hard failure — the content IS in the account
                # and will surface.  Hand off to the library scanner instead
                # of dropping the item; see _register_scanner_handoff.
                logger.warning(f"[blackhole] {release_name} not on mount after "
                               f"{elapsed_mount:.0f}s — confirmed ready on {debrid}, "
                               f"handing off to library scanner")
                try:
                    from utils.metrics import metrics
                    metrics.inc('blackhole_torrent_timeout')
                except Exception:
                    pass
                handed_off = self._register_scanner_handoff(filename)
                # Guard the history block so a parse/log failure can't kill the
                # monitor thread before _remove_pending runs — a leaked
                # _active_monitors entry would block a later re-add of the id.
                if _history:
                    try:
                        _mt, _ep = _enrich_for_history(filename)
                        _detail = ('Ready on debrid, mount slow — handed to library scanner'
                                   if handed_off
                                   else 'Ready on debrid, mount slow — scanner will resolve on next pass')
                        _history.log_event('cached', filename, episode=_ep, source='blackhole',
                                           detail=_detail,
                                           meta={'cause': 'blackhole_mount_handoff',
                                                 'provider': debrid,
                                                 'torrent_id': torrent_id,
                                                 'handoff_registered': handed_off},
                                           media_title=_mt)
                    except Exception as e:
                        logger.debug(f"[blackhole] hand-off history log failed: {e}")
                self._remove_pending(torrent_id)
                return

            mount_path, category, matched_name = self._find_on_mount(
                release_name, debrid=debrid, file_names=file_names,
            )
            if mount_path:
                logger.info(f"[blackhole] Found on mount: {mount_path} (category: {category})")
                break

            logger.debug(f"[blackhole] Waiting for {release_name} on mount ({elapsed_mount:.0f}s)")
            self._stop_event.wait(self.mount_poll_interval)

        if self._stop_event.is_set():
            return

        # Phase 3: Create symlinks
        try:
            count = self._create_symlinks(matched_name, category, mount_path, label=label, debrid=debrid)
            if count > 0:
                logger.info(f"[blackhole] Created {count} symlink(s) for {release_name}")
                if _history:
                    _mt, _ep = _enrich_for_history(filename)
                    _history.log_event('symlink_created', filename, episode=_ep, source='blackhole',
                                       detail=f'{count} symlink(s) for {release_name}',
                                       meta={'cause': 'blackhole_new_import',
                                             'provider': debrid,
                                             'count': count,
                                             'release': release_name},
                                       media_title=_mt)
                try:
                    from utils.metrics import metrics
                    metrics.inc('blackhole_symlink_created')
                except Exception:
                    pass
                if _notify:
                    _notify('download_complete', 'Blackhole: Symlinks Created',
                            f'{count} symlink(s) created for {release_name}')
                # Post-grab audit: did every claimed episode actually land?
                try:
                    self._audit_release_completeness(filename, release_name, mount_path, info)
                except Exception as e:
                    logger.debug(f"[blackhole] Completeness audit error for {release_name}: {e}")
                try:
                    from utils.library import get_scanner
                    scanner = get_scanner()
                    if scanner:
                        scanner.refresh()
                except Exception:
                    pass
            else:
                logger.warning(f"[blackhole] No media files found to symlink for {release_name}")
        except Exception as e:
            logger.error(f"[blackhole] Error creating symlinks for {release_name}: {e}")
            try:
                from utils.metrics import metrics
                metrics.inc('blackhole_symlink_failed')
            except Exception:
                pass

        self._remove_pending(torrent_id)

    def _resume_pending_monitors(self):
        """Resume monitoring for any torrents that were pending before a restart.

        Each entry is validated independently — a malformed or tampered entry
        (e.g. non-dict, label with path-traversal characters) is dropped with
        a warning rather than aborting the whole resume loop. This matters
        because the worker thread calling _resume_pending_monitors has only
        a top-level scan guard, not a resume guard.
        """
        entries = self._load_pending()
        if not entries:
            return

        logger.info(f"[blackhole] Resuming {len(entries)} pending torrent monitor(s)")
        for entry in entries:
            try:
                if not isinstance(entry, dict):
                    logger.warning(f"[blackhole] Skipping non-dict pending entry: {entry!r}")
                    continue
                torrent_id = entry.get('torrent_id')
                filename = entry.get('filename', 'unknown')
                # Legacy entries (pre-label-routing) have no 'label' field → None.
                # Validate because pending_monitors.json is trust-boundary state:
                # a tampered label would be piped into os.path.join downstream
                # and could create directories outside completed_dir.
                label = entry.get('label')
                if label is not None and not (isinstance(label, str) and _is_valid_label(label)):
                    logger.warning(
                        f"[blackhole] Dropping invalid label on pending entry {torrent_id!r}: {label!r}"
                    )
                    label = None
                # Per-grab debrid (plan 39 phase 2).  Pre-plan-39 entries
                # don't carry a 'debrid' key; the only possible origin
                # back then was the instance debrid_service, so default
                # to that.  Validate against the known set so a tampered
                # entry can't redirect API calls to an unknown provider.
                entry_debrid = entry.get('debrid') or entry.get('service') or self.debrid_service
                if entry_debrid not in ('realdebrid', 'alldebrid', 'torbox'):
                    logger.warning(
                        f"[blackhole] Dropping invalid debrid on pending entry "
                        f"{torrent_id!r}: {entry_debrid!r}"
                    )
                    entry_debrid = self.debrid_service
                if torrent_id:
                    self._start_monitor(torrent_id, filename, label=label, debrid=entry_debrid)
            except Exception as e:
                logger.warning(f"[blackhole] Skipping bad pending entry {entry!r}: {e}")

    # ── Local library dedup ─────────────────────────────────────────

    @staticmethod
    def _normalize_name(name):
        """Normalize a library folder or release name for comparison."""
        # Strip year in parens e.g. "Fargo (2014)" -> "Fargo"
        name = re.sub(r'\s*\(\d{4}\)\s*', '', name)
        return name.lower().strip()

    def _dedup_names_match(self, folder, name_norm, name_fuzzy):
        """True when an on-disk library folder matches the parsed release name.

        Strict compare first, then a punctuation-insensitive fallback:
        ``parse_release_name`` strips punctuation from the release side
        (dots→spaces), while arr folders on disk keep it ("What's Eating
        Gilbert Grape (1993)") — a strict-only compare misses every
        punctuation-bearing title and lets a duplicate import through.
        Both sides are year-stripped (``_normalize_name``) BEFORE fuzzing
        because ``norm_for_matching`` keeps digits, so "(1993)" would
        otherwise survive as a token on the folder side only.  Empty fuzzy
        forms (non-ASCII titles that collapse under transliteration) never
        match fuzzily — two distinct CJK titles must not dedup each other.
        """
        folder_norm = self._normalize_name(folder)
        if folder_norm == name_norm:
            return True
        if not name_fuzzy:
            return False
        try:
            from utils.library import norm_for_matching
        except ImportError:
            return False
        folder_fuzzy = norm_for_matching(folder_norm)
        return bool(folder_fuzzy) and folder_fuzzy == name_fuzzy

    def _check_local_library(self, filename):
        """Check if content from this torrent already exists locally.

        Returns True if content exists locally (should skip), False otherwise.
        Skips dedup for titles with prefer-debrid preference (user explicitly
        wants the debrid copy even though a local copy exists).
        """
        if not self.dedup_enabled:
            return False

        name, season, is_tv = parse_release_name(filename)
        if not name:
            return False

        name_norm = self._normalize_name(name)
        # Fuzzy form for the folder loops below (computed once; see
        # _dedup_names_match for why year-strip precedes fuzzing).
        try:
            from utils.library import norm_for_matching as _nfm
            dedup_fuzzy = _nfm(name_norm)
        except Exception:
            dedup_fuzzy = ''

        # Skip dedup for prefer-debrid titles — user wants the debrid copy.
        # Pref keys come from canonical titles via _normalize_title (lowercase
        # + strip trailing `(YYYY)`, punctuation preserved), while release
        # names arrive via parse_release_name (dot-separated, may retain
        # `(YYYY)` when the year parser missed it, punctuation stripped).
        # Check both strict and fuzzy forms so neither asymmetry misses:
        #   strict: _normalize_title both sides — handles parens-preserving
        #           release names and non-ASCII (CJK/Arabic) titles that
        #           collapse to empty under transliteration.
        #   fuzzy : _norm_for_matching both sides — handles the punctuation
        #           mismatch (e.g. "LEGO DC Batman: Family Matters" pref vs
        #           "LEGO.DC.Batman.Family.Matters" release).
        # Call-time imports are intentional — library.py and blackhole.py
        # have a bidirectional circular dependency.
        try:
            from utils.library import normalize_title, norm_for_matching
            from utils.library_prefs import get_all_preferences
            prefs = get_all_preferences()
            name_strict = normalize_title(name)
            name_fuzzy = norm_for_matching(name)
            matched_key = next(
                (k for k, v in prefs.items()
                 if v == 'prefer-debrid'
                 and (normalize_title(k) == name_strict
                      or (name_fuzzy and norm_for_matching(k) == name_fuzzy))),
                None,
            )
            if matched_key is not None:
                logger.info(
                    f"[blackhole] Bypassing local dedup for {filename}: "
                    f"matched prefer-debrid pref {matched_key!r}"
                )
                return False
        except Exception as e:
            logger.warning(
                f"[blackhole] prefer-debrid bypass check failed for {filename}: {e} "
                f"— falling through to dedup"
            )

        if is_tv and self.local_library_tv and os.path.isdir(self.local_library_tv):
            for folder in os.listdir(self.local_library_tv):
                if not self._dedup_names_match(folder, name_norm, dedup_fuzzy):
                    continue
                show_path = os.path.join(self.local_library_tv, folder)
                if season is not None:
                    season_dir = os.path.join(show_path, f"Season {season:02d}")
                    if os.path.isdir(season_dir) and _dir_has_video(season_dir):
                        # Check at episode level if the torrent targets specific episodes
                        target_eps = _parse_episodes(filename)
                        if target_eps:
                            local_eps = _local_episodes(season_dir)
                            if target_eps <= local_eps:
                                logger.info(f"[blackhole] Skipping {filename}: '{folder}' S{season:02d} episodes {sorted(target_eps)} exist locally")
                                return True
                            logger.debug(f"[blackhole] '{folder}' S{season:02d} has local eps {sorted(local_eps)} but torrent has {sorted(target_eps)} — not skipping")
                        else:
                            # Season pack — skip if season folder has video content
                            logger.info(f"[blackhole] Skipping {filename}: '{folder}' Season {season} exists locally")
                            return True
                else:
                    if _dir_has_video(show_path, recursive=True):
                        logger.info(f"[blackhole] Skipping {filename}: '{folder}' exists locally")
                        return True

        if not is_tv and self.local_library_movies and os.path.isdir(self.local_library_movies):
            for folder in os.listdir(self.local_library_movies):
                if not self._dedup_names_match(folder, name_norm, dedup_fuzzy):
                    continue
                movie_path = os.path.join(self.local_library_movies, folder)
                if _dir_has_video(movie_path):
                    logger.info(f"[blackhole] Skipping {filename}: '{folder}' exists locally")
                    return True

        return False

    # ── Debrid rejection auto-retry ──────────────────────────────────

    @staticmethod
    def _alt_exhausted(file_path):
        """Check if alternative releases were already tried and exhausted."""
        return RetryMeta.is_alt_exhausted(file_path)

    @classmethod
    def _is_debrid_rejection(cls, result_text):
        """Check if a debrid error response indicates the hash is blocked.

        Delegates to ``utils.debrid_routing.is_debrid_rejection`` so the
        rejection vocabulary lives in one place — the routing module's
        ``classify_add_failure`` is the source of truth that both this
        predicate and the add-time rescue gate share (plan 41 phase A).
        """
        from utils.debrid_routing import is_debrid_rejection
        return is_debrid_rejection(result_text)

    def _attempt_add_time_rescue(self, file_path, filename, info_hash,
                                 source_debrid, label, dispatch):
        """Plan 41 phase A — add-time cross-debrid rescue.

        When ``source_debrid`` returns a filter_block on the magnet add
        (RD's May-2026 keyword filter) and an alt debrid is configured,
        try the SAME hash on the alt.  On success: extract the alt
        torrent id, start a monitor entry on the alt, log a
        ``debrid_rescued`` history event, leave no file in the watch
        dir, notify — exactly as the success branch of ``_process_file``
        would have, but pointing at the alt provider.

        Returns ``True`` when rescued (caller should ``return``).
        Returns ``False`` on any failure mode — file is left at
        ``file_path`` for the existing alt-release / failed/ fallback
        paths.

        File handling — the rescue stages ``file_path`` to a
        uniquely-named entry under ``.alt_pending/`` BEFORE entering the
        helper's up-to-60s wait_ready poll loop.  Without that move, a
        Sonarr/Radarr re-grab of the same release name during the
        rescue window would POSIX-rename a new file over ours and the
        original grab would be silently lost when the rescue completes.
        Unique name (``.rescue-<random8>-<filename>``) prevents
        collision with the alt-release path's own staging in the same
        directory.

        Distinct from ``_try_alternative_release``: that path searches
        for a DIFFERENT release of the same media; this path tries the
        SAME hash on a DIFFERENT debrid.  Both are useful and chain
        naturally — try cross-rescue first (cheap, hit-cached only),
        fall back to alt-release search.

        Known limitation (BH-3): if the alt's add request raises mid-
        ``response.json()`` (TB serving malformed JSON during an
        incident), the alt may have allocated a torrent_id server-side
        but we have no id to delete from here — orphan on the alt
        account.  Pre-existing in ``_add_to_torbox`` regardless of
        rescue; the rescue path inherits but doesn't widen it.  A
        follow-up wired through ``_add_to_torbox`` is the right place
        to close the orphan window for all callers.
        """
        from utils.debrid_routing import attempt_add_rescue, pick_alt_debrid, classify_add_failure

        alt = pick_alt_debrid(source_debrid)
        if not alt:
            return False

        alt_api_key = self._api_key_for(alt)
        if not alt_api_key:
            return False

        # Resolve alt client in this namespace so tests that patch
        # ``utils.blackhole.get_debrid_client`` (or equivalent) reach it.
        # The shared helper accepts ``alt_client=`` so we can pass the
        # pre-resolved client and skip the helper's internal lookup.
        try:
            from utils.debrid_client import get_debrid_client
            alt_client, _svc = get_debrid_client(service=alt, api_key=alt_api_key)
        except Exception as e:
            logger.warning(
                f"[blackhole] rescue get_debrid_client failed: "
                f"{type(e).__name__}"
            )
            return False
        if alt_client is None or not getattr(alt_client, 'configured', False):
            return False

        alt_handler = dispatch.get(alt)
        if not alt_handler:
            return False

        # Stage the file BEFORE the rescue wait_ready loop (see method
        # docstring).  Unique name keeps us out of alt-release staging's
        # way.  On any failure mode below, we move back to file_path so
        # the alt-release fallback can take over.
        staging_dir = self._alt_pending_dir(label)
        try:
            os.makedirs(staging_dir, exist_ok=True)
        except OSError as e:
            logger.warning(
                f"[blackhole] Could not create rescue staging dir {staging_dir}: {e}. "
                f"Skipping rescue."
            )
            return False
        # Truncate the filename portion so the full staged basename stays
        # under POSIX NAME_MAX (255 bytes) even for long multi-byte
        # names from non-English trackers — without the cap, ``os.rename``
        # below would raise ``ENAMETOOLONG`` and the rescue would silently
        # fall through to alt-release with a misleading "permissions"-
        # shaped log line.  The truncation only affects the staged copy;
        # the recovery path strips the prefix regex-anchored on the 8
        # hex digits, not the trailing length, so a truncated suffix is
        # still recoverable as a plain filename if needed.
        safe_filename = filename[:_RESCUE_STAGED_FILENAME_MAX]
        staged_basename = f'.rescue-{uuid.uuid4().hex[:8]}-{safe_filename}'
        staged_path = os.path.join(staging_dir, staged_basename)
        try:
            os.rename(file_path, staged_path)
        except OSError as e:
            logger.warning(
                f"[blackhole] Could not stage {filename} for rescue: {e}. "
                f"Skipping rescue."
            )
            return False

        # Closure captures the alt-side response so we can detect a
        # "both providers filter-block" case after the helper returns.
        # Stays as ``{'success': False, 'result': None}`` if the helper
        # short-circuits before the add (e.g. cache_probe says
        # not_cached_on_alt) — that's by design.  ``classify_add_failure(None)``
        # returns ``None`` so the post-rescue blocklist gate below
        # correctly doesn't fire for short-circuit cases.
        add_response = {'success': False, 'result': None, 'extract_failed': False}

        def _add_via_handler(client, h):  # noqa: ARG001 — client + h unused; we add via the staged file path
            success, result = alt_handler(staged_path, api_key=alt_api_key)
            add_response['success'] = success
            add_response['result'] = result
            if not success:
                return None
            tid = self._extract_torrent_id(result, debrid=alt)
            if not tid:
                # The alt accepted the add (HTTP 200/201) but the
                # response shape didn't yield a torrent id.  Schema
                # drift on the alt's side — NOT a filter block.  Flag
                # this so the post-rescue blocklist gate below doesn't
                # spuriously annotate the hash as
                # ``filter_blocked_everywhere`` (which would permanently
                # block future re-grabs of a hash the alt actually has).
                add_response['extract_failed'] = True
            return tid or None

        core = attempt_add_rescue(
            info_hash, source_debrid,
            alt_debrid=alt,
            alt_client=alt_client,
            alt_add_fn=_add_via_handler,
            ready_states=TB_READY_STATES,
            stop_event=self._stop_event,
            logger_prefix='blackhole',
        )

        if not core.get('rescued'):
            # Move the staged file back so the existing alt-release /
            # failed-dir fallback paths can take over.  Atomic check-
            # and-link via ``os.link`` (raises FileExistsError when
            # ``file_path`` already exists) prevents the TOCTOU race
            # where a fresh Sonarr drop lands between an ``os.path.exists``
            # check and a follow-up ``os.rename`` — POSIX rename
            # silently overwrites, which would lose the fresh grab.
            # ``os.link`` is atomic on the same filesystem, which is
            # always the case here (both paths are under ``self.watch_dir``).
            try:
                os.link(staged_path, file_path)
                os.unlink(staged_path)
            except FileExistsError:
                # Fresh drop landed during the rescue wait — leave the
                # original at its unique staged name for manual recovery.
                logger.warning(
                    f"[blackhole] {filename} re-appeared at watch dir during rescue wait; "
                    f"original preserved at {staged_path} for manual recovery"
                )
            except OSError as e:
                logger.warning(
                    f"[blackhole] Could not restore {filename} from rescue staging: {e}. "
                    f"File preserved at {staged_path}"
                )

            # Both-providers-filter-block: annotate the blocklist so future
            # re-grabs of the same hash short-circuit at the pre-submit gate.
            # The alt sweep will not rescue this either (it's filter-blocked
            # there too), so blocklisting saves the indexer + API budget.
            #
            # Gate ALSO requires NOT extract_failed — a malformed alt
            # response is schema drift, not a filter block, and would
            # blocklist a hash the alt actually has.
            #
            # Best-effort detection: the trigger only fires when the alt's
            # response carries an RD-shaped ``infringing_file`` / code 35
            # payload.  TorBox doesn't ship a documented filter-block
            # vocabulary today; if/when TB starts filter-blocking with its
            # own response shape, ``classify_add_failure`` won't recognise
            # it and this annotation will silently skip.  That's an
            # acceptable miss — the rescue still falls through to the
            # existing alt-release search; the only cost is an extra
            # re-grab attempt on each Sonarr/Radarr retry cycle until
            # ``_FILTER_BLOCK_KEYWORDS`` is updated.
            alt_result = add_response.get('result')
            if (core.get('reason') == 'add_failed'
                    and not add_response.get('extract_failed')
                    and classify_add_failure(alt_result) == 'filter_block'
                    and _blocklist):
                # Sanitize the title — filename comes from an
                # uploader-controlled torrent name and can carry ASCII
                # control chars that would render as raw text in
                # notifications / logs.  Strip and truncate before
                # storing in the persistent blocklist JSON.
                safe_title = re.sub(r'[\x00-\x1f\x7f]', ' ', filename)[:200]
                try:
                    _blocklist.add(
                        info_hash, safe_title,
                        reason=f'filter_blocked_everywhere ({source_debrid}+{alt})',
                        source='auto',
                    )
                except Exception as e:
                    logger.debug(
                        f"[blackhole] filter_blocked_everywhere blocklist add failed: {e}"
                    )
            return False

        # Rescue succeeded — remove the staged file (it's no longer
        # needed; the monitor entry is on the alt torrent_id now).
        try:
            os.remove(staged_path)
        except OSError as e:
            logger.warning(
                f"[blackhole] Could not remove staged file after rescue: {e}"
            )

        alt_tid = core['alt_torrent_id']

        # Prime alt dedup cache so a re-drop of the same .magnet pre-TTL
        # is caught even before the alt account list refreshes.
        try:
            from utils.search import remember_added_hash
            remember_added_hash(alt, info_hash)
        except ImportError:
            pass

        # Start a monitor entry on the alt so symlink creation fires when
        # the file lands on the alt's mount.
        if self.symlink_enabled:
            try:
                self._start_monitor(alt_tid, filename, label=label, debrid=alt)
            except Exception as e:
                logger.error(
                    f"[blackhole] Failed to start rescue monitor for {filename}: {e}"
                )

        # History — distinct cause from a normal grab so the activity
        # feed shows the recovery story.  ``rescue_stage='add_time'``
        # disambiguates from the sweep-driven rescue path that retargets
        # existing symlinks.
        if _history:
            _mt, _ep = _enrich_for_history(filename)
            _history.log_event(
                'debrid', filename, episode=_ep, source='blackhole',
                detail=f'Filter-blocked on {source_debrid} — rescued via {alt}',
                meta={'cause': _history.CAUSE_DEBRID_RESCUED,
                      'rescue_stage': 'add_time',
                      'from': source_debrid,
                      'to': alt,
                      'info_hash': info_hash,
                      'torrent_id': alt_tid,
                      'provider': alt,
                      'reason': 'infringing_file'},
                media_title=_mt,
            )

        try:
            from utils.metrics import metrics
            metrics.inc('blackhole_processed', {'status': 'rescued'})
        except Exception:
            pass

        if _notify:
            _notify(
                'debrid_rescued',
                f'Debrid rescue: {filename[:60]}',
                f'{source_debrid} filter-blocked, rescued via {alt}',
            )

        logger.info(
            f"[blackhole] Rescued {filename}: {source_debrid} → {alt} "
            f"(alt_tid={alt_tid})"
        )
        return True

    def _try_alternative_release(self, filename, file_path, debrid_handler, label=None, debrid=None):
        """On debrid rejection, query Sonarr/Radarr for an alternative release.

        Parses the episode/movie info from the filename, fetches available
        releases, filters to a different info hash, and tries them until
        one succeeds or all are exhausted.

        Runs in a background thread. On failure, moves the original file
        to the failed/ directory (same as the normal failure path).

        *label* preserves per-arr routing — if the file was staged from
        ``/watch/sonarr/.alt_pending/``, failures land in ``/watch/sonarr/failed/``.

        *debrid* names the provider behind *debrid_handler* so torrent-ID
        extraction and the symlink monitor use the matching schema/API.
        None defaults to the primary service (legacy callers).
        """
        alt_ok = False
        try:
            from utils.arr_client import SonarrClient, RadarrClient

            name, season, is_tv = parse_release_name(filename)
            if not name:
                logger.debug(f"[blackhole] Cannot parse release name for alt-retry: {filename}")
            elif is_tv and season is not None and _parse_episodes(filename):
                alt_ok = self._try_alt_episode(name, season, _parse_episodes(filename),
                                               debrid_handler, filename, file_path, label=label,
                                               debrid=debrid)
            elif not is_tv:
                alt_ok = self._try_alt_movie(name, debrid_handler, filename, file_path, label=label,
                                             debrid=debrid)
            else:
                logger.debug(f"[blackhole] Cannot determine content type for alt-retry: {filename}")
        except Exception as e:
            logger.error(f"[blackhole] Error during alternative release search: {e}")

        if not alt_ok and os.path.exists(file_path):
            # No alternative worked — move to failed/ and mark alts exhausted
            # so retries don't repeat the same alt-release search
            error_dir = self._failed_dir(label)
            os.makedirs(error_dir, exist_ok=True)
            dest = os.path.join(error_dir, filename)
            if os.path.exists(dest):
                base, fext = os.path.splitext(filename)
                dest = os.path.join(error_dir, f"{base}_{int(time.time())}{fext}")
            rename_ok = False
            try:
                os.rename(file_path, dest)
                rename_ok = True
                # Mark alt-exhausted via the centralised helper so any
                # tier_state already seeded on this sidecar is preserved
                # (the old raw-write form clobbered the whole file and
                # would wipe the dwell timer on every alt-exhaustion).
                RetryMeta.mark_alt_exhausted(dest)
            except OSError as e:
                logger.warning(f"[blackhole] Could not move {filename} to failed/: {e}")

            # Notify user — all alternatives exhausted, manual intervention needed
            if _notify:
                detail = (f'File moved to failed/ — manual intervention required.'
                          if rename_ok else
                          f'Could not move to failed/ — file may still be in watch dir.')
                _notify('download_error', 'Blackhole: All Alternatives Failed',
                        f'No working alternative releases found for {filename}. {detail}',
                        level='warning')
            if _history:
                _mt, _ep = _enrich_for_history(filename)
                _history.log_event('failed', filename, episode=_ep, source='blackhole',
                                   detail='All alternative releases exhausted',
                                   meta={'cause': 'alts_exhausted'},
                                   media_title=_mt)

    def _try_alt_episode(self, series_name, season, episodes, debrid_handler, orig_filename, orig_path, label=None, debrid=None):
        """Try alternative releases for a TV episode via Sonarr."""
        from utils.arr_client import SonarrClient

        client = SonarrClient()
        if not client.configured:
            return False

        series = client.find_series_in_library(title=series_name)
        if not series:
            logger.debug(f"[blackhole] Cannot find series '{series_name}' in Sonarr")
            return False

        ep_num = min(episodes)  # primary episode number
        episode_id = client.get_episode_id(series_name, season, ep_num)
        if not episode_id:
            logger.debug(f"[blackhole] Could not find {series_name} S{season:02d}E{ep_num:02d} in Sonarr")
            return False

        releases = client.get_episode_releases(episode_id)
        if not releases:
            logger.debug(f"[blackhole] No alternative releases found for {series_name} S{season:02d}E{ep_num:02d}")
            # Empty arr-alt list is the strongest signal that the
            # preferred tier isn't reachable via the arr's indexers;
            # still allow the compromise path to probe Torrentio.
            releases = []

        self._seed_tier_state(client, 'series', series, orig_path)
        if self._try_releases(releases, debrid_handler, orig_filename, orig_path, label=label,
                              debrid=debrid):
            return True
        return self._try_compromise(
            client, 'series', series,
            context={'media_type': 'series', 'season': season, 'episode': ep_num,
                     'series_id': series.get('id')},
            debrid_handler=debrid_handler,
            orig_filename=orig_filename, orig_path=orig_path, label=label,
            debrid=debrid,
        )

    def _try_alt_movie(self, movie_name, debrid_handler, orig_filename, orig_path, label=None, debrid=None):
        """Try alternative releases for a movie via Radarr."""
        from utils.arr_client import RadarrClient

        client = RadarrClient()
        if not client.configured:
            return False

        movie = client.find_movie_in_library(title=movie_name)
        if not movie:
            logger.debug(f"[blackhole] Could not find '{movie_name}' in Radarr")
            return False

        releases = client.get_movie_releases(movie['id'])
        if not releases:
            logger.debug(f"[blackhole] No alternative releases found for '{movie_name}'")
            releases = []

        self._seed_tier_state(client, 'movie', movie, orig_path)
        if self._try_releases(releases, debrid_handler, orig_filename, orig_path, label=label,
                              debrid=debrid):
            return True
        return self._try_compromise(
            client, 'movie', movie,
            context={'media_type': 'movie'},
            debrid_handler=debrid_handler,
            orig_filename=orig_filename, orig_path=orig_path, label=label,
            debrid=debrid,
        )

    @staticmethod
    def _tb_alt_recovery_enabled():
        return str(os.environ.get(
            'BLACKHOLE_TB_ALT_RECOVERY_ENABLED', 'true')).lower() == 'true'

    def _resolve_arr_identity(self, filename):
        """Resolve ``(imdb_id, media_type, season, episode)`` for *filename*.

        Looks the parsed title up in Radarr (movies) or Sonarr (series) to
        recover the IMDb id Torrentio needs.  Returns ``(None, None, None,
        None)`` when the arr is unconfigured, the title isn't in its
        library, or the record carries no IMDb id.  Never raises.
        """
        try:
            from utils.arr_client import SonarrClient, RadarrClient
        except Exception as e:
            logger.debug(f"[blackhole] TB-alt: arr import failed: {e}")
            return None, None, None, None
        name, season, is_tv = parse_release_name(filename)
        if not name:
            return None, None, None, None
        try:
            if is_tv:
                client = SonarrClient()
                if not client.configured:
                    return None, None, None, None
                series = client.find_series_in_library(title=name)
                if not series:
                    return None, None, None, None
                eps = _parse_episodes(filename)
                episode = min(eps) if eps else None
                return series.get('imdbId'), 'series', season, episode
            client = RadarrClient()
            if not client.configured:
                return None, None, None, None
            movie = client.find_movie_in_library(title=name)
            if not movie:
                return None, None, None, None
            return movie.get('imdbId'), 'movie', None, None
        except Exception as e:
            logger.debug(f"[blackhole] TB-alt: arr lookup failed for {filename}: {e}")
            return None, None, None, None

    def _try_torbox_cached_alternative(self, file_path, filename, info_hash, debrid, label=None):
        """Last-ditch recovery before deleting an uncached-rejected grab.

        The release Radarr/Sonarr grabbed is uncached (on its routed
        debrid, and — for the cross-confirmed path — also on TorBox), but
        OTHER releases of the same title may be cached on TorBox.  Rather
        than let the title fall back to "Wanted", search Torrentio for
        same-title releases, keep only those TorBox has cached AT THE SAME
        quality tier the arr already approved, grab the best one on TorBox,
        and start a TB-routed symlink monitor.

        Returns True iff a cached alternative was grabbed — in which case
        the original watch-dir file has already been removed and the caller
        must NOT delete again.  False → caller falls through to its
        existing delete.  Never raises; any failure degrades to delete.
        """
        if not self._tb_alt_recovery_enabled():
            return False
        # No info hash means we can't exclude the rejected release from the
        # candidate set, and the file already failed the cache gate for a
        # reason unrelated to a swappable hash — decline.
        if not info_hash:
            return False
        tb_key = self._api_key_for('torbox')
        if not tb_key:
            return False
        try:
            from utils.search import search_torrentio, check_debrid_cache, parse_quality
            from utils.quality_compromise import _filter_candidates, _rank_within_tier
            from utils.library import release_matches_title

            # Stay within the arr's already-approved ceiling: only accept
            # alternatives at the SAME quality tier as the rejected release
            # (Radarr/Sonarr already deemed that tier acceptable).  An
            # unparseable tier ('Unknown') can't be safely matched, so we
            # decline rather than risk grabbing below-profile quality.
            target_tier = parse_quality(filename).get('label')
            if not target_tier or target_tier == 'Unknown':
                logger.debug(f"[blackhole] TB-alt: unparseable tier for {filename}; skipping")
                return False

            imdb_id, media_type, season, episode = self._resolve_arr_identity(filename)
            if not imdb_id:
                logger.debug(f"[blackhole] TB-alt: no IMDb id for {filename}; skipping")
                return False

            # One cached pack recovers the whole season — if a sibling episode
            # already grabbed an alternative for this (imdb_id, season), skip
            # the redundant search/probe/grab that would pull another pack and
            # multiply the rclone VFS load.  Checked before the Torrentio search
            # so we also spare the TorBox cache-probe API calls.
            dedup_key = (imdb_id, season)
            if self._tb_alt_recently_grabbed(dedup_key):
                logger.info(f"[blackhole] TB-alt: already recovered "
                            f"{imdb_id} (season={season}) within TTL; "
                            f"skipping redundant grab for {filename}")
                return False

            # Persistent give-up cap: a never-completing title would otherwise
            # be re-grabbed every time its .magnet re-drops, re-arming TorBox's
            # abuse cooldown indefinitely. After _tb_alt_max_attempts grabs for
            # this (imdb_id, season), decline so the caller deletes and the title
            # falls back to Wanted instead of churning TorBox.
            ledger_key = f"tbalt:{imdb_id}:s{season}"
            if attempt_ledger.get(ledger_key) >= self._tb_alt_max_attempts:
                logger.info(f"[blackhole] TB-alt: give-up cap "
                            f"({self._tb_alt_max_attempts}) reached for "
                            f"{imdb_id} (season={season}); declining grab for "
                            f"{filename}, falling back to Wanted")
                return False

            results = search_torrentio(
                imdb_id, media_type=media_type, season=season, episode=episode,
            )
            if not results:
                return False

            rejected = info_hash.lower()
            # Pre-filter to the approved tier and rank by seeders BEFORE
            # probing.  TorBox's cache probe is per-hash and capped
            # (_TORBOX_MAX_PROBES), so probing the full unranked Torrentio
            # result set (often 40-60+ streams across all tiers) would (a)
            # waste the budget on wrong-tier releases and (b) silently leave
            # genuinely-cached same-tier releases past the cap looking
            # uncached — defeating the recovery.  Probing only the best-seeded
            # same-tier slice also bounds worst-case latency on this serial
            # path (each probe can block up to _CACHE_PROBE_TIMEOUT).
            # The arr-approved release name is the trusted title reference —
            # Torrentio's imdb-keyed lists contain mislabeled uploads, and
            # grabbing one would park the wrong movie under this title.
            approved_title = parse_release_name(filename)[0]
            title_toks = set(approved_title.lower().split())
            years = [y for y in re.findall(r'(?<!\d)(?:19|20)\d{2}(?!\d)',
                                           filename)
                     if y not in title_toks]
            approved_year = int(years[0]) if years else None
            candidates = [r for r in results
                          if (r.get('info_hash') or '').lower() != rejected
                          and (r.get('quality') or {}).get('label') == target_tier
                          and release_matches_title(r.get('title') or '',
                                                    approved_title,
                                                    media_year=approved_year)]
            if not candidates:
                logger.debug(f"[blackhole] TB-alt: no same-tier "
                             f"({target_tier}) alternative for {filename}")
                return False
            candidates.sort(key=lambda r: -(r.get('seeds') or 0))
            probe = candidates[:_TB_ALT_MAX_PROBES]

            # RD/AD cache endpoints are dead, so we MUST ask TorBox explicitly
            # here — search_torrents would annotate via the primary debrid (RD)
            # and return all-None.  search_torrentio yields lowercase hashes,
            # matching the lowercased map keys check_debrid_cache returns.
            cache_map = check_debrid_cache(
                [r['info_hash'] for r in probe],
                service='torbox', api_key=tb_key,
            )
            for r in probe:
                r['cached'] = cache_map.get(r['info_hash'])

            # min_seeders=0: a TorBox-cached release is served from TB
            # storage, so seeder count is irrelevant to whether it streams.
            eligible = _filter_candidates(probe, target_tier,
                                          min_seeders=0, only_cached=True)
            best = _rank_within_tier(eligible)
            if not best:
                logger.debug(f"[blackhole] TB-alt: no cached {target_tier} "
                             f"alternative for {filename}")
                return False

            alt_hash = (best.get('info_hash') or '').strip().lower()
            if not alt_hash or not re.match(r'^[a-fA-F0-9]{40}$', alt_hash):
                return False
        except Exception as e:
            logger.debug(f"[blackhole] TB-alt: candidate search failed for {filename}: {e}")
            return False

        # Grab the cached alternative on TorBox via a throwaway .magnet.
        magnet = f'magnet:?xt=urn:btih:{alt_hash}'
        tmp_fd, tmp_path = tempfile.mkstemp(suffix='.magnet', prefix='_tbalt_')
        success = False
        result = None
        try:
            with os.fdopen(tmp_fd, 'w') as f:
                f.write(magnet)
            success, result = self._add_to_torbox(tmp_path, api_key=tb_key)
        except Exception as e:
            logger.warning(f"[blackhole] TB-alt: TorBox add errored for {filename}: {e}")
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass

        if not success:
            logger.info(f"[blackhole] TB-alt: TorBox rejected cached alternative for "
                        f"{filename}: {str(result)[:100]}")
            return False

        # The alternative is now on TorBox.  In symlink mode the monitor is
        # what eventually creates the symlink + triggers the arr import — so
        # if we can't start it (no torrent id, or _start_monitor errors), the
        # title is NOT recovered: the torrent would sit orphaned on TB with
        # nothing tracking it.  Start the monitor BEFORE removing the original
        # and committing to success; on failure, decline (leave the original
        # for the caller's normal rejection handling — the cached orphan is
        # harmless, far better than silently claiming a recovery that never
        # produces a playable file).  With symlinking off, the library scanner
        # owns symlink creation, so no monitor is needed.
        if self.symlink_enabled:
            torrent_id = self._extract_torrent_id(result, debrid='torbox')
            if not torrent_id:
                logger.error(f"[blackhole] TB-alt: could not extract torrent id for "
                             f"{filename}; not claiming recovery")
                return False
            try:
                self._start_monitor(torrent_id, filename, label=label, debrid='torbox')
            except Exception as e:
                logger.error(f"[blackhole] TB-alt: failed to start monitor for "
                             f"{filename}; not claiming recovery: {e}")
                return False

        # Committed: record the dedup hash and remove the original so the
        # scanner doesn't re-process the rejected release on its next pass.
        try:
            from utils.search import remember_added_hash
            remember_added_hash('torbox', alt_hash)
        except ImportError:
            pass
        # Suppress sibling-episode grabs for this season for _TB_ALT_DEDUP_TTL.
        # dedup_key is always bound here: the only way to reach this line is to
        # fall through the candidate-resolution try block (whose except returns
        # False), and dedup_key is assigned before any code that can do so.
        self._remember_tb_alt_grab(dedup_key)
        attempt_ledger.bump(ledger_key)
        try:
            os.remove(file_path)
        except OSError as e:
            logger.warning(f"[blackhole] TB-alt: could not remove original {filename}: {e}")

        alt_title = (best.get('title') or '')[:80]
        logger.info(f"[blackhole] TB-alt: recovered {filename} — grabbed cached "
                    f"{target_tier} alternative on TorBox ({alt_title})")
        # History/notify/metrics are best-effort — the recovery has already
        # succeeded and the original is gone, so a failure here must not raise
        # out of the method and kill the _process_file worker mid-batch.
        try:
            if _history:
                _mt, _ep = _enrich_for_history(filename)
                _history.log_event(
                    'tb_cached_alt_grabbed', filename, episode=_ep, source='blackhole',
                    detail=f'Recovered — grabbed cached {target_tier} alternative on TorBox',
                    meta={'cause': 'tb_cached_alt_grabbed',
                          'info_hash': alt_hash,
                          'rejected_info_hash': info_hash,
                          'rejected_provider': debrid,
                          'provider': 'torbox',
                          'tier': target_tier,
                          'alt_title': alt_title},
                    media_title=_mt,
                )
            if _notify:
                _notify('download_complete', 'Blackhole: Cached Alternative Found',
                        f'{filename}: original uncached, grabbed cached {target_tier} '
                        f'alternative on TorBox', level='info')
            from utils.metrics import metrics
            metrics.inc('blackhole_processed', {'status': 'tb_cached_alt_grabbed'})
        except Exception as e:
            logger.warning(f"[blackhole] TB-alt: post-recovery bookkeeping failed for "
                           f"{filename}: {e}")
        return True

    @staticmethod
    def _tb_alt_dedup_ledger_key(key):
        """attempt_ledger key for a (imdb_id, season) sibling-grab dedup entry.

        Distinct ``tbaltdedup:`` family so it can't collide with the
        ``tbalt:`` give-up counters, which share the same (imdb, season)
        identity but different semantics (TTL'd dedup vs. capped count).
        """
        imdb_id, season = key
        return f"tbaltdedup:{imdb_id}:s{season}"

    def _tb_alt_recently_grabbed(self, key):
        """True if a cached alternative was grabbed for this (imdb_id, season)
        within _TB_ALT_DEDUP_TTL.  Prunes expired entries opportunistically so
        the dict stays bounded over a long-running process.

        The in-memory dict is the fast path; the attempt_ledger mirror makes
        the answer restart-survivable — without it, a container restart
        mid-backfill re-enables sibling season-pack grabs, and each redundant
        TB create is exactly the volume event that arms TB Essential's ~24h
        abuse cooldown."""
        now = time.time()
        with self._tb_alt_grabs_lock:
            expired = [k for k, ts in self._tb_alt_recent_grabs.items()
                       if now - ts >= _TB_ALT_DEDUP_TTL]
            for k in expired:
                del self._tb_alt_recent_grabs[k]
            # Anything still present is, by construction, within the TTL.
            if key in self._tb_alt_recent_grabs:
                return True
        last = attempt_ledger.last_seen_epoch(self._tb_alt_dedup_ledger_key(key))
        return last is not None and (now - last) < _TB_ALT_DEDUP_TTL

    def _remember_tb_alt_grab(self, key):
        """Record that a cached alternative was grabbed for this (imdb_id,
        season) so sibling-episode grabs in the same burst are suppressed —
        including across a container restart (ledger mirror)."""
        with self._tb_alt_grabs_lock:
            self._tb_alt_recent_grabs[key] = time.time()
        attempt_ledger.bump(self._tb_alt_dedup_ledger_key(key))

    @staticmethod
    def _compromise_enabled():
        return str(os.environ.get('QUALITY_COMPROMISE_ENABLED', 'false')).lower() == 'true'

    @staticmethod
    def _season_pack_enabled():
        return str(os.environ.get('SEASON_PACK_FALLBACK_ENABLED', 'false')).lower() == 'true'

    @staticmethod
    def _int_env(name, default, minimum=0):
        """Read an int env var with a default and a floor.

        *minimum* clamps the returned value so a misconfigured negative
        dwell doesn't make the dwell gate bypass (-86400 seconds would
        make ``now - first_attempted_at >= -86400`` vacuously true) and
        a negative ``min_missing`` doesn't make the season-pack probe
        always trigger.  Non-int or empty values return *default*
        (which callers set above *minimum*).
        """
        raw = os.environ.get(name)
        if raw is None or raw == '':
            return default
        try:
            return max(minimum, int(raw))
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _float_env(name, default, minimum=0.0, maximum=None):
        """Read a float env var clamped to ``[minimum, maximum]``.

        Mirrors ``_int_env`` for Phase 7's ratio gate: a misconfigured
        ``SEASON_PACK_FALLBACK_MIN_RATIO=1.5`` would otherwise require
        150% of the season to be missing (gate never trips) and a
        negative ratio would always trip.  Non-float/empty values
        return *default*; NaN/inf do the same because they sneak past
        ``<minimum`` / ``>maximum`` comparisons (NaN compares False to
        everything) and would silently disable the ratio gate without
        failing validation.  *default* must already satisfy the bounds.
        """
        import math
        raw = os.environ.get(name)
        if raw is None or raw == '':
            return default
        try:
            val = float(raw)
        except (ValueError, TypeError):
            return default
        if math.isnan(val) or math.isinf(val):
            return default
        if val < minimum:
            val = minimum
        if maximum is not None and val > maximum:
            val = maximum
        return val

    def _seed_tier_state(self, arr_client, media_type, record, file_path):
        """Read the arr's profile + tier order and seed RetryMeta.tier_state.

        Idempotent: ``RetryMeta.init_tier_state`` refuses to overwrite an
        existing valid tier_state, so re-seeding on every alt-retry is
        safe and keeps the dwell baseline pinned to the first attempt
        (I3).  Failures (no profile, empty tier order, arr offline) are
        logged at debug and left to the caller — the compromise path
        will short-circuit on the resulting ``tier_state=None``.
        """
        if not self._compromise_enabled():
            return
        try:
            if media_type == 'series':
                profile_id = arr_client.get_profile_id_for_series(record.get('id'))
                arr_service = 'sonarr'
            else:
                profile_id = arr_client.get_profile_id_for_movie(record.get('id'))
                arr_service = 'radarr'
            if not profile_id:
                return
            tier_order = arr_client.get_tier_order(profile_id)
            if not tier_order:
                return
            arr_url = getattr(arr_client, 'base_url', '') or ''
            RetryMeta.init_tier_state(
                file_path, arr_service=arr_service, arr_url=arr_url,
                profile_id=profile_id, tier_order=tier_order,
            )
        except Exception as e:
            logger.debug(f"[blackhole] Could not seed tier_state for {file_path}: {e}")

    def _try_compromise(self, arr_client, media_type, record, context,
                        debrid_handler, orig_filename, orig_path, label=None,
                        debrid=None):
        """On arr-alt exhaustion, attempt a cache-aware tier drop.

        Returns True iff a compromise candidate was successfully
        submitted to the debrid service (and, if symlink mode is on, a
        monitor started).  Never raises — any unexpected failure falls
        through to the caller's existing ``failed/`` path.
        """
        if not self._compromise_enabled():
            return False
        try:
            from utils.quality_compromise import (
                should_compromise, find_compromise_candidate,
                find_season_pack_candidate,
            )

            tier_state = RetryMeta.read_tier_state(orig_path)
            # minimum=1: a 0-day dwell is effectively "fire compromise on
            # the first retry" which defeats invariant I3 and the UI
            # validator range (1-30).  Clamp at the runtime level so the
            # two surfaces agree (Phase 7 code-review finding).
            dwell_days = self._int_env('QUALITY_COMPROMISE_DWELL_DAYS', 3, minimum=1)
            min_seeders = self._int_env('QUALITY_COMPROMISE_MIN_SEEDERS', 3, minimum=0)
            only_cached = str(os.environ.get(
                'QUALITY_COMPROMISE_ONLY_CACHED', 'true')).lower() == 'true'
            # Phase 7 cap: how far down the profile we're allowed to
            # descend.  minimum=1 at the runtime surface — ``0`` would
            # intuitively read as "zero drops allowed" but the
            # should_compromise contract treats 0 as "unlimited cap",
            # which is the dangerous opposite.  Users who genuinely
            # want unlimited set a large number (e.g. 10); the profile
            # ceiling still short-circuits via ``no_lower_tier_in_profile``.
            max_tier_drop = self._int_env('QUALITY_COMPROMISE_MAX_TIER_DROP', 2, minimum=1)

            action, reason = should_compromise(
                tier_state, time.time(),
                dwell_seconds=dwell_days * 86400,
                only_cached=only_cached,
                max_tier_drop=max_tier_drop,
            )
            if action != 'advance':
                logger.debug(f"[blackhole] Compromise decision for {orig_filename}: "
                             f"action={action} reason={reason}")
                return False

            tier_order = tier_state['tier_order']
            current_idx = tier_state['current_tier_index']
            preferred_tier = tier_order[current_idx]

            # Observability: capture dwell + per-tier hit counts from the
            # tier_state attempt log BEFORE we advance state.  These ride
            # along on compromise_meta into history + pending_monitors so
            # the dashboard can answer "why did this compromise fire?"
            # without re-deriving from the sidecar (which gets cleaned up
            # after a successful submit).
            first_attempted_at = tier_state.get('first_attempted_at') or time.time()
            dwell_seconds = max(0, int(time.time() - first_attempted_at))
            cached_alts_at_preferred = 0
            uncached_alts_at_preferred = 0
            for _att in tier_state.get('tier_attempts') or []:
                if _att.get('tier_index') == current_idx:
                    cached_alts_at_preferred = _att.get('cached_hits_found', 0) or 0
                    uncached_alts_at_preferred = _att.get('uncached_hits_found', 0) or 0
                    break

            imdb_id = record.get('imdbId')
            if not imdb_id:
                logger.info(f"[blackhole] Compromise skipped for {orig_filename}: "
                            "no IMDb ID on arr record")
                return False

            # Season-pack probe (shows only, opt-in) tries for a cached
            # PACK at the PREFERRED tier BEFORE dropping — a cached pack
            # at 2160p beats a cached episode at 1080p for a show with
            # many holes.  A successful pack grab does NOT advance the
            # tier: per-episode grabs stay at the preferred tier going
            # forward, and the pack just back-fills holes.
            if (self._season_pack_enabled()
                    and media_type == 'series'
                    and not tier_state.get('season_pack_attempted')):
                min_missing = self._int_env('SEASON_PACK_FALLBACK_MIN_MISSING', 4, minimum=1)
                # Clamp ratio to [0, 1] so a typo in .env can't permanently
                # disable or over-trigger the gate.  Default 0.4 matches
                # Phase 7 plan; min_ratio=0 (user override) disables the
                # ratio check and falls back to pure min_missing.
                min_ratio = self._float_env(
                    'SEASON_PACK_FALLBACK_MIN_RATIO', 0.4,
                    minimum=0.0, maximum=1.0,
                )
                pack = find_season_pack_candidate(
                    arr_client=arr_client,
                    series_id=context['series_id'],
                    season_number=context.get('season'),
                    tier_label=preferred_tier,
                    min_missing=min_missing,
                    min_seeders=min_seeders,
                    only_cached=only_cached,
                    min_ratio=min_ratio,
                )
                if pack:
                    logger.info(f"[blackhole] Compromise: season-pack candidate "
                                f"{pack.get('title')} at {preferred_tier} for "
                                f"{orig_filename}")
                    submitted = self._submit_compromise_candidate(
                        pack, debrid_handler, orig_filename, orig_path, label,
                        debrid=debrid,
                        compromise_meta={
                            'preferred_tier': preferred_tier,
                            'grabbed_tier': preferred_tier,
                            'reason': 'season_pack_before_tier_drop',
                            'strategy': 'season_pack',
                            'dwell_seconds': dwell_seconds,
                            'cached_alts_at_preferred': cached_alts_at_preferred,
                            'uncached_alts_at_preferred': uncached_alts_at_preferred,
                        },
                        advance_state=None,
                    )
                    if submitted:
                        # Only consume the pack-probe flag on success —
                        # a transient debrid failure on a GOOD pack
                        # candidate must not prevent the next retry
                        # from trying the pack again.
                        RetryMeta.mark_season_pack_attempted(orig_path)
                        return True
                    # Pack submit failed — fall through to tier-drop
                    # on this same pass rather than wasting a full
                    # retry cycle on the already-fetched tier_state.
                    logger.info(f"[blackhole] Pack submit failed; falling "
                                f"through to tier-drop for {orig_filename}")
                else:
                    # No pack candidate — mark the probe so we don't hit
                    # Torrentio every retry cycle for a show that has
                    # nothing cached at the preferred tier.
                    RetryMeta.mark_season_pack_attempted(orig_path)

            # Tier-drop compromise: probe one tier down, grab best cached.
            next_tier = tier_order[current_idx + 1]
            candidate = find_compromise_candidate(
                arr_client=arr_client, imdb_id=imdb_id,
                tier_label=next_tier, min_seeders=min_seeders,
                only_cached=only_cached, context=context,
            )
            if not candidate:
                logger.info(f"[blackhole] Compromise: no cached {next_tier} "
                            f"candidate for {orig_filename}")
                return False

            logger.info(f"[blackhole] Compromise: grabbing {next_tier} candidate "
                        f"{candidate.get('title')} for {orig_filename} "
                        f"(dropped from {preferred_tier})")
            return self._submit_compromise_candidate(
                candidate, debrid_handler, orig_filename, orig_path, label,
                debrid=debrid,
                compromise_meta={
                    'preferred_tier': preferred_tier,
                    'grabbed_tier': next_tier,
                    'reason': reason,
                    'strategy': 'tier_drop',
                    'dwell_seconds': dwell_seconds,
                    'cached_alts_at_preferred': cached_alts_at_preferred,
                    'uncached_alts_at_preferred': uncached_alts_at_preferred,
                },
                advance_state={
                    'new_tier_index': current_idx + 1,
                    'reason': reason,
                },
            )
        except Exception as e:
            logger.warning(f"[blackhole] Compromise evaluation failed for "
                           f"{orig_filename}: {e}")
            return False

    def _submit_compromise_candidate(self, candidate, debrid_handler,
                                     orig_filename, orig_path, label,
                                     compromise_meta, advance_state,
                                     debrid=None):
        """Submit the candidate's magnet via *debrid_handler*.

        Mirrors the magnet-submission shape of ``_try_releases``'s inner
        loop — factored separately because the compromise path needs to
        record distinct pending/history/notification lineage and tier
        state on success.  Returns True iff the debrid service accepted
        the magnet; on False the caller falls through to the existing
        ``failed/`` path.
        """
        import tempfile

        info_hash = (candidate.get('info_hash') or '').strip()
        # Defence-in-depth: Torrentio results flow through search.py's
        # _HASH_RE filter, but a future caller could feed us a handcrafted
        # candidate dict.  Re-validate before building a magnet URI — a
        # malformed hash would get POSTed to the debrid provider as-is.
        if not info_hash or not re.match(r'^[a-fA-F0-9]{40}$', info_hash):
            logger.warning("[blackhole] Compromise candidate has malformed info_hash")
            return False
        magnet = f'magnet:?xt=urn:btih:{info_hash}'

        tmp_fd, tmp_path = tempfile.mkstemp(suffix='.magnet', prefix='_compromise_')
        success = False
        result = None
        try:
            with os.fdopen(tmp_fd, 'w') as f:
                f.write(magnet)
            success, result = debrid_handler(tmp_path)
        except Exception as e:
            logger.warning(f"[blackhole] Compromise submit errored: {e}")
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass

        if not success:
            logger.info(f"[blackhole] Compromise submission rejected by debrid: "
                        f"{str(result)[:100]}")
            return False

        # Remove the original so retries don't resubmit the rejected hash
        try:
            os.remove(orig_path)
        except OSError as e:
            logger.warning(f"[blackhole] Could not remove original after compromise: {e}")

        # Advance tier state BEFORE starting the monitor so a crash between
        # submit and monitor does not leave the item stuck at the old tier.
        # NB: RetryMeta addresses the sidecar ``<orig_path>.meta``, not
        # ``orig_path`` itself — removing the torrent/magnet above does
        # not invalidate the sidecar we're about to mutate.
        if advance_state:
            RetryMeta.advance_tier(
                orig_path, advance_state['new_tier_index'], advance_state['reason'],
            )

        if self.symlink_enabled:
            torrent_id = self._extract_torrent_id(result, debrid=debrid)
            if torrent_id:
                self._start_monitor(torrent_id, orig_filename, label=label,
                                    compromise=compromise_meta, debrid=debrid)

        title = candidate.get('title', '?')
        preferred = compromise_meta['preferred_tier']
        grabbed = compromise_meta['grabbed_tier']
        strategy = compromise_meta['strategy']
        body = (f'{orig_filename}: grabbed {grabbed} '
                f'(preferred {preferred}, strategy={strategy}) — {title[:80]}')
        # Phase 7: Apprise-only opt-out.  Invariant I7 is preserved —
        # history + pending_monitors annotation still fire below, so the
        # dashboard compromise trail stays intact even when the user
        # silences external notifications.
        notify_enabled = str(os.environ.get(
            'QUALITY_COMPROMISE_NOTIFY', 'true')).lower() == 'true'
        if _notify and notify_enabled:
            _notify('compromise_grabbed', 'Blackhole: Quality Compromise', body,
                    level='info')
        if _history:
            _mt, _ep = _enrich_for_history(orig_filename)
            merged_meta = dict(compromise_meta)
            merged_meta.setdefault('cause', 'compromise_grab')
            _history.log_event(
                'compromise_grabbed', orig_filename,
                episode=_ep, source='blackhole',
                detail=body, media_title=_mt,
                meta=merged_meta,
            )
        return True

    def _try_releases(self, releases, debrid_handler, orig_filename, orig_path, label=None, debrid=None):
        """Try magnet releases one by one until one succeeds on the debrid service.

        Only tries releases with magnet links (direct hashes) to avoid
        the 404 problem with torrent file download URLs.
        Skips the original release's info hash.

        *debrid* names the provider behind *debrid_handler* — without it,
        ID extraction and the monitor default to the primary service and
        misparse/mispoll when the handler is the routed alternative.
        """
        import tempfile

        # Extract original info hash to skip it
        orig_hash = self._extract_info_hash_from_file(orig_path)
        tried = 0
        max_tries = 5

        for r in releases:
            if tried >= max_tries:
                break
            if r.get('rejected'):
                continue
            guid = r.get('guid', '')
            if not guid.startswith('magnet:'):
                continue

            # Extract info hash from magnet URI
            m = re.search(r'btih:([A-Fa-f0-9]+)', guid, re.IGNORECASE)
            if not m:
                continue
            info_hash = m.group(1).upper()

            # Skip if same hash as the one that was rejected
            if orig_hash and info_hash == orig_hash.upper():
                continue

            # Skip blocklisted hashes
            if _blocklist and _blocklist.is_blocked(info_hash):
                logger.debug(f"[blackhole] Skipping blocklisted alternative: {info_hash[:16]}...")
                continue

            tried += 1
            alt_title = r.get('title', 'unknown')
            logger.info(f"[blackhole] Trying alternative release: {alt_title[:60]} (hash {info_hash})")

            # Write magnet to a temp file outside watch_dir to avoid scanner pickup
            import tempfile
            tmp_fd, tmp_path = tempfile.mkstemp(suffix='.magnet', prefix='_alt_')
            try:
                with os.fdopen(tmp_fd, 'w') as f:
                    f.write(guid)
                success, result = debrid_handler(tmp_path)
                if success:
                    logger.info(f"[blackhole] Alternative release accepted: {alt_title[:60]}")
                    # Clean up original file
                    try:
                        os.remove(orig_path)
                    except OSError as e:
                        logger.warning(f"[blackhole] Could not remove original after alt-retry: {e}")
                    # Start symlink monitoring
                    if self.symlink_enabled:
                        torrent_id = self._extract_torrent_id(result, debrid=debrid)
                        if torrent_id:
                            self._start_monitor(torrent_id, orig_filename, label=label,
                                                debrid=debrid)
                    if _notify:
                        _notify('download_complete', 'Blackhole: Alt Release Found',
                                f'Original rejected, using: {alt_title[:60]}')
                    return True
                else:
                    logger.debug(f"[blackhole] Alternative also rejected: {alt_title[:60]}: {str(result)[:100]}")
            except Exception as e:
                logger.debug(f"[blackhole] Error trying alternative {alt_title[:60]}: {e}")
            finally:
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

        # Phase 5 feeds an empty release list through when the arr's
        # indexers return nothing — demote to debug in that case because
        # the compromise path may still succeed and the WARNING would
        # otherwise fire in every normal "no arr alts, Torrentio saves
        # the day" flow.
        if tried > 0:
            logger.warning(f"[blackhole] No working alternative found for {orig_filename} (tried {tried})")
        else:
            logger.debug(f"[blackhole] No arr alternatives to try for {orig_filename}")
        return False

    @staticmethod
    def _extract_info_hash_from_file(file_path):
        """Extract info hash from a .magnet or .torrent file.

        For .magnet: parses the btih: URI parameter.
        For .torrent: locates the bencoded 'info' dict and SHA1 hashes
        its raw bytes (the standard BitTorrent info hash computation).
        """
        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.magnet':
            try:
                with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                    content = f.read().strip()
                m = re.search(r'btih:([A-Fa-f0-9]+)', content, re.IGNORECASE)
                if m:
                    return m.group(1).upper()
            except OSError:
                pass
        elif ext == '.torrent':
            try:
                with open(file_path, 'rb') as f:
                    data = f.read()
                # Find the raw bytes of the 'info' value in the bencoded torrent.
                # Bencode format: ...4:info<value>... where <value> starts with 'd'
                # We find the start of the info value and extract to its matching end.
                marker = b'4:infod'
                idx = data.find(marker)
                if idx == -1:
                    return None
                info_start = idx + len(b'4:info')  # start of the dict value ('d...')
                # Walk the bencoded structure to find the matching 'e'
                info_end = _bencode_end(data, info_start)
                if info_end is not None:
                    info_bytes = data[info_start:info_end]
                    return hashlib.sha1(info_bytes).hexdigest().upper()
            except (OSError, ValueError):
                pass
        return None

    # ── File processing ──────────────────────────────────────────────

    def _process_file(self, file_path, label=None):
        """Process a single torrent/magnet file.

        *label* is the per-arr routing label derived from the subdir of
        ``watch_dir`` containing the file, or None for flat-mode files.
        """
        filename = os.path.basename(file_path)
        if label:
            logger.info(f"[blackhole] Processing: {filename} [label={label}]")
        else:
            logger.info(f"[blackhole] Processing: {filename}")

        # Check local library before submitting to debrid
        if self._check_local_library(filename):
            try:
                os.remove(file_path)
                logger.info(f"[blackhole] Removed {filename} (local duplicate)")
            except OSError as e:
                logger.warning(f"[blackhole] Could not remove {filename}: {e}")
            try:
                from utils.metrics import metrics
                metrics.inc('blackhole_processed', {'status': 'skipped_local'})
            except Exception:
                pass
            return

        # Extract hash once — used by blocklist, dedup, and cache-require
        # gates below.  Falls back to None for files we cannot parse
        # (malformed .magnet, unreadable .torrent); downstream gates then
        # treat "unknown hash" as "can't dedup / can't cache-check" and let
        # the file through to the handler.
        info_hash = self._extract_info_hash_from_file(file_path)

        # Check blocklist before submitting to debrid
        if _blocklist and info_hash and _blocklist.is_blocked(info_hash):
            logger.info(f"[blackhole] Skipping blocklisted torrent: {filename} ({info_hash[:16]}...)")
            if _history:
                _mt, _ep = _enrich_for_history(filename)
                _history.log_event('blocklisted', filename, episode=_ep, source='blackhole',
                                   detail=f'Skipped — info hash is blocklisted',
                                   meta={'cause': 'blocklisted_hash',
                                         'info_hash': info_hash},
                                   media_title=_mt)
            try:
                os.remove(file_path)
            except OSError as e:
                logger.warning(f"[blackhole] Could not remove blocklisted file {filename}: {e}")
            return

        # Decide which debrid this grab routes to (plan 39 phase 2).
        # Cache-aware mode probes both configured debrids and picks
        # whichever has the hash cached; primary_only always returns
        # the configured primary.  Single-debrid setups collapse to
        # the only configured debrid.  The chosen ``debrid`` value then
        # threads through every downstream call (dedup, cache, add,
        # monitor, symlink) so a TB-routed grab never touches RD state.
        debrid = self._route_grab(info_hash)
        api_key = self._api_key_for(debrid)

        # Debrid-account dedup — skip hashes already on the debrid account.
        # Without this, Sonarr/Radarr re-grabs of the same release after a
        # failed import produce duplicate torrent entries the user has to
        # clean up manually.  Distinct from ``BLACKHOLE_DEDUP_ENABLED``
        # (local-filesystem library dedup) — they can be toggled independently.
        debrid_dedup_enabled = str(os.environ.get('BLACKHOLE_DEBRID_DEDUP_ENABLED', 'true')).lower() == 'true'
        require_cached = str(os.environ.get('BLACKHOLE_REQUIRE_CACHED', 'false')).lower() == 'true'

        # Strict-mode bypass: with require_cached ON and no API key, every
        # hash probe returns None → every drop gets deleted.  That's a
        # misconfiguration, not "provider says uncached" — leave the file
        # alone so the user sees the problem and the drop survives once
        # they fix the key.
        if require_cached and not api_key:
            logger.error(
                f"[blackhole] BLACKHOLE_REQUIRE_CACHED is on but {debrid} "
                f"API key is missing — leaving {filename} in watch dir"
            )
            return

        # Strict-mode bypass: a file whose info hash could not be extracted
        # (bencode corruption, missing ``btih:``) must NOT slip past a gate
        # that is explicitly supposed to reject uncached content.  Dedup can
        # safely fall through — "can't dedup" is a best-effort degradation —
        # but cache-required is a safety gate and "can't verify" must mean
        # "refuse".
        if require_cached and info_hash is None:
            logger.warning(
                f"[blackhole] Refusing {filename}: info hash unavailable, "
                f"require-cached is ON"
            )
            if _history:
                _mt, _ep = _enrich_for_history(filename)
                _history.log_event('uncached_rejected', filename, episode=_ep, source='blackhole',
                                   detail='Refused — info hash unavailable under strict mode',
                                   meta={'cause': 'uncached_rejected',
                                         'provider': debrid,
                                         'reason': 'info_hash_unavailable'},
                                   media_title=_mt)
            try:
                os.remove(file_path)
            except OSError as e:
                logger.warning(f"[blackhole] Could not remove refused file {filename}: {e}")
            try:
                from utils.metrics import metrics
                metrics.inc('blackhole_processed', {'status': 'skipped_uncached'})
            except Exception:
                pass
            return

        if info_hash and (debrid_dedup_enabled or require_cached):
            # utils.search is a first-party, always-shipped module — an
            # ImportError here would be a hard bug, not a missing optional
            # dependency, so let it propagate.
            from utils.search import _existing_hashes, check_debrid_cache

            lowered = info_hash.lower()

            if debrid_dedup_enabled:
                existing = _existing_hashes(debrid, api_key)
                if existing is not None and lowered in existing:
                    logger.info(f"[blackhole] Skipping duplicate: {filename} already in {debrid} account")
                    if _history:
                        _mt, _ep = _enrich_for_history(filename)
                        _history.log_event('duplicate', filename, episode=_ep, source='blackhole',
                                           detail=f'Skipped — already in {debrid}',
                                           meta={'cause': 'duplicate_skipped',
                                                 'info_hash': info_hash,
                                                 'provider': debrid},
                                           media_title=_mt)
                    try:
                        os.remove(file_path)
                    except OSError as e:
                        logger.warning(f"[blackhole] Could not remove duplicate file {filename}: {e}")
                    try:
                        from utils.metrics import metrics
                        metrics.inc('blackhole_processed', {'status': 'skipped_duplicate'})
                    except Exception:
                        pass
                    return

            if require_cached:
                cache_map = check_debrid_cache([lowered], service=debrid,
                                               api_key=api_key)
                cached = cache_map.get(lowered)
                if cached is False:
                    # Provider confirmed THIS hash uncached.  Before
                    # deleting, see if a DIFFERENT release of the same title
                    # is cached on TorBox and grab that instead — otherwise
                    # an abundantly-cached title silently falls to "Wanted".
                    if self._try_torbox_cached_alternative(file_path, filename, info_hash, debrid, label=label):
                        return
                    # Provider confirmed uncached — safe to delete; nothing
                    # to wait for.
                    cache_label = 'uncached'
                    logger.info(f"[blackhole] Skipping {cache_label}: {filename} on {debrid}")
                    if _history:
                        _mt, _ep = _enrich_for_history(filename)
                        _history.log_event('uncached_rejected', filename, episode=_ep, source='blackhole',
                                           detail=f'Skipped — {cache_label} on {debrid}',
                                           meta={'cause': 'uncached_rejected',
                                                 'info_hash': info_hash,
                                                 'provider': debrid},
                                           media_title=_mt)
                    try:
                        os.remove(file_path)
                    except OSError as e:
                        logger.warning(f"[blackhole] Could not remove uncached file {filename}: {e}")
                    try:
                        from utils.metrics import metrics
                        metrics.inc('blackhole_processed', {'status': 'skipped_uncached'})
                    except Exception:
                        pass
                    return
                if cached is None:
                    # Cross-probe: only TB has a working cache endpoint; avoid defer-forever on RD/AD.
                    # `debrid != 'torbox'` skip: a TB-routed file that returned None already had its
                    # chance — a re-probe would be circular and burn a second rate-limit slot.
                    _debrid_lc = (debrid or '').lower()
                    tb_key = self._api_key_for('torbox') if _debrid_lc != 'torbox' else None
                    tb_cached = None
                    if tb_key:
                        try:
                            tb_map = check_debrid_cache([lowered], service='torbox', api_key=tb_key)
                            tb_cached = tb_map.get(lowered) if isinstance(tb_map, dict) else None
                        except Exception as e:
                            # Don't orphan the file on an unexpected TB-probe raise — fall through
                            # to defer.  ``check_debrid_cache`` catches network/JSON errors and
                            # returns None, but a schema change at TB could surface a KeyError.
                            logger.debug(f"[blackhole] TB cross-probe raised for {filename}: {e}")
                    if tb_key and tb_cached is False:
                        # This exact hash is uncached on both the routed
                        # debrid and TorBox — but a different release of the
                        # same title may still be TB-cached.  Try to recover
                        # before deleting.
                        if self._try_torbox_cached_alternative(file_path, filename, info_hash, debrid, label=label):
                            return
                        logger.info(
                            f"[blackhole] Skipping uncached (cross-confirmed via TB): "
                            f"{filename} routed to {debrid}"
                        )
                        if _history:
                            _mt, _ep = _enrich_for_history(filename)
                            _history.log_event('uncached_rejected', filename, episode=_ep,
                                               source='blackhole',
                                               detail=f'Skipped — uncached on {debrid} '
                                                      f'(cross-confirmed via torbox)',
                                               meta={'cause': 'uncached_rejected',
                                                     'info_hash': info_hash,
                                                     'provider': debrid,
                                                     'cross_confirmed_via': 'torbox'},
                                               media_title=_mt)
                        try:
                            os.remove(file_path)
                        except OSError as e:
                            logger.warning(
                                f"[blackhole] Could not remove uncached file {filename}: {e}"
                            )
                        try:
                            from utils.metrics import metrics
                            metrics.inc('blackhole_processed',
                                        {'status': 'skipped_uncached_cross_confirmed'})
                        except Exception:
                            pass
                        return

                    # Unknown — API outage, rate-limit, key rotation, or
                    # RD's deprecated endpoint.  Do NOT delete: leave the
                    # drop in the watch dir so the next poll retries.  An
                    # AD/TB blip during a Sonarr grab burst must not
                    # silently eat every in-flight drop.
                    logger.warning(
                        f"[blackhole] Deferring {filename}: cache status unknown on "
                        f"{debrid} (API unavailable?) — leaving in watch dir"
                    )
                    try:
                        from utils.metrics import metrics
                        metrics.inc('blackhole_processed', {'status': 'deferred_cache_unknown'})
                    except Exception:
                        pass
                    return

        dispatch = {
            'realdebrid': self._add_to_realdebrid,
            'alldebrid': self._add_to_alldebrid,
            'torbox': self._add_to_torbox,
        }

        handler = dispatch.get(debrid)
        if not handler:
            logger.error(f"[blackhole] Unsupported debrid service: {debrid}")
            return

        try:
            success, result = handler(file_path, api_key=api_key)
            if success:
                logger.info(f"[blackhole] Added to {debrid}: {filename}")

                # Prime the dedup cache so a re-drop of the same .magnet before
                # TTL expiry is caught even if the debrid account list hasn't
                # been re-fetched yet.
                if info_hash:
                    try:
                        from utils.search import remember_added_hash
                        remember_added_hash(debrid, info_hash)
                    except ImportError:
                        pass

                # Record pending FIRST — prevents orphaned debrid torrents if
                # we crash before reaching file cleanup or notifications.
                # Guarded so a monitor failure doesn't block file cleanup.
                if self.symlink_enabled:
                    torrent_id = self._extract_torrent_id(result, debrid=debrid)
                    if torrent_id:
                        try:
                            self._start_monitor(torrent_id, filename, label=label, debrid=debrid)
                        except Exception as e:
                            logger.error(f"[blackhole] Failed to start monitor for {filename}: {e}")
                    else:
                        logger.warning(f"[blackhole] Could not extract torrent ID for symlink monitoring: {filename}")

                if _history:
                    _mt, _ep = _enrich_for_history(filename)
                    _history.log_event('grabbed', filename, episode=_ep, source='blackhole',
                                       detail=f'Submitted to {debrid}',
                                       meta={'cause': 'blackhole_grab_submitted',
                                             'provider': debrid},
                                       media_title=_mt)
                try:
                    os.remove(file_path)
                except OSError as e:
                    logger.warning(f"[blackhole] Could not remove {filename}: {e}")
                try:
                    from utils.metrics import metrics
                    metrics.inc('blackhole_processed', {'status': 'success'})
                except Exception:
                    pass

                if _notify:
                    if self.symlink_enabled:
                        _notify('download_complete', 'Blackhole: Torrent Submitted',
                                f'{filename} submitted to {debrid}, monitoring for symlinks')
                    else:
                        _notify('download_complete', 'Blackhole: Torrent Added',
                                f'{filename} added to {debrid}')
            else:
                logger.error(f"[blackhole] Failed to add {filename}: {result}")

                # Plan 41 phase A — filter-block cross-rescue BEFORE
                # alt-release search.  Cross-rescue tries the SAME hash on
                # the alt debrid (RD↔TB) and short-circuits when the alt
                # has it cached; alt-release tries a DIFFERENT release of
                # the same media.  They chain: cross-rescue first (cheap,
                # hit-cached only), falls through to alt-release on miss.
                from utils.debrid_routing import classify_add_failure
                if (classify_add_failure(result) == 'filter_block'
                        and info_hash
                        and self._attempt_add_time_rescue(
                            file_path, filename, info_hash, debrid, label, dispatch,
                        )):
                    return

                # On debrid rejection (infringing/blocked), try alternative release
                # in a background thread to avoid blocking the scan loop.
                # Skip if alts were already exhausted in a prior attempt.
                if self._is_debrid_rejection(result) and not self._alt_exhausted(file_path):
                    # Move file out of watch_dir BEFORE launching the thread
                    # to prevent the next scan cycle from picking it up again
                    staging_dir = self._alt_pending_dir(label)
                    os.makedirs(staging_dir, exist_ok=True)
                    staged_path = os.path.join(staging_dir, filename)
                    try:
                        os.rename(file_path, staged_path)
                    except OSError as e:
                        logger.warning(
                            f"[blackhole] Could not stage {filename} for alt-retry: {e}. "
                            f"Skipping alt-retry to prevent duplicate submission."
                        )
                        # Fall through to normal failed/ path below
                    else:
                        threading.Thread(
                            target=self._try_alternative_release,
                            args=(filename, staged_path, handler, label, debrid),
                            daemon=True,
                            name=f'alt-retry-{filename[:30]}',
                        ).start()
                        return  # Alt-retry thread handles cleanup

                error_dir = self._failed_dir(label)
                os.makedirs(error_dir, exist_ok=True)
                dest = os.path.join(error_dir, filename)
                if os.path.exists(dest):
                    base, fext = os.path.splitext(filename)
                    dest = os.path.join(error_dir, f"{base}_{int(time.time())}{fext}")
                os.rename(file_path, dest)
                try:
                    from utils.metrics import metrics
                    metrics.inc('blackhole_processed', {'status': 'failed'})
                except Exception:
                    pass
                # Track retry state
                retries, _ = RetryMeta.read(dest)
                RetryMeta.write(dest, retries + 1)
                if retries + 1 >= MAX_RETRIES:
                    logger.error(f"[blackhole] {filename} has permanently failed after {MAX_RETRIES} attempts")
                    if _notify:
                        _notify('download_error', 'Blackhole: Permanent Failure',
                                f'{filename} failed {MAX_RETRIES} times and will not be retried',
                                level='error')
        except Exception as e:
            logger.error(f"[blackhole] Error processing {filename}: {e}")

    def _retry_failed(self):
        """Scan failed/ directory and retry eligible files.

        Supports both flat layout (watch_dir/failed/<file>) and labeled
        layout (watch_dir/failed/<label>/<file>). Labeled files are moved
        back to watch_dir/<label>/ so the next scan re-detects the label.
        """
        failed_root = os.path.join(self.watch_dir, 'failed')
        if not os.path.exists(failed_root):
            return

        # (label, file_path, filename) triples to retry
        candidates = []
        try:
            for entry in os.listdir(failed_root):
                ep = os.path.join(failed_root, entry)
                if os.path.isfile(ep):
                    candidates.append((None, ep, entry))
                elif os.path.isdir(ep) and _is_valid_label(entry):
                    try:
                        for sub in os.listdir(ep):
                            sp = os.path.join(ep, sub)
                            if os.path.isfile(sp):
                                candidates.append((entry, sp, sub))
                    except OSError:
                        continue
        except OSError:
            return

        for label, file_path, filename in candidates:
            ext = os.path.splitext(filename)[1].lower()
            if ext not in self.SUPPORTED_EXTENSIONS:
                continue

            retries, last_attempt = RetryMeta.read(file_path)

            if retries >= MAX_RETRIES:
                continue

            # Don't retry files where alt-release search was already exhausted
            # (the original hash is debrid-blocked, retrying submits the same hash)
            if self._alt_exhausted(file_path):
                continue

            # Determine backoff delay for this retry
            delay_idx = min(retries, len(RETRY_SCHEDULE) - 1)
            delay = RETRY_SCHEDULE[delay_idx]

            if time.time() - last_attempt < delay:
                continue

            logger.info(f"[blackhole] Retrying failed file: {filename} (attempt {retries + 1}/{MAX_RETRIES})"
                        f"{f' [label={label}]' if label else ''}")
            try:
                from utils.metrics import metrics
                metrics.inc('blackhole_retry')
            except Exception:
                pass

            # Move back to watch dir (or label subdir) for reprocessing.
            # Preserving the label subdir keeps per-arr routing intact.
            if label:
                retry_dir = os.path.join(self.watch_dir, label)
                os.makedirs(retry_dir, exist_ok=True)
                retry_path = os.path.join(retry_dir, filename)
            else:
                retry_path = os.path.join(self.watch_dir, filename)
            # Refuse to clobber a fresh drop with the same filename — POSIX
            # os.rename silently overwrites. Leave the failed file in place
            # and try again next tick; the arr's re-grab wins.
            if os.path.exists(retry_path):
                logger.debug(
                    f"[blackhole] Skipping retry of {filename}: a newer file is already at {retry_path}"
                )
                continue
            try:
                RetryMeta.remove(file_path)
                os.rename(file_path, retry_path)
            except OSError as e:
                logger.error(f"[blackhole] Failed to move {filename} for retry: {e}")

    def _scan(self):
        """Scan watch directory for new files.

        Supports two layouts:
          - Flat: .torrent/.magnet files sit directly in watch_dir → label=None
          - Labeled: one level of subdirectories, each subdir name becomes
            the routing label (e.g. /watch/sonarr/x.torrent → label="sonarr")
        Both layouts coexist. Invalid label names are logged and skipped.
        """
        if not os.path.exists(self.watch_dir):
            return

        now = time.time()
        watch_realpath = os.path.realpath(self.watch_dir)

        for entry in os.listdir(self.watch_dir):
            entry_path = os.path.join(self.watch_dir, entry)

            # Guard against symlink escapes
            real_path = os.path.realpath(entry_path)
            if not real_path.startswith(watch_realpath + os.sep) and real_path != watch_realpath:
                continue

            if os.path.isfile(entry_path):
                self._maybe_process_watch_file(entry_path, entry, now, label=None)
                continue

            if not os.path.isdir(entry_path):
                continue

            # Skip reserved subdirs (failed/, .alt_pending/) — handled separately
            if entry.lower() in _RESERVED_LABELS:
                continue

            # Validate label name. Invalid names are skipped (not processed as
            # unlabeled — that would defeat the purpose and surprise the user).
            if not _is_valid_label(entry):
                logger.warning(f"[blackhole] Ignoring invalid label subdir: {entry!r} "
                               f"(labels must be [A-Za-z0-9_-], max {_LABEL_MAX_LEN} chars)")
                continue

            try:
                sub_entries = os.listdir(entry_path)
            except OSError as e:
                logger.debug(f"[blackhole] Cannot list label subdir {entry}: {e}")
                continue

            for fname in sub_entries:
                fpath = os.path.join(entry_path, fname)
                # Symlink-escape guard (file must remain under watch_dir)
                fp_real = os.path.realpath(fpath)
                if not fp_real.startswith(watch_realpath + os.sep):
                    continue
                if not os.path.isfile(fpath):
                    continue
                self._maybe_process_watch_file(fpath, fname, now, label=entry)

    def _maybe_process_watch_file(self, file_path, filename, now, label):
        """Shared pre-processing: skip in-flight writes, dispatch on extension."""
        try:
            if now - os.path.getmtime(file_path) < 2.0:
                return
        except OSError:
            return
        ext = os.path.splitext(filename)[1].lower()
        if ext in self.SUPPORTED_EXTENSIONS:
            self._process_file(file_path, label=label)

    def _recover_alt_pending(self):
        """On startup, move stranded .alt_pending files to failed/.

        If the container was killed while an alt-retry thread was running,
        files in .alt_pending/ would be orphaned with no recovery path.
        Walks both flat layout (.alt_pending/*) and labeled layout
        (.alt_pending/<label>/*), preserving the label in the failed/ move.
        """
        staging_root = os.path.join(self.watch_dir, '.alt_pending')
        if not os.path.isdir(staging_root):
            return

        # (label, src_path, filename) triples
        stranded = []
        try:
            for entry in os.listdir(staging_root):
                ep = os.path.join(staging_root, entry)
                if os.path.isfile(ep):
                    stranded.append((None, ep, entry))
                elif os.path.isdir(ep) and _is_valid_label(entry):
                    try:
                        for sub in os.listdir(ep):
                            sp = os.path.join(ep, sub)
                            if os.path.isfile(sp):
                                stranded.append((entry, sp, sub))
                    except OSError:
                        continue
        except OSError:
            return

        for label, src, filename in stranded:
            # Strip the ``.rescue-<uuid8>-`` prefix the rescue path adds
            # before staging (plan 41 phase A).  Without this restore,
            # Sonarr/Radarr's blackhole-import would not recognise the
            # mangled filename in ``failed/`` and the file would silently
            # rot.  Alt-release-staging files (the older code path) have
            # no prefix; the regex is a no-op for them.
            recovered_filename = _restore_rescue_basename(filename)
            error_dir = self._failed_dir(label)
            os.makedirs(error_dir, exist_ok=True)
            dest = os.path.join(error_dir, recovered_filename)
            if os.path.exists(dest):
                base, fext = os.path.splitext(recovered_filename)
                dest = os.path.join(error_dir, f"{base}_{int(time.time())}{fext}")
            try:
                os.rename(src, dest)
                is_rescue_orphan = recovered_filename != filename
                if not is_rescue_orphan:
                    # Alt-release staging: the original hash is debrid-blocked
                    # and the alt search was interrupted — mark alt_exhausted
                    # via the centralised helper so tier_state on the
                    # recovered sidecar is preserved.
                    #
                    # Rescue-staged orphans (``.rescue-`` prefix) must NOT be
                    # marked: they were staged by the add-time cross-rescue,
                    # which never ran an alt-release search — flagging them
                    # exhausted makes _retry_failed skip the file forever.
                    # Leaving the sidecar alone lets the normal retry
                    # schedule re-submit (and re-rescue) the release.
                    RetryMeta.mark_alt_exhausted(dest)
                tag = f" [label={label}]" if label else ""
                origin = " [rescue-orphan]" if is_rescue_orphan else ""
                logger.warning(f"[blackhole] Recovered stranded alt-pending file: {recovered_filename}{origin}{tag}")
            except OSError as e:
                logger.warning(f"[blackhole] Could not recover {filename} from alt_pending: {e}")

    def run(self):
        """Main loop - scan at poll_interval."""
        logger.info(f"[blackhole] Watching {self.watch_dir} (poll: {self.poll_interval}s, service: {self.debrid_service})")
        try:
            self._recover_alt_pending()
        except Exception as e:
            logger.error(f"[blackhole] _recover_alt_pending failed at startup: {e}")
        if self.symlink_enabled:
            logger.info(f"[blackhole] Symlink mode enabled: completed={self.completed_dir}, "
                        f"mount={self.rclone_mount}, target_base={self.symlink_target_base}, "
                        f"timeout={self.mount_poll_timeout}s, interval={self.mount_poll_interval}s, "
                        f"max_age={self.symlink_max_age}h")
            try:
                self._resume_pending_monitors()
            except Exception as e:
                # Even a catastrophic load failure must not kill the worker
                # thread — the main scan loop will still handle new drops.
                logger.error(f"[blackhole] _resume_pending_monitors failed at startup: {e}")

        while not self._stop_event.is_set():
            try:
                self._scan()
                self._retry_failed()

                # Run symlink cleanup every 5 minutes
                if self.symlink_enabled and (time.time() - self._last_cleanup) > 300:
                    self._last_cleanup = time.time()
                    self._cleanup_symlinks()
            except Exception as e:
                logger.error(f"[blackhole] Scan error: {e}")
            self._stop_event.wait(self.poll_interval)

    def stop(self):
        self._stop_event.set()


def setup():
    """Initialize and start the blackhole watcher if enabled."""
    global _watcher
    from base import config
    RDAPIKEY = config.RDAPIKEY
    ADAPIKEY = config.ADAPIKEY

    blackhole_enabled = os.environ.get('BLACKHOLE_ENABLED', 'false').lower() == 'true'
    if not blackhole_enabled:
        return None

    watch_dir = os.environ.get('BLACKHOLE_DIR', '/watch')
    try:
        poll_interval = int(os.environ.get('BLACKHOLE_POLL_INTERVAL', '5'))
    except (ValueError, TypeError):
        logger.warning("[blackhole] Invalid BLACKHOLE_POLL_INTERVAL, defaulting to 5s")
        poll_interval = 5

    # Plan 39 phase 2 — collect ALL configured debrid keys so the watcher
    # can route each grab independently.  ``debrid_service`` and
    # ``debrid_api_key`` below resolve to the *primary* (legacy single-
    # value fields kept for back-compat with the per-method default-
    # argument path).
    from utils.debrid_routing import (
        resolve_primary, resolve_routing_mode, configured_debrids,
        VALID_DEBRIDS,
    )
    tb_key = os.environ.get('TORBOX_API_KEY')
    debrid_api_keys = {}
    if RDAPIKEY:
        debrid_api_keys['realdebrid'] = RDAPIKEY
    if ADAPIKEY:
        debrid_api_keys['alldebrid'] = ADAPIKEY
    if tb_key:
        debrid_api_keys['torbox'] = tb_key

    if not debrid_api_keys:
        logger.error("[blackhole] No debrid API key found. Blackhole disabled.")
        return None

    # Validate BLACKHOLE_DEBRID / BLACKHOLE_DEBRID_PRIMARY when set.
    legacy = (os.environ.get('BLACKHOLE_DEBRID') or '').lower()
    if legacy and legacy not in VALID_DEBRIDS:
        logger.error(
            f"[blackhole] Unknown BLACKHOLE_DEBRID {legacy!r}. "
            f"Valid: {', '.join(sorted(VALID_DEBRIDS))}"
        )
        return None
    explicit_primary = (os.environ.get('BLACKHOLE_DEBRID_PRIMARY') or '').lower()
    if explicit_primary and explicit_primary not in VALID_DEBRIDS:
        logger.error(
            f"[blackhole] Unknown BLACKHOLE_DEBRID_PRIMARY {explicit_primary!r}. "
            f"Valid: {', '.join(sorted(VALID_DEBRIDS))}"
        )
        return None

    debrid_service = resolve_primary() or next(iter(debrid_api_keys))
    if debrid_service not in debrid_api_keys:
        logger.error(
            f"[blackhole] Primary debrid {debrid_service!r} has no API key configured. "
            f"Configured: {list(debrid_api_keys)}"
        )
        return None
    debrid_api_key = debrid_api_keys[debrid_service]

    routing_mode = resolve_routing_mode()
    if len(debrid_api_keys) >= 2:
        logger.info(
            f"[blackhole] Multi-debrid routing active — providers: "
            f"{sorted(debrid_api_keys)}, primary: {debrid_service}, "
            f"mode: {routing_mode}"
        )

    os.makedirs(watch_dir, exist_ok=True)

    # Symlink configuration
    symlink_enabled = os.environ.get('BLACKHOLE_SYMLINK_ENABLED', 'false').lower() == 'true'
    completed_dir = os.environ.get('BLACKHOLE_COMPLETED_DIR', '/completed')
    rclone_mount = os.environ.get('BLACKHOLE_RCLONE_MOUNT', '/data')
    # Auto-detect mount name subdirectory if not explicitly configured
    if rclone_mount == '/data' and os.environ.get('RCLONE_MOUNT_NAME'):
        mount_name = os.environ.get('RCLONE_MOUNT_NAME')
        candidate = os.path.join('/data', mount_name)
        if os.path.isdir(os.path.join(candidate, '__all__')) or os.path.isdir(os.path.join(candidate, 'shows')):
            rclone_mount = candidate
            logger.info(f"[blackhole] Auto-detected rclone mount: {rclone_mount}")
    symlink_target_base = os.environ.get('BLACKHOLE_SYMLINK_TARGET_BASE', '')

    try:
        mount_poll_timeout = int(os.environ.get('BLACKHOLE_MOUNT_POLL_TIMEOUT', '300'))
    except (ValueError, TypeError):
        logger.warning("[blackhole] Invalid BLACKHOLE_MOUNT_POLL_TIMEOUT, defaulting to 300s")
        mount_poll_timeout = 300

    try:
        mount_poll_interval = int(os.environ.get('BLACKHOLE_MOUNT_POLL_INTERVAL', '10'))
    except (ValueError, TypeError):
        logger.warning("[blackhole] Invalid BLACKHOLE_MOUNT_POLL_INTERVAL, defaulting to 10s")
        mount_poll_interval = 10

    try:
        symlink_max_age = int(os.environ.get('BLACKHOLE_SYMLINK_MAX_AGE', '72'))
    except (ValueError, TypeError):
        logger.warning("[blackhole] Invalid BLACKHOLE_SYMLINK_MAX_AGE, defaulting to 72h")
        symlink_max_age = 72

    if symlink_enabled:
        # Per-debrid target base validation (plan 39 phase 2).  Each
        # configured debrid needs its own ``symlink_target_base_for_debrid``
        # to resolve to a non-empty path — without it, TB-routed grabs
        # would create relative-path symlinks under the completed dir
        # (because ``_symlink_target_base_for`` falls back to the bare
        # instance default).  Catch this at startup rather than 12h later
        # when a TB grab actually lands.
        from utils.debrid_routing import symlink_target_base_for_debrid as _stb_for
        missing_bases = []
        for svc in debrid_api_keys.keys():
            if not _stb_for(svc):
                missing_bases.append(svc)
        if missing_bases:
            for svc in missing_bases:
                env_hint = (
                    'BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX' if svc == 'torbox'
                    else 'BLACKHOLE_SYMLINK_TARGET_BASE'
                )
                logger.error(
                    f"[blackhole] Symlink target base missing for debrid "
                    f"{svc!r}. Set {env_hint} (or the RD base so the "
                    f"TB-suffix fallback can derive it)."
                )
            return None
        if not symlink_target_base:
            logger.error("[blackhole] BLACKHOLE_SYMLINK_TARGET_BASE is required when symlinks are enabled")
            return None
        os.makedirs(completed_dir, exist_ok=True)

    # Local library dedup configuration
    dedup_enabled = os.environ.get('BLACKHOLE_DEDUP_ENABLED', 'false').lower() == 'true'
    local_library_tv = os.environ.get('BLACKHOLE_LOCAL_LIBRARY_TV', '')
    local_library_movies = os.environ.get('BLACKHOLE_LOCAL_LIBRARY_MOVIES', '')
    if dedup_enabled:
        logger.info(f"[blackhole] Local dedup enabled: tv={local_library_tv}, movies={local_library_movies}")

    _watcher = BlackholeWatcher(
        watch_dir, debrid_api_key, debrid_service, poll_interval,
        symlink_enabled=symlink_enabled,
        completed_dir=completed_dir,
        rclone_mount=rclone_mount,
        symlink_target_base=symlink_target_base,
        mount_poll_timeout=mount_poll_timeout,
        mount_poll_interval=mount_poll_interval,
        symlink_max_age=symlink_max_age,
        dedup_enabled=dedup_enabled,
        local_library_tv=local_library_tv,
        local_library_movies=local_library_movies,
        debrid_api_keys=debrid_api_keys,
    )
    thread = threading.Thread(target=_watcher.run, daemon=True)
    thread.start()
    return _watcher


def stop():
    """Stop the blackhole watcher if running."""
    if _watcher:
        _watcher.stop()
