"""Interactive debrid torrent search via Torrentio.

Allows users to search for torrents from the library detail view
and one-click add them to their debrid provider.  Uses urllib only
(no requests dependency).
"""

import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from base import load_secret_or_env
from utils.logger import get_logger

logger = get_logger()

_SEARCH_TIMEOUT = 10
_ADD_TIMEOUT = 15

# Quality tiers — higher score = better
_QUALITY_PATTERNS = [
    (re.compile(r'(?:2160p|4k|uhd)', re.IGNORECASE), '2160p', 4),
    (re.compile(r'1080p', re.IGNORECASE), '1080p', 3),
    (re.compile(r'720p', re.IGNORECASE), '720p', 2),
    (re.compile(r'480p', re.IGNORECASE), '480p', 1),
]

# Hash validation — 40-char hex
_HASH_RE = re.compile(r'^[a-fA-F0-9]{40}$')

# IMDb ID validation — tt followed by 7-8 digits
_IMDB_RE = re.compile(r'^tt\d{7,8}$')

# Safe magnet prefix
_MAGNET_PREFIX = 'magnet:?xt=urn:btih:'


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_torrentio_url():
    return (os.environ.get('TORRENTIO_URL') or '').rstrip('/')


_SERVICE_KEY_NAMES = {
    'realdebrid': 'rd_api_key',
    'alldebrid': 'ad_api_key',
    'torbox': 'torbox_api_key',
}


def _get_debrid_service():
    """Detect configured debrid service. Returns (service, api_key) or (None, None)."""
    rd = load_secret_or_env('rd_api_key')
    if rd:
        return 'realdebrid', rd
    ad = load_secret_or_env('ad_api_key')
    if ad:
        return 'alldebrid', ad
    tb = load_secret_or_env('torbox_api_key')
    if tb:
        return 'torbox', tb
    return None, None


def _resolve_service_key(service):
    """Resolve the API key for an explicitly-named debrid service, or None."""
    key_name = _SERVICE_KEY_NAMES.get(service)
    if not key_name:
        return None
    return load_secret_or_env(key_name)


def _safe_log_url(url):
    """Strip query parameters from a URL for safe logging (no credentials)."""
    parsed = urllib.parse.urlparse(url)
    return urllib.parse.urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))


def _urllib_get(url, headers=None, timeout=_SEARCH_TIMEOUT):
    """GET request returning parsed JSON or None."""
    req = urllib.request.Request(url, headers=headers or {})
    req.add_header('User-Agent', 'zurgarr/1.0')
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read(10 * 1024 * 1024).decode('utf-8'))
    except (urllib.error.URLError, urllib.error.HTTPError,
            json.JSONDecodeError, OSError, ValueError) as e:
        logger.warning(f"[search] GET {_safe_log_url(url)}: {type(e).__name__}")
        return None


def _urllib_post(url, data=None, json_body=None, headers=None,
                 timeout=_ADD_TIMEOUT, doseq=False):
    """POST request returning parsed JSON or None.

    ``doseq=True`` lets callers pass list-valued dict entries (or a list
    of ``(key, value)`` tuples) so repeated form fields like ``magnets[]``
    encode correctly — without it, ``urlencode`` stringifies the list as
    a single ``['m1', 'm2']`` value.  Default stays False so existing
    scalar-valued callers are unchanged.
    """
    hdrs = dict(headers or {})
    hdrs['User-Agent'] = 'zurgarr/1.0'
    if json_body is not None:
        body = json.dumps(json_body).encode('utf-8')
        hdrs['Content-Type'] = 'application/json'
    elif data is not None:
        body = urllib.parse.urlencode(data, doseq=doseq).encode('utf-8')
        hdrs['Content-Type'] = 'application/x-www-form-urlencoded'
    else:
        body = b''
    req = urllib.request.Request(url, data=body, headers=hdrs, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read(10 * 1024 * 1024)
            if not raw:
                return {}
            return json.loads(raw.decode('utf-8'))
    except (urllib.error.URLError, urllib.error.HTTPError,
            json.JSONDecodeError, OSError, ValueError) as e:
        logger.warning(f"[search] POST {_safe_log_url(url)}: {type(e).__name__}")
        return None


def parse_quality(title):
    """Extract quality label and numeric score from a release title.

    Returns {'label': '1080p', 'score': 3} or {'label': 'Unknown', 'score': 0}.
    """
    for pattern, label, score in _QUALITY_PATTERNS:
        if pattern.search(title):
            return {'label': label, 'score': score}
    return {'label': 'Unknown', 'score': 0}


def _parse_size_bytes(size_str):
    """Parse a human-readable size like '4.2 GB' into bytes."""
    if not size_str:
        return 0
    m = re.search(r'([\d.]+)\s*(GB|MB|TB|KB)', size_str, re.IGNORECASE)
    if not m:
        return 0
    val = float(m.group(1))
    unit = m.group(2).upper()
    multipliers = {'KB': 1024, 'MB': 1024**2, 'GB': 1024**3, 'TB': 1024**4}
    return int(val * multipliers.get(unit, 1))


def _parse_seeds(text):
    """Extract seeder count from Torrentio title metadata."""
    m = re.search(r'👤\s*(\d+)', text)
    if m:
        return int(m.group(1))
    m = re.search(r'seeders?[:\s]*(\d+)', text, re.IGNORECASE)
    if m:
        return int(m.group(1))
    return 0


def _parse_size_from_title(text):
    """Extract size string from Torrentio title metadata."""
    m = re.search(r'💾\s*([\d.]+\s*(?:GB|MB|TB|KB))', text, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r'([\d.]+\s*(?:GB|MB|TB))', text, re.IGNORECASE)
    if m:
        return m.group(1)
    return ''


def _parse_source(text):
    """Extract source/tracker name from Torrentio title metadata."""
    m = re.search(r'⚙️\s*(.+?)(?:\n|$)', text)
    if m:
        return m.group(1).strip()
    return ''


def _hash_to_magnet(info_hash):
    """Convert an info hash to a magnet URI."""
    return f'{_MAGNET_PREFIX}{info_hash}'


# ---------------------------------------------------------------------------
# F9.1 — Torrentio search
# ---------------------------------------------------------------------------

def search_torrentio(imdb_id, media_type='movie', season=None, episode=None):
    """Search Torrentio for streams matching an IMDb ID.

    Args:
        imdb_id: IMDb ID (e.g. 'tt1234567')
        media_type: 'movie' or 'series'
        season: Season number (for series)
        episode: Episode number (for series)

    Returns:
        List of dicts: [{title, info_hash, size_bytes, seeds, source_name,
                         quality: {label, score}}]
    """
    base_url = _get_torrentio_url()
    if not base_url:
        logger.warning("[search] TORRENTIO_URL not configured")
        return []

    if not imdb_id or not _IMDB_RE.match(imdb_id):
        logger.warning(f"[search] Invalid IMDb ID format: {imdb_id!r}")
        return []

    # Build URL
    stream_type = 'movie' if media_type == 'movie' else 'series'
    if stream_type == 'series' and season is not None and episode is not None:
        path = f'/stream/{stream_type}/{imdb_id}:{season}:{episode}.json'
    else:
        path = f'/stream/{stream_type}/{imdb_id}.json'

    url = base_url + path
    data = _urllib_get(url, timeout=_SEARCH_TIMEOUT)
    if not data:
        return []

    streams = data.get('streams', [])
    results = []
    seen_hashes = set()

    for stream in streams:
        info_hash = (stream.get('infoHash') or '').strip().lower()
        if not info_hash or not _HASH_RE.match(info_hash):
            continue
        if info_hash in seen_hashes:
            continue
        seen_hashes.add(info_hash)

        title_text = stream.get('title', '')
        name_text = stream.get('name', '')

        # The release title is typically the first line of the title field
        release_title = title_text.split('\n')[0].strip() if title_text else name_text

        quality = parse_quality(release_title or title_text)
        seeds = _parse_seeds(title_text)
        size_str = _parse_size_from_title(title_text)
        size_bytes = _parse_size_bytes(size_str)
        source_name = _parse_source(title_text)

        results.append({
            'title': release_title,
            'info_hash': info_hash,
            'size_bytes': size_bytes,
            'seeds': seeds,
            'source_name': source_name,
            'quality': quality,
        })

    return results


# ---------------------------------------------------------------------------
# F9.2 — Debrid cache probe (plan 33 Phase 3)
# ---------------------------------------------------------------------------

# Cache-probe timeout per the plan: short enough to keep the decision loop
# responsive, long enough that a slow but healthy debrid API doesn't
# spuriously return "unknown".
_CACHE_PROBE_TIMEOUT = 10

# mylist returns the FULL account (hundreds of torrents, each with its file
# list) in one response — a much bigger payload than a cache probe, so it
# gets a longer ceiling.  This is the library-enumeration path; it must not
# share the snappy cache-probe budget.
_MYLIST_TIMEOUT = 30

# Cap TorBox per-hash fan-out so a large Torrentio result set cannot
# produce a ``N * _CACHE_PROBE_TIMEOUT`` wall-clock stall holding the
# status-server worker thread.  Callers that want more coverage should
# pre-rank by quality/seeders and pass the top-K list.
_TORBOX_MAX_PROBES = 25

# Emit the "RD cache probe is a no-op" warning once per process so users
# with RD + QUALITY_COMPROMISE_ONLY_CACHED=true understand why compromise
# never fires.  A module-level flag avoids log-spam across many searches.
_rd_cache_warning_emitted = False
_ad_cache_warning_emitted = False


def check_debrid_cache(info_hashes, service=None, api_key=None):
    """Check debrid-cache availability for a batch of info hashes.

    Args:
        info_hashes: Iterable of 40-char hex hashes.  Invalid / non-string
            entries are dropped; duplicates are collapsed preserving
            first-seen order.
        service: Optional provider override (``'realdebrid'``,
            ``'alldebrid'``, or ``'torbox'``).  Defaults to the
            auto-detected service via ``_get_debrid_service()``.
        api_key: Optional API key override.  Defaults to the auto-detected
            key alongside the service.

    Returns:
        Mapping of hash -> True / False / None.  ``True`` = provider
        confirms the release is cached (instant debrid download).
        ``False`` = provider confirms uncached.  ``None`` = unknown
        (timeout, API failure, no service configured, or the provider
        does not expose a cache-query endpoint).  Per the plan's I4
        contract, callers under ``QUALITY_COMPROMISE_ONLY_CACHED`` treat
        ``None`` as "not cached" (safe) and under aggressive mode treat
        it as "assume cached".

    Provider notes:
      - **Real-Debrid** deprecated ``/torrents/instantAvailability`` in
        Nov 2024.  Stub returns ``{hash: None}``; no network call.
      - **AllDebrid** discontinued ``/v4/magnet/instant`` (404 ``DISCONTINUED``
        as of May 2026, verified against the live API + the public
        docs at https://docs.alldebrid.com).  No replacement endpoint
        exists — cache state is only knowable post-upload via the
        ``ready`` flag on ``/v4.1/magnet/upload``, which is a
        state-changing operation and therefore unsuitable for a
        read-only pre-add probe.  Stub returns ``{hash: None}``;
        no network call.
      - **TorBox** ``/api/torrents/checkcached`` is the only provider
        endpoint still exposing a working pre-add cache probe.

    URL redaction: every HTTP URL logged by this function is passed
    through ``_safe_log_url`` so API keys in query strings never leak
    into the Zurgarr logs.
    """
    hashes = []
    seen = set()
    for h in info_hashes or ():
        if not isinstance(h, str):
            continue
        h = h.strip().lower()
        if not _HASH_RE.match(h) or h in seen:
            continue
        seen.add(h)
        hashes.append(h)
    if not hashes:
        return {}

    if service is None:
        service, api_key = _get_debrid_service()
    if not service or not api_key:
        return {h: None for h in hashes}

    try:
        if service == 'realdebrid':
            return _check_cache_rd(hashes, api_key)
        if service == 'alldebrid':
            return _check_cache_ad(hashes, api_key)
        if service == 'torbox':
            return _check_cache_tb(hashes, api_key)
    except (urllib.error.URLError, urllib.error.HTTPError,
            json.JSONDecodeError, OSError, ValueError) as e:
        logger.warning(f"[search] Cache probe failed for {service}: {type(e).__name__}")
    return {h: None for h in hashes}


def _check_cache_rd(hashes, api_key):
    """Real-Debrid cache probe stub.

    ``/torrents/instantAvailability`` was deprecated by RD in Nov 2024
    and no replacement exists — there is no pre-add way to know if a
    hash is cached.  We return ``{hash: None}`` uniformly so the
    compromise engine treats RD responses as "unknown"; users who want
    aggressive escalation can set ``QUALITY_COMPROMISE_ONLY_CACHED=false``.
    A one-time ``warning`` surfaces so users with RD + only-cached
    mode understand why compromise never fires.
    """
    global _rd_cache_warning_emitted
    if not _rd_cache_warning_emitted:
        logger.warning(
            "[search] Real-Debrid cache probes are a no-op — RD deprecated "
            "instantAvailability in Nov 2024.  Cache-gated features "
            "(QUALITY_COMPROMISE_ONLY_CACHED, cached_first sort) will treat "
            "all RD releases as 'unknown' and refuse escalation; set "
            "QUALITY_COMPROMISE_ONLY_CACHED=false to opt into aggressive "
            "escalation without cache verification"
        )
        _rd_cache_warning_emitted = True
    return {h: None for h in hashes}


def _check_cache_ad(hashes, api_key):
    """AllDebrid cache probe stub.

    ``/v4/magnet/instant`` was discontinued by AD some time before
    May 2026 (verified live: every call to v4 and v4.1 returns
    ``{"status":"error","error":{"code":"DISCONTINUED",...}}``).  No
    replacement endpoint exists — AD's only remaining cache signal is
    the ``ready`` boolean on a successful ``/v4.1/magnet/upload``
    response, which is a state-changing operation and therefore
    unsuitable for a read-only pre-add probe.  We return
    ``{hash: None}`` uniformly without hitting the network and emit a
    single process-lifetime warning so users with AD +
    ``QUALITY_COMPROMISE_ONLY_CACHED=true`` understand why compromise
    never fires.
    """
    global _ad_cache_warning_emitted
    if not _ad_cache_warning_emitted:
        logger.warning(
            "[search] AllDebrid cache probes are a no-op — AD discontinued "
            "/v4/magnet/instant (no replacement endpoint).  Cache-gated "
            "features (QUALITY_COMPROMISE_ONLY_CACHED, cached_first sort) "
            "will treat all AD releases as 'unknown' and refuse escalation; "
            "set QUALITY_COMPROMISE_ONLY_CACHED=false to opt into aggressive "
            "escalation without cache verification"
        )
        _ad_cache_warning_emitted = True
    return {h: None for h in hashes}


def _check_cache_tb(hashes, api_key):
    """TorBox per-hash cache probe via ``/api/torrents/checkcached``.

    TB's endpoint is per-hash; the batch is capped at
    ``_TORBOX_MAX_PROBES`` so a large Torrentio result set cannot
    blow out the ``_CACHE_PROBE_TIMEOUT`` budget linearly (25 × 10 s
    = ~4 min worst case instead of unbounded).  Hashes beyond the cap
    stay as ``None`` (unknown) — the compromise engine already ranks
    candidates so the top few always get probed.
    """
    headers = {
        'Authorization': f'Bearer {api_key}',
        'User-Agent': 'zurgarr/1.0',
    }
    base_url = 'https://api.torbox.app/v1/api/torrents/checkcached'
    result = {h: None for h in hashes}
    for h in hashes[:_TORBOX_MAX_PROBES]:
        url = f'{base_url}?hash={h}'
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=_CACHE_PROBE_TIMEOUT) as resp:
                raw = resp.read(1 * 1024 * 1024)
                if not raw:
                    continue
                data = json.loads(raw.decode('utf-8'))
        except (urllib.error.URLError, urllib.error.HTTPError,
                json.JSONDecodeError, OSError, ValueError) as e:
            logger.warning(
                f"[search] TB cache probe {_safe_log_url(url)} "
                f"(hash={h[:8]}…): {type(e).__name__}"
            )
            continue
        # TorBox returns {"success": true, "data": {<hash>: {...}} } when
        # cached, and {"success": true, "data": {}} / [] when not.
        # An unexpected type (None, string, etc.) is "unknown" per I4 —
        # we must not conflate API error with a confirmed-uncached.
        if not data.get('success'):
            continue
        payload = data.get('data')
        if not isinstance(payload, dict):
            continue
        result[h] = h in payload
    return result


# ---------------------------------------------------------------------------
# Debrid account dedup — listing hashes already on the account
# ---------------------------------------------------------------------------

# Short TTL so repeated adds in the same UI session hit the API once, but
# the cache goes stale fast enough that an external deletion (via DMM, etc.)
# is picked up on the next attempt.
_EXISTING_HASHES_TTL = 30

# {service: (fetched_at, set_of_lowercase_hashes)}
_existing_hashes_cache = {}
_existing_hashes_lock = threading.Lock()

# In-flight ``(service, hash)`` tuples — a second concurrent add of the
# same hash has to see the first one in progress, otherwise two sibling
# "Add" clicks racing the dedup check can both pass the cached-set probe
# and both submit.  Kept under ``_existing_hashes_lock`` so the check and
# insert are one atomic step.
_inflight_adds = set()


def _existing_hashes(service, api_key, force_refresh=False):
    """Return the set of lowercase info-hashes currently on the debrid account.

    Returns ``None`` when the account cannot be queried (missing service/key,
    API error, unexpected response).  Callers distinguish ``None`` (unknown —
    do not claim "no duplicate") from ``set()`` (confirmed empty account).

    Cached for ``_EXISTING_HASHES_TTL`` seconds per service so a burst of
    "add" clicks issues one list call, not N.  ``remember_added_hash`` keeps
    the cache honest by injecting newly-added hashes without waiting for TTL
    expiry.
    """
    if not service or not api_key:
        return None

    now = time.time()
    with _existing_hashes_lock:
        cached = _existing_hashes_cache.get(service)
        if cached and not force_refresh and (now - cached[0]) < _EXISTING_HASHES_TTL:
            # Return a snapshot so a concurrent ``remember_added_hash`` on
            # the cached set can't mutate the view the caller is iterating.
            return set(cached[1])

    try:
        if service == 'realdebrid':
            hashes = _existing_hashes_rd(api_key)
        elif service == 'alldebrid':
            hashes = _existing_hashes_ad(api_key)
        elif service == 'torbox':
            hashes = _existing_hashes_tb(api_key)
        else:
            return None
    except (urllib.error.URLError, urllib.error.HTTPError,
            json.JSONDecodeError, OSError, ValueError,
            AttributeError, TypeError, KeyError) as e:
        # AttributeError / TypeError / KeyError cover the "API returned an
        # unexpected shape" case (e.g. a list where we expected a dict, or
        # a non-string hash field) without bubbling up into the hot path
        # of add_to_debrid / _process_file and crashing the request.
        logger.warning(f"[search] Dedup probe failed for {service}: {type(e).__name__}")
        return None

    if hashes is None:
        return None
    with _existing_hashes_lock:
        _existing_hashes_cache[service] = (now, hashes)
        snapshot = set(hashes)
    return snapshot


def remember_added_hash(service, info_hash):
    """Update the dedup cache after a successful add so back-to-back duplicates
    are caught without waiting for TTL refresh."""
    if not service or not info_hash:
        return
    h = info_hash.strip().lower()
    if not _HASH_RE.match(h):
        return
    with _existing_hashes_lock:
        cached = _existing_hashes_cache.get(service)
        if cached:
            cached[1].add(h)


def invalidate_existing_hashes_cache(service=None):
    """Drop the dedup cache for one service (or all if None)."""
    with _existing_hashes_lock:
        if service is None:
            _existing_hashes_cache.clear()
        else:
            _existing_hashes_cache.pop(service, None)


def _coerce_hash(value):
    """Return a valid lowercase 40-char hex hash or None.

    Defensive against API responses that return non-string hash fields
    (int, null, nested dict) — ``(x or '').strip().lower()`` would raise
    AttributeError on those, so we type-check first.
    """
    if not isinstance(value, str):
        return None
    h = value.strip().lower()
    return h if _HASH_RE.match(h) else None


# RD list cap.  At 2501 torrents the API truncates silently and the dedup
# gate starts missing older hashes.  We emit a one-time warning when the
# response fills the window so users with heavy accounts at least see
# "dedup may be incomplete" instead of a silent-degradation failure mode.
_RD_LIST_LIMIT = 2500
_rd_list_truncation_warned = False


def _existing_hashes_rd(api_key):
    """RD: GET /torrents?limit=2500 → list of {id, hash, status, ...}."""
    global _rd_list_truncation_warned
    headers = {'Authorization': f'Bearer {api_key}'}
    data = _urllib_get(
        f'https://api.real-debrid.com/rest/1.0/torrents?limit={_RD_LIST_LIMIT}',
        headers=headers, timeout=_CACHE_PROBE_TIMEOUT,
    )
    if not isinstance(data, list):
        return None
    out = set()
    for entry in data:
        if not isinstance(entry, dict):
            continue
        h = _coerce_hash(entry.get('hash'))
        if h:
            out.add(h)
    if len(data) >= _RD_LIST_LIMIT and not _rd_list_truncation_warned:
        logger.warning(
            f"[search] RD returned {len(data)} torrents (limit {_RD_LIST_LIMIT}) — "
            "dedup may miss older entries.  See SEARCH_DEDUP_ENABLED docs."
        )
        _rd_list_truncation_warned = True
    return out


def _existing_hashes_ad(api_key):
    """AD: GET /v4/magnet/status → {data: {magnets: [{hash, ...}]}}."""
    qs = urllib.parse.urlencode({'agent': 'zurgarr', 'apikey': api_key})
    data = _urllib_get(
        f'https://api.alldebrid.com/v4/magnet/status?{qs}',
        timeout=_CACHE_PROBE_TIMEOUT,
    )
    if not isinstance(data, dict) or data.get('status') != 'success':
        return None
    inner = data.get('data') if isinstance(data.get('data'), dict) else {}
    magnets = inner.get('magnets') if isinstance(inner.get('magnets'), list) else []
    out = set()
    for entry in magnets:
        if not isinstance(entry, dict):
            continue
        h = _coerce_hash(entry.get('hash'))
        if h:
            out.add(h)
    return out


def _existing_hashes_tb(api_key):
    """TB: GET /v1/api/torrents/mylist → {data: [{hash, ...}]}."""
    headers = {'Authorization': f'Bearer {api_key}'}
    data = _urllib_get(
        'https://api.torbox.app/v1/api/torrents/mylist',
        headers=headers, timeout=_CACHE_PROBE_TIMEOUT,
    )
    if not isinstance(data, dict) or not data.get('success'):
        return None
    payload = data.get('data') if isinstance(data.get('data'), list) else []
    out = set()
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        h = _coerce_hash(entry.get('hash'))
        if h:
            out.add(h)
    return out


def list_torbox_torrents(api_key, timeout=_MYLIST_TIMEOUT):
    """TB: GET /v1/api/torrents/mylist → full per-torrent listing.

    Returns the entire account in ONE call so the library scanner can
    enumerate TorBox content without walking the throttled FUSE mount
    (the source of the rclone 429 storms).  Each returned dict carries the
    release-folder ``name`` (matches the rclone mount folder) and a ``files``
    list whose ``name`` is the full relative path *including* that folder.

    Returns a ``list[dict]`` on success or ``None`` on any API failure, so
    the caller can distinguish "TB has zero torrents" (``[]``) from "couldn't
    reach TB" (``None``) and fall back to its last-good baseline.
    """
    headers = {'Authorization': f'Bearer {api_key}'}
    # bypass_cache=true: TorBox caches mylist server-side, so without this a
    # scan can promote a STALE page as the authoritative TB baseline and drop
    # since-added titles to "Wanted".  Mirrors debrid_client.list_torrents.
    data = _urllib_get(
        'https://api.torbox.app/v1/api/torrents/mylist?bypass_cache=true',
        headers=headers, timeout=timeout,
    )
    if not isinstance(data, dict) or not data.get('success'):
        return None
    payload = data.get('data') if isinstance(data.get('data'), list) else []
    out = []
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        name = entry.get('name')
        if not isinstance(name, str) or not name:
            continue
        files = []
        raw_files = entry.get('files')
        if isinstance(raw_files, list):
            for f in raw_files:
                if not isinstance(f, dict):
                    continue
                fname = f.get('name')
                if not isinstance(fname, str) or not fname:
                    continue
                size = f.get('size')
                if not isinstance(size, int) or size < 0:
                    size = 0
                short = f.get('short_name')
                files.append({
                    'name': fname,
                    'short_name': short if isinstance(short, str) else os.path.basename(fname),
                    'size': size,
                })
        out.append({
            'name': name,
            'hash': _coerce_hash(entry.get('hash')),
            'id': entry.get('id'),
            'files': files,
            'created_at': entry.get('created_at'),
        })
    return out


# ---------------------------------------------------------------------------
# F9.3 — Search + filter
# ---------------------------------------------------------------------------

def search_torrents(imdb_id, media_type='movie', season=None, episode=None,
                    annotate_cache=False, sort_mode='quality'):
    """Search Torrentio for torrents, sorted by quality then seeds.

    Args:
        imdb_id / media_type / season / episode: forwarded to
            ``search_torrentio`` (see that function for details).
        annotate_cache: When True, every result carries ``cached``
            (``True``/``False``/``None``) and ``cached_service`` fields
            populated by ``check_debrid_cache`` for the auto-detected
            provider.  Default False — the manual-search UI preserves
            its existing behaviour unless the caller opts in.
        sort_mode: ``'quality'`` (default) sorts by quality score then
            seeders.  ``'cached_first'`` sorts by
            (cached desc, quality desc, seeders desc) so a cached 1080p
            outranks an uncached 2160p — useful when the caller wants
            to grab something that will actually stream immediately.
            Implies ``annotate_cache=True``.

    Returns list of dicts sorted per the chosen ``sort_mode``.
    Blocklisted hashes are filtered out.

    Provider note: Real-Debrid's cache-query endpoint was deprecated in
    Nov 2024 and AllDebrid discontinued ``/v4/magnet/instant`` in May 2026,
    so RD and AD annotations are always ``None`` and ``'cached_first'`` sort
    degrades to quality order for those users.  Only TorBox
    (``/api/torrents/checkcached``) still returns meaningful True/False.
    """
    results = search_torrentio(imdb_id, media_type, season, episode)
    if not results:
        return []

    # Filter blocklisted hashes
    try:
        from utils.blocklist import is_blocked
        results = [r for r in results if not is_blocked(r['info_hash'])]
    except ImportError:
        pass

    # Cache annotation — requested explicitly, or implied by cached_first
    # sort.  A single batched probe per search keeps the UI snappy.  We
    # resolve the service once and pass it into ``check_debrid_cache``
    # so the annotation label (``cached_service``) is provably the same
    # as the service that produced the cache map — no double env-var
    # read, no chance of a mid-call key rotation causing divergence.
    want_cache = annotate_cache or sort_mode == 'cached_first'
    if want_cache:
        service, api_key = _get_debrid_service()
        cache_map = check_debrid_cache(
            [r['info_hash'] for r in results],
            service=service, api_key=api_key,
        )
        for r in results:
            r['cached'] = cache_map.get(r['info_hash'])
            r['cached_service'] = service

    if sort_mode == 'cached_first':
        # Normalise None to 0 so the sort can compare uniformly (Python
        # 3 refuses to order None against bool with <).  Unknown sorts
        # equal to uncached — we never promote an unverified release.
        results.sort(key=lambda r: (
            1 if r.get('cached') is True else 0,
            r['quality']['score'],
            r['seeds'],
        ), reverse=True)
    else:
        results.sort(key=lambda r: (
            r['quality']['score'],
            r['seeds'],
        ), reverse=True)

    return results


# ---------------------------------------------------------------------------
# F9.3 — Add to debrid
# ---------------------------------------------------------------------------

def add_to_debrid(info_hash, title='', media_title=None, episode=None,
                  *, service=None, api_key=None, cause=None, source='search'):
    """Add a torrent to a debrid provider via magnet.

    Args:
        info_hash: 40-char hex info hash
        title: Release title for logging/history
        media_title: Canonical library title (e.g. "The Wayfinders") so the
            event surfaces in that show/movie's detail-page Activity panel,
            which exact-matches on title or media_title.
        episode: Optional "SxxEyy" string for episode-scoped adds.
        service: Explicit debrid service ('realdebrid'/'alldebrid'/'torbox').
            When omitted, the RD-first auto-detected service is used. Callers
            that must target a specific provider (e.g. the Wanted→TorBox
            recovery pass) pass this to bypass the RD-first default.
        api_key: Explicit API key for ``service``. When omitted but ``service``
            is given, the key is resolved from env/secrets for that service.
        cause: Override the success-event ``meta['cause']`` slug. Defaults to
            ``'debrid_add_via_search'`` for the interactive-search path.
        source: History event ``source`` field (default ``'search'``).

    Returns:
        {'success': bool, 'torrent_id': str, 'service': str, 'error': str,
         'duplicate': bool}
    """
    if not info_hash or not _HASH_RE.match(info_hash):
        return {'success': False, 'torrent_id': '', 'service': '', 'error': 'Invalid info hash'}

    if service:
        if not api_key:
            api_key = _resolve_service_key(service)
    else:
        service, api_key = _get_debrid_service()
    if not service or not api_key:
        return {'success': False, 'torrent_id': '', 'service': service or '', 'error': 'No debrid service configured'}

    lowered = info_hash.lower()

    # Dedup — skip hashes already on the account.  Default ON because adding
    # the same magnet twice just creates a second torrent entry pointing at
    # the same hash and leaves the user to clean it up in DMM.
    if str(os.environ.get('SEARCH_DEDUP_ENABLED', 'true')).lower() == 'true':
        existing = _existing_hashes(service, api_key)
        if existing is not None and lowered in existing:
            logger.info(f"[search] Skipping add: {title or lowered[:16]} already in {service} account")
            return {'success': False, 'torrent_id': '', 'service': service,
                    'error': 'Already in debrid account', 'duplicate': True}

    # In-flight gate — another thread is mid-add for this same hash.  The
    # account-list probe above can miss this case because the sibling add
    # hasn't reached ``addMagnet`` yet (race between
    # ``_existing_hashes`` → ``add_to_debrid`` on both threads).
    inflight_key = (service, lowered)
    with _existing_hashes_lock:
        if inflight_key in _inflight_adds:
            return {'success': False, 'torrent_id': '', 'service': service,
                    'error': 'Add already in progress', 'duplicate': True}
        _inflight_adds.add(inflight_key)

    try:
        # Require-cached — opt-in gate that refuses uncached torrents before
        # they land in the account as 0%/0-seed entries.  RD's cache probe
        # is a no-op (deprecated Nov 2024) so on RD this effectively blocks
        # all adds; users who still want the gate on AD/TB and not RD
        # should leave it OFF.
        if str(os.environ.get('SEARCH_REQUIRE_CACHED', 'false')).lower() == 'true':
            cache_map = check_debrid_cache([lowered], service=service, api_key=api_key)
            cached = cache_map.get(lowered)
            if cached is not True:
                cache_label = 'uncached' if cached is False else 'cache status unknown'
                logger.info(f"[search] Skipping add: {title or lowered[:16]} {cache_label} on {service}")
                return {'success': False, 'torrent_id': '', 'service': service,
                        'error': f'Not cached on {service} ({cache_label})'}

        magnet = _hash_to_magnet(lowered)

        try:
            if service == 'realdebrid':
                result = _add_to_rd(magnet, api_key)
            elif service == 'alldebrid':
                result = _add_to_ad(magnet, api_key)
            elif service == 'torbox':
                result = _add_to_tb(magnet, api_key)
            else:
                result = {'success': False, 'torrent_id': '', 'error': f'Unknown service: {service}'}
        except Exception as e:
            logger.error(f"[search] Add to {service} failed: {type(e).__name__}")
            result = {'success': False, 'torrent_id': '', 'error': f'Service error: {type(e).__name__}'}

        result['service'] = service
        if result.get('success'):
            remember_added_hash(service, lowered)
    finally:
        with _existing_hashes_lock:
            _inflight_adds.discard(inflight_key)

    # Emit history event.  Scrub well-known credential patterns from the
    # provider error string before it lands in history.jsonl — debrid
    # clients have been seen to echo the request URL (with apikey
    # querystring) in failure messages, and history is a plain file that
    # is often shared verbatim when troubleshooting.
    import re as _re

    def _redact(s):
        if not s:
            return s
        s = _re.sub(r'(apikey|api_key|token|key|bearer)=[^&\s]+',
                    r'\1=***', str(s), flags=_re.IGNORECASE)
        s = _re.sub(r'(Authorization:\s*Bearer\s+)\S+', r'\1***', s,
                    flags=_re.IGNORECASE)
        return s

    try:
        from utils import history as _hist
        if result['success']:
            _hist.log_event(
                'debrid_add',
                title or info_hash[:16],
                episode=episode,
                detail=f'Added to {service} via {source}',
                source=source,
                media_title=media_title,
                meta={'cause': cause or 'debrid_add_via_search',
                      'info_hash': info_hash,
                      'service': service,
                      'torrent_id': result.get('torrent_id', '')},
            )
        else:
            err = _redact(result.get('error', ''))
            _hist.log_event(
                'debrid_add_failed',
                title or info_hash[:16],
                episode=episode,
                detail=f'Failed to add to {service}: {err}',
                source=source,
                media_title=media_title,
                meta={'cause': 'debrid_add_failed',
                      'info_hash': info_hash,
                      'service': service,
                      'error': err},
            )
    except Exception:
        pass

    # Emit notification
    try:
        from utils.notifications import notify
        _via = 'interactive search' if source == 'search' else source
        if result['success']:
            notify('debrid_add_success',
                   f'Added to {service}',
                   f'{title or info_hash[:16]} added via {_via}')
        else:
            notify('debrid_add_failed',
                   f'Failed to add to {service}',
                   f'{title or info_hash[:16]}: {result.get("error", "")}',
                   level='warning')
    except Exception:
        pass

    return result


def _add_to_rd(magnet, api_key):
    """Add magnet to Real-Debrid and select all files."""
    headers = {'Authorization': f'Bearer {api_key}'}

    # Step 1: Add magnet
    resp = _urllib_post(
        'https://api.real-debrid.com/rest/1.0/torrents/addMagnet',
        data={'magnet': magnet},
        headers=headers,
        timeout=_ADD_TIMEOUT,
    )
    if not resp:
        return {'success': False, 'torrent_id': '', 'error': 'Failed to add magnet to RD'}

    torrent_id = resp.get('id', '')
    if not torrent_id:
        return {'success': False, 'torrent_id': '', 'error': 'No torrent ID returned from RD'}

    # Step 2: Select all files
    sel = _urllib_post(
        f'https://api.real-debrid.com/rest/1.0/torrents/selectFiles/{torrent_id}',
        data={'files': 'all'},
        headers=headers,
        timeout=_ADD_TIMEOUT,
    )
    if sel is None:
        logger.warning(f"[search] RD selectFiles failed for torrent {torrent_id}")

    return {'success': True, 'torrent_id': str(torrent_id), 'error': ''}


def _add_to_ad(magnet, api_key):
    """Add magnet to AllDebrid."""
    qs = urllib.parse.urlencode({'agent': 'zurgarr', 'apikey': api_key})
    resp = _urllib_post(
        f'https://api.alldebrid.com/v4/magnet/upload?{qs}',
        data={'magnets[]': magnet},
        timeout=_ADD_TIMEOUT,
    )
    if not resp:
        return {'success': False, 'torrent_id': '', 'error': 'Failed to add magnet to AD'}

    status = resp.get('status', '')
    data = resp.get('data', {})
    magnets = data.get('magnets', [])
    if status == 'success' and magnets:
        mag_id = str(magnets[0].get('id', ''))
        return {'success': True, 'torrent_id': mag_id, 'error': ''}

    error = data.get('error', {}).get('message', 'Unknown error')
    return {'success': False, 'torrent_id': '', 'error': error}


def _add_to_tb(magnet, api_key):
    """Add magnet to TorBox."""
    headers = {'Authorization': f'Bearer {api_key}'}
    resp = _urllib_post(
        'https://api.torbox.app/v1/api/torrents/createtorrent',
        data={'magnet': magnet},
        headers=headers,
        timeout=_ADD_TIMEOUT,
    )
    if not resp:
        return {'success': False, 'torrent_id': '', 'error': 'Failed to add magnet to TorBox'}

    if resp.get('success'):
        torrent_id = str(resp.get('data', {}).get('torrent_id', ''))
        return {'success': True, 'torrent_id': torrent_id, 'error': ''}

    error = resp.get('detail', resp.get('error', 'Unknown error'))
    return {'success': False, 'torrent_id': '', 'error': error}
