"""Interactive debrid torrent search via Torrentio + provider cache checks.

Allows users to search for torrents from the library detail view, see
which results are instantly available (cached) on their debrid provider,
and one-click add them.  Uses urllib only (no requests dependency).
"""

import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request

from base import load_secret_or_env
from utils.logger import get_logger

logger = get_logger()

_SEARCH_TIMEOUT = 10
_CACHE_CHECK_TIMEOUT = 10
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


def _safe_log_url(url):
    """Strip query parameters from a URL for safe logging (no credentials)."""
    parsed = urllib.parse.urlparse(url)
    return urllib.parse.urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))


def _urllib_get(url, headers=None, timeout=_SEARCH_TIMEOUT):
    """GET request returning parsed JSON or None."""
    req = urllib.request.Request(url, headers=headers or {})
    req.add_header('User-Agent', 'pd_zurg/1.0')
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read(10 * 1024 * 1024).decode('utf-8'))
    except (urllib.error.URLError, urllib.error.HTTPError,
            json.JSONDecodeError, OSError, ValueError) as e:
        logger.warning(f"[search] GET {_safe_log_url(url)}: {type(e).__name__}")
        return None


def _urllib_post(url, data=None, json_body=None, headers=None, timeout=_ADD_TIMEOUT):
    """POST request returning parsed JSON or None."""
    hdrs = dict(headers or {})
    hdrs['User-Agent'] = 'pd_zurg/1.0'
    if json_body is not None:
        body = json.dumps(json_body).encode('utf-8')
        hdrs['Content-Type'] = 'application/json'
    elif data is not None:
        body = urllib.parse.urlencode(data).encode('utf-8')
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
# F9.2 — Debrid cache checks
# ---------------------------------------------------------------------------

def check_rd_cache(hashes, api_key):
    """Check Real-Debrid instant availability for a list of hashes.

    Returns set of lowercase hashes that are cached.
    """
    if not hashes or not api_key:
        return set()

    cached = set()
    # RD supports up to ~100 hashes per request via path
    batch_size = 100
    for i in range(0, len(hashes), batch_size):
        batch = hashes[i:i + batch_size]
        hash_path = '/'.join(h.lower() for h in batch)
        url = f'https://api.real-debrid.com/rest/1.0/torrents/instantAvailability/{hash_path}'
        headers = {'Authorization': f'Bearer {api_key}'}
        data = _urllib_get(url, headers=headers, timeout=_CACHE_CHECK_TIMEOUT)
        if not data or not isinstance(data, dict):
            continue
        # RD returns {hash: {rd: [{file_id: {filename, filesize}}]}} for cached
        for h, info in data.items():
            h_lower = h.lower()
            if isinstance(info, dict):
                rd_entries = info.get('rd', [])
                if rd_entries:
                    cached.add(h_lower)

    return cached


def check_ad_cache(hashes, api_key):
    """Check AllDebrid instant availability for a list of hashes.

    Returns set of lowercase hashes that are cached.
    """
    if not hashes or not api_key:
        return set()

    cached = set()
    # AD uses magnets[] query params
    batch_size = 100
    for i in range(0, len(hashes), batch_size):
        batch = hashes[i:i + batch_size]
        params = [('agent', 'pd_zurg'), ('apikey', api_key)]
        for h in batch:
            params.append(('magnets[]', f'{_MAGNET_PREFIX}{h.lower()}'))
        url = f'https://api.alldebrid.com/v4/magnet/instant?{urllib.parse.urlencode(params)}'
        data = _urllib_get(url, timeout=_CACHE_CHECK_TIMEOUT)
        if not data or not isinstance(data, dict):
            continue
        magnets_data = data.get('data', {}).get('magnets', [])
        if not isinstance(magnets_data, list):
            continue
        for idx, magnet_info in enumerate(magnets_data):
            if isinstance(magnet_info, dict) and magnet_info.get('instant'):
                if idx < len(batch):
                    cached.add(batch[idx].lower())

    return cached


def check_tb_cache(hashes, api_key):
    """Check TorBox instant availability for a list of hashes.

    Returns set of lowercase hashes that are cached.
    """
    if not hashes or not api_key:
        return set()

    cached = set()
    batch_size = 100
    for i in range(0, len(hashes), batch_size):
        batch = hashes[i:i + batch_size]
        hash_str = ','.join(h.lower() for h in batch)
        url = f'https://api.torbox.app/v1/api/torrents/checkcached?hash={urllib.parse.quote(hash_str)}&list_files=false'
        headers = {'Authorization': f'Bearer {api_key}'}
        data = _urllib_get(url, headers=headers, timeout=_CACHE_CHECK_TIMEOUT)
        if not data or not isinstance(data, dict):
            continue
        cache_data = data.get('data', {})
        if isinstance(cache_data, dict):
            for h_lower, info in cache_data.items():
                if info:
                    cached.add(h_lower.lower())

    return cached


def check_cache(hashes):
    """Check debrid cache for a list of info hashes using the configured provider.

    Returns set of lowercase hashes that are cached.
    """
    if not hashes:
        return set()

    service, api_key = _get_debrid_service()
    if not service:
        return set()

    checkers = {
        'realdebrid': check_rd_cache,
        'alldebrid': check_ad_cache,
        'torbox': check_tb_cache,
    }
    checker = checkers.get(service)
    if not checker:
        return set()

    try:
        return checker(hashes, api_key)
    except Exception as e:
        logger.error(f"[search] Cache check failed for {service}: {type(e).__name__}: {e}")
        return set()


# ---------------------------------------------------------------------------
# F9.3 — Combined search + cache check
# ---------------------------------------------------------------------------

def search_with_cache(imdb_id, media_type='movie', season=None, episode=None):
    """Search Torrentio and enrich results with debrid cache status.

    Returns list of dicts sorted by: cached first, then quality (desc),
    then seeds (desc). Blocklisted hashes are filtered out.
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

    # Check cache status in one batch
    all_hashes = [r['info_hash'] for r in results]
    cached_hashes = check_cache(all_hashes)

    for r in results:
        r['cached'] = r['info_hash'] in cached_hashes

    # Sort: cached first, then quality score desc, then seeds desc
    results.sort(key=lambda r: (
        r['cached'],
        r['quality']['score'],
        r['seeds'],
    ), reverse=True)

    return results


# ---------------------------------------------------------------------------
# F9.4 — Add to debrid
# ---------------------------------------------------------------------------

def add_to_debrid(info_hash, title=''):
    """Add a torrent to the configured debrid provider via magnet.

    Args:
        info_hash: 40-char hex info hash
        title: Release title for logging/history

    Returns:
        {'success': bool, 'torrent_id': str, 'service': str, 'error': str}
    """
    if not info_hash or not _HASH_RE.match(info_hash):
        return {'success': False, 'torrent_id': '', 'service': '', 'error': 'Invalid info hash'}

    service, api_key = _get_debrid_service()
    if not service:
        return {'success': False, 'torrent_id': '', 'service': '', 'error': 'No debrid service configured'}

    magnet = _hash_to_magnet(info_hash.lower())

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

    # Emit history event
    try:
        from utils import history as _hist
        if result['success']:
            _hist.log_event(
                'debrid_add',
                title or info_hash[:16],
                detail=f'Added to {service} via search',
                source='search',
                meta={'info_hash': info_hash, 'service': service,
                      'torrent_id': result.get('torrent_id', '')},
            )
        else:
            _hist.log_event(
                'debrid_add_failed',
                title or info_hash[:16],
                detail=f'Failed to add to {service}: {result.get("error", "")}',
                source='search',
                meta={'info_hash': info_hash, 'service': service},
            )
    except Exception:
        pass

    # Emit notification
    try:
        from utils.notifications import notify
        if result['success']:
            notify('debrid_add_success',
                   f'Added to {service}',
                   f'{title or info_hash[:16]} added via interactive search')
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
    qs = urllib.parse.urlencode({'agent': 'pd_zurg', 'apikey': api_key})
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
