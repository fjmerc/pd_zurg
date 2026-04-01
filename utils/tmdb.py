"""TMDB API client with on-disk caching for library metadata enrichment.

Provides show/movie search and metadata (episode titles, air dates, posters,
overviews) from The Movie Database.  Feature is opt-in: requires TMDB_API_KEY
env var.  All public functions return None when the key is absent or on error.

Cache lives at /config/tmdb_cache.json with a 7-day TTL per entry.
"""

import json
import os
import threading
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from utils.file_utils import atomic_write
from utils.logger import get_logger

logger = get_logger()

_TMDB_BASE = 'https://api.themoviedb.org/3'
_IMAGE_BASE = 'https://image.tmdb.org/t/p/'
_CACHE_PATH = '/config/tmdb_cache.json'
_CACHE_TTL = 7 * 86400  # 7 days in seconds
_TIMEOUT = 8
_cache_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Low-level API
# ---------------------------------------------------------------------------

def _get_api_key():
    return os.environ.get('TMDB_API_KEY', '').strip()


def _api_get(path, params=None):
    """Make a GET request to the TMDB API. Returns parsed JSON or None."""
    api_key = _get_api_key()
    if not api_key:
        return None

    if params is None:
        params = {}
    params['api_key'] = api_key

    url = _TMDB_BASE + path + '?' + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={'User-Agent': 'pd_zurg/1.0'})

    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            return json.loads(resp.read(10 * 1024 * 1024).decode('utf-8'))
    except (urllib.error.URLError, urllib.error.HTTPError,
            json.JSONDecodeError, OSError, ValueError) as e:
        logger.warning(f"[tmdb] API error for {path}: {type(e).__name__}")
        return None


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

def _pick_best_result(results, year, date_key):
    """Pick the result whose year best matches *year*.

    TMDB's year parameter is a soft hint — popular older titles can outrank
    the correct year match.  When the top result's year is far from the
    requested year (>2 years), scan the first few results for a year match.
    If the top result is already close (within ±2 years), trust TMDB's
    relevance ranking — the folder year may just be a season air year or
    minor date discrepancy.
    """
    if year is None:
        return results[0]

    year_int = int(year)
    first_date = (results[0].get(date_key, '') or '')[:4]
    if first_date:
        try:
            if abs(int(first_date) - year_int) <= 2:
                return results[0]
        except ValueError:
            pass

    # Exact match only — ±2 tolerance already consumed above for the top result
    year_str = str(year_int)
    for r in results[:5]:
        rd = (r.get(date_key, '') or '')[:4]
        if rd == year_str:
            return r
    return results[0]


def search_show(title, year=None, fallback_no_year=False):
    """Search TMDB for a TV show. Returns first result dict or None.

    When *fallback_no_year* is True and a year-filtered search returns no
    results, retries without the year.  Useful for poster/cache enrichment
    where torrent folder names often carry a season air year instead of the
    show's premiere year.  Callers that need precise disambiguation (e.g.
    Sonarr/Radarr series matching) should leave this False.
    """
    params = {'query': title}
    if year is not None:
        params['first_air_date_year'] = year
    data = _api_get('/search/tv', params)
    effective_year = year
    if data and not data.get('results') and year is not None and fallback_no_year:
        # Year filter too strict — retry without it; don't apply year
        # preference on retry results since the year is proven unreliable
        data = _api_get('/search/tv', {'query': title})
        effective_year = None
    if not data or not data.get('results'):
        return None
    r = _pick_best_result(data['results'], effective_year, 'first_air_date')
    return {
        'tmdb_id': r['id'],
        'title': r.get('name', ''),
        'overview': r.get('overview', ''),
        'poster_path': r.get('poster_path') or '',
        'first_air_date': r.get('first_air_date', ''),
    }


def search_movie(title, year=None, fallback_no_year=False):
    """Search TMDB for a movie. Returns first result dict or None.

    When *fallback_no_year* is True and a year-filtered search returns no
    results, retries without the year.  Callers that need precise
    disambiguation should leave this False.
    """
    params = {'query': title}
    if year is not None:
        params['year'] = year
    data = _api_get('/search/movie', params)
    effective_year = year
    if data and not data.get('results') and year is not None and fallback_no_year:
        # Year filter too strict — retry without it
        data = _api_get('/search/movie', {'query': title})
        effective_year = None
    if not data or not data.get('results'):
        return None
    r = _pick_best_result(data['results'], effective_year, 'release_date')
    return {
        'tmdb_id': r['id'],
        'title': r.get('title', ''),
        'overview': r.get('overview', ''),
        'poster_path': r.get('poster_path') or '',
        'release_date': r.get('release_date', ''),
    }


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

def get_show_metadata(tmdb_id):
    """Fetch full show details including all season/episode data.
    Skips Season 0 (specials).
    """
    show = _api_get(f'/tv/{tmdb_id}', params={'append_to_response': 'external_ids'})
    if not show:
        return None

    # IMDb ID from external_ids (appended to response)
    ext = show.get('external_ids', {})
    imdb_id = ext.get('imdb_id') or ''

    result = {
        'tmdb_id': tmdb_id,
        'imdb_id': imdb_id,
        'title': show.get('name', ''),
        'overview': show.get('overview', ''),
        'poster_path': show.get('poster_path') or '',
        'status': show.get('status', ''),
        'seasons': [],
    }

    for s in show.get('seasons', []):
        snum = s.get('season_number', 0)
        if snum == 0:
            continue

        season_data = _api_get(f'/tv/{tmdb_id}/season/{snum}')
        if not season_data:
            continue

        episodes = []
        for ep in season_data.get('episodes', []):
            episodes.append({
                'number': ep.get('episode_number', 0),
                'title': ep.get('name', ''),
                'air_date': ep.get('air_date') or '',
            })

        result['seasons'].append({
            'number': snum,
            'total_episodes': len(episodes),
            'episodes': episodes,
        })

    return result


def get_movie_metadata(tmdb_id):
    """Fetch movie details from TMDB."""
    data = _api_get(f'/movie/{tmdb_id}')
    if not data:
        return None
    return {
        'tmdb_id': tmdb_id,
        'imdb_id': data.get('imdb_id') or '',
        'title': data.get('title', ''),
        'overview': data.get('overview', ''),
        'poster_path': data.get('poster_path') or '',
        'runtime': data.get('runtime') or 0,
        'release_date': data.get('release_date', ''),
    }


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

def _load_cache():
    try:
        with open(_CACHE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def _save_cache(cache):
    os.makedirs(os.path.dirname(_CACHE_PATH), exist_ok=True)
    with atomic_write(_CACHE_PATH) as f:
        json.dump(cache, f, indent=2)


def _is_fresh(entry):
    cached_at = entry.get('cached_at', '')
    if not cached_at:
        return False
    try:
        ts = datetime.fromisoformat(cached_at)
        age = (datetime.now(timezone.utc) - ts).total_seconds()
        return age < _CACHE_TTL
    except (ValueError, TypeError):
        return False


# ---------------------------------------------------------------------------
# Public API (cache-aware)
# ---------------------------------------------------------------------------

def _poster_url(path, size='w300'):
    if not path:
        return ''
    return _IMAGE_BASE + size + path


def get_show_info(title, year=None):
    """Get show metadata with caching. Returns dict or None."""
    if not _get_api_key():
        return None

    from utils.library import _normalize_title
    key = _normalize_title(title)

    with _cache_lock:
        cache = _load_cache()
        entry = cache.get('shows', {}).get(key)
        if entry and _is_fresh(entry):
            return _format_show(entry)

    # Cache miss — fetch from TMDB (fallback_no_year=True because folder
    # years are unreliable and this path is only for poster/metadata caching)
    search = search_show(title, year, fallback_no_year=True)
    if not search:
        return None

    metadata = get_show_metadata(search['tmdb_id'])
    if not metadata:
        return None

    metadata['cached_at'] = datetime.now(timezone.utc).isoformat(timespec='seconds')

    with _cache_lock:
        cache = _load_cache()
        cache.setdefault('shows', {})[key] = metadata
        _save_cache(cache)

    return _format_show(metadata)


def get_movie_info(title, year=None):
    """Get movie metadata with caching. Returns dict or None."""
    if not _get_api_key():
        return None

    from utils.library import _normalize_title
    key = _normalize_title(title)

    with _cache_lock:
        cache = _load_cache()
        entry = cache.get('movies', {}).get(key)
        if entry and _is_fresh(entry):
            return _format_movie(entry)

    search = search_movie(title, year, fallback_no_year=True)
    if not search:
        return None

    metadata = get_movie_metadata(search['tmdb_id'])
    if not metadata:
        return None

    metadata['cached_at'] = datetime.now(timezone.utc).isoformat(timespec='seconds')

    with _cache_lock:
        cache = _load_cache()
        cache.setdefault('movies', {})[key] = metadata
        _save_cache(cache)

    return _format_movie(metadata)


def _format_show(entry):
    return {
        'tmdb_id': entry.get('tmdb_id'),
        'imdb_id': entry.get('imdb_id') or '',
        'title': entry.get('title', ''),
        'overview': entry.get('overview', ''),
        'poster_url': _poster_url(entry.get('poster_path', '')),
        'status': entry.get('status', ''),
        'seasons': entry.get('seasons', []),
    }


def _format_movie(entry):
    return {
        'tmdb_id': entry.get('tmdb_id'),
        'imdb_id': entry.get('imdb_id') or '',
        'title': entry.get('title', ''),
        'overview': entry.get('overview', ''),
        'poster_url': _poster_url(entry.get('poster_path', '')),
        'runtime': entry.get('runtime', 0),
        'release_date': entry.get('release_date', ''),
    }


# ---------------------------------------------------------------------------
# Bulk cache lookup (no API calls)
# ---------------------------------------------------------------------------

def get_cached_posters(items):
    """Return cached poster/status data for a list of library items.

    Performs a single cache file read — no TMDB API calls.  Items without
    a fresh cache entry are silently omitted from the result.

    Args:
        items: list of dicts with 'title', 'year', 'type' ('show'|'movie')

    Returns:
        {normalized_title: {poster_url, tmdb_status, total_episodes}} for shows
        {normalized_title: {poster_url, tmdb_status, runtime}} for movies
    """
    if not _get_api_key():
        return {}

    from utils.library import _normalize_title

    with _cache_lock:
        cache = _load_cache()

    shows_cache = cache.get('shows', {})
    movies_cache = cache.get('movies', {})
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    result = {}

    for item in items:
        key = _normalize_title(item.get('title', ''))
        if not key:
            continue

        item_type = item.get('type', '')
        if item_type == 'show':
            entry = shows_cache.get(key)
            if entry and _is_fresh(entry):
                # Only count aired episodes (air_date non-empty and <= today)
                # to match Sonarr behavior — unaired episodes are not "missing"
                aired_eps = 0
                for s in entry.get('seasons', []):
                    for ep in s.get('episodes', []):
                        ad = ep.get('air_date', '')
                        if ad and ad <= today:
                            aired_eps += 1
                result[key] = {
                    'poster_url': _poster_url(entry.get('poster_path', '')),
                    'tmdb_status': entry.get('status', ''),
                    'total_episodes': aired_eps,
                    'imdb_id': entry.get('imdb_id') or '',
                }
        elif item_type == 'movie':
            entry = movies_cache.get(key)
            if entry and _is_fresh(entry):
                # Movies don't have a status field in the cache; use
                # release_date presence to infer "Released" vs empty.
                rd = entry.get('release_date', '')
                status = 'Released' if rd else ''
                result[key] = {
                    'poster_url': _poster_url(entry.get('poster_path', '')),
                    'tmdb_status': status,
                    'runtime': entry.get('runtime', 0),
                    'imdb_id': entry.get('imdb_id') or '',
                }

    return result


def get_cached_tmdb_ids():
    """Return cached TMDB IDs grouped by section (no API calls).

    Used by the library scanner to build alias maps so differently-named
    items that share a TMDB ID can be merged.

    Returns: {'shows': {norm_title: tmdb_id, ...}, 'movies': {norm_title: tmdb_id, ...}}
    """
    with _cache_lock:
        cache = _load_cache()

    result = {}
    for section in ('shows', 'movies'):
        entries = {}
        for norm_title, entry in cache.get(section, {}).items():
            if _is_fresh(entry):
                tmdb_id = entry.get('tmdb_id')
                if tmdb_id:
                    entries[norm_title] = tmdb_id
        result[section] = entries
    return result


# ---------------------------------------------------------------------------
# Background cache population
# ---------------------------------------------------------------------------

_populate_lock = threading.Lock()
_populate_running = False


def background_populate_cache(items):
    """Fetch TMDB metadata for uncached items in a background thread.

    Rate-limited to ~3 requests/second.  Skips items that get cached by
    other code paths while the background thread is running.  Only one
    population thread runs at a time.

    Args:
        items: list of dicts with 'title', 'year', 'type' ('show'|'movie')
    """
    global _populate_running

    if not _get_api_key() or not items:
        return

    with _populate_lock:
        if _populate_running:
            return
        _populate_running = True

    import time  # noqa: E402 — deferred to avoid unused import when function is a no-op

    def _run():
        global _populate_running
        try:
            from utils.library import _normalize_title
            cached = 0
            logger.info(f"[tmdb] Background cache: fetching metadata for {len(items)} items")
            for item in items:
                title = item.get('title', '')
                year = item.get('year')
                item_type = item.get('type', '')
                if not title:
                    continue

                # Skip if already cached by another path
                key = _normalize_title(title)
                with _cache_lock:
                    c = _load_cache()
                    section = 'shows' if item_type == 'show' else 'movies'
                    entry = c.get(section, {}).get(key)
                    if entry and _is_fresh(entry):
                        continue

                if item_type == 'show':
                    result = get_show_info(title, year)
                else:
                    result = get_movie_info(title, year)

                if result:
                    cached += 1

                time.sleep(0.3)

            logger.info(f"[tmdb] Background cache: done ({cached}/{len(items)} cached)")
        except Exception as e:
            logger.warning(f"[tmdb] Background cache error: {e}")
        finally:
            with _populate_lock:
                _populate_running = False

    try:
        t = threading.Thread(target=_run, daemon=True)
        t.start()
    except Exception:
        with _populate_lock:
            _populate_running = False
