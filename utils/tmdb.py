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
            return json.loads(resp.read().decode('utf-8'))
    except (urllib.error.URLError, urllib.error.HTTPError,
            json.JSONDecodeError, OSError, ValueError) as e:
        logger.warning(f"[tmdb] API error for {path}: {e}")
        return None


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

def search_show(title, year=None):
    """Search TMDB for a TV show. Returns first result dict or None."""
    params = {'query': title}
    if year:
        params['first_air_date_year'] = year
    data = _api_get('/search/tv', params)
    if not data or not data.get('results'):
        return None
    r = data['results'][0]
    return {
        'tmdb_id': r['id'],
        'title': r.get('name', ''),
        'overview': r.get('overview', ''),
        'poster_path': r.get('poster_path') or '',
        'first_air_date': r.get('first_air_date', ''),
    }


def search_movie(title, year=None):
    """Search TMDB for a movie. Returns first result dict or None."""
    params = {'query': title}
    if year:
        params['year'] = year
    data = _api_get('/search/movie', params)
    if not data or not data.get('results'):
        return None
    r = data['results'][0]
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
    show = _api_get(f'/tv/{tmdb_id}')
    if not show:
        return None

    result = {
        'tmdb_id': tmdb_id,
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

    # Cache miss — fetch from TMDB
    search = search_show(title, year)
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

    search = search_movie(title, year)
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
        'title': entry.get('title', ''),
        'overview': entry.get('overview', ''),
        'poster_url': _poster_url(entry.get('poster_path', '')),
        'status': entry.get('status', ''),
        'seasons': entry.get('seasons', []),
    }


def _format_movie(entry):
    return {
        'tmdb_id': entry.get('tmdb_id'),
        'title': entry.get('title', ''),
        'overview': entry.get('overview', ''),
        'poster_url': _poster_url(entry.get('poster_path', '')),
        'runtime': entry.get('runtime', 0),
        'release_date': entry.get('release_date', ''),
    }
