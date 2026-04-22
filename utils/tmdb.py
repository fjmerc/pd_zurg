"""TMDB API client with on-disk caching for library metadata enrichment.

Provides show/movie search and metadata (episode titles, air dates, posters,
overviews) from The Movie Database.  Feature is opt-in: requires TMDB_API_KEY
env var.  All public functions return None when the key is absent or on error.

Cache lives at /config/tmdb_cache.json with a 7-day TTL per entry.
"""

import json
import os
import re
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

# Country used for MPAA/content-rating lookup. US by default; override with
# TMDB_RATING_COUNTRY env var for regions with different certification systems.
_RATING_COUNTRY = (os.environ.get('TMDB_RATING_COUNTRY') or 'US').strip().upper() or 'US'

# Cap cast list to prevent cache bloat. Only name/character/profile_path/order
# are persisted — no biographies.
_CAST_LIMIT = 15


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
    req = urllib.request.Request(url, headers={'User-Agent': 'zurgarr/1.0'})

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

def _genre_names(genres):
    """Extract genre name strings from a TMDB genres[] list."""
    if not isinstance(genres, list):
        return []
    names = []
    for g in genres:
        name = (g or {}).get('name')
        if name:
            names.append(name)
    return names


def _pick_us_certification(release_dates):
    """Return the MPAA certification for the configured country, or ''.

    Iterates TMDB's movie `release_dates.results` looking for the configured
    country entry and returns the first non-empty `certification` string from
    its `release_dates` list.
    """
    results = (release_dates or {}).get('results') or []
    for row in results:
        if (row or {}).get('iso_3166_1') != _RATING_COUNTRY:
            continue
        for rd in row.get('release_dates') or []:
            cert = (rd or {}).get('certification')
            if cert:
                return cert
    return ''


def _pick_us_content_rating(content_ratings):
    """Return the TV content rating for the configured country, or ''."""
    results = (content_ratings or {}).get('results') or []
    for row in results:
        if (row or {}).get('iso_3166_1') != _RATING_COUNTRY:
            continue
        rating = (row or {}).get('rating')
        if rating:
            return rating
    return ''


def _top_cast(credits, limit=_CAST_LIMIT):
    """Return the top N cast entries, sorted by TMDB order.

    Each entry has `name`, `character`, `profile_path`, `order`. Missing
    profile images are kept as empty strings (the frontend falls back to a
    placeholder circle).
    """
    cast_list = (credits or {}).get('cast') or []
    cleaned = []
    for c in cast_list:
        if not isinstance(c, dict):
            continue
        name = c.get('name') or ''
        if not name:
            continue
        cleaned.append({
            'name': name,
            'character': c.get('character') or '',
            'profile_path': c.get('profile_path') or '',
            'order': c.get('order', 9999),
        })
    cleaned.sort(key=lambda e: e.get('order', 9999))
    return cleaned[:limit]


def _directors_from_credits(credits):
    """Return director names from a movie credits.crew[] list."""
    crew = (credits or {}).get('crew') or []
    names = []
    seen = set()
    for c in crew:
        if not isinstance(c, dict):
            continue
        if c.get('job') != 'Director':
            continue
        name = c.get('name') or ''
        if name and name not in seen:
            seen.add(name)
            names.append(name)
    return names


def _creator_names(created_by):
    """Return creator names from a TV show's `created_by[]` list."""
    names = []
    seen = set()
    for c in created_by or []:
        if not isinstance(c, dict):
            continue
        name = c.get('name') or ''
        if name and name not in seen:
            seen.add(name)
            names.append(name)
    return names


def _avg_runtime(episode_run_time):
    """Return average episode runtime as an int, or 0."""
    vals = [int(v) for v in (episode_run_time or []) if isinstance(v, (int, float))]
    if not vals:
        return 0
    return int(round(sum(vals) / len(vals)))


def _cast_with_urls(cast_entries):
    """Expand stored cast entries with absolute profile URLs for API output."""
    out = []
    for c in cast_entries or []:
        out.append({
            'name': c.get('name', ''),
            'character': c.get('character', ''),
            'profile_url': _poster_url(c.get('profile_path', ''), size='w185'),
            'order': c.get('order', 9999),
        })
    return out


def get_show_metadata(tmdb_id):
    """Fetch full show details including all season/episode data.
    Skips Season 0 (specials).
    """
    show = _api_get(
        f'/tv/{tmdb_id}',
        params={'append_to_response': 'external_ids,credits,content_ratings'},
    )
    if not show:
        return None

    # IMDb ID from external_ids (appended to response)
    ext = show.get('external_ids', {})
    imdb_id = ext.get('imdb_id') or ''

    credits = show.get('credits') or {}

    result = {
        'tmdb_id': tmdb_id,
        'imdb_id': imdb_id,
        'title': show.get('name', ''),
        'overview': show.get('overview', ''),
        'poster_path': show.get('poster_path') or '',
        'status': show.get('status', ''),
        'genres': _genre_names(show.get('genres')),
        'vote_average': show.get('vote_average') or 0,
        'content_rating': _pick_us_content_rating(show.get('content_ratings')),
        'episode_run_time': _avg_runtime(show.get('episode_run_time')),
        'creators': _creator_names(show.get('created_by')),
        'cast': _top_cast(credits),
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
    data = _api_get(
        f'/movie/{tmdb_id}',
        params={'append_to_response': 'credits,release_dates'},
    )
    if not data:
        return None
    credits = data.get('credits') or {}
    return {
        'tmdb_id': tmdb_id,
        'imdb_id': data.get('imdb_id') or '',
        'title': data.get('title', ''),
        'overview': data.get('overview', ''),
        'poster_path': data.get('poster_path') or '',
        'runtime': data.get('runtime') or 0,
        'release_date': data.get('release_date', ''),
        'genres': _genre_names(data.get('genres')),
        'vote_average': data.get('vote_average') or 0,
        'certification': _pick_us_certification(data.get('release_dates')),
        'directors': _directors_from_credits(credits),
        'cast': _top_cast(credits),
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
    """Return True when the entry is within its TTL.

    Does NOT check schema — bulk lookups (get_cached_posters,
    find_show_by_season, get_cached_tmdb_ids, etc.) only need the
    seasons/tmdb_id/poster fields, which are present on pre-v2 entries too.
    Callers that care about the v2 Plex-detail fields (genres, cast, etc.)
    combine this with `_has_current_schema()` to trigger a refetch.
    """
    cached_at = entry.get('cached_at', '')
    if not cached_at:
        return False
    try:
        ts = datetime.fromisoformat(cached_at)
        age = (datetime.now(timezone.utc) - ts).total_seconds()
        return age < _CACHE_TTL
    except (ValueError, TypeError):
        return False


def _has_current_schema(entry):
    """Return True when entry contains the v2 Plex-detail fields.

    Uses `cast` as the sentinel key. Pre-v2 entries lack it, so
    `get_show_info`/`get_movie_info` refetch them to backfill the new
    metadata while bulk lookups continue to use the still-valid legacy
    data until the refetch lands.
    """
    return 'cast' in entry


# ---------------------------------------------------------------------------
# Public API (cache-aware)
# ---------------------------------------------------------------------------

def _poster_url(path, size='w300'):
    """Build a TMDB image URL. Rejects paths with characters that could
    break out of a CSS `url('...')` context when the result is embedded in
    an HTML style attribute (defense-in-depth; real TMDB paths never
    contain these).
    """
    if not path:
        return ''
    if any(c in path for c in ("'", '"', '\\', '\n', '\r', ' ', '(', ')')):
        return ''
    return _IMAGE_BASE + size + path


def _cache_key(norm, year=None):
    """Build a year-qualified cache key to disambiguate same-title entries.

    Returns ``"title (year)"`` when a valid 4-digit year is available,
    otherwise just ``"title"``.  The input *norm* should already be
    lowercase with any trailing parenthesized year stripped.
    """
    if year and re.fullmatch(r'\d{4}', str(year)):
        return f"{norm} ({year})"
    return norm


def _cache_lookup(section, norm, year=None):
    """Look up a cache entry, trying year-qualified key first then yearless."""
    if year is not None:
        qualified = _cache_key(norm, year)
        if qualified != norm:
            entry = section.get(qualified)
            if entry:
                return entry
    return section.get(norm)


def remove_cached_entry(normalized_title, media_type, year=None):
    """Remove a TMDB cache entry for a deleted title.

    Tries both year-qualified and plain keys. Returns True if anything
    was removed.  Thread-safe (acquires _cache_lock).
    """
    with _cache_lock:
        cache = _load_cache()
        section_key = 'shows' if media_type == 'show' else 'movies'
        section = cache.get(section_key, {})
        removed = False
        keys_to_try = [normalized_title]
        if year is not None:
            qualified = _cache_key(normalized_title, year)
            if qualified != normalized_title:
                keys_to_try.insert(0, qualified)
        for key in keys_to_try:
            if key in section:
                del section[key]
                removed = True
        if removed:
            cache[section_key] = section
            _save_cache(cache)
        return removed


def get_show_info(title, year=None):
    """Get show metadata with caching. Returns dict or None."""
    if not _get_api_key():
        return None

    from utils.library import _normalize_title
    norm = _normalize_title(title)
    store_key = _cache_key(norm, year)

    with _cache_lock:
        cache = _load_cache()
        entry = _cache_lookup(cache.get('shows', {}), norm, year)
        if entry and _is_fresh(entry) and _has_current_schema(entry):
            return _format_show(entry)

    # Cache miss or schema migration — fetch from TMDB (fallback_no_year=True
    # because folder years are unreliable and this path is only for
    # poster/metadata caching)
    search = search_show(title, year, fallback_no_year=True)
    if not search:
        return None

    metadata = get_show_metadata(search['tmdb_id'])
    if not metadata:
        return None

    metadata['cached_at'] = datetime.now(timezone.utc).isoformat(timespec='seconds')

    with _cache_lock:
        cache = _load_cache()
        cache.setdefault('shows', {})[store_key] = metadata
        _save_cache(cache)

    return _format_show(metadata)


def get_movie_info(title, year=None):
    """Get movie metadata with caching. Returns dict or None."""
    if not _get_api_key():
        return None

    from utils.library import _normalize_title
    norm = _normalize_title(title)
    store_key = _cache_key(norm, year)

    with _cache_lock:
        cache = _load_cache()
        entry = _cache_lookup(cache.get('movies', {}), norm, year)
        if entry and _is_fresh(entry) and _has_current_schema(entry):
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
        cache.setdefault('movies', {})[store_key] = metadata
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
        'genres': entry.get('genres', []) or [],
        'vote_average': entry.get('vote_average', 0) or 0,
        'content_rating': entry.get('content_rating', '') or '',
        'episode_run_time': entry.get('episode_run_time', 0) or 0,
        'creators': entry.get('creators', []) or [],
        'cast': _cast_with_urls(entry.get('cast', [])),
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
        'genres': entry.get('genres', []) or [],
        'vote_average': entry.get('vote_average', 0) or 0,
        'certification': entry.get('certification', '') or '',
        'directors': entry.get('directors', []) or [],
        'cast': _cast_with_urls(entry.get('cast', [])),
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
        year = item.get('year')
        # Use year-qualified result key so same-title-different-year items
        # don't overwrite each other (e.g. "The Bridge" 2011 vs 2013)
        result_key = _cache_key(key, year)

        item_type = item.get('type', '')
        if item_type == 'show':
            entry = _cache_lookup(shows_cache, key, year)
            if entry and _is_fresh(entry):
                # Only count aired episodes (air_date non-empty and <= today)
                # to match Sonarr behavior — unaired episodes are not "missing"
                aired_eps = 0
                for s in entry.get('seasons', []):
                    for ep in s.get('episodes', []):
                        ad = ep.get('air_date', '')
                        if ad and ad <= today:
                            aired_eps += 1
                season_nums = [s['number'] for s in entry.get('seasons', [])]
                info = {
                    'poster_url': _poster_url(entry.get('poster_path', '')),
                    'tmdb_status': entry.get('status', ''),
                    'total_episodes': aired_eps,
                    'imdb_id': entry.get('imdb_id') or '',
                    'max_cached_season': max(season_nums) if season_nums else 0,
                    'title': entry.get('title', '') or '',
                }
                result[result_key] = info
                # Also store under yearless key if no collision
                if key != result_key and key not in result:
                    result[key] = info
        elif item_type == 'movie':
            entry = _cache_lookup(movies_cache, key, year)
            if entry and _is_fresh(entry):
                # Movies don't have a status field in the cache; use
                # release_date presence to infer "Released" vs empty.
                rd = entry.get('release_date', '')
                status = 'Released' if rd else ''
                info = {
                    'poster_url': _poster_url(entry.get('poster_path', '')),
                    'tmdb_status': status,
                    'runtime': entry.get('runtime', 0),
                    'imdb_id': entry.get('imdb_id') or '',
                    'title': entry.get('title', '') or '',
                }
                result[result_key] = info
                if key != result_key and key not in result:
                    result[key] = info

    return result


_PUNCT_STRIP = re.compile(r'[^a-z0-9\s]')

# Common English stop words that should not count for title matching.
# Without this filter, "The Flash" would match "The Office" via "the".
_STOP_WORDS = frozenset({
    'the', 'a', 'an', 'of', 'and', 'in', 'on', 'at', 'to', 'for',
    'is', 'it', 'or', 'by', 'as', 'no', 'not', 'but', 'with', 'from',
})


def _key_words(norm_key):
    """Split a _normalize_title cache key into content words.

    Strips punctuation (apostrophes, colons) and removes stop words so
    that matching is based on meaningful title words only.
    """
    return set(_PUNCT_STRIP.sub('', norm_key).split()) - _STOP_WORDS


def _find_alt_show_entry(shows_cache, norm_key, max_season):
    """Search the show cache for an alternative entry covering *max_season*.

    Returns the best matching cache entry dict (most seasons) whose title
    contains all content words of *norm_key*, or None.  This catches
    reboots/revivals where a prefix was added (e.g. "Daredevil" finding
    "Marvel's Daredevil").

    Stop words are excluded to prevent "The Flash" matching "The Office".
    The query's content words must be a subset of the candidate's content
    words, so "Daredevil" can match "Marvel's Daredevil" but "Gordon"
    cannot match "Flash Gordon".
    """
    norm_words = _key_words(norm_key)
    if not norm_words:
        return None  # all stop words — cannot safely match

    best = None
    best_season_count = 0

    for alt_key, alt_entry in shows_cache.items():
        if alt_key == norm_key or not _is_fresh(alt_entry):
            continue
        alt_seasons = {s['number'] for s in alt_entry.get('seasons', [])}
        if max_season not in alt_seasons:
            continue
        alt_words = _key_words(alt_key)
        if not alt_words:
            continue
        # All query content words must appear in the candidate title.
        # {"daredevil"} ⊆ {"marvels", "daredevil"} ✓
        if not norm_words <= alt_words:
            continue
        if len(alt_seasons) > best_season_count:
            best = alt_entry
            best_season_count = len(alt_seasons)

    return best


def find_show_by_season(norm_key, max_season, year=None):
    """Season-aware show cache lookup.

    When the primary cache entry for *norm_key* doesn't contain
    *max_season*, searches for alternative entries whose title shares
    at least one word with *norm_key* and whose seasons DO cover
    *max_season*.  This handles reboots/revivals that share a common
    title (e.g. "Daredevil" S03 → "Marvel's Daredevil" instead of
    "Daredevil: Born Again" which only has S01-S02).

    Returns a dict ``{poster_url, tmdb_status, total_episodes, imdb_id,
    tmdb_id, max_cached_season}`` or None.
    """
    with _cache_lock:
        cache = _load_cache()
    shows_cache = cache.get('shows', {})

    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    def _format_entry(entry):
        aired_eps = 0
        for s in entry.get('seasons', []):
            for ep in s.get('episodes', []):
                ad = ep.get('air_date', '')
                if ad and ad <= today:
                    aired_eps += 1
        season_nums = [s['number'] for s in entry.get('seasons', [])]
        return {
            'poster_url': _poster_url(entry.get('poster_path', '')),
            'tmdb_status': entry.get('status', ''),
            'total_episodes': aired_eps,
            'imdb_id': entry.get('imdb_id') or '',
            'tmdb_id': entry.get('tmdb_id'),
            'max_cached_season': max(season_nums) if season_nums else 0,
            'title': entry.get('title', '') or '',
        }

    primary = _cache_lookup(shows_cache, norm_key, year)
    if primary and _is_fresh(primary):
        entry_seasons = {s['number'] for s in primary.get('seasons', [])}
        if max_season in entry_seasons:
            return _format_entry(primary)
    elif primary:
        return None  # stale primary, no fallback

    alt = _find_alt_show_entry(shows_cache, norm_key, max_season)
    if alt:
        logger.debug(
            "[tmdb] Season-aware fallback: '%s' S%02d resolved via alternative cache entry",
            norm_key, max_season,
        )
        return _format_entry(alt)

    # Primary exists but doesn't cover max_season, no alternative found.
    return None


def find_show_tmdb_id_by_season(norm_key, max_season, year=None):
    """Like find_show_by_season but returns just the TMDB ID (int or None).

    Used by symlink and rescan code that needs a TMDB ID for Sonarr lookup.
    """
    with _cache_lock:
        cache = _load_cache()
    shows_cache = cache.get('shows', {})

    primary = _cache_lookup(shows_cache, norm_key, year)
    if primary and _is_fresh(primary):
        entry_seasons = {s['number'] for s in primary.get('seasons', [])}
        if max_season in entry_seasons:
            return primary.get('tmdb_id')
    elif primary:
        return None  # stale primary, no fallback

    alt = _find_alt_show_entry(shows_cache, norm_key, max_season)
    if alt:
        logger.debug(
            "[tmdb] Season-aware TMDB ID fallback: '%s' S%02d → TMDB %s",
            norm_key, max_season, alt.get('tmdb_id'),
        )
        return alt.get('tmdb_id')

    # Primary exists but doesn't cover max_season, no alternative found.
    return None


def get_cached_episode_list(normalized_title, year=None):
    """Return aired, non-specials episodes from the TMDB cache.

    Used by the library reconcile to diff expected against present.  Season 0
    is always excluded (``get_show_metadata`` already drops it on fetch, but
    re-filter here for defense against stale pre-v2 cache entries).  Episodes
    with empty or future ``air_date`` are excluded — unaired content isn't
    "missing".  Returns ``[]`` when the title isn't cached or is stale; callers
    must treat an empty return as "don't know, skip" rather than "nothing
    aired" so we never trigger a spurious search for a title we have no data
    on.
    """
    from utils.library import _normalize_title as _nt
    norm = _nt(normalized_title or '')
    if not norm:
        return []
    with _cache_lock:
        cache = _load_cache()
    entry = _cache_lookup(cache.get('shows', {}), norm, year)
    if not entry or not _is_fresh(entry):
        return []
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    out = []
    for s in entry.get('seasons', []):
        snum = s.get('number', 0)
        if snum is None or snum <= 0:
            continue
        for ep in s.get('episodes', []):
            ad = ep.get('air_date', '')
            if not ad or ad > today:
                continue
            enum = ep.get('number', 0)
            if not isinstance(enum, int) or enum <= 0:
                continue
            out.append({'season': snum, 'number': enum, 'air_date': ad})
    return out


def get_cached_tmdb_ids():
    """Return cached TMDB IDs grouped by section (no API calls).

    Used by the library scanner to build alias maps so differently-named
    items that share a TMDB ID can be merged.

    Returns: {'shows': {norm_title: tmdb_id, ...}, 'movies': {norm_title: tmdb_id, ...}}

    Keys include both the stored key (which may be year-qualified like
    ``"the bridge (2013)"``) and a yearless alias when unambiguous, so
    callers using plain ``_normalize_title()`` lookups still match.
    """
    from utils.library import _normalize_title
    with _cache_lock:
        cache = _load_cache()

    _year_re = re.compile(r'^(.+)\s+\(\d{4}\)$')

    result = {}
    for section in ('shows', 'movies'):
        entries = {}
        # Track yearless bases to detect collisions
        yearless_seen = {}  # base -> tmdb_id
        yearless_collisions = set()
        for norm_title, entry in cache.get(section, {}).items():
            if not _is_fresh(entry):
                continue
            tmdb_id = entry.get('tmdb_id')
            if not tmdb_id:
                continue
            entries[norm_title] = tmdb_id
            # Extract yearless base for alias
            m = _year_re.match(norm_title)
            base = m.group(1) if m else norm_title
            if base in yearless_seen and yearless_seen[base] != tmdb_id:
                yearless_collisions.add(base)
            yearless_seen[base] = tmdb_id
        # Add yearless aliases for non-colliding entries
        for norm_title, tmdb_id in list(entries.items()):
            m = _year_re.match(norm_title)
            if m:
                base = m.group(1)
                if base not in yearless_collisions and base not in entries:
                    entries[base] = tmdb_id
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

                # Skip if already cached by another path with current schema.
                # Entries that are TTL-fresh but pre-v2 schema need refetching
                # so the new Plex-detail fields backfill.
                norm = _normalize_title(title)
                with _cache_lock:
                    c = _load_cache()
                    section = 'shows' if item_type == 'show' else 'movies'
                    entry = _cache_lookup(c.get(section, {}), norm, year)
                    if entry and _is_fresh(entry) and _has_current_schema(entry):
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
