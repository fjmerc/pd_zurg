"""API clients for Sonarr, Radarr, and Overseerr.

Delegates media acquisition from the Library "Download" button to the
user's media management stack.  All HTTP calls use urllib (no new deps).

Service priority:
  - TV shows:  Sonarr > Overseerr
  - Movies:    Radarr > Overseerr
  Sonarr/Radarr are preferred because the Download button targets content
  already visible in Plex (via debrid).  Overseerr rejects requests for
  media it considers "available," so it only works as a fallback for
  content not yet in the library.
"""

import json
import os
import urllib.error
import urllib.parse
import urllib.request

from base import load_secret_or_env
from utils.logger import get_logger

logger = get_logger()

_TIMEOUT = 15  # seconds — Arr APIs can be slow on large libraries


# ---------------------------------------------------------------------------
# Base HTTP helpers
# ---------------------------------------------------------------------------

class _ArrClientBase:
    """Shared HTTP plumbing for Arr-style APIs."""

    def __init__(self, url, api_key, service_name):
        self._base = url.rstrip('/') if url else ''
        self._api_key = api_key or ''
        self._name = service_name

    @property
    def configured(self):
        return bool(self._base and self._api_key)

    def _request(self, method, path, body=None, params=None):
        """Make an HTTP request. Returns parsed JSON or None on error."""
        if not self.configured:
            return None

        url = self._base + path
        if params:
            url += '?' + urllib.parse.urlencode(params)

        headers = {
            'User-Agent': 'pd_zurg/1.0',
            'Accept': 'application/json',
        }

        data = None
        if body is not None:
            data = json.dumps(body).encode('utf-8')
            headers['Content-Type'] = 'application/json'

        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        self._add_auth(req)

        try:
            with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
                raw = resp.read(50 * 1024 * 1024)
                if not raw:
                    return {}
                return json.loads(raw.decode('utf-8'))
        except urllib.error.HTTPError as e:
            body_text = ''
            try:
                body_text = e.read(4096).decode('utf-8', errors='replace')
            except Exception as read_err:
                logger.debug(f"[{self._name}] Could not read error body: {read_err}")
            logger.error(f"[{self._name}] HTTP {e.code} for {method} {path}")
            if body_text:
                logger.debug(f"[{self._name}] Response body: {body_text[:200]}")
            return None
        except (urllib.error.URLError, json.JSONDecodeError, OSError, ValueError) as e:
            logger.error(f"[{self._name}] Request failed for {method} {path}: {e}")
            return None

    def _add_auth(self, req):
        """Add service-specific auth header. Override in subclasses."""
        raise NotImplementedError

    def _get(self, path, params=None):
        return self._request('GET', path, params=params)

    def _post(self, path, body=None):
        return self._request('POST', path, body=body)

    def _delete(self, path, params=None):
        return self._request('DELETE', path, params=params)


# ---------------------------------------------------------------------------
# Sonarr
# ---------------------------------------------------------------------------

class SonarrClient(_ArrClientBase):
    """Sonarr v3 API client for TV show acquisition."""

    def __init__(self, url=None, api_key=None):
        url = url or os.environ.get('SONARR_URL', '')
        api_key = api_key or load_secret_or_env('sonarr_api_key') or ''
        super().__init__(url, api_key, 'sonarr')

    def _add_auth(self, req):
        req.add_header('X-Api-Key', self._api_key)

    def test_connection(self):
        """Test API connectivity. Returns True if reachable."""
        result = self._get('/api/v3/system/status')
        return result is not None

    def lookup_series(self, title=None, tmdb_id=None):
        """Find a series by title or TMDB ID.

        Returns the first match dict, or None.
        """
        if tmdb_id:
            result = self._get('/api/v3/series/lookup', {'term': f'tmdb:{tmdb_id}'})
        elif title:
            result = self._get('/api/v3/series/lookup', {'term': title})
        else:
            return None

        if isinstance(result, list) and result:
            return result[0]
        return None

    def get_series(self, series_id):
        """Get a series already in Sonarr by its internal ID."""
        return self._get(f'/api/v3/series/{series_id}')

    def get_all_series(self):
        """Get all series currently in Sonarr."""
        result = self._get('/api/v3/series')
        return result if isinstance(result, list) else []

    def find_series_in_library(self, tmdb_id=None, title=None):
        """Check if a series is already added to Sonarr.

        Returns the series dict if found, None otherwise.
        """
        all_series = self.get_all_series()
        for s in all_series:
            if tmdb_id and s.get('tmdbId') == tmdb_id:
                return s
            if title and s.get('title', '').lower() == title.lower():
                return s
        return None

    def add_series(self, lookup_result):
        """Add a series to Sonarr from a lookup result.

        Uses the first available root folder and quality profile.
        Returns the added series dict or None.
        """
        root_folders = self._get('/api/v3/rootfolder') or []
        if not root_folders:
            logger.error("[sonarr] No root folders configured")
            return None

        quality_profiles = self._get('/api/v3/qualityprofile') or []
        if not quality_profiles:
            logger.error("[sonarr] No quality profiles configured")
            return None

        body = {
            'title': lookup_result.get('title'),
            'tvdbId': lookup_result.get('tvdbId'),
            'tmdbId': lookup_result.get('tmdbId'),
            'imdbId': lookup_result.get('imdbId'),
            'titleSlug': lookup_result.get('titleSlug'),
            'images': lookup_result.get('images', []),
            'seasons': lookup_result.get('seasons', []),
            'qualityProfileId': quality_profiles[0]['id'],
            'rootFolderPath': root_folders[0]['path'],
            'monitored': True,
            'addOptions': {
                'searchForMissingEpisodes': False,
            },
        }
        return self._post('/api/v3/series', body)

    def get_episodes(self, series_id):
        """Get all episodes for a series."""
        return self._get('/api/v3/episode', {'seriesId': series_id}) or []

    def search_episodes(self, episode_ids):
        """Trigger a search for specific episodes by their Sonarr episode IDs.

        Returns the command dict or None.
        """
        if not episode_ids:
            return None
        return self._post('/api/v3/command', {
            'name': 'EpisodeSearch',
            'episodeIds': episode_ids,
        })

    def rescan_series(self, series_id):
        """Trigger a disk rescan for a series so Sonarr picks up new files."""
        return self._post('/api/v3/command', {
            'name': 'RescanSeries',
            'seriesId': series_id,
        })

    def ensure_and_search(self, title, tmdb_id, season_number, episode_numbers):
        """High-level: ensure series exists in Sonarr, then search for episodes.

        Args:
            title: Show title for lookup
            tmdb_id: TMDB ID (preferred for matching)
            season_number: Season number to search
            episode_numbers: List of episode numbers within the season

        Returns dict with status info, or raises on failure.
        """
        # Check if already in Sonarr
        series = self.find_series_in_library(tmdb_id=tmdb_id, title=title)

        if not series:
            # Look up and add
            lookup = self.lookup_series(title=title, tmdb_id=tmdb_id)
            if not lookup:
                return {'status': 'error', 'message': f'Series not found: {title}'}

            series = self.add_series(lookup)
            if not series:
                # Race condition: may have been added between find and add
                series = self.find_series_in_library(tmdb_id=tmdb_id, title=title)
                if not series:
                    return {'status': 'error', 'message': f'Failed to add series to Sonarr: {title}'}
                logger.info(f"[sonarr] Series already existed (race): {title} (ID: {series.get('id')})")
            else:
                logger.info(f"[sonarr] Added series: {title} (ID: {series.get('id')})")

        series_id = series.get('id')
        if series_id is None:
            return {'status': 'error', 'message': f'Sonarr returned series without ID for: {title}'}

        # Get episodes and find the ones we want
        episodes = self.get_episodes(series_id)
        target_ids = []
        for ep in episodes:
            if (ep.get('seasonNumber') == season_number
                    and ep.get('episodeNumber') in episode_numbers):
                ep_id = ep.get('id')
                if ep_id is not None:
                    target_ids.append(ep_id)

        if not target_ids:
            return {
                'status': 'error',
                'message': f'No matching episodes found in Sonarr for S{season_number:02d}',
            }

        # Trigger search
        cmd = self.search_episodes(target_ids)
        if cmd is None:
            return {'status': 'error', 'message': 'Failed to trigger episode search'}

        return {
            'status': 'sent',
            'service': 'sonarr',
            'message': f'Searching for {len(target_ids)} episode(s) of {title} S{season_number:02d}',
            'command_id': cmd.get('id'),
        }

    def delete_episode_file(self, file_id):
        """Delete an episode file by its Sonarr file ID."""
        return self._delete(f'/api/v3/episodefile/{file_id}')

    def remove_episodes(self, title, tmdb_id, season_number, episode_numbers):
        """High-level: remove episode files via Sonarr.

        Finds the series, identifies episode files for the requested
        season/episodes, and deletes them through Sonarr's API.
        """
        series = self.find_series_in_library(tmdb_id=tmdb_id, title=title)
        if not series:
            return {'status': 'error', 'message': f'Series not found in Sonarr: {title}'}

        series_id = series.get('id')
        if series_id is None:
            return {'status': 'error', 'message': 'Sonarr returned series without ID'}

        episodes = self.get_episodes(series_id)
        file_ids = set()
        for ep in episodes:
            if (ep.get('seasonNumber') == season_number
                    and ep.get('episodeNumber') in episode_numbers
                    and ep.get('hasFile')
                    and ep.get('episodeFileId')):
                file_ids.add(ep['episodeFileId'])

        if not file_ids:
            return {
                'status': 'error',
                'message': f'No files found in Sonarr for {title} S{season_number:02d}',
            }

        deleted = 0
        for fid in file_ids:
            result = self.delete_episode_file(fid)
            if result is not None:
                deleted += 1

        if deleted == 0:
            return {'status': 'error', 'message': 'Failed to remove files via Sonarr'}

        return {
            'status': 'removed',
            'service': 'sonarr',
            'message': f'Removed {deleted} episode(s) via Sonarr',
            'removed': deleted,
        }


# ---------------------------------------------------------------------------
# Radarr
# ---------------------------------------------------------------------------

class RadarrClient(_ArrClientBase):
    """Radarr v3 API client for movie acquisition."""

    def __init__(self, url=None, api_key=None):
        url = url or os.environ.get('RADARR_URL', '')
        api_key = api_key or load_secret_or_env('radarr_api_key') or ''
        super().__init__(url, api_key, 'radarr')

    def _add_auth(self, req):
        req.add_header('X-Api-Key', self._api_key)

    def test_connection(self):
        """Test API connectivity. Returns True if reachable."""
        result = self._get('/api/v3/system/status')
        return result is not None

    def lookup_movie(self, title=None, tmdb_id=None):
        """Find a movie by title or TMDB ID.

        Returns the first match dict, or None.
        """
        if tmdb_id:
            result = self._get('/api/v3/movie/lookup', {'term': f'tmdb:{tmdb_id}'})
        elif title:
            result = self._get('/api/v3/movie/lookup', {'term': title})
        else:
            return None

        if isinstance(result, list) and result:
            return result[0]
        return None

    def get_all_movies(self):
        """Get all movies currently in Radarr."""
        result = self._get('/api/v3/movie')
        return result if isinstance(result, list) else []

    def find_movie_in_library(self, tmdb_id=None, title=None):
        """Check if a movie is already added to Radarr.

        Returns the movie dict if found, None otherwise.
        """
        all_movies = self.get_all_movies()
        for m in all_movies:
            if tmdb_id and m.get('tmdbId') == tmdb_id:
                return m
            if title and m.get('title', '').lower() == title.lower():
                return m
        return None

    def add_movie(self, lookup_result):
        """Add a movie to Radarr from a lookup result.

        Uses the first available root folder and quality profile.
        Returns the added movie dict or None.
        """
        root_folders = self._get('/api/v3/rootfolder') or []
        if not root_folders:
            logger.error("[radarr] No root folders configured")
            return None

        quality_profiles = self._get('/api/v3/qualityprofile') or []
        if not quality_profiles:
            logger.error("[radarr] No quality profiles configured")
            return None

        body = {
            'title': lookup_result.get('title'),
            'tmdbId': lookup_result.get('tmdbId'),
            'imdbId': lookup_result.get('imdbId'),
            'titleSlug': lookup_result.get('titleSlug'),
            'images': lookup_result.get('images', []),
            'year': lookup_result.get('year'),
            'qualityProfileId': quality_profiles[0]['id'],
            'rootFolderPath': root_folders[0]['path'],
            'monitored': True,
            'addOptions': {
                'searchForMovie': True,
            },
        }
        return self._post('/api/v3/movie', body)

    def search_movie(self, movie_id):
        """Trigger a search for a specific movie.

        Returns the command dict or None.
        """
        return self._post('/api/v3/command', {
            'name': 'MoviesSearch',
            'movieIds': [movie_id],
        })

    def rescan_movie(self, movie_id):
        """Trigger a disk rescan for a movie so Radarr picks up new files."""
        return self._post('/api/v3/command', {
            'name': 'RescanMovie',
            'movieId': movie_id,
        })

    def ensure_and_search(self, title, tmdb_id):
        """High-level: ensure movie exists in Radarr, then trigger search.

        Args:
            title: Movie title for lookup
            tmdb_id: TMDB ID (preferred for matching)

        Returns dict with status info.
        """
        # Check if already in Radarr
        movie = self.find_movie_in_library(tmdb_id=tmdb_id, title=title)

        if movie:
            # Already in Radarr — trigger a search
            if movie.get('hasFile'):
                return {
                    'status': 'exists',
                    'service': 'radarr',
                    'message': f'{title} already has a file in Radarr',
                }
            movie_id = movie.get('id')
            if movie_id is None:
                return {'status': 'error', 'message': 'Radarr returned movie without ID'}
            cmd = self.search_movie(movie_id)
            if cmd is None:
                return {'status': 'error', 'message': 'Failed to trigger movie search'}
            return {
                'status': 'sent',
                'service': 'radarr',
                'message': f'Searching for {title}',
                'command_id': cmd.get('id'),
            }

        # Look up and add (addOptions.searchForMovie=True triggers immediate search)
        lookup = self.lookup_movie(title=title, tmdb_id=tmdb_id)
        if not lookup:
            return {'status': 'error', 'message': f'Movie not found: {title}'}

        movie = self.add_movie(lookup)
        if not movie:
            # Race condition: may have been added between find and add
            movie = self.find_movie_in_library(tmdb_id=tmdb_id, title=title)
            if not movie:
                return {'status': 'error', 'message': f'Failed to add movie to Radarr: {title}'}
            logger.info(f"[radarr] Movie already existed (race): {title} (ID: {movie.get('id')})")
        else:
            logger.info(f"[radarr] Added movie: {title} (ID: {movie.get('id')})")
        return {
            'status': 'sent',
            'service': 'radarr',
            'message': f'Added {title} to Radarr — searching now',
        }

    def delete_movie_file(self, file_id):
        """Delete a movie file by its Radarr file ID."""
        return self._delete(f'/api/v3/moviefile/{file_id}')

    def remove_movie(self, title, tmdb_id):
        """High-level: remove a movie file via Radarr."""
        movie = self.find_movie_in_library(tmdb_id=tmdb_id, title=title)
        if not movie:
            return {'status': 'error', 'message': f'Movie not found in Radarr: {title}'}

        if not movie.get('hasFile'):
            return {'status': 'error', 'message': f'{title} has no file in Radarr'}

        movie_file = movie.get('movieFile') or {}
        movie_file_id = movie_file.get('id')
        if movie_file_id is None:
            return {'status': 'error', 'message': 'Radarr movie missing file ID'}

        result = self.delete_movie_file(movie_file_id)
        if result is None:
            return {'status': 'error', 'message': 'Failed to remove movie file via Radarr'}

        return {
            'status': 'removed',
            'service': 'radarr',
            'message': f'Removed {title} via Radarr',
            'removed': 1,
        }


# ---------------------------------------------------------------------------
# Overseerr
# ---------------------------------------------------------------------------

class OverseerrClient(_ArrClientBase):
    """Overseerr API client for media requests."""

    def __init__(self, url=None, api_key=None):
        url = url or load_secret_or_env('seerr_address') or ''
        api_key = api_key or load_secret_or_env('seerr_api_key') or ''
        super().__init__(url, api_key, 'overseerr')

    def _add_auth(self, req):
        req.add_header('X-Api-Key', self._api_key)

    def test_connection(self):
        """Test API connectivity. Returns True if reachable."""
        result = self._get('/api/v1/status')
        return result is not None

    def search(self, title):
        """Search Overseerr for a title. Returns first result or None."""
        result = self._get('/api/v1/search', {
            'query': title,
            'page': '1',
            'language': 'en',
        })
        if not result:
            return None
        results = result.get('results', [])
        return results[0] if results else None

    def request_tv(self, tmdb_id, seasons):
        """Request a TV show (specific seasons) in Overseerr.

        Args:
            tmdb_id: TMDB ID of the show
            seasons: List of season numbers to request

        Returns the request dict or None.
        """
        return self._post('/api/v1/request', {
            'mediaType': 'tv',
            'mediaId': tmdb_id,
            'seasons': seasons,
        })

    def request_movie(self, tmdb_id):
        """Request a movie in Overseerr.

        Returns the request dict or None.
        """
        return self._post('/api/v1/request', {
            'mediaType': 'movie',
            'mediaId': tmdb_id,
        })

    def ensure_and_request_tv(self, title, tmdb_id, seasons):
        """High-level: request TV seasons in Overseerr.

        Args:
            title: Show title (for messages)
            tmdb_id: TMDB ID of the show
            seasons: List of season numbers

        Returns dict with status info.
        """
        if not tmdb_id:
            # Try to find it via search
            match = self.search(title)
            if not match:
                return {'status': 'error', 'message': f'Show not found: {title}'}
            tmdb_id = match.get('id')
            if not tmdb_id:
                return {'status': 'error', 'message': f'No TMDB ID found for: {title}'}

        result = self.request_tv(tmdb_id, seasons)
        if result is None:
            return {'status': 'error', 'message': f'Failed to request {title} in Overseerr'}

        season_str = ', '.join(f'S{s:02d}' for s in seasons)
        return {
            'status': 'requested',
            'service': 'overseerr',
            'message': f'Requested {title} {season_str} in Overseerr',
        }

    def ensure_and_request_movie(self, title, tmdb_id):
        """High-level: request a movie in Overseerr.

        Args:
            title: Movie title (for messages)
            tmdb_id: TMDB ID of the movie

        Returns dict with status info.
        """
        if not tmdb_id:
            match = self.search(title)
            if not match:
                return {'status': 'error', 'message': f'Movie not found: {title}'}
            tmdb_id = match.get('id')
            if not tmdb_id:
                return {'status': 'error', 'message': f'No TMDB ID found for: {title}'}

        result = self.request_movie(tmdb_id)
        if result is None:
            return {'status': 'error', 'message': f'Failed to request {title} in Overseerr'}

        return {
            'status': 'requested',
            'service': 'overseerr',
            'message': f'Requested {title} in Overseerr',
        }


# ---------------------------------------------------------------------------
# Service routing
# ---------------------------------------------------------------------------

def get_download_service(media_type):
    """Return the appropriate client for a media type, or None.

    Priority: Sonarr/Radarr > Overseerr > None

    Sonarr/Radarr are preferred because the Library Download button
    targets content already visible in Plex via debrid.  Overseerr
    rejects requests for media it considers "available" (HTTP 403),
    so it only serves as a fallback for content not yet in the library.

    Args:
        media_type: 'show' or 'movie'

    Returns (client_instance, service_name) or (None, None).
    """
    if media_type == 'show':
        client = SonarrClient()
        if client.configured:
            return client, 'sonarr'
    elif media_type == 'movie':
        client = RadarrClient()
        if client.configured:
            return client, 'radarr'

    # Fallback to Overseerr (works for content not yet in Plex)
    client = OverseerrClient()
    if client.configured:
        return client, 'overseerr'

    return None, None


def get_configured_services():
    """Return dict of which services are configured, for the UI.

    Returns:
        {
            'show': 'sonarr' | 'overseerr' | None,
            'movie': 'radarr' | 'overseerr' | None,
        }
    """
    show_svc = get_download_service('show')[1]
    movie_svc = get_download_service('movie')[1]
    return {'show': show_svc, 'movie': movie_svc}
