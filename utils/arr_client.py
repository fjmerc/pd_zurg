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

import datetime
import json
import os
import threading
import urllib.error
import urllib.parse
import urllib.request

from base import load_secret_or_env
from utils.logger import get_logger

logger = get_logger()

_TIMEOUT = 15  # seconds — Arr APIs can be slow on large libraries
_NOT_FOUND = object()  # sentinel for "looked up, not found" in tag cache
_tag_creation_lock = threading.Lock()  # prevents duplicate tags from concurrent requests


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

    def _put(self, path, body=None):
        return self._request('PUT', path, body=body)

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
        self._blackhole_tag_id = None  # None=not looked up, _NOT_FOUND=not found
        self._local_tag_id = None

    def _add_auth(self, req):
        req.add_header('X-Api-Key', self._api_key)

    def test_connection(self):
        """Test API connectivity. Returns True if reachable."""
        result = self._get('/api/v3/system/status')
        return result is not None

    # Usenet client implementations (lowercase for case-insensitive matching).
    # Used to distinguish usenet from torrent clients during tag discovery.
    _USENET_IMPLEMENTATIONS = frozenset({
        'nzbget', 'sabnzbd', 'nzbvortex', 'pneumatic',
        'usenetblackhole', 'usenetdownloadstation',
    })

    def _get_or_create_tag(self, label):
        """Find an existing tag by label or create one. Returns tag ID or None."""
        with _tag_creation_lock:
            tags = self._get('/api/v3/tag') or []
            for t in tags:
                if t.get('label', '').lower() == label.lower():
                    return t['id']
            result = self._post('/api/v3/tag', {'label': label})
            if result and 'id' in result:
                logger.info(f"[sonarr] Created tag '{label}' (ID: {result['id']})")
                return result['id']
            return None

    def _discover_routing_tags(self):
        """Discover tags used by download clients for routing.

        Identifies the blackhole tag and local tag from existing clients.
        When a blackhole client exists, ensures all other enabled clients
        are tagged so they don't act as universal catch-alls that intercept
        downloads meant for the blackhole (debrid).
        """
        if self._blackhole_tag_id is not None:
            return
        clients_raw = self._get('/api/v3/downloadclient')
        if clients_raw is None:
            return  # API error — leave uncached so next call retries
        self._blackhole_tag_id = _NOT_FOUND
        self._local_tag_id = _NOT_FOUND
        untagged_clients = []
        for c in clients_raw:
            if not c.get('enable'):
                continue
            impl = c.get('implementation', '')
            impl_lower = impl.lower()
            tags = c.get('tags', [])
            if impl_lower == 'torrentblackhole':
                if tags:
                    self._blackhole_tag_id = tags[0]
                    logger.debug(f"[sonarr] Blackhole client uses tag {self._blackhole_tag_id}")
                else:
                    bh_tag = self._get_or_create_tag('debrid')
                    if bh_tag is not None:
                        updated = dict(c, tags=[bh_tag])
                        if self._put(f'/api/v3/downloadclient/{c["id"]}', updated):
                            self._blackhole_tag_id = bh_tag
                            logger.info(f"[sonarr] Auto-tagged blackhole client '{c.get('name', '?')}' with debrid tag {bh_tag}")
                        else:
                            logger.warning(f"[sonarr] Failed to auto-tag blackhole client '{c.get('name', '?')}'")
                    else:
                        logger.warning(f"[sonarr] TorrentBlackhole client '{c.get('name', '?')}' has no tags — download routing will not work")
                continue
            if not tags:
                untagged_clients.append(c)
                continue
            if impl_lower not in self._USENET_IMPLEMENTATIONS and self._local_tag_id is _NOT_FOUND:
                self._local_tag_id = tags[0]
                logger.debug(f"[sonarr] Local torrent client ({impl}) uses tag {self._local_tag_id}")

        # No blackhole client found — no routing to fix
        if self._blackhole_tag_id is _NOT_FOUND:
            return

        # When a blackhole exists, untagged clients intercept debrid downloads.
        # Tag them with the local tag so debrid routing is exclusive.
        local_tag = self._local_tag_id if self._local_tag_id is not _NOT_FOUND else None
        tagged_client_ids = set()
        if untagged_clients:
            if local_tag is None:
                local_tag = self._get_or_create_tag('local')
                if local_tag is not None:
                    self._local_tag_id = local_tag
            if local_tag is not None:
                for c in untagged_clients:
                    c_name = c.get('name', c.get('implementation', '?'))
                    updated = dict(c, tags=[local_tag])
                    result = self._put(f'/api/v3/downloadclient/{c["id"]}', updated)
                    if result:
                        tagged_client_ids.add(c['id'])
                        logger.info(f"[sonarr] Tagged untagged client '{c_name}' with local tag {local_tag} to prevent debrid interception")
                    else:
                        logger.warning(f"[sonarr] Failed to tag client '{c_name}'")

        # Fix indexer routing: tag usenet indexers with local tag,
        # and ensure torrent indexers are accessible for debrid-tagged content
        indexers_fixed = self._fix_indexer_routing(tagged_client_ids, local_tag, self._blackhole_tag_id)

        # If torrent indexer tags were just corrected, re-search debrid-tagged
        # series that previously failed (0 indexers were visible for them)
        if indexers_fixed:
            self._search_debrid_missing()

        # Clean up stale queue items from re-tagged clients
        if tagged_client_ids:
            tagged_client_names = {
                c['name'] for c in untagged_clients
                if c['id'] in tagged_client_ids and c.get('name')
            }
            if tagged_client_names:
                self._clear_stale_queue_items(tagged_client_names)

    def _fix_indexer_routing(self, tagged_client_ids, local_tag, debrid_tag=None):
        """Fix indexer routing after auto-tagging download clients.

        1. Clear downloadClientId overrides pointing to newly-tagged clients
        2. Tag untagged usenet indexers with the local tag so they don't
           provide results for debrid-tagged series (which creates stale
           queue items that can't be delivered)
        3. Ensure torrent indexers with existing tags also carry the debrid
           tag so debrid-tagged series can discover them

        Returns True if any torrent indexer tags were fixed (debrid tag added).
        """
        torrent_indexers_fixed = False
        indexers = self._get('/api/v3/indexer')
        if not indexers:
            return False
        for ix in indexers:
            ix_name = ix.get('name', '?')
            changed = False
            updated = dict(ix)
            # Clear hardcoded downloadClientId pointing to re-tagged clients
            if updated.get('downloadClientId', 0) in tagged_client_ids:
                updated['downloadClientId'] = 0
                changed = True
                logger.debug(f"[sonarr] Clearing downloadClientId on indexer '{ix_name}'")
            # Tag untagged usenet indexers so they only serve local-tagged series
            if ix.get('protocol') == 'usenet' and local_tag is not None:
                existing_tags = updated.get('tags', [])
                if not existing_tags:
                    updated['tags'] = [local_tag]
                    changed = True
                    logger.debug(f"[sonarr] Tagging usenet indexer '{ix_name}' with local tag")
                elif local_tag not in existing_tags:
                    logger.info(f"[sonarr] Usenet indexer '{ix_name}' has existing tags {existing_tags} — verify it excludes debrid series")
            # Ensure torrent indexers are accessible for debrid-tagged content.
            # If a torrent indexer has tags but not the debrid tag, debrid-tagged
            # series/episodes won't find it during search. Only auto-tag indexers
            # whose sole tag is the local tag (set by our auto-routing); respect
            # user-configured tags by warning instead of overriding.
            if ix.get('protocol') == 'torrent' and debrid_tag is not None:
                existing_tags = updated.get('tags', [])
                if existing_tags and debrid_tag not in existing_tags:
                    if local_tag is not None and existing_tags == [local_tag]:
                        updated['tags'] = [local_tag, debrid_tag]
                        changed = True
                        torrent_fix_pending = True
                        logger.debug(f"[sonarr] Adding debrid tag to torrent indexer '{ix_name}' so debrid-tagged content can use it")
                    else:
                        torrent_fix_pending = False
                        logger.info(f"[sonarr] Torrent indexer '{ix_name}' has tags {existing_tags} but not debrid — verify it should serve debrid content")
            else:
                torrent_fix_pending = False
            if changed:
                result = self._put(f'/api/v3/indexer/{ix["id"]}', updated)
                if result:
                    if torrent_fix_pending:
                        torrent_indexers_fixed = True
                    logger.info(f"[sonarr] Fixed indexer routing for '{ix_name}'")
                else:
                    logger.warning(f"[sonarr] Failed to fix indexer routing for '{ix_name}'")
        return torrent_indexers_fixed

    def _search_debrid_missing(self):
        """Trigger search for debrid-tagged series with missing episodes.

        Called once after torrent indexer tags are fixed so that previously
        failed searches (0 indexers visible) get retried.
        """
        debrid_tag = self._blackhole_tag_id
        if debrid_tag is None or debrid_tag is _NOT_FOUND:
            return
        series_list = self._get('/api/v3/series')
        if not series_list:
            return
        missing_ids = []
        for s in series_list:
            if debrid_tag not in s.get('tags', []):
                continue
            if not s.get('monitored'):
                continue
            stats = s.get('statistics', {})
            if stats.get('episodeCount', 0) > stats.get('episodeFileCount', 0):
                missing_ids.append(s['id'])
        if not missing_ids:
            return
        max_batch = 25
        if len(missing_ids) > max_batch:
            logger.warning(
                f"[sonarr] {len(missing_ids)} debrid series with missing episodes — "
                f"searching first {max_batch} to avoid overloading Sonarr"
            )
            missing_ids = missing_ids[:max_batch]
        logger.info(f"[sonarr] Searching {len(missing_ids)} debrid-tagged series with missing episodes after indexer routing fix")
        for series_id in missing_ids:
            result = self._post('/api/v3/command', {'name': 'SeriesSearch', 'seriesId': series_id})
            if result is None:
                logger.warning(f"[sonarr] Failed to trigger search for series {series_id}")

    def _clear_stale_queue_items(self, client_names):
        """Remove queue items stuck as unavailable for newly-tagged clients."""
        queue = self._get('/api/v3/queue', {'pageSize': 1000, 'includeUnknownSeriesItems': 'true'})
        if not queue:
            return
        for r in queue.get('records', []):
            if (r.get('status') == 'downloadClientUnavailable'
                    and r.get('downloadClient') in client_names):
                item_id = r.get('id')
                if item_id is None:
                    continue
                title = r.get('title', '?')[:60]
                result = self._delete(f'/api/v3/queue/{item_id}', {'removeFromClient': 'true', 'blocklist': 'false'})
                if result is not None:
                    logger.info(f"[sonarr] Removed stale queue item '{title}' (was assigned to re-tagged client)")
                else:
                    logger.warning(f"[sonarr] Failed to remove stale queue item '{title}'")

    def _get_blackhole_tag_id(self):
        """Find the tag ID used by the TorrentBlackhole download client."""
        self._discover_routing_tags()
        return None if self._blackhole_tag_id is _NOT_FOUND else self._blackhole_tag_id

    def _get_local_tag_id(self):
        """Find the tag ID used by non-blackhole download clients."""
        self._discover_routing_tags()
        return None if self._local_tag_id is _NOT_FOUND else self._local_tag_id

    def _ensure_debrid_routing(self, series):
        """Add debrid tag and remove local tag so downloads route through blackhole."""
        debrid_tag = self._get_blackhole_tag_id()
        local_tag = self._get_local_tag_id()
        if debrid_tag is None:
            logger.warning(f"[sonarr] No blackhole tag configured — cannot route to debrid: {series.get('title')}")
            return series
        tags = list(series.get('tags', []))
        changed = False
        if debrid_tag not in tags:
            tags.append(debrid_tag)
            changed = True
        if local_tag is not None and local_tag in tags:
            tags.remove(local_tag)
            changed = True
        if not changed:
            return series
        series_copy = dict(series, tags=tags)
        result = self._put(f'/api/v3/series/{series["id"]}', series_copy)
        if result:
            logger.info(f"[sonarr] Routed to debrid: {series.get('title')}")
            return result
        logger.warning(f"[sonarr] Failed to update routing tags for: {series.get('title')}")
        return series

    def _ensure_local_routing(self, series):
        """Add local tag and remove debrid tag so downloads route to local clients."""
        debrid_tag = self._get_blackhole_tag_id()
        local_tag = self._get_local_tag_id()
        if local_tag is None and debrid_tag is None:
            return series
        if local_tag is None:
            # Can't add local tag — only safe to remove debrid if local tag exists
            logger.warning(f"[sonarr] No local torrent client tag configured — cannot route to local: {series.get('title')}")
            return series
        tags = list(series.get('tags', []))
        changed = False
        if debrid_tag is not None and debrid_tag in tags:
            tags.remove(debrid_tag)
            changed = True
        if local_tag not in tags:
            tags.append(local_tag)
            changed = True
        if not changed:
            return series
        series_copy = dict(series, tags=tags)
        result = self._put(f'/api/v3/series/{series["id"]}', series_copy)
        if result:
            logger.info(f"[sonarr] Routed to local: {series.get('title')}")
            return result
        logger.warning(f"[sonarr] Failed to update routing tags for: {series.get('title')}")
        return series

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

    def add_series(self, lookup_result, tags=None):
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
            'tags': tags or [],
            'addOptions': {
                'searchForMissingEpisodes': False,
            },
        }
        return self._post('/api/v3/series', body)

    def get_episodes(self, series_id):
        """Get all episodes for a series."""
        return self._get('/api/v3/episode', {'seriesId': series_id}) or []

    def get_episode_releases(self, episode_id):
        """Fetch available releases for an episode from all indexers.

        Returns list of release dicts, or empty list on failure.
        """
        result = self._get('/api/v3/release', {'episodeId': episode_id})
        return result if isinstance(result, list) else []

    def get_episode_id(self, series_title, season_number, episode_number):
        """Find a Sonarr episode ID by series title and S/E numbers.

        Returns the episode ID int, or None.
        """
        series = self.find_series_in_library(title=series_title)
        if not series:
            return None
        # Use season filter to avoid fetching all episodes for long-running shows
        episodes = self._get('/api/v3/episode', {
            'seriesId': series['id'],
            'seasonNumber': season_number,
        }) or []
        for ep in episodes:
            if ep.get('episodeNumber') == episode_number:
                return ep.get('id')
        return None

    def get_recent_grabs(self, page_size=50):
        """Fetch recent 'grabbed' history events.

        Returns list of history records with eventType='grabbed'.
        """
        result = self._get('/api/v3/history', {
            'pageSize': page_size,
            'sortKey': 'date',
            'sortDirection': 'descending',
            'eventType': 'grabbed',
        })
        if result and isinstance(result, dict):
            return result.get('records', [])
        return []

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

    def ensure_and_search(self, title, tmdb_id, season_number, episode_numbers, prefer_debrid=None):
        """High-level: ensure series exists in Sonarr, then search for episodes.

        Args:
            title: Show title for lookup
            tmdb_id: TMDB ID (preferred for matching)
            season_number: Season number to search
            episode_numbers: List of episode numbers within the season
            prefer_debrid: True=route via blackhole, False=route locally, None=don't touch

        Returns dict with status info, or raises on failure.
        """
        # Check if already in Sonarr
        series = self.find_series_in_library(tmdb_id=tmdb_id, title=title)

        just_added = False
        if not series:
            # Look up and add
            lookup = self.lookup_series(title=title, tmdb_id=tmdb_id)
            if not lookup:
                return {'status': 'error', 'message': f'Series not found: {title}'}

            add_tags = []
            if prefer_debrid is True:
                tag_id = self._get_blackhole_tag_id()
                if tag_id is not None:
                    add_tags.append(tag_id)
            elif prefer_debrid is False:
                tag_id = self._get_local_tag_id()
                if tag_id is not None:
                    add_tags.append(tag_id)
            else:
                # No preference — default to local tag so standard clients work
                tag_id = self._get_local_tag_id()
                if tag_id is not None:
                    add_tags.append(tag_id)
            series = self.add_series(lookup, tags=add_tags)
            if not series:
                # Race condition: may have been added between find and add
                series = self.find_series_in_library(tmdb_id=tmdb_id, title=title)
                if not series:
                    return {'status': 'error', 'message': f'Failed to add series to Sonarr: {title}'}
                logger.info(f"[sonarr] Series already existed (race): {title} (ID: {series.get('id')})")
                just_added = True
            else:
                logger.info(f"[sonarr] Added series: {title} (ID: {series.get('id')})")
                just_added = True

        series_id = series.get('id')
        if series_id is None:
            return {'status': 'error', 'message': f'Sonarr returned series without ID for: {title}'}

        # Route downloads through the correct client
        if prefer_debrid is True:
            series = self._ensure_debrid_routing(series)
        elif prefer_debrid is False:
            series = self._ensure_local_routing(series)

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
            if just_added:
                return {
                    'status': 'pending',
                    'service': 'sonarr',
                    'message': f'Added {title} to Sonarr — episode data is loading. Try again in a moment.',
                }
            return {
                'status': 'error',
                'message': f'No matching episodes found in Sonarr for S{season_number:02d}',
            }

        # Clear any stale queue items for this series before searching
        self._clear_unavailable_queue_items(series_id)

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

    def _clear_unavailable_queue_items(self, series_id):
        """Remove 'downloadClientUnavailable' queue items for the given series.

        Only removes items that have been stuck for at least 2 minutes to avoid
        deleting transiently unavailable items that may self-heal.
        """
        queue = self._get('/api/v3/queue', {'pageSize': 1000})
        if not queue:
            return
        now = datetime.datetime.utcnow()
        for r in queue.get('records', []):
            if (r.get('status') == 'downloadClientUnavailable'
                    and r.get('seriesId') == series_id):
                added = r.get('added', '')
                try:
                    added_dt = datetime.datetime.fromisoformat(added.rstrip('Z'))
                    if (now - added_dt).total_seconds() < 120:
                        continue
                except (ValueError, TypeError, AttributeError):
                    pass
                item_id = r.get('id')
                if item_id is None:
                    continue
                title = r.get('title', '?')[:60]
                result = self._delete(f'/api/v3/queue/{item_id}',
                                      {'removeFromClient': 'true', 'blocklist': 'false'})
                if result is not None:
                    logger.info(f"[sonarr] Removed stale queue item '{title}'")
                else:
                    logger.warning(f"[sonarr] Failed to remove stale queue item '{title}'")

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

    def audit_routing(self):
        """Re-audit download client and indexer routing tags.

        Resets cached tag state and re-runs discovery so any manual changes
        in Sonarr are detected and corrected. Fixes are logged individually
        by _discover_routing_tags.
        """
        self._blackhole_tag_id = None
        self._local_tag_id = None
        self._discover_routing_tags()

    def clean_all_stale_queue_items(self, max_age_seconds=120):
        """Remove ALL downloadClientUnavailable queue items older than max_age.

        Unlike _clear_stale_queue_items (which targets specific client names),
        this sweeps the entire queue.

        Returns number of items removed.
        """
        queue = self._get('/api/v3/queue', {'pageSize': 1000, 'includeUnknownSeriesItems': 'true'})
        if not queue:
            return 0
        removed = 0
        now = datetime.datetime.now(datetime.timezone.utc)
        for r in queue.get('records', []):
            if r.get('status') != 'downloadClientUnavailable':
                continue
            added = r.get('added', '')
            try:
                added_dt = datetime.datetime.fromisoformat(added.replace('Z', '+00:00'))
                if (now - added_dt).total_seconds() < max_age_seconds:
                    continue
            except (ValueError, TypeError, AttributeError):
                continue  # Can't determine age — skip rather than delete
            item_id = r.get('id')
            if item_id is None:
                continue
            title = r.get('title', '?')[:60]
            result = self._delete(f'/api/v3/queue/{item_id}',
                                  {'removeFromClient': 'true', 'blocklist': 'false'})
            if result is not None:
                removed += 1
                logger.info(f"[sonarr] Cleaned stale queue item '{title}'")
            else:
                logger.warning(f"[sonarr] Failed to clean stale queue item '{title}'")
        return removed


# ---------------------------------------------------------------------------
# Radarr
# ---------------------------------------------------------------------------

class RadarrClient(_ArrClientBase):
    """Radarr v3 API client for movie acquisition."""

    def __init__(self, url=None, api_key=None):
        url = url or os.environ.get('RADARR_URL', '')
        api_key = api_key or load_secret_or_env('radarr_api_key') or ''
        super().__init__(url, api_key, 'radarr')
        self._blackhole_tag_id = None  # None=not looked up, _NOT_FOUND=not found
        self._local_tag_id = None

    def _add_auth(self, req):
        req.add_header('X-Api-Key', self._api_key)

    def test_connection(self):
        """Test API connectivity. Returns True if reachable."""
        result = self._get('/api/v3/system/status')
        return result is not None

    _USENET_IMPLEMENTATIONS = SonarrClient._USENET_IMPLEMENTATIONS

    def _get_or_create_tag(self, label):
        """Find an existing tag by label or create one. Returns tag ID or None."""
        with _tag_creation_lock:
            tags = self._get('/api/v3/tag') or []
            for t in tags:
                if t.get('label', '').lower() == label.lower():
                    return t['id']
            result = self._post('/api/v3/tag', {'label': label})
            if result and 'id' in result:
                logger.info(f"[radarr] Created tag '{label}' (ID: {result['id']})")
                return result['id']
            return None

    def _discover_routing_tags(self):
        """Discover tags used by download clients for routing.

        Identifies the blackhole tag and local tag from existing clients.
        When a blackhole client exists, ensures all other enabled clients
        are tagged so they don't act as universal catch-alls that intercept
        downloads meant for the blackhole (debrid).
        """
        if self._blackhole_tag_id is not None:
            return
        clients_raw = self._get('/api/v3/downloadclient')
        if clients_raw is None:
            return  # API error — leave uncached so next call retries
        self._blackhole_tag_id = _NOT_FOUND
        self._local_tag_id = _NOT_FOUND
        untagged_clients = []
        for c in clients_raw:
            if not c.get('enable'):
                continue
            impl = c.get('implementation', '')
            impl_lower = impl.lower()
            tags = c.get('tags', [])
            if impl_lower == 'torrentblackhole':
                if tags:
                    self._blackhole_tag_id = tags[0]
                    logger.debug(f"[radarr] Blackhole client uses tag {self._blackhole_tag_id}")
                else:
                    bh_tag = self._get_or_create_tag('debrid')
                    if bh_tag is not None:
                        updated = dict(c, tags=[bh_tag])
                        if self._put(f'/api/v3/downloadclient/{c["id"]}', updated):
                            self._blackhole_tag_id = bh_tag
                            logger.info(f"[radarr] Auto-tagged blackhole client '{c.get('name', '?')}' with debrid tag {bh_tag}")
                        else:
                            logger.warning(f"[radarr] Failed to auto-tag blackhole client '{c.get('name', '?')}'")
                    else:
                        logger.warning(f"[radarr] TorrentBlackhole client '{c.get('name', '?')}' has no tags — download routing will not work")
                continue
            if not tags:
                untagged_clients.append(c)
                continue
            if impl_lower not in self._USENET_IMPLEMENTATIONS and self._local_tag_id is _NOT_FOUND:
                self._local_tag_id = tags[0]
                logger.debug(f"[radarr] Local torrent client ({impl}) uses tag {self._local_tag_id}")

        # No blackhole client found — no routing to fix
        if self._blackhole_tag_id is _NOT_FOUND:
            return

        # When a blackhole exists, untagged clients intercept debrid downloads.
        # Tag them with the local tag so debrid routing is exclusive.
        local_tag = self._local_tag_id if self._local_tag_id is not _NOT_FOUND else None
        tagged_client_ids = set()
        if untagged_clients:
            if local_tag is None:
                local_tag = self._get_or_create_tag('local')
                if local_tag is not None:
                    self._local_tag_id = local_tag
            if local_tag is not None:
                for c in untagged_clients:
                    c_name = c.get('name', c.get('implementation', '?'))
                    updated = dict(c, tags=[local_tag])
                    result = self._put(f'/api/v3/downloadclient/{c["id"]}', updated)
                    if result:
                        tagged_client_ids.add(c['id'])
                        logger.info(f"[radarr] Tagged untagged client '{c_name}' with local tag {local_tag} to prevent debrid interception")
                    else:
                        logger.warning(f"[radarr] Failed to tag client '{c_name}'")

        # Fix indexer routing: tag usenet indexers with local tag,
        # and ensure torrent indexers are accessible for debrid-tagged content
        indexers_fixed = self._fix_indexer_routing(tagged_client_ids, local_tag, self._blackhole_tag_id)

        # If torrent indexer tags were just corrected, re-search debrid-tagged
        # movies that previously failed (0 indexers were visible for them)
        if indexers_fixed:
            self._search_debrid_missing()

        # Clean up stale queue items from re-tagged clients
        if tagged_client_ids:
            tagged_client_names = {
                c['name'] for c in untagged_clients
                if c['id'] in tagged_client_ids and c.get('name')
            }
            if tagged_client_names:
                self._clear_stale_queue_items(tagged_client_names)

    def _fix_indexer_routing(self, tagged_client_ids, local_tag, debrid_tag=None):
        """Fix indexer routing after auto-tagging download clients.

        1. Clear downloadClientId overrides pointing to newly-tagged clients
        2. Tag untagged usenet indexers with the local tag so they don't
           provide results for debrid-tagged movies
        3. Ensure torrent indexers with existing tags also carry the debrid
           tag so debrid-tagged movies can discover them

        Returns True if any torrent indexer tags were fixed (debrid tag added).
        """
        torrent_indexers_fixed = False
        indexers = self._get('/api/v3/indexer')
        if not indexers:
            return False
        for ix in indexers:
            ix_name = ix.get('name', '?')
            changed = False
            updated = dict(ix)
            if updated.get('downloadClientId', 0) in tagged_client_ids:
                updated['downloadClientId'] = 0
                changed = True
                logger.debug(f"[radarr] Clearing downloadClientId on indexer '{ix_name}'")
            if ix.get('protocol') == 'usenet' and local_tag is not None:
                existing_tags = updated.get('tags', [])
                if not existing_tags:
                    updated['tags'] = [local_tag]
                    changed = True
                    logger.debug(f"[radarr] Tagging usenet indexer '{ix_name}' with local tag")
                elif local_tag not in existing_tags:
                    logger.info(f"[radarr] Usenet indexer '{ix_name}' has existing tags {existing_tags} — verify it excludes debrid movies")
            # Ensure torrent indexers are accessible for debrid-tagged content.
            # If a torrent indexer has tags but not the debrid tag, debrid-tagged
            # movies won't find it during search. Only auto-tag indexers whose
            # sole tag is the local tag (set by our auto-routing); respect
            # user-configured tags by warning instead of overriding.
            if ix.get('protocol') == 'torrent' and debrid_tag is not None:
                existing_tags = updated.get('tags', [])
                if existing_tags and debrid_tag not in existing_tags:
                    if local_tag is not None and existing_tags == [local_tag]:
                        updated['tags'] = [local_tag, debrid_tag]
                        changed = True
                        torrent_fix_pending = True
                        logger.debug(f"[radarr] Adding debrid tag to torrent indexer '{ix_name}' so debrid-tagged content can use it")
                    else:
                        torrent_fix_pending = False
                        logger.info(f"[radarr] Torrent indexer '{ix_name}' has tags {existing_tags} but not debrid — verify it should serve debrid content")
            else:
                torrent_fix_pending = False
            if changed:
                result = self._put(f'/api/v3/indexer/{ix["id"]}', updated)
                if result:
                    if torrent_fix_pending:
                        torrent_indexers_fixed = True
                    logger.info(f"[radarr] Fixed indexer routing for '{ix_name}'")
                else:
                    logger.warning(f"[radarr] Failed to fix indexer routing for '{ix_name}'")
        return torrent_indexers_fixed

    def _search_debrid_missing(self):
        """Trigger search for debrid-tagged movies missing files.

        Called once after torrent indexer tags are fixed so that previously
        failed searches (0 indexers visible) get retried.
        """
        debrid_tag = self._blackhole_tag_id
        if debrid_tag is None or debrid_tag is _NOT_FOUND:
            return
        movies = self._get('/api/v3/movie')
        if not movies:
            return
        missing_ids = [
            m['id'] for m in movies
            if debrid_tag in m.get('tags', [])
            and m.get('monitored')
            and not m.get('hasFile')
        ]
        if not missing_ids:
            return
        logger.info(f"[radarr] Searching {len(missing_ids)} debrid-tagged missing movie(s) after indexer routing fix")
        self._post('/api/v3/command', {'name': 'MoviesSearch', 'movieIds': missing_ids})

    def _clear_stale_queue_items(self, client_names):
        """Remove queue items stuck as unavailable for newly-tagged clients."""
        queue = self._get('/api/v3/queue', {'pageSize': 1000, 'includeUnknownMovieItems': 'true'})
        if not queue:
            return
        for r in queue.get('records', []):
            if (r.get('status') == 'downloadClientUnavailable'
                    and r.get('downloadClient') in client_names):
                item_id = r.get('id')
                if item_id is None:
                    continue
                title = r.get('title', '?')[:60]
                result = self._delete(f'/api/v3/queue/{item_id}', {'removeFromClient': 'true', 'blocklist': 'false'})
                if result is not None:
                    logger.info(f"[radarr] Removed stale queue item '{title}' (was assigned to re-tagged client)")
                else:
                    logger.warning(f"[radarr] Failed to remove stale queue item '{title}'")

    def _clear_unavailable_queue_items(self, movie_id):
        """Remove 'downloadClientUnavailable' queue items for the given movie.

        Only removes items that have been stuck for at least 2 minutes to avoid
        deleting transiently unavailable items that may self-heal.
        """
        queue = self._get('/api/v3/queue', {'pageSize': 1000})
        if not queue:
            return
        now = datetime.datetime.utcnow()
        for r in queue.get('records', []):
            if (r.get('status') == 'downloadClientUnavailable'
                    and r.get('movieId') == movie_id):
                added = r.get('added', '')
                try:
                    added_dt = datetime.datetime.fromisoformat(added.rstrip('Z'))
                    if (now - added_dt).total_seconds() < 120:
                        continue
                except (ValueError, TypeError, AttributeError):
                    pass
                item_id = r.get('id')
                if item_id is None:
                    continue
                title = r.get('title', '?')[:60]
                result = self._delete(f'/api/v3/queue/{item_id}',
                                      {'removeFromClient': 'true', 'blocklist': 'false'})
                if result is not None:
                    logger.info(f"[radarr] Removed stale queue item '{title}'")
                else:
                    logger.warning(f"[radarr] Failed to remove stale queue item '{title}'")

    def _get_blackhole_tag_id(self):
        """Find the tag ID used by the TorrentBlackhole download client."""
        self._discover_routing_tags()
        return None if self._blackhole_tag_id is _NOT_FOUND else self._blackhole_tag_id

    def _get_local_tag_id(self):
        """Find the tag ID used by non-blackhole download clients."""
        self._discover_routing_tags()
        return None if self._local_tag_id is _NOT_FOUND else self._local_tag_id

    def _ensure_debrid_routing(self, movie):
        """Add debrid tag and remove local tag so downloads route through blackhole."""
        debrid_tag = self._get_blackhole_tag_id()
        local_tag = self._get_local_tag_id()
        if debrid_tag is None:
            logger.warning(f"[radarr] No blackhole tag configured — cannot route to debrid: {movie.get('title')}")
            return movie
        tags = list(movie.get('tags', []))
        changed = False
        if debrid_tag not in tags:
            tags.append(debrid_tag)
            changed = True
        if local_tag is not None and local_tag in tags:
            tags.remove(local_tag)
            changed = True
        if not changed:
            return movie
        movie_copy = dict(movie, tags=tags)
        result = self._put(f'/api/v3/movie/{movie["id"]}', movie_copy)
        if result:
            logger.info(f"[radarr] Routed to debrid: {movie.get('title')}")
            return result
        logger.warning(f"[radarr] Failed to update routing tags for: {movie.get('title')}")
        return movie

    def _ensure_local_routing(self, movie):
        """Add local tag and remove debrid tag so downloads route to local clients."""
        debrid_tag = self._get_blackhole_tag_id()
        local_tag = self._get_local_tag_id()
        if local_tag is None and debrid_tag is None:
            return movie
        if local_tag is None:
            logger.warning(f"[radarr] No local torrent client tag configured — cannot route to local: {movie.get('title')}")
            return movie
        tags = list(movie.get('tags', []))
        changed = False
        if debrid_tag is not None and debrid_tag in tags:
            tags.remove(debrid_tag)
            changed = True
        if local_tag not in tags:
            tags.append(local_tag)
            changed = True
        if not changed:
            return movie
        movie_copy = dict(movie, tags=tags)
        result = self._put(f'/api/v3/movie/{movie["id"]}', movie_copy)
        if result:
            logger.info(f"[radarr] Routed to local: {movie.get('title')}")
            return result
        logger.warning(f"[radarr] Failed to update routing tags for: {movie.get('title')}")
        return movie

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

    def add_movie(self, lookup_result, tags=None):
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
            'tags': tags or [],
            'addOptions': {
                'searchForMovie': True,
            },
        }
        return self._post('/api/v3/movie', body)

    def get_movie_releases(self, movie_id):
        """Fetch available releases for a movie from all indexers.

        Returns list of release dicts, or empty list on failure.
        """
        result = self._get('/api/v3/release', {'movieId': movie_id})
        return result if isinstance(result, list) else []

    def get_recent_grabs(self, page_size=50):
        """Fetch recent 'grabbed' history events.

        Returns list of history records with eventType='grabbed'.
        """
        result = self._get('/api/v3/history', {
            'pageSize': page_size,
            'sortKey': 'date',
            'sortDirection': 'descending',
            'eventType': 'grabbed',
        })
        if result and isinstance(result, dict):
            return result.get('records', [])
        return []

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

    def ensure_and_search(self, title, tmdb_id, prefer_debrid=None):
        """High-level: ensure movie exists in Radarr, then trigger search.

        Args:
            title: Movie title for lookup
            tmdb_id: TMDB ID (preferred for matching)
            prefer_debrid: True=route via blackhole, False=route locally, None=don't touch

        Returns dict with status info.
        """
        # Check if already in Radarr
        movie = self.find_movie_in_library(tmdb_id=tmdb_id, title=title)

        if movie:
            # Route downloads through the correct client before any search
            if prefer_debrid is True:
                movie = self._ensure_debrid_routing(movie)
            elif prefer_debrid is False:
                movie = self._ensure_local_routing(movie)

            # Already in Radarr — skip search only if no routing preference
            if movie.get('hasFile') and prefer_debrid is None:
                return {
                    'status': 'exists',
                    'service': 'radarr',
                    'message': f'{title} already has a file in Radarr',
                }
            movie_id = movie.get('id')
            if movie_id is None:
                return {'status': 'error', 'message': 'Radarr returned movie without ID'}

            # Clear any stale queue items for this movie before searching
            self._clear_unavailable_queue_items(movie_id)

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

        add_tags = []
        if prefer_debrid is True:
            tag_id = self._get_blackhole_tag_id()
            if tag_id is not None:
                add_tags.append(tag_id)
        elif prefer_debrid is False:
            tag_id = self._get_local_tag_id()
            if tag_id is not None:
                add_tags.append(tag_id)
        else:
            tag_id = self._get_local_tag_id()
            if tag_id is not None:
                add_tags.append(tag_id)
        movie = self.add_movie(lookup, tags=add_tags)
        if not movie:
            # Race condition: may have been added between find and add
            movie = self.find_movie_in_library(tmdb_id=tmdb_id, title=title)
            if not movie:
                return {'status': 'error', 'message': f'Failed to add movie to Radarr: {title}'}
            logger.info(f"[radarr] Movie already existed (race): {title} (ID: {movie.get('id')})")
            # Apply routing to the race-found movie
            if prefer_debrid is True:
                movie = self._ensure_debrid_routing(movie)
            elif prefer_debrid is False:
                movie = self._ensure_local_routing(movie)
            if movie.get('id') is not None:
                self._clear_unavailable_queue_items(movie['id'])
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

    def audit_routing(self):
        """Re-audit download client and indexer routing tags.

        Resets cached tag state and re-runs discovery so any manual changes
        in Radarr are detected and corrected. Fixes are logged individually
        by _discover_routing_tags.
        """
        self._blackhole_tag_id = None
        self._local_tag_id = None
        self._discover_routing_tags()

    def clean_all_stale_queue_items(self, max_age_seconds=120):
        """Remove ALL downloadClientUnavailable queue items older than max_age.

        Returns number of items removed.
        """
        queue = self._get('/api/v3/queue', {'pageSize': 1000, 'includeUnknownMovieItems': 'true'})
        if not queue:
            return 0
        removed = 0
        now = datetime.datetime.now(datetime.timezone.utc)
        for r in queue.get('records', []):
            if r.get('status') != 'downloadClientUnavailable':
                continue
            added = r.get('added', '')
            try:
                added_dt = datetime.datetime.fromisoformat(added.replace('Z', '+00:00'))
                if (now - added_dt).total_seconds() < max_age_seconds:
                    continue
            except (ValueError, TypeError, AttributeError):
                continue  # Can't determine age — skip rather than delete
            item_id = r.get('id')
            if item_id is None:
                continue
            title = r.get('title', '?')[:60]
            result = self._delete(f'/api/v3/queue/{item_id}',
                                  {'removeFromClient': 'true', 'blocklist': 'false'})
            if result is not None:
                removed += 1
                logger.info(f"[radarr] Cleaned stale queue item '{title}'")
            else:
                logger.warning(f"[radarr] Failed to clean stale queue item '{title}'")
        return removed


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
