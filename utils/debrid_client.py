"""Debrid provider API clients for torrent listing and deletion.

Provides a unified interface across Real-Debrid, AllDebrid, and TorBox
for managing torrents at the provider level (Layer 1). Used by the
source preference system to remove debrid content when the user
chooses to prefer local copies.
"""

import re

import requests

from base import load_secret_or_env
from utils.api_metrics import tracked_request
from utils.library import normalize_title, parse_folder_name
from utils.logger import get_logger

logger = get_logger()

_TIMEOUT = 15

# Torrent IDs must be alphanumeric (with hyphens/underscores allowed)
_SAFE_ID = re.compile(r'^[a-zA-Z0-9_-]+$')

MAX_BATCH_DELETE = 50


class DebridClientBase:
    """Base class for debrid provider API clients."""

    def __init__(self, api_key, service_name):
        self._api_key = api_key or ''
        self._name = service_name

    @property
    def configured(self):
        return bool(self._api_key)

    def list_torrents(self):
        """List all torrents from the provider.

        Returns list of dicts: [{id, filename, status, bytes}, ...]
        Raises on API error (caller must handle).
        """
        raise NotImplementedError

    def delete_torrent(self, torrent_id):
        """Delete a torrent by ID. Returns True on success.

        Implementations may receive string-serialized IDs and are
        responsible for their own type coercion.
        """
        raise NotImplementedError

    def find_torrents_by_title(self, normalized_title, target_year=None):
        """Find all torrents matching a show/movie title.

        Parses each torrent filename using the same logic the library
        scanner uses for mount folders, then compares normalized titles.

        Args:
            normalized_title: Pre-normalized title (e.g., 'the eternaut').
                Caller must normalize via library.normalize_title() before calling.
            target_year: Optional year to narrow matches. When both the
                target and parsed torrent have a year, they must agree.

        Returns list of dicts: [{id, filename, parsed_title, year}, ...]
        Raises if list_torrents() fails (API error).
        """
        matches = []

        torrents = self.list_torrents()
        for t in torrents:
            filename = t.get('filename', '')
            if not filename:
                continue
            # Strip .mkv/.mp4 etc. suffix before parsing — RD sometimes
            # stores single-file torrents with the extension in the filename
            name = filename
            for ext in ('.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv',
                        '.ts', '.m4v', '.webm'):
                if name.lower().endswith(ext):
                    name = name[:-len(ext)]
                    break
            parsed_title, parsed_year = parse_folder_name(name)
            normalized = normalize_title(parsed_title)
            if normalized != normalized_title:
                continue
            # Year-aware matching: if both sides have a year, they must agree
            if target_year is not None and parsed_year is not None:
                if target_year != parsed_year:
                    continue
            matches.append({
                'id': t['id'],
                'filename': filename,
                'parsed_title': parsed_title,
                'year': parsed_year,
            })

        return matches

    def _sanitize_error(self, error):
        """Remove API key from error messages to prevent log leakage."""
        msg = str(error)
        if self._api_key:
            msg = msg.replace(self._api_key, '***')
        return msg


class RealDebridClient(DebridClientBase):
    """Real-Debrid API client for torrent management."""

    _BASE = 'https://api.real-debrid.com/rest/1.0'

    def __init__(self, api_key=None):
        api_key = api_key or load_secret_or_env('rd_api_key') or ''
        super().__init__(api_key, 'realdebrid')

    def _headers(self):
        return {'Authorization': f'Bearer {self._api_key}'}

    def list_torrents(self):
        resp = tracked_request(
            self._name, requests.get,
            f'{self._BASE}/torrents',
            headers=self._headers(),
            params={'limit': 2500},
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            return []
        return [
            {
                'id': str(t.get('id', '')),
                'filename': t.get('filename', ''),
                'status': t.get('status', ''),
                'bytes': t.get('bytes', 0),
            }
            for t in data
        ]

    def delete_torrent(self, torrent_id):
        if not _SAFE_ID.match(str(torrent_id)):
            logger.error(f"[debrid] RD invalid torrent ID: {torrent_id!r}")
            return False
        try:
            resp = tracked_request(
                self._name, requests.delete,
                f'{self._BASE}/torrents/delete/{torrent_id}',
                headers=self._headers(),
                timeout=_TIMEOUT,
            )
            if resp.status_code == 204:
                logger.info(f"[debrid] RD deleted torrent: {torrent_id}")
                return True
            logger.error(f"[debrid] RD delete failed for {torrent_id}: HTTP {resp.status_code}")
            return False
        except requests.RequestException as e:
            logger.error(f"[debrid] RD delete failed for {torrent_id}: {self._sanitize_error(e)}")
            return False


class AllDebridClient(DebridClientBase):
    """AllDebrid API client for magnet management."""

    _BASE = 'https://api.alldebrid.com/v4'

    def __init__(self, api_key=None):
        api_key = api_key or load_secret_or_env('ad_api_key') or ''
        super().__init__(api_key, 'alldebrid')

    def _params(self):
        return {'agent': 'pd_zurg', 'apikey': self._api_key}

    def list_torrents(self):
        resp = tracked_request(
            self._name, requests.get,
            f'{self._BASE}/magnet/status',
            params=self._params(),
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        magnets = data.get('data', {}).get('magnets', [])
        if not isinstance(magnets, list):
            return []
        return [
            {
                'id': str(m.get('id', '')),
                'filename': m.get('filename', ''),
                'status': m.get('statusCode', ''),
                'bytes': m.get('size', 0),
            }
            for m in magnets
        ]

    def delete_torrent(self, torrent_id):
        if not _SAFE_ID.match(str(torrent_id)):
            logger.error(f"[debrid] AD invalid torrent ID: {torrent_id!r}")
            return False
        try:
            params = {**self._params(), 'id': torrent_id}
            resp = tracked_request(
                self._name, requests.get,
                f'{self._BASE}/magnet/delete',
                params=params,
                timeout=_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get('status') == 'success':
                logger.info(f"[debrid] AD deleted magnet: {torrent_id}")
                return True
            logger.error(f"[debrid] AD delete failed for {torrent_id}: status={data.get('status')}")
            return False
        except (requests.RequestException, ValueError) as e:
            logger.error(f"[debrid] AD delete failed for {torrent_id}: {self._sanitize_error(e)}")
            return False


class TorBoxClient(DebridClientBase):
    """TorBox API client for torrent management."""

    _BASE = 'https://api.torbox.app/v1/api'

    def __init__(self, api_key=None):
        api_key = api_key or load_secret_or_env('torbox_api_key') or ''
        super().__init__(api_key, 'torbox')

    def _headers(self):
        return {'Authorization': f'Bearer {self._api_key}'}

    def list_torrents(self):
        resp = tracked_request(
            self._name, requests.get,
            f'{self._BASE}/torrents/mylist',
            headers=self._headers(),
            params={'bypass_cache': 'true'},
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        torrents = data.get('data', [])
        if not isinstance(torrents, list):
            return []
        return [
            {
                'id': str(t.get('id', '')),
                'filename': t.get('name', ''),
                'status': t.get('download_state', ''),
                'bytes': t.get('size', 0),
            }
            for t in torrents
        ]

    def delete_torrent(self, torrent_id):
        if not _SAFE_ID.match(str(torrent_id)):
            logger.error(f"[debrid] TB invalid torrent ID: {torrent_id!r}")
            return False
        try:
            resp = tracked_request(
                self._name, requests.post,
                f'{self._BASE}/torrents/controltorrent',
                headers=self._headers(),
                json={'torrent_id': int(torrent_id), 'operation': 'Delete'},
                timeout=_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get('success'):
                logger.info(f"[debrid] TB deleted torrent: {torrent_id}")
                return True
            logger.error(f"[debrid] TB delete failed for {torrent_id}: success={data.get('success')}")
            return False
        except (requests.RequestException, ValueError) as e:
            logger.error(f"[debrid] TB delete failed for {torrent_id}: {self._sanitize_error(e)}")
            return False


def get_debrid_client():
    """Factory — returns the appropriate debrid client based on configured API key.

    Priority: Real-Debrid > AllDebrid > TorBox (matches blackhole.py detection).

    Returns (client, service_name) or (None, None) if nothing configured.
    """
    rd = RealDebridClient()
    if rd.configured:
        return rd, 'realdebrid'

    ad = AllDebridClient()
    if ad.configured:
        return ad, 'alldebrid'

    tb = TorBoxClient()
    if tb.configured:
        return tb, 'torbox'

    return None, None
