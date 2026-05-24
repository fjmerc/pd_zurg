"""Debrid provider API clients for torrent listing and deletion.

Provides a unified interface across Real-Debrid, AllDebrid, and TorBox
for managing torrents at the provider level (Layer 1). Used by the
source preference system to remove debrid content when the user
chooses to prefer local copies.
"""

import os
import re

import requests

from base import load_secret_or_env
from utils.api_metrics import tracked_request
from utils.library import MEDIA_EXTENSIONS, normalize_title, parse_folder_name
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
            normalized_title: Pre-normalized title (e.g., 'the eternaut'),
                or an iterable of acceptable normalized titles for cases
                where the same canonical title has multiple parsed-folder
                aliases (e.g. multi-language torrents).  Empty strings are
                ignored.  Caller must normalize via library.normalize_title()
                before calling.
            target_year: Optional year to narrow matches. When both the
                target and parsed torrent have a year, they must agree.

        Returns list of dicts: [{id, filename, parsed_title, year}, ...]
        Raises if list_torrents() fails (API error).
        """
        if isinstance(normalized_title, str):
            accept = {normalized_title} if normalized_title else set()
        else:
            accept = {n for n in normalized_title if n}
        if not accept:
            return []

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
            if normalized not in accept:
                continue
            # Year-aware matching: if both sides have a year, they must agree
            if target_year is not None and parsed_year is not None:
                if target_year != parsed_year:
                    continue
            matches.append({
                'id': t['id'],
                'filename': filename,
                'hash': t.get('hash', ''),
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
                'hash': (t.get('hash') or '').upper(),
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

    def probe_file(self, torrent_id, sample_file_link=None):
        """Probe a torrent for RD-side filter blocks.

        Detects the May 2026 keyword filter-gate (RD returns 403 with
        ``{"error":"infringing_file","error_code":35}`` or 404 for
        filtered files). Used by the debrid health reconciler.

        Args:
            torrent_id: RD torrent ID. Validated against ``_SAFE_ID``.
            sample_file_link: Optional pre-fetched RD restricted link.
                When ``None``, ``/torrents/info/{id}`` is called and the
                smallest selected media file's link is used.

        Returns a dict:
            ``{'status': 'healthy'}`` — 200 from ``/unrestrict/link``.
            ``{'status': 'blocked', 'reason': 'infringing_file', 'http': 403}``
                — RD returned a recognised filter response.
            ``{'status': 'blocked', 'reason': 'not_found', 'http': 404}``
                — file gone from RD's hosters.
            ``{'status': 'unknown', 'error': <reason>}`` — anything else
                (network, 5xx, malformed body, missing media file, etc.).
                Re-probe ASAP on next sweep; don't reset healthy-TTL.
        """
        if not _SAFE_ID.match(str(torrent_id)):
            logger.error(f"[debrid] RD invalid torrent ID: {torrent_id!r}")
            return {'status': 'unknown', 'error': 'invalid_torrent_id'}

        if not sample_file_link:
            sample_file_link = self._pick_smallest_media_link(torrent_id)
            if not sample_file_link:
                return {'status': 'unknown', 'error': 'no_media_files'}

        try:
            resp = tracked_request(
                self._name, requests.post,
                f'{self._BASE}/unrestrict/link',
                headers=self._headers(),
                data={'link': sample_file_link},
                timeout=_TIMEOUT,
            )
        except requests.RequestException as e:
            logger.warning(
                f"[debrid] RD probe failed for {torrent_id}: "
                f"{self._sanitize_error(e)}"
            )
            return {'status': 'unknown', 'error': type(e).__name__}

        if resp.status_code == 200:
            return {'status': 'healthy'}

        if resp.status_code == 404:
            return {'status': 'blocked', 'reason': 'not_found', 'http': 404}

        if resp.status_code == 403:
            try:
                body = resp.json()
            except ValueError:
                body = {}
            if not isinstance(body, dict):
                body = {}
            if (body.get('error_code') == 35
                    or body.get('error') == 'infringing_file'):
                return {
                    'status': 'blocked',
                    'reason': 'infringing_file',
                    'http': 403,
                }
            # 403 with a body shape we don't recognise — RD's filter
            # format may have drifted. Surface at WARN so future drift
            # is visible without crashing the sweep.
            logger.warning(
                f"[debrid] RD probe got unclassified 403 for {torrent_id}: "
                f"body_keys={list(body.keys())}"
            )
            return {'status': 'unknown', 'error': 'http_403_unclassified'}

        return {'status': 'unknown', 'error': f'http_{resp.status_code}'}

    def _pick_smallest_media_link(self, torrent_id):
        """Return the restricted link for the smallest selected media file
        in ``torrent_id``, or ``None`` on any failure / no media file.

        ``/torrents/info`` returns ``files`` (all files in the torrent,
        with ``selected: 1`` for included ones) and ``links`` (restricted
        URLs parallel to the selected subset). We pick the smallest media
        file to minimise probe traffic — a release that fails the filter
        almost always fails on every file, so one sample is sufficient
        signal.
        """
        try:
            resp = tracked_request(
                self._name, requests.get,
                f'{self._BASE}/torrents/info/{torrent_id}',
                headers=self._headers(),
                timeout=_TIMEOUT,
            )
            resp.raise_for_status()
            info = resp.json()
        except (requests.RequestException, ValueError) as e:
            logger.warning(
                f"[debrid] RD info failed for {torrent_id}: "
                f"{self._sanitize_error(e)}"
            )
            return None

        if not isinstance(info, dict):
            return None

        files = info.get('files') or []
        links = info.get('links') or []
        if not isinstance(files, list) or not isinstance(links, list):
            return None

        selected = [
            f for f in files
            if isinstance(f, dict) and f.get('selected') == 1
        ]
        # RD's contract: ``links`` is parallel to the selected file
        # subset. A mismatch means we can't safely pair files to links —
        # bail rather than probe the wrong file.
        if not selected or len(selected) != len(links):
            return None

        media = [
            (f, link)
            for f, link in zip(selected, links)
            if isinstance(f.get('path'), str)
            and os.path.splitext(f['path'])[1].lower() in MEDIA_EXTENSIONS
            and isinstance(f.get('bytes'), int)
            and f['bytes'] > 0
            and isinstance(link, str) and link
        ]
        if not media:
            return None

        return min(media, key=lambda pair: pair[0]['bytes'])[1]


class AllDebridClient(DebridClientBase):
    """AllDebrid API client for magnet management."""

    _BASE = 'https://api.alldebrid.com/v4'

    def __init__(self, api_key=None):
        api_key = api_key or load_secret_or_env('ad_api_key') or ''
        super().__init__(api_key, 'alldebrid')

    def _params(self):
        return {'agent': 'zurgarr', 'apikey': self._api_key}

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
                'hash': (m.get('hash') or '').upper(),
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
                'hash': (t.get('hash') or '').upper(),
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


_SERVICE_CLASSES = {
    'realdebrid': RealDebridClient,
    'alldebrid': AllDebridClient,
    'torbox': TorBoxClient,
}


def get_debrid_client(service=None, api_key=None):
    """Factory — returns the appropriate debrid client.

    When ``service`` is given, builds a client for that specific provider
    (optionally with an explicit ``api_key`` override).  This is the
    **correct path** for callers that already know which provider they
    want to talk to — e.g. the blackhole watcher, which is bound to
    ``self.debrid_service`` / ``self.debrid_api_key`` for the lifetime
    of the process and must NOT route a torrent-ID through the priority
    fallback below (an AD magnet ID sent to RD can silently hit an
    unrelated RD torrent that happens to share the ID shape).

    When ``service`` is ``None``, falls back to priority-based detection
    (Real-Debrid > AllDebrid > TorBox) — matches the historical behavior
    for callers that don't care which account answers.

    Returns (client, service_name) or (None, None) when nothing is
    configured / the requested service isn't available.
    """
    if service:
        cls = _SERVICE_CLASSES.get(service)
        if not cls:
            return None, None
        client = cls(api_key) if api_key else cls()
        return (client, service) if client.configured else (None, None)

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
