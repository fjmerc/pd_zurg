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

# Torrent IDs must be alphanumeric (with hyphens/underscores allowed).
# Real provider IDs are short (RD ~13 chars, TB numeric, AD numeric); the
# 128-char cap keeps a crafted oversized id out of logs and request URLs.
_SAFE_ID = re.compile(r'^[a-zA-Z0-9_-]{1,128}$')

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
        """Remove API key from error messages to prevent log leakage.

        Exact-string replacement alone misses urlencoded variants of the key
        embedded in request URLs (requests exceptions echo the full URL, and
        AllDebrid carries the key as an ``apikey=`` query param), so also
        redact credential-looking query params and bearer headers by regex.
        """
        msg = str(error)
        if self._api_key:
            msg = msg.replace(self._api_key, '***')
        msg = re.sub(r'(apikey|api_key|token|key|bearer)=[^&\s]+',
                     r'\1=***', msg, flags=re.IGNORECASE)
        msg = re.sub(r'(Authorization:\s*Bearer\s+)\S+', r'\1***', msg,
                     flags=re.IGNORECASE)
        return msg


# RD /torrents page size. list_torrents() does not paginate, so a
# response of exactly this length may be truncated — consumers that
# treat absence-from-list as "deleted on RD" (debrid_health pruning)
# must refuse to act on a possibly-incomplete list.
RD_LIST_LIMIT = 2500

# RD torrent lifecycle states, for pollers that watch a torrent after
# ``RealDebridClient.add_magnet``.  ``downloaded`` is the only "on RD
# storage, mountable" state; the fail set are terminal — waiting longer
# never rescues them.
RD_READY_STATES = frozenset({'downloaded'})
RD_FAIL_STATES = frozenset({'magnet_error', 'error', 'virus', 'dead'})


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
            params={'limit': RD_LIST_LIMIT},
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            # RD can return HTTP 200 with an error dict (auth degradation)
            # — a silent [] here would read as "account is empty" to
            # consumers like debrid_health's stale-entry pruning.
            raise ValueError(f'RD /torrents returned non-list payload ({type(data).__name__})')
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

    def add_magnet(self, info_hash):
        """Add a hash-only magnet to RD and select all files.

        Used by the plan 39 phase 3 cross-debrid rescue path when the
        primary debrid (typically TB) is filter-blocked and RD has the
        content cached — symmetric with ``TorBoxClient.add_magnet`` so
        either direction is supported.

        Returns the RD torrent ID string on success, or ``None`` on
        failure.  Errors are logged with the API key masked.
        """
        # Callers can't see the HTTP status through the None return, but
        # a 403/451 here means RD's keyword filter rejected the content —
        # a permanent condition, not a transient blip.  Record it so
        # attempt_add_rescue can surface it to callers as 'http_status'.
        self.last_add_status = None
        if not info_hash:
            return None
        magnet = f'magnet:?xt=urn:btih:{info_hash.upper()}'
        try:
            add_resp = tracked_request(
                self._name, requests.post,
                f'{self._BASE}/torrents/addMagnet',
                headers=self._headers(),
                data={'magnet': magnet},
                timeout=_TIMEOUT,
            )
            if add_resp.status_code not in (200, 201):
                self.last_add_status = add_resp.status_code
                logger.warning(
                    f"[debrid] RD addMagnet failed for {info_hash[:8]}…: "
                    f"HTTP {add_resp.status_code}"
                )
                return None
            torrent_id = add_resp.json().get('id')
            if not torrent_id:
                # 200 with no id: last_add_status stays None on purpose —
                # this is a malformed success, not a filter block, so
                # callers classify it as transient.
                return None
        except (requests.RequestException, ValueError) as e:
            logger.warning(
                f"[debrid] RD addMagnet failed for {info_hash[:8]}…: "
                f"{self._sanitize_error(e)}"
            )
            return None

        # selectFiles must be OUTSIDE the addMagnet try-block.  If the
        # add succeeded and the select failed, we'd otherwise return
        # None and leak a half-added torrent in RD's UI.  Now we own
        # the torrent_id and can clean up on failure.
        try:
            tracked_request(
                self._name, requests.post,
                f'{self._BASE}/torrents/selectFiles/{torrent_id}',
                headers=self._headers(),
                data={'files': 'all'},
                timeout=_TIMEOUT,
            )
        except (requests.RequestException, ValueError) as e:
            logger.warning(
                f"[debrid] RD selectFiles failed for {torrent_id} — "
                f"deleting half-added torrent: {self._sanitize_error(e)}"
            )
            try:
                self.delete_torrent(torrent_id)
            except Exception as cleanup_err:
                # Surface secondary failures so an operator can clean
                # up manually — silent ``pass`` here would leave the
                # half-added torrent stuck on the user's account with
                # no log breadcrumb.
                logger.warning(
                    f"[debrid] RD delete_torrent for half-added "
                    f"{torrent_id} also failed: "
                    f"{self._sanitize_error(cleanup_err)}. "
                    f"Manual cleanup required."
                )
            return None
        return str(torrent_id)

    def torrent_info(self, torrent_id):
        """Return the full ``/torrents/info/{id}`` dict, or ``None`` on error.

        Carries fields ``torrent_status``/``list_torrents`` drop:
        ``original_filename`` (the real torrent top-level name),
        ``files`` (with per-file ``path``), and ``added`` (ISO
        timestamp).  Consumers: symlink-retarget anchor derivation and
        the probe-add pre-existing-torrent guard.
        """
        if not _SAFE_ID.match(str(torrent_id)):
            return None
        try:
            resp = tracked_request(
                self._name, requests.get,
                f'{self._BASE}/torrents/info/{torrent_id}',
                headers=self._headers(),
                timeout=_TIMEOUT,
            )
            if resp.status_code != 200:
                # A 401 (stale key) / 429 (rate limit) / 5xx here reads the
                # same as a missing entry to the pre-existing-add guard
                # (both → None → conservatively "pre-existing"); log so an
                # orphaned probe entry left by that path is traceable.
                logger.debug(
                    f"[debrid] RD torrent_info HTTP {resp.status_code} "
                    f"for {torrent_id}"
                )
                return None
            data = resp.json()
            if not isinstance(data, dict):
                return None
            return data
        except (requests.RequestException, ValueError) as e:
            logger.warning(
                f"[debrid] RD torrent_info failed for {torrent_id}: "
                f"{self._sanitize_error(e)}"
            )
            return None

    def torrent_status(self, torrent_id):
        """Return the ``status`` of an RD torrent (or '' on error).

        Symmetric with ``TorBoxClient.torrent_status`` — used by the
        Wanted-recovery RD path to poll for ``downloaded`` after
        ``add_magnet``.  RD vocabulary: ``magnet_conversion``,
        ``waiting_files_selection``, ``queued``, ``downloading``,
        ``downloaded``, ``error``, ``magnet_error``, ``virus``, ``dead``,
        ``uploading``, ``compressing``.
        """
        info = self.torrent_info(torrent_id)
        if not info:
            return ''
        return str(info.get('status') or '')

    def select_files(self, torrent_id):
        """POST ``selectFiles files=all`` for a torrent.  True on 2xx.

        RD occasionally ignores the selectFiles that ``add_magnet``
        fires while the magnet is still in ``magnet_conversion``,
        leaving the torrent parked in ``waiting_files_selection``.
        Pollers that observe that state call this to re-issue the
        selection instead of timing out on a torrent that would have
        been instantly ready.
        """
        if not _SAFE_ID.match(str(torrent_id)):
            return False
        try:
            resp = tracked_request(
                self._name, requests.post,
                f'{self._BASE}/torrents/selectFiles/{torrent_id}',
                headers=self._headers(),
                data={'files': 'all'},
                timeout=_TIMEOUT,
            )
            return 200 <= resp.status_code < 300
        except (requests.RequestException, ValueError) as e:
            logger.warning(
                f"[debrid] RD select_files failed for {torrent_id}: "
                f"{self._sanitize_error(e)}"
            )
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

        # 403 and 451 both carry the filter-block body in the wild.
        # Verified live 2026-05-24: RD's May 2026 filter actually returns
        # HTTP 451 "Unavailable For Legal Reasons" (RFC 7725) with body
        # ``{"error":"infringing_file","error_code":35}`` — not the 403
        # that ElfHosted's writeup and Decypharr's repair worker assume.
        # Treat both status codes identically since the body shape is the
        # same; either accept means the file is filter-blocked.
        if resp.status_code in (403, 451):
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
                    'http': resp.status_code,
                }
            # Recognised status code but unrecognised body shape — RD's
            # response format may have drifted. Surface at WARN so future
            # drift is visible without crashing the sweep.
            logger.warning(
                f"[debrid] RD probe got unclassified {resp.status_code} "
                f"for {torrent_id}: body_keys={list(body.keys())}"
            )
            return {
                'status': 'unknown',
                'error': f'http_{resp.status_code}_unclassified',
            }

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
        info = self.torrent_info(torrent_id)
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
            # TB's ``/torrents/controltorrent`` operation field is
            # case-sensitive — accepts lowercase verbs only ("delete",
            # "reannounce", "resume", "stop_seeding") and rejects
            # capitalised forms with HTTP 200 + ``success=false`` +
            # ``error="INVALID_OPTION"``.  Pre-fix this sent ``"Delete"``
            # and silently failed every call, which is how the user's
            # TB account accumulated ghost entries from blackhole
            # cleanup-on-timeout + rescue cleanup-on-failure — every
            # delete looked succeeded in our logs (no exception) but
            # the torrent stayed.
            resp = tracked_request(
                self._name, requests.post,
                f'{self._BASE}/torrents/controltorrent',
                headers=self._headers(),
                json={'torrent_id': int(torrent_id), 'operation': 'delete'},
                timeout=_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get('success'):
                logger.info(f"[debrid] TB deleted torrent: {torrent_id}")
                return True
            # ``data.get('detail', '')`` returns ``None`` (not the default)
            # when the key is present with a ``null`` value — and TB's
            # error payloads use ``error``/``detail`` interchangeably with
            # ``null`` sometimes seen for either.  Coerce-to-empty before
            # subscripting so the diagnostic log doesn't raise
            # ``TypeError: 'NoneType' object is not subscriptable`` and
            # tear down every cleanup-on-failure caller.
            detail = str(data.get('detail') or data.get('error') or '')[:80]
            logger.error(
                f"[debrid] TB delete failed for {torrent_id}: "
                f"success={data.get('success')} detail={detail}"
            )
            return False
        except (requests.RequestException, ValueError, TypeError) as e:
            logger.error(f"[debrid] TB delete failed for {torrent_id}: {self._sanitize_error(e)}")
            return False

    def add_magnet(self, info_hash):
        """Add a hash-only magnet to TorBox.

        Used by the plan 39 phase 3 cross-debrid rescue path — when RD
        filter-blocks a hash and TB has it cached, this method puts the
        same hash on the user's TB account so the file is reachable via
        the TB rclone mount.  Returns the TB torrent ID string on
        success, or ``None`` on failure.

        TB's ``/torrents/createtorrent`` accepts a magnet form-field
        identical to the blackhole add path — there's no separate
        "rescue" endpoint to maintain.
        """
        if not info_hash:
            return None
        magnet = f'magnet:?xt=urn:btih:{info_hash.upper()}'
        try:
            resp = tracked_request(
                self._name, requests.post,
                f'{self._BASE}/torrents/createtorrent',
                headers=self._headers(),
                data={'magnet': magnet},
                timeout=_TIMEOUT,
            )
            if resp.status_code not in (200, 201):
                logger.warning(
                    f"[debrid] TB createtorrent failed for {info_hash[:8]}…: "
                    f"HTTP {resp.status_code}"
                )
                return None
            # Defensive: TB nominally returns ``{'data': {...}}`` but the
            # endpoint has been observed to return a list, a bare string,
            # or ``None`` under transient gateway / WAF responses.  Treat
            # anything non-dict as an unparseable success and bail —
            # AttributeError on .get() would surface as a noisy stacktrace.
            body = resp.json()
            data = body.get('data') if isinstance(body, dict) else None
            if not isinstance(data, dict):
                logger.warning(
                    f"[debrid] TB createtorrent returned unparseable body "
                    f"for {info_hash[:8]}…"
                )
                return None
            tb_id = data.get('torrent_id') or data.get('id')
            if not tb_id:
                return None
            return str(tb_id)
        except (requests.RequestException, ValueError) as e:
            logger.warning(
                f"[debrid] TB createtorrent failed for {info_hash[:8]}…: "
                f"{self._sanitize_error(e)}"
            )
            return None

    def torrent_info(self, torrent_id):
        """Return the full per-torrent mylist dict, or ``None`` on error.

        Carries ``files[].name`` — the only field that exposes the real
        on-disk TB folder (mylist ``name`` is a sanitized display string;
        the actual folder is the first component of ``files[].name``).
        Consumers: symlink-retarget TB-side folder derivation.
        """
        if not _SAFE_ID.match(str(torrent_id)):
            return None
        try:
            resp = tracked_request(
                self._name, requests.get,
                f'{self._BASE}/torrents/mylist',
                headers=self._headers(),
                params={'id': torrent_id, 'bypass_cache': 'true'},
                timeout=_TIMEOUT,
            )
            if resp.status_code != 200:
                # Same rationale as the RD path: a non-200 is indistinguishable
                # from a missing entry to the pre-existing-add guard; log it so
                # the resulting conservative skip-delete is traceable.
                logger.debug(
                    f"[debrid] TB torrent_info HTTP {resp.status_code} "
                    f"for {torrent_id}"
                )
                return None
            data = resp.json().get('data')
            if not isinstance(data, dict):
                return None
            return data
        except (requests.RequestException, ValueError) as e:
            logger.warning(
                f"[debrid] TB torrent_info failed for {torrent_id}: "
                f"{self._sanitize_error(e)}"
            )
            return None

    def torrent_status(self, torrent_id):
        """Return the ``download_state`` of a TB torrent (or '' on error).

        Lightweight wrapper used by the phase 3 rescue path to poll for
        a ready state after add_magnet — distinct from the heavier
        ``list_torrents`` because we only need one torrent's state.
        Callers compare against ``blackhole.TB_READY_STATES`` rather
        than a single literal — TB returns ``cached`` for instant cache
        hits (typical post-cache-probe rescue), ``completed`` for full
        BT downloads, and ``uploading`` for the post-download seed
        phase.  All three indicate the file is on TB storage.
        """
        info = self.torrent_info(torrent_id)
        if not info:
            return ''
        return str(info.get('download_state') or '')


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


def _all_configured_clients():
    """Return ``[(service_name, client), ...]`` for every provider keyed.

    Order follows ``_SERVICE_CLASSES`` insertion (RD, AD, TB) so a combined
    torrent list matches the single-provider priority order users already
    see elsewhere.
    """
    clients = []
    for svc, cls in _SERVICE_CLASSES.items():
        client = cls()
        if client.configured:
            clients.append((svc, client))
    return clients


def has_configured_debrid():
    """True when at least one provider has a usable key.

    Uses the same client-instantiation path as the actual query
    (``load_secret_or_env``, which honors Docker secrets), so it must be
    used for pre-flight gates instead of env-only checks — otherwise a
    secrets-only deployment gets a false "no provider configured".
    """
    return bool(_all_configured_clients())


def find_torrents_by_title_multi(normalized_titles, target_year=None):
    """Find matching torrents across ALL configured debrid providers.

    Unlike ``get_debrid_client()`` — which returns only the single priority
    provider — this queries every configured account, so a torrent living
    on a secondary provider (e.g. TorBox in an RD+TB setup) is still
    discoverable/removable from the library UI.  Each returned match carries
    a ``service`` field naming the provider it was found on, so the caller
    can route the eventual delete to the right account.

    Returns ``(matches, errors)``:
      - ``matches``: list of dicts ``{id, filename, hash, parsed_title,
        year, service}``
      - ``errors``: ``{service: sanitized_message}`` for providers whose
        listing call raised.  A partial failure does NOT abort the others —
        an RD outage must not block removing a TB item, and vice versa.
    """
    matches = []
    errors = {}
    for svc, client in _all_configured_clients():
        try:
            found = client.find_torrents_by_title(
                normalized_titles, target_year=target_year)
        except Exception as e:  # noqa: BLE001 - isolate one provider's failure
            errors[svc] = client._sanitize_error(e)
            continue
        for m in found:
            m['service'] = svc
            matches.append(m)
    return matches, errors


def delete_torrents_multi(items):
    """Delete torrents grouped by their owning provider.

    Args:
        items: iterable of ``{'id': <torrent id>, 'service': <provider>}``.
            Each id is routed to a client for its own ``service`` (via
            ``get_debrid_client(service=...)``) — never the priority
            fallback — so an id found on TorBox is deleted from TorBox and
            can't be misdirected to an unrelated RD torrent of the same
            id shape.

    Returns ``(deleted, failed)`` where ``failed`` lists the torrent-id
    strings that did not delete (unconfigured provider or provider-side
    failure).
    """
    by_service = {}
    for it in items:
        by_service.setdefault(it['service'], []).append(str(it['id']))

    deleted = 0
    failed = []
    for svc, ids in by_service.items():
        client, _name = get_debrid_client(service=svc)
        if client is None:
            failed.extend(ids)
            continue
        for tid in ids:
            if client.delete_torrent(tid):
                deleted += 1
            else:
                failed.append(tid)
    return deleted, failed
