"""Trigger Plex library section refreshes after the library scanner symlinks
new debrid content.

Why this exists
---------------
Content delivered by the library scanner (``library.py::_create_debrid_symlinks``)
is symlinked directly into the arr's canonical media folder and picked up by a
Sonarr/Radarr *RescanSeries/RescanMovie* — which is a disk rescan, NOT an import
event, so the arr's "update Plex library" connection never fires.  Zurg's
``on_library_update`` hook only covers RealDebrid content (not the TorBox mount),
so TorBox/scanner-delivered titles reach disk with nothing telling Plex to scan
— they stay invisible until a manual Plex scan.  This helper closes that gap.

Design
------
* urllib only (no ``plexapi`` dependency in the main process), matching
  ``arr_client.py`` / ``status_server.py``.
* Auth via the ``X-Plex-Token`` *header*, never the URL, so the token stays out
  of any logged request line (see the credential-handling rules).
* Best-effort: every failure is logged and swallowed.  A Plex refresh must never
  break a library scan.
* Full section refresh scoped by media type — no ``/data/media`` → Plex path
  translation, avoiding the mount-path-mismatch bug class.  Plex's scanner is
  incremental, so refreshing a section only processes changed files.
"""

import json
import urllib.request
import urllib.error
import logging

from base import load_secret_or_env

logger = logging.getLogger(__name__)

# Accepted media types -> the Plex library section ``type`` string.
_VALID_MEDIA_TYPES = {'show', 'movie'}

# Listing is a real request whose body we need; refreshes are fire-and-forget
# (empty 200), so give them a tighter budget — with a black-holed Plex the
# refreshes run serially on the scan thread and shouldn't stall it for minutes.
_LIST_TIMEOUT = 15
_REFRESH_TIMEOUT = 10


def _config():
    """(address, token) for the Plex server.

    Uses ``base.load_secret_or_env`` — Docker-secret first, env-var fallback —
    the same canonical precedence every other Plex-authenticating path in the
    project uses (``PLEXADD``/``PLEXTOKEN``, ``duplicate_cleanup``, Zurg's hook),
    so a rotated ``/run/secrets/plex_token`` can't be shadowed by a stale env var
    for this path alone."""
    addr = load_secret_or_env('plex_address')
    token = load_secret_or_env('plex_token')
    return addr, token


def _get_json(url, token):
    req = urllib.request.Request(
        url, headers={'X-Plex-Token': token, 'Accept': 'application/json'})
    with urllib.request.urlopen(req, timeout=_LIST_TIMEOUT) as resp:
        return json.loads(resp.read().decode('utf-8'))


def _refresh_section(base, section_key, token):
    url = f'{base}/library/sections/{section_key}/refresh'
    req = urllib.request.Request(url, headers={'X-Plex-Token': token})
    with urllib.request.urlopen(req, timeout=_REFRESH_TIMEOUT):
        return True


def refresh_plex_sections(media_type):
    """Refresh every Plex library section whose type is ``media_type``.

    ``media_type`` is ``'show'`` or ``'movie'``.  Returns the number of sections
    successfully asked to refresh.  Best-effort: returns 0 (never raises) when
    Plex isn't configured, the media type is unknown, the server is unreachable,
    or the section listing fails; a per-section failure is logged and the
    remaining sections still fire.
    """
    if media_type not in _VALID_MEDIA_TYPES:
        logger.debug(f"[plex] refresh skipped — unknown media_type {media_type!r}")
        return 0

    try:
        addr, token = _config()
    except Exception as e:
        logger.warning(f"[plex] Could not read Plex config for refresh: {e}")
        return 0
    if not addr or not token:
        logger.debug("[plex] refresh skipped — PLEX_ADDRESS/PLEX_TOKEN not set")
        return 0
    base = addr.rstrip('/')

    try:
        data = _get_json(f'{base}/library/sections', token)
        container = data.get('MediaContainer') if isinstance(data, dict) else None
        directories = container.get('Directory') if isinstance(container, dict) else None
        directories = directories if isinstance(directories, list) else []
    except Exception as e:
        logger.warning(f"[plex] Could not list library sections for refresh: {e}")
        return 0

    matching = [d for d in directories
                if isinstance(d, dict) and d.get('type') == media_type and d.get('key')]
    if not matching:
        logger.debug(f"[plex] No '{media_type}' sections to refresh")
        return 0

    refreshed = 0
    for d in matching:
        key = d.get('key')
        try:
            _refresh_section(base, key, token)
            refreshed += 1
            logger.info(
                f"[plex] Requested refresh of section {key} "
                f"({d.get('title', '?')})")
        except urllib.error.HTTPError as e:
            # A per-section HTTP error (e.g. a 500 on one section) is isolated —
            # keep refreshing the remaining sections.
            logger.warning(f"[plex] Refresh of section {key} failed: {e}")
        except urllib.error.URLError as e:
            # Connection-level failure — Plex is unreachable, so every remaining
            # section would fail the same way and waste the scan thread's time.
            logger.warning(
                f"[plex] Plex unreachable during section refresh — aborting "
                f"remaining refreshes: {e}")
            break
        except Exception as e:
            logger.warning(f"[plex] Refresh of section {key} failed: {e}")

    return refreshed
