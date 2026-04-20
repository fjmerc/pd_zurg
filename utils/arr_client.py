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
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from base import load_secret_or_env
from utils.logger import get_logger

logger = get_logger()

try:
    from utils import history as _history
except ImportError:
    _history = None

_TIMEOUT = 15  # seconds — Arr APIs can be slow on large libraries
_RELEASE_TIMEOUT = 120  # seconds — interactive search queries all indexers synchronously
_NOT_FOUND = object()  # sentinel for "looked up, not found" in tag cache
_tag_creation_lock = threading.Lock()  # prevents duplicate tags from concurrent requests

# Sonarr/Radarr rejection-reason substrings that the force-grab is designed
# to override (the feature's whole purpose is bypassing the cutoff when the
# user wants a debrid copy of a file they already have locally).  A release
# whose rejections are ALL in this family is still eligible; rejections for
# anything else (profile quality tier, custom format score, size floor,
# parse failure, etc.) disqualify the release.
_CUTOFF_REJECTION_SUBSTRINGS = (
    'meets quality cutoff',
    'equal or higher custom format score',
    'equal or higher preference',
)


def _is_number(v):
    """True for real numeric types (int/float), False for bool or non-numeric.

    `bool` subclasses `int` in Python, so `isinstance(True, int)` is True —
    exclude it explicitly so a `customFormatScore=True` from a buggy
    serializer doesn't sort as 1 above a legitimate score of 0.
    """
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _force_grab_eligible(release):
    """Return True if *release* should be considered by _grab_debrid_release.

    Non-rejected releases are always eligible.  Rejected releases are
    eligible only if every rejection reason is a string falling within
    the cutoff-met family — the force-grab's sole intended bypass.
    Rejections for profile violations (quality tier, custom format score,
    etc.) always disqualify.

    Note: the cutoff-met substrings are English only.  On a non-English
    Sonarr/Radarr instance, cutoff rejections won't match, and the feature
    degrades to "grab only non-rejected candidates" — still safe, just
    less permissive.  Configure `uiLanguage` to English on the arr or
    adjust the allowlist if this matters.
    """
    if not release.get('rejected'):
        return True
    reasons = release.get('rejections') or []
    if not reasons:
        logger.warning(
            f"[arr] Release marked rejected with no rejection reasons — "
            f"treating as ineligible: guid={release.get('guid')!r} "
            f"title={release.get('title')!r}"
        )
        return False
    return all(
        isinstance(reason, str)
        and any(sub in reason.lower() for sub in _CUTOFF_REJECTION_SUBSTRINGS)
        for reason in reasons
    )


def _force_grab_sort_key(release):
    """Sort key for force-grab candidates: highest customFormatScore, then seeders.

    Missing or non-numeric scores/seeders sort to the BOTTOM (via -inf/-1),
    not to zero — that prevents a release with an unknown score from
    outranking a release with a legitimately-negative score.  Booleans are
    rejected (bool is-a int in Python) to avoid False sorting as 0.
    """
    score = release.get('customFormatScore')
    if not _is_number(score):
        score = float('-inf')
    seeders = release.get('seeders')
    if not _is_number(seeders):
        seeders = -1
    return (score, seeders)


def _release_identifier(release):
    """Return the best stable identifier for dedup/logging, or None.

    Prefers guid → infoHash → title.  A release with none of these is
    undedupable and should be filtered out before push rather than
    pushed with a placeholder that collides on subsequent iterations.
    """
    return release.get('guid') or release.get('infoHash') or release.get('title') or None


def _log_val(v):
    """Format a release field for log output: number as-is, else '?'."""
    return v if _is_number(v) else '?'


# ---------------------------------------------------------------------------
# Quality-profile tier parsing (shared by Sonarr and Radarr — I5 parity)
# ---------------------------------------------------------------------------

# Per-profile cache TTL.  Profiles rarely change; paying an HTTP round trip
# on every compromise decision would be wasteful.  15 min matches the plan.
_PROFILE_CACHE_TTL_SECONDS = 15 * 60

# Fallback resolution-label parser when quality.resolution is missing or 0.
# Sonarr/Radarr quality names follow `<source>-<resolution>p` (`WEBDL-2160p`,
# `Bluray-1080p`, `HDTV-720p`, `WEBRip-480p`), so the resolution token is
# always at the end of the name.  Anchoring to end-of-string prevents a
# custom user-defined quality like `Mobile-480p-low` from being silently
# collapsed into the standard 480p tier (which would violate I1 — the
# profile is the ceiling, never invent a tier).  Anything outside the
# recognised tokens returns None and is dropped from the tier list.
_RESOLUTION_NAME_PATTERN = re.compile(r'(?:^|[^0-9])(2160|1080|720|576|480|360)p\Z', re.IGNORECASE)


def _resolution_label(quality):
    """Return a canonical resolution tier label ('2160p'/'1080p'/...) or None.

    Prefers the integer ``quality.resolution`` field (present since Sonarr v3
    and Radarr v4, values in pixels — 2160, 1080, 720, 576, 480, 360).  Falls
    back to parsing the quality name for older servers that don't populate
    ``resolution``.  Anything unrecognised returns None; the caller treats
    that as "drop this item" rather than assigning it to an arbitrary tier —
    this preserves Invariant I1 (the profile is the ceiling: never grab a
    tier we couldn't classify).
    """
    if not isinstance(quality, dict):
        return None
    res = quality.get('resolution')
    if _is_number(res) and res > 0:
        return f'{int(res)}p'
    name = quality.get('name')
    if not isinstance(name, str):
        return None
    m = _RESOLUTION_NAME_PATTERN.search(name)
    if m:
        return f'{m.group(1)}p'
    return None


def _iter_allowed_qualities(profile_items):
    """Yield (resolution_label, quality_name) pairs for every allowed quality.

    Handles both bare top-level items and groups:
      - Bare item: ``{'quality': {...}, 'allowed': true, 'items': []}``
      - Group:    ``{'name': 'HD-1080p', 'items': [...], 'allowed': true}``

    A disallowed top-level entry suppresses its entire subtree (matches
    Sonarr/Radarr UI semantics — unchecking a group hides every quality
    inside it even if their inner ``allowed`` flags are still ``true``).
    Inner items within an allowed group contribute only when their own
    ``allowed`` is ``true`` so partial-group selections are respected.
    """
    if not isinstance(profile_items, list):
        return
    for entry in profile_items:
        if not isinstance(entry, dict) or not entry.get('allowed'):
            continue
        inner_items = entry.get('items')
        if isinstance(inner_items, list) and inner_items:
            for inner in inner_items:
                if not isinstance(inner, dict) or not inner.get('allowed'):
                    continue
                q = inner.get('quality')
                label = _resolution_label(q)
                if label is not None:
                    yield label, (q or {}).get('name')
            continue
        q = entry.get('quality')
        label = _resolution_label(q)
        if label is not None:
            yield label, (q or {}).get('name')


# ---------------------------------------------------------------------------
# Base HTTP helpers
# ---------------------------------------------------------------------------

class _ArrClientBase:
    """Shared HTTP plumbing for Arr-style APIs."""

    def __init__(self, url, api_key, service_name):
        self._base = url.rstrip('/') if url else ''
        self._api_key = api_key or ''
        self._name = service_name
        # profile_id -> (expiry_monotonic, profile_dict).  15-min TTL; see
        # get_quality_profile.  Successful fetches only — failed fetches are
        # not cached so a transient 5xx doesn't lock us out of a profile for
        # 15 minutes.
        self._profile_cache = {}
        self._profile_cache_lock = threading.Lock()

    @property
    def configured(self):
        return bool(self._base and self._api_key)

    @property
    def base_url(self):
        """Public accessor for the arr's base URL.

        Used by callers that need a stable identifier for the instance
        (e.g. the compromise engine's ``arr_url_hash`` that disambiguates
        per-arr tier state in multi-instance setups).  Reaching into
        ``_base`` directly couples to the private layout; this property
        is the contract.
        """
        return self._base

    def _request(self, method, path, body=None, params=None, timeout=None):
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
            with urllib.request.urlopen(req, timeout=_TIMEOUT if timeout is None else timeout) as resp:
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

    def _get(self, path, params=None, timeout=None):
        return self._request('GET', path, params=params, timeout=timeout)

    def _post(self, path, body=None):
        return self._request('POST', path, body=body)

    def _put(self, path, body=None):
        return self._request('PUT', path, body=body)

    def _delete(self, path, params=None):
        return self._request('DELETE', path, params=params)

    # -- Quality profile reader (shared, Invariant I5 parity) --------------

    def get_quality_profile(self, profile_id):
        """Fetch a single quality profile by ID; 15-min in-memory cache.

        Profiles rarely change and compromise checks can fire repeatedly for
        the same item; caching amortises the HTTP cost across the dwell
        window.  ``profile_id`` must be a positive int; anything else
        returns None without an HTTP call.  Failed fetches (network error,
        404) return None and are NOT cached.
        """
        if not _is_number(profile_id) or profile_id <= 0:
            return None
        profile_id = int(profile_id)
        now = time.monotonic()
        with self._profile_cache_lock:
            entry = self._profile_cache.get(profile_id)
            if entry and entry[0] > now:
                return entry[1]
        profile = self._get(f'/api/v3/qualityprofile/{profile_id}')
        if not isinstance(profile, dict):
            return None
        with self._profile_cache_lock:
            self._profile_cache[profile_id] = (
                time.monotonic() + _PROFILE_CACHE_TTL_SECONDS,
                profile,
            )
        return profile

    def get_tier_order(self, profile_id):
        """Return the profile's allowed resolutions in preference order.

        Example: for a profile permitting Remux-2160p, WEBDL-2160p, Bluray-1080p,
        WEBDL-1080p, HDTV-720p, returns ``['2160p', '1080p', '720p']`` — within-
        resolution sources are collapsed (the arr's ``customFormatScore`` handles
        that preference within a tier; see ``_force_grab_sort_key``).

        Invariant I1 — profile is the ceiling: entries with ``allowed: false``
        are excluded, groups marked not-allowed suppress their whole subtree,
        and unrecognised resolution labels are dropped rather than guessed.

        Returns an empty list if the profile is missing, malformed, or
        contains no allowed resolvable qualities.
        """
        profile = self.get_quality_profile(profile_id)
        if not profile:
            return []
        seen = set()
        ordered = []
        for label, _quality_name in _iter_allowed_qualities(profile.get('items')):
            if label not in seen:
                seen.add(label)
                ordered.append(label)
        return ordered

    def _profile_id_from_record(self, record):
        """Extract ``qualityProfileId`` from a series/movie record, or None."""
        if not isinstance(record, dict):
            return None
        pid = record.get('qualityProfileId')
        if _is_number(pid) and pid > 0:
            return int(pid)
        return None


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
        self._usenet_tag_id = None

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

        Identifies the blackhole tag, local tag, and usenet tag from existing
        clients. When a blackhole client exists, ensures all other enabled
        clients are tagged so they don't act as universal catch-alls that
        intercept downloads meant for the blackhole (debrid).

        Usenet clients get an additional 'usenet' tag so prefer-local routing
        can target usenet exclusively while keeping them available for
        untagged/local-tagged content.
        """
        if self._blackhole_tag_id is not None:
            return
        clients_raw = self._get('/api/v3/downloadclient')
        if clients_raw is None:
            return  # API error — leave uncached so next call retries
        self._blackhole_tag_id = _NOT_FOUND
        self._local_tag_id = _NOT_FOUND
        self._usenet_tag_id = _NOT_FOUND
        untagged_clients = []
        usenet_clients = []
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
            if impl_lower in self._USENET_IMPLEMENTATIONS:
                usenet_clients.append(c)
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

        # Ensure usenet clients carry a dedicated 'usenet' tag so
        # prefer-local routing can target usenet exclusively.
        usenet_tag = None
        if usenet_clients:
            usenet_tag = self._get_or_create_tag('usenet')
            if usenet_tag is not None:
                self._usenet_tag_id = usenet_tag
                # Refresh local_tag in case it was just created above
                if local_tag is None:
                    local_tag = self._local_tag_id if self._local_tag_id is not _NOT_FOUND else None
                for c in usenet_clients:
                    c_tags = list(c.get('tags', []))
                    needed = []
                    if local_tag is not None and local_tag not in c_tags:
                        needed.append(local_tag)
                    if usenet_tag not in c_tags:
                        needed.append(usenet_tag)
                    if not needed:
                        continue
                    new_tags = c_tags + needed
                    c_name = c.get('name', c.get('implementation', '?'))
                    updated = dict(c, tags=new_tags)
                    if self._put(f'/api/v3/downloadclient/{c["id"]}', updated):
                        logger.info(f"[sonarr] Ensured usenet client '{c_name}' has usenet tag {usenet_tag}")
                    else:
                        logger.warning(f"[sonarr] Failed to update tags on usenet client '{c_name}'")

        # Fix indexer routing: tag usenet indexers with local+usenet tags,
        # and ensure torrent indexers are accessible for debrid-tagged content
        indexers_fixed = self._fix_indexer_routing(tagged_client_ids, local_tag, self._blackhole_tag_id, usenet_tag)

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

    def _fix_indexer_routing(self, tagged_client_ids, local_tag, debrid_tag=None, usenet_tag=None):
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
            torrent_fix_pending = False
            updated = dict(ix)
            # Clear hardcoded downloadClientId pointing to re-tagged clients
            if updated.get('downloadClientId', 0) in tagged_client_ids:
                updated['downloadClientId'] = 0
                changed = True
                logger.debug(f"[sonarr] Clearing downloadClientId on indexer '{ix_name}'")
            # Tag usenet indexers with local tag (and usenet tag if available)
            # so they only serve local/usenet-tagged series, not debrid ones.
            if ix.get('protocol') == 'usenet' and local_tag is not None:
                existing_tags = list(updated.get('tags', []))
                desired = [local_tag]
                if usenet_tag is not None:
                    desired.append(usenet_tag)
                if not existing_tags:
                    updated['tags'] = desired
                    changed = True
                    logger.debug(f"[sonarr] Tagging usenet indexer '{ix_name}' with tags {desired}")
                else:
                    missing = [t for t in desired if t not in existing_tags]
                    if missing:
                        updated['tags'] = existing_tags + missing
                        changed = True
                        logger.debug(f"[sonarr] Adding tags {missing} to usenet indexer '{ix_name}'")
                    elif local_tag not in existing_tags:
                        logger.info(f"[sonarr] Usenet indexer '{ix_name}' has existing tags {existing_tags} — verify it excludes debrid series")
            # Ensure torrent indexers are accessible for debrid-tagged content.
            # Sonarr v4 requires indexers to share a tag with the series —
            # untagged indexers are NOT universal. Add the debrid tag to:
            #   a) untagged torrent indexers (invisible to debrid series)
            #   b) indexers whose only tags are auto-routing ones (local/usenet)
            # Respect user-configured tags by warning instead of overriding.
            if ix.get('protocol') == 'torrent' and debrid_tag is not None:
                existing_tags = updated.get('tags', [])
                if debrid_tag not in existing_tags:
                    auto_tags = {t for t in (local_tag, usenet_tag) if t is not None}
                    if not existing_tags or (local_tag is not None and set(existing_tags) <= auto_tags):
                        # Add debrid tag; for untagged indexers also add local
                        # tag so the indexer serves both routing paths.
                        new_tags = set(existing_tags) | {debrid_tag}
                        if not existing_tags and local_tag is not None:
                            new_tags.add(local_tag)
                        updated['tags'] = list(new_tags)
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

    def _audit_untagged_series(self):
        """Find monitored series with no routing tag and apply the debrid tag.

        Self-heals the silent-failure mode where Overseerr (or a direct
        Sonarr-UI add) creates a series with an empty tag list: torrent
        indexers tagged with a routing tag treat untagged series as
        ineligible, so the post-add search returns "0 active indexers" and
        the series sits idle forever.

        Considers a series untagged iff its tag list shares no element with
        {debrid, local, usenet} — any routing tag is treated as a deliberate
        user choice and left alone. Unmonitored series are also skipped.

        Capped at 25 per run to bound search pressure on Sonarr.
        """
        if os.environ.get('ROUTING_AUTO_TAG_UNTAGGED', 'true').strip().lower() != 'true':
            return
        debrid_tag = self._blackhole_tag_id
        if debrid_tag is None or debrid_tag is _NOT_FOUND:
            return
        routing_tags = {debrid_tag}
        if self._local_tag_id not in (None, _NOT_FOUND):
            routing_tags.add(self._local_tag_id)
        if self._usenet_tag_id not in (None, _NOT_FOUND):
            routing_tags.add(self._usenet_tag_id)

        series_list = self._get('/api/v3/series')
        if not series_list:
            return
        candidates = [
            s for s in series_list
            if s.get('monitored')
            and routing_tags.isdisjoint(s.get('tags') or [])
        ]
        if not candidates:
            return

        max_batch = 25
        if len(candidates) > max_batch:
            logger.warning(
                f"[sonarr] {len(candidates)} untagged monitored series — "
                f"tagging first {max_batch} this cycle (remainder next run)"
            )
            candidates = candidates[:max_batch]

        # Apply the debrid tag directly (bypassing _ensure_debrid_routing)
        # so the audit can trust _put's None/non-None return as the success
        # signal. _ensure_debrid_routing's return-shape-based detection breaks
        # on 200-with-empty-body responses, which would skip the search for a
        # series that was actually tagged on Sonarr's side. Candidates are
        # known to lack local/usenet tags (filtered by routing_tags.isdisjoint),
        # so there's nothing to strip — a plain append is sufficient.
        tagged_ids = []
        for series in candidates:
            new_tags = list(series.get('tags') or []) + [debrid_tag]
            result = self._put(f'/api/v3/series/{series["id"]}', dict(series, tags=new_tags))
            if result is None:
                logger.warning(f"[sonarr] Failed to auto-tag untagged series: {series.get('title')}")
                continue
            logger.info(f"[sonarr] Auto-tagged untagged series with debrid tag: {series.get('title')}")
            tagged_ids.append(series['id'])

        if not tagged_ids:
            return

        search_count = 0
        for series_id in tagged_ids:
            result = self._post('/api/v3/command', {'name': 'SeriesSearch', 'seriesId': series_id})
            if result is None:
                logger.warning(f"[sonarr] Failed to trigger search for auto-tagged series {series_id}")
            else:
                search_count += 1

        logger.info(
            f"[sonarr] Auto-tagged {len(tagged_ids)} untagged monitored series with debrid tag, "
            f"triggered {search_count} search(es)"
        )
        if _history:
            _history.log_event(
                'routing_repaired',
                f'Sonarr auto-tagged {len(tagged_ids)} series',
                source='scheduler',
                detail=f'Applied debrid tag + triggered {search_count} search(es)',
            )

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

    def _get_usenet_tag_id(self):
        """Find the tag ID used exclusively by usenet download clients."""
        self._discover_routing_tags()
        return None if self._usenet_tag_id is _NOT_FOUND else self._usenet_tag_id

    def _ensure_debrid_routing(self, series):
        """Add debrid tag and remove local/usenet tags so downloads route through blackhole."""
        debrid_tag = self._get_blackhole_tag_id()
        local_tag = self._get_local_tag_id()
        usenet_tag = self._get_usenet_tag_id()
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
        if usenet_tag is not None and usenet_tag in tags:
            tags.remove(usenet_tag)
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
        """Route downloads to usenet (preferred) or any local client.

        When a usenet tag exists, applies usenet tag so only usenet clients
        and indexers handle the download.  Falls back to the local tag when
        no usenet client is configured.
        """
        debrid_tag = self._get_blackhole_tag_id()
        local_tag = self._get_local_tag_id()
        usenet_tag = self._get_usenet_tag_id()
        # Determine which tag to apply: usenet if available, else local
        target_tag = usenet_tag if usenet_tag is not None else local_tag
        if target_tag is None and debrid_tag is None:
            return series
        if target_tag is None:
            logger.warning(f"[sonarr] No local/usenet client tag configured — cannot route to local: {series.get('title')}")
            return series
        tags = list(series.get('tags', []))
        changed = False
        if debrid_tag is not None and debrid_tag in tags:
            tags.remove(debrid_tag)
            changed = True
        # When using usenet tag, remove stale local tag to keep routing clean
        if usenet_tag is not None and local_tag is not None and local_tag in tags:
            tags.remove(local_tag)
            changed = True
        if target_tag not in tags:
            tags.append(target_tag)
            changed = True
        if not changed:
            return series
        series_copy = dict(series, tags=tags)
        result = self._put(f'/api/v3/series/{series["id"]}', series_copy)
        if result:
            label = 'usenet' if usenet_tag is not None else 'local'
            logger.info(f"[sonarr] Routed to {label}: {series.get('title')}")
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

    def get_profile_id_for_series(self, series_id):
        """Return the quality profile ID assigned to a series, or None.

        Convenience wrapper around ``get_series`` that extracts the series
        record's ``qualityProfileId`` field; used by the compromise engine
        to look up which tier list to consult.
        """
        return self._profile_id_from_record(self.get_series(series_id))

    def get_all_series(self):
        """Get all series currently in Sonarr."""
        result = self._get('/api/v3/series')
        return result if isinstance(result, list) else []

    def find_series_in_library(self, tmdb_id=None, title=None):
        """Check if a series is already added to Sonarr.

        When both tmdb_id and title are provided, prefers a match on both
        criteria before falling back to single-criterion matches.
        Returns the series dict if found, None otherwise.
        """
        all_series = self.get_all_series()
        if tmdb_id and title:
            for s in all_series:
                if s.get('tmdbId') == tmdb_id and s.get('title', '').lower() == title.lower():
                    return s
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

        Uses a longer timeout because this queries all indexers synchronously.
        Returns list of release dicts, or empty list on failure.
        """
        result = self._get('/api/v3/release', {'episodeId': episode_id}, timeout=_RELEASE_TIMEOUT)
        return result if isinstance(result, list) else []

    def push_release(self, release):
        """Push a release for manual download, bypassing quality cutoff.

        Args:
            release: Release dict from get_episode_releases/get_movie_releases.

        Returns the response dict or None on failure.
        """
        return self._post('/api/v3/release', release)

    def _grab_debrid_release(self, episode_id, season_number=None, title='', seen_guids=None):
        """Interactive search + force grab of a torrent release for an episode.

        Used when prefer_debrid=True and the episode already has a file at
        cutoff quality.  Normal EpisodeSearch won't grab because the file
        isn't an upgrade — this bypasses the cutoff by doing an interactive
        search and manually pushing the best torrent release.

        Respects Sonarr's profile: releases rejected for reasons other than
        cutoff-met (quality tier not allowed, custom-format score below
        minimum, etc.) are excluded, and the remaining candidates are
        sorted by (customFormatScore desc, seeders desc) so the user's
        profile scoring picks the winner.  Filters by season_number because
        Sonarr returns releases for ALL seasons, not just the requested
        episode's season.  Skips releases already in seen_guids to avoid
        pushing the same season pack twice.

        Returns the pushed release's identifier (guid/infoHash/title) on
        success, or None on failure.
        """
        releases = self.get_episode_releases(episode_id)
        if not releases:
            logger.debug(f"[sonarr] No releases found for episode {episode_id}")
            return None
        _seen = seen_guids or set()
        torrents = [
            r for r in releases
            if r.get('protocol') == 'torrent'
            and (season_number is None
                 or r.get('seasonNumber') in (season_number, 0, None))
            and _release_identifier(r) is not None
            and r.get('guid') not in _seen
            and r.get('infoHash') not in _seen
            and _force_grab_eligible(r)
        ]
        if not torrents:
            logger.debug(f"[sonarr] No eligible torrent releases for episode {episode_id} S{season_number or '?'}")
            return None
        torrents.sort(key=_force_grab_sort_key, reverse=True)
        best = torrents[0]
        result = self.push_release(best)
        if result is not None:
            size_gb = round((best.get('size') or 0) / 1024**3, 2)
            logger.info(
                f"[sonarr] Force-grabbed debrid release for {title or f'episode {episode_id}'}: "
                f"{best.get('title', '?')} "
                f"(score={_log_val(best.get('customFormatScore'))}, "
                f"seeders={_log_val(best.get('seeders'))}, "
                f"size={size_gb}GB)"
            )
            return _release_identifier(best)
        logger.warning(f"[sonarr] Failed to push release for episode {episode_id}")
        return None

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
        Filters client-side for compatibility with all Sonarr/Radarr versions.
        """
        result = self._get('/api/v3/history', {
            'pageSize': page_size,
            'sortKey': 'date',
            'sortDirection': 'descending',
        })
        if result and isinstance(result, dict):
            return [r for r in result.get('records', [])
                    if isinstance(r, dict) and r.get('eventType') == 'grabbed']
        return []

    def search_episodes(self, episode_ids, media_title=None):
        """Trigger a search for specific episodes by their Sonarr episode IDs.

        Returns the command dict or None.
        """
        if not episode_ids:
            return None
        result = self._post('/api/v3/command', {
            'name': 'EpisodeSearch',
            'episodeIds': episode_ids,
        })
        if result and _history:
            _history.log_event('search_triggered', f'Sonarr episodes {episode_ids}',
                               source='arr', detail=f'EpisodeSearch for {len(episode_ids)} episode(s)',
                               media_title=media_title)
        return result

    def rescan_series(self, series_id, media_title=None):
        """Trigger a disk rescan for a series so Sonarr picks up new files."""
        result = self._post('/api/v3/command', {
            'name': 'RescanSeries',
            'seriesId': series_id,
        })
        if result and _history:
            _history.log_event('rescan_triggered', f'Sonarr series {series_id}',
                               source='arr', detail='RescanSeries',
                               media_title=media_title)
        return result

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
                usenet_id = self._get_usenet_tag_id()
                tag_id = usenet_id if usenet_id is not None else self._get_local_tag_id()
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
        has_file_ids = []
        no_file_ids = []
        for ep in episodes:
            if (ep.get('seasonNumber') == season_number
                    and ep.get('episodeNumber') in episode_numbers):
                ep_id = ep.get('id')
                if ep_id is not None:
                    target_ids.append(ep_id)
                    if ep.get('hasFile'):
                        has_file_ids.append(ep_id)
                    else:
                        no_file_ids.append(ep_id)

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

        # When prefer_debrid is set and episodes already have files, Sonarr's
        # automatic search won't grab because the existing files meet the
        # quality cutoff.  Use interactive search + manual push to bypass it.
        # Grabs each episode individually, deduplicating by GUID so season
        # packs are only pushed once.
        if prefer_debrid is True and has_file_ids:
            grabbed = 0
            seen_guids = set()
            for hf_id in has_file_ids:
                result_guid = self._grab_debrid_release(
                    hf_id, season_number=season_number,
                    title=f'{title} S{season_number:02d}', seen_guids=seen_guids,
                )
                if result_guid:
                    grabbed += 1
                    seen_guids.add(result_guid)
            # Search any episodes without files normally
            if no_file_ids:
                self.search_episodes(no_file_ids, media_title=title)
            if grabbed:
                return {
                    'status': 'sent',
                    'service': 'sonarr',
                    'message': f'Force-grabbed {grabbed} debrid release(s) for {title} S{season_number:02d}',
                }
            # All interactive grabs failed — no_file_ids already searched above,
            # only re-search has_file episodes as last resort.
            cmd = self.search_episodes(has_file_ids, media_title=title)
            return {
                'status': 'sent',
                'service': 'sonarr',
                'message': f'Searching for {len(target_ids)} episode(s) of {title} S{season_number:02d}',
                'command_id': cmd.get('id') if cmd else None,
            }

        # Trigger search
        cmd = self.search_episodes(target_ids, media_title=title)
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

    def delete_series(self, title, tmdb_id=None, delete_files=True):
        """Delete a series entirely from Sonarr."""
        series = self.find_series_in_library(tmdb_id=tmdb_id, title=title)
        if not series:
            return {'status': 'error', 'message': f'Series not found in Sonarr: {title}'}

        series_id = series.get('id')
        if series_id is None:
            return {'status': 'error', 'message': 'Sonarr returned series without ID'}

        params = {'deleteFiles': str(delete_files).lower()}
        result = self._delete(f'/api/v3/series/{series_id}', params=params)
        # _request returns {} for 204 No Content (success), None for errors
        if result is not None:
            return {
                'status': 'deleted',
                'service': 'sonarr',
                'message': f'Deleted {title} from Sonarr',
            }
        return {'status': 'error', 'message': 'Failed to delete series from Sonarr'}

    def audit_routing(self):
        """Re-audit download client and indexer routing tags.

        Resets cached tag state and re-runs discovery so any manual changes
        in Sonarr are detected and corrected. Fixes are logged individually
        by _discover_routing_tags. Also sweeps for series that were added
        without a routing tag (e.g. via Overseerr with empty tag config) and
        applies the debrid tag so their searches succeed.
        """
        self._blackhole_tag_id = None
        self._local_tag_id = None
        self._usenet_tag_id = None
        self._discover_routing_tags()
        self._audit_untagged_series()

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
        self._usenet_tag_id = None

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

        Identifies the blackhole tag, local tag, and usenet tag from existing
        clients. When a blackhole client exists, ensures all other enabled
        clients are tagged so they don't act as universal catch-alls that
        intercept downloads meant for the blackhole (debrid).

        Usenet clients get an additional 'usenet' tag so prefer-local routing
        can target usenet exclusively while keeping them available for
        untagged/local-tagged content.
        """
        if self._blackhole_tag_id is not None:
            return
        clients_raw = self._get('/api/v3/downloadclient')
        if clients_raw is None:
            return  # API error — leave uncached so next call retries
        self._blackhole_tag_id = _NOT_FOUND
        self._local_tag_id = _NOT_FOUND
        self._usenet_tag_id = _NOT_FOUND
        untagged_clients = []
        usenet_clients = []
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
            if impl_lower in self._USENET_IMPLEMENTATIONS:
                usenet_clients.append(c)
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

        # Ensure usenet clients carry a dedicated 'usenet' tag so
        # prefer-local routing can target usenet exclusively.
        usenet_tag = None
        if usenet_clients:
            usenet_tag = self._get_or_create_tag('usenet')
            if usenet_tag is not None:
                self._usenet_tag_id = usenet_tag
                # Refresh local_tag in case it was just created above
                if local_tag is None:
                    local_tag = self._local_tag_id if self._local_tag_id is not _NOT_FOUND else None
                for c in usenet_clients:
                    c_tags = list(c.get('tags', []))
                    needed = []
                    if local_tag is not None and local_tag not in c_tags:
                        needed.append(local_tag)
                    if usenet_tag not in c_tags:
                        needed.append(usenet_tag)
                    if not needed:
                        continue
                    new_tags = c_tags + needed
                    c_name = c.get('name', c.get('implementation', '?'))
                    updated = dict(c, tags=new_tags)
                    if self._put(f'/api/v3/downloadclient/{c["id"]}', updated):
                        logger.info(f"[radarr] Ensured usenet client '{c_name}' has usenet tag {usenet_tag}")
                    else:
                        logger.warning(f"[radarr] Failed to update tags on usenet client '{c_name}'")

        # Fix indexer routing: tag usenet indexers with local+usenet tags,
        # and ensure torrent indexers are accessible for debrid-tagged content
        indexers_fixed = self._fix_indexer_routing(tagged_client_ids, local_tag, self._blackhole_tag_id, usenet_tag)

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

    def _fix_indexer_routing(self, tagged_client_ids, local_tag, debrid_tag=None, usenet_tag=None):
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
            torrent_fix_pending = False
            updated = dict(ix)
            if updated.get('downloadClientId', 0) in tagged_client_ids:
                updated['downloadClientId'] = 0
                changed = True
                logger.debug(f"[radarr] Clearing downloadClientId on indexer '{ix_name}'")
            # Tag usenet indexers with local tag (and usenet tag if available)
            # so they only serve local/usenet-tagged movies, not debrid ones.
            if ix.get('protocol') == 'usenet' and local_tag is not None:
                existing_tags = list(updated.get('tags', []))
                desired = [local_tag]
                if usenet_tag is not None:
                    desired.append(usenet_tag)
                if not existing_tags:
                    updated['tags'] = desired
                    changed = True
                    logger.debug(f"[radarr] Tagging usenet indexer '{ix_name}' with tags {desired}")
                else:
                    missing = [t for t in desired if t not in existing_tags]
                    if missing:
                        updated['tags'] = existing_tags + missing
                        changed = True
                        logger.debug(f"[radarr] Adding tags {missing} to usenet indexer '{ix_name}'")
                    elif local_tag not in existing_tags:
                        logger.info(f"[radarr] Usenet indexer '{ix_name}' has existing tags {existing_tags} — verify it excludes debrid movies")
            # Ensure torrent indexers are accessible for debrid-tagged content.
            # Sonarr/Radarr v4 requires indexers to share a tag with the
            # movie — untagged indexers are NOT universal. Add the debrid tag
            # to untagged and auto-routing-only torrent indexers. Respect
            # user-configured tags by warning instead of overriding.
            if ix.get('protocol') == 'torrent' and debrid_tag is not None:
                existing_tags = updated.get('tags', [])
                if debrid_tag not in existing_tags:
                    auto_tags = {t for t in (local_tag, usenet_tag) if t is not None}
                    if not existing_tags or (local_tag is not None and set(existing_tags) <= auto_tags):
                        new_tags = set(existing_tags) | {debrid_tag}
                        if not existing_tags and local_tag is not None:
                            new_tags.add(local_tag)
                        updated['tags'] = list(new_tags)
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

    def _audit_untagged_movies(self):
        """Find monitored movies with no routing tag and apply the debrid tag.

        Self-heals the silent-failure mode where Overseerr (or a direct
        Radarr-UI add) creates a movie with an empty tag list: torrent
        indexers tagged with a routing tag treat untagged movies as
        ineligible, so the post-add search returns "0 active indexers" and
        the movie sits idle forever.

        Considers a movie untagged iff its tag list shares no element with
        {debrid, local, usenet} — any routing tag is treated as a deliberate
        user choice and left alone. Unmonitored movies are also skipped.

        Capped at 25 per run to bound search pressure on Radarr.
        """
        if os.environ.get('ROUTING_AUTO_TAG_UNTAGGED', 'true').strip().lower() != 'true':
            return
        debrid_tag = self._blackhole_tag_id
        if debrid_tag is None or debrid_tag is _NOT_FOUND:
            return
        routing_tags = {debrid_tag}
        if self._local_tag_id not in (None, _NOT_FOUND):
            routing_tags.add(self._local_tag_id)
        if self._usenet_tag_id not in (None, _NOT_FOUND):
            routing_tags.add(self._usenet_tag_id)

        movies = self._get('/api/v3/movie')
        if not movies:
            return
        candidates = [
            m for m in movies
            if m.get('monitored')
            and routing_tags.isdisjoint(m.get('tags') or [])
        ]
        if not candidates:
            return

        max_batch = 25
        if len(candidates) > max_batch:
            logger.warning(
                f"[radarr] {len(candidates)} untagged monitored movies — "
                f"tagging first {max_batch} this cycle (remainder next run)"
            )
            candidates = candidates[:max_batch]

        # Direct PUT (see Sonarr counterpart for rationale — avoids response
        # shape fragility in _ensure_debrid_routing).
        tagged_ids = []
        for movie in candidates:
            new_tags = list(movie.get('tags') or []) + [debrid_tag]
            result = self._put(f'/api/v3/movie/{movie["id"]}', dict(movie, tags=new_tags))
            if result is None:
                logger.warning(f"[radarr] Failed to auto-tag untagged movie: {movie.get('title')}")
                continue
            logger.info(f"[radarr] Auto-tagged untagged movie with debrid tag: {movie.get('title')}")
            tagged_ids.append(movie['id'])

        if not tagged_ids:
            return

        search_ok = self._post('/api/v3/command', {'name': 'MoviesSearch', 'movieIds': tagged_ids}) is not None
        if not search_ok:
            logger.warning(f"[radarr] Failed to trigger search for {len(tagged_ids)} auto-tagged movie(s)")
        search_count = len(tagged_ids) if search_ok else 0

        logger.info(
            f"[radarr] Auto-tagged {len(tagged_ids)} untagged monitored movie(s) with debrid tag, "
            f"triggered {search_count} search(es)"
        )
        if _history:
            _history.log_event(
                'routing_repaired',
                f'Radarr auto-tagged {len(tagged_ids)} movie(s)',
                source='scheduler',
                detail=f'Applied debrid tag + triggered {search_count} search(es)',
            )

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

    def _get_usenet_tag_id(self):
        """Find the tag ID used exclusively by usenet download clients."""
        self._discover_routing_tags()
        return None if self._usenet_tag_id is _NOT_FOUND else self._usenet_tag_id

    def _ensure_debrid_routing(self, movie):
        """Add debrid tag and remove local/usenet tags so downloads route through blackhole."""
        debrid_tag = self._get_blackhole_tag_id()
        local_tag = self._get_local_tag_id()
        usenet_tag = self._get_usenet_tag_id()
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
        if usenet_tag is not None and usenet_tag in tags:
            tags.remove(usenet_tag)
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
        """Route downloads to usenet (preferred) or any local client.

        When a usenet tag exists, applies usenet tag so only usenet clients
        and indexers handle the download.  Falls back to the local tag when
        no usenet client is configured.
        """
        debrid_tag = self._get_blackhole_tag_id()
        local_tag = self._get_local_tag_id()
        usenet_tag = self._get_usenet_tag_id()
        target_tag = usenet_tag if usenet_tag is not None else local_tag
        if target_tag is None and debrid_tag is None:
            return movie
        if target_tag is None:
            logger.warning(f"[radarr] No local/usenet client tag configured — cannot route to local: {movie.get('title')}")
            return movie
        tags = list(movie.get('tags', []))
        changed = False
        if debrid_tag is not None and debrid_tag in tags:
            tags.remove(debrid_tag)
            changed = True
        if usenet_tag is not None and local_tag is not None and local_tag in tags:
            tags.remove(local_tag)
            changed = True
        if target_tag not in tags:
            tags.append(target_tag)
            changed = True
        if not changed:
            return movie
        movie_copy = dict(movie, tags=tags)
        result = self._put(f'/api/v3/movie/{movie["id"]}', movie_copy)
        if result:
            label = 'usenet' if usenet_tag is not None else 'local'
            logger.info(f"[radarr] Routed to {label}: {movie.get('title')}")
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

    def get_movie(self, movie_id):
        """Get a movie already in Radarr by its internal ID."""
        return self._get(f'/api/v3/movie/{movie_id}')

    def get_profile_id_for_movie(self, movie_id):
        """Return the quality profile ID assigned to a movie, or None.

        Convenience wrapper around ``get_movie`` that extracts the movie
        record's ``qualityProfileId`` field; mirrors Sonarr's
        ``get_profile_id_for_series``.
        """
        return self._profile_id_from_record(self.get_movie(movie_id))

    def find_movie_in_library(self, tmdb_id=None, title=None):
        """Check if a movie is already added to Radarr.

        When both tmdb_id and title are provided, prefers a match on both
        criteria before falling back to single-criterion matches.
        Returns the movie dict if found, None otherwise.
        """
        all_movies = self.get_all_movies()
        if tmdb_id and title:
            for m in all_movies:
                if m.get('tmdbId') == tmdb_id and m.get('title', '').lower() == title.lower():
                    return m
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

        Uses a longer timeout because this queries all indexers synchronously.
        Returns list of release dicts, or empty list on failure.
        """
        result = self._get('/api/v3/release', {'movieId': movie_id}, timeout=_RELEASE_TIMEOUT)
        return result if isinstance(result, list) else []

    def push_release(self, release):
        """Push a release for manual download, bypassing quality cutoff."""
        return self._post('/api/v3/release', release)

    def _grab_debrid_release(self, movie_id, title=''):
        """Interactive search + force grab of a torrent release for a movie.

        Used when prefer_debrid=True and the movie already has a file at
        cutoff quality.  Bypasses the cutoff via manual push.

        Respects Radarr's profile: releases rejected for reasons other than
        cutoff-met (quality tier not allowed, custom-format score below
        minimum, etc.) are excluded, and the remaining candidates are
        sorted by (customFormatScore desc, seeders desc) so the user's
        profile scoring picks the winner.

        Returns True if a release was pushed, False otherwise.
        """
        releases = self.get_movie_releases(movie_id)
        if not releases:
            logger.debug(f"[radarr] No releases found for movie {movie_id}")
            return False
        torrents = [
            r for r in releases
            if r.get('protocol') == 'torrent'
            and _release_identifier(r) is not None
            and _force_grab_eligible(r)
        ]
        if not torrents:
            logger.debug(f"[radarr] No eligible torrent releases for movie {movie_id}")
            return False
        torrents.sort(key=_force_grab_sort_key, reverse=True)
        best = torrents[0]
        result = self.push_release(best)
        if result is not None:
            size_gb = round((best.get('size') or 0) / 1024**3, 2)
            logger.info(
                f"[radarr] Force-grabbed debrid release for {title or f'movie {movie_id}'}: "
                f"{best.get('title', '?')} "
                f"(score={_log_val(best.get('customFormatScore'))}, "
                f"seeders={_log_val(best.get('seeders'))}, "
                f"size={size_gb}GB)"
            )
            return True
        logger.warning(f"[radarr] Failed to push release for movie {movie_id}")
        return False

    def get_recent_grabs(self, page_size=50):
        """Fetch recent 'grabbed' history events.

        Returns list of history records with eventType='grabbed'.
        Filters client-side for compatibility with all Sonarr/Radarr versions.
        """
        result = self._get('/api/v3/history', {
            'pageSize': page_size,
            'sortKey': 'date',
            'sortDirection': 'descending',
        })
        if result and isinstance(result, dict):
            return [r for r in result.get('records', [])
                    if isinstance(r, dict) and r.get('eventType') == 'grabbed']
        return []

    def search_movie(self, movie_id, media_title=None):
        """Trigger a search for a specific movie.

        Returns the command dict or None.
        """
        result = self._post('/api/v3/command', {
            'name': 'MoviesSearch',
            'movieIds': [movie_id],
        })
        if result and _history:
            _history.log_event('search_triggered', f'Radarr movie {movie_id}',
                               source='arr', detail='MoviesSearch',
                               media_title=media_title)
        return result

    def rescan_movie(self, movie_id, media_title=None):
        """Trigger a disk rescan for a movie so Radarr picks up new files."""
        result = self._post('/api/v3/command', {
            'name': 'RescanMovie',
            'movieId': movie_id,
        })
        if result and _history:
            _history.log_event('rescan_triggered', f'Radarr movie {movie_id}',
                               source='arr', detail='RescanMovie',
                               media_title=media_title)
        return result

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

            # When prefer_debrid is set and movie already has a file, use
            # interactive search + manual push to bypass quality cutoff.
            if prefer_debrid is True and movie.get('hasFile'):
                grabbed = self._grab_debrid_release(movie_id, title=title)
                if grabbed:
                    return {
                        'status': 'sent',
                        'service': 'radarr',
                        'message': f'Force-grabbed debrid release for {title}',
                    }
                # Fall through to normal search as last resort

            cmd = self.search_movie(movie_id, media_title=title)
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
            usenet_id = self._get_usenet_tag_id()
            tag_id = usenet_id if usenet_id is not None else self._get_local_tag_id()
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

    def delete_movie(self, title, tmdb_id=None, delete_files=True):
        """Delete a movie entirely from Radarr."""
        movie = self.find_movie_in_library(tmdb_id=tmdb_id, title=title)
        if not movie:
            return {'status': 'error', 'message': f'Movie not found in Radarr: {title}'}

        movie_id = movie.get('id')
        if movie_id is None:
            return {'status': 'error', 'message': 'Radarr returned movie without ID'}

        params = {'deleteFiles': str(delete_files).lower()}
        result = self._delete(f'/api/v3/movie/{movie_id}', params=params)
        # _request returns {} for 204 No Content (success), None for errors
        if result is not None:
            return {
                'status': 'deleted',
                'service': 'radarr',
                'message': f'Deleted {title} from Radarr',
            }
        return {'status': 'error', 'message': 'Failed to delete movie from Radarr'}

    def audit_routing(self):
        """Re-audit download client and indexer routing tags.

        Resets cached tag state and re-runs discovery so any manual changes
        in Radarr are detected and corrected. Fixes are logged individually
        by _discover_routing_tags. Also sweeps for movies that were added
        without a routing tag (e.g. via Overseerr with empty tag config) and
        applies the debrid tag so their searches succeed.
        """
        self._blackhole_tag_id = None
        self._local_tag_id = None
        self._usenet_tag_id = None
        self._discover_routing_tags()
        self._audit_untagged_movies()

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
