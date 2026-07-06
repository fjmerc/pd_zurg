"""Per-grab debrid routing helpers (plan 39 phase 2).

These pure functions answer three questions for every blackhole grab:

  1. Which debrid should host this torrent? (``pick_debrid_for_grab``)
  2. Where does that debrid's rclone mount live inside the container?
     (``mount_for_debrid``)
  3. What ``BLACKHOLE_SYMLINK_TARGET_BASE`` path do symlinks for that
     debrid point at? (``symlink_target_base_for_debrid``)

The same three helpers are consumed by ``utils/blackhole.py`` (new grabs),
``utils/debrid_health.py`` (phase 3 cross-debrid rescue), and
``utils/library.py`` (phase 4 dual-mount enumeration). Keeping them in
one module means the per-debrid mount-path / target-base contract has
a single source of truth — change it here, every caller updates.

Stateless, pure, easy to mock. No network calls live in this module;
the cache-aware routing path injects a probe callable so tests can
stub the debrid-cache lookup without monkeypatching ``utils.search``.
"""

import os
import re
from datetime import datetime
from utils.logger import get_logger

logger = get_logger()


# ---------------------------------------------------------------------------
# Service identifiers — must match the existing vocabulary used throughout
# blackhole.py / search.py / debrid_client.py / settings_api.py
# ---------------------------------------------------------------------------

REALDEBRID = 'realdebrid'
ALLDEBRID = 'alldebrid'
TORBOX = 'torbox'
VALID_DEBRIDS = (REALDEBRID, ALLDEBRID, TORBOX)

# Routing modes (env: BLACKHOLE_DEBRID_ROUTING)
ROUTING_CACHE_AWARE = 'cache_aware'
ROUTING_PRIMARY_ONLY = 'primary_only'
ROUTING_TAG = 'tag'                 # Reserved — Sonarr/Radarr tag-driven (future)
ROUTING_ROUND_ROBIN = 'round_robin' # Reserved — stress-test mode (future)
VALID_ROUTING_MODES = (
    ROUTING_CACHE_AWARE, ROUTING_PRIMARY_ONLY,
    ROUTING_TAG, ROUTING_ROUND_ROBIN,
)


# ---------------------------------------------------------------------------
# Configured-debrid discovery
# ---------------------------------------------------------------------------

def configured_debrids():
    """Return the tuple of debrid services that have an API key set.

    Order: RD, AD, TB.  Empty tuple when no debrid is configured (which
    today is a startup hard-error elsewhere, but the helper itself stays
    pure and returns ``()``).
    """
    services = []
    if os.environ.get('RD_API_KEY'):
        services.append(REALDEBRID)
    if os.environ.get('AD_API_KEY'):
        services.append(ALLDEBRID)
    if os.environ.get('TORBOX_API_KEY'):
        services.append(TORBOX)
    return tuple(services)


def resolve_primary():
    """Return the configured primary debrid service.

    Resolution order:

    1. ``BLACKHOLE_DEBRID_PRIMARY`` env var, if set to a configured debrid.
    2. Legacy ``BLACKHOLE_DEBRID`` env var (pre-plan-39 single-debrid
       routing knob), if set to a configured debrid.
    3. First configured debrid in (RD, AD, TB) order.

    Returns ``None`` when no debrid is configured.
    """
    configured = configured_debrids()
    if not configured:
        return None

    primary = (os.environ.get('BLACKHOLE_DEBRID_PRIMARY') or '').lower()
    if primary in VALID_DEBRIDS and primary in configured:
        return primary

    legacy = (os.environ.get('BLACKHOLE_DEBRID') or '').lower()
    if legacy in VALID_DEBRIDS and legacy in configured:
        return legacy

    return configured[0]


_RESERVED_MODES_WARNED = set()


def resolve_routing_mode():
    """Return the active routing mode.

    Explicit env var wins.  Otherwise: ``cache_aware`` when two or more
    debrids are configured, ``primary_only`` when only one (or zero) is.

    Reserved-but-unimplemented modes (``tag``, ``round_robin``) fall
    through to the default and emit a one-shot WARNING per process so
    operators don't silently get a different behaviour than they asked
    for.  The check is at decision-time rather than startup so it
    surfaces under SIGHUP env-var reloads too.
    """
    mode = (os.environ.get('BLACKHOLE_DEBRID_ROUTING') or '').lower()
    if mode in (ROUTING_CACHE_AWARE, ROUTING_PRIMARY_ONLY):
        return mode
    if mode in (ROUTING_TAG, ROUTING_ROUND_ROBIN):
        if mode not in _RESERVED_MODES_WARNED:
            _RESERVED_MODES_WARNED.add(mode)
            logger.warning(
                f"[routing] BLACKHOLE_DEBRID_ROUTING={mode!r} is reserved "
                f"for a future phase — falling back to the default "
                f"({ROUTING_CACHE_AWARE!r} with multi-debrid, "
                f"{ROUTING_PRIMARY_ONLY!r} otherwise).  Pick "
                f"{ROUTING_CACHE_AWARE!r} or {ROUTING_PRIMARY_ONLY!r} "
                f"explicitly to silence this warning."
            )
    elif mode:
        if mode not in _RESERVED_MODES_WARNED:
            _RESERVED_MODES_WARNED.add(mode)
            logger.warning(
                f"[routing] Unknown BLACKHOLE_DEBRID_ROUTING={mode!r} — "
                f"falling back to the default.  Valid: "
                f"{sorted(VALID_ROUTING_MODES)}"
            )
    configured = configured_debrids()
    return ROUTING_CACHE_AWARE if len(configured) >= 2 else ROUTING_PRIMARY_ONLY


# ---------------------------------------------------------------------------
# Mount + symlink-target lookup
# ---------------------------------------------------------------------------

def mount_for_debrid(debrid, rclone_mount_base='/data'):
    """Return the container-local rclone mount path for ``debrid``.

    Mirrors the layout produced by ``rclone/rclone.py``:

      - Real-Debrid (and AllDebrid when configured alone) → ``$RCLONE_MOUNT_NAME``
        under ``rclone_mount_base`` (typically ``/data/zurgarr``).
      - When RD and AD coexist, each gets a ``_RD`` / ``_AD`` suffix; we
        pick the one matching the requested debrid.
      - TorBox → ``$TORBOX_MOUNT_NAME`` (default ``torbox``).

    Returns ``None`` for an unknown debrid name.
    """
    if debrid == TORBOX:
        return os.path.join(rclone_mount_base, os.environ.get('TORBOX_MOUNT_NAME') or 'torbox')

    rclonemn = os.environ.get('RCLONE_MOUNT_NAME') or ''
    if not rclonemn:
        return None

    rd_key = bool(os.environ.get('RD_API_KEY'))
    ad_key = bool(os.environ.get('AD_API_KEY'))
    if rd_key and ad_key:
        # Dual-Zurg layout — see rclone/rclone.py setup()
        if debrid == REALDEBRID:
            return os.path.join(rclone_mount_base, f"{rclonemn}_RD")
        if debrid == ALLDEBRID:
            return os.path.join(rclone_mount_base, f"{rclonemn}_AD")
        return None

    if debrid in (REALDEBRID, ALLDEBRID):
        # Single-Zurg layout — both single-debrid configurations land at
        # the unsuffixed mount name (only one of RD/AD is set).
        return os.path.join(rclone_mount_base, rclonemn)

    return None


def symlink_target_base_for_debrid(debrid):
    """Return the host-side symlink target base for ``debrid``.

    The base is the path Plex/Sonarr/Radarr see — distinct from the
    container-local rclone mount.  TorBox uses a separate base so it
    can be a separate Plex library (plan 39 Q1 decision).

      - Real-Debrid / AllDebrid → ``BLACKHOLE_SYMLINK_TARGET_BASE`` (existing).
      - TorBox → ``BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX``.  When unset
        and the RD base is set, falls back to ``<RD base>_torbox``.
        When neither is set, returns ``''`` — callers should refuse to
        create symlinks (logged as an error elsewhere).
    """
    if debrid == TORBOX:
        explicit = os.environ.get('BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX') or ''
        if explicit:
            return explicit
        rd_base = os.environ.get('BLACKHOLE_SYMLINK_TARGET_BASE') or ''
        if rd_base:
            return rd_base.rstrip('/') + '_torbox'
        return ''
    return os.environ.get('BLACKHOLE_SYMLINK_TARGET_BASE') or ''


# ---------------------------------------------------------------------------
# Routing decision
# ---------------------------------------------------------------------------

def pick_debrid_for_grab(info_hash, *, routing_mode=None, primary=None,
                         configured=None, cache_probe=None):
    """Return the debrid service to use for a new grab.

    Args:
        info_hash: torrent info hash (uppercase or lowercase — caller
            doesn't need to normalise; the cache probe is responsible).
        routing_mode: one of ``cache_aware``/``primary_only``/``tag``/
            ``round_robin``.  Defaults to ``resolve_routing_mode()``.
        primary: the primary debrid service.  Defaults to ``resolve_primary()``.
        configured: tuple of available debrids.  Defaults to ``configured_debrids()``.
        cache_probe: optional callable ``(debrid, info_hash) -> Optional[bool]``
            used by ``cache_aware`` mode.  ``True`` = cached, ``False`` =
            confirmed uncached, ``None`` = probe unavailable / errored.
            When omitted, ``cache_aware`` degrades to ``primary_only``.

    Returns the chosen debrid service name, or ``None`` if nothing is
    configured.

    Routing semantics:

    - ``primary_only`` (single-debrid setups + explicit override):
      always returns ``primary``.
    - ``cache_aware`` (default with two debrids): probe both for cache.
      One cached → that one wins.  Both cached → primary.  Neither
      cached or probes unavailable → primary.  This is best-effort —
      caller's existing ``BLACKHOLE_REQUIRE_CACHED`` gate decides
      whether the grab proceeds for the uncached case.
    - ``tag`` and ``round_robin`` are reserved for future phases;
      today they fall through to ``primary_only``.
    """
    routing_mode = routing_mode or resolve_routing_mode()
    primary = primary or resolve_primary()
    configured = configured if configured is not None else configured_debrids()

    if not configured:
        return None
    if len(configured) == 1:
        return configured[0]
    if not primary:
        primary = configured[0]

    if routing_mode == ROUTING_PRIMARY_ONLY:
        return primary

    if routing_mode == ROUTING_CACHE_AWARE:
        if cache_probe is None or not info_hash:
            return primary
        cache_results = {}
        for svc in configured:
            try:
                cache_results[svc] = cache_probe(svc, info_hash)
            except Exception as e:
                # Probe failure is treated as "unknown", never as
                # "uncached" — invariant inherited from search.py I4.
                logger.debug(
                    f"[routing] cache probe for {svc} failed: "
                    f"{type(e).__name__} — treating as unknown"
                )
                cache_results[svc] = None
        cached = [svc for svc, ok in cache_results.items() if ok is True]
        if len(cached) == 1:
            return cached[0]
        if primary in cached:
            return primary
        if cached:
            return cached[0]
        return primary

    # Future modes — degrade safely to primary
    logger.debug(
        f"[routing] mode {routing_mode!r} not yet implemented — "
        f"using primary ({primary})"
    )
    return primary


# ---------------------------------------------------------------------------
# Add-time error classification (plan 41 phase A)
# ---------------------------------------------------------------------------
#
# Three sites need to ask "is this debrid response a filter block?" — the
# blackhole alt-release fallback (``blackhole._is_debrid_rejection``), the
# sweep-driven cross-rescue gate (``debrid_health.is_filter_block``), and
# the new add-time rescue (this module).  Centralising the classification
# keeps the three sites in lockstep when a new error code/keyword surfaces
# and gives plan-40's ``DebridProvider.classify_error()`` a single seed
# function to absorb.

# RD's documented refusal-for-this-hash error codes.  35 = infringing_file
# (the May-2026 keyword filter), 30 = torrent_file_invalid (the file is
# rejected outright — bad bencode / unknown extension / disabled file
# type).  Both signal "try another release," but only 35/infringing_file
# is a true filter-block that cross-rescue can fix; invalid_torrent is
# permanent for that hash regardless of debrid.
_FILTER_BLOCK_CODES = {35}
_FILTER_BLOCK_KEYWORDS = {'infringing_file'}
_INVALID_TORRENT_CODES = {30}
_INVALID_TORRENT_KEYWORDS = {'torrent_file_invalid'}

# Precompiled regex for ``"error_code"`` extraction.  Anchored so the
# match terminates at a non-digit — naive substring matching (e.g.
# ``'"error_code": 35' in rt``) would false-match ``error_code: 350``
# or ``error_code: 35099`` as code 35 (filter_block), triggering a
# spurious cross-rescue + ``filter_blocked_everywhere`` blocklist
# annotation when RD adds a new code in the 35X range.
_ERROR_CODE_RE = re.compile(r'"error_code"\s*:\s*(\d+)\b')


def classify_add_failure(result_text):
    """Classify a debrid add-magnet/add-torrent failure response.

    Returns one of:
      - ``'filter_block'`` — the debrid recognises the hash but refuses
        to unrestrict (RD's May-2026 keyword filter).  Cross-rescue to an
        alt debrid may recover the content.
      - ``'invalid_torrent'`` — the debrid refuses the file outright
        (corruption, disallowed file type).  Alt-release search is the
        right next step; cross-rescue would just hit the same wall.
      - ``None`` — not a recognised rejection.  Caller should treat as
        a generic add failure (network, auth, rate-limit, etc.).

    Accepts any string-shaped error payload.  Matching is case-insensitive
    keyword search + JSON ``error_code`` extraction so both RD's raw JSON
    (``{"error":"infringing_file","error_code":35}``) and a pre-flattened
    error message survive.  Non-string inputs return ``None``.
    """
    if not isinstance(result_text, str):
        return None
    rt = result_text.lower()
    # Keyword first — cheapest, also catches non-JSON formatted errors.
    if any(kw in rt for kw in _FILTER_BLOCK_KEYWORDS):
        return 'filter_block'
    if any(kw in rt for kw in _INVALID_TORRENT_KEYWORDS):
        return 'invalid_torrent'
    # Numeric error_code fallback — covers cases where RD changes the
    # human-readable string but keeps the numeric code stable.  The
    # regex is digit-boundary-anchored (see ``_ERROR_CODE_RE`` docstring)
    # so a future code like 350 doesn't false-match 35.
    for m in _ERROR_CODE_RE.finditer(rt):
        code = int(m.group(1))
        if code in _FILTER_BLOCK_CODES:
            return 'filter_block'
        if code in _INVALID_TORRENT_CODES:
            return 'invalid_torrent'
    return None


def is_debrid_rejection(result_text):
    """Backward-compat shim for the old ``_is_debrid_rejection`` predicate.

    Returns ``True`` when the response represents either a filter_block
    or an invalid_torrent — the original predicate's union semantics.
    Prefer ``classify_add_failure`` in new code so the caller can branch
    on which kind of rejection it is.
    """
    return classify_add_failure(result_text) is not None


# ---------------------------------------------------------------------------
# TorBox mount-lookup candidate builder (plan 41 phase B.3)
# ---------------------------------------------------------------------------

# Pre-compiled regex shared by ``strip_indexer_prefix`` and any future
# consumer needing the same "drop leading ``[indexer.to]`` block" rule.
# Originally lived in ``utils.blackhole`` and was re-imported here as a
# private name; moved during the phase-B reviewer fix-up so the cross-
# module dependency is on a public API, not an underscore-prefixed
# helper that could silently rename out from under us.
_INDEXER_PREFIX_RE = re.compile(r'^\[[^\]]+\]\s*')


def strip_indexer_prefix(name):
    """Strip a leading ``[indexer.to] `` block from a release name.

    Returns the stripped name, or *name* unchanged if no leading
    bracket-block was present.  Used by:

      - ``BlackholeWatcher._find_on_torbox_mount`` — drops the indexer
        tag the scraper adds to TB's ``data.name`` so the mount-search
        fuzzy match can hit the bare folder name TB stores.
      - ``build_tb_lookup_candidates`` (below) — same purpose, building
        the candidate list before ``_find_on_torbox_mount`` runs.
    """
    if not name:
        return name
    return _INDEXER_PREFIX_RE.sub('', name, count=1)


def build_tb_lookup_candidates(release_name, file_names=None):
    """Return an ordered list of folder-name candidates to probe on the
    TorBox WebDAV mount.

    TB's API ``data.name`` field returns the indexer's *display* title.
    For non-English trackers (Russian/Italian/etc) that title is in the
    native language, while the actual folder TB writes to its WebDAV
    layout uses the ``.torrent``'s ``info.name`` (typically the English
    release-group form).  The API name → WebDAV folder mismatch caused
    every grab from non-English trackers to time out on the 300s
    mount-poll before this fix.

    Concrete case observed 2026-05-25 (For All Mankind S03 from a
    Russian tracker):
        API name: ``Ради всего человечества  For All Mankind  Сезон 3 ...``
        TB folder: ``For.All.Mankind.S03.1080p.ATVP.WEB-DL.DDP5.1.H.264-EniaHD``

    Candidate order (deduped, original order preserved):
      1. ``release_name`` (API name) — common case, exact match.
      2. ``release_name`` with media extension stripped (single-file
         releases where the API echoes the file name).
      3. ``release_name`` with leading ``[indexer.to]`` prefix stripped.
      4. (3) with media extension stripped.
      5. First path segment of each entry in ``file_names``.  TB's
         ``data.files[].name`` is the actual on-WebDAV path, so the
         segment before the first ``/`` is the folder name TB used.
         This is the candidate that bridges the language gap.

    ``file_names`` may be ``None`` (no TB API data available — caller
    didn't extract); the function then returns only the API-derived
    candidates.
    """
    # Keep lazy: utils.blackhole ↔ utils.debrid_routing form a cycle if
    # either imports the other at module level.  MEDIA_EXTENSIONS is
    # duplicated across utils.{library,blackhole,scheduled_tasks} (the
    # 3-file rule per CLAUDE.md) so we import from blackhole here for
    # consistency — the value must match.  ``strip_indexer_prefix``
    # lives in this module (moved during phase-B reviewer fix-up to
    # eliminate the cross-module private-name dependency).
    from utils.blackhole import MEDIA_EXTENSIONS

    if not release_name:
        return []

    candidates = []
    seen = set()

    def _add(name):
        if name and name not in seen:
            candidates.append(name)
            seen.add(name)

    _add(release_name)
    base, ext = os.path.splitext(release_name)
    if ext.lower() in MEDIA_EXTENSIONS and base:
        _add(base)

    stripped = strip_indexer_prefix(release_name)
    if stripped and stripped != release_name:
        _add(stripped)
        s_base, s_ext = os.path.splitext(stripped)
        if s_ext.lower() in MEDIA_EXTENSIONS and s_base:
            _add(s_base)

    # Files-derived candidates — only fired when ``file_names`` is
    # populated.  Each path may use either '/' (POSIX, TB default) or
    # '\' (Windows-uploaded torrents) as separator — handle both.
    if file_names:
        for fn in file_names:
            if not isinstance(fn, str) or not fn:
                continue
            # Take the first path segment regardless of separator.
            head = fn.split('/', 1)[0].split('\\', 1)[0]
            if head:
                _add(head)

    return candidates


def is_filter_block_reason(reason):
    """Predicate: does ``reason`` represent a filter block?

    Operates on the structured ``reason`` field returned by
    ``debrid_client.probe_file()`` (used by the sweep-driven
    cross-rescue gate in ``debrid_health.py``).  Distinct surface from
    ``classify_add_failure`` (which parses a raw add-response payload)
    but shares the same ``_FILTER_BLOCK_KEYWORDS`` vocabulary so a future
    RD return-string change updates both sites at once.
    """
    return (reason or '').lower() in _FILTER_BLOCK_KEYWORDS


# ---------------------------------------------------------------------------
# Cross-debrid rescue core (plan 41 phase A)
# ---------------------------------------------------------------------------
#
# Two sites need to "add a hash to the alt debrid and wait until it's
# ready": the sweep-driven cross-rescue in ``debrid_health`` (post-block
# rescue with symlink retarget) and the add-time rescue in
# ``blackhole._process_file`` (RD's add returned filter_block; try TB
# before giving up).  The shared core is cache-probe → add → wait-ready;
# the post-ready actions differ per caller (retarget existing symlinks
# vs. start a new monitor entry).
#
# The add primitive and ready-state vocabulary are dependency-injected
# to avoid an import cycle with ``utils.blackhole`` (which already
# imports from this module).

_DEFAULT_RESCUE_READY_TIMEOUT = 60
_DEFAULT_RESCUE_POLL_INTERVAL = 3

# Clock-skew headroom for the pre-existing add guard.  A provider's
# ``added``/``created_at`` timestamp is server-side; our ``probe_start``
# is local.  This grace absorbs the skew between the two clocks so a
# freshly-created probe entry (server time a beat behind our local start)
# isn't misread as pre-existing.  Kept small: too large a window lets a
# torrent the user added seconds before our probe slip through as "ours"
# and be deleted.  30s comfortably covers real NTP-synced skew.
_PREEXISTING_GRACE_SECONDS = 30


def make_preexisting_check(probe_start, grace=_PREEXISTING_GRACE_SECONDS,
                           timestamp_fields=('added', 'created_at')):
    """Build a ``preexisting_check`` for :func:`attempt_add_rescue`.

    A provider's add endpoint dedups by hash: adding a magnet the account
    already holds returns the PRE-EXISTING torrent's id (RD and TB both do
    this).  A rescue that later deletes that id on failure would destroy
    the user's own content.  The returned closure fetches the alt entry's
    creation timestamp (``added`` for RD, ``created_at`` for TB) and
    reports pre-existing when it predates our probe (minus ``grace`` for
    clock skew).

    Conservative on every uncertainty — no ``torrent_info`` method, a
    non-dict response, a missing/unparseable timestamp, or a naive
    (tz-less) datetime all return ``True``.  An orphaned probe entry is a
    tolerable leak; deleting the user's torrent is not.
    """
    def _check(client, tid):
        info_fn = getattr(client, 'torrent_info', None)
        if not callable(info_fn):
            return True
        info = info_fn(tid)
        if not isinstance(info, dict):
            return True
        raw = ''
        for field in timestamp_fields:
            v = info.get(field)
            if isinstance(v, str) and v.strip():
                raw = v.strip()
                break
        if not raw:
            return True
        try:
            dt = datetime.fromisoformat(raw.replace('Z', '+00:00'))
        except (ValueError, TypeError):
            return True
        # A naive timestamp has no anchor to compare against a UTC epoch —
        # treat as unknown rather than guessing the zone.
        if dt.tzinfo is None:
            return True
        return dt.timestamp() < probe_start - grace

    return _check


def pick_alt_debrid(source_debrid, configured=None):
    """Return the cross-rescue target for a torrent currently on ``source_debrid``.

    Today's only supported direction is RD ⇄ TB; future combinations
    (AD ⇄ TB, AD ⇄ RD) land here as explicit additions rather than
    silent fallthroughs.  Returns ``None`` when no valid alt is
    configured.
    """
    if configured is None:
        configured = configured_debrids()
    if source_debrid not in configured:
        return None
    # RD and TB rescue each other; AD has no cross-rescue partner yet.
    if source_debrid == REALDEBRID and TORBOX in configured:
        return TORBOX
    if source_debrid == TORBOX and REALDEBRID in configured:
        return REALDEBRID
    return None


def attempt_add_rescue(info_hash, source_debrid, *,
                       alt_debrid=None,
                       cache_probe=None,
                       alt_client=None,
                       alt_add_fn=None,
                       status_fn=None,
                       ready_states=None,
                       fail_states=None,
                       stop_event=None,
                       ready_timeout=None,
                       poll_interval=None,
                       preexisting_check=None,
                       logger_prefix='rescue'):
    """Probe alt-debrid cache, add the hash, and wait for a ready state.

    Shared core between the sweep-driven cross-rescue (post-block,
    debrid_health phase 3) and the add-time rescue (blackhole phase A
    of plan 41).  Caller is responsible for any post-ready action
    (symlink retargeting, monitor start, etc.).

    Arguments:

      info_hash: torrent info hash (case-insensitive; cache probe is
        responsible for normalisation).
      source_debrid: the provider the hash came from.  Used by
        ``pick_alt_debrid`` to select the cross-rescue target when
        ``alt_debrid`` is not specified.
      alt_debrid: override the auto-picked alt.  Default = ``pick_alt_debrid(source)``.
      cache_probe: callable ``(debrid, info_hash) -> Optional[bool]``
        for checking whether the alt has the hash cached.  Defaults to
        ``utils.search.check_debrid_cache``.  Rescue is a HIT-CACHED
        operation; uncached → fail with ``not_cached_on_alt``.
      alt_client: pre-resolved alt debrid client (matches the type
        produced by ``utils.debrid_client.get_debrid_client``).  When
        ``None``, the helper resolves one.  Pass in when the caller
        already has a configured client to save a redundant lookup.
      alt_add_fn: callable ``(alt_client, info_hash) -> Optional[str]``
        performing the add and returning the alt-side torrent id.
        Defaults to ``alt_client.add_magnet(info_hash)``.  Blackhole
        callers pass a closure that calls ``_add_to_torbox`` against an
        existing watch-dir file so ``.torrent`` files (not just magnets)
        are handled.
      status_fn: callable ``(alt_client, alt_torrent_id) -> str`` returning
        the torrent's current status string.  Defaults to
        ``alt_client.torrent_status(alt_torrent_id)``.
      ready_states: iterable of lowercase status strings that count as
        "ready" (e.g. ``{'cached', 'completed', 'uploading'}`` for TB).
        REQUIRED — no safe default since vocabularies differ per provider.
      fail_states: optional iterable of lowercase status strings that are
        terminal failures (e.g. ``{'magnet_error', 'error', 'virus',
        'dead'}`` for RD).  Observing one short-circuits the poll loop —
        the alt entry is deleted and the result is
        ``reason='failed_state'`` (with the observed state in ``state``)
        instead of burning the full ``ready_timeout`` on a dead magnet.
        Default ``None`` — no short-circuit, existing callers unchanged.
      stop_event: optional ``threading.Event``.  When set during the
        poll loop, the helper aborts and cleans up the alt entry.
      ready_timeout / poll_interval: override the module defaults
        (60s / 3s).  Per-caller tests monkeypatch these to keep
        tests fast.
      preexisting_check: optional callable ``(alt_client, alt_torrent_id)
        -> bool`` consulted before any cleanup delete.  Providers with
        add-time hash-dedup (RD's ``addMagnet`` returns the
        PRE-EXISTING torrent's id when the hash is already on the
        account) can hand back a torrent the USER owns — deleting it on
        ``never_ready``/``stop_requested`` would destroy their
        in-flight download.  Return ``True`` to skip the delete; the
        failure result then carries ``'preexisting': True``.  If the
        check itself raises, the entry is treated as pre-existing —
        an orphaned probe entry beats destroying user data.
      logger_prefix: tag prepended to all log lines.  Defaults to
        ``'rescue'``; callers should pass a more specific prefix like
        ``'[debrid_health] rescue'`` or ``'[blackhole] rescue'``.

    Returns a dict.  On success:

        {'rescued': True, 'to': <alt>, 'alt_torrent_id': <id>, 'alt_client': obj}

    On failure:

        {'rescued': False, 'reason': <slug>, 'alt_torrent_id': <id>|None}

    Failure ``reason`` slugs:
      - ``no_alt_configured``: no alt debrid is available for rescue.
      - ``unsupported_source``: source_debrid not in the rescue graph.
      - ``cache_probe_error``: cache lookup raised.
      - ``not_cached_on_alt``: alt does not have the hash cached.
      - ``no_alt_client``: alt client couldn't be constructed.
      - ``add_error``: alt's add raised (network, auth, malformed).
      - ``add_failed``: alt's add returned an empty torrent id.
      - ``stop_requested``: stop_event fired mid-rescue; alt entry cleaned.
      - ``never_ready``: alt accepted the add but didn't reach a ready
        state within ``ready_timeout``; alt entry cleaned.
      - ``failed_state``: the torrent hit a state listed in
        ``fail_states``; alt entry cleaned, observed state in ``state``.
      - ``misconfigured``: ``ready_states`` was omitted (developer error,
        not a runtime condition — see argument docs above).

    Never raises.  All alt-side allocations are best-effort cleaned up
    on failure (callers see an ``alt_torrent_id`` key only when cleanup
    may have failed — useful for follow-up debugging).
    """
    import time

    if ready_states is None:
        return {'rescued': False, 'reason': 'misconfigured',
                'alt_torrent_id': None,
                'detail': 'ready_states required'}
    ready_states = {s.lower() for s in ready_states}
    fail_states = {s.lower() for s in fail_states} if fail_states else set()
    ready_timeout = ready_timeout if ready_timeout is not None else _DEFAULT_RESCUE_READY_TIMEOUT
    poll_interval = poll_interval if poll_interval is not None else _DEFAULT_RESCUE_POLL_INTERVAL

    # 1. Resolve alt debrid
    alt = alt_debrid or pick_alt_debrid(source_debrid)
    if not alt:
        # Distinguish "source isn't in our rescue graph" (config bug)
        # from "user only has one debrid configured" (expected for
        # single-debrid setups).
        configured = configured_debrids()
        if source_debrid not in configured:
            return {'rescued': False, 'reason': 'unsupported_source',
                    'alt_torrent_id': None}
        return {'rescued': False, 'reason': 'no_alt_configured',
                'alt_torrent_id': None}

    # 2. Cache probe on alt
    if cache_probe is None:
        # Lazy-import the default probe so unit tests can stub it via
        # ``cache_probe=`` without monkeypatching ``utils.search``.
        try:
            from utils.search import check_debrid_cache
        except Exception as e:
            logger.warning(
                f"[{logger_prefix}] cache probe import failed: {type(e).__name__}"
            )
            return {'rescued': False, 'reason': 'cache_probe_error',
                    'alt_torrent_id': None}

        alt_key = os.environ.get(f'{alt.upper()}_API_KEY') if alt != REALDEBRID else os.environ.get('RD_API_KEY')

        def _default_probe(svc, h):
            cache_map = check_debrid_cache([h.lower()], service=svc, api_key=alt_key)
            return cache_map.get(h.lower())

        cache_probe = _default_probe

    try:
        cached = cache_probe(alt, info_hash)
    except Exception as e:
        logger.warning(
            f"[{logger_prefix}] cache probe failed for {info_hash[:8]}…: "
            f"{type(e).__name__}"
        )
        return {'rescued': False, 'reason': 'cache_probe_error',
                'alt_torrent_id': None}

    if cached is not True:
        return {'rescued': False, 'reason': 'not_cached_on_alt',
                'alt_torrent_id': None}

    # 3. Resolve alt client (caller may have pre-resolved)
    if alt_client is None:
        try:
            from utils.debrid_client import get_debrid_client
        except Exception as e:
            logger.warning(
                f"[{logger_prefix}] debrid_client import failed: {type(e).__name__}"
            )
            return {'rescued': False, 'reason': 'no_alt_client',
                    'alt_torrent_id': None}
        alt_key = os.environ.get(f'{alt.upper()}_API_KEY') if alt != REALDEBRID else os.environ.get('RD_API_KEY')
        try:
            alt_client, _svc = get_debrid_client(service=alt, api_key=alt_key)
        except Exception as e:
            logger.warning(
                f"[{logger_prefix}] get_debrid_client failed: {type(e).__name__}"
            )
            return {'rescued': False, 'reason': 'no_alt_client',
                    'alt_torrent_id': None}
        if alt_client is None or not getattr(alt_client, 'configured', False):
            return {'rescued': False, 'reason': 'no_alt_client',
                    'alt_torrent_id': None}

    # 4. Add to alt
    if alt_add_fn is None:
        def alt_add_fn(client, h):
            return client.add_magnet(h)

    try:
        alt_tid = alt_add_fn(alt_client, info_hash)
    except Exception as e:
        logger.warning(
            f"[{logger_prefix}] alt add failed for {info_hash[:8]}…: "
            f"{type(e).__name__}"
        )
        return {'rescued': False, 'reason': 'add_error',
                'http_status': getattr(alt_client, 'last_add_status', None),
                'alt_torrent_id': None}
    if not alt_tid:
        # Surface the add's HTTP status (if the client recorded one) so
        # callers can tell a permanent filter block (403/451) from a
        # transient failure without reaching into client internals.
        return {'rescued': False, 'reason': 'add_failed',
                'http_status': getattr(alt_client, 'last_add_status', None),
                'alt_torrent_id': None}
    alt_tid = str(alt_tid)

    def _is_preexisting():
        if preexisting_check is None:
            return False
        try:
            preexisting = bool(preexisting_check(alt_client, alt_tid))
        except Exception as e:
            logger.warning(
                f"[{logger_prefix}] preexisting check failed for "
                f"{info_hash[:8]}…: {type(e).__name__} — treating alt "
                f"entry {alt_tid} as pre-existing (skipping delete)"
            )
            return True
        if preexisting:
            logger.warning(
                f"[{logger_prefix}] alt entry {alt_tid} for "
                f"{info_hash[:8]}… predates this add (provider hash-dedup "
                f"returned an existing torrent) — skipping cleanup delete"
            )
        return preexisting

    # 5. Stop-event short-circuit between add and poll loop
    if stop_event is not None and stop_event.is_set():
        preexisting = _is_preexisting()
        if not preexisting:
            try:
                alt_client.delete_torrent(alt_tid)
            except Exception:
                pass
        return {'rescued': False, 'reason': 'stop_requested',
                'alt_torrent_id': alt_tid, 'preexisting': preexisting}

    # 6. Poll for ready
    if status_fn is None:
        def status_fn(client, tid):
            return client.torrent_status(tid)

    deadline = time.time() + ready_timeout
    is_ready = False
    failed_state = None
    while time.time() < deadline:
        if stop_event is not None and stop_event.is_set():
            break
        try:
            state_str = status_fn(alt_client, alt_tid)
        except Exception:
            state_str = ''
        state_norm = (state_str or '').strip().lower()
        if state_norm in ready_states:
            is_ready = True
            break
        if state_norm in fail_states:
            failed_state = state_norm
            break
        # Use stop_event.wait() instead of time.sleep so SIGTERM aborts
        # within one poll interval rather than stalling on a 3s sleep.
        if stop_event is not None:
            if stop_event.wait(poll_interval):
                break
        else:
            time.sleep(poll_interval)

    if not is_ready:
        preexisting = _is_preexisting()
        if not preexisting:
            try:
                alt_client.delete_torrent(alt_tid)
            except Exception:
                pass
        if failed_state:
            return {'rescued': False, 'reason': 'failed_state',
                    'alt_torrent_id': alt_tid, 'state': failed_state,
                    'preexisting': preexisting}
        # Honour stop-event observed mid-poll loop so the caller can
        # distinguish a SIGTERM-driven abort from a genuine timeout.
        reason = 'stop_requested' if (stop_event is not None and stop_event.is_set()) else 'never_ready'
        return {'rescued': False, 'reason': reason,
                'alt_torrent_id': alt_tid, 'preexisting': preexisting}

    return {'rescued': True, 'to': alt,
            'alt_torrent_id': alt_tid, 'alt_client': alt_client,
            'preexisting': False}
