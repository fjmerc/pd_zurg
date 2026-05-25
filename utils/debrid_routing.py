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
