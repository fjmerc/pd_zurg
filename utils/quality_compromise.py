"""Quality-compromise decision engine (plan 33 Phase 4).

Pure-logic helpers plus two thin I/O wrappers used by Phase 5 when the
blackhole retry loop has exhausted the arr's alternative list at the
user's preferred tier.  The arr profile remains the ceiling (invariant
I1 — callers pass the tier label they want, and the search helpers
double-check it); cache availability is enforced here under ``only_cached``
(invariant I4); monotonic downward movement (I2) is enforced upstream in
``RetryMeta.advance_tier`` — this module only returns decisions.

Real-Debrid note: ``check_debrid_cache`` / ``search_torrents`` treat RD
as ``cached=None`` uniformly (RD deprecated their instant-availability
endpoint Nov 2024).  Under ``only_cached=True`` that means RD users will
see ``find_compromise_candidate`` return ``None`` — documented in
``utils/search.py`` and surfaced to users via the Phase 3 one-shot
warning; Phase 5/7 messaging should stay consistent with that behaviour.
"""

import re

from utils.blocklist import is_blocked
from utils.search import search_torrents

_EPISODE_TOKEN_RE = re.compile(r'S\d{1,2}E\d+', re.IGNORECASE)


def should_compromise(tier_state, now, dwell_seconds, only_cached):
    """Decide whether to advance to the next tier.  Pure function, no I/O.

    Args:
        tier_state: The v2 dict returned by ``RetryMeta.read_tier_state``,
            or ``None`` for legacy sidecars.  Relied-upon keys:
            ``tier_order`` (list of resolution labels, ordered
            preferred-first), ``current_tier_index`` (int >= 0),
            ``first_attempted_at`` (Unix timestamp of the first
            preferred-tier attempt — the dwell baseline).
        now: Current Unix timestamp (caller-supplied so tests are
            deterministic).
        dwell_seconds: How long the item must have been at the preferred
            tier before compromise may fire (invariant I3).
        only_cached: Informational — accepted for signature symmetry
            with ``find_compromise_candidate``.  The cache gate itself
            fires in that helper; history/notification callers can log
            the flag alongside the returned reason.

    Returns:
        ``(action, reason)`` where ``action`` is one of ``'stay'``,
        ``'advance'``, ``'exhausted'`` and ``reason`` is a short logging
        string.  Callers should map ``'stay'`` to "keep retrying at the
        current tier", ``'advance'`` to "try to compromise one tier
        down", and ``'exhausted'`` to "no lower tier is permitted — fail
        normally".
    """
    del only_cached  # currently informational; see docstring
    if tier_state is None:
        return ('stay', 'legacy_no_tier_state')
    if not isinstance(tier_state, dict):
        return ('stay', 'invalid_tier_state')

    tier_order = tier_state.get('tier_order')
    if not isinstance(tier_order, list):
        return ('stay', 'invalid_tier_state')
    current = tier_state.get('current_tier_index', 0)
    if not isinstance(current, int) or isinstance(current, bool) or current < 0:
        return ('stay', 'invalid_tier_state')

    if current + 1 >= len(tier_order):
        return ('exhausted', 'no_lower_tier_in_profile')

    first = tier_state.get('first_attempted_at')
    if not isinstance(first, (int, float)) or isinstance(first, bool):
        return ('stay', 'invalid_tier_state')

    if (now - first) < dwell_seconds:
        return ('stay', 'dwell_not_elapsed')

    return ('advance', 'dwell_elapsed')


def _filter_candidates(results, tier_label, min_seeders, only_cached):
    """Shared filter chain for compromise + season-pack candidate lists."""
    # I1: a missing tier_label is a caller bug — refuse to filter against
    # None, because a release whose quality parser failed also carries
    # label=None and would compare equal, turning the ceiling into a no-op.
    if not tier_label:
        return []
    out = []
    for r in results or []:
        quality = r.get('quality') or {}
        # I1: double-check tier label even though the caller passed it —
        # a search result whose resolution doesn't match the requested
        # tier is ineligible no matter how well everything else scores.
        if quality.get('label') != tier_label:
            continue
        # Defence-in-depth: search_torrents() already filters blocklisted
        # hashes, but _filter_candidates is the shared entry point for any
        # callers that supply result lists from other sources.  is_blocked
        # internally uppercases the hash, so case normalisation is not our
        # concern here.
        info_hash = r.get('info_hash') or ''
        if info_hash and is_blocked(info_hash):
            continue
        if (r.get('seeds') or 0) < min_seeders:
            continue
        # I4: cached=None (unknown — e.g. Real-Debrid post-Nov-2024) is
        # treated as "not cached" so RD users under only_cached=True
        # never end up with a compromise grab that might not stream.
        if only_cached and r.get('cached') is not True:
            continue
        out.append(r)
    return out


def _rank_within_tier(candidates):
    """Sort a same-tier candidate list and return the best.

    Torrentio results don't carry Sonarr/Radarr's ``customFormatScore``
    (that score only exists arr-side, computed from the user's custom
    formats), so ``_force_grab_sort_key``-style scoring isn't available
    here.  Fall back to ``(seeds desc, size_bytes asc)``: seeders rewards
    availability (a cached release with 0 seeds may evaporate from
    debrid caches) and smaller size rewards debrid storage economy —
    preferring a well-seeded 8 GB cached release over a 60 GB REMUX
    that happens to match the same tier label.
    """
    if not candidates:
        return None
    return sorted(
        candidates,
        key=lambda r: (-(r.get('seeds') or 0), r.get('size_bytes') or 0),
    )[0]


def find_compromise_candidate(arr_client, imdb_id, tier_label, min_seeders,
                              only_cached, context):
    """Return the best cached release at ``tier_label``, or ``None``.

    Args:
        arr_client: Currently unused — retained for signature symmetry
            with ``find_season_pack_candidate`` and to give Phase 5 a
            single call shape across both helpers.
        imdb_id: IMDb ID to search against (e.g. ``'tt1234567'``).
        tier_label: Required quality label, e.g. ``'1080p'``.  Releases
            at other tiers are rejected even if they otherwise match.
        min_seeders: Minimum seeder floor for candidate eligibility.
        only_cached: If True, only ``cached is True`` releases survive
            (``cached=None`` is treated as not cached — invariant I4).
        context: Dict carrying ``media_type`` (``'movie'`` or
            ``'series'``), ``season``, ``episode`` — forwarded to
            ``search_torrents`` so the same function works for movies
            and single-episode TV searches.
    """
    del arr_client  # accepted for signature symmetry; no arr I/O needed here
    ctx = context or {}
    results = search_torrents(
        imdb_id,
        media_type=ctx.get('media_type', 'movie'),
        season=ctx.get('season'),
        episode=ctx.get('episode'),
        annotate_cache=True,
        sort_mode='cached_first',
    )
    candidates = _filter_candidates(results, tier_label, min_seeders, only_cached)
    return _rank_within_tier(candidates)


def _looks_like_single_season_pack(title, season_number):
    """True iff *title* names the target season without an episode token.

    Complements ``_is_multi_season_pack`` for the single-season case —
    e.g. ``Show.S03.1080p.BluRay-GROUP`` is a pack even though the
    multi-season regex doesn't fire on it.  Requires no
    ``S\\d+E\\d+`` token anywhere in the title so we don't mis-flag a
    single-episode release.
    """
    if not title:
        return False
    if _EPISODE_TOKEN_RE.search(title):
        return False
    return bool(re.search(rf'S{season_number:02d}(?!\d)', title, re.IGNORECASE))


def find_season_pack_candidate(arr_client, series_id, season_number,
                               tier_label, min_missing, min_seeders,
                               only_cached):
    """Return the best cached season pack at ``tier_label`` for *series_id*, or ``None``.

    TV-only.  Preflight: the series must have at least ``min_missing``
    episodes in *season_number* with ``hasFile=False`` — the pack
    strategy only pays off when enough individual searches have failed
    to justify grabbing the whole season.

    ``season_number`` is coerced to ``int`` so callers handing a string
    through from ``pending_monitors.json`` or a URL query param don't
    crash the decision loop when it reaches the ``:02d`` format spec.

    Detection: series-scoped Torrentio probe (``/stream/series/<imdb>``),
    then keep releases that either match ``_is_multi_season_pack`` (with
    the resulting range covering *season_number*) or name the single
    target season via ``S{season:02d}`` without an ``SxxEyy`` episode
    token.  Same tier-label + blocklist + seeders + cache filters as
    ``find_compromise_candidate`` via the shared helper.
    """
    try:
        season_number = int(season_number)
    except (TypeError, ValueError):
        return None

    episodes = arr_client.get_episodes(series_id) or []
    missing = 0
    for ep in episodes:
        if not isinstance(ep, dict):
            continue
        if ep.get('seasonNumber') != season_number:
            continue
        if not ep.get('hasFile'):
            missing += 1
    if missing < min_missing:
        return None

    series = arr_client.get_series(series_id) or {}
    imdb_id = series.get('imdbId')
    if not imdb_id:
        return None

    # Lazy import: Phase 5 wires this module into blackhole.py, which
    # would otherwise import us before its module body finishes loading.
    from utils.blackhole import _is_multi_season_pack

    results = search_torrents(
        imdb_id,
        media_type='series',
        annotate_cache=True,
        sort_mode='cached_first',
    )

    packs = []
    for r in results or []:
        title = r.get('title') or ''
        is_multi, s_start, s_end = _is_multi_season_pack(title)
        if is_multi:
            # Complete-collection matches (s_start=s_end=None) assume
            # coverage — the regex can't tell us the range from the name.
            if s_start is None or s_start <= season_number <= s_end:
                packs.append(r)
                continue
        if _looks_like_single_season_pack(title, season_number):
            packs.append(r)

    candidates = _filter_candidates(packs, tier_label, min_seeders, only_cached)
    return _rank_within_tier(candidates)
