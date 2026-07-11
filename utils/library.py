"""Library scanner for debrid (rclone mount) and local media content.

Walks Zurg mount categories and local library directories to build a
unified item list, cross-referencing by title to detect content present
in both sources.
"""

import copy
import os
import re
import shutil
import threading
import unicodedata
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote as urllib_quote
from utils.logger import get_logger
from utils.quality_parser import parse_quality

logger = get_logger()

try:
    from utils import history as _history
except ImportError:
    _history = None

try:
    from utils import blocklist as _blocklist
except ImportError:
    _blocklist = None

MEDIA_EXTENSIONS = {'.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.ts', '.m4v', '.webm'}

# Wanted-recovery terminal give-up: after this many recovery passes where a
# title's top release is confirmed RD-filter-blocked AND TorBox-uncached in
# the SAME pass, stop probing it entirely (persisted in the attempt ledger as
# ``wantedblock:<imdb>``).  Without this a title doomed on both providers is
# re-probed on the 7-day RD-miss timer forever.  Read by the /api/stuck
# collector too, so it stays a single source of truth.
WANTED_FILTER_GIVEUP_STRIKES = 3


def gap_fill_enabled():
    """Return ``True`` when the unconditional episode-completeness reconcile is
    enabled.  Single source of truth for the ``GAP_FILL_ENABLED`` env var so
    ``library.py`` and ``scheduled_tasks.py`` can't drift on parsing rules.
    Default ``true`` — opt-out, not opt-in.
    """
    return os.environ.get('GAP_FILL_ENABLED', 'true').strip().lower() == 'true'


def wanted_tb_recovery_enabled():
    """Return ``True`` when the Wanted→TorBox recovery pass is enabled.

    Default ``true`` — opt-out.  This pass proactively grabs cached TorBox
    copies of "Wanted" library ghosts (monitored, no file) that the arr's
    own indexer search never managed to grab.  The arr searches its
    Prowlarr/Torznab pool, a different population than the Torrentio feed
    zurgarr queries directly, so cached content can sit in Wanted forever.
    """
    return os.environ.get('WANTED_TB_RECOVERY_ENABLED', 'true').strip().lower() == 'true'


def wanted_tb_recovery_max_per_scan():
    """Per-scan cap on Wanted→TorBox recovery adds.  Default 2.

    Bounds TorBox create-API usage (60/hr limit) — but the binding
    constraint in practice is TB Essential's abuse system, which arms a
    ~24h account cooldown on create-volume *bursts* (observed live: a
    5-creates-in-one-minute burst armed it minutes later, capping
    throughput at ~5/day).  A trickle of 2 per scan (~11 min apart) works
    the same backlog at up to ~250/day without presenting as a burst.
    Non-integer/<=0 values fall back to the default rather than disabling
    the pass silently.
    """
    raw = os.environ.get('WANTED_TB_RECOVERY_MAX_PER_SCAN', '2')
    try:
        n = int(str(raw).strip())
    except (TypeError, ValueError):
        return 2
    return n if n > 0 else 2


def wanted_rd_recovery_enabled():
    """Return ``True`` when the RD leg of Wanted recovery is enabled.

    Default ``true`` — opt-out.  RD's cache-query endpoint is dead
    (deprecated Nov 2024), so the RD leg probes by adding: magnet add →
    instantly ready means cached (keep it), anything else means uncached
    (delete the probe add and fall back to the TorBox trickle).  RD has
    no create-volume cooldown, so whenever RD has the content cached this
    leg drains the Wanted backlog far faster than the TB trickle.
    """
    return os.environ.get('WANTED_RD_RECOVERY_ENABLED', 'true').strip().lower() == 'true'


def wanted_rd_recovery_max_per_scan():
    """Per-scan cap on RD probe-adds.  Default 4.

    Counts ATTEMPTS (adds), not successes — the add itself is the
    expensive unit here (addMagnet + selectFiles + status polls + a
    delete on miss).  RD has no create-volume abuse cooldown, so this
    can safely sit higher than the TorBox trickle cap; the binding
    constraint is the pass's own time budget (each uncached attempt
    burns up to ``_WANTED_RD_READY_TIMEOUT`` seconds of polling).
    Non-integer/<=0 values fall back to the default rather than
    disabling the leg silently.
    """
    raw = os.environ.get('WANTED_RD_RECOVERY_MAX_PER_SCAN', '4')
    try:
        n = int(str(raw).strip())
    except (TypeError, ValueError):
        return 4
    return n if n > 0 else 4


def wanted_season_recovery_enabled():
    """Return ``True`` when Wanted recovery may also probe season packs for
    partially-present shows (not just fully-absent ghosts).

    Default ``true`` — opt-out.  Rides the TB leg only (shares its budget
    and requires ``WANTED_TB_RECOVERY_ENABLED``); a single cached
    season-pack add fills every gap in that season via the symlink phase,
    which is how scattered per-episode holes the arr's indexers never
    close actually drain.
    """
    return os.environ.get('WANTED_SEASON_RECOVERY_ENABLED', 'true').strip().lower() == 'true'


# Plan 41 phase B.2 — NFS attribute-cache delay between symlink creation
# and arr rescan trigger.  See ``_create_debrid_symlinks`` for the
# narrative.  Lifted to a module-level helper so it can be unit-tested
# in isolation without exercising the entire scanner pipeline.
_NFS_RESCAN_DELAY_MAX = 300


def _resolve_nfs_rescan_delay():
    """Return the configured rescan delay in seconds, clamped to ``[0, 300]``.

    Empty/unset env yields 0.  Non-integer values yield 0 (best-effort —
    a typo shouldn't crash the scanner; it just disables the mitigation).
    Values >300 are clamped — a 5-minute ceiling caps user mistakes
    without ever blocking the scan loop indefinitely.  Negative values
    are clamped to 0.
    """
    raw = os.environ.get('LIBRARY_RESCAN_NFS_DELAY', '0') or '0'
    try:
        delay = int(raw)
    except (TypeError, ValueError):
        return 0
    return max(0, min(delay, _NFS_RESCAN_DELAY_MAX))

# Folders to skip during library scans (non-media content)
_SKIP_FOLDERS = {
    'plex versions', 'subs', 'subtitles', 'featurettes',
    'behind the scenes', 'behind-the-scenes', 'deleted scenes',
    'interviews', 'scenes', 'trailers', 'sample', 'samples',
    '.actors', 'bonus', 'bonuses',
    '.recycle', '@eadir', '@recently-snapshot',
}


def _all_debrid_symlink_prefixes():
    """Return all configured per-debrid symlink-target prefixes (each with
    trailing ``os.sep`` so ``startswith`` matches whole path components only).

    Plan 39 introduced per-debrid target bases — TorBox content lives under
    ``BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX`` (or auto-derived ``<RD>_torbox``).
    Local scanners that only checked the RD base would silently misclassify
    TB-routed folders as local content, dropping them into the wrong
    movies/shows bucket and rendering show episodes in the movies UI.

    Iterates ``VALID_DEBRIDS`` rather than hard-coding TB so future providers
    (AD's pending per-base env var, Premiumize) auto-extend without touching
    this helper.  Paths are normalised via ``os.path.normpath`` so
    consecutive separators (``/mnt//debrid``) and relative configs collapse
    to canonical form before the prefix is built.  Empty bases are dropped;
    the result is deduped and order-preserved.
    """
    bases = []
    rd = (os.environ.get('BLACKHOLE_SYMLINK_TARGET_BASE') or '').strip()
    if rd:
        bases.append(rd)
    try:
        from utils.debrid_routing import VALID_DEBRIDS, symlink_target_base_for_debrid
        for svc in VALID_DEBRIDS:
            try:
                b = (symlink_target_base_for_debrid(svc) or '').strip()
            except Exception as e:
                logger.debug("[library] symlink_target_base_for_debrid(%r) failed: %s", svc, e)
                continue
            if b:
                bases.append(b)
    except ImportError as e:
        # debrid_routing import shouldn't fail in production but log it so
        # a misconfigured install leaves a trace instead of silently
        # degrading to RD-only behaviour (re-introducing the very bug
        # this helper exists to prevent).
        logger.debug("[library] debrid_routing import failed: %s", e)
    seen = set()
    result = []
    for b in bases:
        # normpath collapses consecutive seps + resolves relative segments
        prefix = os.path.normpath(b).rstrip(os.sep) + os.sep
        if prefix not in seen:
            seen.add(prefix)
            result.append(prefix)
    return tuple(result)

# Quality and codec markers stripped when parsing folder names
_QUALITY_PATTERN = re.compile(
    r'[\s.\-_(\[]('
    r'2160p|1080p|1080i|720p|480p|4K|UHD|HD|SD|'
    r'BluRay|Blu-Ray|BDRip|BDRemux|REMUX|BDMV|'
    r'WEB-DL|WEBRip|WEBRIP|WEBDL|WEB|'
    r'HDTV|DVDRip|DVD|HDRip|'
    r'x264|x265|H264|H265|HEVC|AVC|AV1|VP9|'
    r'AAC|AC3|DTS|TrueHD|FLAC|MP3|EAC3|'
    r'HDR|HDR10|DV|DoVi|Atmos|'
    r'PROPER|REPACK|EXTENDED|THEATRICAL|'
    r'NF|AMZN|HULU|DSNP|ATVP|PCOK|HBOMAX|HBO|IMAX'
    r').*$',
    re.IGNORECASE,
)

_SEASON_EPISODE_PATTERN = re.compile(
    r'[\s.\-_]S\d{1,2}(E\d{1,2})?.*$',
    re.IGNORECASE,
)

_YEAR_PATTERN = re.compile(r'\s*\((\d{4})\)\s*$')
_YEAR_INLINE_PATTERN = re.compile(r'[\s.\-_](\d{4})(?:[\s.\-_]|$)')
_DOTS_DASHES_PATTERN = re.compile(r'[.\-_]')
_MULTI_SPACE_PATTERN = re.compile(r'\s{2,}')


_SITE_PREFIX_PATTERN = re.compile(
    r'^(?:www\.[\w-]+\.(?:org|com|net|to|io|me|cc)[\s.\-_]+)',
    re.IGNORECASE,
)
_BRACKET_TAG_PATTERN = re.compile(r'^\[.*?\][\s.\-_]*')

# "Title - <GenreWord(s)> <year> ..." — some release naming conventions put a
# genre descriptor between the title and the year.  Strip it ONLY when the
# word(s) after the dash match a closed allowlist and a plausible 19xx/20xx
# year follows, so titles with legitimate " - Subtitle" (Leon - The
# Professional, Blade Runner - The Final Cut, Inception - Director's Cut)
# stay untouched.  "War" is deliberately excluded — too ambiguous between
# genre and real title word.  "Phycological" is the user-observed misspelling.
# Separator class `[\s._]` accepts space-dash-space AND dotted/underscored
# variants (Movie.-.Sci-Fi.2014..., Movie_-_Sci-Fi_2014_...).  Year boundary
# accepts whitespace/punctuation/close-paren/close-bracket/end-of-string only
# so `2020s`/`2014th` can't masquerade as years and silently lose the year.
# The optional `[(\[]?` inside the lookahead handles parenthesized years
# (Movie - Sci-Fi (2014) 1080p) without consuming the opening bracket, so
# the downstream _MID_YEAR_PATTERN still extracts the year.
_GENRE_SUFFIX_PATTERN = re.compile(
    r'[\s._]-[\s._]+('
    r'Sci-?Fi|Science[\s._]+Fiction|'
    r'(?:Psychological|Phycological)[\s._]+Thriller|'
    r'Action|Adventure|Animation|Biography|Comedy|Crime|'
    r'Documentary|Drama|Family|Fantasy|Horror|Musical|'
    r'Mystery|Romance|Thriller|Western'
    r')[\s._]+(?=[(\[]?(?:19|20)\d{2}(?:[\s.\-_)\]]|$))',
    re.IGNORECASE,
)

# Patterns for _clean_title
_SEASON_TEXT_PATTERN = re.compile(
    r'[\s.\-_]+Seasons?[\s.\-_]+\d+(?:[\s.\-_]*[-\u2013][\s.\-_]*\d+|[\s.\-_]+(?:to|and|&)[\s.\-_]+\d+)?'
    r'|[\s.\-_]+S\d{1,2}[\s.\-_]*[-\u2013][\s.\-_]*S\d{1,2}',
    re.IGNORECASE,
)
_MID_YEAR_PATTERN = re.compile(r'\s*\((\d{4})\)')
_EMPTY_PARENS_PATTERN = re.compile(r'\s*\(\s*\)')
_CONTAINER_SUFFIX_PATTERN = re.compile(r'\s+(?:Mp4|MKV|AVI)\s*$', re.IGNORECASE)
_EXTRAS_PATTERN = re.compile(r'\s*\+\s*\w+.*$')
_TRAILING_YEAR_PATTERN = re.compile(r'\s+(\d{4})\s*$')
_COMPLETE_SUFFIX_PATTERN = re.compile(r'\s+Complete\s*$', re.IGNORECASE)

# Parenthesized/bracketed blocks containing quality keywords
_QUALITY_KEYWORDS = (
    r'1080p|720p|2160p|480p|4K|BluRay|BDRip|BDRemux|BDmux|REMUX|'
    r'WEB-DL|WEBRip|WEBDL|WEB DL|x264|x265|H264|H265|HEVC|AVC|'
    r'AAC|AC3|DTS|EAC3|FLAC|TrueHD|Atmos|HDR|DDP\d'
)
_PAREN_QUALITY_PATTERN = re.compile(
    r'\s*\([^)]*(?:' + _QUALITY_KEYWORDS + r')[^)]*\)',
    re.IGNORECASE,
)
_BRACKET_QUALITY_PATTERN = re.compile(
    r'\s*\[[^\]]*(?:' + _QUALITY_KEYWORDS + r')[^\]]*\]',
    re.IGNORECASE,
)

# Edition/cut tags that appear between title and quality info
_EDITION_PATTERN = re.compile(
    r'\s+(?:'
    r'DC|Director\'?s?\s*Cut|Extended(?:\s+(?:Edition|Cut))?|'
    r'Theatrical(?:\s+Cut)?|Unrated|Remastered|'
    r'Criterion|Special\s+Edition|Platinum\s+Edition|'
    r'Anniversary(?:\s+\w+)*\s+Edition|'
    r'\d+(?:st|nd|rd|th)\s+Anniversary(?:\s+\w+)*\s+Edition'
    r')\s*$',
    re.IGNORECASE,
)

# Language tag followed by codec/audio info (e.g., "ITA Ac3 2.0 ENG ...")
# or trailing standalone language tags (e.g., "Title ITA")
_LANG_CODEC_PATTERN = re.compile(
    r'\s+(?:ITA|ENG|FRA|GER|ESP|MULTI|DUAL|LATINO)\s+'
    r'(?:Ac3|AAC|DTS|DD|DDP|FLAC).*$',
    re.IGNORECASE,
)
_TRAILING_LANG_PATTERN = re.compile(
    r'\s+(?:ITA|FRA|GER|ESP|MULTI|DUAL|LATINO)\s*$',
    re.IGNORECASE,
)

# Bracketed year: "[2011]" — strip brackets, extract year
_BRACKET_YEAR_PATTERN = re.compile(r'\s*\[(\d{4})\]\s*$')


def _clean_title(title, year):
    """Normalize a partially-parsed title by stripping season text, container
    suffixes, and extracting mid-string years.  Runs BEFORE dots-to-spaces for
    season patterns, then after for the rest.
    """
    # Strip "+ Extras" suffixes (before dots-to-spaces)
    title = _EXTRAS_PATTERN.sub('', title)

    # Strip "Season X" / "Seasons X-Y" / "S01-S02" text and everything after
    season_match = _SEASON_TEXT_PATTERN.search(title)
    if season_match:
        title = title[:season_match.start()]

    # Convert dots/dashes/underscores to spaces
    title = _DOTS_DASHES_PATTERN.sub(' ', title)
    title = _MULTI_SPACE_PATTERN.sub(' ', title).strip()

    # Strip empty parentheses: "Badlands ()" → "Badlands"
    title = _EMPTY_PARENS_PATTERN.sub('', title).strip()

    # Strip container suffixes: "Mp4", "MKV", "AVI"
    title = _CONTAINER_SUFFIX_PATTERN.sub('', title).strip()

    # Strip "Complete" suffix
    title = _COMPLETE_SUFFIX_PATTERN.sub('', title).strip()

    # Strip parenthesized/bracketed quality blocks: "(1080p BluRay...)", "[BDremux 1080p]"
    title = _PAREN_QUALITY_PATTERN.sub('', title).strip()
    title = _BRACKET_QUALITY_PATTERN.sub('', title).strip()

    # Strip language + codec patterns: "ITA Ac3 2.0 ENG Ac3 5.1..."
    title = _LANG_CODEC_PATTERN.sub('', title).strip()
    # Strip trailing standalone language tags: "Title ITA"
    title = _TRAILING_LANG_PATTERN.sub('', title).strip()

    # Extract bracketed year: "[2011]" → year field
    if year is None:
        bracket_year_match = _BRACKET_YEAR_PATTERN.search(title)
        if bracket_year_match:
            candidate = int(bracket_year_match.group(1))
            if 1900 <= candidate <= 2100:
                year = candidate
                title = title[:bracket_year_match.start()] + title[bracket_year_match.end():]
                title = _MULTI_SPACE_PATTERN.sub(' ', title).strip()

    # Extract mid-string year in parens: "(2003)" → year field
    if year is None:
        mid_match = _MID_YEAR_PATTERN.search(title)
        if mid_match:
            candidate = int(mid_match.group(1))
            if 1900 <= candidate <= 2100:
                year = candidate
                title = title[:mid_match.start()] + title[mid_match.end():]
                title = _MULTI_SPACE_PATTERN.sub(' ', title).strip()

    # Extract trailing bare year: "Show Name 2023" → year field
    if year is None:
        trail_match = _TRAILING_YEAR_PATTERN.search(title)
        if trail_match:
            candidate = int(trail_match.group(1))
            remaining = title[:trail_match.start()].strip()
            if 1900 <= candidate <= 2100 and remaining:
                year = candidate
                title = remaining

    # Strip edition/cut tags: "Criterion", "Extended Edition", etc.
    # Guard: don't strip if it would empty the title
    stripped = _EDITION_PATTERN.sub('', title).strip()
    if stripped:
        title = stripped

    return title, year


def _parse_folder_name(name):
    title = name

    # Strip site/indexer prefixes: "www.UIndex.org.Show.Name" → "Show.Name"
    title = _SITE_PREFIX_PATTERN.sub('', title)
    # Strip bracket tags: "[TorrentDay] Show.Name" → "Show.Name"
    title = _BRACKET_TAG_PATTERN.sub('', title)
    # Strip genre descriptor between title and year (see _GENRE_SUFFIX_PATTERN):
    #   "Predestination - Sci-Fi 2014 ..." → "Predestination 2014 ..."
    title = _GENRE_SUFFIX_PATTERN.sub(' ', title)

    # Strip trailing year in parens: "Movie Name (2024)"
    year = None
    year_match = _YEAR_PATTERN.search(title)
    if year_match:
        year = int(year_match.group(1))
        title = title[:year_match.start()].strip()
        return _clean_title(title, year)

    # Extract mid-string year in parens before quality truncation:
    # "Almost Famous (2000) DC (1080p BluRay...)" → year=2000, strip at year
    # This prevents quality patterns from cutting mid-paren and leaving
    # mangled titles like "Almost Famous (2000) DC (1080p".
    mid_year_match = _MID_YEAR_PATTERN.search(title)
    if mid_year_match:
        candidate = int(mid_year_match.group(1))
        if 1900 <= candidate <= 2100:
            year = candidate
            title = title[:mid_year_match.start()].strip()
            return _clean_title(title, year)

    # Strip S01E01-style markers (TV episodes/seasons)
    season_match = _SEASON_EPISODE_PATTERN.search(title)
    if season_match:
        title = title[:season_match.start()]
        return _clean_title(title, None)

    # Strip quality markers
    quality_match = _QUALITY_PATTERN.search(title)
    if quality_match:
        title = title[:quality_match.start()]

    # Check for inline year before quality markers: "Movie.Name.2024.1080p"
    inline_match = _YEAR_INLINE_PATTERN.search(title)
    if inline_match:
        candidate = int(inline_match.group(1))
        if 1900 <= candidate <= 2100:
            year = candidate
            title = title[:inline_match.start()]

    return _clean_title(title, year)


_EPISODE_PATTERN = re.compile(r'S\d{1,2}E\d{1,2}', re.IGNORECASE)
_EPISODE_ID_PATTERN = re.compile(r'S(\d{1,2})E(\d{1,2})', re.IGNORECASE)
_SEASON_DIR_PATTERN = re.compile(r'^Season\s+(\d+)$', re.IGNORECASE)

# Plan 41 phase B.1 — TV markers beyond ``SxxExx``.  Without these,
# season packs (``S22.COMPLETE``), multi-season packs (``S01-S04``),
# and ``Season 3`` folders that don't carry per-episode markers in the
# folder name AND lack ``SxxExx``-tagged media inside (TB partial caches,
# delete-after-watch torrents) get bucketed as movies — Radarr then
# fields wasted lookups + gap-fill searches that loop indefinitely.
#
# Order matters: multi-season range FIRST (otherwise ``S01-S04`` would
# match the single-season form ``S01`` and over-report a single season).
# Each pattern has a negative lookahead/lookaround so ``SxxExx`` isn't
# double-counted by the season-only matcher (the ``SxxExx`` form is
# already handled by ``_EPISODE_PATTERN`` via ``_collect_episodes``).
_MULTI_SEASON_RANGE_PATTERN = re.compile(
    r'\bS(\d{1,2})\s*[-–]\s*S?(\d{1,2})\b', re.IGNORECASE,
)
_SEASON_ONLY_PATTERN = re.compile(
    r'\bS(\d{1,2})(?![Ee\d])', re.IGNORECASE,
)
_SEASON_WORD_PATTERN = re.compile(
    r'\bSeasons?\.?\s*(\d{1,2})\b', re.IGNORECASE,
)


def _merge_show_group(show_groups, key, title, year, episodes, path):
    """Merge a newly-discovered folder's data into the running show-group dict.

    Single source of truth used by BOTH ``_scan_mount`` (FUSE-mount
    branch) and ``_webdav_scan_mount`` (WebDAV PROPFIND branch).  Plan
    41 phase B second-pass reviewer fix-up — the two scan paths
    previously carried structurally-identical merge code that drifted
    out of lockstep when the path-swap heuristic was added: the
    fix landed in the FUSE branch but missed the WebDAV branch (the
    HOT path, since PROPFIND runs before FUSE fallback).  Lifting
    the merge into a helper means a future change to merge semantics
    updates both scan paths atomically.

    Semantics:
      - If ``key`` is not in ``show_groups``, insert a fresh entry.
      - Otherwise, union the incoming ``episodes`` dict into the
        stored one, preferring per-season higher ``_folder_ep_count``
        on key collisions (season-pack > individual-episode grabs).
      - Swap ``path`` to the new folder when its episode count
        (``len(episodes)``) is strictly greater than the stored
        folder's BEFORE-merge count.  Empty marker (len 0) loses to
        any populated folder; equal counts keep the first-seen path
        for stability.
      - Prefer the title carrying a year over a no-year title.  On
        no-year tie, prefer title-cased over lower-cased capitalisation.

    Mutates ``show_groups`` in place.  Returns nothing.
    """
    if key not in show_groups:
        show_groups[key] = {
            'title': title,
            'year': year,
            'episodes': dict(episodes),
            'path': path,
        }
        return

    existing = show_groups[key]['episodes']
    existing_count_before = len(existing)
    for ep_key, ep_info in episodes.items():
        if ep_key not in existing:
            existing[ep_key] = ep_info
        elif ep_info.get('_folder_ep_count', 1) > existing[ep_key].get('_folder_ep_count', 1):
            existing[ep_key] = ep_info
    if len(episodes) > existing_count_before:
        show_groups[key]['path'] = path
    if year and not show_groups[key]['year']:
        show_groups[key]['year'] = year
        show_groups[key]['title'] = title
    elif title[0:1].isupper() and not show_groups[key]['title'][0:1].isupper():
        show_groups[key]['title'] = title


def _detect_tv_marker(folder_name):
    """Return ``True`` when *folder_name* carries any TV-content marker.

    Recognises:
      - ``SxxExx`` per-episode tags (the canonical case).
      - ``Sxx`` season-only tags (``S22.COMPLETE``, ``S03.1080p``).
      - ``Sxx-Syy`` or ``Sxx-yy`` multi-season ranges (``S01-S04``).
      - ``Season N`` / ``Seasons N`` word form (``Season.3.``,
        ``Seasons 1``).

    Used as a secondary classification gate in ``_scan_mount`` when
    ``_collect_episodes`` returned empty — common on TB's flat layout
    when the pack folder names a season range but the files inside are
    still being cached, or when an indexer sanitises file names to drop
    the per-episode marker.  Pre-fix these folders bucketed as movies
    and produced cascading wasted Radarr API calls.
    """
    if not folder_name:
        return False
    if _EPISODE_PATTERN.search(folder_name):
        return True
    if _MULTI_SEASON_RANGE_PATTERN.search(folder_name):
        return True
    if _SEASON_ONLY_PATTERN.search(folder_name):
        return True
    if _SEASON_WORD_PATTERN.search(folder_name):
        return True
    return False


def _release_covers_season(release_name, season, episode):
    """Classify *release_name* against a (season, episode) recovery target.

    Returns ``'pack'`` when the name marks a season pack covering *season*
    (``S03``, ``S01-S04``, ``Season 3``, ``S22.COMPLETE``), ``'episode'``
    when it carries an exact ``SxxEyy`` tag for the target episode, and
    ``None`` for everything else — a different episode, a pack for another
    season, or a name with no TV marker at all (Torrentio series result
    lists are imdb-keyed and polluted with mislabeled uploads).

    The ``SxxEyy`` check runs FIRST: an episode tag also matches the
    season-only pattern's number, so without the early return a
    wrong-episode release (``S03E09`` when we want ``S03E02``) would
    misclassify as a season-3 pack.
    """
    if not release_name:
        return None
    m = _EPISODE_ID_PATTERN.search(release_name)
    if m:
        if int(m.group(1)) == season and int(m.group(2)) == episode:
            return 'episode'
        return None
    m = _MULTI_SEASON_RANGE_PATTERN.search(release_name)
    if m:
        lo, hi = int(m.group(1)), int(m.group(2))
        if lo > hi:
            lo, hi = hi, lo
        return 'pack' if lo <= season <= hi else None
    m = _SEASON_ONLY_PATTERN.search(release_name)
    if m:
        return 'pack' if int(m.group(1)) == season else None
    m = _SEASON_WORD_PATTERN.search(release_name)
    if m:
        return 'pack' if int(m.group(1)) == season else None
    return None


def _get_folder_mtime(path):
    """Return folder mtime as Unix timestamp, or 0 on failure."""
    try:
        return int(os.path.getmtime(path))
    except OSError as e:
        logger.debug(f"[library] Cannot stat {path}: {e}")
        return 0


def _parse_tb_timestamp(value):
    """Parse a TorBox ``created_at`` string to a Unix timestamp, or 0.

    TorBox returns ISO-8601 (``2024-01-15T12:34:56.000Z`` or with a
    ``+00:00`` offset).  Used to populate ``date_added`` for API-scanned
    TB items — better than the old FUSE-walk folder mtime, since it's the
    real torrent-add time rather than whenever rclone materialised the dir.
    """
    if not isinstance(value, str) or not value:
        return 0
    v = value.strip()
    if v.endswith('Z'):
        v = v[:-1] + '+00:00'
    try:
        return int(datetime.fromisoformat(v).timestamp())
    except (ValueError, TypeError):
        return 0


def _collect_episodes(folder_path):
    """Collect episode details from a torrent folder.

    Returns dict: {(season_num, ep_num): {'file': str, 'path': str, 'folder': str}}
    Handles both structured (Season X subdirs) and flat layouts (S01E01.mkv
    directly in folder).
    """
    episodes = {}
    folder_name = os.path.basename(folder_path)
    try:
        with os.scandir(folder_path) as it:
            for entry in it:
                if entry.is_dir(follow_symlinks=False):
                    season_match = _SEASON_DIR_PATTERN.match(entry.name)
                    if not season_match:
                        continue
                    season_num = int(season_match.group(1))
                    try:
                        with os.scandir(entry.path) as season_it:
                            for f in season_it:
                                if not f.is_file(follow_symlinks=False):
                                    continue
                                ext = os.path.splitext(f.name)[1].lower()
                                if ext not in MEDIA_EXTENSIONS:
                                    continue
                                ep_match = _EPISODE_ID_PATTERN.search(f.name)
                                if ep_match:
                                    key = (int(ep_match.group(1)), int(ep_match.group(2)))
                                else:
                                    # File in Season dir but no S##E## in name — assign sequential
                                    key = (season_num, len(episodes) + 1000)
                                try:
                                    sz = f.stat(follow_symlinks=False).st_size
                                except OSError:
                                    sz = 0
                                episodes[key] = {'file': f.name, 'path': f.path, 'size_bytes': sz, 'folder': folder_name}
                    except (PermissionError, OSError):
                        pass
                elif entry.is_file(follow_symlinks=False):
                    ext = os.path.splitext(entry.name)[1].lower()
                    if ext in MEDIA_EXTENSIONS:
                        ep_match = _EPISODE_ID_PATTERN.search(entry.name)
                        if ep_match:
                            key = (int(ep_match.group(1)), int(ep_match.group(2)))
                            try:
                                sz = entry.stat(follow_symlinks=False).st_size
                            except OSError:
                                sz = 0
                            episodes[key] = {'file': entry.name, 'path': entry.path, 'size_bytes': sz, 'folder': folder_name}
    except (PermissionError, OSError, FileNotFoundError):
        pass
    return episodes


def _find_largest_movie_video(mount_dir):
    """Return ``(relpath, size)`` of the largest video file in a movie folder.

    Searches the top level first and only descends one level of
    subdirectories when the top level holds no video — some torrents nest
    the feature inside a release-named subfolder. The descent skips
    ``_SKIP_FOLDERS`` (sample/extras/featurettes/subs/...), the same set
    scan-time detection skips, so a featurette isn't picked as the feature.
    This mirrors the one-level depth scan-time movie detection uses, so a
    movie that was *detected* as on-debrid can also be *symlinked*.

    ``relpath`` is relative to *mount_dir* (may contain a subdir component).
    Returns ``(None, -1)`` when no video is found.
    """
    try:
        with os.scandir(mount_dir) as it:
            entries = list(it)
    except OSError:
        return None, -1
    best_rel = None
    best_size = -1
    for entry in entries:
        if os.path.splitext(entry.name)[1].lower() not in MEDIA_EXTENSIONS:
            continue
        try:
            if not entry.is_file(follow_symlinks=True):
                continue
            sz = entry.stat().st_size
        except OSError:
            sz = 0
        if sz > best_size:
            best_size = sz
            best_rel = entry.name
    if best_rel is not None:
        return best_rel, best_size
    # No top-level video — descend one level (nested-folder torrents).
    # Skip extras/sample/subtitle subdirs (same set scan-time detection
    # skips) so a featurette or sample isn't mistaken for the feature.
    for entry in entries:
        if entry.name.lower() in _SKIP_FOLDERS:
            continue
        try:
            if not entry.is_dir(follow_symlinks=False):
                continue
        except OSError:
            continue
        try:
            with os.scandir(entry.path) as sub:
                for f in sub:
                    if os.path.splitext(f.name)[1].lower() not in MEDIA_EXTENSIONS:
                        continue
                    try:
                        if not f.is_file(follow_symlinks=True):
                            continue
                        sz = f.stat().st_size
                    except OSError:
                        sz = 0
                    if sz > best_size:
                        best_size = sz
                        best_rel = os.path.join(entry.name, f.name)
        except OSError:
            continue
    return best_rel, best_size


def _mount_has_content(mount_real, flat=False):
    """Return True iff *mount_real* looks like a live, populated debrid mount.

    A missing or stalled/throttled FUSE mount makes ``os.path.exists`` return
    False for every path under it — which the symlink-cleanup pass would read
    as "all targets gone" and mass-delete valid symlinks.  This guard biases
    toward safety: when a mount can't be confirmed populated, callers must
    skip deletion for symlinks routed to it.

    The RD/Zurg mount is categorized (``movies/``, ``shows/``, ``anime/``);
    Zurg category stubs can persist when all content is gone, so "healthy"
    means at least one category dir is non-empty.  The TorBox mount is flat
    (no category dirs), so a non-empty top-level listing is the strongest
    signal available — pass ``flat=True`` for it.
    """
    try:
        if not os.path.isdir(mount_real) or not os.listdir(mount_real):
            return False
        if flat:
            return True
        # Single source of truth for the category set, shared with the
        # verify_symlinks guard and the blackhole scanner.
        from utils.blackhole import MOUNT_CATEGORIES
        return any(
            os.path.isdir(os.path.join(mount_real, c))
            and os.listdir(os.path.join(mount_real, c))
            for c in MOUNT_CATEGORIES
        )
    except OSError:
        return False


def _build_season_data(episodes_dict, default_source='debrid'):
    """Build sorted season_data list from an episodes dict.

    Args:
        episodes_dict: {(season_num, ep_num): {'file': str, ...}}
        default_source: source label for episodes without explicit 'source' key

    Returns: list of season dicts sorted by season number, episodes sorted within.
    """
    by_season = {}
    for (season_num, ep_num), info in episodes_dict.items():
        if season_num not in by_season:
            by_season[season_num] = []
        ep = {
            'number': ep_num,
            'file': info['file'],
            'source': info.get('source', default_source),
        }
        ep['quality'] = parse_quality(info['file'])
        ep['size_bytes'] = info.get('size_bytes', 0)
        # 'both'-sourced episodes keep debrid's folder (see merge at ~L1278),
        # so blocking targets the debrid release the user saw — not the local dir.
        folder = info.get('folder')
        if folder:
            ep['folder'] = folder
        by_season[season_num].append(ep)

    result = []
    for snum in sorted(by_season.keys()):
        eps = sorted(by_season[snum], key=lambda e: e['number'])
        result.append({
            'number': snum,
            'episode_count': len(eps),
            'episodes': eps,
        })
    return result


def _get_movie_quality_from_folder(folder_path):
    """Find the primary media file in a movie folder and parse its quality + size.

    Returns (quality_dict, size_bytes) for the largest media file found.
    """
    best_file = None
    best_size = 0
    try:
        with os.scandir(folder_path) as it:
            for entry in it:
                if not entry.is_file(follow_symlinks=True):
                    continue
                ext = os.path.splitext(entry.name)[1].lower()
                if ext not in MEDIA_EXTENSIONS:
                    continue
                try:
                    sz = entry.stat(follow_symlinks=True).st_size
                except OSError:
                    sz = 0
                if sz > best_size or best_file is None:
                    best_file = entry.name
                    best_size = sz
    except (PermissionError, OSError):
        pass
    if best_file:
        return parse_quality(best_file), best_size
    return {'resolution': None, 'source': None, 'codec': None, 'hdr': None, 'label': None}, 0


def _get_movie_quality_from_webdav(contents):
    """Find the primary media file from WebDAV folder contents and parse its quality + size.

    Returns (quality_dict, size_bytes) for the largest media file found.
    Also checks subdirectories since some movie torrents nest the file.
    """
    best_file = None
    best_size = 0
    for fname, fsize, _fpath in contents.get('files', []):
        ext = os.path.splitext(fname)[1].lower()
        if ext not in MEDIA_EXTENSIONS:
            continue
        if fsize > best_size or best_file is None:
            best_file = fname
            best_size = fsize
    for _subdir, files in contents.get('season_files', {}).items():
        for fname, fsize, _fpath in files:
            ext = os.path.splitext(fname)[1].lower()
            if ext not in MEDIA_EXTENSIONS:
                continue
            if fsize > best_size or best_file is None:
                best_file = fname
                best_size = fsize
    if best_file:
        return parse_quality(best_file), best_size
    return {'resolution': None, 'source': None, 'codec': None, 'hdr': None, 'label': None}, 0


def _count_show_content(show_path):
    seasons = 0
    episodes = 0
    flat_episodes = 0
    season_re = re.compile(r'^Season\s+\d+$', re.IGNORECASE)
    try:
        with os.scandir(show_path) as it:
            for entry in it:
                if entry.is_dir(follow_symlinks=False):
                    if season_re.match(entry.name):
                        seasons += 1
                        try:
                            with os.scandir(entry.path) as season_it:
                                for file_entry in season_it:
                                    if file_entry.is_file(follow_symlinks=False):
                                        ext = os.path.splitext(file_entry.name)[1].lower()
                                        if ext in MEDIA_EXTENSIONS:
                                            episodes += 1
                        except (PermissionError, OSError):
                            pass
                elif entry.is_file(follow_symlinks=False):
                    # Count flat episode files (e.g., S03E01.mkv directly in folder)
                    ext = os.path.splitext(entry.name)[1].lower()
                    if ext in MEDIA_EXTENSIONS and _EPISODE_PATTERN.search(entry.name):
                        flat_episodes += 1
    except (PermissionError, OSError, FileNotFoundError):
        pass

    # If no Season subdirs but flat episode files exist, report as 1 season
    if seasons == 0 and flat_episodes > 0:
        seasons = 1
        episodes = flat_episodes

    return seasons, episodes


def _discover_mount():
    mount_name = os.environ.get('RCLONE_MOUNT_NAME', '').strip()
    if mount_name:
        candidate = os.path.join('/data', mount_name)
        if os.path.isdir(candidate):
            for marker in ('__all__', 'movies', 'shows'):
                if os.path.isdir(os.path.join(candidate, marker)):
                    logger.debug(f"[library] Discovered mount via RCLONE_MOUNT_NAME: {candidate}")
                    return candidate

    blackhole_mount = os.environ.get('BLACKHOLE_RCLONE_MOUNT', '').strip()
    if blackhole_mount and os.path.isdir(blackhole_mount):
        for marker in ('__all__', 'movies', 'shows'):
            if os.path.isdir(os.path.join(blackhole_mount, marker)):
                logger.debug(f"[library] Discovered mount via BLACKHOLE_RCLONE_MOUNT: {blackhole_mount}")
                return blackhole_mount

    if os.path.isdir('/data'):
        for marker in ('__all__', 'movies', 'shows'):
            if os.path.isdir(os.path.join('/data', marker)):
                logger.debug("[library] Discovered mount at /data fallback")
                return '/data'

    return None


def _discover_zurg_url(mount_path):
    """Map a discovered rclone mount path back to the corresponding Zurg URL.

    Zurg stores its port in env vars ZURG_PORT_RealDebrid / ZURG_PORT_AllDebrid.
    When both providers are configured, mount names get _RD / _AD suffixes.
    """
    mount_name = os.path.basename(mount_path) if mount_path else ''
    rd_port = os.environ.get('ZURG_PORT_RealDebrid', '').strip()
    ad_port = os.environ.get('ZURG_PORT_AllDebrid', '').strip()

    if mount_name.endswith('_RD') and rd_port:
        return f'http://localhost:{rd_port}'
    if mount_name.endswith('_AD') and ad_port:
        return f'http://localhost:{ad_port}'
    # Single provider — use whichever port is set
    if rd_port:
        return f'http://localhost:{rd_port}'
    if ad_port:
        return f'http://localhost:{ad_port}'
    return None


def _get_zurg_auth():
    """Get Zurg WebDAV auth credentials if configured."""
    user = os.environ.get('ZURG_USER', '').strip()
    password = os.environ.get('ZURG_PASS', '').strip()
    return (user, password) if user and password else None


def _enrich_with_tmdb_cache(movies, shows, _shared_tmdb_cache=None):
    """Attach cached TMDB poster/status data to library items for grid cards.

    Performs a single bulk cache lookup (no API calls).  Items without
    cached data get None fields.  Triggers background population for
    uncached items.

    When a TMDB hit yields a canonical title that differs from the parsed
    folder title (e.g. multi-language torrent folders that bundle both
    titles), the item's `title` is replaced with the canonical TMDB title
    so the UI shows a clean name.  Returns a list of (old_norm, new_norm)
    rename pairs so the caller can wire them into the scanner's alias map,
    keeping disk-stored prefs/pending lookups working.

    Args:
        movies, shows: lists of library items to enrich in place.
        _shared_tmdb_cache: optional pre-loaded TMDB cache dict from the
            caller.  When provided, skips the per-call disk read AND
            ensures the merge step and the enrichment step see the same
            cache snapshot — without that consistency, a populate-cache
            write between the two loads could leave a parser-junk
            debrid item un-merged at merge-time and renamed at
            enrichment-time, surfacing as a transient duplicate row.
    """
    try:
        from utils.tmdb import get_cached_posters, background_populate_cache, find_show_by_season
    except ImportError:
        for item in movies:
            item['poster_url'] = None
            item['tmdb_status'] = None
            item['imdb_id'] = None
        for item in shows:
            item['poster_url'] = None
            item['tmdb_status'] = None
            item['imdb_id'] = None
            item['total_episodes'] = None
            item['missing_episodes'] = None
        return []

    all_items = [
        {'title': m['title'], 'year': m.get('year'), 'type': 'movie'}
        for m in movies
    ] + [
        {'title': s['title'], 'year': s.get('year'), 'type': 'show'}
        for s in shows
    ]

    cached = get_cached_posters(all_items)

    # Pre-load full TMDB cache once for the canonical-prefix fallback
    # below.  Used when get_cached_posters returns no match for an item
    # because its parsed title carries extra junk (actor name, genre
    # tag) that prevents direct cache key lookup — the resolver bridges
    # parsed-folder-name to the canonical TMDB entry.  When the caller
    # already loaded the cache (the merge step in _scan_read does), we
    # reuse that snapshot so both phases see consistent state.
    if _shared_tmdb_cache is not None:
        _full_tmdb_cache = _shared_tmdb_cache
    else:
        try:
            from utils import tmdb as _tmdb_mod
            with _tmdb_mod._cache_lock:
                _full_tmdb_cache = _tmdb_mod._load_cache()
        except Exception as e:
            logger.debug("[library] full TMDB cache load for enrichment failed: %s", e)
            _full_tmdb_cache = {}

    def _resolve_canonical_info(item, is_tv):
        """When direct cache lookup misses, try the prefix resolver and
        rebuild a get_cached_posters-shaped info dict from the matched
        cache entry.  Returns the info dict on hit, None on miss.
        """
        parsed_t = item.get('_parsed_title') or item.get('title') or ''
        yr = item.get('year')
        canonical = _find_canonical_tmdb_via_prefix(
            parsed_t, yr, is_tv=is_tv, _tmdb_cache=_full_tmdb_cache,
        )
        if not canonical:
            return None
        # Re-query get_cached_posters with the canonical title — produces
        # the exact same shape get_cached_posters returns for direct hits,
        # so _maybe_rename and downstream code see a consistent dict.
        # ``yr`` is the debrid item's parsed year; if it diverges from the
        # canonical TMDB release year (off-by-one is common around
        # festival vs wide-release dates), the year-qualified key misses
        # but the yearless fallback below recovers it because
        # get_cached_posters stores info under both keys (tmdb.py:664-666).
        try:
            recheck = get_cached_posters([{
                'title': canonical['title'],
                'year': yr,
                'type': 'show' if is_tv else 'movie',
            }])
        except Exception:
            return None
        canon_norm = _normalize_title(canonical['title'])
        return (recheck.get(f"{canon_norm} ({yr})" if yr else canon_norm)
                or recheck.get(canon_norm))

    uncached = []
    renames = []  # list of (old_norm, new_norm)

    def _maybe_rename(item, info):
        """Replace item['title'] with the canonical TMDB title when it
        differs.  Stashes the original parsed title in `_parsed_title` so
        downstream code that needs to match against parsed-name-keyed state
        (debrid filenames, TMDB cache, path_index) can still find it.
        Records the (old_norm, new_norm) pair for alias bookkeeping.
        """
        parsed_title = (item.get('title') or '').strip()
        if not parsed_title:
            return  # nothing meaningful to rename from
        canonical = (info.get('title') or '').strip()
        if not canonical or canonical == parsed_title:
            return
        old_norm = _normalize_title(parsed_title)
        new_norm = _normalize_title(canonical)
        if not old_norm or not new_norm:
            return  # never alias under an empty key — would pollute the map
        # Preserve the parsed title BEFORE overwriting so downstream lookups
        # (find_torrents_by_title, TMDB cache by parsed-folder key, etc.)
        # can still resolve the original.
        item['_parsed_title'] = parsed_title
        item['title'] = canonical
        if old_norm == new_norm:
            return  # display changed but normalize key didn't — no alias needed
        renames.append((old_norm, new_norm))

    for movie in movies:
        key = _normalize_title(movie['title'])
        yr = movie.get('year')
        info = cached.get(f"{key} ({yr})" if yr else key) or cached.get(key)
        if not info:
            # Direct lookup missed — try the canonical-prefix fallback so
            # parser-junk titles like "Gattaca Ethan Hawke Sci Fi" still
            # get renamed to the canonical TMDB title and pick up posters.
            info = _resolve_canonical_info(movie, is_tv=False)
        if info:
            _maybe_rename(movie, info)
            movie['poster_url'] = info['poster_url'] or None
            movie['tmdb_status'] = info.get('tmdb_status') or None
            movie['imdb_id'] = info.get('imdb_id') or None
        else:
            movie['poster_url'] = None
            movie['tmdb_status'] = None
            movie['imdb_id'] = None
            uncached.append({'title': movie['title'], 'year': movie.get('year'), 'type': 'movie'})

    for show in shows:
        key = _normalize_title(show['title'])
        yr = show.get('year')
        info = cached.get(f"{key} ({yr})" if yr else key) or cached.get(key)
        if not info:
            # Symmetric with the movies branch.
            info = _resolve_canonical_info(show, is_tv=True)
        if info:
            # Season-aware validation: if the show has seasons beyond what
            # the cached TMDB entry covers, the cache may have matched the
            # wrong show (e.g. "Daredevil" S03 hitting "Born Again" which
            # only has S01-S02 instead of Netflix's "Marvel's Daredevil").
            show_max = max(
                (s['number'] for s in show.get('season_data', []) if s.get('number')),
                default=0,
            )
            cached_max = info.get('max_cached_season', 0)
            if show_max > 0 and cached_max < show_max:
                better = find_show_by_season(key, show_max, yr)
                if better and better.get('max_cached_season', 0) >= show_max:
                    info = better
            _maybe_rename(show, info)
            show['poster_url'] = info['poster_url'] or None
            show['tmdb_status'] = info.get('tmdb_status') or None
            show['imdb_id'] = info.get('imdb_id') or None
            total = info.get('total_episodes') or 0
            show['total_episodes'] = total if total > 0 else None
            have = show.get('episodes', 0)
            show['missing_episodes'] = max(0, total - have) if total > 0 else None
        else:
            show['poster_url'] = None
            show['tmdb_status'] = None
            show['imdb_id'] = None
            show['total_episodes'] = None
            show['missing_episodes'] = None
            uncached.append({'title': show['title'], 'year': show.get('year'), 'type': 'show'})

    if uncached:
        background_populate_cache(uncached)

    return renames


# Scan-scoped cache for Sonarr's series list.  ``_scan_read`` fetches
# once and the same read-phase pass is also reused by ``_scan_effects``
# (via ``_create_debrid_symlinks``) so the scanner doesn't round-trip
# Sonarr twice per cycle.  TTL is deliberately short — long enough to
# span one scan (read → effects takes seconds) but short enough that
# back-to-back manual refreshes get fresh data.
_SONARR_SERIES_TTL = 120
_sonarr_series_cache = {'data': None, 'ts': 0.0}
_sonarr_series_lock = threading.Lock()

# Radarr full-movie-list cache, mirroring the Sonarr posture above. Used
# by _apply_radarr_wanted_movies to inject monitored-but-no-file movies
# into the library data as "ghost" entries so they surface in the Wanted
# view. Same TTL — short enough that a manual rescan picks up changes,
# long enough to span a single scan_read pass without re-hitting Radarr.
_RADARR_MOVIES_TTL = 120
_radarr_movies_cache = {'data': None, 'ts': 0.0}
_radarr_movies_lock = threading.Lock()


def _get_radarr_movies_list(client, force_refresh=False):
    """Fetch Radarr's full movie list with a short TTL cache.

    Returns the raw movie list on success, ``None`` on fetch failure, or
    ``[]`` when Radarr returns an empty library. Mirrors
    ``_get_sonarr_series_list``.
    """
    now = time.monotonic()
    with _radarr_movies_lock:
        if not force_refresh:
            cached = _radarr_movies_cache.get('data')
            ts = _radarr_movies_cache.get('ts', 0.0)
            if cached is not None and (now - ts) < _RADARR_MOVIES_TTL:
                return cached
    try:
        movie_list = client.get_all_movies() or []
    except Exception as e:
        logger.warning(f"[library] Could not fetch Radarr movies: {e}")
        return None
    with _radarr_movies_lock:
        _radarr_movies_cache['data'] = movie_list
        _radarr_movies_cache['ts'] = now
    return movie_list


def _get_sonarr_series_list(client, force_refresh=False):
    """Fetch Sonarr's full series list with a short TTL cache.

    Returns the raw series list on success, ``None`` on fetch failure, or
    ``[]`` when Sonarr returns an empty library.  Caches the result under
    a process-wide lock so concurrent callers share one HTTP round-trip.
    """
    now = time.monotonic()
    with _sonarr_series_lock:
        if not force_refresh:
            cached = _sonarr_series_cache.get('data')
            ts = _sonarr_series_cache.get('ts', 0.0)
            if cached is not None and (now - ts) < _SONARR_SERIES_TTL:
                return cached
    try:
        series_list = client.get_all_series() or []
    except Exception as e:
        logger.warning(f"[library] Could not fetch Sonarr series: {e}")
        return None
    with _sonarr_series_lock:
        _sonarr_series_cache['data'] = series_list
        _sonarr_series_cache['ts'] = now
    return series_list


def _sonarr_monitored_missing(series):
    """Monitored-aware missing-episode math for a single Sonarr series.

    Returns ``(missing, monitored_total, unmonitored_seasons)`` summed from
    the series' per-season ``statistics`` — aired monitored ``episodeCount``
    minus ``episodeFileCount`` — skipping specials (season 0) and seasons
    the user has unmonitored.  Shared by ``_apply_sonarr_monitored_filter``
    (rebasing real library shows) and ``_apply_sonarr_wanted_shows``
    (injecting fully-absent monitored series) so both agree on the same
    arithmetic.

    Note ``episodeFileCount`` counts files across ALL episodes in a season
    regardless of per-episode monitored flag, so in a mixed season it can
    exceed the monitored ``episodeCount``; the ``max(0, …)`` clamp keeps
    that from going negative (at the cost of hiding genuine gaps in such
    seasons — accepted, see the original call-site note).
    """
    missing = 0
    monitored_total = 0
    unmonitored_nums = []
    for sd in series.get('seasons') or []:
        snum = sd.get('seasonNumber')
        if snum is None or snum <= 0:
            continue  # skip specials
        if not sd.get('monitored'):
            unmonitored_nums.append(snum)
            continue
        stats = sd.get('statistics') or {}
        ep_count = stats.get('episodeCount', 0) or 0
        ep_file = stats.get('episodeFileCount', 0) or 0
        monitored_total += ep_count
        missing += max(0, ep_count - ep_file)
    return missing, monitored_total, sorted(unmonitored_nums)


def _apply_sonarr_monitored_filter(shows, degraded=None):
    """Rebase show missing-episode counts against Sonarr's monitored view.

    The TMDB-only math in ``_enrich_with_tmdb_cache`` counts every aired
    TMDB episode that isn't on disk as "missing" — which inflates badly
    for long-running shows where the user has explicitly unmonitored
    older seasons in Sonarr (e.g. Grey's Anatomy S1–S15).  Sonarr already
    exposes the monitored-aware counts per season in one bulk endpoint:
    ``season.statistics.episodeCount`` is aired monitored episodes and
    ``episodeFileCount`` is those that have a file.  Summing
    ``max(0, episodeCount - episodeFileCount)`` across monitored seasons
    gives the "truly wanted, still missing" count.

    Matching cascade (TMDB ID first since it's the only unambiguous
    identifier): cached-TMDB-ID under either the parsed folder title or
    the enrichment-upgraded title → exact lowercase title (year-qualified
    then plain) → ``_norm_for_matching`` fuzzy.  Collision-safe: if two
    Sonarr series share a lowercase or normalized title (common for
    reboots like "Magnum P.I." 1980 vs 2018, or international retitles),
    those title-level keys are marked ambiguous and skipped — TMDB ID
    remains the only reliable path for colliding titles.

    Side effects per matched show:
      * ``missing_episodes`` replaced with Sonarr's monitored-aware count.
      * ``unmonitored_seasons`` list attached so the UI can skip those
        seasons when rendering the TMDB-expected missing-episode table,
        and so gap-fill can short-circuit rather than round-tripping
        Sonarr once per unmonitored season.

    Shows without a Sonarr match (or when Sonarr is unreachable) keep
    the TMDB-only calculation — it's conservative but preserves the
    existing behavior for hand-imported libraries.

    Returns the set of Sonarr series ids that matched a real library show,
    so ``_apply_sonarr_wanted_shows`` can inject ghosts only for the
    monitored series that remain unmatched (no double-counting).  Returns
    an empty set on every early exit.

    ``degraded`` is an optional mutable set: when a *configured* Sonarr's
    series-list fetch fails (returns ``None`` — network/DNS error, not an
    empty library), ``'sonarr_series'`` is added so the scan payload can
    flag that wanted counts fell back to the inflated TMDB-only math.
    """
    matched_ids = set()
    if not shows:
        return matched_ids
    try:
        from utils.arr_client import get_download_service
        client, svc = get_download_service('show')
    except Exception as e:
        logger.debug(f"[library] Sonarr unavailable for monitored filter: {e}")
        return matched_ids
    if not client or svc != 'sonarr':
        return matched_ids
    series_list = _get_sonarr_series_list(client)
    if series_list is None:
        if degraded is not None:
            degraded.add('sonarr_series')
        return matched_ids
    if not series_list:
        return matched_ids

    by_tmdb = {}
    by_norm = {}
    by_title = {}
    norm_collisions = set()
    title_collisions = set()
    for s in series_list:
        tid = s.get('tmdbId')
        if tid:
            # TMDB IDs are globally unique so a duplicate is a Sonarr
            # data bug, not a reboot collision — first-writer-wins is
            # the safest stance.
            by_tmdb.setdefault(tid, s)
        title = s.get('title', '')
        if not title:
            continue
        tk = title.lower()
        if tk in by_title and by_title[tk] is not s:
            title_collisions.add(tk)
        else:
            by_title[tk] = s
        nk = _norm_for_matching(title)
        if nk:
            if nk in by_norm and by_norm[nk] is not s:
                norm_collisions.add(nk)
            else:
                by_norm[nk] = s

    # Resolve TMDB IDs for library shows via the cached TMDB→ID map so
    # reboots/differently-titled entries match the same as the rest of
    # the scanner.
    try:
        from utils.tmdb import get_cached_tmdb_ids
        cached_show_ids = get_cached_tmdb_ids().get('shows', {})
    except Exception as e:
        logger.debug(f"[library] TMDB ID cache unavailable for monitored filter, skipping: {e}")
        cached_show_ids = {}

    def _lookup_tmdb_id(candidate, year):
        if not candidate:
            return None
        nt = _normalize_title(candidate)
        if not nt:
            return None
        return (cached_show_ids.get(f"{nt} ({year})") if year else None) or cached_show_ids.get(nt)

    def _title_match(candidate, year):
        if not candidate:
            return None
        if year:
            key = f"{candidate} ({year})".lower()
            if key in by_title and key not in title_collisions:
                return by_title[key]
        key = candidate.lower()
        if key in by_title and key not in title_collisions:
            return by_title[key]
        nk = _norm_for_matching(candidate)
        if nk and nk in by_norm and nk not in norm_collisions:
            return by_norm[nk]
        return None

    for show in shows:
        series = None
        title = show.get('title', '')
        parsed_title = show.get('_parsed_title') or ''
        year = show.get('year')

        # 1) TMDB ID match — try both the parsed-folder title and the
        # enrichment-upgraded display title, since the TMDB cache can be
        # keyed under either depending on what got scanned first.
        for candidate in (parsed_title, title):
            tmdb_id = _lookup_tmdb_id(candidate, year)
            if tmdb_id:
                series = by_tmdb.get(tmdb_id)
                if series:
                    break

        # 2/3) Title match (exact year-qualified → exact plain → fuzzy
        # norm), skipping any key shared by multiple Sonarr series.
        if not series:
            for candidate in (title, parsed_title):
                series = _title_match(candidate, year)
                if series:
                    break

        if not series:
            continue

        sid = series.get('id')
        if sid is not None:
            matched_ids.add(sid)

        missing, monitored_total, unmonitored_nums = _sonarr_monitored_missing(series)
        show['missing_episodes'] = missing
        show['unmonitored_seasons'] = unmonitored_nums
        # ``monitored_episodes`` is the denominator the UI needs for a
        # progress bar that agrees with the "X missing" pill — otherwise
        # the bar stays red at a low TMDB-based ratio while the pill
        # reads "1 missing" for a show with large unmonitored back
        # catalogue.  Left off-payload when the show had no monitored
        # seasons (user paused all seasons) so the frontend falls back
        # to the TMDB total and doesn't draw a divide-by-zero bar.
        if monitored_total > 0:
            show['monitored_episodes'] = monitored_total

    return matched_ids


def _apply_sonarr_wanted_shows(shows, matched_ids, pending=None, degraded=None):
    """Inject Sonarr-monitored series with no on-disk episodes as "ghost"
    show entries so fully-absent wanted TV surfaces in the Wanted view and
    is counted by the recovery metric.

    This is the TV mirror of ``_apply_radarr_wanted_movies``.  The library
    scanner reads episodes from disk, so a monitored series you haven't
    downloaded *any* episode of is invisible to the rest of the pipeline —
    and, crucially, to the recovery denominator, which sums per-show
    ``missing_episodes``.  A show that's partially on disk already carries
    a Sonarr-aware ``missing_episodes`` (set by
    ``_apply_sonarr_monitored_filter``); this adds the missing half for
    series with zero matched episodes.

    ``matched_ids`` is the set of Sonarr series ids that already matched a
    real library show (the return value of the monitored filter); those
    are skipped so we never duplicate a card or double-count episodes.
    That shortcut alone is not sufficient: a partially-on-disk show whose
    title-match cascade the filter *missed* never lands in ``matched_ids``,
    so without a second guard its real entry (carrying a TMDB-based
    ``missing_episodes``) AND a Sonarr-based ghost would both count toward
    the recovery denominator — a double-count.  So we also dedup against
    the on-disk ``shows`` list by ``tmdb_id``, ``imdb_id``, and the
    ``(norm_title, year)`` pair, mirroring ``_apply_radarr_wanted_movies``.
    The later ``_dedup_shows_by_external_id`` pass can't be relied on here
    because it keys on ``imdb_id`` only, which TVDB-only series and cache
    misses frequently lack.

    Runs AFTER enrichment, so each ghost's ``missing_episodes`` comes from
    Sonarr's monitored season statistics (aired-monitored only — unaired
    episodes are excluded) rather than TMDB-total math.  A series is
    injected only when that count is > 0, so series Sonarr already
    considers satisfied never appear.

    Ghost entry shape mirrors the movie ghost: ``source='wanted'`` (outside
    the ``('local','debrid','both')`` set every effect path checks), empty
    ``_episodes`` / ``season_data`` so symlink, search, preference, and
    path-index loops naturally no-op.  ``imdb_id`` is carried from Sonarr
    so a later ``_dedup_shows_by_external_id`` pass collapses any ghost that
    collides with a real show whose title-match the filter happened to miss.

    Pending suppression mirrors the movie path: a series currently being
    downloaded is already represented by the pending bucket, so its ghost
    is skipped to avoid double-counting.

    Returns the count of ghost entries injected (for caller logging).

    ``degraded`` mirrors ``_apply_sonarr_monitored_filter``: a failed
    series-list fetch from a configured Sonarr adds ``'sonarr_series'``
    (here the failure *deflates* wanted — ghosts never get injected).
    """
    pending = pending or {}
    matched_ids = matched_ids or set()
    try:
        from utils.arr_client import get_download_service
        client, svc = get_download_service('show')
    except Exception as e:
        logger.debug(f"[library] Sonarr unavailable for wanted-shows: {e}")
        return 0
    if not client or svc != 'sonarr':
        return 0

    series_list = _get_sonarr_series_list(client)
    if series_list is None:
        if degraded is not None:
            degraded.add('sonarr_series')
        return 0
    if not series_list:
        return 0

    # Build dedup keys from the real on-disk shows. matched_ids only
    # captures title-cascade hits; these sets catch a real show the
    # cascade missed so we never inject a ghost beside it (double-count).
    existing_tmdb_ids = set()
    existing_imdb_ids = set()
    existing_keys = set()
    for sh in shows:
        if sh.get('source') == 'wanted':
            continue
        tid = sh.get('tmdb_id')
        if tid:
            existing_tmdb_ids.add(tid)
        iid = sh.get('imdb_id')
        if iid:
            existing_imdb_ids.add(iid)
        norm = _normalize_title(sh.get('title') or '')
        if norm:
            existing_keys.add((norm, sh.get('year')))

    injected = 0
    for s in series_list:
        if not isinstance(s, dict):
            continue
        if not s.get('monitored'):
            continue
        sid = s.get('id')
        if sid is not None and sid in matched_ids:
            continue
        title = s.get('title') or ''
        if not title:
            continue

        year = s.get('year')
        tmdb_id = s.get('tmdbId')
        imdb_id = s.get('imdbId')
        norm = _normalize_title(title)
        if tmdb_id and tmdb_id in existing_tmdb_ids:
            continue
        if imdb_id and imdb_id in existing_imdb_ids:
            continue
        if (norm, year) in existing_keys:
            continue

        missing, monitored_total, unmonitored_nums = _sonarr_monitored_missing(s)
        if missing <= 0:
            continue

        # Pending suppression: a title (or any alias) currently downloading
        # is already counted under the pending bucket; skip its ghost.
        if pending:
            pe = pending.get(norm)
            if not pe and _scanner is not None:
                for alias in _scanner.aliases_for(norm):
                    pe = pending.get(alias)
                    if pe:
                        break
            if pe:
                continue

        ghost = {
            'title': title,
            'year': year,
            'type': 'show',
            'source': 'wanted',
            'size_bytes': 0,
            'path': '',
            'missing': True,
            'missing_episodes': missing,
            'unmonitored_seasons': unmonitored_nums,
            # Empty so downstream effect loops (symlinks, searches, prefs,
            # path-index) iterate zero episodes and naturally no-op.
            'season_data': [],
            '_episodes': {},
            '_sonarr_id': sid,
            '_sonarr_tmdb_id': tmdb_id,
        }
        if monitored_total > 0:
            ghost['monitored_episodes'] = monitored_total
        if imdb_id:
            ghost['imdb_id'] = imdb_id
        if tmdb_id:
            ghost['tmdb_id'] = tmdb_id
        shows.append(ghost)
        injected += 1
        # Update dedup keys so a duplicate Sonarr entry can't double-inject.
        if tmdb_id:
            existing_tmdb_ids.add(tmdb_id)
        if imdb_id:
            existing_imdb_ids.add(imdb_id)
        existing_keys.add((norm, year))

    return injected


def _dedup_shows_by_external_id(shows):
    """Collapse shows that share the same IMDb ID into a single entry.

    Three different debrid folder names for the same series (e.g.
    ``Your Friends And Neighbors``, ``Your Friends Neighbors``, and
    ``Your Friends and Neighbours``) survive ``_dedup_by_tmdb`` when
    the TMDB alias map doesn't carry all three normalized titles.
    ``_enrich_with_tmdb_cache`` then stamps the same ``imdb_id`` on
    each — at which point we have N library cards showing the SAME
    canonical title and external ID but different debrid paths.

    Keys by ``imdb_id`` only.  Enrichment currently does not stamp
    ``tmdb_id`` on shows, so a tmdb fallback would be dead code; if
    that changes (an enrichment refactor adds it), this helper can be
    extended without breaking callers.

    For each multi-entry group:
      * picks a survivor via ``_rank`` (source 'both' > 'local' > 'debrid',
        then year-populated, then more episodes)
      * unions ``_episodes`` with PER-EPISODE quality compare on
        collisions (larger ``size_bytes`` wins — preserves the better
        release rather than first-seen)
      * rebuilds ``season_data`` from the merged dict via
        ``_build_season_data`` so downstream consumers (composition
        card, prefs enforcer, gap-fill, search loops) see the full
        episode set, not just the survivor's
      * PRESERVES the survivor's Sonarr-aware ``missing_episodes`` /
        ``monitored_episodes`` from ``_apply_sonarr_monitored_filter``
        (which ran pre-merge) — naively recomputing against
        ``total_episodes`` would revert to TMDB-all math and inflate
        missing counts on shows with unmonitored seasons (Grey's
        Anatomy regression)
      * recomputes ``size_bytes`` from the merged ``_episodes`` so
        overlapping episode releases don't double-count
      * promotes ``source`` to ``'both'`` when any input is local-or-mixed

    Runs in place on the ``shows`` list. O(n).
    """
    if not shows or len(shows) < 2:
        return

    groups = {}  # imdb_id -> list of show dicts (preserves order)
    no_id = []
    for show in shows:
        imdb = show.get('imdb_id')
        if imdb:
            groups.setdefault(imdb, []).append(show)
        else:
            no_id.append(show)

    if not any(len(g) > 1 for g in groups.values()):
        return  # no collisions, nothing to do

    def _has_sonarr_filter(s):
        # _apply_sonarr_monitored_filter writes monitored_episodes only
        # for shows whose title matched a Sonarr series.  Prefer those
        # as the survivor so the merged entry inherits the Sonarr-aware
        # counts (Grey's-Anatomy unmonitored-seasons math), not TMDB-all.
        return s.get('monitored_episodes') is not None

    def _rank(s):
        # Sonarr-filtered FIRST so a sibling with monitored counts wins
        # over a survivor that missed the title-match cascade.  Then
        # source 'both' > 'local' > 'debrid', year populated, most episodes.
        src = s.get('source', '')
        src_rank = {'both': 0, 'local': 1, 'debrid': 2}.get(src, 3)
        return (
            0 if _has_sonarr_filter(s) else 1,
            src_rank,
            0 if s.get('year') else 1,
            -len(s.get('_episodes') or {}),
        )

    def _episode_size(info):
        # Best-effort size extraction — used for collision tie-breaking.
        # Falls back to 0 so missing-size entries lose ties to known-good
        # entries.  ``info`` is a per-episode dict from ``_episodes``.
        try:
            return int(info.get('size_bytes') or 0)
        except (TypeError, ValueError):
            return 0

    def _resolve_merged_source(group):
        sources = {s.get('source') for s in group if s.get('source')}
        if 'both' in sources or ('local' in sources and 'debrid' in sources):
            return 'both'
        if 'local' in sources and 'debrid' not in sources:
            return 'local'
        return 'debrid'

    result = []
    for imdb_id, group in groups.items():
        if len(group) == 1:
            result.append(group[0])
            continue
        best = min(group, key=_rank)
        merged = dict(best)

        # Compute the merged source FIRST so the season_data rebuild sees
        # the right default_source.  Episodes from siblings often lack an
        # explicit 'source' key (FUSE/WebDAV scanners don't stamp one);
        # _build_season_data falls back to default_source for those, so
        # passing 'both' here would falsely tag debrid-only sibling
        # episodes as 'both' and surface bogus source badges in the UI.
        # Override only the show-level source; per-episode source labels
        # come from the survivor's explicit per-ep keys when present.
        merged_source = _resolve_merged_source(group)
        # The season_data rebuild uses a non-promoted default for the
        # episode fallback path so debrid-only sibling episodes stay
        # tagged 'debrid' rather than inheriting the show-level 'both'.
        episode_default_source = 'debrid' if 'debrid' in (s.get('source') for s in group) else merged_source

        # Union episodes with per-episode quality compare.  Per-episode
        # ``size_bytes`` proxies for quality (1080p AMZN WEB-DL > 720p
        # WEB cap, etc.).  On collision the larger-size info wins;
        # equal-size collisions keep first-seen (best's release) so
        # folder/blocklist tracking points at the survivor's release.
        # Episodes lacking a 'file' key (legacy list-of-tuples shape
        # from _normalize_episodes_for_merge produces empty info dicts)
        # are skipped — _build_season_data would crash on them.
        merged_eps = {}
        for item in group:
            for ep_key, ep_info in (item.get('_episodes') or {}).items():
                if not isinstance(ep_info, dict) or 'file' not in ep_info:
                    continue
                existing = merged_eps.get(ep_key)
                if existing is None or _episode_size(ep_info) > _episode_size(existing):
                    merged_eps[ep_key] = ep_info
        merged['_episodes'] = merged_eps
        merged['seasons'] = len({ek[0] for ek in merged_eps})
        merged['episodes'] = len(merged_eps)

        # Rebuild season_data from the unioned episode dict — downstream
        # consumers (composition card sizes, prefs enforcer, gap-fill,
        # search loops) all iterate ``season_data`` not ``_episodes``.
        merged['season_data'] = _build_season_data(merged_eps, episode_default_source)

        # Recompute size_bytes from the merged per-episode sizes.  Summing
        # show.size_bytes across siblings would double-count overlapping
        # releases.  Fall back to the max sibling show-level size when
        # all merged episodes lack per-ep sizes (legacy scanner path)
        # so the composition card doesn't silently zero-out.
        per_episode_total = sum(_episode_size(info) for info in merged_eps.values())
        if per_episode_total > 0:
            merged['size_bytes'] = per_episode_total
        else:
            fallback_max = max((s.get('size_bytes') or 0 for s in group), default=0)
            merged['size_bytes'] = fallback_max

        # Preserve the survivor's Sonarr-aware missing_episodes /
        # monitored_episodes.  Because ``_rank`` now puts Sonarr-filtered
        # entries first, the survivor either HAS monitored math
        # (correct) or NONE of the siblings did (so survivor's TMDB-all
        # math is the best available).  Don't recompute against
        # total_episodes — would revert to TMDB-all math and re-inflate
        # missing on shows with unmonitored seasons.

        merged['source'] = merged_source

        # Use the earliest date_added (skip zero = stat failure) — most
        # honest "when did this enter the user's library" timestamp.
        dates = [s.get('date_added', 0) for s in group if s.get('date_added', 0) > 0]
        if dates:
            merged['date_added'] = min(dates)

        logger.debug(
            "[library] external-id dedup: collapsed %d entries for imdb=%s "
            "(%r) — kept %r, dropped %d", len(group), imdb_id,
            merged.get('title'), best.get('path', ''), len(group) - 1
        )

        result.append(merged)

    shows[:] = result + no_id


def _strip_ghost_duplicates(movies):
    """Drop ghost entries whose post-enrichment ``(norm, year)`` collides
    with a real on-disk movie.

    The pre-enrichment dedup in ``_apply_radarr_wanted_movies`` uses
    parsed-folder norms, which is correct at injection time but doesn't
    survive enrichment: ``_enrich_with_tmdb_cache._maybe_rename`` can
    rewrite a real entry's ``title`` to its canonical TMDB spelling
    (e.g. ``"F1 The Movie"`` → ``"F1"``), which may now match a ghost
    we already injected. Without this pass, the library renders the
    same movie as two cards — one real (Available, green) and one ghost
    (Wanted, red).

    Real entries always win — they're on disk, the ghost was a synthetic
    placeholder. Runs in place on the ``movies`` list. O(n).
    """
    real_keys = set()
    for m in movies:
        if m.get('source') == 'wanted':
            continue
        norm = _normalize_title(m.get('title') or '')
        if norm:
            real_keys.add((norm, m.get('year')))
    if not real_keys:
        return
    movies[:] = [
        m for m in movies
        if m.get('source') != 'wanted'
        or (_normalize_title(m.get('title') or ''), m.get('year')) not in real_keys
    ]


def _apply_radarr_wanted_movies(movies, pending=None, degraded=None):
    """Inject Radarr-monitored movies with no file into the movie list as
    "ghost" entries so they surface in the Wanted view.

    The library scanner reads from disk (mount + local), so a movie that
    Radarr knows you want but hasn't downloaded yet is invisible to the
    rest of the pipeline. Without this function, the Wanted filter only
    works for shows (where ``missing_episodes > 0`` flags shows whose
    library entries exist but lack some episodes). This adds the missing
    half of that experience for movies.

    Ghost entry shape::

        {'title', 'year', 'source': 'wanted', 'size_bytes': 0,
         'path': '', 'missing': True,
         '_radarr_id': <int>, '_radarr_tmdb_id': <int>}

    Source is set to a new ``'wanted'`` label that is intentionally
    outside the ``('local', 'debrid', 'both')`` set used by everywhere
    else in the codebase. Existing source-conditional code paths
    (preference enforcement, symlink work, library stats bucketing) all
    use ``src in ('local', 'debrid', 'both')`` checks that naturally
    skip ghost entries — no defensive changes required at those sites.

    Dedup: a Radarr movie is suppressed when an existing library entry
    matches it via either the Radarr-supplied ``tmdbId`` OR the
    normalized title + year pair. TMDB ID is the unambiguous path;
    title+year fallback handles legacy library entries that pre-date
    TMDB enrichment.

    Pending suppression: a Radarr movie currently being downloaded is
    already counted under the 'pending' bucket via the existing
    ``pending_monitors`` mechanism; we skip the ghost so it doesn't
    double-count.

    Radarr-unavailable or movie-list-fetch-fails posture: log a debug
    note and return without injecting. The scan continues with whatever
    real movies it discovered.

    Returns the count of ghost entries injected (for caller logging).

    ``degraded`` mirrors the Sonarr helpers: a failed movie-list fetch
    from a configured Radarr adds ``'radarr_movies'`` (ghost movies never
    get injected, deflating the wanted denominator).
    """
    pending = pending or {}
    try:
        from utils.arr_client import get_download_service
        client, svc = get_download_service('movie')
    except Exception as e:
        logger.debug(f"[library] Radarr unavailable for wanted-movies: {e}")
        return 0
    if not client or svc != 'radarr':
        return 0

    radarr_movies = _get_radarr_movies_list(client)
    if radarr_movies is None:
        if degraded is not None:
            degraded.add('radarr_movies')
        return 0
    if not radarr_movies:
        return 0

    # Build dedup keys from the existing movies list. TMDB ID set is
    # authoritative; the (norm_title, year) fallback catches entries
    # without a tmdb_id (e.g. hand-imported libraries).
    existing_tmdb_ids = set()
    existing_keys = set()
    for m in movies:
        tid = m.get('tmdb_id')
        if tid:
            existing_tmdb_ids.add(tid)
        norm = _normalize_title(m.get('title') or '')
        if norm:
            existing_keys.add((norm, m.get('year')))

    injected = 0
    for rm in radarr_movies:
        if not isinstance(rm, dict):
            continue
        if not rm.get('monitored'):
            continue
        if rm.get('hasFile'):
            continue
        title = rm.get('title') or ''
        if not title:
            continue
        year = rm.get('year')
        tmdb_id = rm.get('tmdbId')

        if tmdb_id and tmdb_id in existing_tmdb_ids:
            continue
        norm = _normalize_title(title)
        if (norm, year) in existing_keys:
            continue

        # Pending state suppression: if this title (or any alias) is in
        # the pending dict, the existing pending/unavailable buckets
        # already account for it.
        if pending:
            pe = pending.get(norm)
            if not pe and _scanner is not None:
                for alias in _scanner.aliases_for(norm):
                    pe = pending.get(alias)
                    if pe:
                        break
            if pe:
                continue

        ghost = {
            'title': title,
            'year': year,
            # ``type`` is load-bearing for the /api/library/metadata UI
            # call — without it, the JS sends ``type=undefined`` and the
            # server defaults to 'show', poisoning the TMDB cache with
            # show-shaped data under movie-style keys.
            'type': 'movie',
            'source': 'wanted',
            'size_bytes': 0,
            'path': '',
            'missing': True,
            # Radarr's computed "has reached minimum availability" flag.
            # Stamped so the recovery metric can exclude announced/unreleased
            # titles from its denominator; the Wanted UI view ignores it and
            # still shows every monitored-but-missing movie.
            'is_available': bool(rm.get('isAvailable')),
            '_radarr_id': rm.get('id'),
            '_radarr_tmdb_id': tmdb_id,
        }
        movies.append(ghost)
        injected += 1
        # Update dedup keys so a duplicate Radarr entry (rare but
        # possible across reboots / data bugs) doesn't double-inject.
        if tmdb_id:
            existing_tmdb_ids.add(tmdb_id)
        existing_keys.add((norm, year))

    return injected


def _normalize_title(title):
    t = title.lower()
    t = re.sub(r'\s*\(\d{4}\)\s*$', '', t)
    t = t.strip()
    return t


def _norm_for_matching(title):
    """Normalize title for fuzzy matching across systems.

    Transliterates unicode to ASCII (e.g., Amélie → Amelie), replaces
    hyphens/underscores with spaces (so "Cover-Up" matches "Cover Up"),
    strips remaining punctuation but keeps digits for disambiguation.
    Titles like "(500) Days of Summer" and "500 Days of Summer" match,
    while "Flash (2014)" and "Flash (2023)" remain distinct.
    """
    t = title.lower()
    # Transliterate unicode to ASCII (é → e, ñ → n, etc.)
    t = unicodedata.normalize('NFKD', t).encode('ascii', 'ignore').decode('ascii')
    # Normalize common symbols to words before stripping
    t = t.replace('&', ' and ')
    # Replace word-separating punctuation with spaces before stripping
    t = t.replace('-', ' ').replace('_', ' ')
    # Strip remaining punctuation but keep alphanumeric and spaces
    t = re.sub(r'[^a-z0-9\s]', '', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


_BARE_YEAR_RE = re.compile(r'\s*\(\d{4}\)\s*$')


_ANY_YEAR_RE = re.compile(r'(?<!\d)(?:19|20)\d{2}(?!\d)')

_LEADING_ARTICLES = ('the', 'a', 'an')


def _release_matches_title(release_title, media_title, media_year=None):
    """True when a torrent release name plausibly belongs to *media_title*.

    Torrentio stream lists are keyed by IMDb id but polluted with
    mislabeled uploads (e.g. a "Fight Club" release inside The Fountain's
    list), so auto-add paths must sanity-check the release name before
    adding.  Accepts an exact normalized match or the media title as a
    token-aligned prefix of the parsed release name ("F1" → "F1 The
    Movie") — never the reverse, so a short junk name can't claim a
    longer title.  A leading article is stripped on both sides (scene
    names routinely drop the "The" the arr keeps).  When *media_year*
    is known (or embedded as "(YYYY)" in the media title), a release
    whose years ALL sit >1 away is rejected — this catches sequels
    riding the prefix rule ("Dune Part Two 2024" claiming "Dune" 2021)
    and same-name remakes.
    """
    from utils.blackhole import parse_release_name
    raw_release = str(release_title or '')
    parsed, _season, _is_tv = parse_release_name(raw_release)
    media_raw = str(media_title or '')
    media = _BARE_YEAR_RE.sub('', media_raw)
    rel_norm = _norm_for_matching(parsed)
    media_norm = _norm_for_matching(media)
    if not rel_norm or not media_norm:
        return False
    rel_tokens = rel_norm.split()
    media_tokens = media_norm.split()
    if rel_tokens and rel_tokens[0] in _LEADING_ARTICLES:
        rel_tokens = rel_tokens[1:]
    if media_tokens and media_tokens[0] in _LEADING_ARTICLES:
        media_tokens = media_tokens[1:]
    if not rel_tokens or not media_tokens:
        return False
    if rel_tokens[:len(media_tokens)] != media_tokens:
        return False
    if media_year is None:
        m = _BARE_YEAR_RE.search(media_raw)
        if m:
            media_year = int(re.search(r'\d{4}', m.group(0)).group(0))
    if media_year:
        try:
            year = int(media_year)
        except (TypeError, ValueError):
            return True
        # Years that are part of the title itself (e.g. "1917") don't
        # count as release-year evidence; ±1 tolerance for release-date
        # vs production-year tagging.
        years = [int(y) for y in _ANY_YEAR_RE.findall(raw_release)
                 if y not in media_tokens]
        if years and all(abs(y - year) > 1 for y in years):
            return False
    return True


def _extract_tmdb_entry_year(entry):
    """Pull a 4-digit year from a TMDB cache entry. Movies use
    ``release_date``, shows use ``first_air_date``. Returns int or None.
    """
    for key in ('release_date', 'first_air_date'):
        date = entry.get(key, '') or ''
        if isinstance(date, str) and len(date) >= 4 and date[:4].isdigit():
            return int(date[:4])
    return None


def _find_canonical_tmdb_via_prefix(parsed_title, parsed_year, is_tv,
                                    _tmdb_cache=None):
    """Token-aligned prefix lookup against the TMDB cache.

    Recovers the canonical TMDB entry when the parsed title contains
    extra tokens (actor names, genre tags) appended before the year —
    e.g.  parsed ``"Gattaca Ethan Hawke Sci Fi"`` + 1997 resolves to the
    cache entry for ``"Gattaca"``.  Complements the existing direct
    ``cached_tmdb_*.get(_norm)`` cascade step which only does an exact
    key match.

    Strict to limit false positives:
    - Token-aligned prefix on the cache key (release names start with
      the title; we don't accept mid-string matches).
    - Year confirmation when both sides have a year.
    - Single-token candidates require year confirmation (defends
      against cache entries titled "The" / "It" prefixing every release).
    - Multi-token candidates fail-open on missing entry-year (legacy
      entries lack ``release_date`` / ``first_air_date``).
    - Longest-prefix wins.

    Args:
        parsed_title: parsed-from-folder title string.
        parsed_year: parsed year int, or None.
        is_tv: True for shows, False for movies.
        _tmdb_cache: optional pre-loaded cache dict (for tests).

    Returns:
        Dict with ``title`` (canonical str) and ``tmdb_id`` (int) on hit,
        or None.  Safe under all error paths — never raises.
    """
    if not parsed_title:
        return None
    if _tmdb_cache is None:
        try:
            from utils import tmdb as _tmdb
            with _tmdb._cache_lock:
                _tmdb_cache = _tmdb._load_cache()
        except Exception as e:
            logger.debug("[library] canonical lookup cache load failed: %s", e)
            return None

    section_key = 'shows' if is_tv else 'movies'
    section = _tmdb_cache.get(section_key, {})
    if not section or not isinstance(section, dict):
        return None

    parsed_tokens = _norm_for_matching(parsed_title).split()
    if not parsed_tokens:
        return None

    best = None
    best_token_count = 0
    for cache_key, entry in section.items():
        try:
            if not isinstance(entry, dict) or not isinstance(cache_key, str):
                continue
            bare_key = _BARE_YEAR_RE.sub('', cache_key)
            candidate_tokens = _norm_for_matching(bare_key).split()
            if not candidate_tokens or len(candidate_tokens) > len(parsed_tokens):
                continue
            if parsed_tokens[:len(candidate_tokens)] != candidate_tokens:
                continue
            # Single-word cache title prefixing a multi-word parse:
            # demand year confirmation (fail-closed).
            if len(candidate_tokens) == 1 and len(parsed_tokens) > 1:
                if parsed_year is None:
                    continue
                entry_year = _extract_tmdb_entry_year(entry)
                if entry_year != parsed_year:
                    continue
            elif parsed_year is not None:
                # Multi-token candidate: fail-open on missing entry year.
                entry_year = _extract_tmdb_entry_year(entry)
                if entry_year is not None and entry_year != parsed_year:
                    continue
            if len(candidate_tokens) > best_token_count:
                title_str = entry.get('title')
                tmdb_id = entry.get('tmdb_id')
                if (isinstance(title_str, str) and title_str.strip()
                        and tmdb_id):
                    best = {'title': title_str.strip(), 'tmdb_id': tmdb_id}
                    best_token_count = len(candidate_tokens)
        except Exception as e:
            logger.debug("[library] skipping malformed cache entry %r: %s",
                         cache_key, e)
            continue
    return best


def _match_arr_entry(title, year, parsed_title, arr_map, arr_map_norm,
                     arr_by_tmdb, cached_tmdb, is_tv, max_season=0,
                     _tmdb_cache=None):
    """Resolve a library title to an arr library entry (Sonarr/Radarr).

    The single implementation of the title-match cascade documented in
    CLAUDE.md — used by movie/show symlink dir selection and movie/show
    post-symlink rescan triggers.  Order:

    1. Year-qualified exact match — disambiguates same-title
       different-year entries (e.g. "The Bridge" 2011 vs 2013).
    2. Bare exact match, then fuzzy ``_norm_for_matching`` match.
    3. TMDB-cache key lookup.  The cache is keyed by the parsed-folder
       norm — callers pass ``parsed_title`` (preserved when the display
       title was upgraded to canonical) so renamed items still resolve.
       For TV, a season-aware sub-step uses the item's max season to
       disambiguate reboots/revivals sharing a title.
    4. Token-aligned prefix match against the TMDB cache — recovers
       parsed titles with appended actor/genre tokens (e.g. "Gattaca
       Ethan Hawke Sci Fi" → "Gattaca").

    Args:
        title: display title of the library item.
        year: item year int, or None.
        parsed_title: parsed-from-folder title (falls back to ``title``
            when falsy).
        arr_map: {lowercase title: arr info} exact-match map.
        arr_map_norm: {_norm_for_matching key: arr info} fuzzy map.
        arr_by_tmdb: {tmdb_id: arr info} map.
        cached_tmdb: {normalized title: tmdb_id} section from
            ``get_cached_tmdb_ids()`` (movies or shows).
        is_tv: True for shows, False for movies.
        max_season: (TV only) max season number for the season-aware
            fallback; 0/None skips that sub-step.
        _tmdb_cache: pre-loaded full TMDB cache for the prefix-match
            step (avoids per-call disk reads).

    Returns the arr info dict, or None if nothing matched.
    """
    if year:
        arr_info = arr_map.get(f"{title} ({year})".lower())
        if arr_info:
            return arr_info
    arr_info = arr_map.get(title.lower()) or arr_map_norm.get(_norm_for_matching(title))
    if arr_info:
        return arr_info

    parsed = parsed_title or title
    norm = _normalize_title(parsed)
    tmdb_id = (cached_tmdb.get(f"{norm} ({year})") if year else None) or cached_tmdb.get(norm)
    if tmdb_id:
        arr_info = arr_by_tmdb.get(tmdb_id)
        if arr_info:
            return arr_info
    if is_tv and max_season:
        from utils.tmdb import find_show_tmdb_id_by_season
        alt_id = find_show_tmdb_id_by_season(norm, max_season, year)
        if alt_id and alt_id != tmdb_id:
            arr_info = arr_by_tmdb.get(alt_id)
            if arr_info:
                return arr_info

    canonical = _find_canonical_tmdb_via_prefix(
        parsed, year, is_tv=is_tv, _tmdb_cache=_tmdb_cache,
    )
    if canonical:
        return arr_by_tmdb.get(canonical['tmdb_id'])
    return None


# Public aliases for cross-module reuse (e.g., debrid_client title matching)
parse_folder_name = _parse_folder_name
normalize_title = _normalize_title
norm_for_matching = _norm_for_matching
release_matches_title = _release_matches_title


def _build_tmdb_aliases():
    """Build alias maps from TMDB cache for title cross-referencing.

    When different sources use different names for the same title
    (e.g. debrid "Star Wars Andor" vs Sonarr "Andor"), both resolve
    to the same TMDB ID in the cache.  This reads the cache (no API
    calls) and returns mappings so the merge phase can match them.

    Returns (show_aliases, movie_aliases) where each is a dict of
    {normalized_title: set of other normalized_titles with same TMDB ID}.
    """
    try:
        from utils.tmdb import get_cached_tmdb_ids
    except ImportError:
        return {}, {}

    try:
        cached_ids = get_cached_tmdb_ids()
    except Exception as e:
        logger.debug(f"[library] TMDB alias cache load failed, skipping: {e}")
        return {}, {}

    def _aliases_for_section(section):
        id_to_titles = {}
        for norm_title, tmdb_id in section.items():
            id_to_titles.setdefault(tmdb_id, set()).add(norm_title)
        aliases = {}
        for titles in id_to_titles.values():
            if len(titles) > 1:
                for t in titles:
                    aliases[t] = titles - {t}
        return aliases

    return (
        _aliases_for_section(cached_ids.get('shows', {})),
        _aliases_for_section(cached_ids.get('movies', {})),
    )


class _WebDAVUnsupportedError(RuntimeError):
    """Zurg does not honor recursive PROPFIND.

    Raised either on first detection (folders returned with no files) or on
    subsequent scans once `_webdav_unsupported` has been memoized, so the
    caller can log the memoized case at DEBUG instead of repeating INFO.
    """


# Re-validate the persisted Zurg capability flag after this many seconds.
# A new Zurg release could add recursive PROPFIND support, so we don't
# want a single detection to permanently block the WebDAV path.
_WEBDAV_CAPABILITY_TTL_S = 7 * 24 * 3600

# Hard upper bound on the capability cache file size.  The legitimate
# document is a fixed-shape JSON object well under 1 KB; anything larger
# is a tampered or corrupted file we refuse to parse.
_WEBDAV_CAPABILITY_MAX_BYTES = 4096


# Schema version for the persisted library cache (movies/shows + path
# indexes).  Bump on incompatible field renames so an upgrade
# automatically discards stale on-disk caches instead of mis-loading
# them; in-memory state is unaffected.
_LIBRARY_CACHE_SCHEMA = 1

# Hard upper bound on the persisted library cache file size.  At ~1840
# items with full metadata, expected payloads are 5–10 MB; the cap is
# generous enough for libraries 2–3× larger before the scanner refuses
# to load.
_LIBRARY_CACHE_MAX_BYTES = 16 * 1024 * 1024

# A persisted cache whose `ts` is more than this many seconds in the
# future is rejected as clock-skew / tampering.  Mirrors the capability
# cache's posture; one day is loose enough to absorb container clock
# drift without letting a forged record permanently shadow a real scan.
_LIBRARY_CACHE_FUTURE_TS_TOLERANCE_S = 86400


def _strict_int(x):
    """True only for canonical Python ints, not bools.

    ``bool`` is a subclass of ``int`` in Python, so a hand-edited cache
    with ``"season": true`` would pass a naive ``isinstance(x, int)``
    check.  Used by the strict cache loader to reject such values.
    """
    return isinstance(x, int) and not isinstance(x, bool)


def _strict_number(x):
    """True only for canonical numeric types, not bools."""
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def _serialize_cache_state(cache, path_index, local_path_index, alias_norms):
    """Build the schema-1 envelope that ``library_cache.json`` stores.

    Tuple-keyed indexes serialize as lists of ``[norm, sn, en, path]``
    rows because JSON object keys must be strings.  Alias sets become
    sorted lists for deterministic round-trip.
    """
    from version import VERSION as _VERSION
    return {
        'schema': _LIBRARY_CACHE_SCHEMA,
        'ts': time.time(),
        'zurgarr_version': _VERSION,
        'cache': cache,
        'path_index': [
            [norm, sn, en, p] for (norm, sn, en), p in path_index.items()
        ],
        'local_path_index': [
            [norm, sn, en, p] for (norm, sn, en), p in local_path_index.items()
        ],
        'alias_norms': {k: sorted(v) for k, v in alias_norms.items()},
    }


def _deserialize_cache_state(envelope):
    """Validate and convert the persisted envelope back to live state.

    Returns ``(cache, path_index, local_path_index, alias_norms)`` on
    success or ``None`` on any failure.  Strict-types throughout: any
    field of the wrong shape — including the ``bool``-as-``int`` trap —
    rejects the whole envelope.
    """
    from version import VERSION as _VERSION

    if not isinstance(envelope, dict):
        return None
    # ``schema`` must be a strict int (rejects ``bool`` as well — see
    # ``_strict_int``) that equals the current schema.
    schema = envelope.get('schema')
    if not _strict_int(schema) or schema != _LIBRARY_CACHE_SCHEMA:
        return None
    ts = envelope.get('ts')
    if not _strict_number(ts):
        return None
    if ts > time.time() + _LIBRARY_CACHE_FUTURE_TS_TOLERANCE_S:
        return None
    if envelope.get('zurgarr_version') != _VERSION:
        return None

    cache = envelope.get('cache')
    if not isinstance(cache, dict):
        return None
    if not isinstance(cache.get('movies'), list):
        return None
    if not isinstance(cache.get('shows'), list):
        return None
    # Inner cache fields used by downstream consumers — strict-validate
    # so a tampered file can't silently feed wrong types into the UI
    # render path or scan-effects loop.  ``preferences`` is allowed to
    # be missing (some old envelopes won't have it); when present it
    # must be a dict.
    if 'preferences' in cache and not isinstance(cache['preferences'], dict):
        return None
    if not isinstance(cache.get('last_scan'), str):
        return None
    if not _strict_int(cache.get('scan_duration_ms')):
        return None
    # ``arr_degraded`` is a per-scan runtime signal for the recovery
    # snapshot writer — a warm-started payload must never replay a
    # previous run's degradation flag.
    cache.pop('arr_degraded', None)

    raw_pi = envelope.get('path_index')
    raw_lpi = envelope.get('local_path_index')
    raw_an = envelope.get('alias_norms')
    if not isinstance(raw_pi, list) or not isinstance(raw_lpi, list):
        return None
    if not isinstance(raw_an, dict):
        return None

    def _to_index(rows):
        out = {}
        for row in rows:
            if not isinstance(row, list) or len(row) != 4:
                return None
            norm, sn, en, p = row
            if not isinstance(norm, str) or not isinstance(p, str):
                return None
            if not _strict_int(sn) or not _strict_int(en):
                return None
            out[(norm, sn, en)] = p
        return out

    path_index = _to_index(raw_pi)
    if path_index is None:
        return None
    local_path_index = _to_index(raw_lpi)
    if local_path_index is None:
        return None

    alias_norms = {}
    for k, v in raw_an.items():
        if not isinstance(k, str) or not isinstance(v, list):
            return None
        if not all(isinstance(x, str) for x in v):
            return None
        alias_norms[k] = set(v)

    return cache, path_index, local_path_index, alias_norms


class LibraryScanner:
    # Guards the three Wanted-recovery memo dicts (_wanted_tb_cooldown,
    # _wanted_rd_miss, _wanted_no_results).  The scan-effects thread owns all
    # writes during a pass; HTTP threads read snapshots (/api/stuck) and clear
    # keys (retry action).  Bare membership checks on the effects thread stay
    # lock-free (GIL-atomic, no iteration); anything that iterates, copies,
    # rebinds, or mutates must hold this.  Class-level so test instances built
    # via __new__ (bypassing __init__) still have it.
    _wanted_memo_lock = threading.Lock()

    def __init__(self):
        self._mount_path = _discover_mount()
        self._local_movies_path = os.environ.get('BLACKHOLE_LOCAL_LIBRARY_MOVIES', '').strip() or None
        self._local_tv_path = os.environ.get('BLACKHOLE_LOCAL_LIBRARY_TV', '').strip() or None
        self._cache = None
        self._cache_time = 0
        self._ttl = 600
        # Set by _scan_mount when a walk is truncated (deadline hit or the
        # listing raised — e.g. a TorBox 429). _scan_read reads it right after
        # the (synchronous) TB scan to decide whether to fall back to the
        # last-known-good TB set instead of overwriting with partial data.
        self._last_scan_mount_truncated = False
        # Last COMPLETE TorBox scan results, kept in memory so an incomplete
        # scan doesn't drop TB titles to "Wanted". In-memory only — lost on
        # restart, repopulated by the first complete scan.
        self._last_tb_movies = None
        self._last_tb_shows = None
        self._lock = threading.Lock()
        self._scanning = False
        self._effects_running = False
        self._path_index = {}
        self._local_path_index = {}
        self._path_lock = threading.Lock()
        # {(norm, sn): timestamp} — suppress re-search for 1 hour.  No lock:
        # only touched from _search_for_missing_episodes, whose sole caller
        # (_scan_effects) is serialized by _effects_running under self._lock.
        self._search_cooldown = {}
        # {imdb_or_imdb:s:e: monotonic ts} — suppress re-probing a Wanted ghost
        # against Torrentio/TorBox for _WANTED_TB_RECOVERY_COOLDOWN after a miss
        # or a grab, so a deep backlog isn't re-walked every scan.
        self._wanted_tb_cooldown = {}
        # {imdb_or_imdb:s:e: monotonic ts} — titles whose RD probe-add came
        # back uncached (or filter-blocked).  Longer-lived than the TB
        # cooldown (_WANTED_RD_MISS_TTL) so the same title isn't add/delete
        # churned against RD every few hours; a miss usually means RD lacks
        # the content family, not just that one release.
        self._wanted_rd_miss = {}
        # {imdb_or_imdb:s:e: monotonic ts} — Torrentio returned no usable
        # results for the title.  Leg-independent (nothing for either leg to
        # add), so it gates the pass before the per-leg memos; expires on the
        # short TB cooldown TTL so newly-released content isn't held back.
        self._wanted_no_results = {}
        # Rehydrate the three memo dicts from the last persisted snapshot so
        # a container restart doesn't re-probe the whole Wanted backlog
        # through Torrentio + TB checkcached (observed live: a restart mid-
        # drain re-ground all ~135 titles, burning the very TB create budget
        # the memos exist to protect).
        self._load_wanted_memos()
        self._alias_norms = {}     # {norm_title: set of alias norm_titles}
        try:
            self._debrid_unavailable_days = int(os.environ.get('DEBRID_UNAVAILABLE_THRESHOLD_DAYS', '3'))
        except (ValueError, TypeError):
            self._debrid_unavailable_days = 3
        try:
            self._force_grab_max_attempts = int(os.environ.get('FORCE_GRAB_MAX_ATTEMPTS', '12'))
        except (ValueError, TypeError):
            self._force_grab_max_attempts = 12
        try:
            self._pending_warning_hours = int(os.environ.get('PENDING_WARNING_HOURS', '24'))
        except (ValueError, TypeError):
            self._pending_warning_hours = 24
        self._last_had_local = None    # None=unknown, True=had local content
        self._local_drop_alerted = False

        # Memoized capability: once we detect that Zurg does not honor
        # `Depth: infinity`, skip the doomed PROPFIND on every subsequent
        # scan and go straight to FUSE.  `_logged` tracks whether the
        # detection message has already been emitted at INFO so subsequent
        # fallbacks don't spam the log every cache TTL.  The flag is
        # persisted to `library_capabilities.json` so a container restart
        # doesn't re-pay the doomed PROPFIND cost on every cold scan; the
        # cache is re-validated after 7 days and on ZURG_VERSION change.
        self._webdav_unsupported = False
        self._webdav_unsupported_logged = False
        self._capabilities_path = os.path.join(
            os.environ.get('CONFIG_DIR', '/config'),
            'library_capabilities.json',
        )
        self._load_webdav_capability()

        # Persisted snapshot of the last successful scan — populated on
        # every scan completion and reloaded on startup so the Library
        # page renders the last-known-good list immediately instead of
        # waiting on a cold scan (51s on the user's instance, where
        # Zurg lacks recursive PROPFIND so the FUSE walk is the only
        # path).  Strict validation; any failure mode falls back to
        # current behavior (fresh scan).
        self._library_cache_path = os.path.join(
            os.environ.get('CONFIG_DIR', '/config'),
            'library_cache.json',
        )
        self._load_persisted_cache()

        # Per-title basename sets of previously-created debrid symlinks.
        # Used to classify new symlink_created events as "new import" vs
        # "upgrade (replaced prior file)" vs "state_init" (first scan after
        # restart, can't tell).  Persisted across restarts so a container
        # restart doesn't mis-label every existing symlink as an upgrade.
        self._scan_state_path = os.path.join(
            os.environ.get('CONFIG_DIR', '/config'),
            'library_scan_state.json',
        )
        self._last_symlinked_files = {}   # title -> set(basename)
        self._state_was_bootstrapped = not os.path.isfile(self._scan_state_path)
        if not self._state_was_bootstrapped:
            # Guard against a tampered/corrupt state file — the loader must
            # never take the scanner offline.  A 10 MB cap prevents a
            # malicious file from swallowing RAM; on any validation miss we
            # wipe state and treat the next scan as a fresh bootstrap.
            try:
                import json as _json
                try:
                    if os.path.getsize(self._scan_state_path) > 10 * 1024 * 1024:
                        raise ValueError('scan state file too large')
                except OSError:
                    pass
                with open(self._scan_state_path, 'r', encoding='utf-8') as fh:
                    raw = _json.load(fh)
                if not isinstance(raw, dict):
                    raise ValueError('scan state root is not an object')
                titles = raw.get('titles', {})
                if not isinstance(titles, dict):
                    raise ValueError('scan state "titles" is not an object')
                self._last_symlinked_files = {
                    t: {b for b in v if isinstance(b, str)}
                    for t, v in titles.items()
                    if isinstance(t, str) and isinstance(v, list)
                }
            except (OSError, ValueError, TypeError, AttributeError) as e:
                logger.warning(f"[library] Could not load scan state: {e}")
                self._last_symlinked_files = {}
                self._state_was_bootstrapped = True

        if self._mount_path:
            logger.info(f"[library] Mount path: {self._mount_path}")
        else:
            logger.warning("[library] No rclone mount discovered; debrid library will be empty")

        if self._local_movies_path:
            logger.info(f"[library] Local movies: {self._local_movies_path}")
        if self._local_tv_path:
            logger.info(f"[library] Local TV: {self._local_tv_path}")

    def is_scanning(self):
        with self._lock:
            return self._scanning

    def get_cached_stats(self):
        """Return composition stats from the current cache, or ``None``.

        Non-blocking and never triggers a scan — the status dashboard
        polls /api/status every few seconds and must not stall waiting
        on a fresh library scan.

        Snapshots the top-level ``movies``/``shows`` lists under
        ``_lock`` because ``_scan_effects._cleanup_disc_rips`` does an
        in-place ``movies[:] = [...]`` slice-assignment AFTER the cache
        has been published to readers (refresh() publishes before
        running effects).  Iterating that list without a snapshot can
        skip or double-count items mid-cleanup.
        """
        with self._lock:
            cache = self._cache
            if not cache:
                return None
            snapshot = {
                'movies': list(cache.get('movies') or []),
                'shows': list(cache.get('shows') or []),
                'last_scan': cache.get('last_scan'),
                'scan_duration_ms': cache.get('scan_duration_ms'),
            }
        try:
            return compute_library_stats(snapshot)
        except Exception:
            return None

    def _get_pref(self, norm, preferences):
        """Look up a preference by normalized title, checking aliases if needed."""
        pref = preferences.get(norm)
        if not pref:
            for alias in self._alias_norms.get(norm, ()):
                pref = preferences.get(alias)
                if pref:
                    break
        return pref

    def _route_for(self, norm, preferences):
        """Map a stored preference to an ``ensure_and_search`` route selector.

        Returns True for prefer-debrid (force-grab debrid copies),
        False for prefer-local, and None when no preference is set
        (gap-fill only — Sonarr's own routing tag decides the destination).
        """
        pref = self._get_pref(norm, preferences)
        if pref == 'prefer-debrid':
            return True
        if pref == 'prefer-local':
            return False
        return None

    def _compute_missing_episodes(self, show):
        """Return ``[(season, episode), ...]`` of aired TMDB episodes that the
        scan did not find under any source.

        Defends the "Lucky Hank E04" user story: an aired episode the TMDB
        cache expects but neither debrid nor local holds a file for.  Specials
        (season 0) and unaired episodes are excluded at the TMDB helper.
        Seasons listed in ``show['unmonitored_seasons']`` are also excluded
        — otherwise gap-fill would fan out a search per season only for
        ``ensure_and_search`` to short-circuit with "all unmonitored",
        which costs one Sonarr round-trip per skipped season.

        Returns ``[]`` when TMDB has no cached episode list for the show —
        an empty return must be treated as "don't know" so we never trigger
        a spurious search for a title we lack ground truth on.
        """
        from utils.tmdb import get_cached_episode_list
        norm = _normalize_title(show.get('title', ''))
        expected = get_cached_episode_list(norm, show.get('year'))
        if not expected:
            return []
        unmonitored = set(show.get('unmonitored_seasons') or ())
        present = set()
        for sd in show.get('season_data', []):
            sn = sd.get('number')
            if not sn:
                continue
            for ep in sd.get('episodes', []):
                en = ep.get('number')
                if en and ep.get('source') in ('debrid', 'local', 'both'):
                    present.add((sn, en))
        missing = []
        for ep in expected:
            if ep['season'] in unmonitored:
                continue
            key = (ep['season'], ep['number'])
            if key not in present:
                missing.append(key)
        return missing

    @staticmethod
    def _discover_torbox_mount():
        """Return the TorBox rclone-mount path inside the container, or
        ``None`` when TB isn't configured / hasn't come up yet.

        Plan 39 phase 4 — defers to ``utils.debrid_routing.mount_for_debrid``
        for the path computation so the per-debrid mount contract lives in
        one place (also used by blackhole.py and debrid_health.py).  The
        ``isdir`` check protects against the partial-config case where
        ``TORBOX_API_KEY`` is set but ``TORBOX_WEBDAV_USER/PASS`` is not
        — the mount doesn't come up, so scanning would just walk an empty
        bind-only dir.  Returning None there avoids logging confusing
        "no items on TB mount" messages.
        """
        if not os.environ.get('TORBOX_API_KEY'):
            return None
        try:
            from utils.debrid_routing import mount_for_debrid, TORBOX
        except Exception:
            return None
        path = mount_for_debrid(TORBOX)
        # Must be both a directory AND have at least one entry to be a
        # real mount (an empty bind dir would have no entries; a live
        # FUSE mount surfaces TB's category folders even when the
        # account has zero torrents — they appear as empty dirs).
        if not path or not os.path.isdir(path):
            return None
        try:
            # Accept the mount if listdir succeeds (even if empty).  We
            # don't require content — an empty TB account is a valid
            # state, just yields zero library items.
            os.listdir(path)
        except OSError:
            return None
        return path

    @staticmethod
    def _normalize_episodes_for_merge(eps):
        """Coerce an ``_episodes`` value into the dict-of-info-dicts shape.

        ``_scan_mount`` emits the dict shape (see library.py:4543).  The
        WebDAV path omits the field, and a few legacy tests still feed a
        list of ``(season, ep)`` tuples — both must merge cleanly into
        the dict-form output that downstream consumers expect.
        """
        if isinstance(eps, dict):
            return dict(eps)
        if not eps:
            return {}
        out = {}
        for e in eps:
            key = tuple(e) if isinstance(e, list) else e
            if isinstance(key, tuple) and len(key) == 2:
                out[key] = {}
        return out

    @staticmethod
    def _union_tb_items(last_good, partial):
        """Union two TorBox item lists, keyed by normalized title.

        Used to recover from an incomplete TB scan: ``last_good`` is the most
        recent COMPLETE scan, ``partial`` is the truncated current scan.
        Partial entries win on collision (fresh data), and titles present
        only in ``last_good`` are carried over so a rate-limited walk doesn't
        drop them to "Wanted". Movies and shows are unioned separately by the
        caller, so there's no cross-type key collision.

        Keyed by ``_normalize_title`` to match the keying ``_scan_mount`` and
        ``_merge_alt_debrid_items`` already use — same-normalized titles that
        differ only by year (e.g. ``Dune (1984)`` vs ``Dune (2021)``) already
        collapse to one entry inside a single scan, so the union introduces no
        new title-loss beyond that pre-existing pipeline limitation.
        ``title`` is coerced via ``or ''`` so a None value can't raise.
        """
        by_key = {}
        for it in (last_good or []):
            by_key[_normalize_title(it.get('title') or '')] = it
        for it in (partial or []):
            by_key[_normalize_title(it.get('title') or '')] = it
        return list(by_key.values())

    @staticmethod
    def _merge_alt_debrid_items(primary_movies, primary_shows,
                                alt_movies, alt_shows):
        """Merge alt-debrid items into the primary lists.

        Plan 39 phase 4 — items unique to the alt debrid are appended.
        Items present on BOTH debrids (matched by normalized title) keep
        the primary entry but gain ``has_alt_source=True`` and
        ``alt_source_debrid=<alt name>`` so the UI can render a
        "RD + TB" pair-badge instead of two cards.

        Match is by ``_normalize_title`` only — the same key the rest of
        the merge pipeline uses.  Year-aware tie-break could land later;
        for the MVP, same-title-different-year (e.g. ``Dune (1984)`` vs
        ``Dune (2021)``) collapses into one card if both mounts have
        both.  That's a known limitation; in practice the cache rarely
        holds two films of the same title on the same provider.
        """
        if not (alt_movies or alt_shows):
            return primary_movies, primary_shows

        def _key(item):
            return _normalize_title(item.get('title', ''))

        primary_movie_keys = {_key(m): m for m in primary_movies}
        primary_show_keys = {_key(s): s for s in primary_shows}

        for it in alt_movies:
            k = _key(it)
            if k and k in primary_movie_keys:
                primary_movie_keys[k]['has_alt_source'] = True
                primary_movie_keys[k]['alt_source_debrid'] = it.get('source_debrid')
            else:
                primary_movies.append(it)
                if k:
                    primary_movie_keys[k] = it

        for it in alt_shows:
            k = _key(it)
            if k and k in primary_show_keys:
                primary_show_keys[k]['has_alt_source'] = True
                primary_show_keys[k]['alt_source_debrid'] = it.get('source_debrid')
                # Merge episode sets so the season/episode counts reflect
                # the union — TB might have episodes RD doesn't.
                #
                # ``_scan_mount`` (library.py:4543) produces ``_episodes``
                # as a dict keyed by ``(season, ep)`` with episode-info
                # dict values; the WebDAV scan path leaves the field
                # absent.  We also defensively handle the legacy
                # list-of-tuples shape that earlier tests pinned, so a
                # mixed-shape merge (one side dict, one side list) still
                # works — but always emit the dict shape, since the rest
                # of the pipeline (e.g. ``_dedup_by_tmdb``,
                # episode-level cross-ref) expects dict keys + info
                # values.  Primary wins on dupes so the canonical path /
                # source-debrid badge stays consistent.
                p_eps = primary_show_keys[k].get('_episodes')
                a_eps = it.get('_episodes')

                merged_eps = LibraryScanner._normalize_episodes_for_merge(p_eps)
                a_norm = LibraryScanner._normalize_episodes_for_merge(a_eps)
                if a_norm:
                    for ek, ev in a_norm.items():
                        merged_eps.setdefault(ek, ev)
                    primary_show_keys[k]['_episodes'] = merged_eps
                    unique_seasons = {s for s, _e in merged_eps} if merged_eps else set()
                    primary_show_keys[k]['seasons'] = len(unique_seasons)
                    primary_show_keys[k]['episodes'] = len(merged_eps)
            else:
                primary_shows.append(it)
                if k:
                    primary_show_keys[k] = it

        return primary_movies, primary_shows

    @staticmethod
    def _dedup_by_tmdb(items, aliases):
        """Merge items that share a TMDB ID but have different normalized titles.

        Torrents on the debrid mount may use different names for the same
        show (e.g. "Andor" vs "Star Wars Andor").  _scan_mount groups by
        normalized title, so they end up as separate entries.  This merges
        them using the TMDB alias map, combining episodes and preferring
        the title with a year or better capitalization.
        """
        if not aliases:
            logger.debug("[library] TMDB alias map empty, skipping debrid dedup")
            return items

        # Map each norm key to its canonical (first-seen) key via aliases
        canon = {}  # norm_key -> canonical norm_key
        for item in items:
            key = _normalize_title(item['title'])
            if key in canon:
                continue
            # Check if any existing canonical key is an alias of this key
            for alias in sorted(aliases.get(key, ())):
                if alias in canon:
                    canon[key] = canon[alias]
                    break
            if key not in canon:
                canon[key] = key

        # Group items by canonical key
        groups = {}  # canonical_key -> list of items
        for item in items:
            key = _normalize_title(item['title'])
            ckey = canon[key]
            groups.setdefault(ckey, []).append(item)

        # Merge each group
        result = []
        for ckey, group in groups.items():
            if len(group) == 1:
                result.append(group[0])
                continue

            # Pick the best title: prefer one with a year, then better caps
            best = group[0]
            for item in group[1:]:
                if item.get('year') and not best.get('year'):
                    best = item
                elif not best.get('year'):
                    if item['title'][0:1].isupper() and not best['title'][0:1].isupper():
                        best = item

            merged = dict(best)

            # Use earliest date_added from the group (skip 0 = stat failure)
            dates = [item.get('date_added', 0) for item in group if item.get('date_added', 0) > 0]
            if dates:
                merged['date_added'] = min(dates)

            # Merge episodes from all items in the group (shows only)
            if any(item.get('_episodes') for item in group):
                merged_eps = dict(merged.get('_episodes', {}))
                for item in group:
                    if item is best:
                        continue
                    for ep_key, ep_info in item.get('_episodes', {}).items():
                        if ep_key not in merged_eps:
                            merged_eps[ep_key] = ep_info
                        elif ep_info.get('_folder_ep_count', 1) > merged_eps[ep_key].get('_folder_ep_count', 1):
                            merged_eps[ep_key] = ep_info
                merged['_episodes'] = merged_eps
                merged['seasons'] = len({ek[0] for ek in merged_eps})
                merged['episodes'] = len(merged_eps)

            merged_key = _normalize_title(merged['title'])
            for item in group:
                item_key = _normalize_title(item['title'])
                if item_key != merged_key:
                    logger.debug(
                        f"[library] TMDB dedup (debrid): '{item_key}' merged into '{merged_key}'"
                    )

            result.append(merged)

        return result

    def _scan_read(self):
        """Read-only scan: enumerate mount + local, merge, build indexes.

        Returns the library data dict without running any side effects
        (no preference enforcement, debrid searches, or symlink creation).
        """
        start = time.monotonic()
        deadline = start + 30

        # Deferred mount discovery — status_server.setup() creates the scanner
        # before Zurg/rclone start, so the mount may not exist yet.
        if not self._mount_path:
            self._mount_path = _discover_mount()
            if self._mount_path:
                logger.info(f"[library] Mount path discovered (deferred): {self._mount_path}")

        debrid_movies = []
        debrid_shows = []

        if self._mount_path:
            # Try WebDAV PROPFIND directly to Zurg (bypasses FUSE/rclone)
            try:
                debrid_movies, debrid_shows = self._webdav_scan_mount(deadline)
                # Source-tag every item so the UI badge logic below can
                # distinguish RD/AD content from TB content.  WebDAV scan
                # paths don't go through _scan_mount, so the tagging
                # needs to happen here too.  Resolve the primary debrid
                # for the badge rather than hard-coding 'realdebrid' —
                # AD-only or TB-only setups need the correct provider tag.
                from utils.debrid_routing import resolve_primary
                primary_badge = resolve_primary() or 'realdebrid'
                for it in debrid_movies + debrid_shows:
                    it.setdefault('source_debrid', primary_badge)
                logger.debug("[library] WebDAV scan succeeded")
            except Exception as e:
                # Quiet down the recurring "using FUSE" log once the
                # capability is memoized — first detection at INFO, every
                # subsequent scan at DEBUG.  Other transient WebDAV failures
                # still log at INFO so they remain visible.
                is_unsupported = isinstance(e, _WebDAVUnsupportedError)
                if is_unsupported and self._webdav_unsupported_logged:
                    logger.debug(f"[library] WebDAV scan unavailable, using FUSE: {e}")
                else:
                    logger.info(f"[library] WebDAV scan unavailable, using FUSE: {e}")
                    if is_unsupported:
                        self._webdav_unsupported_logged = True
                try:
                    from utils.rclone_rc import refresh_dir
                    from base import TORBOX_MOUNT_NAME
                    # Skip the TorBox mount: it's enumerated below via the
                    # mylist API, so a recursive PROPFIND walk over it here is
                    # pure collateral and trips WebDAV listing rate-limits.
                    refresh_dir('', recursive=True,
                                exclude_mounts={TORBOX_MOUNT_NAME})
                except Exception as e:
                    logger.debug(f"[library] RC refresh before FUSE scan failed: {e}")
                debrid_movies, debrid_shows = self._scan_mount(self._mount_path, deadline)

        # Plan 39 phase 4 — second-pass scan against the TorBox mount.
        # Adds items that live on TB only AND flags items present on
        # both mounts with ``has_alt_source=True`` so the UI can render
        # "available on RD + TB" without duplicating the card.
        tb_mount = self._discover_torbox_mount()
        if tb_mount:
            # TB enumeration goes through the mylist API, not a FUSE walk.
            # The old per-folder scandir/stat walk over the 5-tps rclone
            # mount (~450 folders) generated 429 storms that contended with
            # real content downloads — occasionally abandoning an in-flight
            # download.  One mylist call returns the whole account with zero
            # FUSE ops.  The mount is still discovered above because it's
            # needed for symlink TARGETS (real file access), and the
            # synthesized item paths must match its layout.
            try:
                tb_movies, tb_shows = self._scan_torbox_via_api(tb_mount)
                tb_incomplete = self._last_scan_mount_truncated
            except Exception as e:
                logger.warning(f"[library] TB API scan failed: {e}")
                tb_movies, tb_shows, tb_incomplete = [], [], True

            # An incomplete TB walk (TorBox 429 / deadline) must not be
            # treated as the authoritative TB set — otherwise every truncated
            # scan drops the missing titles to "Wanted". Fall back to the last
            # COMPLETE scan, unioning the partial over it so newly-grabbed
            # content still appears. Only a complete scan becomes the new
            # baseline. Tradeoff: a title genuinely deleted from TB during a
            # run of truncated scans lingers as "available" until the next
            # complete scan re-promotes a fresh baseline — cheap next to
            # flipping the whole TB library to "Wanted" hourly.
            if tb_incomplete:
                if self._last_tb_movies is not None or self._last_tb_shows is not None:
                    tb_movies = self._union_tb_items(self._last_tb_movies or [], tb_movies)
                    tb_shows = self._union_tb_items(self._last_tb_shows or [], tb_shows)
                    logger.warning(
                        "[library] TB scan incomplete (rate-limited/timeout) — "
                        "merged partial over last-good TB set (%d movies, %d shows) "
                        "to avoid dropping titles to 'Wanted'",
                        len(tb_movies), len(tb_shows),
                    )
                else:
                    logger.warning(
                        "[library] TB scan incomplete and no prior good TB scan "
                        "to fall back on; using partial result (%d movies, %d shows)",
                        len(tb_movies), len(tb_shows),
                    )
            else:
                # Deep-copy: the same dicts flow into _merge_alt_debrid_items
                # (sets has_alt_source/alt_source_debrid in place) and the
                # downstream dedup/enrichment stages, all of which mutate
                # items. A shallow list() copy would let those stages silently
                # corrupt the baseline, so a later truncated scan would carry
                # over polluted entries. Snapshot independent copies instead.
                self._last_tb_movies = copy.deepcopy(tb_movies)
                self._last_tb_shows = copy.deepcopy(tb_shows)

            debrid_movies, debrid_shows = self._merge_alt_debrid_items(
                debrid_movies, debrid_shows, tb_movies, tb_shows,
            )
            logger.debug(
                f"[library] TB scan: {len(tb_movies)} movies, "
                f"{len(tb_shows)} shows from {tb_mount}"
            )

        # TMDB-based alias maps: when different sources (or different
        # torrents) use different names for the same title (e.g. "Star
        # Wars Andor" vs "Andor"), both resolve to the same TMDB ID in
        # the cache.  Alias maps let us merge them.
        show_aliases, movie_aliases = _build_tmdb_aliases()

        # Deduplicate debrid entries that share a TMDB ID but have
        # different parsed titles (e.g. "Andor" and "Star Wars Andor"
        # both on the debrid mount as separate torrent groups).
        debrid_shows = self._dedup_by_tmdb(debrid_shows, show_aliases)
        debrid_movies = self._dedup_by_tmdb(debrid_movies, movie_aliases)

        # Pre-load the full TMDB cache once for the canonical-prefix
        # fallback used in the title-level merge step (and the enrichment
        # step further down).  Without this, every parser-junk-bearing
        # debrid item that fails the direct + alias merge match would
        # re-read /config/tmdb_cache.json from disk via the resolver.
        try:
            from utils import tmdb as _tmdb_mod
            with _tmdb_mod._cache_lock:
                _full_tmdb_cache = _tmdb_mod._load_cache()
        except Exception as e:
            logger.debug("[library] full TMDB cache load for merge failed: %s", e)
            _full_tmdb_cache = {}

        local_movies = self._scan_local_movies()
        local_shows = self._scan_local_shows()

        # Build normalized title index for cross-referencing
        debrid_movie_keys = {_normalize_title(m['title']): m for m in debrid_movies}
        debrid_show_keys = {_normalize_title(s['title']): s for s in debrid_shows}

        local_movie_keys = {_normalize_title(lm['title']) for lm in local_movies}
        local_movie_map = {_normalize_title(lm['title']): lm for lm in local_movies}

        # Seed alias_norms with all known TMDB aliases so preference
        # lookups work regardless of which name was used.  Each name maps
        # to the set of all its aliases (handles 3+ title groups correctly).
        # Build the map locally and atomically rebind self._alias_norms at
        # the end of _scan_read — readers (request threads) iterate the set
        # values, so mutating sets in-place mid-scan would race.
        alias_norms_local = {}
        for all_aliases in (show_aliases, movie_aliases):
            seen = set()
            for norm_key, alias_set in all_aliases.items():
                if norm_key in seen:
                    continue
                group = alias_set | {norm_key}
                seen.update(group)
                for name in group:
                    alias_norms_local[name] = group - {name}

        movies = []
        merged_local_movie_keys = set()
        # Merge debrid + local movies (title-level)
        for item in debrid_movies:
            key = _normalize_title(item['title'])
            matched_key = None
            if key in local_movie_keys:
                matched_key = key
            else:
                for alias in sorted(movie_aliases.get(key, ())):
                    if alias in local_movie_keys:
                        matched_key = alias
                        break
            if matched_key is None:
                # Final fallback: token-aligned prefix lookup against the
                # TMDB cache.  Bridges parser-junk debrid titles (e.g.
                # "Gattaca Ethan Hawke Sci Fi") to their canonical local
                # counterpart ("Gattaca").  Without this, debrid releases
                # whose folder name carries actor/genre tokens before the
                # year stay split into a separate library item from their
                # local copy — and the user sees two posters.
                parsed_t = item.get('_parsed_title') or item.get('title') or ''
                yr = item.get('year')
                canonical = _find_canonical_tmdb_via_prefix(
                    parsed_t, yr, is_tv=False, _tmdb_cache=_full_tmdb_cache,
                )
                if canonical:
                    canon_key = _normalize_title(canonical['title'])
                    # Self-loop guard: canon_key == key means the parsed
                    # title is already canonical and any alias would be a
                    # degenerate self-edge polluting alias_norms_local.
                    if (canon_key and canon_key != key
                            and canon_key in local_movie_keys):
                        matched_key = canon_key
                        logger.debug(
                            f"[library] TMDB prefix match (movie): debrid '{key}' "
                            f"→ canonical '{canon_key}'"
                        )
            if matched_key is not None:
                if matched_key != key:
                    logger.debug(
                        f"[library] alias-merge (movie): debrid '{key}' ↔ local '{matched_key}'"
                    )
                    # Single registration site for both alias-match and
                    # prefix-match paths.  Set semantics make this idempotent
                    # across rescans.
                    alias_norms_local.setdefault(key, set()).add(matched_key)
                    alias_norms_local.setdefault(matched_key, set()).add(key)
                item = dict(item)
                item['source'] = 'both'
                # Use earliest date_added from either source
                local_movie = local_movie_map.get(matched_key)
                if local_movie:
                    if local_movie.get('date_added'):
                        item['date_added'] = min(item.get('date_added', 0), local_movie['date_added'])
                    if local_movie.get('path'):
                        item['local_path'] = local_movie['path']
                merged_local_movie_keys.add(matched_key)
            movies.append(item)

        for lm in local_movies:
            key = _normalize_title(lm['title'])
            if key not in debrid_movie_keys and key not in merged_local_movie_keys:
                movies.append(lm)

        # Inject ghost entries for Radarr-monitored movies that have no
        # file yet, so they surface in the Wanted view. Done after the
        # debrid+local merge so dedup keys see the full real-movie set,
        # and before TMDB enrichment so ghost entries also pick up
        # posters/imdb_ids. Silently no-ops when Radarr is unavailable.
        try:
            from utils.library_prefs import get_all_pending as _gap
            _pending_snapshot = _gap() or {}
        except Exception:
            _pending_snapshot = {}
        # Tokens accumulated when a configured arr's bulk-list fetch fails
        # mid-scan (DNS blip, restart race).  The recovery snapshot writer
        # skips degraded scans so a transient failure can't poison the
        # daily wanted/on-disk time series.
        arr_degraded = set()
        ghosts_added = _apply_radarr_wanted_movies(
            movies, pending=_pending_snapshot, degraded=arr_degraded)
        if ghosts_added:
            logger.debug(f"[library] Injected {ghosts_added} Radarr wanted movie(s) as ghost entries")

        # Merge debrid + local shows with episode-level cross-referencing
        local_show_map = {_normalize_title(ls['title']): ls for ls in local_shows}

        shows = []
        merged_local_show_keys = set()
        for item in debrid_shows:
            key = _normalize_title(item['title'])
            local_key = None
            if key in local_show_map:
                local_key = key
            else:
                for alias in sorted(show_aliases.get(key, ())):
                    if alias in local_show_map:
                        local_key = alias
                        break
            if local_key is None:
                # Final fallback: token-aligned prefix lookup against the
                # TMDB cache — symmetric with the movies merge cascade.
                parsed_t = item.get('_parsed_title') or item.get('title') or ''
                yr = item.get('year')
                canonical = _find_canonical_tmdb_via_prefix(
                    parsed_t, yr, is_tv=True, _tmdb_cache=_full_tmdb_cache,
                )
                if canonical:
                    canon_key = _normalize_title(canonical['title'])
                    # Self-loop guard — see movies branch comment.
                    if (canon_key and canon_key != key
                            and canon_key in local_show_map):
                        local_key = canon_key
                        logger.debug(
                            f"[library] TMDB prefix match (show): debrid '{key}' "
                            f"→ canonical '{canon_key}'"
                        )
            if local_key is not None:
                if local_key != key:
                    logger.debug(
                        f"[library] alias-merge (show): debrid '{key}' ↔ local '{local_key}'"
                    )
                    alias_norms_local.setdefault(key, set()).add(local_key)
                    alias_norms_local.setdefault(local_key, set()).add(key)
                merged_local_show_keys.add(local_key)
                item = dict(item)
                local_item = local_show_map[local_key]
                debrid_eps = item.get('_episodes', {})
                local_eps = local_item.get('_episodes', {})

                # Merge at episode level
                merged = {}
                for ek, info in debrid_eps.items():
                    if ek in local_eps:
                        merged[ek] = dict(info, source='both',
                                          local_path=local_eps[ek].get('path', ''))
                    else:
                        merged[ek] = dict(info, source='debrid')
                for ek, info in local_eps.items():
                    if ek not in debrid_eps:
                        merged[ek] = dict(info, source='local')

                item['_episodes'] = merged

                # Roll up show-level source from episode sources
                sources = {ep.get('source') for ep in merged.values()}
                if len(sources) > 1 or 'both' in sources:
                    item['source'] = 'both'
                elif 'local' in sources:
                    item['source'] = 'local'
                else:
                    item['source'] = 'debrid'

                # Update counts from merged episodes
                item['seasons'] = len({ek[0] for ek in merged})
                item['episodes'] = len(merged)
                # Use earliest date_added from either source
                if local_item.get('date_added'):
                    item['date_added'] = min(item.get('date_added', 0), local_item['date_added'])
            shows.append(item)

        for ls in local_shows:
            key = _normalize_title(ls['title'])
            if key not in debrid_show_keys and key not in merged_local_show_keys:
                shows.append(ls)

        # Build season_data first so enrichment's season-aware TMDB fallback
        # has data to work with.  Don't pop _episodes yet — path_index still
        # needs it after the (possibly title-renaming) enrichment runs.
        for show in shows:
            eps = show.get('_episodes', {})
            show['season_data'] = _build_season_data(eps, show.get('source', 'debrid'))

        from utils.library_prefs import get_all_preferences

        preferences = get_all_preferences()
        # Enrichment may replace item['title'] with the canonical TMDB title
        # (e.g. multi-language torrent folders that bundle two titles).  Wire
        # the (old → new) normalized-title pairs into the local alias map so
        # prefs/pending entries saved under the old name still resolve.
        # Reuse the cache snapshot loaded for the merge step so both
        # phases see the same state (closes a transient-duplicate window
        # where a populate-cache write between two loads could leave an
        # item un-merged-then-renamed within a single scan).
        renames = _enrich_with_tmdb_cache(
            movies, shows, _shared_tmdb_cache=_full_tmdb_cache,
        ) or []
        for old_norm, new_norm in renames:
            if not old_norm or not new_norm:
                continue
            alias_norms_local.setdefault(new_norm, set()).add(old_norm)
            alias_norms_local.setdefault(old_norm, set()).add(new_norm)

        # Re-base ``missing_episodes`` against Sonarr's monitored view so
        # long-running shows like Grey's Anatomy (22 seasons, older ones
        # unmonitored) don't report 100s of "missing" episodes the user
        # never asked to track.  Also surfaces unmonitored season numbers
        # to the UI and to gap-fill so neither invents phantom work.
        matched_series_ids = _apply_sonarr_monitored_filter(
            shows, degraded=arr_degraded)

        # Inject ghost entries for Sonarr-monitored series with no episode
        # on disk yet — the TV mirror of the Radarr wanted-movie injection
        # above.  Skips series already matched to a real library show
        # (``matched_series_ids``) so we never double-count.  Reuses the
        # same pending snapshot as the movie path so in-flight downloads
        # don't double-count against the pending bucket.  Silently no-ops
        # when Sonarr is unavailable.
        show_ghosts = _apply_sonarr_wanted_shows(
            shows, matched_series_ids, pending=_pending_snapshot,
            degraded=arr_degraded,
        )
        if show_ghosts:
            logger.debug(f"[library] Injected {show_ghosts} Sonarr wanted show(s) as ghost entries")

        # Strip ghost movies that now duplicate a real entry. Enrichment
        # may have renamed a real movie to a canonical TMDB title that
        # matches a ghost we injected pre-enrichment (e.g. parsed
        # "F1 The Movie" → canonical "F1" collides with a Radarr ghost
        # titled "F1"). Pre-enrichment dedup couldn't see this collision.
        _strip_ghost_duplicates(movies)

        # Collapse show entries that share an IMDb ID post-enrichment.
        # The alias-map dedup in ``_dedup_by_tmdb`` runs pre-enrichment
        # off normalized parsed-folder titles, so three debrid folders
        # whose parsed names are "your friends and neighbors" /
        # "your friends neighbors" / "your friends and neighbours"
        # survive as three groups even though enrichment stamps the
        # same canonical title + imdb_id on all three.
        _dedup_shows_by_external_id(shows)

        # Build path indexes keyed by post-rename normalized titles.  When
        # two items collide on the same (norm, season, episode) — possible
        # when distinct shows share a canonical title and dedup didn't merge
        # them (no shared TMDB ID) — keep the first and warn so the bug is
        # diagnosable instead of silently linking the wrong file.
        path_index = {}
        local_path_index = {}
        for show in shows:
            eps = show.get('_episodes', {})
            norm = _normalize_title(show['title'])
            show_source = show.get('source', 'debrid')
            for (sn, en), info in eps.items():
                src = info.get('source', show_source)
                p = info.get('path', '')
                lp = info.get('local_path', '')
                if src in ('debrid', 'both') and p:
                    existing = path_index.get((norm, sn, en))
                    if existing and existing != p:
                        logger.warning(
                            "[library] path_index collision for %r S%02dE%02d — "
                            "keeping %r, dropping %r", norm, sn, en, existing, p,
                        )
                    else:
                        path_index[(norm, sn, en)] = p
                if src == 'local' and p:
                    existing_local = local_path_index.get((norm, sn, en))
                    if existing_local and existing_local != p:
                        logger.warning(
                            "[library] local_path_index collision for %r S%02dE%02d — "
                            "keeping %r, dropping %r", norm, sn, en, existing_local, p,
                        )
                    else:
                        local_path_index[(norm, sn, en)] = p
                if lp:
                    existing_local = local_path_index.get((norm, sn, en))
                    if existing_local and existing_local != lp:
                        logger.warning(
                            "[library] local_path_index collision for %r S%02dE%02d — "
                            "keeping %r, dropping %r", norm, sn, en, existing_local, lp,
                        )
                    else:
                        local_path_index[(norm, sn, en)] = lp

        with self._path_lock:
            self._path_index = path_index
            self._local_path_index = local_path_index
            # Atomically rebind the alias map so request threads iterating
            # the previous map's sets aren't disturbed mid-flight.
            self._alias_norms = alias_norms_local

        for show in shows:
            show.pop('_episodes', None)

        if arr_degraded:
            logger.warning(
                f"[library] Scan completed with degraded arr enrichment: "
                f"{', '.join(sorted(arr_degraded))} — wanted counts are "
                f"unreliable this scan")

        elapsed_ms = int((time.monotonic() - start) * 1000)
        return {
            'movies': movies,
            'shows': shows,
            'preferences': preferences,
            'last_scan': datetime.now(timezone.utc).isoformat(timespec='seconds'),
            'scan_duration_ms': elapsed_ms,
            'arr_degraded': sorted(arr_degraded),
        }

    def _scan_effects(self, data, force_enforce=False):
        """Run side effects: preference enforcement, searches, symlinks.

        These operations involve external API calls (Sonarr, Radarr, TMDB,
        debrid providers) and can take 30-60 seconds.  Separated from the
        read phase so refresh() can update the UI cache before running them.
        """
        # Defensive copies — data is shared with the cache that UI reads
        shows = list(data['shows'])
        movies = list(data['movies'])
        preferences = data.get('preferences', {})
        with self._path_lock:
            path_index = dict(self._path_index)
            local_path_index = dict(self._local_path_index)
        self._cleanup_disc_rips(movies)
        changed = self._enforce_preferences(shows, movies, preferences, path_index,
                                              local_path_index, force=force_enforce)
        self._search_for_missing_episodes(shows, movies, preferences)
        self._recover_wanted_via_debrid(shows, movies, preferences)
        self._recover_local_fallback_routing(shows, movies)
        self._clear_resolved_pending(shows, movies)
        self._escalate_stuck_pending()
        self._warn_stalled_pending()
        self._cleanup_broken_debrid_symlinks()
        self._create_debrid_symlinks(shows, movies, path_index)
        return changed

    def scan(self, force_enforce=False):
        data = self._scan_read()
        # Respect _effects_running to prevent concurrent _scan_effects
        # execution when refresh() is already running effects on another thread
        with self._lock:
            if self._effects_running:
                return data
            self._effects_running = True
        try:
            changed = self._scan_effects(data, force_enforce)
            if changed:
                with self._lock:
                    self._cache_time = 0
        finally:
            with self._lock:
                self._effects_running = False
        return data

    def peek_data(self):
        """Return the cached scan payload without ever triggering a scan.

        Monitoring readers (/api/stuck) must not pay — or race — a
        synchronous library scan; stale-or-None is acceptable there.
        """
        with self._lock:
            return self._cache

    def get_data(self):
        with self._lock:
            now = time.monotonic()
            ttl = self._ttl if self._mount_path else 10
            if self._cache is not None and (now - self._cache_time) < ttl:
                return self._cache
            # Background scan already running — return stale cache instead
            # of triggering a duplicate synchronous scan
            if self._scanning and self._cache is not None:
                return self._cache

        # Cache expired or empty — scan synchronously so caller always gets data
        data = self.scan()
        # Capture a coherent index snapshot before any concurrent scan can
        # rebind the live ones — pairs this ``data`` with these indexes on
        # disk so the warm-start state is internally consistent.
        idx = self._snapshot_indexes_for_persist()
        with self._lock:
            self._cache = data
            self._cache_time = time.monotonic()
        self._persist_cache(data, *idx)
        return data

    def refresh(self, _rescan_depth=0):
        with self._lock:
            if self._scanning:
                return
            self._scanning = True

        def _run():
            data = None
            rescan_needed = False
            try:
                # Read phase — update cache immediately so UI gets data fast
                had_mount_before = self._mount_path is not None
                data = self._scan_read()
                has_mount_now = self._mount_path is not None
                # Capture index snapshot before another scan can rebind.
                idx = self._snapshot_indexes_for_persist()
                with self._lock:
                    self._cache = data
                    if not self._mount_path:
                        self._cache_time = time.monotonic() - self._ttl + 10
                    else:
                        self._cache_time = time.monotonic()
                    logger.debug(
                        f"[library] Read scan complete: {len(data['movies'])} movies, "
                        f"{len(data['shows'])} shows in {data['scan_duration_ms']}ms"
                    )
                self._persist_cache(data, *idx)
                # Mount appeared mid-scan — the scan started before the mount
                # was available so debrid content is missing.  Schedule a
                # follow-up scan so it appears within seconds of startup.
                if not had_mount_before and has_mount_now and _rescan_depth < 1:
                    rescan_needed = True
            except Exception as e:
                logger.error(f"[library] Scan error: {e}")
            finally:
                with self._lock:
                    self._scanning = False

            # Effects phase — runs after _scanning cleared so UI polling
            # stops promptly.  _effects_running prevents overlapping effects.
            run_effects = False
            if data is not None:
                with self._lock:
                    if not self._effects_running:
                        self._effects_running = True
                        run_effects = True
            if run_effects:
                try:
                    changed = self._scan_effects(data)
                    if changed:
                        # Enforcement modified files — invalidate cache so next
                        # UI poll triggers a fresh read with correct source info
                        with self._lock:
                            self._cache_time = 0
                except Exception as e:
                    logger.error(f"[library] Scan effects error: {e}")
                finally:
                    with self._lock:
                        self._effects_running = False

            if rescan_needed:
                logger.info("[library] Mount discovered mid-scan, re-scanning for debrid content")
                self.refresh(_rescan_depth=_rescan_depth + 1)

        t = threading.Thread(target=_run, daemon=True)
        t.start()

    def aliases_for(self, normalized_title):
        """Return the alias set for *normalized_title* (excluding itself).

        Thread-safe snapshot — copies the set under `_path_lock` so the
        caller can iterate without racing the next scan's atomic rebind.
        """
        with self._path_lock:
            return set(self._alias_norms.get(normalized_title, ()))

    def get_episode_path(self, normalized_title, season, episode):
        """Get debrid mount path for an episode."""
        with self._path_lock:
            result = self._path_index.get((normalized_title, season, episode))
            if not result:
                for alias in self._alias_norms.get(normalized_title, ()):
                    result = self._path_index.get((alias, season, episode))
                    if result:
                        break
            return result

    def get_local_episode_path(self, normalized_title, season, episode):
        """Get local library path for an episode."""
        with self._path_lock:
            result = self._local_path_index.get((normalized_title, season, episode))
            if not result:
                for alias in self._alias_norms.get(normalized_title, ()):
                    result = self._local_path_index.get((alias, season, episode))
                    if result:
                        break
            return result

    def _cleanup_disc_rips(self, movies):
        """Remove disc rips from debrid: blocklist hash, delete torrent, drop from list.

        Disc rips have mount folders with files but none matching MEDIA_EXTENSIONS,
        so the quality parser returns size_bytes == 0 (key must be present and
        explicitly zero; missing key is ignored).  Runs during _scan_effects()
        before preference enforcement so downstream effects don't process them.
        """
        if not movies:
            return 0
        # Quick filter: only debrid movies with no media files (size_bytes == 0)
        candidates = [
            m for m in movies
            if m.get('source') == 'debrid' and m.get('size_bytes', -1) == 0 and m.get('path')
        ]
        if not candidates:
            return 0

        # Verify each candidate has files but none in MEDIA_EXTENSIONS.
        # Check top-level AND one subdirectory deep (matches the show-demotion
        # pattern at _scan_mount line ~2486) to avoid false positives on releases
        # that nest the movie file inside a subdirectory.
        confirmed = []
        for m in candidates:
            try:
                entries = list(os.scandir(m['path']))
            except (OSError, PermissionError):
                continue
            if not entries:
                continue  # Empty folder — mount issue, not disc rip
            has_any_file = any(e.is_file(follow_symlinks=True) for e in entries)
            if not has_any_file:
                # No files at top level — check if subdirs exist (disc rips have BDMV/ etc.)
                if not any(e.is_dir() for e in entries):
                    continue
            has_media = any(
                os.path.splitext(e.name)[1].lower() in MEDIA_EXTENSIONS
                for e in entries if e.is_file(follow_symlinks=True)
            )
            if not has_media:
                # Check one level of subdirectories
                for e in entries:
                    if e.is_dir(follow_symlinks=False):
                        try:
                            with os.scandir(e.path) as sub_it:
                                for sf in sub_it:
                                    if sf.is_file(follow_symlinks=True):
                                        ext = os.path.splitext(sf.name)[1].lower()
                                        if ext in MEDIA_EXTENSIONS:
                                            has_media = True
                                            break
                        except OSError:
                            pass
                    if has_media:
                        break
            if not has_media:
                confirmed.append(m)

        if not confirmed:
            return 0

        cleaned = 0
        cleaned_paths = set()
        try:
            from utils.debrid_client import get_debrid_client
            client, _svc = get_debrid_client()
        except Exception as e:
            logger.warning(f"[library] Disc rip cleanup: debrid client unavailable: {e}")
            client = None

        for m in confirmed:
            title = m['title']
            year = m.get('year')
            # find_torrents_by_title parses raw debrid filenames — match
            # against the parsed folder norm, which equals item['title'] for
            # un-renamed items and is preserved on `_parsed_title` for items
            # whose display title was upgraded to the canonical TMDB name.
            parsed_norm = _normalize_title(m.get('_parsed_title') or title)
            accept_norms = {parsed_norm} | self.aliases_for(parsed_norm)
            bl_hash = ''
            deleted = 0

            # Find matching debrid torrent(s) and extract hash
            if client:
                try:
                    matches = client.find_torrents_by_title(accept_norms, target_year=year)
                except Exception as e:
                    logger.debug(f"[library] Disc rip lookup failed for {title}: {e}")
                    matches = []
                for match in matches:
                    if not bl_hash and match.get('hash'):
                        bl_hash = match['hash']
                    if client.delete_torrent(str(match['id'])):
                        deleted += 1

            # Auto-blocklist
            if bl_hash and _blocklist and str(os.environ.get('BLOCKLIST_AUTO_ADD', 'true')).lower() == 'true':
                _blocklist.add(bl_hash, title, reason='disc rip (no usable media files)', source='auto')
                if _history:
                    _history.log_event('blocklist_added', title, source='library',
                                       detail='Auto-blocklisted: disc rip',
                                       meta={'cause': 'auto_blocklist_added',
                                             'blocklist_reason': 'disc rip',
                                             'info_hash': bl_hash})

            if deleted or bl_hash:
                logger.info(f"[library] Disc rip cleaned: {title} — "
                            f"deleted {deleted} torrent(s), blocklisted {'yes' if bl_hash else 'no'}")
                if _history:
                    _history.log_event('failed', title, source='library',
                                       detail=f'Disc rip removed ({deleted} torrent(s) deleted)',
                                       meta={'cause': 'disc_rip_rejected',
                                             'info_hash': bl_hash,
                                             'deleted_torrents': deleted})
                try:
                    from utils.notifications import notify
                    notify('download_error', 'Library: Disc Rip Removed',
                           f'{title} contains no recognized media files. '
                           f'Blocklisted and removed from debrid.')
                except Exception:
                    pass
                cleaned += 1
                cleaned_paths.add(m['path'])

        # Remove only successfully cleaned disc rips from the movies list
        if cleaned_paths:
            movies[:] = [m for m in movies if m.get('path') not in cleaned_paths]

        return cleaned

    def _enforce_preferences(self, shows, movies, preferences, path_index, local_path_index,
                              force=False):
        """Auto-enforce source preferences after a scan.

        For prefer-debrid: if an episode has source=both (debrid copy arrived),
        replace the local file with a symlink to the debrid mount.

        Returns True if any enforcement action was taken (cache should be invalidated).

        For prefer-local: if an episode has source=both (local copy arrived),
        delete the debrid torrent via provider API.

        Only runs if LIBRARY_PREFERENCE_AUTO_ENFORCE is true, or force=True.
        """
        if not force:
            auto_enforce = os.environ.get('LIBRARY_PREFERENCE_AUTO_ENFORCE', 'false').lower() == 'true'
            if not auto_enforce:
                return False

        rclone_mount = os.environ.get('BLACKHOLE_RCLONE_MOUNT', '').strip()
        symlink_base = os.environ.get('BLACKHOLE_SYMLINK_TARGET_BASE', '').strip()

        if not preferences:
            return False

        from utils.library_prefs import replace_local_with_symlinks, clear_pending, get_all_pending

        # Track titles processed this scan to avoid redundant operations
        enforced_this_scan = set()

        # Load pending state to guard local-fallback episodes from symlink replacement
        all_pending = get_all_pending()

        # Enforce prefer-debrid: replace local files with symlinks for source=both episodes
        if rclone_mount and symlink_base and self._local_tv_path:
            for show in shows:
                norm = _normalize_title(show['title'])
                pref = self._get_pref(norm, preferences)
                if pref != 'prefer-debrid':
                    continue

                # Guard: don't replace local files for episodes downloaded via local-fallback
                fallback_guard = set()
                fb_entry = all_pending.get(norm, {})
                if not fb_entry:
                    for alias in self._alias_norms.get(norm, ()):
                        fb_entry = all_pending.get(alias, {})
                        if fb_entry:
                            break
                if fb_entry.get('direction') == 'to-local-fallback':
                    fallback_guard = {
                        (e['season'], e['episode']) for e in fb_entry.get('episodes', [])
                    }

                to_switch = []
                for sd in show.get('season_data', []):
                    for ep in sd.get('episodes', []):
                        if ep.get('source') != 'both':
                            continue
                        sn, en = sd['number'], ep['number']
                        if (sn, en) in fallback_guard:
                            continue  # local-fallback episode — don't replace
                        local_p = local_path_index.get((norm, sn, en))
                        debrid_p = path_index.get((norm, sn, en))
                        if local_p and debrid_p and not os.path.islink(local_p):
                            to_switch.append({
                                'local_path': local_p,
                                'debrid_path': debrid_p,
                                'season': sn,
                                'episode': en,
                            })

                if to_switch:
                    result = replace_local_with_symlinks(
                        to_switch, self._local_tv_path, rclone_mount, symlink_base
                    )
                    if result.get('switched', 0) > 0:
                        logger.info(
                            f"[library] Auto-enforced prefer-debrid for {show['title']}: "
                            f"switched {result['switched']} episode(s) to symlinks"
                        )
                        enforced_this_scan.add(norm)
                        if _history:
                            _history.log_event('switched_source', show['title'], source='library',
                                               detail=f"Switched {result['switched']} episode(s) to debrid",
                                               meta={'cause': 'preference_source_switch',
                                                     'from': 'local',
                                                     'to': 'debrid',
                                                     'count': result['switched'],
                                                     'media_type': 'show'})
                        # Only clear pending for episodes that were actually switched
                        # (those whose local_path is now a symlink)
                        cleared = [
                            {'season': e['season'], 'episode': e['episode']}
                            for e in to_switch if os.path.islink(e['local_path'])
                        ]
                        if cleared:
                            clear_pending(norm, cleared)
                        try:
                            from utils.notifications import notify
                            notify('library_refresh',
                                   f"Source switch: {show['title']}",
                                   f"Switched {result['switched']} episode(s) to debrid streaming")
                        except Exception:
                            pass

        # Enforce prefer-debrid for movies: replace local file with symlink
        if rclone_mount and symlink_base and self._local_movies_path:
            for movie in movies:
                norm = _normalize_title(movie['title'])
                if norm in enforced_this_scan:
                    continue
                pref = self._get_pref(norm, preferences)
                if pref != 'prefer-debrid':
                    continue
                if movie.get('source') != 'both':
                    continue

                # Guard: don't replace local files for movies downloaded via local-fallback
                fb_entry = all_pending.get(norm, {})
                if not fb_entry:
                    for alias in self._alias_norms.get(norm, ()):
                        fb_entry = all_pending.get(alias, {})
                        if fb_entry:
                            break
                if fb_entry.get('direction') == 'to-local-fallback':
                    continue

                local_dir = movie.get('local_path')
                debrid_dir = movie.get('path')
                if not local_dir or not debrid_dir:
                    continue

                # Find largest media file in local dir
                local_file = None
                local_size = -1
                try:
                    for fname in os.listdir(local_dir):
                        ext = os.path.splitext(fname)[1].lower()
                        if ext in MEDIA_EXTENSIONS:
                            fpath = os.path.join(local_dir, fname)
                            if os.path.islink(fpath):
                                continue  # already a symlink
                            try:
                                sz = os.path.getsize(fpath)
                            except OSError:
                                sz = 0
                            if sz > local_size:
                                local_size = sz
                                local_file = fname
                except OSError:
                    continue
                if not local_file:
                    continue

                # Find largest media file in debrid dir
                debrid_file = None
                debrid_size = -1
                try:
                    for fname in os.listdir(debrid_dir):
                        ext = os.path.splitext(fname)[1].lower()
                        if ext in MEDIA_EXTENSIONS:
                            fpath = os.path.join(debrid_dir, fname)
                            try:
                                sz = os.path.getsize(fpath)
                            except OSError:
                                sz = 0
                            if sz > debrid_size:
                                debrid_size = sz
                                debrid_file = fname
                except OSError:
                    continue
                if not debrid_file:
                    continue

                local_fpath = os.path.join(local_dir, local_file)
                debrid_fpath = os.path.join(debrid_dir, debrid_file)

                to_switch = [{
                    'local_path': local_fpath,
                    'debrid_path': debrid_fpath,
                    'season': 0,
                    'episode': 0,
                }]
                result = replace_local_with_symlinks(
                    to_switch, self._local_movies_path, rclone_mount, symlink_base
                )
                if result.get('switched', 0) > 0:
                    logger.info(
                        f"[library] Auto-enforced prefer-debrid for movie {movie['title']}: "
                        f"switched to symlink"
                    )
                    if _history:
                        _history.log_event('switched_source', movie['title'], source='library',
                                           detail="Switched movie to debrid",
                                           meta={'cause': 'preference_source_switch',
                                                 'from': 'local',
                                                 'to': 'debrid',
                                                 'media_type': 'movie'})
                    # Movie is atomic — one file switched means the whole title is done
                    clear_pending(norm)
                    enforced_this_scan.add(norm)
                    try:
                        from utils.notifications import notify
                        notify('library_refresh',
                               f"Source switch: {movie['title']}",
                               f"Switched movie to debrid streaming")
                    except Exception:
                        pass

        # Enforce prefer-local: delete debrid torrents ONLY when ALL debrid
        # episodes have local copies (source=both for every debrid episode).
        # This prevents deleting seasons/episodes that have no local backup.
        prefer_local_safe = {}
        for show in shows:
            norm = _normalize_title(show['title'])
            if self._get_pref(norm, preferences) != 'prefer-local':
                continue
            has_debrid_only = False
            has_both = False
            for sd in show.get('season_data', []):
                for ep in sd.get('episodes', []):
                    src = ep.get('source')
                    if src == 'debrid':
                        has_debrid_only = True
                    elif src == 'both':
                        has_both = True
            # Only safe to delete if there are both-source eps AND no debrid-only eps
            if has_both and not has_debrid_only:
                prefer_local_safe[norm] = show

        for movie in movies:
            norm = _normalize_title(movie['title'])
            if self._get_pref(norm, preferences) == 'prefer-local' and movie.get('source') == 'both':
                prefer_local_safe[norm] = movie

        if prefer_local_safe:
            try:
                from utils.debrid_client import get_debrid_client
                client, svc = get_debrid_client()
                if client:
                    for norm, item in prefer_local_safe.items():
                        if norm in enforced_this_scan:
                            continue
                        year = item.get('year')
                        # Debrid filenames carry the parsed folder title.
                        # Match against the parsed norm (preserved by the
                        # canonical-title rename) so renamed items still hit.
                        parsed_norm = _normalize_title(
                            item.get('_parsed_title') or item['title']
                        )
                        matches = client.find_torrents_by_title(parsed_norm, target_year=year)
                        if matches:
                            deleted = 0
                            for m in matches:
                                if client.delete_torrent(m['id']):
                                    deleted += 1
                            if deleted:
                                logger.info(
                                    f"[library] Auto-enforced prefer-local for {item['title']}: "
                                    f"deleted {deleted} debrid torrent(s)"
                                )
                                if _history:
                                    _history.log_event('switched_source', item['title'], source='library',
                                                       detail=f"Removed {deleted} debrid torrent(s) — prefer-local",
                                                       meta={'cause': 'preference_source_switch',
                                                             'from': 'debrid',
                                                             'to': 'local',
                                                             'count': deleted})
                                clear_pending(norm)
                                enforced_this_scan.add(norm)
                                try:
                                    from utils.notifications import notify
                                    notify('library_refresh',
                                           f"Source switch: {item['title']}",
                                           f"Removed {deleted} debrid torrent(s) — now playing from local storage")
                                except Exception:
                                    pass
            except Exception as e:
                logger.error(f"[library] Auto-enforce prefer-local failed: {e}")

        return bool(enforced_this_scan)

    _SEARCH_BUDGET_SECONDS = 30
    _SEARCH_RETRY_HOURS = 6
    # Wanted→TorBox recovery: cap Torrentio fan-out per title probed against
    # TB's cache, and how long a probed-and-missed (or just-grabbed) title is
    # skipped before being re-probed.  6h matches the arr-search retry window.
    _WANTED_TB_MAX_PROBES = 12
    _WANTED_TB_RECOVERY_COOLDOWN = 6 * 3600
    # RD leg of the Wanted recovery pass.  The ready timeout is short on
    # purpose: cached content reaches ``downloaded`` within seconds of
    # selectFiles; anything still converting/downloading after 20s is an
    # uncached miss and the probe add is deleted.  The miss TTL is long
    # (7 days vs the 6h TB cooldown) because a miss usually means RD
    # doesn't have the content family cached at all — re-probing a
    # different release of the same title every 6h would just churn
    # add/delete cycles against RD's API.
    _WANTED_RD_READY_TIMEOUT = 20
    _WANTED_RD_POLL_INTERVAL = 2
    _WANTED_RD_MISS_TTL = 7 * 24 * 3600
    # RD's addMagnet hash-dedups: adding a hash already on the account
    # returns the PRE-EXISTING torrent's id.  Before any probe-cleanup
    # delete, the torrent's ``added`` timestamp is compared against the
    # probe start; anything older than this grace (clock-skew headroom
    # between RD's server and ours) was NOT created by the probe and
    # must never be deleted.  Kept small: too wide a window lets a torrent
    # the user added seconds before our probe read as "ours" and be
    # deleted — the exact data-loss we're guarding against.
    _WANTED_RD_PREEXISTING_GRACE = 30
    # Time budget for the whole Wanted recovery pass.  Wider than
    # _SEARCH_BUDGET_SECONDS (30s) because each uncached RD probe-add can
    # legitimately burn _WANTED_RD_READY_TIMEOUT seconds of polling — a
    # 30s ceiling would cap the pass at ~1 RD attempt per scan.
    _WANTED_RECOVERY_BUDGET_SECONDS = 120

    def _check_pending_freshness(self, norm, pending, direction):
        """Resolve a title's pending entry and decide retry-vs-wait.

        Shared by the shows and movies paths of
        ``_search_for_missing_episodes`` (previously two copy-pasted blocks
        that had already drifted).

        Returns ``(pending_norm, verdict, pending_keys)``:
          * ``pending_norm`` — the key the entry actually lives under (the
            title's norm, or an alias norm).
          * ``verdict`` — one of:
              - ``'give-up'``: escalated to ``debrid-unavailable``; caller
                must skip the title entirely.
              - ``'retry'``: a same-direction entry exists but its last
                search is older than ``_SEARCH_RETRY_HOURS``; caller may
                re-search.
              - ``'wait'``: a fresh same-direction entry exists; a
                "Waiting for retry" status (with ``next_retry_at``) has
                already been recorded on it.  The shows caller filters
                ``pending_keys`` out of its candidates and falls through;
                the movies caller skips the title.
              - ``'none'``: no same-direction entry — either the title was
                never pending, or a stale-direction entry was just cleared
                (so ``touch_pending_searched``/``update_pending_error``
                can't mutate a wrong-direction entry that would zombify
                when the search loop errors out).
          * ``pending_keys`` — the entry's ``(season, episode)`` set; only
            populated for ``'wait'``.
        """
        from utils.library_prefs import clear_pending, update_pending_error

        pending_entry = pending.get(norm)
        pending_norm = norm  # key under which the entry lives
        if not pending_entry:
            for _pa in self._alias_norms.get(norm, ()):
                pending_entry = pending.get(_pa)
                if pending_entry:
                    pending_norm = _pa
                    break
        pending_entry = pending_entry or {}
        pe_dir = pending_entry.get('direction', '')
        if pe_dir == 'debrid-unavailable':
            return pending_norm, 'give-up', set()
        if pe_dir and pe_dir != direction:
            # Direction changed (e.g., user flipped from prefer-debrid to
            # unset).  Clear the stale-direction entry.
            clear_pending(pending_norm)
            return pending_norm, 'none', set()
        if pe_dir != direction:
            return pending_norm, 'none', set()  # no entry for this direction

        # Same-direction entry: retry only if the last search is stale.
        last_ts = pending_entry.get('last_searched') or pending_entry.get('created')
        stale = True
        ls_dt = None
        age_hours = 0.0
        if last_ts:
            try:
                ls_dt = datetime.fromisoformat(last_ts)
                if ls_dt.tzinfo is None:
                    ls_dt = ls_dt.replace(tzinfo=timezone.utc)
                age_hours = (datetime.now(timezone.utc) - ls_dt).total_seconds() / 3600
                if age_hours < self._SEARCH_RETRY_HOURS:
                    stale = False
            except (ValueError, TypeError):
                pass
        if stale:
            return pending_norm, 'retry', set()

        # Fresh entry — record "waiting" status with next retry time.
        remaining_h = self._SEARCH_RETRY_HOURS - age_hours
        retry_dt = ls_dt + timedelta(hours=self._SEARCH_RETRY_HOURS)
        update_pending_error(
            pending_norm,
            f"Waiting for retry ({remaining_h:.1f}h remaining)",
            next_retry_at=retry_dt.isoformat(timespec='seconds'),
            increment_retry=False,
        )
        # Tolerate malformed persisted entries (legacy schema / hand edits):
        # a missing season/episode key must not abort the whole search loop.
        pending_keys = {
            (e['season'], e['episode'])
            for e in pending_entry.get('episodes', [])
            if isinstance(e, dict) and e.get('season') is not None and e.get('episode') is not None
        }
        return pending_norm, 'wait', pending_keys

    def _search_for_missing_episodes(self, shows, movies, preferences):
        """Unconditional episode-completeness reconcile.

        For every show and movie, issue arr searches for:
          * Missing-anywhere: aired TMDB episodes present in neither debrid
            nor local (the "Lucky Hank E04" case).  Runs regardless of
            source preference so any viewer hole gets filled.  Gated by
            ``GAP_FILL_ENABLED`` (default ``true``).
          * Local-only (``prefer-debrid`` only): episodes that exist
            locally but not on debrid — the original preference-enforcement
            behavior this function was built for.

        Route selection: ``prefer-debrid`` → True (force-grab debrid copies);
        ``prefer-local`` → False (route via local); unset → None (Sonarr's
        own tag decides).  All routes pass ``respect_monitored=True`` so an
        explicitly-unmonitored episode is never re-searched against the
        user's intent — prefer-debrid still force-grabs debrid copies, but
        only for episodes the user is actively monitoring.

        Pending direction is chosen per route: ``to-debrid`` / ``to-local``
        / ``to-any``.  ``to-any`` resolves on any source and is never
        escalated to ``debrid-unavailable``.

        Skips episodes on cooldown from a recent failed search.  Episodes
        with existing pending entries of the same direction are retried
        after ``_SEARCH_RETRY_HOURS`` to handle transient indexer failures.
        Respects a time budget to avoid blocking the scan thread too long.
        """
        gap_fill_on = gap_fill_enabled()

        from utils.library_prefs import get_all_pending, set_pending, touch_pending_searched, update_pending_error, mark_debrid_unavailable
        from utils.tmdb import search_show as tmdb_search_show, search_movie as tmdb_search_movie
        from utils import attempt_ledger

        # pending is a snapshot; set_pending calls below write new entries
        # that won't be visible in this snapshot — acceptable since each
        # title is processed at most once per scan.
        pending = get_all_pending()
        now = time.monotonic()
        deadline = now + self._SEARCH_BUDGET_SECONDS

        # Expire old cooldown entries (older than 1 hour)
        cooldown = getattr(self, '_search_cooldown', {})
        self._search_cooldown = {
            k: t for k, t in cooldown.items()
            if now - t < 3600
        }

        # --- Shows via Sonarr ---
        try:
            from utils.arr_client import get_download_service
            show_client, show_svc = get_download_service('show')
        except Exception:
            show_client, show_svc = None, None

        if show_client and show_svc == 'sonarr':
            for show in shows:
                if time.monotonic() > deadline:
                    logger.info("[library] Search budget exhausted, deferring remaining to next scan")
                    break
                # Ghost entries (source='wanted') are Sonarr's own
                # monitored-no-file series — Sonarr is ALREADY searching
                # for them. Their season_data is empty but
                # _compute_missing_episodes derives candidates from the
                # TMDB episode cache, so without this guard a fully-absent
                # ghost would (a) fire redundant Sonarr search commands and
                # (b) write a set_pending entry that suppresses the ghost on
                # the next scan — the same self-erase regression the movie
                # path guards against below.  This gate is also one half of
                # the inverse-gate invariant with _recover_wanted_via_debrid
                # (see the comment there) that prevents dual acquisition of
                # GHOSTS.  Partial-show seasons are a deliberate exception:
                # that pass's season targets probe TB packs for the same
                # missing episodes this pass searches the arr for — a
                # dual-path overlap that self-corrects at import time.
                if show.get('source') == 'wanted':
                    continue
                norm = _normalize_title(show['title'])
                route = self._route_for(norm, preferences)
                direction = {True: 'to-debrid', False: 'to-local', None: 'to-any'}[route]

                # Check pending state — skip debrid-unavailable, allow retries
                # for stale entries whose direction matches the current route.
                # 'wait' deliberately falls through: candidates are filtered
                # by pending_keys below, so non-pending episodes of the same
                # show can still be searched.
                pending_norm, verdict, pending_keys = \
                    self._check_pending_freshness(norm, pending, direction)
                if verdict == 'give-up':
                    continue  # escalated — stop retrying
                is_retry = verdict == 'retry'

                # Build the candidate set:
                #   - Missing-anywhere (aired TMDB episodes absent from all sources).
                #     Gated by GAP_FILL_ENABLED so operators can opt out.
                #   - Local-only (route=True only): legacy prefer-debrid behavior.
                candidates = set()
                if gap_fill_on:
                    for sn, en in self._compute_missing_episodes(show):
                        candidates.add((sn, en))
                if route is True:
                    for sd in show.get('season_data', []):
                        for ep in sd.get('episodes', []):
                            src = ep.get('source')
                            if src in ('debrid', 'both'):
                                continue  # already on debrid
                            candidates.add((sd['number'], ep['number']))

                by_season = {}
                for sn, en in candidates:
                    if (sn, en) in pending_keys:
                        continue  # already searching
                    if (norm, sn) in self._search_cooldown:
                        continue  # recently attempted
                    by_season.setdefault(sn, []).append(en)

                if not by_season:
                    continue

                total = sum(len(eps) for eps in by_season.values())
                retry_tag = ' (retry)' if is_retry else ''
                route_tag = {True: 'debrid', False: 'local', None: 'any'}[route]
                logger.info(
                    f"[library] Gap-fill search{retry_tag} [{route_tag}] for {show['title']}: "
                    f"{total} episode(s) across {len(by_season)} season(s)"
                )

                # Touch last_searched immediately so overlapping scans
                # don't re-process the same title concurrently.  Safe no-op
                # when no entry exists; on direction change the final
                # set_pending() below resets the entry cleanly.
                touch_pending_searched(pending_norm)

                # Resolve TMDB ID for accurate Sonarr matching (only when
                # year is available for reliable disambiguation)
                show_tmdb_id = None
                if show.get('year'):
                    try:
                        tmdb_hit = tmdb_search_show(show['title'], show['year'])
                        if tmdb_hit:
                            show_tmdb_id = tmdb_hit['tmdb_id']
                    except Exception as e:
                        logger.debug(f"[library] TMDB lookup failed for {show['title']!r}, falling back to title search: {e}")

                respect_mon = True  # all routes honor Sonarr's monitored flag
                new_pending = []
                capped_count = 0
                for sn, ep_nums in by_season.items():
                    # Force-grab give-up gate: the prefer_debrid=True path
                    # re-grabs an already-present file to push it to debrid,
                    # which re-arms TorBox's abuse cooldown.  After
                    # FORCE_GRAB_MAX_ATTEMPTS futile grabs on a stuck title,
                    # stop poking debrid for this season.
                    fg_key = f"fg:{norm}:s{sn}"
                    if route is True and attempt_ledger.get(fg_key) >= self._force_grab_max_attempts:
                        logger.info(
                            f"[library] Force-grab give-up for {show['title']} S{sn:02d}: "
                            f"{self._force_grab_max_attempts} attempts exhausted, "
                            f"stopping debrid re-grab"
                        )
                        capped_count += 1
                        continue
                    try:
                        result = show_client.ensure_and_search(
                            show['title'], show_tmdb_id, sn, ep_nums,
                            prefer_debrid=route, respect_monitored=respect_mon,
                        )
                        status = result.get('status', '')
                        if status in ('sent', 'pending'):
                            for en in ep_nums:
                                new_pending.append({'season': sn, 'episode': en})
                            if route is True and result.get('grabbed'):
                                attempt_ledger.bump(fg_key)
                        elif status == 'skipped':
                            # User-unmonitored episodes — respect intent, don't
                            # cooldown or error-log (would churn retry_count).
                            logger.debug(
                                f"[library] {show['title']} S{sn:02d}: "
                                f"{result.get('message', 'skipped (unmonitored)')}"
                            )
                            self._search_cooldown[(norm, sn)] = now
                        elif status == 'error':
                            err_msg = f"Sonarr: {result.get('message', 'unknown error')}"
                            logger.warning(
                                f"[library] Search failed for {show['title']} S{sn:02d}: "
                                f"{result.get('message', 'unknown error')}"
                            )
                            self._search_cooldown[(norm, sn)] = now
                            update_pending_error(pending_norm, err_msg)
                        else:
                            update_pending_error(pending_norm, "No search results found", increment_retry=False)
                    except Exception as e:
                        logger.error(f"[library] Search error for {show['title']} S{sn:02d}: {e}")
                        self._search_cooldown[(norm, sn)] = now
                        update_pending_error(pending_norm, f"Sonarr: {e}")

                if new_pending:
                    set_pending(pending_norm, new_pending, direction)
                elif capped_count and capped_count == len(by_season):
                    # EVERY actionable season hit the force-grab cap (none were
                    # sent and none errored transiently) — escalate the title to
                    # debrid-unavailable so the UI reflects give-up and the loop
                    # stops re-poking debrid.  A transient error on an un-capped
                    # season leaves capped_count < len(by_season), so the title
                    # keeps its to-debrid direction and retries next scan.
                    mark_debrid_unavailable(pending_norm)

        # --- Movies via Radarr ---
        try:
            movie_client, movie_svc = get_download_service('movie')
        except Exception:
            movie_client, movie_svc = None, None

        if movie_client and movie_svc == 'radarr':
            for movie in movies:
                if time.monotonic() > deadline:
                    logger.info("[library] Search budget exhausted, deferring remaining to next scan")
                    break
                # Ghost entries (source='wanted') are Radarr's own
                # monitored-no-file movies — Radarr is ALREADY searching
                # for them via its scheduled RSS sync. Triggering a
                # second pd_zurg search here would (a) hit Radarr with
                # redundant /command/MoviesSearch calls, (b) write a
                # set_pending entry which would suppress the ghost on
                # the next scan (the bug surfaced in the 071ba5d review:
                # Wanted view self-erases after one scan cycle).  Also one
                # half of the inverse-gate invariant with
                # _recover_wanted_via_debrid (see the comment there).
                if movie.get('source') == 'wanted':
                    continue
                norm = _normalize_title(movie['title'])
                route = self._route_for(norm, preferences)
                direction = {True: 'to-debrid', False: 'to-local', None: 'to-any'}[route]

                # Candidate check: route=True searches whenever debrid copy missing
                # (legacy prefer-debrid); other routes search only when missing from
                # every source.  Gap-fill gated by GAP_FILL_ENABLED.
                src = movie.get('source')
                if route is True:
                    if src in ('debrid', 'both'):
                        continue  # already on debrid
                else:
                    if not gap_fill_on:
                        continue
                    if src in ('debrid', 'local', 'both'):
                        continue  # already available somewhere

                # A movie has no per-episode granularity, so a fresh pending
                # entry ('wait') skips the whole title — unlike the shows
                # path, which falls through and filters by pending_keys.
                pending_norm, verdict, _ = \
                    self._check_pending_freshness(norm, pending, direction)
                if verdict == 'give-up':
                    continue  # escalated — stop retrying
                if verdict == 'wait':
                    continue  # recent search — skip
                movie_is_retry = verdict == 'retry'
                if (norm, 0) in self._search_cooldown:
                    continue  # recently attempted

                # Force-grab give-up gate (mirrors the shows path): after
                # FORCE_GRAB_MAX_ATTEMPTS futile debrid re-grabs on a stuck
                # movie, escalate to debrid-unavailable and stop poking debrid.
                fg_key = f"fg:{norm}"
                if route is True and attempt_ledger.get(fg_key) >= self._force_grab_max_attempts:
                    logger.info(
                        f"[library] Force-grab give-up for movie {movie['title']}: "
                        f"{self._force_grab_max_attempts} attempts exhausted, "
                        f"stopping debrid re-grab"
                    )
                    mark_debrid_unavailable(pending_norm)
                    continue

                retry_tag = ' (retry)' if movie_is_retry else ''
                route_tag = {True: 'debrid', False: 'local', None: 'any'}[route]
                logger.info(f"[library] Gap-fill search{retry_tag} [{route_tag}] for movie: {movie['title']}")

                # Touch immediately to prevent overlapping scans.  Safe no-op
                # when no entry exists; direction reset is handled by the
                # final set_pending() below.
                touch_pending_searched(pending_norm)

                movie_tmdb_id = None
                if movie.get('year'):
                    try:
                        tmdb_hit = tmdb_search_movie(movie['title'], movie['year'])
                        if tmdb_hit:
                            movie_tmdb_id = tmdb_hit['tmdb_id']
                    except Exception as e:
                        logger.debug(f"[library] TMDB lookup failed for {movie['title']!r}, falling back to title search: {e}")
                respect_mon = True  # all routes honor Radarr's monitored flag
                try:
                    result = movie_client.ensure_and_search(
                        movie['title'], movie_tmdb_id, prefer_debrid=route,
                        respect_monitored=respect_mon,
                    )
                    status = result.get('status', '')
                    if status in ('sent', 'pending'):
                        set_pending(pending_norm, [{'season': 0, 'episode': 0}], direction)
                        if route is True and result.get('grabbed'):
                            attempt_ledger.bump(fg_key)
                    elif status == 'skipped':
                        # Respect_monitored short-circuit — user intentionally
                        # unmonitored; don't touch pending or retry_count.
                        logger.debug(
                            f"[library] Movie {movie['title']}: "
                            f"{result.get('message', 'skipped (unmonitored)')}"
                        )
                        self._search_cooldown[(norm, 0)] = now
                    elif status == 'error':
                        err_msg = f"Radarr: {result.get('message', 'unknown error')}"
                        logger.warning(
                            f"[library] Search failed for movie {movie['title']}: "
                            f"{result.get('message', 'unknown error')}"
                        )
                        self._search_cooldown[(norm, 0)] = now
                        update_pending_error(pending_norm, err_msg)
                    else:
                        update_pending_error(pending_norm, "No search results found", increment_retry=False)
                except Exception as e:
                    logger.error(f"[library] Search error for movie {movie['title']}: {e}")
                    self._search_cooldown[(norm, 0)] = now
                    update_pending_error(pending_norm, f"Radarr: {e}")

    def _recover_wanted_via_debrid(self, shows, movies, preferences):
        """Proactively grab debrid-cached copies of "Wanted" media.

        The arr searches its own indexer pool (Prowlarr/Torznab); zurgarr
        queries the Torrentio feed directly.  These are different populations,
        so a title can be cached on a debrid yet sit in "Wanted" forever
        because the arr's search never surfaces a grabbable release.
        Empirically ~93% of the live Wanted backlog is already cached on
        TorBox at full resolution — the gap to 100% recovery is this
        acquisition path, not supply.  This pass closes it, in two legs per
        Wanted ghost (movie or first missing episode of a show), both fed by
        a single Torrentio search per title:

        * **TB leg** (``WANTED_TB_RECOVERY_ENABLED``): probe candidates
          against TorBox's still-working cache endpoint and add the best
          cached release.  Runs FIRST and claims every TB-cached title
          exclusively — the RD leg never fires on one.  Bounded per scan
          (``WANTED_TB_RECOVERY_MAX_PER_SCAN``, default 2 — a trickle,
          because create-volume bursts arm TB Essential's ~24h abuse
          cooldown).  Enforcement backoff (an add failing while the
          account cooldown flag is set) disables only this leg — the RD
          leg keeps draining.
        * **RD leg** (``WANTED_RD_RECOVERY_ENABLED``): fallback for titles
          TorBox does NOT have cached (or can't answer for this pass) —
          measured live at ~8% hit rate vs TB's 93-97% cache coverage, so
          spending its add/delete churn on TB-cached titles is pure waste.
          RD's cache probe is dead, so the add IS the probe — add the top
          release, keep it if it goes instantly ready (cached), delete it
          otherwise.  RD has no create-volume cooldown, so this leg caps
          only on attempts per scan
          (``WANTED_RD_RECOVERY_MAX_PER_SCAN``).  Every attempt doubles as
          an RD cache-hit measurement (``wanted_rd_recovered`` vs
          ``wanted_rd_uncached`` history causes).

        Beyond whole-title ghosts, the pass also builds **season targets**
        (``WANTED_SEASON_RECOVERY_ENABLED``) for partially-present shows:
        each season with missing aired episodes is probed for a season
        pack covering it (single-episode releases for the first missing
        episode are the fallback).  Season targets are TB-only, share the
        TB budget with ghosts (ghosts first), and one cached pack add
        fills every gap in the season — the symlink phase skips episodes
        already on disk.

        The scanner's own symlink phase links recovered content on a
        subsequent scan, and Radarr/Sonarr import it from the mount.
        Per-title cooldowns keep a deep backlog from being re-probed every
        scan (6h for the TB leg, 7 days for RD misses — see
        ``_wanted_rd_miss``).
        """
        if not os.environ.get('TORRENTIO_URL'):
            return  # no Torrentio feed to search against

        from base import load_secret_or_env
        from utils import search as _search
        from utils import attempt_ledger as _ledger

        # --- TB leg availability -------------------------------------
        tb_key = load_secret_or_env('torbox_api_key')
        tb_ok = bool(tb_key) and wanted_tb_recovery_enabled()
        if tb_ok:
            # ``cooldown_until`` on /user/me is ADVISORY, not an enforcement
            # signal: on Pro plans organic creates succeed while the flag is
            # set, and organic traffic keeps re-arming it — gating the leg
            # on the flag self-starves recovery indefinitely (observed live:
            # "skipped — cooldown for 83693s" on every scan while 181
            # blackhole creates/week succeeded).  So the leg always attempts
            # its first add; ENFORCEMENT is detected from the add actually
            # failing while the cooldown flag is set, which disables the
            # leg for the rest of the pass (see the add-failure handler
            # below).  The pre-check remains purely informational.
            try:
                from utils.blackhole import _check_torbox_cooldown
                tb_cooldown = _check_torbox_cooldown(tb_key)
            except Exception:
                tb_cooldown = 0
            if tb_cooldown > 0:
                logger.info(
                    f"[library] Wanted→TB leg: TorBox cooldown flag set "
                    f"({int(tb_cooldown)}s) — proceeding anyway; an add "
                    f"failure will confirm enforcement and back off")

        # --- RD leg availability -------------------------------------
        rd_key = load_secret_or_env('rd_api_key')
        rd_ok = bool(rd_key) and wanted_rd_recovery_enabled()
        rd_client = None
        if rd_ok:
            try:
                from utils.debrid_client import get_debrid_client
                rd_client, _svc = get_debrid_client(
                    service='realdebrid', api_key=rd_key)
            except Exception as e:
                logger.warning(f"[library] Wanted→RD leg disabled — client "
                               f"init failed: {type(e).__name__}")
                rd_client = None
            if rd_client is None or not getattr(rd_client, 'configured', False):
                rd_ok = False

        if not tb_ok and not rd_ok:
            return

        now = time.monotonic()
        # Expire per-title cooldown/miss entries.
        with self._wanted_memo_lock:
            self._wanted_tb_cooldown = {
                k: t for k, t in self._wanted_tb_cooldown.items()
                if now - t < self._WANTED_TB_RECOVERY_COOLDOWN
            }
            self._wanted_rd_miss = {
                k: t for k, t in self._wanted_rd_miss.items()
                if now - t < self._WANTED_RD_MISS_TTL
            }
            self._wanted_no_results = {
                k: t for k, t in self._wanted_no_results.items()
                if now - t < self._WANTED_TB_RECOVERY_COOLDOWN
            }

        tb_budget = wanted_tb_recovery_max_per_scan()
        rd_budget = wanted_rd_recovery_max_per_scan()
        deadline = now + self._WANTED_RECOVERY_BUDGET_SECONDS
        tb_added = 0
        rd_added = 0
        rd_attempts = 0

        # Build the target list: released movie ghosts + show ghosts (probing
        # the first still-missing episode, defaulting to S01E01) + season
        # targets for partially-present shows (probing a season pack that
        # covers the season's missing episodes).
        #
        # INVARIANT (movies + ghost shows): this pass and
        # _search_for_missing_episodes have inverse source gates — that pass
        # skips source == 'wanted' items, this one's ghost targets are ONLY
        # them.  The complementary gates prevent the two acquisition paths
        # (arr search vs. direct TB add) from double-acquiring the same
        # title in one scan; if either gate changes, re-check the other.
        #
        # DELIBERATE EXCEPTION (season targets): partial-show seasons are
        # dual-path — gap-fill keeps firing arr searches for the same
        # missing episodes while this pass probes TB for a season pack.
        # The mechanisms differ (indexer grab → blackhole vs. direct
        # debrid add → symlink) and the overlap self-corrects: if both
        # succeed, the arr import simply replaces the symlinked file.
        # Season targets exist precisely because gap-fill's indexer pool
        # has been failing on these seasons.
        targets = []  # (media_type, item, season, episode)
        for m in movies:
            if m.get('source') != 'wanted':
                continue
            if not m.get('is_available', True):
                continue  # unreleased — Torrentio won't have a real release
            if not m.get('imdb_id'):
                continue
            targets.append(('movie', m, None, None))
        for s in shows:
            if s.get('source') != 'wanted':
                continue
            if not s.get('imdb_id'):
                continue
            # A "wanted" show ghost is a Sonarr-monitored series with ZERO
            # on-disk episodes (_apply_sonarr_wanted_shows only injects
            # fully-absent series), so every episode is genuinely missing and
            # S01E01 is always a legitimate entry point.  We still prefer the
            # exact first missing episode when TMDB has the episode list —
            # _compute_missing_episodes returns [] both for "all present"
            # (impossible here — the ghost has nothing) and for "TMDB cache
            # miss / don't know", and in either case the S01E01 fallback is
            # correct for a fully-absent ghost rather than a blind guess.
            season, episode = 1, 1
            try:
                miss = self._compute_missing_episodes(s)
            except Exception:
                miss = []
            if miss:
                season, episode = miss[0]
            targets.append(('series', s, season, episode))

        # Season targets: partially-present shows whose seasons still have
        # missing aired episodes.  One cached pack add fills every gap in
        # the season via the symlink phase (which skips episodes already on
        # disk), so these drain scattered per-episode holes the arr's
        # indexers never close.  TB-only — probing a whole pack through
        # RD's add-poll-delete cycle is expensive at a measured ~8% hit
        # rate — and appended AFTER the ghost targets so whole-title
        # recovery keeps first claim on the shared TB budget.  Sorted by
        # missing-count descending so the biggest gaps drain first.
        if tb_ok and wanted_season_recovery_enabled():
            season_targets = []
            for s in shows:
                if s.get('source') == 'wanted':
                    continue  # ghosts are handled above
                if not s.get('imdb_id'):
                    continue
                me = s.get('missing_episodes')
                if not (isinstance(me, int) and me > 0):
                    continue
                try:
                    miss = self._compute_missing_episodes(s)
                except Exception:
                    miss = []
                if not miss:
                    continue  # [] = "don't know" — never probe blind
                by_season = {}
                for sn, en in miss:
                    by_season.setdefault(sn, []).append(en)
                for sn, eps in by_season.items():
                    season_targets.append(
                        (len(eps), ('season', s, sn, min(eps))))
            season_targets.sort(key=lambda t: t[0], reverse=True)
            targets.extend(t for _, t in season_targets)

        try:
            from utils.blocklist import is_blocked as _is_blocked
        except ImportError:
            _is_blocked = None

        for media_type, item, season, episode in targets:
            tb_active = tb_ok and tb_added < tb_budget
            rd_active = rd_ok and rd_attempts < rd_budget
            if not tb_active and not rd_active:
                break
            now_loop = time.monotonic()
            if now_loop > deadline:
                logger.info("[library] Wanted recovery time budget exhausted, "
                            "deferring remainder to next scan")
                break
            imdb = item['imdb_id']
            if media_type == 'movie':
                key = imdb
            elif media_type == 'season':
                # Distinct namespace from the per-episode ghost keys so a
                # pack probe and an episode probe of the same season never
                # share memo/ledger state.
                key = f"{imdb}:{season}:pack"
            else:
                key = f"{imdb}:{season}:{episode}"
            # Terminal give-up: this title's top releases are confirmed
            # doomed on BOTH providers (RD filter-blocked + TB uncached)
            # across WANTED_FILTER_GIVEUP_STRIKES passes.  Stop probing —
            # re-probing a filter-blocked release never changes.  The strike
            # is keyed per-probe (``key`` is the imdb for movies but
            # ``imdb:season:episode`` for shows), so one blocked episode
            # never abandons the whole series and per-episode strikes climb
            # independently — one bump per pass, not one per episode.  An
            # operator Retry (clear_retry_state) or the ledger prune sweep
            # clears the strike and gives the title a fresh chance.
            if imdb and _ledger.get(f'wantedblock:{key}') >= WANTED_FILTER_GIVEUP_STRIKES:
                continue
            if key in self._wanted_no_results:
                continue
            # Per-leg gates — a TB cooldown must never suppress the RD leg
            # (or vice versa); each leg answers only to its own memo.
            # Season targets are TB-only (see the target-build comment).
            rd_try = (rd_active and media_type != 'season'
                      and key not in self._wanted_rd_miss)
            tb_try = tb_active and key not in self._wanted_tb_cooldown
            if not rd_try and not tb_try:
                continue

            media_title = item.get('title')
            try:
                results = _search.search_torrentio(
                    imdb,
                    media_type='series' if media_type == 'season'
                    else media_type,
                    season=season, episode=episode)
            except Exception:
                results = []
            # Blocklisted hashes were rejected for a reason (bad release,
            # prior filter block, ...) — never re-add them via this pass.
            if results and _is_blocked is not None:
                try:
                    results = [r for r in results
                               if not _is_blocked(r.get('info_hash', ''))]
                except Exception:
                    logger.warning("[library] Wanted recovery blocklist "
                                   "filter failed — using unfiltered results")
            # Torrentio stream lists are imdb-keyed but polluted with
            # mislabeled uploads; without this check a 2160p "Fight Club"
            # junk entry outranks the real 1080p release and gets added
            # in the wrong title's slot.
            if results:
                if not media_title:
                    logger.warning(
                        f"[library] Wanted recovery: no title on ghost "
                        f"{key} — skipping title-match filter")
                else:
                    try:
                        kept = [r for r in results
                                if _release_matches_title(
                                    r.get('title') or '', media_title,
                                    media_year=item.get('year'))]
                        dropped = len(results) - len(kept)
                        if dropped:
                            logger.info(
                                f"[library] Wanted recovery dropped {dropped}"
                                f"/{len(results)} title-mismatched result(s) "
                                f"for '{media_title}'")
                        results = kept
                    except Exception:
                        logger.warning(
                            "[library] Wanted recovery title-match filter "
                            "failed — using unfiltered results")
            # Season targets: keep only releases that actually cover the
            # target — a pack marking this season (or a range spanning it)
            # or an exact-episode release for the first missing episode.
            # Everything else (wrong season, wrong episode, no TV marker)
            # is junk from Torrentio's imdb-keyed result list.
            season_cls = {}
            if media_type == 'season' and results:
                kept = []
                for r in results:
                    ih = r.get('info_hash') or ''
                    cls = _release_covers_season(
                        r.get('title') or '', season, episode)
                    if ih and cls:
                        season_cls[ih] = cls
                        kept.append(r)
                if len(kept) < len(results):
                    logger.debug(
                        f"[library] Wanted season recovery dropped "
                        f"{len(results) - len(kept)}/{len(results)} "
                        f"non-covering result(s) for '{media_title}' "
                        f"S{season:02d}")
                results = kept
            if not results:
                self._memo_wanted(self._wanted_no_results, key)
                continue

            # Rank by quality score then seeds; cap the TB cache fan-out.
            results.sort(
                key=lambda r: ((r.get('quality') or {}).get('score', 0),
                               r.get('seeds', 0)),
                reverse=True,
            )
            if media_type == 'season':
                # Stable re-sort: packs before single-episode releases (one
                # pack add covers the whole season's gaps); the quality
                # order above is preserved within each class.
                results.sort(
                    key=lambda r: season_cls.get(
                        r.get('info_hash') or '') != 'pack')

            ep_str = (f"S{season:02d}E{episode:02d}"
                      if media_type in ('series', 'season') else None)

            # ---- TB leg first: cache probe + add ---------------------
            # TB gets first claim: ~93-97% of the live Wanted backlog is
            # TB-cached vs a measured ~8% RD probe hit rate, so the RD leg
            # is demoted to a fallback that only fires on titles TB does
            # NOT have cached (or whose cache state it can't answer for
            # this pass).
            tb_uncached = False  # TB probe ran cleanly and found nothing
            if tb_try:
                probe = results[:self._WANTED_TB_MAX_PROBES]
                hashes = [r['info_hash'] for r in probe]
                tb_probe_ok = True
                try:
                    cmap = _search.check_debrid_cache(
                        hashes, service='torbox', api_key=tb_key)
                except Exception:
                    cmap = {}
                    tb_probe_ok = False
                cached = next(
                    (r for r in probe if cmap.get(r['info_hash'])), None)
                if cached:
                    # season_cls is empty for non-season targets, so this
                    # collapses to ep_str everywhere else.
                    add_ep = (f"S{season:02d}"
                              if season_cls.get(cached['info_hash']) == 'pack'
                              else ep_str)
                    try:
                        result = _search.add_to_debrid(
                            cached['info_hash'],
                            title=cached.get('title') or media_title or '',
                            media_title=media_title,
                            episode=add_ep,
                            service='torbox',
                            api_key=tb_key,
                            cause='wanted_tb_recovered',
                            source='library',
                        )
                    except Exception as e:
                        logger.error(
                            f"[library] Wanted→TB recovery add failed for "
                            f"{media_title!r}: {type(e).__name__}")
                        result = {}
                    # Cool the title down regardless of outcome: a success
                    # is added (no need to re-add), a duplicate is already
                    # present, and a transient failure shouldn't be retried
                    # until the window elapses.
                    self._memo_wanted(self._wanted_tb_cooldown, key)
                    if result.get('success'):
                        tb_added += 1
                    elif not result.get('duplicate'):
                        # Enforcement check: an add failure WHILE the account
                        # cooldown flag is set means TB is actually rejecting
                        # creates (HTTP 400 DOWNLOAD_SERVER_ERROR surfaces as
                        # a generic failure here) — stop burning the remaining
                        # TB budget this pass.  A failure with no cooldown
                        # flag is a transient error; keep going.
                        try:
                            from utils.blackhole import _check_torbox_cooldown
                            # force_refresh: the advisory pre-check may have
                            # cached 0 seconds ago; a cooldown armed by THIS
                            # add's rejection would be masked by that cache,
                            # so re-query /user/me.
                            tb_cooldown = _check_torbox_cooldown(
                                tb_key, force_refresh=True)
                        except Exception:
                            # Can't distinguish enforcement from a transient
                            # add failure — keep the leg alive (blast radius
                            # is the small remaining per-scan budget) but
                            # leave a trace for the operator.
                            logger.warning(
                                "[library] Wanted→TB leg: cooldown re-probe "
                                "failed after an add failure — cannot "
                                "confirm enforcement, leg stays active")
                            tb_cooldown = 0
                        if tb_cooldown > 0:
                            logger.info(
                                f"[library] Wanted→TB leg backing off — add "
                                f"failed with account cooldown active "
                                f"({int(tb_cooldown)}s); enforcement "
                                f"confirmed")
                            tb_ok = False
                    # TB-cached: the RD leg never fires on this title — even
                    # after a transient add failure the cached TB copy is the
                    # cheaper retry once the cooldown memo expires.
                    continue
                tb_uncached = tb_probe_ok

            # ---- RD leg: fallback for TB-uncached titles --------------
            # The probe needs a full poll window of headroom left in the
            # pass budget — starting with a clamped-short window would
            # misclassify cached titles as 7-day misses.  Checked HERE (not
            # at loop top) because the TB leg just consumed some of it.
            if rd_try and (deadline - time.monotonic()
                           < self._WANTED_RD_READY_TIMEOUT):
                rd_try = False
            rd_outcome = None
            if rd_try:
                top_hash = (results[0].get('info_hash') or '').lower()
                if top_hash and _ledger.get(f'rdblock:{top_hash}') > 0:
                    # RD's keyword filter already rejected this exact hash on
                    # a previous pass — the verdict is deterministic and
                    # persisted, so treat it as a confirmed filter block
                    # WITHOUT re-adding (the in-memory miss memo dies on
                    # restart, which was re-adding known-blocked hashes 6-7
                    # times per title across deploys).  Costs no API call and
                    # no budget; still pairs with the TB probe above so the
                    # ``wantedblock:`` give-up strike keeps accruing.
                    logger.debug(f"[library] Wanted→RD probe skipped for "
                                 f"{media_title!r} — hash {top_hash[:8]}… has "
                                 f"a persisted filter-block verdict")
                    rd_outcome = 'filter_blocked'
                else:
                    rd_outcome = self._wanted_rd_probe_add(
                        rd_client, rd_key, results[0],
                        media_title, ep_str, key)
                    if rd_outcome != 'skipped':
                        # Only real add attempts count against the budget —
                        # local skips (dedup hit, listing unavailable) cost
                        # no RD API adds.
                        rd_attempts += 1
                if rd_outcome == 'recovered':
                    rd_added += 1
                    self._memo_wanted(self._wanted_tb_cooldown, key)
                    continue

            # ---- combine the legs' verdicts ---------------------------
            if not tb_try:
                # The TB leg couldn't run this pass (disabled / budget spent
                # / per-title cooldown): the "uncached" half of the give-up
                # signature can't be confirmed, so don't accrue a strike.
                # Fall back to the 7-day RD-miss memo so a blocked release
                # isn't re-probed against RD every scan while TB is
                # unavailable.
                if rd_outcome == 'filter_blocked':
                    self._memo_wanted(self._wanted_rd_miss, key)
                continue
            if rd_outcome == 'filter_blocked' and imdb and tb_uncached:
                # Both providers confirmed failing THIS pass — accrue a
                # terminal give-up strike.  Deliberately DON'T cool the
                # title down: leaving both memos clear lets each leg
                # re-confirm on the next scan so the strike count can
                # climb to WANTED_FILTER_GIVEUP_STRIKES (a few passes),
                # after which the top-of-loop guard skips it for good.
                self._record_wanted_filter_giveup(
                    key, imdb, media_title, ep_str)
            elif rd_outcome == 'filter_blocked' and imdb:
                # TB probe errored — can't confirm uncached, so no strike;
                # cool down + memo RD-miss to avoid re-probing the blocked
                # release every scan while TB's cache state is unknown.
                self._memo_wanted(self._wanted_tb_cooldown, key)
                self._memo_wanted(self._wanted_rd_miss, key)
            else:
                # Plain TB miss (RD didn't filter-block) — cool down so we
                # don't re-walk it before the window elapses.
                self._memo_wanted(self._wanted_tb_cooldown, key)

        if rd_added or tb_added:
            logger.info(f"[library] Wanted recovery added {rd_added} release(s) "
                        f"to RealDebrid and {tb_added} to TorBox")
        # Persist the memo state so a restart resumes the drain where it
        # left off instead of re-probing the whole backlog.
        self._persist_wanted_memos()

    def _record_wanted_filter_giveup(self, key, imdb, media_title, ep_str):
        """Bump the persistent both-providers give-up strike for a Wanted
        ghost, logging the terminal event exactly once when the count first
        reaches ``WANTED_FILTER_GIVEUP_STRIKES`` (after which the loop's top
        guard skips the probe, so no further strikes accrue).

        ``key`` is the per-probe key (imdb for movies, ``imdb:season:episode``
        for shows) so a series accrues strikes per-episode and one blocked
        episode never terminates the whole show."""
        from utils import attempt_ledger as _ledger
        try:
            strikes = _ledger.bump(f'wantedblock:{key}')
        except Exception:
            return
        if strikes != WANTED_FILTER_GIVEUP_STRIKES:
            # Below threshold: still accruing.  Once it equals the threshold
            # the top-of-loop guard skips the probe on every later pass, so the
            # count freezes here and this event fires exactly once — until the
            # prune sweep expires the key, after which it re-climbs from 1 and
            # re-crosses at ``==`` again (never ``>``).
            return
        logger.info(f"[library] Wanted recovery gave up on {media_title!r} "
                    f"({imdb}) — filter-blocked on RD and uncached on TorBox "
                    f"across {strikes} passes")
        try:
            from utils import history as _hist
            _hist.log_event(
                'debrid_add_failed', media_title or imdb,
                episode=ep_str,
                detail='Recovery gave up — filter-blocked on RealDebrid and '
                       'uncached on TorBox',
                source='library',
                media_title=media_title,
                meta={'cause': _hist.CAUSE_WANTED_FILTER_GIVEUP,
                      'imdb_id': imdb,
                      'strikes': strikes},
            )
        except Exception:
            pass

    def _wanted_rd_probe_add(self, rd_client, rd_key, release,
                             media_title, ep_str, key):
        """RD leg of the Wanted recovery pass — the add IS the cache probe.

        Adds ``release`` to RD and polls briefly for ``downloaded``
        (instantly ready = cached).  On a hit the torrent is kept, verified
        against the May-2026 keyword filter via ``probe_file``, and a
        ``wanted_rd_recovered`` history event is logged.  On a genuine cache
        miss the probe add is deleted (by ``attempt_add_rescue``), the title
        is memoized in ``_wanted_rd_miss``, and a ``wanted_rd_uncached``
        event records the measurement.

        Returns a state string — the caller uses it for flow, budget
        accounting, and terminal give-up strike counting:

        * ``'recovered'`` — kept, filter-clean recovery; skip the TB leg.
        * ``'filter_blocked'`` — RD's keyword filter rejected this release
          (add-time 403/451, or an instant-ready add whose file 451s).
          Deterministic and permanent for this release, so — unlike a plain
          cache miss — it is NOT memoized here; the caller pairs it with the
          TB leg's result to bump the ``wantedblock:<imdb>`` give-up strike.
          Counts against the per-scan budget; falls through to the TB leg.
        * ``'attempted'`` — an RD add fired but didn't stick for a
          retry-worthy reason (genuine cache miss, add error).  Memoized in
          ``_wanted_rd_miss`` when it's a miss.  Counts against the budget;
          falls through to the TB leg.
        * ``'skipped'`` — no RD API add happened (no hash, dedup hit,
          account listing unavailable).  Free — doesn't burn budget.

        Never raises.
        """
        from utils import search as _search
        from utils.debrid_routing import attempt_add_rescue, make_preexisting_check
        from utils.debrid_client import RD_READY_STATES, RD_FAIL_STATES

        info_hash = (release.get('info_hash') or '').lower()
        title = release.get('title') or media_title or ''
        if not info_hash:
            return 'skipped'

        # Dedup against the RD account listing.  Two distinct outcomes:
        #
        # * Listing unavailable (API blip): bail without adding.  RD hands
        #   back the PRE-EXISTING torrent's id when the hash is already on
        #   the account, so an add we can't dedup risks polling a torrent
        #   we didn't create — and then deleting a user's in-flight
        #   download on "miss".  Transient, no memo.
        # * Hash already on the account (parked blocked entry, or content
        #   the mount has under a mismatched name): skip and memoize —
        #   re-adding won't change anything within the miss window.
        #
        # force_refresh bypasses the 30s dedup-cache TTL.  The staleness
        # window is CORRELATED, not random: the user manually adding a
        # popular release is exactly when the probe grabs the same hash —
        # a stale "not on account" answer here hands the probe the user's
        # own torrent id (RD hash-dedup) and the miss-cleanup delete
        # would destroy their in-flight download.
        try:
            existing = _search._existing_hashes('realdebrid', rd_key,
                                                force_refresh=True)
        except Exception:
            existing = None
        if existing is None:
            logger.info(f"[library] Wanted→RD probe skipped for "
                        f"{media_title!r} — RD account listing unavailable")
            return 'skipped'
        if info_hash in existing:
            logger.info(f"[library] Wanted→RD probe skipped for "
                        f"{media_title!r} — hash already on RD account")
            self._memo_wanted(self._wanted_rd_miss, key)
            return 'skipped'

        # Re-issue selectFiles (once) if the add's own selection fired
        # during magnet conversion and didn't stick — otherwise a cached
        # torrent can park in waiting_files_selection until the timeout.
        reselected = []

        def _status_fn(client, tid):
            st = (client.torrent_status(tid) or '').strip().lower()
            if st == 'waiting_files_selection' and not reselected:
                reselected.append(True)
                client.select_files(tid)
            return st

        # Last line of defense behind the force-refreshed dedup listing
        # above: the listing can still miss (RD truncates /torrents at
        # 2500 entries, dropping the OLDEST; or the user adds between
        # our listing and our add).  RD's addMagnet hash-dedup then
        # hands back the user's own torrent id, and every cleanup
        # delete on that id destroys THEIR torrent.  Anything whose
        # ``added`` timestamp predates the probe wasn't created by us.
        # Unknown (info fetch failed, timestamp missing/unparseable) →
        # treat as pre-existing: an orphaned probe entry beats
        # destroying user data.
        probe_start = time.time()
        # Narrowed to RD's field only: this leg always holds an RD client
        # (``created_at`` is TB's field, never present here).  The factory
        # default checks both — don't rely on it in case a future third
        # field would change precedence.
        _preexisting = make_preexisting_check(
            probe_start, grace=self._WANTED_RD_PREEXISTING_GRACE,
            timestamp_fields=('added',))

        core = attempt_add_rescue(
            info_hash, 'torbox',
            alt_debrid='realdebrid',
            # RD's cache endpoint is dead — bypass the probe; the add
            # itself (instant-ready or not) is the cache check.
            cache_probe=lambda *_: True,
            alt_client=rd_client,
            status_fn=_status_fn,
            ready_states=RD_READY_STATES,
            fail_states=RD_FAIL_STATES,
            ready_timeout=self._WANTED_RD_READY_TIMEOUT,
            poll_interval=self._WANTED_RD_POLL_INTERVAL,
            preexisting_check=_preexisting,
            logger_prefix='library.wanted_rd',
        )

        if not core.get('rescued'):
            reason = core.get('reason', 'error')
            if reason in ('never_ready', 'failed_state'):
                # Genuine cache miss — memoize + log the measurement.
                self._memo_wanted(self._wanted_rd_miss, key)
                self._log_wanted_rd_miss(
                    title, media_title, ep_str, info_hash,
                    reason=core.get('state') or reason)
            elif core.get('http_status') in (403, 451):
                # RD's keyword filter rejected at addMagnet time (HTTP
                # 451/403) — deterministic and permanent for this release.
                # Report it as a filter block (not a plain miss) so the
                # caller can pair it with the TB leg and bump the terminal
                # give-up strike; the 7-day _wanted_rd_miss memo is wrong
                # here (re-probing a filter-blocked release never changes).
                self._log_wanted_rd_miss(
                    title, media_title, ep_str, info_hash,
                    reason='infringing_add')
                self._persist_rd_filter_block(info_hash)
                return 'filter_blocked'
            # Other add_error/add_failed are transient RD-side failures —
            # no memo, the title gets another shot on a later scan.
            return 'attempted'

        tid = core.get('alt_torrent_id', '')

        # Add-time filter-block check: RD accepts and instantly "readies"
        # filter-gated content, then 451s the actual file.  Catch it now so
        # a blocked add is never counted as a recovery (and the TB leg gets
        # its chance immediately instead of after the next health sweep).
        try:
            probe = rd_client.probe_file(tid)
        except Exception:
            probe = {'status': 'unknown'}
        if probe.get('status') == 'blocked':
            # Same pre-existing guard as the miss-cleanup deletes: if
            # RD's hash-dedup handed us the user's own (blocked) torrent,
            # leave it in place — the filter verdict still stands.
            try:
                skip_delete = _preexisting(rd_client, tid)
            except Exception:
                skip_delete = True
            if skip_delete:
                logger.warning(
                    f"[library] Wanted→RD probe: torrent {tid} predates "
                    f"this probe (RD hash-dedup) — leaving the blocked "
                    f"entry in place"
                )
            else:
                try:
                    rd_client.delete_torrent(tid)
                except Exception:
                    pass
            # Same permanent-filter class as the add-time 403/451 branch —
            # report as a filter block (no _wanted_rd_miss memo) so the
            # caller can accrue the terminal give-up strike.
            probe_reason = probe.get('reason', 'blocked')
            self._log_wanted_rd_miss(
                title, media_title, ep_str, info_hash,
                reason=probe_reason)
            # Only persist a restart-surviving verdict for a genuine keyword
            # filter block (``infringing_file``).  ``probe_file`` also returns
            # status='blocked' for a bare HTTP 404 (``not_found`` — the file
            # is transiently unresolvable, e.g. RD hoster indexing lag on a
            # just-added torrent); persisting THAT would lock a possibly-cached
            # hash out of the RD leg for 30 idle days on a transient error.
            if probe_reason == 'infringing_file':
                self._persist_rd_filter_block(info_hash)
            return 'filter_blocked'
        # 'unknown' (network blip, 5xx) → keep the torrent; the health
        # sweep re-probes it on its own cycle.

        try:
            _search.remember_added_hash('realdebrid', info_hash)
        except Exception:
            pass
        try:
            from utils import history as _hist
            _hist.log_event(
                'debrid_add', title,
                episode=ep_str,
                detail='Recovered Wanted — instantly ready on realdebrid',
                source='library',
                media_title=media_title,
                meta={'cause': _hist.CAUSE_WANTED_RD_RECOVERED,
                      'info_hash': info_hash,
                      'service': 'realdebrid',
                      'torrent_id': tid},
            )
        except Exception:
            pass
        logger.info(f"[library] Wanted→RD recovery: {media_title!r} "
                    f"instantly ready on RealDebrid ({info_hash[:8]}…)")
        return 'recovered'

    @staticmethod
    def _persist_rd_filter_block(info_hash):
        """Persist RD's keyword-filter verdict for *info_hash*.

        The verdict is deterministic per hash (RD 451s the same release
        forever), but the in-memory ``_wanted_rd_miss`` memo dies on
        restart — observed live as 6-7 redundant probe adds of the same
        blocked hash per title across a multi-deploy weekend.  The
        ``rdblock:`` ledger key survives restarts; the recovery loop reads
        it to treat the hash as filter-blocked without re-adding.  Cleared
        by the ledger's idle-decay prune, which doubles as the safety
        valve in case RD ever unblocks a hash."""
        try:
            from utils import attempt_ledger as _ledger
            _ledger.bump(f'rdblock:{(info_hash or "").lower()}')
        except Exception:
            pass

    def _log_wanted_rd_miss(self, title, media_title, ep_str, info_hash,
                            reason):
        try:
            from utils import history as _hist
            _hist.log_event(
                'debrid_add_failed', title,
                episode=ep_str,
                detail=f'Wanted recovery probe — {reason}',
                source='library',
                media_title=media_title,
                meta={'cause': _hist.CAUSE_WANTED_RD_UNCACHED,
                      'info_hash': info_hash,
                      'service': 'realdebrid',
                      'reason': reason},
            )
        except Exception:
            pass

    def _memo_wanted(self, memo, key):
        """Stamp ``memo[key] = now`` under the memo lock (writer side)."""
        with self._wanted_memo_lock:
            memo[key] = time.monotonic()

    def wanted_recovery_snapshot(self):
        """Point-in-time copy of the Wanted-recovery memos for cross-thread
        readers (the /api/stuck collector).

        Keys are ``imdb`` (movies) or ``imdb:season:episode`` (shows).
        Values are converted from monotonic stamps to age-in-seconds so
        callers never have to compare against this process's monotonic
        clock themselves.
        """
        now = time.monotonic()
        with self._wanted_memo_lock:
            return {
                'no_results': {k: now - t for k, t in self._wanted_no_results.items()},
                'rd_miss': {k: now - t for k, t in self._wanted_rd_miss.items()},
                'tb_cooldown': {k: now - t for k, t in self._wanted_tb_cooldown.items()},
            }

    def clear_wanted_memos(self, imdb_id):
        """Drop every recovery memo for ``imdb_id`` (movie key or any
        ``imdb:s:e`` episode key) so the next scan's recovery pass retries
        the title immediately.  Returns the number of keys removed."""
        if not imdb_id:
            return 0
        prefix = f"{imdb_id}:"
        removed = 0
        with self._wanted_memo_lock:
            for d in (self._wanted_no_results, self._wanted_rd_miss,
                      self._wanted_tb_cooldown):
                for k in [k for k in d if k == imdb_id or k.startswith(prefix)]:
                    del d[k]
                    removed += 1
        if removed:
            # Rewrite the snapshot so a restart can't resurrect a memo the
            # operator just cleared via the Stuck-tab Retry action.
            self._persist_wanted_memos()
        return removed

    def reload_wanted_memos(self):
        """Merge the on-disk memo snapshot into the live dicts (used by
        backup restore).  ``_load_wanted_memos`` is additive and
        TTL-aware, so calling it on a running scanner is safe."""
        self._load_wanted_memos()

    @staticmethod
    def _wanted_memos_file():
        return os.path.join(
            os.environ.get('CONFIG_DIR', '/config'), 'wanted_memos.json')

    def _persist_wanted_memos(self):
        """Snapshot the three Wanted-recovery memo dicts to disk.

        One atomic write per recovery pass (plus one per operator Retry).
        Ages are stored in seconds (monotonic stamps are meaningless across
        restarts) together with a wall-clock ``saved_at`` so the loader can
        account for the downtime between save and reload.  Best-effort: a
        read-only or full ``/config`` must never break a scan.
        """
        payload = {
            'version': 1,
            'saved_at': time.time(),
            'memos': self.wanted_recovery_snapshot(),
        }
        try:
            import json as _json
            from utils.file_utils import atomic_write
            with atomic_write(self._wanted_memos_file()) as fh:
                _json.dump(payload, fh, separators=(',', ':'))
        except (OSError, ValueError, TypeError) as e:
            logger.debug(f"[library] Could not persist wanted memos: {e}")

    def _load_wanted_memos(self):
        """Rehydrate the memo dicts from the persisted snapshot (init-time).

        Each stored age is grown by the wall-clock downtime since
        ``saved_at``; entries already past their TTL are dropped rather than
        loaded.  Clock skew makes the ages approximate, which is fine — the
        memos are API-pressure hints, not correctness state (worst case a
        title is re-probed a little early or late).  Missing/corrupt file
        loads nothing.
        """
        import json as _json
        path = self._wanted_memos_file()
        try:
            with open(path, encoding='utf-8') as fh:
                payload = _json.load(fh)
        except (OSError, ValueError):
            return
        if not isinstance(payload, dict):
            return
        memos = payload.get('memos')
        saved_at = payload.get('saved_at')
        if not isinstance(memos, dict) or not isinstance(saved_at, (int, float)):
            return
        downtime = max(0.0, time.time() - saved_at)
        now = time.monotonic()
        ttls = {
            'no_results': self._WANTED_TB_RECOVERY_COOLDOWN,
            'rd_miss': self._WANTED_RD_MISS_TTL,
            'tb_cooldown': self._WANTED_TB_RECOVERY_COOLDOWN,
        }
        loaded = 0
        with self._wanted_memo_lock:
            for name, target in (('no_results', self._wanted_no_results),
                                 ('rd_miss', self._wanted_rd_miss),
                                 ('tb_cooldown', self._wanted_tb_cooldown)):
                stored = memos.get(name)
                if not isinstance(stored, dict):
                    continue
                ttl = ttls[name]
                for key, age in stored.items():
                    if not isinstance(key, str) or not isinstance(age, (int, float)):
                        continue
                    current_age = age + downtime
                    if current_age < 0 or current_age >= ttl:
                        continue
                    target[key] = now - current_age
                    loaded += 1
        if loaded:
            logger.info(f"[library] Restored {loaded} Wanted-recovery memo(s) "
                        f"from {path}")

    def _clear_resolved_pending(self, shows, movies):
        """Clear pending entries that are resolved or stale.

        Resolved: direction is 'to-debrid' and source is now 'debrid'/'both',
        or direction is 'to-local' and source is now 'local'/'both'.

        Stale: episode no longer exists in any source (deleted or never
        existed). Note: episodes whose source is the opposite of the goal
        (e.g., 'to-debrid' but still 'local') are legitimately in-progress
        and must NOT be cleared.

        Runs unconditionally on every scan.
        """
        from utils.library_prefs import get_all_pending, clear_pending
        from utils import attempt_ledger

        # Time-decay the give-up ledger once per scan: a title abandoned 30+
        # days ago gets a fresh budget if it ever flows back through a loop.
        attempt_ledger.prune(30 * 24 * 3600)

        pending = get_all_pending()
        if not pending:
            return

        # Build a source lookup: {norm_title: {(season, episode): source}}
        # Also register alias keys so pending entries stored under either
        # the debrid or local title can be resolved.
        source_map = {}
        for show in shows:
            norm = _normalize_title(show['title'])
            ep_sources = {}
            for sd in show.get('season_data', []):
                for ep in sd.get('episodes', []):
                    ep_sources[(sd['number'], ep['number'])] = ep.get('source', '')
            source_map[norm] = ep_sources
            for alias in self._alias_norms.get(norm, ()):
                if alias not in source_map:
                    source_map[alias] = ep_sources

        for movie in movies:
            norm = _normalize_title(movie['title'])
            movie_sources = {(0, 0): movie.get('source', '')}
            source_map[norm] = movie_sources
            for alias in self._alias_norms.get(norm, ()):
                if alias not in source_map:
                    source_map[alias] = movie_sources

        # Snapshot pending; clear_pending re-reads under lock so concurrent writes are safe
        for norm_title, entry in list(pending.items()):
            direction = entry.get('direction', '')
            episodes = entry.get('episodes', [])
            sources = source_map.get(norm_title, {})
            resolved = []
            # If the title itself isn't in the library at all, clear everything
            title_exists = norm_title in source_map
            for ep in episodes:
                key = (ep.get('season', 0), ep.get('episode', 0))
                src = sources.get(key, '')
                if direction == 'to-debrid' and src in ('debrid', 'both'):
                    resolved.append(ep)
                elif direction == 'debrid-unavailable' and src in ('debrid', 'both'):
                    resolved.append(ep)  # content appeared on debrid after all
                elif direction in ('to-local', 'to-local-fallback') and src in ('local', 'both'):
                    resolved.append(ep)
                elif direction == 'to-any' and src in ('debrid', 'local', 'both'):
                    resolved.append(ep)  # gap-fill: any source satisfies the user story
                elif not src and not title_exists:
                    # Title gone from library entirely — stale
                    resolved.append(ep)
            if resolved:
                logger.debug(f"[library] Clearing {len(resolved)} pending episode(s) for "
                             f"{norm_title!r} (direction={direction!r})")
                clear_pending(norm_title, resolved)
                # Content landed on debrid — drop the force-grab give-up counter
                # so a future re-acquisition gets a fresh attempt budget. Reset
                # both key forms (movie `fg:{norm}` and per-season `fg:{norm}:sN`);
                # a missing key is a safe no-op.
                if direction in ('to-debrid', 'debrid-unavailable'):
                    attempt_ledger.reset(f"fg:{norm_title}")
                    for ep in resolved:
                        attempt_ledger.reset(f"fg:{norm_title}:s{ep.get('season', 0)}")

    def _escalate_stuck_pending(self):
        """Mark to-debrid entries as debrid-unavailable after threshold days.

        When debrid simply doesn't have the content, stop retrying and let
        the user decide to download locally.
        """
        from utils.library_prefs import get_all_pending, mark_debrid_unavailable

        pending = get_all_pending()
        if not pending:
            return

        now = datetime.now(timezone.utc)
        threshold_days = self._debrid_unavailable_days
        escalated = []

        for norm_title, entry in list(pending.items()):
            if entry.get('direction') != 'to-debrid':
                continue
            created = entry.get('created')
            if not created:
                continue
            try:
                created_dt = datetime.fromisoformat(created)
                if created_dt.tzinfo is None:
                    created_dt = created_dt.replace(tzinfo=timezone.utc)
                age_days = (now - created_dt).days
                if age_days >= threshold_days:
                    mark_debrid_unavailable(norm_title)
                    escalated.append(norm_title)
                    logger.info(
                        f"[library] Marked {norm_title!r} as debrid-unavailable "
                        f"after {age_days} days"
                    )
            except (ValueError, TypeError):
                pass

        if escalated:
            if _history:
                from utils import retry_counter as _rc
                for t in escalated:
                    # Pick up the retry-cycle count from whichever arr has
                    # been searching for this title.  We don't know the id
                    # here, but the counter is keyed by (service, id);
                    # record the threshold itself as the meaningful datum.
                    meta = {'cause': 'debrid_unavailable_marked',
                            'age_days': threshold_days}
                    _history.log_event('debrid_unavailable', t, source='library',
                                       detail=f'Marked debrid-unavailable after {threshold_days}+ days',
                                       meta=meta)
            try:
                from utils.notifications import notify
                summary = ', '.join(escalated[:5])
                if len(escalated) > 5:
                    summary += f', +{len(escalated) - 5} more'
                notify('debrid_unavailable',
                       f'Debrid Unavailable ({len(escalated)})',
                       f'Content not found on debrid after {threshold_days} days — retries '
                       f'continue in arr: {summary}',
                       level='warning')
            except Exception as e:
                logger.warning(f"[library] Could not send debrid_unavailable notification: {e}")

    def _warn_stalled_pending(self):
        """Send notification for items pending > PENDING_WARNING_HOURS.

        Notifies once per item (tracks via 'warned_at' field on pending entry).
        Warns for 'to-debrid' AND 'to-any' directions — both represent items
        actively searching for content.  'to-any' entries are never escalated
        (never become 'debrid-unavailable') but still need a visibility signal
        for the user after the threshold so a long-standing gap doesn't go
        unnoticed.  Runs after _escalate_stuck_pending() so newly-escalated
        items are already direction='debrid-unavailable' and won't be warned.
        """
        from utils.library_prefs import get_all_pending, set_pending_warned

        threshold_hours = self._pending_warning_hours
        if threshold_hours <= 0:
            return  # disabled

        pending = get_all_pending()
        if not pending:
            return

        now = datetime.now(timezone.utc)
        warned = []

        for norm_title, entry in list(pending.items()):
            if entry.get('direction') not in ('to-debrid', 'to-any'):
                continue
            if entry.get('warned_at'):
                continue  # already warned
            created = entry.get('created')
            if not created:
                continue
            try:
                created_dt = datetime.fromisoformat(created)
                if created_dt.tzinfo is None:
                    created_dt = created_dt.replace(tzinfo=timezone.utc)
                age_hours = (now - created_dt).total_seconds() / 3600
                if age_hours >= threshold_hours:
                    warned.append((norm_title, entry))
                    logger.info(
                        f"[library] Pending warning for {norm_title!r} "
                        f"after {age_hours:.0f} hours"
                    )
            except (ValueError, TypeError):
                pass

        if warned:
            try:
                from utils.notifications import notify
                lines = []
                for title, entry in warned[:5]:
                    err = entry.get('last_error', '')
                    line = title
                    if err:
                        line += f' ({err})'
                    lines.append(line)
                summary = ', '.join(lines)
                if len(warned) > 5:
                    summary += f', +{len(warned) - 5} more'
                notify('pending_warning',
                       f'Pending Warning ({len(warned)})',
                       f'Items stuck searching for {threshold_hours}+ hours: {summary}',
                       level='warning')
                for norm_title, _ in warned:
                    set_pending_warned(norm_title)
            except Exception as e:
                logger.warning(f"[library] Could not send pending_warning notification: {e}")

    def _recover_local_fallback_routing(self, shows, movies):
        """Re-route series/movies back to debrid after local-fallback downloads complete.

        When a local-fallback download completes (episode has source 'local'
        or 'both'), clear the pending entry.  If ALL local-fallback episodes
        for a title are resolved, re-route the series back to debrid.
        """
        from utils.library_prefs import get_all_pending, clear_pending

        pending = get_all_pending()
        if not pending:
            return

        # Build source map
        source_map = {}
        for show in shows:
            norm = _normalize_title(show['title'])
            ep_sources = {}
            for sd in show.get('season_data', []):
                for ep in sd.get('episodes', []):
                    ep_sources[(sd['number'], ep['number'])] = ep.get('source', '')
            source_map[norm] = ep_sources
            for alias in self._alias_norms.get(norm, ()):
                if alias not in source_map:
                    source_map[alias] = ep_sources

        for movie in movies:
            norm = _normalize_title(movie['title'])
            source_map[norm] = {(0, 0): movie.get('source', '')}
            for alias in self._alias_norms.get(norm, ()):
                if alias not in source_map:
                    source_map[alias] = {(0, 0): movie.get('source', '')}

        titles_to_reroute = []

        for norm_title, entry in list(pending.items()):
            if entry.get('direction') != 'to-local-fallback':
                continue

            sources = source_map.get(norm_title, {})
            episodes = entry.get('episodes', [])
            resolved = []

            for ep in episodes:
                key = (ep.get('season', 0), ep.get('episode', 0))
                src = sources.get(key, '')
                if src in ('local', 'both'):
                    resolved.append(ep)

            if resolved:
                clear_pending(norm_title, resolved)
                logger.info(
                    f"[library] Local-fallback resolved for {norm_title!r}: "
                    f"{len(resolved)} episode(s)"
                )

            # All episodes resolved → re-route back to debrid
            if len(resolved) >= len(episodes):
                titles_to_reroute.append(norm_title)

        if not titles_to_reroute:
            return

        # Re-route resolved titles back to debrid
        from utils.arr_client import get_download_service

        try:
            show_client, show_svc = get_download_service('show')
        except Exception:
            show_client, show_svc = None, None
        try:
            movie_client, movie_svc = get_download_service('movie')
        except Exception:
            movie_client, movie_svc = None, None

        show_norms = {_normalize_title(s['title']): s for s in shows}
        movie_norms = {_normalize_title(m['title']): m for m in movies}

        def _resolve_via_aliases(norm, mapping):
            """Look up *norm* in *mapping*, falling back through aliases.

            Pending entries written before a canonical-title rename are keyed
            by the parsed norm; current items are keyed by the canonical norm.
            The alias map bridges the two.
            """
            item = mapping.get(norm)
            if item is None:
                for alias in self._alias_norms.get(norm, ()):
                    item = mapping.get(alias)
                    if item is not None:
                        break
            return item

        for norm_title in titles_to_reroute:
            # Try as show
            show = _resolve_via_aliases(norm_title, show_norms)
            if show and show_client and show_svc == 'sonarr':
                try:
                    series = show_client.find_series_in_library(title=show['title'])
                    if series:
                        show_client._ensure_debrid_routing(series)
                        logger.info(
                            f"[library] Re-routed {show['title']!r} back to debrid "
                            f"after local-fallback completed"
                        )
                except Exception as e:
                    logger.warning(f"[library] Failed to re-route {norm_title!r}: {e}")

            # Try as movie
            movie = _resolve_via_aliases(norm_title, movie_norms)
            if movie and movie_client and movie_svc == 'radarr':
                try:
                    radarr_movie = movie_client.find_movie_in_library(title=movie['title'])
                    if radarr_movie:
                        movie_client._ensure_debrid_routing(radarr_movie)
                        logger.info(
                            f"[library] Re-routed {movie['title']!r} back to debrid "
                            f"after local-fallback completed"
                        )
                except Exception as e:
                    logger.warning(f"[library] Failed to re-route movie {norm_title!r}: {e}")

    def _cleanup_broken_debrid_symlinks(self):
        """Remove broken debrid symlinks from local library directories.

        When a debrid torrent is removed or replaced (e.g. user switches from
        a buffering 2160p to a 1080p), the symlinks created by
        ``_create_debrid_symlinks`` become broken.  This method detects those
        broken links and removes them so the next symlink-creation pass can
        lay down fresh links for replacement content.

        Walks all configured (debrid-target-prefix, rclone-mount) pairs so
        TorBox-routed symlinks under ``<RD_base>_torbox`` get cleanup too —
        otherwise broken TB symlinks accumulate on disk forever (plan 39
        dual-debrid gap).  Real files and non-debrid symlinks are never modified.

        Must run BEFORE ``_create_debrid_symlinks`` in the effects pipeline.
        """
        if not str(os.environ.get('BLACKHOLE_SYMLINK_ENABLED', '')).lower() == 'true':
            return
        rclone_mount = os.environ.get('BLACKHOLE_RCLONE_MOUNT', '').strip()
        symlink_base = os.environ.get('BLACKHOLE_SYMLINK_TARGET_BASE', '').strip()
        if not rclone_mount or not symlink_base:
            return

        rclone_real = os.path.realpath(rclone_mount)
        # Build (target-prefix, rclone-mount-real-path) pairs for every
        # configured debrid so the check-path translation step finds the
        # right mount per symlink.  RD is the primary pair; TB is appended
        # when its mount discovers cleanly.  Same pattern as
        # ``_create_debrid_symlinks::_mount_target_pairs``.
        debrid_pair_list = [(os.path.normpath(symlink_base).rstrip(os.sep) + os.sep, rclone_real)]
        tb_real = None
        try:
            from utils.debrid_routing import TORBOX, symlink_target_base_for_debrid
            tb_base = (symlink_target_base_for_debrid(TORBOX) or '').strip()
            tb_mount = self._discover_torbox_mount() if tb_base else None
            if tb_base and tb_mount:
                tb_real = os.path.realpath(tb_mount)
                tb_prefix = os.path.normpath(tb_base).rstrip(os.sep) + os.sep
                if (tb_prefix, tb_real) not in debrid_pair_list:
                    debrid_pair_list.append((tb_prefix, tb_real))
        except Exception as exc:
            logger.debug("[library] TB cleanup-pair resolution failed: %s", exc)
        debrid_prefixes_only = tuple(p for p, _m in debrid_pair_list)

        # Per-mount health guard.  A missing or stalled/throttled FUSE mount
        # makes os.path.exists() return False for everything under it, which
        # would mass-delete valid symlinks.  Compute health PER MOUNT — RD and
        # TB fail independently (e.g. TB under a 429 read-throttle while RD is
        # fine), so a global "any mount up" check is not enough: the per-symlink
        # deletion below must skip symlinks routed to an unhealthy mount.  The
        # TB mount is flat (no category dirs); RD/Zurg is categorized.
        mount_health = {
            _m: _mount_has_content(_m, flat=(_m == tb_real))
            for _p, _m in debrid_pair_list
        }
        if not any(mount_health.values()):
            logger.debug("[library] No debrid mount has content — "
                         "skipping broken symlink cleanup")
            return

        removed = 0
        # Unique release names of deleted symlinks; fed to _attempt_arr_research
        # below so Sonarr/Radarr can re-search disappeared content.  Mirrors the
        # phase-3 step in scheduled_tasks.verify_symlinks — without this, the
        # library cleanup silently deletes broken symlinks and the arrs only
        # learn about it on their next disk scan.
        affected_releases = set()
        # Hoisted out of the per-symlink loop so an ImportError raises once and
        # loud instead of N times into a debug log.  Symmetric with the
        # post-loop _attempt_arr_research import below.
        try:
            from utils.scheduled_tasks import _extract_release_info
        except ImportError as exc:
            logger.warning(
                "[library] Could not import _extract_release_info — broken-symlink "
                "release tracking disabled this scan: %s", exc,
            )
            _extract_release_info = None

        for lib_path in (self._local_movies_path, self._local_tv_path):
            if not lib_path or not os.path.isdir(lib_path):
                continue
            try:
                with os.scandir(lib_path) as top:
                    for entry in top:
                        if not entry.is_dir(follow_symlinks=False):
                            continue
                        # Walk into subdirectories (Season dirs for TV, flat for movies)
                        for root, _dirs, files in os.walk(entry.path):
                            for fname in files:
                                fpath = os.path.join(root, fname)
                                if not os.path.islink(fpath):
                                    continue
                                target = os.readlink(fpath)
                                # Resolve relative symlinks to absolute
                                if not os.path.isabs(target):
                                    target = os.path.normpath(
                                        os.path.join(os.path.dirname(fpath), target)
                                    )
                                # Match the symlink target to one of the configured
                                # debrid (prefix, mount) pairs so RD and TB symlinks
                                # both get cleanup against the right mount.
                                matched_prefix = None
                                matched_mount = None
                                for _p, _m in debrid_pair_list:
                                    if target.startswith(_p):
                                        matched_prefix = _p
                                        matched_mount = _m
                                        break
                                if matched_prefix is None:
                                    continue
                                # Translate arr-namespace target to the per-debrid
                                # rclone mount for existence check.
                                check_path = os.path.normpath(
                                    matched_mount + os.sep + target[len(matched_prefix):]
                                )
                                # Ensure translated path stays within the mount
                                if not check_path.startswith(matched_mount + os.sep):
                                    continue
                                # Skip deletion when this symlink's mount is not
                                # confirmed healthy — os.path.exists() returns
                                # False for everything on a throttled/stalled
                                # mount, so deleting here would tear down valid
                                # symlinks (the TB-throttle symlink-thrash bug).
                                if not mount_health.get(matched_mount, False):
                                    continue
                                if not os.path.exists(check_path):
                                    # Re-verify still a symlink to narrow TOCTOU window
                                    if not os.path.islink(fpath):
                                        continue
                                    try:
                                        os.unlink(fpath)
                                        removed += 1
                                        logger.debug(
                                            "[library] Removed broken debrid symlink: %s -> %s",
                                            fpath, target,
                                        )
                                    except OSError as e:
                                        logger.debug("[library] Failed to remove broken symlink %s: %s", fpath, e)
                                        continue
                                    # Record for re-search after the loop.  WARNING (not
                                    # debug) because a parse failure here loses the
                                    # reconcile signal — the symlink is gone but the arr
                                    # won't be told until verify_symlinks runs (≤6 h).
                                    if _extract_release_info is not None:
                                        try:
                                            release_name, _, _ = _extract_release_info(
                                                target, debrid_prefixes_only
                                            )
                                        except Exception as exc:
                                            logger.warning(
                                                "[library] Could not parse release name from %s — "
                                                "arr re-search will be deferred to verify_symlinks: %s",
                                                target, exc,
                                            )
                                            release_name = None
                                        if release_name:
                                            affected_releases.add(release_name)
            except (PermissionError, OSError) as e:
                logger.debug("[library] Cannot scan %s for broken symlinks: %s", lib_path, e)

        if not removed:
            return

        logger.info("[library] Cleaned up %d broken debrid symlink(s) from local library", removed)

        # Trigger arr re-search for disappeared content so Sonarr/Radarr stop
        # serving phantom file records.  Gated on gap_fill_enabled() to honor
        # the same opt-out as verify_symlinks.  _attempt_arr_research has its
        # own retrigger cooldown so frequent library scans don't stampede.
        searched = 0
        if affected_releases and gap_fill_enabled():
            try:
                from utils.scheduled_tasks import _attempt_arr_research
            except ImportError:
                _attempt_arr_research = None
            if _attempt_arr_research:
                for release_name in affected_releases:
                    try:
                        if _attempt_arr_research(release_name):
                            searched += 1
                    except Exception as exc:
                        logger.warning(
                            "[library] Re-search failed for '%s': %s",
                            release_name, exc,
                        )

        if _history:
            try:
                _history.log_event(
                    'cleanup', 'Library Symlink Cleanup',
                    source='library_scan',
                    meta={
                        'cause': 'library_symlink_cleanup',
                        'deleted': removed,
                        'searched': searched,
                    },
                )
            except Exception as exc:
                logger.debug("[library] Failed to log cleanup event: %s", exc)

    def _create_debrid_symlinks(self, shows, movies, path_index):
        """Create local library symlinks for debrid-only content.

        When content exists on the debrid mount but has no local presence,
        create an organized symlink structure so Sonarr/Radarr can discover it:
          TV:     {local_tv}/Show Name (Year)/Season XX/filename.mkv
          Movies: {local_movies}/Movie Name (Year)/filename.mkv

        Directory names use the parsed torrent title — Sonarr/Radarr's import
        function will remap to canonical naming on import.

        Runs when BLACKHOLE_SYMLINK_ENABLED=true and the required paths are set.
        Idempotent — skips items that already have a local file or symlink.
        """
        if not str(os.environ.get('BLACKHOLE_SYMLINK_ENABLED', '')).lower() == 'true':
            return
        rclone_mount = os.environ.get('BLACKHOLE_RCLONE_MOUNT', '').strip()
        symlink_base = os.environ.get('BLACKHOLE_SYMLINK_TARGET_BASE', '').strip()
        if not rclone_mount or not symlink_base:
            return
        if not self._local_tv_path and not self._local_movies_path:
            return

        # Guard: if the local scan found zero local/both items, the network
        # mount is probably not ready.  Creating symlinks into an empty local
        # library would pollute it with debrid-only content and mask the real
        # local files once the mount recovers.
        has_local_movies = any(m.get('source') in ('local', 'both') for m in movies)
        has_local_shows = any(s.get('source') in ('local', 'both') for s in shows)
        if not has_local_movies and not has_local_shows:
            if self._last_had_local is True and not self._local_drop_alerted:
                logger.warning("[library] Local library content dropped to zero — "
                               "network mount may have failed")
                try:
                    from utils.notifications import notify
                    notify('health_error', 'Local Library Empty',
                           'Library scan found zero local content. '
                           'A network mount may have dropped.',
                           level='error')
                except Exception as exc:
                    logger.debug(f"[library] Failed to send mount-drop notification: {exc}")
                self._local_drop_alerted = True
            logger.info("[library] Skipping debrid symlink creation — local library appears empty "
                        "(network mount may not be ready)")
            return

        # Local content present — update baseline and reset alert state
        self._last_had_local = True
        self._local_drop_alerted = False

        real_mount = os.path.realpath(rclone_mount)

        # Per-debrid (rclone-mount, symlink-target-base) pairs.  RD/AD's
        # pair is always first (the primary).  When TorBox is configured
        # AND its mount is reachable, its pair is appended so episodes
        # that landed on the TB mount get symlinks pointing at the TB
        # target base — keeping the RD/TB split that Plex libraries
        # depend on.  Without this, the prefix-check below silently
        # skips every TB-source episode and only RD content ever gets
        # symlinked into the local arr library — the load-bearing
        # final step of plan 39 phase 4 that wasn't wired up.
        _mount_target_pairs = [(real_mount, symlink_base)]
        try:
            from utils.debrid_routing import TORBOX, symlink_target_base_for_debrid as _tb_base_for
            _tb_mount = self._discover_torbox_mount()
            if _tb_mount:
                _tb_real = os.path.realpath(_tb_mount)
                _tb_symlink_base = _tb_base_for(TORBOX)
                if _tb_symlink_base:
                    _mount_target_pairs.append((_tb_real, _tb_symlink_base))
        except Exception as e:
            logger.debug(f"[library] TB symlink pair resolution failed: {e}")

        def _resolve_symlink_target(real_debrid_path):
            """Return host-side symlink target for *real_debrid_path*.

            Searches per-debrid (mount, target_base) pairs in order and
            returns ``<base> + <suffix-under-that-mount>`` for the first
            match.  Returns ``None`` when the path is under no known
            debrid mount — caller should skip (don't create symlinks
            pointing at unmapped filesystems).
            """
            for _m, _b in _mount_target_pairs:
                if real_debrid_path.startswith(_m + os.sep) or real_debrid_path == _m:
                    return _b + real_debrid_path[len(_m):]
            return None

        created = 0
        phantom_sources = 0
        symlinked_shows = set()   # titles that got new symlinks
        symlinked_movies = set()  # titles that got new symlinks
        # Per-title details for the activity event: set of new basenames
        # linked this scan, their total size, and the best-guess "replaces"
        # filename pulled from the prior state.
        _symlink_new_files = {}   # title -> list[{'file', 'size'}]
        _symlink_replaces = {}    # title -> prior-basename (best guess)
        _symlink_is_upgrade = {}  # title -> bool (True iff prior state was non-empty)
        # Defensive getattr — tests bypass __init__ via LibraryScanner.__new__().
        _prior_symlinks = getattr(self, '_last_symlinked_files', None)
        if _prior_symlinks is None:
            _prior_symlinks = {}
            self._last_symlinked_files = _prior_symlinks
        # Reset the rescan-chain stash unconditionally so a previous scan's
        # event ids can never leak into this cycle's rescan calls — even
        # when this scan creates zero symlinks.
        self._pending_rescan_prior_ids = {}
        _symlink_years = {}       # title -> parsed year (for year-aware rescan matching)
        # canonical title -> parsed-folder title (when display was upgraded
        # via TMDB rename).  Lets the rescan-trigger TMDB cache fallback use
        # the parsed-folder norm — TMDB cache is keyed by parsed norm.
        _symlink_parsed = {}
        failed_titles = {}        # title -> last error string

        # Fetch arr libraries for canonical folder names and rescan IDs.
        # Index by both exact lowercase title and normalized title (stripped
        # of punctuation) so titles like "(500) Days of Summer" match
        # "500 Days of Summer" from the torrent folder name.
        sonarr_map = {}  # lowercase title -> info
        sonarr_map_norm = {}  # normalized title -> info
        radarr_map = {}
        radarr_map_norm = {}
        from utils.arr_client import get_download_service
        sonarr_fetch_failed = False
        radarr_fetch_failed = False
        try:
            client, svc = get_download_service('show')
            if client and svc == 'sonarr':
                # Share the series list with ``_apply_sonarr_monitored_filter``
                # via the scan-scoped TTL cache — one HTTP round-trip per
                # scan cycle instead of one per consumer.
                series_list = _get_sonarr_series_list(client)
                if series_list is None:
                    sonarr_fetch_failed = True
                else:
                    for s in series_list:
                        t = s.get('title', '')
                        if not t:
                            continue
                        p = s.get('path', '')
                        info = {
                            'folder': os.path.basename(p) if p else '',
                            'id': s.get('id'),
                            'tvdb_id': s.get('tvdbId'),
                            'tmdb_id': s.get('tmdbId'),
                            'client': client,
                        }
                        sonarr_map[t.lower()] = info
                        nk = _norm_for_matching(t)
                        if nk and nk not in sonarr_map_norm:
                            sonarr_map_norm[nk] = info
        except Exception as e:
            sonarr_fetch_failed = True
            logger.warning(f"[library] Could not fetch Sonarr library: {e}")
        try:
            client, svc = get_download_service('movie')
            if client and svc == 'radarr':
                # Share the movie list with _apply_radarr_wanted_movies
                # via the scan-scoped TTL cache — one HTTP round-trip
                # per scan cycle instead of one per consumer. Mirrors
                # the Sonarr pattern at line 3634.
                radarr_movies_list = _get_radarr_movies_list(client)
                if radarr_movies_list is None:
                    radarr_fetch_failed = True
                    radarr_movies_list = []
                for m in radarr_movies_list:
                    t = m.get('title', '')
                    if not t:
                        continue
                    p = m.get('path', '')
                    info = {
                        'folder': os.path.basename(p) if p else '',
                        'id': m.get('id'),
                        'tmdb_id': m.get('tmdbId'),
                        'client': client,
                    }
                    radarr_map[t.lower()] = info
                    nk = _norm_for_matching(t)
                    if nk and nk not in radarr_map_norm:
                        radarr_map_norm[nk] = info
        except Exception as e:
            radarr_fetch_failed = True
            logger.warning(f"[library] Could not fetch Radarr library: {e}")

        # Build TMDB ID → arr info maps for fallback matching when torrent
        # titles differ from TMDB titles (e.g. "F1 The Movie" vs "F1",
        # "Special Ops Lioness" vs "Lioness")
        radarr_by_tmdb = {}
        for info in radarr_map.values():
            tid = info.get('tmdb_id')
            if tid:
                radarr_by_tmdb[tid] = info
        sonarr_by_tmdb = {}
        for info in sonarr_map.values():
            tid = info.get('tmdb_id')
            if tid:
                sonarr_by_tmdb[tid] = info
        # Load cached TMDB IDs so we can translate Zurgarr titles → TMDB IDs
        from utils.tmdb import get_cached_tmdb_ids
        cached_tmdb_ids = get_cached_tmdb_ids()
        cached_tmdb_movies = cached_tmdb_ids.get('movies', {})
        cached_tmdb_shows = cached_tmdb_ids.get('shows', {})

        # Pre-load the full TMDB cache once for the scan-scoped prefix-match
        # fallback (the new final cascade step in dir-selection / rescan).
        # Without this, each invocation of _find_canonical_tmdb_via_prefix
        # re-reads /config/tmdb_cache.json from disk and re-acquires
        # _tmdb._cache_lock — multiplied by up to 4 cascade sites × N
        # symlinked items, that's hundreds of redundant reads per scan and
        # widens the lock window for concurrent TMDB writers.
        from utils import tmdb as _tmdb_mod
        try:
            with _tmdb_mod._cache_lock:
                _tmdb_full_cache = _tmdb_mod._load_cache()
        except Exception as e:
            logger.debug("[library] full TMDB cache load for prefix-match failed: %s", e)
            _tmdb_full_cache = {}

        # Lazy import (module-level would re-form the library ↔ blackhole cycle)
        from utils.blackhole import is_obfuscated_name as _is_obfuscated_name

        # --- Movies ---
        if self._local_movies_path:
            real_movies_root = os.path.realpath(self._local_movies_path)
            for movie in movies:
                if movie.get('source') not in ('debrid', 'both'):
                    continue
                mount_dir = movie.get('path')
                if not mount_dir:
                    continue

                title = movie['title']
                year = movie.get('year')

                # Anti-DMCA obfuscated payloads (hex name + tracker tag, e.g.
                # EZTV) parse into junk hex "movies" — never import them.
                # The blackhole monitor handles their real identity via the
                # .magnet-derived display name.
                if _is_obfuscated_name(title) or _is_obfuscated_name(os.path.basename(mount_dir)):
                    continue

                # Skip blocklisted items by mount folder name (full release name).
                # Only match on the release folder — not the parsed title — so
                # that blocking one quality/release doesn't block replacements
                # of the same movie (e.g. blocking a 2160p doesn't block a 1080p).
                if _blocklist:
                    mount_folder = os.path.basename(mount_dir)
                    if _blocklist.is_blocked_title(mount_folder):
                        continue

                arr_info = _match_arr_entry(
                    title, year, movie.get('_parsed_title'),
                    radarr_map, radarr_map_norm, radarr_by_tmdb,
                    cached_tmdb_movies, is_tv=False,
                    _tmdb_cache=_tmdb_full_cache,
                )
                if arr_info and arr_info['folder']:
                    movie_dir = arr_info['folder']
                else:
                    movie_dir = f"{title} ({year})" if year else title

                # For source='both', only create a symlink if Radarr's folder
                # has no media files.  This handles wrong-dir symlinks (the
                # movie lives in a differently-named dir) without creating
                # duplicates alongside real local files.
                if movie.get('source') == 'both':
                    target_dir = os.path.join(self._local_movies_path, movie_dir)
                    if os.path.isdir(target_dir) and self._has_media_files(target_dir):
                        continue

                # Find the largest media file in the torrent folder.  Searches
                # one level deep so nested-folder torrents (video tucked inside
                # a release-named subdir) still get symlinked — without this
                # they're detected as on-debrid but never linkable, showing
                # "available" in zurgarr yet "missing" in Radarr.
                media_rel, media_size = _find_largest_movie_video(mount_dir)
                if not media_rel:
                    continue
                # Local library uses a flat filename (the basename); the arr
                # re-imports by content, so the nesting need not be preserved.
                media_file = os.path.basename(media_rel)

                local_path = os.path.join(
                    self._local_movies_path, movie_dir, media_file
                )

                real_local_dir = os.path.realpath(
                    os.path.join(self._local_movies_path, movie_dir)
                )
                if not real_local_dir.startswith(real_movies_root + os.sep) and real_local_dir != real_movies_root:
                    logger.warning("[library] Refusing movie symlink outside local library: %r", local_path)
                    continue

                if os.path.islink(local_path) or os.path.exists(local_path):
                    continue

                real_debrid = os.path.realpath(os.path.join(mount_dir, media_rel))
                symlink_target = _resolve_symlink_target(real_debrid)
                if symlink_target is None:
                    continue

                try:
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                    os.symlink(symlink_target, local_path)
                    created += 1
                    symlinked_movies.add(title)
                    # Record the new basename + size for activity event meta.
                    prior = _prior_symlinks.get(title) or set()
                    if prior and media_file not in prior and title not in _symlink_replaces:
                        # Best guess for "replaced" filename: the one prior
                        # entry we saw last time (movies have a single file).
                        _symlink_replaces[title] = sorted(prior)[0]
                    if prior:
                        _symlink_is_upgrade[title] = True
                    entry = {'file': media_file}
                    if media_size and media_size > 0:
                        entry['size'] = media_size
                    _symlink_new_files.setdefault(title, []).append(entry)
                    if year:
                        _symlink_years[title] = year
                    parsed_t = movie.get('_parsed_title')
                    if parsed_t and parsed_t != title:
                        _symlink_parsed[title] = parsed_t
                except FileExistsError:
                    pass
                except OSError as e:
                    failed_titles[title] = str(e)
                    logger.warning(
                        "[library] Failed to create movie symlink for %r: %s",
                        title, e
                    )

        # --- TV Shows ---
        # Pre-compute max season per title for season-aware TMDB fallback
        _show_max_season = {}
        for _s in shows:
            sdata = _s.get('season_data', [])
            if sdata:
                _show_max_season[_s['title']] = max(
                    (sd['number'] for sd in sdata if sd.get('number')), default=0,
                )

        if self._local_tv_path:
            real_tv_root = os.path.realpath(self._local_tv_path)

            for show in shows:
                norm = _normalize_title(show['title'])
                title = show['title']
                year = show.get('year')
                # Same obfuscated-payload guard as the movie loop above
                # (title AND mount folder, for Sonarr/Radarr parity).
                if _is_obfuscated_name(title) or _is_obfuscated_name(os.path.basename(show.get('path', ''))):
                    continue
                arr_info = _match_arr_entry(
                    title, year, show.get('_parsed_title'),
                    sonarr_map, sonarr_map_norm, sonarr_by_tmdb,
                    cached_tmdb_shows, is_tv=True,
                    max_season=_show_max_season.get(title, 0),
                    _tmdb_cache=_tmdb_full_cache,
                )
                if arr_info and arr_info['folder']:
                    show_dir = arr_info['folder']
                else:
                    show_dir = f"{title} ({year})" if year else title

                for sd in show.get('season_data', []):
                    snum = sd['number']
                    season_dir = f"Season {snum:02d}"
                    for ep in sd.get('episodes', []):
                        if ep.get('source') != 'debrid':
                            continue
                        enum = ep['number']
                        debrid_path = path_index.get((norm, snum, enum))
                        if not debrid_path:
                            continue

                        # Skip blocklisted items by release folder name.
                        # Only match on the release folder — not the parsed title — so
                        # that blocking one release doesn't block replacements.
                        # debrid_path may be .../release/Season N/ep.mkv or .../release/ep.mkv;
                        # climb past Season dirs to reach the actual release folder.
                        if _blocklist:
                            parent = os.path.dirname(debrid_path)
                            parent_name = os.path.basename(parent)
                            if _SEASON_DIR_PATTERN.match(parent_name):
                                release_folder = os.path.basename(os.path.dirname(parent))
                            else:
                                release_folder = parent_name
                            if _blocklist.is_blocked_title(release_folder):
                                continue

                        filename = os.path.basename(debrid_path)
                        local_path = os.path.join(
                            self._local_tv_path, show_dir, season_dir, filename
                        )

                        # Validate output stays within local library root
                        real_local_dir = os.path.realpath(
                            os.path.join(self._local_tv_path, show_dir, season_dir)
                        )
                        if not real_local_dir.startswith(real_tv_root + os.sep) and real_local_dir != real_tv_root:
                            logger.warning("[library] Refusing symlink outside local library: %r", local_path)
                            continue

                        if os.path.islink(local_path) or os.path.exists(local_path):
                            continue

                        # Enumeration (TB mylist API / Zurg WebDAV) can report
                        # folder names the FUSE layer renames (e.g. TorBox
                        # strips '&' from on-disk folders), so path_index may
                        # carry paths that don't exist on the mount. A symlink
                        # to such a path is born broken and churns
                        # create→cleanup every scan. The mount is ground
                        # truth: skip sources that don't resolve. (exists()
                        # also returns False on a dead/ENOTCONN mount —
                        # skipping there is benign: nothing is created or
                        # deleted, and the next healthy scan links normally.)
                        if not os.path.exists(debrid_path):
                            phantom_sources += 1
                            if phantom_sources <= 5:
                                logger.warning(
                                    "[library] Skipping symlink for %r S%02dE%02d: "
                                    "source does not exist on mount: %r",
                                    title, snum, enum, debrid_path,
                                )
                            continue

                        # Translate mount path to Sonarr/arr namespace
                        real_debrid = os.path.realpath(debrid_path)
                        symlink_target = _resolve_symlink_target(real_debrid)
                        if symlink_target is None:
                            continue

                        try:
                            os.makedirs(os.path.dirname(local_path), exist_ok=True)
                            os.symlink(symlink_target, local_path)
                            created += 1
                            symlinked_shows.add(title)
                            prior = _prior_symlinks.get(title) or set()
                            if prior and filename not in prior:
                                _symlink_is_upgrade[title] = True
                                if title not in _symlink_replaces:
                                    # First prior file we saw — used by the
                                    # "Upgraded: old.mkv → new.mkv" UI string.
                                    # Arbitrary but deterministic choice for
                                    # shows, where the prior set may contain
                                    # many episodes.
                                    _symlink_replaces[title] = sorted(prior)[0]
                            try:
                                ep_size = os.path.getsize(debrid_path)
                            except OSError:
                                ep_size = 0
                            entry = {'file': filename}
                            if ep_size > 0:
                                entry['size'] = ep_size
                            _symlink_new_files.setdefault(title, []).append(entry)
                            if year:
                                _symlink_years[title] = year
                            parsed_t = show.get('_parsed_title')
                            if parsed_t and parsed_t != title:
                                _symlink_parsed[title] = parsed_t
                        except FileExistsError:
                            pass
                        except OSError as e:
                            failed_titles[title] = str(e)
                            logger.warning(
                                "[library] Failed to create symlink for %r S%02dE%02d: %s",
                                title, snum, enum, e
                            )

        if phantom_sources > 5:
            logger.warning(
                "[library] Skipped %d symlink(s) total whose enumerated "
                "source path does not exist on the mount",
                phantom_sources,
            )
        if created:
            logger.info(f"[library] Created {created} debrid symlink(s) in local library")
            # Cause picker: first-scan-after-restart bypasses upgrade heuristic
            # because we have no prior state to diff against — labeling those
            # symlinks as "upgrades" would mis-attribute every existing file
            # after a container restart.
            state_init = getattr(self, '_state_was_bootstrapped', False)
            symlink_event_ids = {}  # title -> event id (for rescan chaining)
            if _history:
                for t in sorted(symlinked_shows | symlinked_movies):
                    entries = _symlink_new_files.get(t, [])
                    is_show = t in symlinked_shows
                    total_size = sum(e.get('size', 0) for e in entries) or None
                    primary = entries[0] if entries else {}
                    if state_init:
                        cause = 'library_state_init'
                    elif _symlink_is_upgrade.get(t):
                        cause = 'library_upgrade_replaced'
                    else:
                        cause = 'library_new_import'
                    meta = {'cause': cause,
                            'count': len(entries),
                            'files': [e['file'] for e in entries[:10]]}
                    if primary.get('file'):
                        meta['file'] = primary['file']
                    if total_size:
                        meta['size_bytes'] = total_size
                    if _symlink_replaces.get(t):
                        meta['replaces'] = _symlink_replaces[t]
                    if is_show:
                        meta['media_type'] = 'show'
                    else:
                        meta['media_type'] = 'movie'
                    ev_id = _history.log_event('symlink_created', t, source='library',
                                               detail='Debrid symlink(s) created in local library',
                                               meta=meta)
                    if ev_id:
                        symlink_event_ids[t] = ev_id
            # Persist updated state so subsequent scans can diff.  Also
            # prune titles that no longer exist in the current debrid
            # library so the file doesn't grow unbounded over months of
            # additions and removals.  `current_titles` is the union of
            # every show/movie title the scanner just inspected.
            # Guard: a test scanner built via __new__() may not have a
            # state path configured — skip persistence in that case.
            state_path = getattr(self, '_scan_state_path', None)
            if state_path:
                try:
                    import json as _json
                    from utils import file_utils as _fu
                    for t, entries in _symlink_new_files.items():
                        _prior_symlinks.setdefault(t, set()).update(
                            e['file'] for e in entries if e.get('file')
                        )
                    current_titles = {s['title'] for s in shows} | {
                        m['title'] for m in movies
                    }
                    # Drop titles absent from the current scan — if the
                    # user removed the movie or it was never re-seen on
                    # the mount, the stored basenames can't help classify
                    # future upgrades for it.
                    for stale in [t for t in _prior_symlinks if t not in current_titles]:
                        _prior_symlinks.pop(stale, None)
                    payload = {
                        'titles': {t: sorted(s) for t, s in _prior_symlinks.items()},
                    }
                    with _fu.atomic_write(state_path) as fh:
                        fh.write(_json.dumps(payload, separators=(',', ':')))
                    self._state_was_bootstrapped = False
                except Exception as e:
                    logger.warning(f"[library] Failed to persist scan state: {e}")
            # Stash the event ids for rescan-trigger chaining (read by the
            # caller inside scan() — see _trigger_rescans).
            self._pending_rescan_prior_ids = symlink_event_ids
            # Batch notification for symlink_created
            try:
                from utils.notifications import notify
                all_titles = sorted(symlinked_shows | symlinked_movies)
                summary = ', '.join(all_titles[:5])
                if len(all_titles) > 5:
                    summary += f', +{len(all_titles) - 5} more'
                notify('symlink_created',
                       f'Debrid Symlinks Created ({created})',
                       f'Created {created} symlink(s): {summary}')
            except Exception:
                pass

        if failed_titles:
            if _history:
                for t, err in failed_titles.items():
                    _history.log_event('symlink_failed', t, source='library',
                                       detail=f'Symlink creation failed: {err}',
                                       meta={'cause': 'symlink_create_failed',
                                             'error': err})
            try:
                from utils.notifications import notify
                titles = sorted(failed_titles)[:5]
                summary = ', '.join(titles)
                if len(failed_titles) > 5:
                    summary += f', +{len(failed_titles) - 5} more'
                notify('symlink_failed',
                       f'Symlink Failed ({len(failed_titles)})',
                       f'Failed to create symlinks: {summary}',
                       level='warning')
            except Exception:
                pass

        if created:
            # Plan 41 phase B.2 — NFS attribute-cache race mitigation.
            # When Sonarr/Radarr lives on a different host from the symlink
            # target and reaches it via an NFS share, the arr's view of the
            # share is cached by the kernel (default 30-60s attribute TTL).
            # A rescan fired immediately after symlink creation walks the
            # directory before the cache refreshes, sees nothing new, and
            # completes without imports — the file only lands on the next
            # 1h library_scan cycle (which then re-triggers the rescan and
            # this time succeeds).  Sleeping briefly here lets NFS see the
            # new symlinks before the arr stat()s them.
            nfs_delay = _resolve_nfs_rescan_delay()
            # Only sleep when at least one arr is configured AND has matching
            # symlinks — otherwise the sleep stalls the scan loop with no
            # corresponding rescan fire (e.g. Radarr-only user just got show
            # symlinks and SONARR_URL is unset; the rescan loop would warn
            # and skip).  Reviewer feedback (code-reviewer Phase B LOW #2).
            will_rescan_shows = bool(symlinked_shows and os.environ.get('SONARR_URL'))
            will_rescan_movies = bool(symlinked_movies and os.environ.get('RADARR_URL'))
            if nfs_delay > 0 and (will_rescan_shows or will_rescan_movies):
                logger.info(
                    f"[library] Sleeping {nfs_delay}s before arr rescans to let "
                    f"NFS attribute cache invalidate (LIBRARY_RESCAN_NFS_DELAY)"
                )
                # NOTE: this sleep is NOT interruptible by SIGTERM — the
                # scanner runs in a daemon thread, so the worst case on
                # shutdown is the rescan trigger never fires for the
                # symlinks just created.  Next container startup's
                # library_scan cycle re-discovers them and triggers the
                # rescan properly; no data loss.  Adding cooperative
                # shutdown (via a ``_stop_event`` on ``LibraryScanner``)
                # is deferred to plan 40 since it requires broader
                # restructuring of the scanner threading model.
                time.sleep(nfs_delay)

            # Trigger arr rescans so Sonarr/Radarr discover the new files.
            # Exception safety: the stash was reset to {} at the top of this
            # method, so even if the rescan loop raises the next scan won't
            # pick up stale ids.  No finally clause needed.
            if symlinked_shows and not sonarr_map:
                if sonarr_fetch_failed:
                    logger.warning(
                        "[library] Created show symlinks but could not fetch Sonarr library "
                        "(API unreachable?) — rescans skipped"
                    )
                elif os.environ.get('SONARR_URL'):
                    logger.warning(
                        "[library] Created show symlinks but Sonarr library is empty — "
                        "rescans skipped"
                    )
                else:
                    logger.warning(
                        "[library] Created show symlinks but SONARR_URL is not configured — "
                        "Sonarr won't discover new files until its next scheduled disk scan. "
                        "Set SONARR_URL and SONARR_API_KEY for automatic rescans."
                    )
            for title in symlinked_shows:
                info = _match_arr_entry(
                    title, _symlink_years.get(title), _symlink_parsed.get(title),
                    sonarr_map, sonarr_map_norm, sonarr_by_tmdb,
                    cached_tmdb_shows, is_tv=True,
                    max_season=_show_max_season.get(title, 0),
                    _tmdb_cache=_tmdb_full_cache,
                )
                if info and info.get('id') and info.get('client'):
                    try:
                        prior = (getattr(self, '_pending_rescan_prior_ids', {}) or {}).get(title)
                        info['client'].rescan_series(info['id'], media_title=title,
                                                     cause='post_symlink_rescan',
                                                     prior_event_id=prior)
                        # Successful symlink + rescan — drop any lingering
                        # retry-cycle counter so future searches don't show
                        # stale "retry #47" annotations for a satisfied item.
                        try:
                            from utils import retry_counter as _rc
                            _rc.reset('sonarr', info['id'])
                        except Exception as e:
                            logger.debug(f"[library] retry_counter reset failed for sonarr:{info['id']}: {e}")
                        logger.info(f"[library] Triggered Sonarr rescan for {title}")
                    except Exception as e:
                        logger.warning(f"[library] Sonarr rescan failed for {title}: {e}")
                elif sonarr_map:
                    logger.warning(f"[library] Could not match '{title}' to a Sonarr series — rescan skipped")
            if symlinked_movies and not radarr_map:
                if radarr_fetch_failed:
                    logger.warning(
                        "[library] Created movie symlinks but could not fetch Radarr library "
                        "(API unreachable?) — rescans skipped"
                    )
                elif os.environ.get('RADARR_URL'):
                    logger.warning(
                        "[library] Created movie symlinks but Radarr library is empty — "
                        "rescans skipped"
                    )
                else:
                    logger.warning(
                        "[library] Created movie symlinks but RADARR_URL is not configured — "
                        "Radarr won't discover new files until its next scheduled disk scan. "
                        "Set RADARR_URL and RADARR_API_KEY for automatic rescans."
                    )
            for title in symlinked_movies:
                info = _match_arr_entry(
                    title, _symlink_years.get(title), _symlink_parsed.get(title),
                    radarr_map, radarr_map_norm, radarr_by_tmdb,
                    cached_tmdb_movies, is_tv=False,
                    _tmdb_cache=_tmdb_full_cache,
                )
                if info and info.get('id') and info.get('client'):
                    try:
                        prior = (getattr(self, '_pending_rescan_prior_ids', {}) or {}).get(title)
                        info['client'].rescan_movie(info['id'], media_title=title,
                                                    cause='post_symlink_rescan',
                                                    prior_event_id=prior)
                        try:
                            from utils import retry_counter as _rc
                            _rc.reset('radarr', info['id'])
                        except Exception as e:
                            logger.debug(f"[library] retry_counter reset failed for radarr:{info['id']}: {e}")
                        logger.info(f"[library] Triggered Radarr rescan for {title}")
                    except Exception as e:
                        logger.warning(f"[library] Radarr rescan failed for {title}: {e}")
                elif radarr_map:
                    logger.warning(f"[library] Could not match '{title}' to a Radarr movie — rescan skipped")

    # Category names that indicate TV/show content
    _SHOW_CATEGORIES = {'shows', 'tv', 'anime', 'series', 'television'}
    # Internal Zurg directories to always skip
    _SKIP_CATEGORIES = {'__all__', '__unplayable__'}

    def _scan_mount(self, mount_path, deadline=None, source_debrid=None, flat_layout=False):
        """Scan all category directories on the mount and aggregate by title.

        Debrid mounts have one folder per torrent, so the same show appears
        many times (one per grabbed episode/season pack). This method collects
        episode IDs from every folder, then groups by normalized title so each
        show becomes a single entry with correct source_debrid badge.
        Movies are also deduplicated by title.

        ``source_debrid`` (plan 39 phase 4) tags every returned item with
        the provider name so the UI can distinguish RD from TB content.
        Defaults to the resolved primary debrid (AD-only and TB-only setups
        get the correct badge; pre-fix this was hard-coded to ``realdebrid``
        which mislabelled non-RD primaries).  The second pass over the alt
        mount explicitly passes ``source_debrid='torbox'``.

        ``flat_layout=True`` (plan 39 phase 4 follow-up) treats *mount_path*
        as a single category root — releases live directly under it with
        no ``shows/movies/anime/__all__`` subdivision.  This matches TorBox's
        WebDAV layout; Zurg-backed mounts (RD/AD) keep the categorized
        default.  Pre-fix this method assumed 2-level structure unconditionally,
        so TB scans iterated each release folder AS A CATEGORY and looked
        for sub-folders inside (finding only media files) — every TB show
        and movie except the few with internal subdirs was silently dropped.
        """
        if source_debrid is None:
            from utils.debrid_routing import resolve_primary
            source_debrid = resolve_primary() or 'realdebrid'
        from utils.blackhole import is_obfuscated_name as _is_obfuscated_name

        # Reset the per-scan truncation flag; set True below if the walk is
        # cut short (deadline or a listing error). The caller checks this to
        # avoid letting a partial scan drop titles.
        self._last_scan_mount_truncated = False

        if flat_layout:
            # Sentinel '' so the join below evaluates to ``mount_path`` itself.
            scan_dirs = ['']
            logger.debug(f"[library] Scanning flat mount root: {mount_path}")
        else:
            try:
                categories = []
                with os.scandir(mount_path) as it:
                    for entry in it:
                        if entry.is_dir(follow_symlinks=False):
                            categories.append(entry.name)
            except (PermissionError, OSError) as e:
                logger.warning(f"[library] Cannot list mount {mount_path}: {e}")
                self._last_scan_mount_truncated = True
                return [], []

            non_special = [c for c in categories if c not in self._SKIP_CATEGORIES]
            scan_dirs = non_special if non_special else [c for c in categories if c == '__all__']

            if not scan_dirs:
                logger.warning("[library] No directories found on mount")
                return [], []

            logger.debug(f"[library] Scanning mount categories: {scan_dirs}")

        # Collect raw per-folder data
        show_groups = {}   # normalized_title -> {title, year, episodes, path}
        movie_groups = {}  # normalized_title -> {title, year, path}
        timed_out = False

        for category in scan_dirs:
            cat_path = mount_path if flat_layout else os.path.join(mount_path, category)
            # Flat layout has no category hint; rely on per-folder episode
            # detection.  Categorized: 'shows'/'tv'/'anime' etc. pre-classify.
            category_is_shows = (not flat_layout) and category.lower() in self._SHOW_CATEGORIES
            try:
                with os.scandir(cat_path) as it:
                    for entry in it:
                        if deadline is not None and time.monotonic() > deadline:
                            logger.warning("[library] Timeout during mount scan")
                            timed_out = True
                            self._last_scan_mount_truncated = True
                            break
                        if not entry.is_dir(follow_symlinks=False):
                            continue
                        if entry.name.lower() in _SKIP_FOLDERS:
                            continue
                        if _is_obfuscated_name(entry.name):
                            logger.debug(
                                f"[library] Skipping obfuscated mount folder: {entry.name}"
                            )
                            continue
                        title, year = _parse_folder_name(entry.name)
                        if not title:
                            continue
                        episodes = _collect_episodes(entry.path)
                        is_show = len(episodes) > 0

                        # Plan 41 phase B.1 — folder-name TV-marker
                        # fallback.  On TB's flat layout the folder name
                        # often carries Sxx / Sxx-Syy / Season N markers
                        # without per-episode tags, and the files inside
                        # may not be cached yet (or got sanitised by the
                        # indexer to drop SxxExx).  ``_collect_episodes``
                        # returns empty in that case; without this
                        # fallback the entry buckets as a movie and
                        # cascades into wasted Radarr API calls + gap-
                        # fill searches that loop indefinitely.
                        #
                        # We flip ``is_show=True`` WITHOUT injecting
                        # synthetic episode entries — those would
                        # surface as "Episode 0" placeholders in the
                        # library UI's per-episode breakdown.  The
                        # show-entry bucket downstream gets a 0-episode
                        # show that the next scan cycle (after TB
                        # finishes caching) fills in with real
                        # ``SxxExx`` files; until then the Sonarr
                        # rescan trigger still fires correctly because
                        # it's keyed on title, not episode count.
                        if not is_show and _detect_tv_marker(entry.name):
                            is_show = True
                            logger.debug(
                                f"[library] Classifying {entry.name!r} as TV via "
                                f"folder-name marker (no SxxExx files found inside)"
                            )

                        if not is_show and category_is_shows:
                            # Zurg says show but no S##E## episodes found.
                            # Check top level AND immediate subdirs for
                            # recognized media extensions (.mkv, .mp4, etc.).
                            # Present → trust Zurg (anime/non-standard naming).
                            # Absent → demote to movie (BluRay disc rips have
                            # only .m2ts under BDMV/STREAM/, not in MEDIA_EXTENSIONS).
                            has_media = False
                            try:
                                with os.scandir(entry.path) as _it:
                                    for _f in _it:
                                        ext = os.path.splitext(_f.name)[1].lower()
                                        if ext in MEDIA_EXTENSIONS and (_f.is_file() or _f.is_symlink()):
                                            has_media = True
                                            break
                                        if not has_media and _f.is_dir(follow_symlinks=False):
                                            try:
                                                with os.scandir(_f.path) as _sub:
                                                    for _sf in _sub:
                                                        ext = os.path.splitext(_sf.name)[1].lower()
                                                        if ext in MEDIA_EXTENSIONS and (_sf.is_file() or _sf.is_symlink()):
                                                            has_media = True
                                                            break
                                            except OSError:
                                                pass
                                        if has_media:
                                            break
                            except OSError:
                                pass
                            if has_media:
                                is_show = True
                            else:
                                logger.debug("[library] Reclassifying '%s' as movie "
                                             "(Zurg shows/ but no recognizable media files)",
                                             entry.name)

                        if is_show:
                            # Tag episodes with per-season episode count so
                            # season packs are preferred over individual
                            # episode downloads. Per-season count ensures a
                            # high-quality S03 pack (20 eps) isn't beaten by
                            # a lower-quality S01-S08 mega-pack just because
                            # the mega-pack has more total files.
                            # On ties (equal per-season count), first-seen wins.
                            season_counts = {}
                            for ep_key in episodes:
                                season_counts[ep_key[0]] = season_counts.get(ep_key[0], 0) + 1
                            for ep_key in episodes:
                                episodes[ep_key]['_folder_ep_count'] = season_counts[ep_key[0]]

                            key = _normalize_title(title)
                            _merge_show_group(show_groups, key, title, year, episodes, entry.path)
                        else:
                            key = _normalize_title(title)
                            if key not in movie_groups:
                                movie_groups[key] = {
                                    'title': title,
                                    'year': year,
                                    'path': entry.path,
                                }
                            elif year and not movie_groups[key]['year']:
                                movie_groups[key]['year'] = year
                                movie_groups[key]['title'] = title
            except (PermissionError, OSError) as e:
                # A listing error mid-walk (e.g. a TorBox 429 surfacing as
                # 'couldn't list files') means the result is incomplete —
                # flag it so the caller doesn't treat partial data as the
                # full TB set and drop titles to "Wanted".
                #
                # Unlike the deadline branch this does NOT break: a categorized
                # (RD) mount may have several categories and a transient error
                # in one shouldn't abandon the others (best-effort scan). For
                # flat-layout (TB) there's a single category so continue vs.
                # break is moot, but the truncation flag still routes the TB
                # caller to the last-good fallback.
                logger.warning(f"[library] Cannot scan {cat_path}: {e}")
                self._last_scan_mount_truncated = True
            if timed_out:
                break

        # Convert aggregated groups to item lists
        movies = []
        for g in movie_groups.values():
            mq, msz = _get_movie_quality_from_folder(g['path'])
            movies.append({
                'title': g['title'],
                'year': g['year'],
                'source': 'debrid',
                'source_debrid': source_debrid,
                'type': 'movie',
                'seasons': 0,
                'episodes': 0,
                'path': g['path'],
                'quality': mq,
                'size_bytes': msz,
                'date_added': _get_folder_mtime(g['path']),
            })

        shows = []
        for g in show_groups.values():
            eps = g['episodes']
            unique_seasons = {s for s, _e in eps} if eps else set()
            shows.append({
                'title': g['title'],
                'year': g['year'],
                'source': 'debrid',
                'source_debrid': source_debrid,
                'type': 'show',
                'seasons': len(unique_seasons),
                'episodes': len(eps),
                '_episodes': eps,
                'path': g['path'],
                'date_added': _get_folder_mtime(g['path']),
            })

        return movies, shows

    def _load_webdav_capability(self):
        """Pre-set `_webdav_unsupported` from the persisted capability cache.

        Skips the load (treats as fresh) on any of:
          * file missing or unreadable
          * size cap exceeded
          * not a valid JSON object
          * `webdav_unsupported` not truthy
          * `ts` missing, malformed, older than 7 days, or in the far future
            (clock skew safety: more than a day ahead is rejected)
          * `zurg_version` differs from the current `ZURG_VERSION` env var
        Any failure logs a warning but never takes the scanner offline —
        the worst-case is one extra Depth: infinity attempt next scan.
        """
        try:
            if not os.path.isfile(self._capabilities_path):
                return

            import json as _json
            # Open before sizing so the size check uses the same fd that
            # the read consumes — closes the TOCTOU window between
            # `getsize` and `open` and removes the prior `except OSError:
            # pass` fallthrough that could have allowed an unbounded read.
            # JSONDecodeError and UnicodeDecodeError both subclass
            # ValueError and are caught by the outer except.
            with open(self._capabilities_path, 'r', encoding='utf-8') as fh:
                size = os.fstat(fh.fileno()).st_size
                if size > _WEBDAV_CAPABILITY_MAX_BYTES:
                    logger.warning(
                        "[library] capability cache exceeds size cap, ignoring"
                    )
                    return
                raw = _json.load(fh)
            if not isinstance(raw, dict):
                return
            # Strict-bool: a hand-edited file containing the string "yes"
            # or "false" should NOT lock the scanner into FUSE mode.  Only
            # the canonical Python-bool True qualifies.
            if raw.get('webdav_unsupported') is not True:
                return
            ts = raw.get('ts')
            if not isinstance(ts, (int, float)):
                return
            age = time.time() - ts
            # Two distinct invalidation causes — split the messages so the
            # log surfaces clock-skew separately from genuine TTL expiry.
            if age > _WEBDAV_CAPABILITY_TTL_S:
                logger.info(
                    "[library] capability cache expired, re-evaluating Zurg PROPFIND support"
                )
                return
            if age < -86400:
                logger.warning(
                    "[library] capability cache has a future timestamp "
                    "(clock skew or tampering?), ignoring"
                )
                return
            recorded_version = raw.get('zurg_version')
            current_version = os.environ.get('ZURG_VERSION') or None
            if recorded_version != current_version:
                logger.info(
                    f"[library] ZURG_VERSION changed ({recorded_version!r} -> "
                    f"{current_version!r}); invalidating capability cache"
                )
                return

            self._webdav_unsupported = True
            # Prior detection already surfaced the INFO message at the time
            # the cache was written; suppress a duplicate boot-time line.
            self._webdav_unsupported_logged = True
            logger.info(
                "[library] Loaded persisted Zurg capability: lacks recursive PROPFIND "
                "(skipping doomed Depth: infinity probes until cache expires)"
            )
        except (OSError, ValueError, TypeError) as e:
            logger.warning(f"[library] Could not load capability cache: {e}")

    def _persist_webdav_capability(self):
        """Atomic-write the capability cache after first detection.

        Best-effort: a write failure logs a warning and otherwise no-ops so
        a read-only `/config` (or quota-exhausted volume) can't fail the
        scan.  In-memory memoization still works for the rest of the
        process lifetime; the cost is one re-detection on next restart.
        """
        try:
            from utils.file_utils import atomic_write
            import json as _json
            payload = {
                'webdav_unsupported': True,
                'ts': time.time(),
                'zurg_version': os.environ.get('ZURG_VERSION') or None,
            }
            with atomic_write(self._capabilities_path) as fh:
                _json.dump(payload, fh)
        except (OSError, ValueError, TypeError) as e:
            # Best-effort: filesystem errors (read-only /config, no
            # space, missing parent dir) and any future
            # serialization-shape change must not propagate out of the
            # detection path and turn a successful FUSE fallback into a
            # whole-scan failure.
            logger.warning(f"[library] Could not persist capability cache: {e}")

    def _load_persisted_cache(self):
        """Pre-populate ``_cache`` and the path indexes from disk.

        Mirrors ``_load_webdav_capability``'s posture: any validation
        failure leaves init state untouched (current behavior — fresh
        scan).  The size cap is checked via ``os.fstat`` on the open
        fd, not ``os.path.getsize``, to close the TOCTOU window between
        sizing and reading (carried forward from
        ``_load_webdav_capability``).

        On success ``_cache_time`` is set so the next ``get_data()``
        call still serves from the persisted cache, but a refresh is
        scheduled within roughly a minute to bring the on-disk
        snapshot up to date with the live mount.
        """
        try:
            if not os.path.isfile(self._library_cache_path):
                return

            import json as _json
            with open(self._library_cache_path, 'r', encoding='utf-8') as fh:
                size = os.fstat(fh.fileno()).st_size
                if size > _LIBRARY_CACHE_MAX_BYTES:
                    logger.warning(
                        "[library] cache file exceeds size cap, ignoring"
                    )
                    return
                envelope = _json.load(fh)

            result = _deserialize_cache_state(envelope)
            if result is None:
                logger.warning(
                    "[library] cache file failed validation, ignoring"
                )
                return

            cache, path_index, local_path_index, alias_norms = result
            with self._lock:
                self._cache = cache
                # Mark the persisted view fresh so the first ``get_data()``
                # call after restart serves it immediately.  The scheduled
                # ``library_scan`` task triggers the next refresh on its
                # normal cadence; we don't need to pre-age cache_time to
                # force a sooner one (and the mount-absent branch's
                # ttl=10 makes any pre-aging fragile anyway).
                self._cache_time = time.monotonic()
            with self._path_lock:
                self._path_index = path_index
                self._local_path_index = local_path_index
                self._alias_norms = alias_norms

            logger.info(
                f"[library] Loaded persisted cache: "
                f"{len(cache.get('movies', []))} movies, "
                f"{len(cache.get('shows', []))} shows"
            )
        except (OSError, ValueError, TypeError) as e:
            logger.warning(f"[library] Could not load library cache: {e}")

    def _snapshot_indexes_for_persist(self):
        """Capture a coherent copy of the three index structures.

        Returned as ``(path_index, local_path_index, alias_norms)``
        snapshots — independent of any subsequent rebind by another
        scan thread.  Callers should invoke this *immediately* after
        ``_scan_read`` returns so the snapshot reflects the same scan
        whose ``data`` they are about to persist; the surrounding code
        is structured so concurrent scans rebind the live indexes via
        ``_path_lock`` without disturbing this captured view.
        """
        with self._path_lock:
            pi = dict(self._path_index)
            lpi = dict(self._local_path_index)
            an = {k: set(v) for k, v in self._alias_norms.items()}
        return pi, lpi, an

    def _persist_cache(self, cache, path_index, local_path_index, alias_norms):
        """Atomic-write the library cache + indexes after a scan.

        ``path_index`` / ``local_path_index`` / ``alias_norms`` must be
        callee-owned snapshots — typically captured via
        ``_snapshot_indexes_for_persist`` immediately after the scan
        that produced ``cache``, so the on-disk envelope pairs the same
        scan's cache and indexes.  (A residual microsecond race exists
        between ``_scan_read`` returning and the snapshot-capture call;
        it can only mismatch when two scans complete in interleaved
        order, which is rare and bounded by the scheduler cadence
        refreshing the persisted view on every cycle.)

        Best-effort: a write failure logs a warning but never propagates
        out of the scan path (read-only ``/config``, quota exhaustion,
        or a future change adding a non-serializable field would
        otherwise turn a successful scan into a failed one).
        """
        # Tolerate partially-constructed instances (e.g. unit-test scanners
        # built via ``LibraryScanner.__new__``) that skipped __init__ and
        # therefore lack the path attribute.  No-op rather than raise.
        if not getattr(self, '_library_cache_path', None):
            return
        try:
            from utils.file_utils import atomic_write
            import json as _json
            envelope = _serialize_cache_state(
                cache, path_index, local_path_index, alias_norms
            )
            with atomic_write(self._library_cache_path) as fh:
                _json.dump(envelope, fh)
        except (OSError, ValueError, TypeError) as e:
            logger.warning(f"[library] Could not persist library cache: {e}")

    def _webdav_scan_mount(self, deadline=None):
        """Scan the debrid mount via WebDAV PROPFIND directly to Zurg.

        Bypasses FUSE/rclone to avoid hundreds of kernel round-trips.
        Returns (movies, shows) in the same format as _scan_mount().

        Raises Exception on any failure so the caller can fall back to FUSE.
        """
        # Memoized: Zurg already proved it doesn't honor Depth: infinity on a
        # prior scan.  Skip the PROPFIND round-trip entirely.
        if self._webdav_unsupported:
            raise _WebDAVUnsupportedError(
                "Zurg lacks recursive PROPFIND (memoized)"
            )

        from utils.webdav import propfind
        from utils.blackhole import is_obfuscated_name as _is_obfuscated_name

        zurg_url = _discover_zurg_url(self._mount_path)
        if not zurg_url:
            raise RuntimeError("Cannot discover Zurg URL for WebDAV scan")

        auth = _get_zurg_auth()
        base_dav = f"{zurg_url}/dav/"
        remaining = max(5, int(deadline - time.monotonic())) if deadline else 30

        # Step 1: List categories (mirrors _scan_mount's category selection)
        entries = propfind(base_dav, depth=1, auth=auth, timeout=min(remaining, 10))
        # Skip the root directory itself — its href may be '/', '/dav/', or empty
        _root_hrefs = {'/', '/dav/', '/dav', ''}
        all_cats = []
        for e in entries:
            if e['is_collection'] and e['name'] and e['href'].rstrip('/') not in _root_hrefs:
                all_cats.append(e['name'])

        non_special = [c for c in all_cats if c not in self._SKIP_CATEGORIES]
        scan_cats = non_special if non_special else [c for c in all_cats if c == '__all__']
        if not scan_cats:
            logger.warning("[library] WebDAV: no scannable categories found")
            return [], []

        logger.debug(f"[library] WebDAV scanning categories: {scan_cats}")

        # Step 2: PROPFIND each category with depth infinity
        show_groups = {}
        movie_groups = {}

        # Aggregate detection state — only conclude that Zurg lacks recursive
        # PROPFIND if EVERY category that returned folders returned zero
        # files.  A single quirky empty category (e.g. partial torrents,
        # zero-content folders) shouldn't tar a working Zurg instance into
        # a 7-day FUSE-only lockout.
        empty_categories = []         # [(name, folder_count), ...]
        any_files_seen = False
        scan_completed = True         # cleared if we break out on deadline

        for category in scan_cats:
            if deadline and time.monotonic() > deadline:
                logger.warning("[library] WebDAV: deadline reached, skipping remaining categories")
                scan_completed = False
                break

            cat_url = f"{zurg_url}/dav/{urllib_quote(category, safe='')}/"
            remaining = max(5, int(deadline - time.monotonic())) if deadline else 30
            cat_is_shows = category.lower() in self._SHOW_CATEGORIES

            try:
                cat_entries = propfind(cat_url, depth='infinity', auth=auth,
                                       timeout=min(remaining, 25))
            except Exception as e:
                logger.warning(f"[library] WebDAV PROPFIND failed for {category}, skipping: {e}")
                continue

            # Group entries by torrent folder.
            # Hrefs are already URL-decoded by webdav.propfind().
            # Zurg may return absolute (/dav/movies/...) or relative (folder/file)
            # hrefs — normalise both to a relative path below the category.
            cat_prefix = f"/dav/{category}/"
            cat_prefix_short = f"/{category}/"
            folders = {}

            for entry in cat_entries:
                href = entry['href']
                if href.startswith(cat_prefix):
                    rel = href[len(cat_prefix):]
                elif href.startswith(cat_prefix_short):
                    rel = href[len(cat_prefix_short):]
                elif not href.startswith('/'):
                    # Relative href (bare folder/file path)
                    rel = href
                else:
                    continue
                rel = rel.rstrip('/')
                if not rel:
                    continue  # category dir itself

                parts = rel.split('/')
                folder_name = parts[0]
                if folder_name.lower() in _SKIP_FOLDERS:
                    continue
                if _is_obfuscated_name(folder_name):
                    continue

                if folder_name not in folders:
                    folders[folder_name] = {'files': [], 'season_files': {}}

                if entry['is_collection']:
                    continue  # skip directory entries, we only need files

                mount_path = self._mount_path_for(category, rel)
                if not mount_path:
                    continue
                if len(parts) == 2:
                    # File directly in torrent folder: folder/file.mkv
                    folders[folder_name]['files'].append(
                        (parts[1], entry['size'], mount_path)
                    )
                elif len(parts) == 3:
                    # File in subfolder: folder/Season 1/S01E01.mkv
                    folders[folder_name]['season_files'].setdefault(parts[1], []).append(
                        (parts[2], entry['size'], mount_path)
                    )

            cat_has_files = any(
                contents['files'] or contents['season_files']
                for contents in folders.values()
            )
            if folders and not cat_has_files:
                # This category looks like the "Zurg doesn't recurse"
                # signature, but defer the verdict until all categories
                # have been scanned.  Skip processing — empty folders
                # would mis-classify in the downstream loop (shows could
                # be reclassified as movies, etc.) and we have no real
                # data for them anyway.
                empty_categories.append((category, len(folders)))
                continue
            if cat_has_files:
                any_files_seen = True

            # Step 3: Process folders into show_groups / movie_groups
            for folder_name, contents in folders.items():
                title, year = _parse_folder_name(folder_name)
                if not title:
                    continue

                episodes = self._collect_episodes_from_webdav(contents, folder_name)
                is_show = len(episodes) > 0
                if not is_show and cat_is_shows:
                    has_media = any(
                        os.path.splitext(fn)[1].lower() in MEDIA_EXTENSIONS
                        for fn, _sz, _p in contents.get('files', [])
                    ) or any(
                        os.path.splitext(fn)[1].lower() in MEDIA_EXTENSIONS
                        for files in contents.get('season_files', {}).values()
                        for fn, _sz, _p in files
                    )
                    if has_media:
                        is_show = True
                    else:
                        logger.debug("[library] Reclassifying '%s' as movie "
                                     "(Zurg shows/ but no recognizable media files)",
                                     folder_name)

                if is_show:
                    season_counts = {}
                    for ep_key in episodes:
                        season_counts[ep_key[0]] = season_counts.get(ep_key[0], 0) + 1
                    for ep_key in episodes:
                        episodes[ep_key]['_folder_ep_count'] = season_counts[ep_key[0]]

                    key = _normalize_title(title)
                    _merge_show_group(
                        show_groups, key, title, year, episodes,
                        os.path.join(self._mount_path, category, folder_name),
                    )
                else:
                    key = _normalize_title(title)
                    if key not in movie_groups:
                        movie_groups[key] = {
                            'title': title,
                            'year': year,
                            'path': os.path.join(self._mount_path, category, folder_name),
                            '_contents': contents,
                        }
                    elif year and not movie_groups[key]['year']:
                        movie_groups[key]['year'] = year
                        movie_groups[key]['title'] = title

        # Post-loop verdict on Zurg's recursive-PROPFIND support.  Only
        # memoize "unsupported" if the scan completed AND no category
        # produced any files AND at least one category returned folders.
        # The scan-completed gate prevents a deadline-induced partial
        # scan from poisoning the memoization.
        if scan_completed and empty_categories and not any_files_seen:
            self._webdav_unsupported = True
            self._persist_webdav_capability()
            cat_summary = ', '.join(f'{c}({n})' for c, n in empty_categories)
            raise _WebDAVUnsupportedError(
                f"WebDAV depth-infinity returned folders but 0 files for [{cat_summary}] "
                f"— Zurg likely does not support recursive PROPFIND"
            )
        if empty_categories and any_files_seen:
            # Some categories work, others have folders but no files.
            # Likely a quirk in those categories (zero-content torrents,
            # mid-download placeholders) rather than a Zurg-wide
            # capability issue.  Log for diagnosability and continue
            # with the categories that did return data.
            cat_summary = ', '.join(f'{c}({n})' for c, n in empty_categories)
            logger.info(
                f"[library] WebDAV: skipped categories with folders-but-no-files "
                f"[{cat_summary}] (other categories returned files, so Zurg "
                f"recursion works — not memoizing as unsupported)"
            )

        # Convert to output format (same as _scan_mount)
        # Note: date_added is 0 for WebDAV-scanned items because calling
        # _get_folder_mtime() would issue FUSE stat calls, defeating the
        # purpose of the WebDAV bypass.  FUSE-based scans populate real
        # mtimes; WebDAV items fall back to sort-bottom for "Newest Added".
        movies = []
        for g in movie_groups.values():
            mq, msz = _get_movie_quality_from_webdav(g.get('_contents', {}))
            movies.append({
                'title': g['title'],
                'year': g['year'],
                'source': 'debrid',
                'type': 'movie',
                'seasons': 0,
                'episodes': 0,
                'path': g['path'],
                'quality': mq,
                'size_bytes': msz,
                'date_added': 0,
            })

        shows = []
        for g in show_groups.values():
            eps = g['episodes']
            unique_seasons = {s for s, _e in eps} if eps else set()
            shows.append({
                'title': g['title'],
                'year': g['year'],
                'source': 'debrid',
                'type': 'show',
                'seasons': len(unique_seasons),
                'episodes': len(eps),
                '_episodes': eps,
                'path': g['path'],
                'date_added': 0,
            })

        return movies, shows

    def _mount_path_for(self, category, rel_path):
        """Translate a WebDAV relative path to a FUSE mount path."""
        result = os.path.normpath(os.path.join(self._mount_path, category, rel_path))
        # Guard against path traversal via ".." in crafted hrefs
        cat_root = os.path.normpath(os.path.join(self._mount_path, category))
        if not result.startswith(cat_root + os.sep) and result != cat_root:
            return None
        return result

    @staticmethod
    def _collect_episodes_from_webdav(contents, folder_name=''):
        """Extract episodes from WebDAV folder contents.

        Mirrors _collect_episodes() logic but works on pre-parsed WebDAV data
        instead of os.scandir. `folder_name` is the release folder basename
        so each episode can be traced back to the torrent it came from (used
        by the per-episode block action to blocklist the source release).
        """
        episodes = {}

        # Check season subdirectories
        for season_dir, files in contents.get('season_files', {}).items():
            season_match = _SEASON_DIR_PATTERN.match(season_dir)
            if not season_match:
                continue
            season_num = int(season_match.group(1))
            for fname, fsize, fpath in files:
                ext = os.path.splitext(fname)[1].lower()
                if ext not in MEDIA_EXTENSIONS:
                    continue
                ep_match = _EPISODE_ID_PATTERN.search(fname)
                if ep_match:
                    key = (int(ep_match.group(1)), int(ep_match.group(2)))
                else:
                    key = (season_num, len(episodes) + 1000)
                episodes[key] = {'file': fname, 'path': fpath, 'size_bytes': fsize, 'folder': folder_name}

        # Check flat files in folder root
        for fname, fsize, fpath in contents.get('files', []):
            ext = os.path.splitext(fname)[1].lower()
            if ext in MEDIA_EXTENSIONS:
                ep_match = _EPISODE_ID_PATTERN.search(fname)
                if ep_match:
                    key = (int(ep_match.group(1)), int(ep_match.group(2)))
                    episodes[key] = {'file': fname, 'path': fpath, 'size_bytes': fsize, 'folder': folder_name}

        return episodes

    def _scan_torbox_via_api(self, tb_mount):
        """Enumerate TorBox library content via the mylist API.

        Replaces the throttled per-folder FUSE walk (``_scan_mount(
        flat_layout=True)``): that walk issued thousands of scandir/stat
        calls over a 5-tps rclone mount and contended with real content
        downloads, producing 429 storms that occasionally abandoned an
        in-flight download.  ``mylist`` returns the whole account (folder
        names + per-file paths/sizes) in ONE HTTP call, so enumeration
        costs zero FUSE ops.  The FUSE mount is still required for symlink
        TARGETS (real file access) — only enumeration moves to the API.

        Returns ``(movies, shows)`` in ``_scan_mount`` output shape.  Sets
        ``self._last_scan_mount_truncated`` True (so the caller falls back
        to its last-good TB baseline instead of dropping titles to
        "Wanted") only when the API call FAILS — i.e. ``list_torbox_torrents``
        returns ``None``.  An empty account (``[]``) is a complete,
        authoritative zero-item scan and is NOT treated as incomplete.
        """
        self._last_scan_mount_truncated = False

        from base import load_secret_or_env
        from utils import search
        from utils.blackhole import is_obfuscated_name as _is_obfuscated_name

        api_key = load_secret_or_env('torbox_api_key')
        if not api_key:
            self._last_scan_mount_truncated = True
            return [], []

        # TORBOX_SCAN_TIMEOUT was the old FUSE-walk deadline; it now caps the
        # single mylist HTTP call instead (read live from os.environ so a
        # SIGHUP-less UI change applies next scan).  Floor at 10s — a healthy
        # mylist responds in seconds, but a large account over a slow link
        # shouldn't spuriously fail and drop TB to the last-good fallback.
        try:
            tb_timeout = max(int(os.environ.get('TORBOX_SCAN_TIMEOUT', '180')), 10)
        except (ValueError, TypeError):
            tb_timeout = 180
        torrents = search.list_torbox_torrents(api_key, timeout=tb_timeout)
        if torrents is None:
            logger.warning("[library] TB mylist API call failed; treating "
                           "TB scan as incomplete (will fall back to last-good)")
            self._last_scan_mount_truncated = True
            return [], []

        # Build the per-folder structure _collect_episodes_from_webdav
        # expects: folder name -> {'files': [(fname, size, path)],
        # 'season_files': {subdir: [(fname, size, path)]}}.
        #
        # CRITICAL: the on-disk folder is the FIRST path component of each
        # file's mylist ``name`` — NOT the torrent-level ``name``.  rclone
        # lays files out at <mount>/<files[].name>, where files[].name is the
        # ORIGINAL torrent path (e.g. "Tulsa.King.S02.../ep.mkv").  The
        # entry-level ``name`` is a SANITIZED display string (spaces for dots,
        # truncated, & -> and) that matches the on-disk folder for only ~20%
        # of torrents (live: 102/490).  Keying paths off it would synthesize
        # non-existent targets for ~80% of TB content.  Deriving the folder
        # from the file path matches the live mount for 485/490 entries
        # (1717/1749 files); the handful of stragglers are unicode/special-
        # char folders rclone renames, which a FUSE walk wouldn't serve either.
        folders = {}
        folder_created = {}
        for t in torrents:
            # list_torbox_torrents normalizes its output, but guard defensively
            # so one malformed entry degrades to a skip rather than raising and
            # aborting the whole scan (which would lose all genuinely-new TB
            # content for the cycle by tripping the last-good fallback).
            if not isinstance(t, dict):
                continue
            created = _parse_tb_timestamp(t.get('created_at'))
            files = t.get('files')
            if not isinstance(files, list):
                continue
            for f in files:
                if not isinstance(f, dict):
                    continue
                frel = f.get('name')
                if not isinstance(frel, str) or not frel:
                    continue
                # Split into clean path components: drop empties (so a "//"
                # can't reintroduce an absolute component that os.path.join
                # would treat as a new root and escape the mount) and reject
                # any ".." traversal. The synthesized path is rebuilt from the
                # cleaned components, so it always stays under <tb_mount> and
                # _resolve_symlink_target can map it back to the TB symlink
                # base; nothing can point outside the TB mount.
                parts = [p for p in frel.split('/') if p]
                if '..' in parts:
                    continue
                # A bare file at the mount root (no torrent folder) can't be
                # classified into a title dir; the old FUSE walk only
                # enumerated top-level DIRS, so skip to match its behavior.
                if len(parts) < 2:
                    continue
                folder_name = parts[0]
                if _is_obfuscated_name(folder_name):
                    logger.debug(
                        f"[library] Skipping obfuscated TB folder: {folder_name}"
                    )
                    continue
                subparts = parts[1:]
                size = f.get('size', 0)
                if not isinstance(size, int) or size < 0:
                    size = 0
                synth_path = os.path.join(tb_mount, *parts)
                bucket = folders.setdefault(
                    folder_name, {'files': [], 'season_files': {}})
                folder_created[folder_name] = max(
                    folder_created.get(folder_name, 0), created)
                if len(subparts) == 1:
                    bucket['files'].append((subparts[0], size, synth_path))
                elif len(subparts) == 2:
                    bucket['season_files'].setdefault(subparts[0], []).append(
                        (subparts[1], size, synth_path)
                    )
                # Deeper nesting is ignored — mirrors _webdav_scan_mount,
                # which only buckets 2- and 3-level paths.

        show_groups = {}
        movie_groups = {}
        for folder_name, contents in folders.items():
            title, year = _parse_folder_name(folder_name)
            if not title:
                continue
            episodes = self._collect_episodes_from_webdav(contents, folder_name)
            is_show = len(episodes) > 0
            # Flat-layout TV-marker fallback (mirrors _scan_mount): a season
            # pack still caching on TB carries the marker in its folder name
            # but has no SxxExx files yet.  Flip to show WITHOUT injecting
            # synthetic episodes; the next scan fills in real files.
            if not is_show and _detect_tv_marker(folder_name):
                is_show = True
            created = folder_created.get(folder_name, 0)
            if is_show:
                season_counts = {}
                for ep_key in episodes:
                    season_counts[ep_key[0]] = season_counts.get(ep_key[0], 0) + 1
                for ep_key in episodes:
                    episodes[ep_key]['_folder_ep_count'] = season_counts[ep_key[0]]
                key = _normalize_title(title)
                _merge_show_group(show_groups, key, title, year, episodes,
                                  os.path.join(tb_mount, folder_name))
                g = show_groups[key]
                if created > g.get('date_added', 0):
                    g['date_added'] = created
            else:
                key = _normalize_title(title)
                if key not in movie_groups:
                    movie_groups[key] = {
                        'title': title,
                        'year': year,
                        'path': os.path.join(tb_mount, folder_name),
                        'date_added': created,
                        '_contents': contents,
                    }
                else:
                    if year and not movie_groups[key]['year']:
                        movie_groups[key]['year'] = year
                        movie_groups[key]['title'] = title
                    if created > movie_groups[key].get('date_added', 0):
                        movie_groups[key]['date_added'] = created

        movies = []
        for g in movie_groups.values():
            mq, msz = _get_movie_quality_from_webdav(g.get('_contents', {}))
            movies.append({
                'title': g['title'],
                'year': g['year'],
                'source': 'debrid',
                'source_debrid': 'torbox',
                'type': 'movie',
                'seasons': 0,
                'episodes': 0,
                'path': g['path'],
                'quality': mq,
                'size_bytes': msz,
                'date_added': g.get('date_added', 0),
            })

        shows = []
        for g in show_groups.values():
            eps = g['episodes']
            unique_seasons = {s for s, _e in eps} if eps else set()
            shows.append({
                'title': g['title'],
                'year': g['year'],
                'source': 'debrid',
                'source_debrid': 'torbox',
                'type': 'show',
                'seasons': len(unique_seasons),
                'episodes': len(eps),
                '_episodes': eps,
                'path': g['path'],
                'date_added': g.get('date_added', 0),
            })

        logger.debug(
            f"[library] TB API scan: {len(torrents)} torrents → "
            f"{len(movies)} movies, {len(shows)} shows"
        )
        return movies, shows

    def _scan_local_movies(self):
        items = []
        if not self._local_movies_path:
            return items
        if not os.path.isdir(self._local_movies_path):
            logger.warning(f"[library] Local movies path not found: {self._local_movies_path}")
            return items
        symlink_prefixes = _all_debrid_symlink_prefixes()
        from utils.blackhole import is_obfuscated_name as _is_obfuscated_name
        try:
            with os.scandir(self._local_movies_path) as it:
                for entry in it:
                    if not entry.is_dir(follow_symlinks=False):
                        continue
                    # Skip known non-media folders before any I/O
                    if entry.name.lower() in _SKIP_FOLDERS:
                        continue
                    if _is_obfuscated_name(entry.name):
                        continue
                    # Skip folders that only contain debrid symlinks
                    if symlink_prefixes and self._is_debrid_symlink_dir(entry.path, symlink_prefixes):
                        continue
                    # Skip folders with no media files — these are either empty
                    # Radarr placeholders or dirs whose symlinks were deleted.
                    # Classifying them as local would block symlink recreation.
                    if not self._has_media_files(entry.path):
                        continue
                    title, year = _parse_folder_name(entry.name)
                    if not title:
                        continue
                    mq, msz = _get_movie_quality_from_folder(entry.path)
                    items.append({
                        'title': title,
                        'year': year,
                        'source': 'local',
                        'type': 'movie',
                        'seasons': 0,
                        'episodes': 0,
                        'path': entry.path,
                        'quality': mq,
                        'size_bytes': msz,
                        'date_added': _get_folder_mtime(entry.path),
                    })
        except (PermissionError, OSError) as e:
            logger.warning(f"[library] Cannot scan local movies: {e}")
        return items

    @staticmethod
    def _is_debrid_symlink_dir(path, symlink_prefixes):
        """Check if a directory contains only debrid symlinks (no real media files).

        ``symlink_prefixes`` is a tuple of trailing-``os.sep``-terminated
        prefixes — one per configured debrid target base.  A symlink whose
        target starts with ANY of them counts as a debrid symlink; a symlink
        targeting an unknown path counts as non-debrid and disqualifies the
        whole dir.  Plan 39: dual-debrid setups have a separate prefix for
        each provider; checking only one would misclassify the other.

        Only considers media-extension files. Non-media files (.nfo, .srt, .jpg)
        are ignored so Radarr metadata doesn't cause false local classification.
        Returns False for empty directories.
        """
        if not symlink_prefixes:
            return False
        has_debrid_symlink = False
        try:
            with os.scandir(path) as it:
                for f in it:
                    ext = os.path.splitext(f.name)[1].lower()
                    if ext not in MEDIA_EXTENSIONS:
                        continue
                    if f.is_symlink():
                        target = os.readlink(f.path)
                        if not any(target.startswith(p) for p in symlink_prefixes):
                            return False  # symlink to non-debrid location
                        has_debrid_symlink = True
                    elif f.is_file(follow_symlinks=False):
                        return False  # real media file = genuine local content
        except OSError:
            return False
        return has_debrid_symlink  # False for empty dirs

    @staticmethod
    def _has_media_files(path):
        """Check if a directory contains at least one *resolving* media file.

        Used to avoid classifying metadata-only directories (leftover .nfo/.jpg
        from Radarr after symlinks were deleted) as genuine local content.

        A symlink counts only if it RESOLVES: a dangling video symlink (target
        gone, or pointing outside every configured debrid base after a target-
        base rename) is morally identical to a deleted symlink — counting it as
        local would inflate the recovery metric and hide the title from
        "Wanted", blocking symlink recreation. Real files short-circuit before
        any deref so a live mount isn't stat'd unnecessarily.
        """
        try:
            with os.scandir(path) as it:
                for f in it:
                    ext = os.path.splitext(f.name)[1].lower()
                    if ext not in MEDIA_EXTENSIONS:
                        continue
                    if f.is_file(follow_symlinks=False):
                        return True
                    if f.is_symlink() and os.path.exists(f.path):
                        return True
        except OSError:
            pass
        return False

    @staticmethod
    def _is_debrid_symlink_only(path, symlink_prefixes):
        """Check if a show directory tree contains only debrid symlinks (no real media files).

        ``symlink_prefixes`` is a tuple of debrid-target prefixes (one per
        configured debrid).  See ``_is_debrid_symlink_dir`` for the dual-debrid
        rationale — the same single-prefix bug applied here, mis-bucketing
        TB-routed show folders as local TV (and, when the show name collided
        with a movie folder, surfacing as a movie card in the library UI).

        Walks into Season subdirectories to check episode files. Non-media files
        are ignored. Returns False for empty directories.
        """
        if not symlink_prefixes:
            return False
        has_any_media = False
        try:
            with os.scandir(path) as it:
                for entry in it:
                    if entry.is_dir(follow_symlinks=False):
                        # Check inside Season dirs
                        try:
                            with os.scandir(entry.path) as season_it:
                                for f in season_it:
                                    ext = os.path.splitext(f.name)[1].lower()
                                    if ext not in MEDIA_EXTENSIONS:
                                        continue
                                    has_any_media = True
                                    if f.is_symlink():
                                        target = os.readlink(f.path)
                                        if not any(target.startswith(p) for p in symlink_prefixes):
                                            return False
                                    elif f.is_file(follow_symlinks=False):
                                        return False  # real file
                        except OSError:
                            pass
                    elif entry.is_symlink():
                        ext = os.path.splitext(entry.name)[1].lower()
                        if ext in MEDIA_EXTENSIONS:
                            has_any_media = True
                            target = os.readlink(entry.path)
                            if not any(target.startswith(p) for p in symlink_prefixes):
                                return False
                    elif entry.is_file(follow_symlinks=False):
                        ext = os.path.splitext(entry.name)[1].lower()
                        if ext in MEDIA_EXTENSIONS:
                            return False  # real file
        except OSError:
            return False
        return has_any_media  # only True if we found media and all were debrid symlinks

    def _scan_local_shows(self):
        items = []
        if not self._local_tv_path:
            return items
        if not os.path.isdir(self._local_tv_path):
            logger.warning(f"[library] Local TV path not found: {self._local_tv_path}")
            return items
        symlink_prefixes = _all_debrid_symlink_prefixes()
        from utils.blackhole import is_obfuscated_name as _is_obfuscated_name
        try:
            with os.scandir(self._local_tv_path) as it:
                for entry in it:
                    if not entry.is_dir(follow_symlinks=False):
                        continue
                    # Skip known non-media folders before any I/O
                    if entry.name.lower() in _SKIP_FOLDERS:
                        continue
                    if _is_obfuscated_name(entry.name):
                        continue
                    # Skip show folders that are entirely debrid symlinks
                    if symlink_prefixes and self._is_debrid_symlink_only(entry.path, symlink_prefixes):
                        continue
                    title, year = _parse_folder_name(entry.name)
                    if not title:
                        continue
                    eps = _collect_episodes(entry.path)
                    if eps:
                        unique_seasons = {s for s, _e in eps}
                        items.append({
                            'title': title,
                            'year': year,
                            'source': 'local',
                            'type': 'show',
                            'seasons': len(unique_seasons),
                            'episodes': len(eps),
                            '_episodes': eps,
                            'path': entry.path,
                            'date_added': _get_folder_mtime(entry.path),
                        })
                    else:
                        # Fallback for shows without parseable episode patterns
                        seasons, ep_count = _count_show_content(entry.path)
                        # Skip dirs with no media files — empty placeholders
                        # or dirs whose symlinks were deleted
                        if ep_count == 0:
                            continue
                        items.append({
                            'title': title,
                            'year': year,
                            'source': 'local',
                            'type': 'show',
                            'seasons': seasons,
                            'episodes': ep_count,
                            '_episodes': {},
                            'path': entry.path,
                            'date_added': _get_folder_mtime(entry.path),
                        })
        except (PermissionError, OSError) as e:
            logger.warning(f"[library] Cannot scan local TV: {e}")
        return items


def remove_title_symlinks(title, media_type, year=None):
    """Remove local library symlinks for a deleted title.

    Walks the appropriate local library directory (TV or movies), finds
    the folder whose parsed title matches *title* (normalized comparison),
    and removes it only if every file inside is a symlink (no real files).

    Also checks BLACKHOLE_COMPLETED_DIR for leftover release symlinks.

    Matching uses ``_normalize_title`` only (no ``_norm_for_matching`` or
    TMDB ID fallback) for safety — titles that only match via TMDB alias
    (e.g. debrid name differs from arr name) may leave orphaned dirs.

    Returns list of directory paths that were removed.
    """
    norm_target = _normalize_title(title)
    removed = []

    if media_type == 'show':
        lib_path = os.environ.get('BLACKHOLE_LOCAL_LIBRARY_TV', '').strip()
    else:
        lib_path = os.environ.get('BLACKHOLE_LOCAL_LIBRARY_MOVIES', '').strip()

    # Remove from local library
    if lib_path and os.path.isdir(lib_path):
        try:
            with os.scandir(lib_path) as it:
                for entry in it:
                    if not entry.is_dir(follow_symlinks=False):
                        continue
                    parsed_title, parsed_year = _parse_folder_name(entry.name)
                    if not parsed_title:
                        continue
                    if _normalize_title(parsed_title) != norm_target:
                        continue
                    # Year guard: if both sides have a year, they must agree
                    if year is not None and parsed_year is not None and year != parsed_year:
                        continue
                    # Safety: only remove if non-empty and all files are symlinks
                    if _dir_contains_only_symlinks(entry.path):
                        try:
                            shutil.rmtree(entry.path)
                            logger.info(f"[cleanup] Removed symlink directory: {entry.path}")
                            removed.append(entry.path)
                        except OSError as e:
                            logger.warning(f"[cleanup] Failed to remove {entry.path}: {e}")
                    else:
                        logger.info(f"[cleanup] Skipping {entry.path} — contains real files or is empty")
        except (PermissionError, OSError) as e:
            logger.warning(f"[cleanup] Cannot scan {lib_path}: {e}")

    # Remove from completed dir — handles both flat and labeled layouts
    # via iter_release_dirs. Under labeled mode, the same release title
    # may exist under multiple labels (e.g. sonarr/ and radarr/); we
    # remove it everywhere it matches.
    completed_dir = os.environ.get('BLACKHOLE_COMPLETED_DIR', '').strip()
    if completed_dir and os.path.isdir(completed_dir):
        try:
            from utils.blackhole import iter_release_dirs
            for _label, release_name, release_path in iter_release_dirs(completed_dir):
                parsed_title, parsed_year = _parse_folder_name(release_name)
                if not parsed_title:
                    continue
                if _normalize_title(parsed_title) != norm_target:
                    continue
                if year is not None and parsed_year is not None and year != parsed_year:
                    continue
                if _dir_contains_only_symlinks(release_path):
                    try:
                        shutil.rmtree(release_path)
                        logger.info(f"[cleanup] Removed completed symlink directory: {release_path}")
                        removed.append(release_path)
                    except OSError as e:
                        logger.warning(f"[cleanup] Failed to remove {release_path}: {e}")
        except (PermissionError, OSError) as e:
            logger.warning(f"[cleanup] Cannot scan {completed_dir}: {e}")

    return removed


def _dir_contains_only_symlinks(dirpath):
    """Return True if *dirpath* is non-empty and every file is a symlink."""
    has_files = False
    for root, dirs, files in os.walk(dirpath):
        for f in files:
            has_files = True
            fpath = os.path.join(root, f)
            if not os.path.islink(fpath):
                return False
    return has_files


_scanner = None


def setup():
    global _scanner
    _scanner = LibraryScanner()
    _scanner.refresh()


def get_scanner():
    return _scanner


def compute_library_stats(data):
    """Summarize a scan ``data`` payload into composition counts and sizes.

    Returns counts and bytes broken down by source label (``local``/
    ``debrid``/``both``) for movies, shows, and (for shows) episodes.
    Both top-level ``size_bytes`` (per-movie / per-show) and per-episode
    sizes are summed; shows whose source is ``both`` count once under
    ``both`` for the show row, while their episodes are bucketed by the
    episode-level source so the user can see how a mixed library is
    actually distributed across providers.
    """
    sources = ('local', 'debrid', 'both')
    movies_by_src = {s: 0 for s in sources}
    movies_size_by_src = {s: 0 for s in sources}
    shows_by_src = {s: 0 for s in sources}
    shows_size_by_src = {s: 0 for s in sources}
    episodes_by_src = {s: 0 for s in sources}
    episodes_size_by_src = {s: 0 for s in sources}

    for movie in data.get('movies', []) or []:
        # Ghost entries from _apply_radarr_wanted_movies (source='wanted')
        # are Radarr-monitored-but-not-downloaded items. They aren't part
        # of the on-disk library, so they MUST NOT count toward local /
        # debrid / both buckets — otherwise the Library Composition card
        # would inflate the totals with content that doesn't exist yet.
        src = movie.get('source') or 'debrid'
        if src == 'wanted':
            continue
        if src not in movies_by_src:
            src = 'debrid'
        movies_by_src[src] += 1
        try:
            sz = int(movie.get('size_bytes') or 0)
        except (TypeError, ValueError):
            sz = 0
        if sz > 0:
            movies_size_by_src[src] += sz

    for show in data.get('shows', []) or []:
        show_src = show.get('source') or 'debrid'
        # Ghost shows from _apply_sonarr_wanted_shows (source='wanted') are
        # Sonarr-monitored-but-not-downloaded series. Like ghost movies,
        # they aren't on-disk library and MUST NOT count toward the
        # local/debrid/both buckets — otherwise the Composition card
        # inflates with content that doesn't exist yet.
        if show_src == 'wanted':
            continue
        if show_src not in shows_by_src:
            show_src = 'debrid'
        shows_by_src[show_src] += 1
        for season in show.get('season_data', []) or []:
            for ep in season.get('episodes', []) or []:
                esrc = ep.get('source') or show_src
                if esrc not in episodes_by_src:
                    esrc = 'debrid'
                episodes_by_src[esrc] += 1
                try:
                    esz = int(ep.get('size_bytes') or 0)
                except (TypeError, ValueError):
                    esz = 0
                if esz > 0:
                    episodes_size_by_src[esrc] += esz
                    shows_size_by_src[show_src] += esz

    movies_total_size = sum(movies_size_by_src.values())
    shows_total_size = sum(shows_size_by_src.values())

    return {
        'movies': {
            'total': sum(movies_by_src.values()),
            'by_source': movies_by_src,
            'size_bytes': movies_total_size,
            'size_by_source': movies_size_by_src,
        },
        'shows': {
            'total': sum(shows_by_src.values()),
            'by_source': shows_by_src,
            'episodes': {
                'total': sum(episodes_by_src.values()),
                'by_source': episodes_by_src,
                'size_bytes': sum(episodes_size_by_src.values()),
                'size_by_source': episodes_size_by_src,
            },
            'size_bytes': shows_total_size,
            'size_by_source': shows_size_by_src,
        },
        'totals': {
            'items': sum(movies_by_src.values()) + sum(shows_by_src.values()),
            'size_bytes': movies_total_size + shows_total_size,
        },
        'last_scan': data.get('last_scan'),
        'scan_duration_ms': data.get('scan_duration_ms'),
    }


def get_wanted_counts(data, pending=None):
    """Count items needing attention from library data.

    Returns dict with keys: missing, unavailable, pending, fallback.
    Each value is the number of items (shows/movies) matching that filter.
    """
    pending = pending or {}
    counts = {'missing': 0, 'unavailable': 0, 'pending': 0, 'fallback': 0}

    # Pending entries may be keyed by the parsed-folder norm even when the
    # display title was upgraded to canonical via TMDB rename.  Resolve via
    # scanner aliases so renamed items still match their pending entry.
    scanner = _scanner

    def _pending_for(title):
        norm = _normalize_title(title or '')
        if not norm:
            return {}
        pe = pending.get(norm)
        if not pe and scanner is not None:
            for alias in scanner.aliases_for(norm):
                pe = pending.get(alias)
                if pe:
                    break
        return pe or {}

    for show in data.get('shows', []):
        # Missing: TMDB enrichment sets missing_episodes = total - have.
        # season_data from the scan only contains episodes WITH files,
        # so we use the pre-computed count instead of iterating episodes.
        me = show.get('missing_episodes')
        if me is not None and me > 0:
            counts['missing'] += 1

        pe = _pending_for(show.get('title', ''))
        direction = pe.get('direction', '')
        if direction == 'debrid-unavailable':
            counts['unavailable'] += 1
        if direction in ('to-local', 'to-debrid', 'to-local-fallback'):
            counts['pending'] += 1
        if direction == 'to-local-fallback':
            counts['fallback'] += 1

    for movie in data.get('movies', []):
        # Ghost entries from _apply_radarr_wanted_movies carry
        # ``missing=True``; real entries don't have this key. The old
        # ``missing_episodes`` check here was always False for movies
        # (the field is only assigned by show enrichment), so the
        # 'missing' bucket effectively excluded movies entirely until
        # this fix landed.
        if movie.get('missing'):
            counts['missing'] += 1

        pe = _pending_for(movie.get('title', ''))
        direction = pe.get('direction', '')
        if direction == 'debrid-unavailable':
            counts['unavailable'] += 1
        if direction in ('to-local', 'to-debrid', 'to-local-fallback'):
            counts['pending'] += 1
        if direction == 'to-local-fallback':
            counts['fallback'] += 1

    return counts
