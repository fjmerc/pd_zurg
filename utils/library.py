"""Library scanner for debrid (rclone mount) and local media content.

Walks Zurg mount categories and local library directories to build a
unified item list, cross-referencing by title to detect content present
in both sources.
"""

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


def gap_fill_enabled():
    """Return ``True`` when the unconditional episode-completeness reconcile is
    enabled.  Single source of truth for the ``GAP_FILL_ENABLED`` env var so
    ``library.py`` and ``scheduled_tasks.py`` can't drift on parsing rules.
    Default ``true`` — opt-out, not opt-in.
    """
    return os.environ.get('GAP_FILL_ENABLED', 'true').strip().lower() == 'true'

# Folders to skip during library scans (non-media content)
_SKIP_FOLDERS = {
    'plex versions', 'subs', 'subtitles', 'featurettes',
    'behind the scenes', 'behind-the-scenes', 'deleted scenes',
    'interviews', 'scenes', 'trailers', 'sample', 'samples',
    '.actors', 'bonus', 'bonuses',
    '.recycle', '@eadir', '@recently-snapshot',
}

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


def _get_folder_mtime(path):
    """Return folder mtime as Unix timestamp, or 0 on failure."""
    try:
        return int(os.path.getmtime(path))
    except OSError as e:
        logger.debug(f"[library] Cannot stat {path}: {e}")
        return 0


def _collect_episodes(folder_path):
    """Collect episode details from a torrent folder.

    Returns dict: {(season_num, ep_num): {'file': str, 'path': str}}
    Handles both structured (Season X subdirs) and flat layouts (S01E01.mkv
    directly in folder).
    """
    episodes = {}
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
                                episodes[key] = {'file': f.name, 'path': f.path, 'size_bytes': sz}
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
                            episodes[key] = {'file': entry.name, 'path': entry.path, 'size_bytes': sz}
    except (PermissionError, OSError, FileNotFoundError):
        pass
    return episodes


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


def _enrich_with_tmdb_cache(movies, shows):
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


# Public aliases for cross-module reuse (e.g., debrid_client title matching)
parse_folder_name = _parse_folder_name
normalize_title = _normalize_title
norm_for_matching = _norm_for_matching


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


class LibraryScanner:
    def __init__(self):
        self._mount_path = _discover_mount()
        self._local_movies_path = os.environ.get('BLACKHOLE_LOCAL_LIBRARY_MOVIES', '').strip() or None
        self._local_tv_path = os.environ.get('BLACKHOLE_LOCAL_LIBRARY_TV', '').strip() or None
        self._cache = None
        self._cache_time = 0
        self._ttl = 600
        self._lock = threading.Lock()
        self._scanning = False
        self._effects_running = False
        self._path_index = {}
        self._local_path_index = {}
        self._path_lock = threading.Lock()
        self._search_cooldown = {}  # {(norm, sn): timestamp} — suppress re-search for 1 hour
        self._alias_norms = {}     # {norm_title: set of alias norm_titles}
        try:
            self._debrid_unavailable_days = int(os.environ.get('DEBRID_UNAVAILABLE_THRESHOLD_DAYS', '3'))
        except (ValueError, TypeError):
            self._debrid_unavailable_days = 3
        try:
            self._pending_warning_hours = int(os.environ.get('PENDING_WARNING_HOURS', '24'))
        except (ValueError, TypeError):
            self._pending_warning_hours = 24
        self._last_had_local = None    # None=unknown, True=had local content
        self._local_drop_alerted = False

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

        Returns ``[]`` when TMDB has no cached episode list for the show —
        an empty return must be treated as "don't know" so we never trigger
        a spurious search for a title we lack ground truth on.
        """
        from utils.tmdb import get_cached_episode_list
        norm = _normalize_title(show.get('title', ''))
        expected = get_cached_episode_list(norm, show.get('year'))
        if not expected:
            return []
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
            key = (ep['season'], ep['number'])
            if key not in present:
                missing.append(key)
        return missing

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
                logger.debug("[library] WebDAV scan succeeded")
            except Exception as e:
                logger.info(f"[library] WebDAV scan unavailable, using FUSE: {e}")
                try:
                    from utils.rclone_rc import refresh_dir
                    refresh_dir('', recursive=True)
                except Exception:
                    pass
                debrid_movies, debrid_shows = self._scan_mount(self._mount_path, deadline)

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
            if matched_key is not None:
                if matched_key != key:
                    logger.debug(
                        f"[library] TMDB alias match (movie): debrid '{key}' ↔ local '{matched_key}'"
                    )
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
            if local_key is not None:
                if local_key != key:
                    logger.debug(
                        f"[library] TMDB alias match: debrid '{key}' ↔ local '{local_key}'"
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
        renames = _enrich_with_tmdb_cache(movies, shows) or []
        for old_norm, new_norm in renames:
            if not old_norm or not new_norm:
                continue
            alias_norms_local.setdefault(new_norm, set()).add(old_norm)
            alias_norms_local.setdefault(old_norm, set()).add(new_norm)

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

        elapsed_ms = int((time.monotonic() - start) * 1000)
        return {
            'movies': movies,
            'shows': shows,
            'preferences': preferences,
            'last_scan': datetime.now(timezone.utc).isoformat(timespec='seconds'),
            'scan_duration_ms': elapsed_ms,
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
        with self._lock:
            self._cache = data
            self._cache_time = time.monotonic()
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
                                       meta={'info_hash': bl_hash})

            if deleted or bl_hash:
                logger.info(f"[library] Disc rip cleaned: {title} — "
                            f"deleted {deleted} torrent(s), blocklisted {'yes' if bl_hash else 'no'}")
                if _history:
                    _history.log_event('failed', title, source='library',
                                       detail=f'Disc rip removed ({deleted} torrent(s) deleted)',
                                       meta={'info_hash': bl_hash})
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
                                               detail=f"Switched {result['switched']} episode(s) to debrid")
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
                                           detail="Switched movie to debrid")
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
                                                       detail=f"Removed {deleted} debrid torrent(s) — prefer-local")
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

        from utils.library_prefs import get_all_pending, set_pending, touch_pending_searched, update_pending_error
        from utils.tmdb import search_show as tmdb_search_show, search_movie as tmdb_search_movie

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
                norm = _normalize_title(show['title'])
                route = self._route_for(norm, preferences)
                direction = {True: 'to-debrid', False: 'to-local', None: 'to-any'}[route]

                # Check pending state — skip debrid-unavailable, allow retries
                # for stale entries whose direction matches the current route
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
                    continue  # escalated — stop retrying
                pending_keys = set()
                is_retry = False
                if pe_dir and pe_dir != direction:
                    # Direction changed (e.g., user flipped from prefer-debrid
                    # to unset).  Clear the stale-direction entry so
                    # touch_pending_searched / update_pending_error below don't
                    # mutate a wrong-direction entry that would then zombify
                    # when the whole season loop errors out.
                    from utils.library_prefs import clear_pending
                    clear_pending(pending_norm)
                    pending_entry = {}
                    pe_dir = ''
                if pe_dir == direction:
                    # Check if the last search attempt is recent enough to skip
                    last_ts = pending_entry.get('last_searched') or pending_entry.get('created')
                    stale = True
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
                        is_retry = True  # allow retry — pending_keys stays empty
                    else:
                        pending_keys = {
                            (e['season'], e['episode'])
                            for e in pending_entry.get('episodes', [])
                        }
                        # Record "waiting" status with next retry time
                        remaining_h = self._SEARCH_RETRY_HOURS - age_hours
                        retry_dt = ls_dt + timedelta(hours=self._SEARCH_RETRY_HOURS)
                        update_pending_error(
                            pending_norm,
                            f"Waiting for retry ({remaining_h:.1f}h remaining)",
                            next_retry_at=retry_dt.isoformat(timespec='seconds'),
                            increment_retry=False,
                        )

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
                for sn, ep_nums in by_season.items():
                    try:
                        result = show_client.ensure_and_search(
                            show['title'], show_tmdb_id, sn, ep_nums,
                            prefer_debrid=route, respect_monitored=respect_mon,
                        )
                        status = result.get('status', '')
                        if status in ('sent', 'pending'):
                            for en in ep_nums:
                                new_pending.append({'season': sn, 'episode': en})
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

                pending_entry = pending.get(norm)
                pending_norm = norm
                if not pending_entry:
                    for _pa in self._alias_norms.get(norm, ()):
                        pending_entry = pending.get(_pa)
                        if pending_entry:
                            pending_norm = _pa
                            break
                pending_entry = pending_entry or {}
                pe_dir = pending_entry.get('direction', '')
                if pe_dir == 'debrid-unavailable':
                    continue  # escalated — stop retrying
                movie_is_retry = False
                if pe_dir and pe_dir != direction:
                    # Direction changed (preference flipped) — clear stale-direction
                    # entry so touch / update_pending_error below don't zombify it.
                    from utils.library_prefs import clear_pending
                    clear_pending(pending_norm)
                    pending_entry = {}
                    pe_dir = ''
                if pe_dir == direction:
                    # Allow retry if last search is stale
                    last_ts = pending_entry.get('last_searched') or pending_entry.get('created')
                    stale = True
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
                    if not stale:
                        # Record "waiting" status with next retry time
                        remaining_h = self._SEARCH_RETRY_HOURS - age_hours
                        retry_dt = ls_dt + timedelta(hours=self._SEARCH_RETRY_HOURS)
                        update_pending_error(
                            pending_norm,
                            f"Waiting for retry ({remaining_h:.1f}h remaining)",
                            next_retry_at=retry_dt.isoformat(timespec='seconds'),
                            increment_retry=False,
                        )
                        continue  # recent search — skip
                    movie_is_retry = True
                if (norm, 0) in self._search_cooldown:
                    continue  # recently attempted

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
                for t in escalated:
                    _history.log_event('debrid_unavailable', t, source='library',
                                       detail=f'Marked debrid-unavailable after {threshold_days}+ days')
            try:
                from utils.notifications import notify
                summary = ', '.join(escalated[:5])
                if len(escalated) > 5:
                    summary += f', +{len(escalated) - 5} more'
                notify('debrid_unavailable',
                       f'Debrid Unavailable ({len(escalated)})',
                       f'Content not found on debrid after {threshold_days} days: {summary}',
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

        Only touches symlinks whose targets start with BLACKHOLE_SYMLINK_TARGET_BASE.
        Real files and non-debrid symlinks are never modified.

        Must run BEFORE ``_create_debrid_symlinks`` in the effects pipeline.
        """
        if not str(os.environ.get('BLACKHOLE_SYMLINK_ENABLED', '')).lower() == 'true':
            return
        rclone_mount = os.environ.get('BLACKHOLE_RCLONE_MOUNT', '').strip()
        symlink_base = os.environ.get('BLACKHOLE_SYMLINK_TARGET_BASE', '').strip()
        if not rclone_mount or not symlink_base:
            return

        rclone_real = os.path.realpath(rclone_mount)
        base_prefix = symlink_base.rstrip(os.sep) + os.sep

        # Guard: verify the rclone mount exists, is responsive, and has content.
        # A missing or stalled FUSE mount makes os.path.exists() return False
        # for everything, which would cause mass deletion of valid symlinks.
        # Zurg category stubs (movies/, shows/) can persist even when all
        # content is gone, so check that at least one known category dir
        # is non-empty.  Mirrors the guard in scheduled_tasks.verify_symlinks.
        try:
            if not os.path.isdir(rclone_real) or not os.listdir(rclone_real):
                logger.debug("[library] Rclone mount empty or missing at %s — "
                             "skipping broken symlink cleanup", rclone_real)
                return
            _mount_cats = ('movies', 'shows', 'anime')
            if not any(
                os.path.isdir(os.path.join(rclone_real, c))
                and os.listdir(os.path.join(rclone_real, c))
                for c in _mount_cats
            ):
                logger.debug("[library] Rclone mount categories empty at %s — "
                             "skipping broken symlink cleanup", rclone_real)
                return
        except OSError:
            logger.debug("[library] Rclone mount not responsive at %s — "
                         "skipping broken symlink cleanup", rclone_real)
            return

        removed = 0

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
                                if not target.startswith(base_prefix):
                                    continue
                                # Translate arr-namespace target to rclone mount for existence check
                                check_path = os.path.normpath(
                                    rclone_real + os.sep + target[len(base_prefix):]
                                )
                                # Ensure translated path stays within the mount
                                if not check_path.startswith(rclone_real + os.sep):
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
            except (PermissionError, OSError) as e:
                logger.debug("[library] Cannot scan %s for broken symlinks: %s", lib_path, e)

        if removed:
            logger.info("[library] Cleaned up %d broken debrid symlink(s) from local library", removed)

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
        created = 0
        symlinked_shows = set()   # titles that got new symlinks
        symlinked_movies = set()  # titles that got new symlinks
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
                for s in (client.get_all_series() or []):
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
                for m in (client.get_all_movies() or []):
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
        from utils.tmdb import get_cached_tmdb_ids, find_show_tmdb_id_by_season
        cached_tmdb_ids = get_cached_tmdb_ids()
        cached_tmdb_movies = cached_tmdb_ids.get('movies', {})
        cached_tmdb_shows = cached_tmdb_ids.get('shows', {})

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

                # Skip blocklisted items by mount folder name (full release name).
                # Only match on the release folder — not the parsed title — so
                # that blocking one quality/release doesn't block replacements
                # of the same movie (e.g. blocking a 2160p doesn't block a 1080p).
                if _blocklist:
                    mount_folder = os.path.basename(mount_dir)
                    if _blocklist.is_blocked_title(mount_folder):
                        continue

                # Year-qualified exact match first to disambiguate same-title
                # different-year entries (e.g. "The Bridge" 2011 vs 2013)
                arr_info = None
                if year:
                    arr_info = radarr_map.get(f"{title} ({year})".lower())
                if not arr_info:
                    arr_info = radarr_map.get(title.lower()) or radarr_map_norm.get(_norm_for_matching(title))
                # Fallback: match via TMDB ID when title differs.  The TMDB
                # cache is keyed by the parsed-folder norm — use _parsed_title
                # (preserved when the display title was upgraded to canonical)
                # so renamed items still resolve via cache.
                if not arr_info:
                    _norm = _normalize_title(movie.get('_parsed_title') or title)
                    tmdb_id = (cached_tmdb_movies.get(f"{_norm} ({year})") if year else None) or cached_tmdb_movies.get(_norm)
                    if tmdb_id:
                        arr_info = radarr_by_tmdb.get(tmdb_id)
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

                # Find the largest media file in the torrent folder
                media_file = None
                media_size = -1
                try:
                    for fname in os.listdir(mount_dir):
                        ext = os.path.splitext(fname)[1].lower()
                        if ext in MEDIA_EXTENSIONS:
                            fpath = os.path.join(mount_dir, fname)
                            try:
                                sz = os.path.getsize(fpath)
                            except OSError:
                                sz = 0
                            if sz > media_size:
                                media_size = sz
                                media_file = fname
                except OSError:
                    continue
                if not media_file:
                    continue

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

                real_debrid = os.path.realpath(os.path.join(mount_dir, media_file))
                if not real_debrid.startswith(real_mount + os.sep) and real_debrid != real_mount:
                    continue
                symlink_target = symlink_base + real_debrid[len(real_mount):]

                try:
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                    os.symlink(symlink_target, local_path)
                    created += 1
                    symlinked_movies.add(title)
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
                # Year-qualified exact match first to disambiguate same-title
                # different-year entries (e.g. "The Bridge" 2011 vs 2013)
                arr_info = None
                if year:
                    arr_info = sonarr_map.get(f"{title} ({year})".lower())
                if not arr_info:
                    arr_info = sonarr_map.get(title.lower()) or sonarr_map_norm.get(_norm_for_matching(title))
                if not arr_info:
                    # Season-aware TMDB fallback: use the show's max season
                    # to disambiguate reboots/revivals with the same title.
                    # The TMDB cache is keyed by parsed-folder norm — use
                    # _parsed_title so renamed items still resolve via cache.
                    show_max_sn = _show_max_season.get(title)
                    parsed_norm = _normalize_title(show.get('_parsed_title') or title)
                    tmdb_id = (cached_tmdb_shows.get(f"{parsed_norm} ({year})") if year else None) or cached_tmdb_shows.get(parsed_norm)
                    if tmdb_id:
                        arr_info = sonarr_by_tmdb.get(tmdb_id)
                    if not arr_info and show_max_sn:
                        alt_id = find_show_tmdb_id_by_season(parsed_norm, show_max_sn, year)
                        if alt_id and alt_id != tmdb_id:
                            arr_info = sonarr_by_tmdb.get(alt_id)
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

                        # Translate mount path to Sonarr/arr namespace
                        real_debrid = os.path.realpath(debrid_path)
                        if not real_debrid.startswith(real_mount + os.sep) and real_debrid != real_mount:
                            continue
                        symlink_target = symlink_base + real_debrid[len(real_mount):]

                        try:
                            os.makedirs(os.path.dirname(local_path), exist_ok=True)
                            os.symlink(symlink_target, local_path)
                            created += 1
                            symlinked_shows.add(title)
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

        if created:
            logger.info(f"[library] Created {created} debrid symlink(s) in local library")
            if _history:
                for t in symlinked_shows:
                    _history.log_event('symlink_created', t, source='library',
                                       detail=f'Debrid symlink(s) created in local library')
                for t in symlinked_movies:
                    _history.log_event('symlink_created', t, source='library',
                                       detail=f'Debrid symlink(s) created in local library')
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
                                       detail=f'Symlink creation failed: {err}')
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
            # Trigger arr rescans so Sonarr/Radarr discover the new files
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
                # Year-qualified exact match first (symmetric with show dir selection)
                info = None
                _yr = _symlink_years.get(title)
                if _yr:
                    info = sonarr_map.get(f"{title} ({_yr})".lower())
                if not info:
                    info = sonarr_map.get(title.lower()) or sonarr_map_norm.get(_norm_for_matching(title))
                if not info:
                    # TMDB cache is keyed by parsed-folder norm — use the
                    # stashed parsed title so renamed items still resolve.
                    norm_t = _normalize_title(_symlink_parsed.get(title) or title)
                    tmdb_id = (cached_tmdb_shows.get(f"{norm_t} ({_yr})") if _yr else None) or cached_tmdb_shows.get(norm_t)
                    if tmdb_id:
                        info = sonarr_by_tmdb.get(tmdb_id)
                    if not info:
                        max_sn = _show_max_season.get(title, 0)
                        if max_sn:
                            alt_id = find_show_tmdb_id_by_season(norm_t, max_sn, _yr)
                            if alt_id and alt_id != tmdb_id:
                                info = sonarr_by_tmdb.get(alt_id)
                if info and info.get('id') and info.get('client'):
                    try:
                        info['client'].rescan_series(info['id'], media_title=title)
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
                # Year-qualified exact match first (symmetric with movie dir selection)
                info = None
                _yr = _symlink_years.get(title)
                if _yr:
                    info = radarr_map.get(f"{title} ({_yr})".lower())
                if not info:
                    info = radarr_map.get(title.lower()) or radarr_map_norm.get(_norm_for_matching(title))
                if not info:
                    # TMDB cache is keyed by parsed-folder norm — use the
                    # stashed parsed title so renamed items still resolve.
                    _norm_t = _normalize_title(_symlink_parsed.get(title) or title)
                    tmdb_id = (cached_tmdb_movies.get(f"{_norm_t} ({_yr})") if _yr else None) or cached_tmdb_movies.get(_norm_t)
                    if tmdb_id:
                        info = radarr_by_tmdb.get(tmdb_id)
                if info and info.get('id') and info.get('client'):
                    try:
                        info['client'].rescan_movie(info['id'], media_title=title)
                        logger.info(f"[library] Triggered Radarr rescan for {title}")
                    except Exception as e:
                        logger.warning(f"[library] Radarr rescan failed for {title}: {e}")
                elif radarr_map:
                    logger.warning(f"[library] Could not match '{title}' to a Radarr movie — rescan skipped")

    # Category names that indicate TV/show content
    _SHOW_CATEGORIES = {'shows', 'tv', 'anime', 'series', 'television'}
    # Internal Zurg directories to always skip
    _SKIP_CATEGORIES = {'__all__', '__unplayable__'}

    def _scan_mount(self, mount_path, deadline=None):
        """Scan all category directories on the mount and aggregate by title.

        Debrid mounts have one folder per torrent, so the same show appears
        many times (one per grabbed episode/season pack). This method collects
        episode IDs from every folder, then groups by normalized title so each
        show becomes a single entry with correct season/episode counts.
        Movies are also deduplicated by title.
        """
        try:
            categories = []
            with os.scandir(mount_path) as it:
                for entry in it:
                    if entry.is_dir(follow_symlinks=False):
                        categories.append(entry.name)
        except (PermissionError, OSError) as e:
            logger.warning(f"[library] Cannot list mount {mount_path}: {e}")
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
            cat_path = os.path.join(mount_path, category)
            category_is_shows = category.lower() in self._SHOW_CATEGORIES
            try:
                with os.scandir(cat_path) as it:
                    for entry in it:
                        if deadline is not None and time.monotonic() > deadline:
                            logger.warning("[library] Timeout during mount scan")
                            timed_out = True
                            break
                        if not entry.is_dir(follow_symlinks=False):
                            continue
                        if entry.name.lower() in _SKIP_FOLDERS:
                            continue
                        title, year = _parse_folder_name(entry.name)
                        if not title:
                            continue
                        episodes = _collect_episodes(entry.path)
                        is_show = len(episodes) > 0
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
                            if key not in show_groups:
                                show_groups[key] = {
                                    'title': title,
                                    'year': year,
                                    'episodes': dict(episodes),
                                    'path': entry.path,
                                }
                            else:
                                existing = show_groups[key]['episodes']
                                for ep_key, ep_info in episodes.items():
                                    if ep_key not in existing:
                                        existing[ep_key] = ep_info
                                    elif ep_info.get('_folder_ep_count', 1) > existing[ep_key].get('_folder_ep_count', 1):
                                        existing[ep_key] = ep_info
                                # Prefer title with year or better capitalization
                                if year and not show_groups[key]['year']:
                                    show_groups[key]['year'] = year
                                    show_groups[key]['title'] = title
                                elif title[0:1].isupper() and not show_groups[key]['title'][0:1].isupper():
                                    show_groups[key]['title'] = title
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
                logger.warning(f"[library] Cannot scan {cat_path}: {e}")
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
                'type': 'show',
                'seasons': len(unique_seasons),
                'episodes': len(eps),
                '_episodes': eps,
                'path': g['path'],
                'date_added': _get_folder_mtime(g['path']),
            })

        return movies, shows

    def _webdav_scan_mount(self, deadline=None):
        """Scan the debrid mount via WebDAV PROPFIND directly to Zurg.

        Bypasses FUSE/rclone to avoid hundreds of kernel round-trips.
        Returns (movies, shows) in the same format as _scan_mount().

        Raises Exception on any failure so the caller can fall back to FUSE.
        """
        from utils.webdav import propfind

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

        for category in scan_cats:
            if deadline and time.monotonic() > deadline:
                logger.warning("[library] WebDAV: deadline reached, skipping remaining categories")
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

            # Zurg may not support true depth-infinity — if every entry is a
            # collection and no files were found, the PROPFIND only returned
            # folder names without recursing into them.  Bail out so the
            # caller falls back to FUSE scanning which handles this correctly.
            has_files = any(
                contents['files'] or contents['season_files']
                for contents in folders.values()
            )
            if folders and not has_files:
                raise RuntimeError(
                    f"WebDAV depth-infinity returned {len(folders)} folders but 0 files "
                    f"for {category} — Zurg likely does not support recursive PROPFIND"
                )

            # Step 3: Process folders into show_groups / movie_groups
            for folder_name, contents in folders.items():
                title, year = _parse_folder_name(folder_name)
                if not title:
                    continue

                episodes = self._collect_episodes_from_webdav(contents)
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
                    if key not in show_groups:
                        show_groups[key] = {
                            'title': title,
                            'year': year,
                            'episodes': dict(episodes),
                            'path': os.path.join(self._mount_path, category, folder_name),
                        }
                    else:
                        existing = show_groups[key]['episodes']
                        for ep_key, ep_info in episodes.items():
                            if ep_key not in existing:
                                existing[ep_key] = ep_info
                            elif ep_info.get('_folder_ep_count', 1) > existing[ep_key].get('_folder_ep_count', 1):
                                existing[ep_key] = ep_info
                        if year and not show_groups[key]['year']:
                            show_groups[key]['year'] = year
                            show_groups[key]['title'] = title
                        elif title[0:1].isupper() and not show_groups[key]['title'][0:1].isupper():
                            show_groups[key]['title'] = title
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
    def _collect_episodes_from_webdav(contents):
        """Extract episodes from WebDAV folder contents.

        Mirrors _collect_episodes() logic but works on pre-parsed WebDAV data
        instead of os.scandir.
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
                episodes[key] = {'file': fname, 'path': fpath, 'size_bytes': fsize}

        # Check flat files in folder root
        for fname, fsize, fpath in contents.get('files', []):
            ext = os.path.splitext(fname)[1].lower()
            if ext in MEDIA_EXTENSIONS:
                ep_match = _EPISODE_ID_PATTERN.search(fname)
                if ep_match:
                    key = (int(ep_match.group(1)), int(ep_match.group(2)))
                    episodes[key] = {'file': fname, 'path': fpath, 'size_bytes': fsize}

        return episodes

    def _scan_local_movies(self):
        items = []
        if not self._local_movies_path:
            return items
        if not os.path.isdir(self._local_movies_path):
            logger.warning(f"[library] Local movies path not found: {self._local_movies_path}")
            return items
        symlink_base = os.environ.get('BLACKHOLE_SYMLINK_TARGET_BASE', '').strip()
        try:
            with os.scandir(self._local_movies_path) as it:
                for entry in it:
                    if not entry.is_dir(follow_symlinks=False):
                        continue
                    # Skip known non-media folders before any I/O
                    if entry.name.lower() in _SKIP_FOLDERS:
                        continue
                    # Skip folders that only contain debrid symlinks
                    if symlink_base and self._is_debrid_symlink_dir(entry.path, symlink_base):
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
    def _is_debrid_symlink_dir(path, symlink_base):
        """Check if a directory contains only debrid symlinks (no real media files).

        Only considers media-extension files. Non-media files (.nfo, .srt, .jpg)
        are ignored so Radarr metadata doesn't cause false local classification.
        Returns False for empty directories.
        """
        prefix = symlink_base.rstrip(os.sep) + os.sep
        has_debrid_symlink = False
        try:
            with os.scandir(path) as it:
                for f in it:
                    ext = os.path.splitext(f.name)[1].lower()
                    if ext not in MEDIA_EXTENSIONS:
                        continue
                    if f.is_symlink():
                        target = os.readlink(f.path)
                        if not target.startswith(prefix):
                            return False  # symlink to non-debrid location
                        has_debrid_symlink = True
                    elif f.is_file(follow_symlinks=False):
                        return False  # real media file = genuine local content
        except OSError:
            return False
        return has_debrid_symlink  # False for empty dirs

    @staticmethod
    def _has_media_files(path):
        """Check if a directory contains at least one media file (real or symlink).

        Used to avoid classifying metadata-only directories (leftover .nfo/.jpg
        from Radarr after symlinks were deleted) as genuine local content.
        """
        try:
            with os.scandir(path) as it:
                for f in it:
                    ext = os.path.splitext(f.name)[1].lower()
                    if ext in MEDIA_EXTENSIONS and (f.is_file() or f.is_symlink()):
                        return True
        except OSError:
            pass
        return False

    @staticmethod
    def _is_debrid_symlink_only(path, symlink_base):
        """Check if a show directory tree contains only debrid symlinks (no real media files).

        Walks into Season subdirectories to check episode files. Non-media files
        are ignored. Returns False for empty directories.
        """
        prefix = symlink_base.rstrip(os.sep) + os.sep
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
                                        if not os.readlink(f.path).startswith(prefix):
                                            return False
                                    elif f.is_file(follow_symlinks=False):
                                        return False  # real file
                        except OSError:
                            pass
                    elif entry.is_symlink():
                        ext = os.path.splitext(entry.name)[1].lower()
                        if ext in MEDIA_EXTENSIONS:
                            has_any_media = True
                            if not os.readlink(entry.path).startswith(prefix):
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
        symlink_base = os.environ.get('BLACKHOLE_SYMLINK_TARGET_BASE', '').strip()
        try:
            with os.scandir(self._local_tv_path) as it:
                for entry in it:
                    if not entry.is_dir(follow_symlinks=False):
                        continue
                    # Skip known non-media folders before any I/O
                    if entry.name.lower() in _SKIP_FOLDERS:
                        continue
                    # Skip show folders that are entirely debrid symlinks
                    if symlink_base and self._is_debrid_symlink_only(entry.path, symlink_base):
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
        me = movie.get('missing_episodes')
        if me is not None and me > 0:
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
