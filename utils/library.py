"""Library scanner for debrid (rclone mount) and local media content.

Walks Zurg mount categories and local library directories to build a
unified item list, cross-referencing by title to detect content present
in both sources.
"""

import os
import re
import threading
import time
from datetime import datetime, timezone
from utils.logger import get_logger

logger = get_logger()

MEDIA_EXTENSIONS = {'.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.ts', '.m4v', '.webm'}

# Quality and codec markers stripped when parsing folder names
_QUALITY_PATTERN = re.compile(
    r'[\s.\-_]('
    r'2160p|1080p|1080i|720p|480p|4K|UHD|HD|SD|'
    r'BluRay|Blu-Ray|BDRip|BDRemux|REMUX|BDMV|'
    r'WEB-DL|WEBRip|WEBRIP|WEBDL|WEB|'
    r'HDTV|DVDRip|DVD|HDRip|'
    r'x264|x265|H264|H265|HEVC|AVC|AV1|VP9|'
    r'AAC|AC3|DTS|TrueHD|FLAC|MP3|EAC3|'
    r'HDR|HDR10|DV|DoVi|Atmos|'
    r'PROPER|REPACK|EXTENDED|THEATRICAL|'
    r'NF|AMZN|HULU|DSNP|ATVP|PCOK|HBO|MAX|IMAX'
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


def _parse_folder_name(name):
    title = name

    # Strip site/indexer prefixes: "www.UIndex.org.Show.Name" → "Show.Name"
    title = _SITE_PREFIX_PATTERN.sub('', title)
    # Strip bracket tags: "[TorrentDay] Show.Name" → "Show.Name"
    title = _BRACKET_TAG_PATTERN.sub('', title)

    # Strip trailing year in parens: "Movie Name (2024)"
    year = None
    year_match = _YEAR_PATTERN.search(title)
    if year_match:
        year = int(year_match.group(1))
        title = title[:year_match.start()].strip()
        # Replace dots/dashes/underscores with spaces
        title = _DOTS_DASHES_PATTERN.sub(' ', title)
        title = _MULTI_SPACE_PATTERN.sub(' ', title).strip()
        return title, year

    # Strip S01E01-style markers (TV episodes/seasons)
    season_match = _SEASON_EPISODE_PATTERN.search(title)
    if season_match:
        title = title[:season_match.start()]
        title = _DOTS_DASHES_PATTERN.sub(' ', title)
        title = _MULTI_SPACE_PATTERN.sub(' ', title).strip()
        return title, None

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

    title = _DOTS_DASHES_PATTERN.sub(' ', title)
    title = _MULTI_SPACE_PATTERN.sub(' ', title).strip()
    return title, year


_EPISODE_PATTERN = re.compile(r'S\d{1,2}E\d{1,2}', re.IGNORECASE)
_EPISODE_ID_PATTERN = re.compile(r'S(\d{1,2})E(\d{1,2})', re.IGNORECASE)
_SEASON_DIR_PATTERN = re.compile(r'^Season\s+(\d+)$', re.IGNORECASE)


def _collect_episode_ids(folder_path):
    """Collect unique (season, episode) tuples from a torrent folder.

    Handles both structured (Season X subdirs) and flat layouts (S01E01.mkv
    directly in folder). Returns a set of (season_num, episode_num) ints.
    """
    ids = set()
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
                                    ids.add((int(ep_match.group(1)), int(ep_match.group(2))))
                                else:
                                    # File in Season dir but no S##E## in name — assign sequential
                                    ids.add((season_num, len(ids) + 1000))
                    except (PermissionError, OSError):
                        pass
                elif entry.is_file(follow_symlinks=False):
                    ext = os.path.splitext(entry.name)[1].lower()
                    if ext in MEDIA_EXTENSIONS:
                        ep_match = _EPISODE_ID_PATTERN.search(entry.name)
                        if ep_match:
                            ids.add((int(ep_match.group(1)), int(ep_match.group(2))))
    except (PermissionError, OSError, FileNotFoundError):
        pass
    return ids


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


def _normalize_title(title):
    t = title.lower()
    t = re.sub(r'\s*\(\d{4}\)\s*$', '', t)
    t = t.strip()
    return t


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

        if self._mount_path:
            logger.info(f"[library] Mount path: {self._mount_path}")
        else:
            logger.warning("[library] No rclone mount discovered; debrid library will be empty")

        if self._local_movies_path:
            logger.info(f"[library] Local movies: {self._local_movies_path}")
        if self._local_tv_path:
            logger.info(f"[library] Local TV: {self._local_tv_path}")

    def scan(self):
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
            debrid_movies, debrid_shows = self._scan_mount(self._mount_path, deadline)

        local_movies = self._scan_local_movies()
        local_shows = self._scan_local_shows()

        # Build normalized title index for cross-referencing
        debrid_movie_keys = {_normalize_title(m['title']): m for m in debrid_movies}
        debrid_show_keys = {_normalize_title(s['title']): s for s in debrid_shows}

        movies = []
        # Merge debrid + local movies
        for item in debrid_movies:
            key = _normalize_title(item['title'])
            if key in {_normalize_title(lm['title']) for lm in local_movies}:
                item = dict(item)
                item['source'] = 'both'
            movies.append(item)

        for lm in local_movies:
            key = _normalize_title(lm['title'])
            if key not in debrid_movie_keys:
                movies.append(lm)

        shows = []
        for item in debrid_shows:
            key = _normalize_title(item['title'])
            if key in {_normalize_title(ls['title']) for ls in local_shows}:
                item = dict(item)
                item['source'] = 'both'
            shows.append(item)

        for ls in local_shows:
            key = _normalize_title(ls['title'])
            if key not in debrid_show_keys:
                shows.append(ls)

        elapsed_ms = int((time.monotonic() - start) * 1000)
        return {
            'movies': movies,
            'shows': shows,
            'last_scan': datetime.now(timezone.utc).isoformat(timespec='seconds'),
            'scan_duration_ms': elapsed_ms,
        }

    def get_data(self):
        with self._lock:
            now = time.monotonic()
            if self._cache is not None and (now - self._cache_time) < self._ttl:
                return self._cache

        # Cache expired or empty — scan synchronously so caller always gets data
        data = self.scan()
        with self._lock:
            self._cache = data
            self._cache_time = time.monotonic()
        return data

    def refresh(self):
        with self._lock:
            if self._scanning:
                return
            self._scanning = True

        def _run():
            try:
                data = self.scan()
                with self._lock:
                    self._cache = data
                    self._cache_time = time.monotonic()
                    logger.debug(
                        f"[library] Scan complete: {len(data['movies'])} movies, "
                        f"{len(data['shows'])} shows in {data['scan_duration_ms']}ms"
                    )
            except Exception as e:
                logger.error(f"[library] Scan error: {e}")
            finally:
                with self._lock:
                    self._scanning = False

        t = threading.Thread(target=_run, daemon=True)
        t.start()

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
        show_groups = {}   # normalized_title -> {title, year, episode_ids, path}
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
                        title, year = _parse_folder_name(entry.name)
                        episode_ids = _collect_episode_ids(entry.path)
                        is_show = len(episode_ids) > 0 or category_is_shows

                        if is_show:
                            key = _normalize_title(title)
                            if key not in show_groups:
                                show_groups[key] = {
                                    'title': title,
                                    'year': year,
                                    'episode_ids': set(episode_ids),
                                    'path': entry.path,
                                }
                            else:
                                show_groups[key]['episode_ids'] |= episode_ids
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
            movies.append({
                'title': g['title'],
                'year': g['year'],
                'source': 'debrid',
                'type': 'movie',
                'seasons': 0,
                'episodes': 0,
                'path': g['path'],
            })

        shows = []
        for g in show_groups.values():
            unique_seasons = {s for s, _e in g['episode_ids']} if g['episode_ids'] else set()
            shows.append({
                'title': g['title'],
                'year': g['year'],
                'source': 'debrid',
                'type': 'show',
                'seasons': len(unique_seasons),
                'episodes': len(g['episode_ids']),
                'path': g['path'],
            })

        return movies, shows

        return movies, shows

    def _scan_local_movies(self):
        items = []
        if not self._local_movies_path:
            return items
        if not os.path.isdir(self._local_movies_path):
            logger.warning(f"[library] Local movies path not found: {self._local_movies_path}")
            return items
        try:
            with os.scandir(self._local_movies_path) as it:
                for entry in it:
                    if not entry.is_dir(follow_symlinks=False):
                        continue
                    title, year = _parse_folder_name(entry.name)
                    items.append({
                        'title': title,
                        'year': year,
                        'source': 'local',
                        'type': 'movie',
                        'seasons': 0,
                        'episodes': 0,
                        'path': entry.path,
                    })
        except (PermissionError, OSError) as e:
            logger.warning(f"[library] Cannot scan local movies: {e}")
        return items

    def _scan_local_shows(self):
        items = []
        if not self._local_tv_path:
            return items
        if not os.path.isdir(self._local_tv_path):
            logger.warning(f"[library] Local TV path not found: {self._local_tv_path}")
            return items
        try:
            with os.scandir(self._local_tv_path) as it:
                for entry in it:
                    if not entry.is_dir(follow_symlinks=False):
                        continue
                    title, year = _parse_folder_name(entry.name)
                    seasons, episodes = _count_show_content(entry.path)
                    items.append({
                        'title': title,
                        'year': year,
                        'source': 'local',
                        'type': 'show',
                        'seasons': seasons,
                        'episodes': episodes,
                        'path': entry.path,
                    })
        except (PermissionError, OSError) as e:
            logger.warning(f"[library] Cannot scan local TV: {e}")
        return items


_scanner = None


def setup():
    global _scanner
    _scanner = LibraryScanner()
    _scanner.refresh()


def get_scanner():
    return _scanner
