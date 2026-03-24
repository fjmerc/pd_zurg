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


def _parse_folder_name(name):
    title = name

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


def _count_show_content(show_path):
    seasons = 0
    episodes = 0
    season_re = re.compile(r'^Season\s+\d+$', re.IGNORECASE)
    try:
        with os.scandir(show_path) as it:
            for entry in it:
                if not entry.is_dir(follow_symlinks=False):
                    continue
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
    except (PermissionError, OSError, FileNotFoundError):
        pass
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

        debrid_movies = []
        debrid_shows = []

        if self._mount_path:
            debrid_movies = self._scan_mount_movies(self._mount_path, deadline)
            if time.monotonic() < deadline:
                debrid_shows = self._scan_mount_shows(self._mount_path, deadline)
            else:
                logger.warning("[library] Mount scan timeout reached before shows scan")

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

    def _scan_mount_movies(self, mount_path, deadline=None):
        movies_dir = os.path.join(mount_path, 'movies')
        items = []
        if not os.path.isdir(movies_dir):
            return items
        try:
            with os.scandir(movies_dir) as it:
                for entry in it:
                    if deadline is not None and time.monotonic() > deadline:
                        logger.warning("[library] Timeout during mount movies scan")
                        break
                    if not entry.is_dir(follow_symlinks=False):
                        continue
                    title, year = _parse_folder_name(entry.name)
                    items.append({
                        'title': title,
                        'year': year,
                        'source': 'debrid',
                        'type': 'movie',
                        'seasons': 0,
                        'episodes': 0,
                        'path': entry.path,
                    })
        except (PermissionError, OSError) as e:
            logger.warning(f"[library] Cannot scan {movies_dir}: {e}")
        return items

    def _scan_mount_shows(self, mount_path, deadline=None):
        shows_dir = os.path.join(mount_path, 'shows')
        items = []
        if not os.path.isdir(shows_dir):
            return items
        try:
            with os.scandir(shows_dir) as it:
                for entry in it:
                    if deadline is not None and time.monotonic() > deadline:
                        logger.warning("[library] Timeout during mount shows scan")
                        break
                    if not entry.is_dir(follow_symlinks=False):
                        continue
                    title, year = _parse_folder_name(entry.name)
                    seasons, episodes = _count_show_content(entry.path)
                    items.append({
                        'title': title,
                        'year': year,
                        'source': 'debrid',
                        'type': 'show',
                        'seasons': seasons,
                        'episodes': episodes,
                        'path': entry.path,
                    })
        except (PermissionError, OSError) as e:
            logger.warning(f"[library] Cannot scan {shows_dir}: {e}")
        return items

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
