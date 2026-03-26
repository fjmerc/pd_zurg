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

# Patterns for _clean_title
_SEASON_TEXT_PATTERN = re.compile(
    r'[\s.\-_]+Seasons?[\s.\-_]+\d+(?:[\s.\-_]*[-\u2013][\s.\-_]*\d+|[\s.\-_]+(?:to|and|&)[\s.\-_]+\d+)?'
    r'|[\s.\-_]+S\d{1,2}[\s.\-_]*[-\u2013][\s.\-_]*S\d{1,2}',
    re.IGNORECASE,
)
_MID_YEAR_PATTERN = re.compile(r'\s*\((\d{4})\)')
_CONTAINER_SUFFIX_PATTERN = re.compile(r'\s+(?:Mp4|MKV|AVI)\s*$', re.IGNORECASE)
_EXTRAS_PATTERN = re.compile(r'\s*\+\s*\w+.*$')
_TRAILING_YEAR_PATTERN = re.compile(r'\s+(\d{4})\s*$')
_COMPLETE_SUFFIX_PATTERN = re.compile(r'\s+Complete\s*$', re.IGNORECASE)


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

    # Strip container suffixes: "Mp4", "MKV", "AVI"
    title = _CONTAINER_SUFFIX_PATTERN.sub('', title).strip()

    # Strip "Complete" suffix
    title = _COMPLETE_SUFFIX_PATTERN.sub('', title).strip()

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

    return title, year


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
                                episodes[key] = {'file': f.name, 'path': f.path}
                    except (PermissionError, OSError):
                        pass
                elif entry.is_file(follow_symlinks=False):
                    ext = os.path.splitext(entry.name)[1].lower()
                    if ext in MEDIA_EXTENSIONS:
                        ep_match = _EPISODE_ID_PATTERN.search(entry.name)
                        if ep_match:
                            key = (int(ep_match.group(1)), int(ep_match.group(2)))
                            episodes[key] = {'file': entry.name, 'path': entry.path}
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
        by_season[season_num].append({
            'number': ep_num,
            'file': info['file'],
            'source': info.get('source', default_source),
        })

    result = []
    for snum in sorted(by_season.keys()):
        eps = sorted(by_season[snum], key=lambda e: e['number'])
        result.append({
            'number': snum,
            'episode_count': len(eps),
            'episodes': eps,
        })
    return result


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


# Public aliases for cross-module reuse (e.g., debrid_client title matching)
parse_folder_name = _parse_folder_name
normalize_title = _normalize_title


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
        self._path_index = {}
        self._local_path_index = {}
        self._path_lock = threading.Lock()

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

        local_movie_keys = {_normalize_title(lm['title']) for lm in local_movies}

        movies = []
        # Merge debrid + local movies (title-level, unchanged)
        for item in debrid_movies:
            key = _normalize_title(item['title'])
            if key in local_movie_keys:
                item = dict(item)
                item['source'] = 'both'
            movies.append(item)

        for lm in local_movies:
            key = _normalize_title(lm['title'])
            if key not in debrid_movie_keys:
                movies.append(lm)

        # Merge debrid + local shows with episode-level cross-referencing
        local_show_map = {_normalize_title(ls['title']): ls for ls in local_shows}

        shows = []
        for item in debrid_shows:
            key = _normalize_title(item['title'])
            if key in local_show_map:
                item = dict(item)
                local_item = local_show_map[key]
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
                item['seasons'] = len({s for s, _ in merged})
                item['episodes'] = len(merged)
            shows.append(item)

        for ls in local_shows:
            key = _normalize_title(ls['title'])
            if key not in debrid_show_keys:
                shows.append(ls)

        # Build path indexes and season_data, then strip internal _episodes
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
                    path_index[(norm, sn, en)] = p
                if src == 'local' and p:
                    local_path_index[(norm, sn, en)] = p
                if lp:
                    local_path_index[(norm, sn, en)] = lp

        with self._path_lock:
            self._path_index = path_index
            self._local_path_index = local_path_index

        for show in shows:
            eps = show.pop('_episodes', {})
            show['season_data'] = _build_season_data(eps, show.get('source', 'debrid'))

        from utils.library_prefs import get_all_preferences

        preferences = get_all_preferences()
        self._enforce_preferences(shows, movies, preferences, path_index, local_path_index)
        self._clear_resolved_pending(shows, movies)
        self._create_debrid_symlinks(shows, movies, path_index)

        elapsed_ms = int((time.monotonic() - start) * 1000)
        return {
            'movies': movies,
            'shows': shows,
            'preferences': preferences,
            'last_scan': datetime.now(timezone.utc).isoformat(timespec='seconds'),
            'scan_duration_ms': elapsed_ms,
        }

    def get_data(self):
        with self._lock:
            now = time.monotonic()
            ttl = self._ttl if self._mount_path else 10
            if self._cache is not None and (now - self._cache_time) < ttl:
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
                    # If mount not found, expire cache in ~10s so we retry quickly
                    if not self._mount_path:
                        self._cache_time = time.monotonic() - self._ttl + 10
                    else:
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

    def get_episode_path(self, normalized_title, season, episode):
        """Get debrid mount path for an episode."""
        with self._path_lock:
            return self._path_index.get((normalized_title, season, episode))

    def get_local_episode_path(self, normalized_title, season, episode):
        """Get local library path for an episode."""
        with self._path_lock:
            return self._local_path_index.get((normalized_title, season, episode))

    def _enforce_preferences(self, shows, movies, preferences, path_index, local_path_index):
        """Auto-enforce source preferences after a scan.

        For prefer-debrid: if an episode has source=both (debrid copy arrived),
        replace the local file with a symlink to the debrid mount.

        For prefer-local: if an episode has source=both (local copy arrived),
        delete the debrid torrent via provider API.

        Only runs if LIBRARY_PREFERENCE_AUTO_ENFORCE is true (default).
        """
        auto_enforce = os.environ.get('LIBRARY_PREFERENCE_AUTO_ENFORCE', 'false').lower() == 'true'
        if not auto_enforce:
            return

        rclone_mount = os.environ.get('BLACKHOLE_RCLONE_MOUNT', '').strip()
        symlink_base = os.environ.get('BLACKHOLE_SYMLINK_TARGET_BASE', '').strip()

        if not preferences:
            return

        from utils.library_prefs import replace_local_with_symlinks, clear_pending

        # Track titles processed this scan to avoid redundant operations
        enforced_this_scan = set()

        # Enforce prefer-debrid: replace local files with symlinks for source=both episodes
        if rclone_mount and symlink_base and self._local_tv_path:
            for show in shows:
                norm = _normalize_title(show['title'])
                pref = preferences.get(norm)
                if pref != 'prefer-debrid':
                    continue

                to_switch = []
                for sd in show.get('season_data', []):
                    for ep in sd.get('episodes', []):
                        if ep.get('source') != 'both':
                            continue
                        sn, en = sd['number'], ep['number']
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

        # Enforce prefer-local: delete debrid torrents ONLY when ALL debrid
        # episodes have local copies (source=both for every debrid episode).
        # This prevents deleting seasons/episodes that have no local backup.
        prefer_local_safe = {}
        for show in shows:
            norm = _normalize_title(show['title'])
            if preferences.get(norm) != 'prefer-local':
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
            if preferences.get(norm) == 'prefer-local' and movie.get('source') == 'both':
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
                        matches = client.find_torrents_by_title(norm, target_year=year)
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

    def _clear_resolved_pending(self, shows, movies):
        """Clear pending entries for episodes whose source now matches the goal.

        If pending direction is 'to-debrid' and the episode source is now
        'debrid' or 'both', the pending is resolved. Same for 'to-local'
        when source is 'local' or 'both'. Runs unconditionally on every scan.
        """
        from utils.library_prefs import get_all_pending, clear_pending

        pending = get_all_pending()
        if not pending:
            return

        # Build a source lookup: {norm_title: {(season, episode): source}}
        source_map = {}
        for show in shows:
            norm = _normalize_title(show['title'])
            ep_sources = {}
            for sd in show.get('season_data', []):
                for ep in sd.get('episodes', []):
                    ep_sources[(sd['number'], ep['number'])] = ep.get('source', '')
            source_map[norm] = ep_sources

        for movie in movies:
            norm = _normalize_title(movie['title'])
            source_map[norm] = {(0, 0): movie.get('source', '')}

        for norm_title, entry in pending.items():
            direction = entry.get('direction', '')
            episodes = entry.get('episodes', [])
            sources = source_map.get(norm_title, {})
            resolved = []
            for ep in episodes:
                key = (ep.get('season', 0), ep.get('episode', 0))
                src = sources.get(key, '')
                if direction == 'to-debrid' and src in ('debrid', 'both'):
                    resolved.append(ep)
                elif direction == 'to-local' and src in ('local', 'both'):
                    resolved.append(ep)
            if resolved:
                clear_pending(norm_title, resolved)

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

        real_mount = os.path.realpath(rclone_mount)
        created = 0
        symlinked_shows = set()   # titles that got new symlinks
        symlinked_movies = set()  # titles that got new symlinks

        # Fetch arr libraries for canonical folder names and rescan IDs
        sonarr_map = {}  # lowercase title -> {'folder': str, 'id': int, 'client': obj}
        radarr_map = {}  # lowercase title -> {'folder': str, 'id': int, 'client': obj}
        try:
            from utils.arr_client import get_download_service
            client, svc = get_download_service('show')
            if client and svc == 'sonarr':
                for s in (client.get_all_series() or []):
                    p = s.get('path', '')
                    sonarr_map[s.get('title', '').lower()] = {
                        'folder': os.path.basename(p) if p else '',
                        'id': s.get('id'),
                        'client': client,
                    }
            client, svc = get_download_service('movie')
            if client and svc == 'radarr':
                for m in (client.get_all_movies() or []):
                    p = m.get('path', '')
                    radarr_map[m.get('title', '').lower()] = {
                        'folder': os.path.basename(p) if p else '',
                        'id': m.get('id'),
                        'client': client,
                    }
        except Exception as e:
            logger.debug(f"[library] Could not fetch arr libraries for folder naming: {e}")

        # --- Movies ---
        if self._local_movies_path:
            real_movies_root = os.path.realpath(self._local_movies_path)
            for movie in movies:
                if movie.get('source') != 'debrid':
                    continue
                mount_dir = movie.get('path')
                if not mount_dir:
                    continue

                title = movie['title']
                year = movie.get('year')
                arr_info = radarr_map.get(title.lower())
                if arr_info and arr_info['folder']:
                    movie_dir = arr_info['folder']
                else:
                    movie_dir = f"{title} ({year})" if year else title

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
                except FileExistsError:
                    pass
                except OSError as e:
                    logger.warning(
                        "[library] Failed to create movie symlink for %r: %s",
                        title, e
                    )

        # --- TV Shows ---
        if not self._local_tv_path:
            if created:
                logger.info(f"[library] Created {created} debrid symlink(s) in local library")
            return

        real_tv_root = os.path.realpath(self._local_tv_path)

        for show in shows:
            norm = _normalize_title(show['title'])
            title = show['title']
            year = show.get('year')
            arr_info = sonarr_map.get(title.lower())
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
                    except FileExistsError:
                        pass
                    except OSError as e:
                        logger.warning(
                            "[library] Failed to create symlink for %r S%02dE%02d: %s",
                            title, snum, enum, e
                        )

        if created:
            logger.info(f"[library] Created {created} debrid symlink(s) in local library")
            # Trigger arr rescans using already-fetched library data
            for title in symlinked_shows:
                info = sonarr_map.get(title.lower())
                if info and info.get('id') and info.get('client'):
                    try:
                        info['client'].rescan_series(info['id'])
                        logger.debug(f"[library] Triggered Sonarr rescan for {title}")
                    except Exception as e:
                        logger.debug(f"[library] Sonarr rescan failed for {title}: {e}")
            for title in symlinked_movies:
                info = radarr_map.get(title.lower())
                if info and info.get('id') and info.get('client'):
                    try:
                        info['client'].rescan_movie(info['id'])
                        logger.debug(f"[library] Triggered Radarr rescan for {title}")
                    except Exception as e:
                        logger.debug(f"[library] Radarr rescan failed for {title}: {e}")

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
                        title, year = _parse_folder_name(entry.name)
                        episodes = _collect_episodes(entry.path)
                        is_show = len(episodes) > 0 or category_is_shows

                        if is_show:
                            key = _normalize_title(title)
                            if key not in show_groups:
                                show_groups[key] = {
                                    'title': title,
                                    'year': year,
                                    'episodes': dict(episodes),
                                    'path': entry.path,
                                }
                            else:
                                show_groups[key]['episodes'].update(episodes)
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
            })

        return movies, shows

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
                    # Skip folders that only contain debrid symlinks
                    if symlink_base and self._is_debrid_symlink_dir(entry.path, symlink_base):
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
                    # Skip show folders that are entirely debrid symlinks
                    if symlink_base and self._is_debrid_symlink_only(entry.path, symlink_base):
                        continue
                    title, year = _parse_folder_name(entry.name)
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
                        })
                    else:
                        # Fallback for shows without parseable episode patterns
                        seasons, ep_count = _count_show_content(entry.path)
                        items.append({
                            'title': title,
                            'year': year,
                            'source': 'local',
                            'type': 'show',
                            'seasons': seasons,
                            'episodes': ep_count,
                            '_episodes': {},
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
