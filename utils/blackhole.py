"""Blackhole watch folder for .torrent and .magnet files.

Monitors a directory for torrent/magnet files, submits them to the
configured debrid service, and removes the file after processing.
Compatible with Sonarr/Radarr blackhole download client configuration.

When symlink mode is enabled, monitors submitted torrents until content
appears on the rclone mount, then creates symlinks in a completed
directory for Sonarr/Radarr to import.
"""

import hashlib
import json
import os
import re
import shutil
import time
import threading
import requests
from utils.file_utils import atomic_write
from utils.logger import get_logger

logger = get_logger()

try:
    from utils.notifications import notify as _notify
except ImportError:
    _notify = None

try:
    from utils import history as _history
except ImportError:
    _history = None

try:
    from utils import blocklist as _blocklist
except ImportError:
    _blocklist = None

from utils.api_metrics import tracked_request

_watcher = None

# Retry configuration for failed torrent submissions
RETRY_SCHEDULE = [300, 900, 3600]  # 5 min, 15 min, 1 hour
MAX_RETRIES = 3

# Media file extensions for symlink creation
MEDIA_EXTENSIONS = {'.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.ts', '.m4v', '.webm'}

# Zurg mount category directories (checked in order; __all__ is fallback)
MOUNT_CATEGORIES = ['shows', 'movies', 'anime']

# Label routing: subdir names in watch_dir that are NOT labels
# (retry staging and alt-retry staging — handled by dedicated logic)
_RESERVED_LABELS = {'failed', '.alt_pending'}

# Label validation: alphanumeric plus hyphen/underscore, max 64 chars
_LABEL_MAX_LEN = 64
_LABEL_RE = re.compile(r'^[A-Za-z0-9_-]+$')


def _is_valid_label(name):
    """Return True if *name* is a valid per-arr routing label.

    Labels must be alphanumeric plus hyphen/underscore, max 64 chars,
    and cannot match any reserved name (case-insensitive).
    """
    if not name or not isinstance(name, str):
        return False
    if len(name) > _LABEL_MAX_LEN:
        return False
    if name.lower() in _RESERVED_LABELS:
        return False
    if not _LABEL_RE.match(name):
        return False
    return True


def iter_release_dirs(completed_dir):
    """Yield ``(label, release_name, release_path)`` for each release dir under *completed_dir*.

    Handles three layouts:
      - Flat: ``completed_dir/<release_name>/`` (contains files) → label=None
      - Labeled: ``completed_dir/<label>/<release_name>/`` → label=<label>
      - Mixed: both coexist (users mid-migration)

    Heuristic for distinguishing a label parent dir from a flat-mode release
    dir:
      1. The name must match the label whitelist (``_is_valid_label``).
      2. The dir is EITHER empty (a label subdir awaiting its first release)
         OR contains only subdirectories (no loose files, which would imply
         a release dir that happens to have a label-compatible name).
    Anything else is treated as a flat-mode release dir with label=None.

    Known caveat: a flat-mode release dir whose name matches the label
    whitelist and whose contents are exclusively subdirectories (e.g. a
    `Season 01/` subdir containing files) is misclassified as a label
    parent. In practice release names almost always include dots/spaces
    or bracket tags, so the whitelist rejects them. If a user runs into
    this, rename the release dir or switch to the `strict`/`off` modes
    (planned follow-up).

    Consumers of ``BLACKHOLE_COMPLETED_DIR`` (cleanup, empty-dir sweep,
    symlink verification, title removal) should use this helper instead
    of ``os.listdir(completed_dir)``.
    """
    if not completed_dir or not os.path.isdir(completed_dir):
        return

    try:
        top_entries = os.listdir(completed_dir)
    except OSError:
        return

    for entry in top_entries:
        entry_path = os.path.join(completed_dir, entry)
        if not os.path.isdir(entry_path):
            continue

        # Decide whether this is a label dir or a flat-mode release dir.
        # A label dir has a valid label name AND either:
        #   - is empty (the user just created /completed/sonarr/), OR
        #   - contains at least one subdirectory (a release).
        # Stray loose files inside a label dir (e.g. .DS_Store, Thumbs.db,
        # arr lockfiles) are ignored rather than demoting the whole dir to
        # flat-mode — demotion would cause _cleanup_symlinks to wipe the
        # entire label tree when it aged out.
        is_label = False
        if _is_valid_label(entry):
            try:
                children = list(os.scandir(entry_path))
            except OSError:
                children = []
            has_subdir = any(c.is_dir(follow_symlinks=False) for c in children)
            if has_subdir or not children:
                is_label = True

        if is_label:
            try:
                for sub in os.listdir(entry_path):
                    sub_path = os.path.join(entry_path, sub)
                    if os.path.isdir(sub_path):
                        yield (entry, sub, sub_path)
            except OSError:
                continue
        else:
            yield (None, entry, entry_path)

# Terminal debrid statuses that mean the torrent will never complete
RD_TERMINAL_ERRORS = {'magnet_error', 'error', 'virus', 'dead'}
AD_TERMINAL_ERRORS = {'Error'}
TB_TERMINAL_ERRORS = {'error', 'failed'}


def _bencode_end(data, start):
    """Find the end offset of a bencoded value starting at `start`.

    Supports dicts (d...e), lists (l...e), integers (iNe), and byte strings (N:...).
    Returns the offset ONE PAST the last byte, or None on parse error.
    """
    if start >= len(data):
        return None
    ch = data[start:start + 1]
    if ch == b'd' or ch == b'l':
        pos = start + 1
        while pos < len(data) and data[pos:pos + 1] != b'e':
            pos = _bencode_end(data, pos)
            if pos is None:
                return None
            # Dicts have key-value pairs; after key we need the value
            if ch == b'd':
                pos = _bencode_end(data, pos)
                if pos is None:
                    return None
        return pos + 1 if pos < len(data) else None
    elif ch == b'i':
        end = data.find(b'e', start + 1)
        return end + 1 if end != -1 else None
    elif ch and ch[0:1].isdigit():
        colon = data.find(b':', start)
        if colon == -1:
            return None
        try:
            length = int(data[start:colon])
        except ValueError:
            return None
        return colon + 1 + length
    return None


def _parse_episodes(filename):
    """Extract episode numbers from a release filename.

    Returns a set of episode ints, or empty set for season packs.
    Handles S01E04, S01E04E05, S01E04-E06, etc.
    """
    name = re.sub(r'\.(torrent|magnet)$', '', filename, flags=re.IGNORECASE)
    # Match S01E04, S01E04E05, S01E04-E06, etc.
    m = re.search(r'S\d+(E\d+(?:[E\-]E?\d+)*)', name, re.IGNORECASE)
    if not m:
        return set()
    ep_str = m.group(1)
    nums = [int(x) for x in re.findall(r'\d+', ep_str)]
    if len(nums) == 2 and '-' in ep_str:
        lo, hi = nums
        if lo <= hi and (hi - lo) < 100:
            return set(range(lo, hi + 1))
        return {lo, hi}
    return set(nums)


def _enrich_for_history(filename):
    """Extract media_title and episode string from a torrent filename for history logging."""
    name, season, is_tv = parse_release_name(filename)
    eps = _parse_episodes(filename)
    ep_str = None
    if is_tv and season is not None and eps:
        ep_str = f"S{season:02d}" + "".join(f"E{e:02d}" for e in sorted(eps))
    elif is_tv and season is not None:
        ep_str = f"S{season:02d}"
    return name or None, ep_str


def _local_episodes(season_dir):
    """Extract episode numbers from files in a local season directory."""
    eps = set()
    try:
        for f in os.listdir(season_dir):
            for m in re.finditer(r'(?<![a-zA-Z])[Ee](\d+)', f):
                eps.add(int(m.group(1)))
    except OSError:
        pass
    return eps


def parse_release_name(filename):
    """Extract show/movie name and season from a release filename.

    Returns (name, season_number_or_None, is_tv).
    """
    # Remove file extension
    name = re.sub(r'\.(torrent|magnet)$', '', filename, flags=re.IGNORECASE)

    # Try to find season pattern (S01E01, S01, Season 1)
    season_match = re.search(
        r'[.\s]S(\d{1,2})[E.\s]|[.\s]S(\d{1,2})[.\s]|[.\s]S(\d{1,2})$|Season[.\s](\d{1,2})',
        name, re.IGNORECASE,
    )

    if season_match:
        season = int(next(g for g in season_match.groups() if g is not None))
        # Everything before the season marker is the show name
        show_name = name[:season_match.start()]
        show_name = re.sub(r'[.\-_]', ' ', show_name).strip()
        show_name = re.sub(r'\s*\(?\d{4}\)?\s*$', '', show_name).strip()
        return show_name, season, True

    # No season pattern — likely a movie
    year_match = re.search(r'[.\s](\d{4})[.\s]', name)
    if year_match:
        movie_name = name[:year_match.start()]
    else:
        quality_match = re.search(
            r'[.\s](1080p|720p|2160p|4K|WEB|BluRay|BDRip|HDTV|REMUX)',
            name, re.IGNORECASE,
        )
        movie_name = name[:quality_match.start()] if quality_match else name

    movie_name = re.sub(r'[.\-_]', ' ', movie_name).strip()
    return movie_name, None, False


def _is_multi_season_pack(release_name):
    """Detect if a release name indicates a multi-season pack.

    Returns (is_multi, season_start, season_end).
    For 'Complete Series/Collection' patterns returns (True, None, None)
    since the range isn't known from the name alone.
    """
    # 1. S01E01-S05E10 (cross-season episode range)
    m = re.search(r'S(\d{1,2})E\d+\s*[-–]\s*S(\d{1,2})E\d+', release_name, re.IGNORECASE)
    if m:
        s1, s2 = int(m.group(1)), int(m.group(2))
        if s1 != s2:
            return True, min(s1, s2), max(s1, s2)

    # 2. S01-S05 (both prefixed with S)
    m = re.search(r'S(\d{1,2})\s*[-–]\s*S(\d{1,2})', release_name, re.IGNORECASE)
    if m:
        s1, s2 = int(m.group(1)), int(m.group(2))
        if s1 != s2:
            return True, min(s1, s2), max(s1, s2)

    # 3. S01-05 (first prefixed, second bare number)
    # S\d{1,2} immediately followed by dash then digits — no E between S## and dash.
    # (?![a-zA-Z\d]) prevents matching encoding markers like S05-10bit or S02-3D.
    m = re.search(r'S(\d{1,2})[-–](\d{1,2})(?![a-zA-Z\d])', release_name, re.IGNORECASE)
    if m:
        s1, s2 = int(m.group(1)), int(m.group(2))
        if s1 != s2:
            return True, min(s1, s2), max(s1, s2)

    # 4. Season(s) 1-5 / Seasons 1 & 2 / Seasons 1 and 2
    m = re.search(r'Seasons?[.\s]*(\d{1,2})[.\s]*(?:[-–&+]|and)[.\s]*(\d{1,2})', release_name, re.IGNORECASE)
    if m:
        s1, s2 = int(m.group(1)), int(m.group(2))
        if s1 != s2:
            return True, min(s1, s2), max(s1, s2)

    # 5. Series 1-3
    m = re.search(r'Series[.\s]*(\d{1,2})[.\s]*[-–][.\s]*(\d{1,2})', release_name, re.IGNORECASE)
    if m:
        s1, s2 = int(m.group(1)), int(m.group(2))
        if s1 != s2:
            return True, min(s1, s2), max(s1, s2)

    # 6. Complete Series / Complete Collection
    if re.search(r'Complete[.\s](?:Series|Collection)', release_name, re.IGNORECASE):
        return True, None, None

    return False, None, None


def _extract_file_season(filepath):
    """Extract season number from a media file path within a release.

    filepath is relative to the release root, e.g. 'Season 02/Show.S02E05.mkv'.
    Returns season number as int, or None if unparseable.
    """
    parts = filepath.replace('\\', '/').split('/')
    filename = parts[-1]

    # Check filename for SxxExx pattern (most reliable)
    m = re.search(r'[Ss](\d{1,2})[Ee]\d+', filename)
    if m:
        return int(m.group(1))

    # Fallback: Sxx without Exx (e.g., S03.Special.mkv) — must not re-match SxxExx
    m = re.search(r'[Ss](\d{1,2})(?=[.\s\-_]|$)(?![Ee]\d)', filename)
    if m:
        return int(m.group(1))

    # Check parent directories for season indicators
    for part in parts[:-1]:
        m = re.search(r'[Ss]eason[.\s]*(\d{1,2})', part, re.IGNORECASE)
        if m:
            return int(m.group(1))
        m = re.match(r'^S(\d{1,2})$', part, re.IGNORECASE)
        if m:
            return int(m.group(1))

    return None


def _build_season_release_name(original_name, season_num):
    """Construct a per-season release name from a multi-season pack name.

    Replaces the multi-season indicator with a single-season S{XX} pattern.
    Example: 'Breaking.Bad.S01-S05.1080p.BluRay-GROUP'
           → 'Breaking.Bad.S03.1080p.BluRay-GROUP'
    """
    sxx = f'S{season_num:02d}'

    # Try each multi-season pattern and replace with single season
    patterns = [
        r'S\d{1,2}E\d+\s*[-–]\s*S\d{1,2}E\d+',       # S01E01-S05E10
        r'S\d{1,2}\s*[-–]\s*S\d{1,2}',                  # S01-S05
        r'S\d{1,2}[-–]\d{1,2}',                          # S01-05
        r'Seasons?[.\s]*\d{1,2}[.\s]*(?:[-–&+]|and)[.\s]*\d{1,2}',  # Seasons 1-5
        r'Series[.\s]*\d{1,2}[.\s]*[-–][.\s]*\d{1,2}',  # Series 1-3
        r'Complete[.\s](?:Series|Collection)',             # Complete Series
    ]
    for pattern in patterns:
        result = re.sub(pattern, sxx, original_name, count=1, flags=re.IGNORECASE)
        if result != original_name:
            # Clean up double dots from replacement
            result = re.sub(r'\.{2,}', '.', result)
            return result.strip('.')

    # Fallback: append season
    result = f'{original_name}.{sxx}'
    result = re.sub(r'\.{2,}', '.', result)
    return result.strip('.')


class RetryMeta:
    """Tracks retry state for failed blackhole files via JSON sidecar files.

    State survives container restarts since it's persisted to disk.
    """

    @staticmethod
    def meta_path(file_path):
        return file_path + '.meta'

    @staticmethod
    def read(file_path):
        """Read retry count and last attempt time. Returns (retries, last_attempt)."""
        meta = RetryMeta.meta_path(file_path)
        if os.path.exists(meta):
            try:
                with open(meta, 'r') as f:
                    data = json.load(f)
                return data.get('retries', 0), data.get('last_attempt', 0)
            except (json.JSONDecodeError, IOError):
                return 0, 0
        return 0, 0

    @staticmethod
    def write(file_path, retries):
        """Write retry count and current timestamp."""
        meta = RetryMeta.meta_path(file_path)
        try:
            with open(meta, 'w') as f:
                json.dump({'retries': retries, 'last_attempt': time.time()}, f)
        except IOError as e:
            logger.debug(f"[blackhole] Could not write retry meta for {file_path}: {e}")

    @staticmethod
    def remove(file_path):
        """Clean up sidecar meta file."""
        meta = RetryMeta.meta_path(file_path)
        try:
            if os.path.exists(meta):
                os.remove(meta)
        except OSError:
            pass


class BlackholeWatcher:
    SUPPORTED_EXTENSIONS = {'.torrent', '.magnet'}

    def __init__(self, watch_dir, debrid_api_key, debrid_service='realdebrid',
                 poll_interval=5, symlink_enabled=False, completed_dir='/completed',
                 rclone_mount='/data', symlink_target_base='', mount_poll_timeout=300,
                 mount_poll_interval=10, symlink_max_age=72,
                 dedup_enabled=False, local_library_tv='', local_library_movies=''):
        self.watch_dir = watch_dir
        self.debrid_api_key = debrid_api_key
        self.debrid_service = debrid_service
        self.poll_interval = poll_interval
        self._stop_event = threading.Event()

        # Local library dedup configuration
        self.dedup_enabled = dedup_enabled
        self.local_library_tv = local_library_tv
        self.local_library_movies = local_library_movies

        # Symlink configuration
        self.symlink_enabled = symlink_enabled
        self.completed_dir = completed_dir
        self.rclone_mount = rclone_mount
        self.symlink_target_base = symlink_target_base
        self.mount_poll_timeout = mount_poll_timeout
        self.mount_poll_interval = mount_poll_interval
        self.symlink_max_age = symlink_max_age

        # Active monitor tracking (prevents duplicate monitors)
        self._active_monitors = set()
        self._monitors_lock = threading.RLock()
        if symlink_enabled:
            self._pending_file = os.path.join(completed_dir, 'pending_monitors.json')
        else:
            self._pending_file = os.path.join(watch_dir, 'pending_monitors.json')
        self._last_cleanup = 0

    # ── Debrid submission methods ────────────────────────────────────

    def _add_to_realdebrid(self, file_path):
        """Add a torrent/magnet to Real-Debrid."""
        ext = os.path.splitext(file_path)[1].lower()
        headers = {'Authorization': f'Bearer {self.debrid_api_key}'}

        if ext == '.magnet':
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                magnet_link = f.read().strip()
            url = 'https://api.real-debrid.com/rest/1.0/torrents/addMagnet'
            response = tracked_request('realdebrid', requests.post, url, headers=headers, data={'magnet': magnet_link}, timeout=30)
        elif ext == '.torrent':
            url = 'https://api.real-debrid.com/rest/1.0/torrents/addTorrent'
            with open(file_path, 'rb') as f:
                response = tracked_request('realdebrid', requests.put, url,
                                           headers={**headers, 'Content-Type': 'application/x-bittorrent'},
                                           data=f.read(), timeout=30)
        else:
            return False, f'Unsupported extension: {ext}'

        if response.status_code in (200, 201):
            torrent_id = response.json().get('id')
            if not torrent_id:
                return False, 'Real-Debrid response missing torrent id'
            select_url = f'https://api.real-debrid.com/rest/1.0/torrents/selectFiles/{torrent_id}'
            select_resp = tracked_request('realdebrid', requests.post, select_url, headers=headers, data={'files': 'all'}, timeout=30)
            if select_resp.status_code not in (200, 202, 204):
                logger.warning(f"[blackhole] selectFiles failed for {torrent_id}: HTTP {select_resp.status_code}")
            return True, torrent_id
        else:
            return False, response.text[:200]

    def _add_to_alldebrid(self, file_path):
        """Add a torrent/magnet to AllDebrid."""
        ext = os.path.splitext(file_path)[1].lower()
        params = {'agent': 'pd_zurg', 'apikey': self.debrid_api_key}

        if ext == '.magnet':
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                magnet_link = f.read().strip()
            url = 'https://api.alldebrid.com/v4/magnet/upload'
            response = tracked_request('alldebrid', requests.post, url, params=params, data={'magnets[]': magnet_link}, timeout=30)
        elif ext == '.torrent':
            url = 'https://api.alldebrid.com/v4/magnet/upload/file'
            with open(file_path, 'rb') as f:
                response = tracked_request('alldebrid', requests.post, url, params=params, files={'files[]': f}, timeout=30)
        else:
            return False, f'Unsupported extension: {ext}'

        if response.status_code == 200:
            return True, response.json()
        else:
            return False, response.text[:200]

    def _add_to_torbox(self, file_path):
        """Add a torrent/magnet to TorBox."""
        ext = os.path.splitext(file_path)[1].lower()
        headers = {'Authorization': f'Bearer {self.debrid_api_key}'}
        url = 'https://api.torbox.app/v1/api/torrents/createtorrent'

        if ext == '.magnet':
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                magnet_link = f.read().strip()
            response = tracked_request('torbox', requests.post, url, headers=headers, data={'magnet': magnet_link}, timeout=30)
        elif ext == '.torrent':
            with open(file_path, 'rb') as f:
                response = tracked_request('torbox', requests.post, url, headers=headers, files={'file': f}, timeout=30)
        else:
            return False, f'Unsupported extension: {ext}'

        if response.status_code in (200, 201):
            return True, response.json()
        else:
            return False, response.text[:200]

    # ── Torrent ID extraction ────────────────────────────────────────

    def _extract_torrent_id(self, result):
        """Extract a normalized torrent ID string from the debrid submission result."""
        try:
            if self.debrid_service == 'realdebrid':
                return str(result)
            elif self.debrid_service == 'alldebrid':
                return str(result['data']['magnets'][0]['id'])
            elif self.debrid_service == 'torbox':
                data = result.get('data', {})
                return str(data.get('torrent_id') or data.get('id', ''))
        except (KeyError, IndexError, TypeError) as e:
            logger.warning(f"[blackhole] Could not extract torrent ID from {self.debrid_service} response: {e}")
        return None

    # ── Debrid status check methods ──────────────────────────────────

    def _check_realdebrid_status(self, torrent_id):
        """Check torrent status on Real-Debrid. Returns (status, info_dict)."""
        headers = {'Authorization': f'Bearer {self.debrid_api_key}'}
        url = f'https://api.real-debrid.com/rest/1.0/torrents/info/{torrent_id}'
        response = tracked_request('realdebrid', requests.get, url, headers=headers, timeout=30)
        if response.status_code == 200:
            info = response.json()
            return info.get('status', 'unknown'), info
        if response.status_code == 404:
            logger.warning(f"[blackhole] RD torrent {torrent_id} no longer exists (404)")
            return 'dead', {}  # Treat as terminal so monitor stops immediately
        logger.warning(f"[blackhole] RD status check failed for {torrent_id}: HTTP {response.status_code}")
        return 'api_error', {}

    def _check_alldebrid_status(self, torrent_id):
        """Check torrent status on AllDebrid. Returns (status, info_dict)."""
        params = {'agent': 'pd_zurg', 'apikey': self.debrid_api_key, 'id': torrent_id}
        url = 'https://api.alldebrid.com/v4/magnet/status'
        response = tracked_request('alldebrid', requests.get, url, params=params, timeout=30)
        if response.status_code == 200:
            info = response.json()
            if info.get('status') != 'success':
                logger.warning(f"[blackhole] AD API error for {torrent_id}: {info.get('status')}")
                return 'api_error', info
            try:
                magnet = info['data']['magnets']
                if not isinstance(magnet, dict):
                    return 'unknown', info
                return magnet.get('status', 'unknown'), info
            except (KeyError, TypeError):
                return 'unknown', info
        logger.warning(f"[blackhole] AD status check failed for {torrent_id}: HTTP {response.status_code}")
        return 'api_error', {}

    def _check_torbox_status(self, torrent_id):
        """Check torrent status on TorBox. Returns (status, info_dict)."""
        headers = {'Authorization': f'Bearer {self.debrid_api_key}'}
        url = 'https://api.torbox.app/v1/api/torrents/mylist'
        params = {'id': torrent_id}
        response = tracked_request('torbox', requests.get, url, headers=headers, params=params, timeout=30)
        if response.status_code == 200:
            info = response.json()
            data = info.get('data')
            if not isinstance(data, dict):
                return 'unknown', info
            return data.get('download_state', 'unknown'), info
        logger.warning(f"[blackhole] TorBox status check failed for {torrent_id}: HTTP {response.status_code}")
        return 'api_error', {}

    def _is_torrent_ready(self, status):
        """Check if the debrid status indicates the torrent is fully downloaded."""
        if self.debrid_service == 'realdebrid':
            return status == 'downloaded'
        elif self.debrid_service == 'alldebrid':
            return status == 'Ready'
        elif self.debrid_service == 'torbox':
            return status == 'completed'
        return False

    def _is_terminal_error(self, status):
        """Check if the debrid status indicates a terminal (unrecoverable) error."""
        if self.debrid_service == 'realdebrid':
            return status in RD_TERMINAL_ERRORS
        elif self.debrid_service == 'alldebrid':
            return status in AD_TERMINAL_ERRORS
        elif self.debrid_service == 'torbox':
            return status in TB_TERMINAL_ERRORS
        return False

    def _extract_release_name(self, info):
        """Extract the release/folder name from the debrid torrent info response."""
        try:
            if self.debrid_service == 'realdebrid':
                return info.get('filename', '')
            elif self.debrid_service == 'alldebrid':
                return info['data']['magnets'].get('filename', '')
            elif self.debrid_service == 'torbox':
                return info['data'].get('name', '')
        except (KeyError, TypeError):
            pass
        return ''

    def _extract_hash_from_info(self, info):
        """Extract the info hash from a debrid torrent info response."""
        try:
            if self.debrid_service == 'realdebrid':
                return (info.get('hash') or '').upper()
            elif self.debrid_service == 'alldebrid':
                return (info['data']['magnets'].get('hash') or '').upper()
            elif self.debrid_service == 'torbox':
                return (info['data'].get('hash') or '').upper()
        except (KeyError, TypeError):
            pass
        return ''

    def _has_usable_media_files(self, info):
        """Check if the debrid torrent contains any files with recognized media extensions.

        Returns True if at least one file matches MEDIA_EXTENSIONS.
        Returns True (assume usable) if file info is unavailable — never reject
        what we can't verify.
        """
        try:
            filenames = self._extract_filenames_from_info(info)
        except Exception:
            return True  # Can't verify — assume usable
        if not filenames:
            return True  # No file info available — assume usable
        return any(
            os.path.splitext(f)[1].lower() in MEDIA_EXTENSIONS
            for f in filenames
        )

    def _extract_filenames_from_info(self, info):
        """Extract flat list of filenames from a debrid torrent info response.

        Provider-specific extraction; returns empty list if structure is unexpected.
        """
        if self.debrid_service == 'realdebrid':
            files = info.get('files')
            if not isinstance(files, list):
                return []
            return [
                os.path.basename(f['path'])
                for f in files
                if f.get('selected') == 1 and f.get('path')
            ]
        elif self.debrid_service == 'alldebrid':
            try:
                files = info['data']['magnets']['files']
            except (KeyError, TypeError):
                return []
            if not isinstance(files, list):
                return []
            # AD uses nested structure: 'n' = name, 'e' = children
            result = []
            stack = list(files)
            while stack:
                node = stack.pop()
                if not isinstance(node, dict):
                    continue
                children = node.get('e')
                if isinstance(children, list):
                    stack.extend(children)
                elif node.get('n'):
                    result.append(node['n'])
            return result
        elif self.debrid_service == 'torbox':
            try:
                files = info['data']['files']
            except (KeyError, TypeError):
                return []
            if not isinstance(files, list):
                return []
            return [f['name'] for f in files if f.get('name')]
        return []

    # ── Mount scanning ───────────────────────────────────────────────

    def _find_on_mount(self, release_name):
        """Search the rclone mount for a release folder.

        Returns (full_path, category, matched_name) or (None, None, None) if not found.
        Checks categorized directories first, then __all__ as fallback.
        Also tries stripping video file extensions since Zurg strips them
        from single-file torrent folder names.
        """
        # Try both the original name and with video extension stripped
        candidates = [release_name]
        base, ext = os.path.splitext(release_name)
        if ext.lower() in MEDIA_EXTENSIONS and base:
            candidates.append(base)

        for name in candidates:
            for category in MOUNT_CATEGORIES:
                path = os.path.join(self.rclone_mount, category, name)
                if os.path.isdir(path):
                    return path, category, name
            # Fallback to __all__
            path = os.path.join(self.rclone_mount, '__all__', name)
            if os.path.isdir(path):
                return path, '__all__', name
        return None, None, None

    # ── Symlink creation ─────────────────────────────────────────────

    def _completed_base(self, label):
        """Return the base output directory, prefixed by *label* when set.

        With label="sonarr" → /completed/sonarr
        With label=None     → /completed  (flat-mode, backward compatible)
        """
        if label:
            return os.path.join(self.completed_dir, label)
        return self.completed_dir

    def _failed_dir(self, label):
        """Return the failed/ staging dir for *label* (or flat failed/ if None)."""
        base = os.path.join(self.watch_dir, 'failed')
        if label:
            return os.path.join(base, label)
        return base

    def _alt_pending_dir(self, label):
        """Return the .alt_pending/ staging dir for *label* (or flat if None)."""
        base = os.path.join(self.watch_dir, '.alt_pending')
        if label:
            return os.path.join(base, label)
        return base

    def _create_symlinks(self, release_name, category, mount_path, label=None):
        """Create symlinks in the completed directory for media files.

        Symlink targets use BLACKHOLE_SYMLINK_TARGET_BASE so they resolve
        correctly on the Sonarr/Radarr host.

        For multi-season packs, splits files into per-season directories
        with constructed release names that Sonarr can parse individually.

        When *label* is set, output is nested under ``completed_dir/<label>/``
        so each arr only sees its own items (see ``.plans/31-blackhole-per-arr-routing.md``).

        Returns the number of symlinks created.
        """
        is_multi, _, _ = _is_multi_season_pack(release_name)

        if is_multi:
            split_count = self._create_split_season_symlinks(release_name, category, mount_path, label=label)
            if split_count is not None:
                return split_count
            logger.debug(f"[blackhole] Could not split {release_name} by season, using single dir")

        # Single-dir logic (original behavior, now label-aware)
        completed_base = self._completed_base(label)
        completed_release_dir = os.path.join(completed_base, release_name)
        os.makedirs(completed_release_dir, exist_ok=True)
        count = 0

        for root, _dirs, files in os.walk(mount_path):
            for f in files:
                ext = os.path.splitext(f)[1].lower()
                if ext not in MEDIA_EXTENSIONS:
                    continue
                if 'sample' in f.lower():
                    continue

                rel = os.path.relpath(os.path.join(root, f), mount_path)
                symlink_path = os.path.normpath(os.path.join(completed_release_dir, rel))
                target = os.path.join(self.symlink_target_base, category, release_name, rel)

                # Guard against path traversal from adversarial release names
                if not symlink_path.startswith(completed_release_dir + os.sep):
                    logger.warning(f"[blackhole] Skipping path traversal attempt: {rel}")
                    continue

                os.makedirs(os.path.dirname(symlink_path), exist_ok=True)

                if os.path.islink(symlink_path) or os.path.exists(symlink_path):
                    logger.debug(f"[blackhole] Symlink already exists: {symlink_path}")
                    continue

                try:
                    os.symlink(target, symlink_path)
                    logger.info(f"[blackhole] Symlink: {rel} -> {target}")
                    count += 1
                except OSError as e:
                    logger.error(f"[blackhole] Failed to create symlink {symlink_path}: {e}")

        return count

    def _create_split_season_symlinks(self, release_name, category, mount_path, label=None):
        """Split a multi-season pack into per-season symlink directories.

        Groups media files by season, creates a separate completed directory
        for each season with a constructed release name, and returns the
        total number of symlinks created. Returns None if fewer than 2 seasons
        are detected (caller should fall back to single-dir).

        When *label* is set, season dirs are nested under ``completed_dir/<label>/``.
        """
        season_files = {}

        for root, _dirs, files in os.walk(mount_path):
            for f in files:
                ext = os.path.splitext(f)[1].lower()
                if ext not in MEDIA_EXTENSIONS:
                    continue
                if 'sample' in f.lower():
                    continue

                rel = os.path.relpath(os.path.join(root, f), mount_path)
                season = _extract_file_season(rel)
                if season is None:
                    logger.warning(f"[blackhole] Cannot determine season for '{f}' in multi-season pack {release_name}, skipping")
                    continue

                season_files.setdefault(season, []).append(rel)

        if len(season_files) < 2:
            return None

        count = 0
        completed_base = self._completed_base(label)
        completed_real = os.path.normpath(completed_base)
        logger.info(f"[blackhole] Multi-season pack: {release_name} → splitting into {len(season_files)} seasons")

        for season_num, rel_list in sorted(season_files.items()):
            season_name = _build_season_release_name(release_name, season_num)
            season_dir = os.path.normpath(os.path.join(completed_base, season_name))

            # Guard against path traversal in the constructed season dir name
            if not season_dir.startswith(completed_real + os.sep):
                logger.warning(f"[blackhole] Skipping path traversal in season name: {season_name}")
                continue

            os.makedirs(season_dir, exist_ok=True)

            for rel in rel_list:
                symlink_path = os.path.normpath(os.path.join(season_dir, rel))
                target = os.path.join(self.symlink_target_base, category, release_name, rel)

                if not symlink_path.startswith(season_dir + os.sep):
                    logger.warning(f"[blackhole] Skipping path traversal attempt: {rel}")
                    continue

                os.makedirs(os.path.dirname(symlink_path), exist_ok=True)

                if os.path.islink(symlink_path) or os.path.exists(symlink_path):
                    logger.debug(f"[blackhole] Symlink already exists: {symlink_path}")
                    continue

                try:
                    os.symlink(target, symlink_path)
                    logger.info(f"[blackhole] Symlink (S{season_num:02d}): {rel} -> {target}")
                    count += 1
                except OSError as e:
                    logger.error(f"[blackhole] Failed to create symlink {symlink_path}: {e}")

            logger.info(f"[blackhole]   Season {season_num:02d}: {len(rel_list)} file(s) → {season_name}")

        return count

    # ── Symlink cleanup ──────────────────────────────────────────────

    def _cleanup_symlinks(self):
        """Remove broken symlinks and aged-out directories from the completed dir.

        Handles both flat (``completed_dir/<release>``) and labeled
        (``completed_dir/<label>/<release>``) layouts via ``iter_release_dirs``.
        Empty label dirs left behind after all their releases are cleaned up
        are removed as well, but the top-level ``completed_dir`` itself is
        never removed.
        """
        if not self.symlink_enabled or not self.completed_dir:
            return
        if not os.path.exists(self.completed_dir):
            return

        now = time.time()
        max_age_secs = self.symlink_max_age * 3600

        # Pre-compute once — both are stable across the loop
        rclone_real = os.path.realpath(self.rclone_mount)
        target_base_real = ''
        if self.symlink_target_base:
            target_base_real = os.path.realpath(self.symlink_target_base) + '/'

        cleaned_label_parents = set()

        for label, release_name, entry_path in iter_release_dirs(self.completed_dir):
            # Remove broken symlinks within this release dir.
            # Symlinks point to SYMLINK_TARGET_BASE which only exists in
            # Sonarr/Radarr's container — translate to the rclone mount
            # before checking existence.
            has_valid = False
            for root, _dirs, files in os.walk(entry_path):
                for f in files:
                    fp = os.path.join(root, f)
                    if os.path.islink(fp):
                        target = os.readlink(fp)
                        if not os.path.isabs(target):
                            target = os.path.realpath(os.path.join(os.path.dirname(fp), target))
                        check_target = fp
                        if target_base_real and target.startswith(target_base_real):
                            check_target = rclone_real + '/' + target[len(target_base_real):]
                        if not os.path.exists(check_target):
                            try:
                                os.unlink(fp)
                                logger.debug(f"[blackhole] Removed broken symlink: {fp}")
                            except OSError:
                                pass
                        else:
                            has_valid = True

            # Remove dir if no valid files remain or if aged out
            try:
                mtime = os.path.getmtime(entry_path)
            except OSError:
                continue

            should_remove = not has_valid
            if max_age_secs > 0 and (now - mtime) > max_age_secs:
                should_remove = True

            if should_remove:
                try:
                    shutil.rmtree(entry_path, ignore_errors=True)
                    display = f"{label}/{release_name}" if label else release_name
                    logger.info(f"[blackhole] Cleaned up completed dir: {display}")
                    if label:
                        cleaned_label_parents.add(os.path.join(self.completed_dir, label))
                except Exception as e:
                    logger.debug(f"[blackhole] Failed to clean up {entry_path}: {e}")

        # Remove now-empty label dirs. The top-level completed_dir is never
        # in cleaned_label_parents by construction.
        for parent in cleaned_label_parents:
            try:
                if os.path.isdir(parent) and not os.listdir(parent):
                    os.rmdir(parent)
                    logger.debug(f"[blackhole] Removed empty label dir: {parent}")
            except OSError:
                pass

    # ── Pending monitor persistence ──────────────────────────────────

    def _load_pending(self):
        """Load pending monitor entries from disk."""
        if not os.path.exists(self._pending_file):
            return []
        try:
            with open(self._pending_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return []

    def _save_pending(self, entries):
        """Save pending monitor entries to disk atomically."""
        try:
            with atomic_write(self._pending_file) as f:
                json.dump(entries, f)
        except (IOError, OSError) as e:
            logger.debug(f"[blackhole] Could not write pending monitors: {e}")

    def _add_pending(self, torrent_id, filename, label=None):
        """Add a torrent to the pending monitors file."""
        with self._monitors_lock:
            entries = self._load_pending()
            if any(e['torrent_id'] == torrent_id for e in entries):
                return
            entry = {
                'torrent_id': torrent_id,
                'filename': filename,
                'service': self.debrid_service,
                'timestamp': time.time(),
            }
            # Persist label alongside the torrent so restart/resume keeps routing
            if label is not None:
                entry['label'] = label
            entries.append(entry)
            self._save_pending(entries)

    def _remove_pending(self, torrent_id):
        """Remove a torrent from the pending monitors file."""
        with self._monitors_lock:
            entries = self._load_pending()
            entries = [e for e in entries if e['torrent_id'] != torrent_id]
            self._save_pending(entries)
            self._active_monitors.discard(torrent_id)

    # ── Monitor orchestration ────────────────────────────────────────

    def _start_monitor(self, torrent_id, filename, label=None):
        """Spawn a background thread to monitor a torrent and create symlinks."""
        with self._monitors_lock:
            if torrent_id in self._active_monitors:
                logger.debug(f"[blackhole] Already monitoring torrent {torrent_id}")
                return
            self._active_monitors.add(torrent_id)

        self._add_pending(torrent_id, filename, label=label)
        t = threading.Thread(
            target=self._monitor_and_symlink,
            args=(torrent_id, filename, label),
            daemon=True,
        )
        t.start()
        tag = f" [label={label}]" if label else ""
        logger.info(f"[blackhole] Monitoring torrent {torrent_id} for {filename}{tag}")

    def _monitor_and_symlink(self, torrent_id, filename, label=None):
        """Background thread: poll debrid status, wait for mount, create symlinks.

        This method runs in its own thread and must not block the main scan loop.
        *label* is the per-arr routing label (e.g. "sonarr"); None means flat mode.
        """
        status_dispatch = {
            'realdebrid': self._check_realdebrid_status,
            'alldebrid': self._check_alldebrid_status,
            'torbox': self._check_torbox_status,
        }
        check_status = status_dispatch.get(self.debrid_service)
        if not check_status:
            logger.error(f"[blackhole] No status checker for {self.debrid_service}")
            self._remove_pending(torrent_id)
            return

        # Phase 1: Wait for debrid to finish downloading
        start_time = time.time()
        release_name = None
        info = {}

        while not self._stop_event.is_set():
            elapsed = time.time() - start_time
            if elapsed > self.mount_poll_timeout:
                logger.warning(f"[blackhole] Timeout waiting for debrid to process {filename} "
                               f"(torrent {torrent_id}, {elapsed:.0f}s)")
                try:
                    from utils.metrics import metrics
                    metrics.inc('blackhole_torrent_timeout')
                except Exception:
                    pass
                if _notify:
                    _notify('download_error', 'Blackhole: Torrent Timeout',
                            f'{filename} timed out waiting for debrid processing',
                            level='warning')
                self._remove_pending(torrent_id)
                return

            try:
                status, info = check_status(torrent_id)
            except Exception as e:
                logger.warning(f"[blackhole] Error checking status for {torrent_id}: {e}")
                self._stop_event.wait(self.mount_poll_interval)
                continue

            if self._is_torrent_ready(status):
                release_name = self._extract_release_name(info)
                logger.info(f"[blackhole] Torrent ready: {filename} (release: {release_name})")
                # Disc rip detection: check debrid file list before mount wait
                if not self._has_usable_media_files(info):
                    logger.warning(f"[blackhole] No recognized media files in {filename} — "
                                   f"auto-blocklisting and removing from debrid.")
                    _mt, _ep = _enrich_for_history(filename) if _history else (None, None)
                    if _blocklist and str(os.environ.get('BLOCKLIST_AUTO_ADD', 'true')).lower() == 'true':
                        bl_hash = self._extract_hash_from_info(info)
                        if bl_hash:
                            _blocklist.add(bl_hash, filename, reason='disc rip (no usable media files)', source='auto')
                            if _history:
                                _history.log_event('blocklist_added', filename, episode=_ep, source='blackhole',
                                                   detail='Auto-blocklisted: disc rip',
                                                   meta={'info_hash': bl_hash},
                                                   media_title=_mt)
                    try:
                        from utils.debrid_client import get_debrid_client
                        client, _svc = get_debrid_client()
                        if client:
                            client.delete_torrent(str(torrent_id))
                    except Exception as e:
                        logger.debug(f"[blackhole] Failed to delete disc rip from debrid: {e}")
                    if _history:
                        _history.log_event('failed', filename, episode=_ep, source='blackhole',
                                           detail='Rejected: no usable media files',
                                           meta={'provider': self.debrid_service, 'torrent_id': torrent_id},
                                           media_title=_mt)
                    try:
                        from utils.metrics import metrics
                        metrics.inc('blackhole_disc_rip_rejected')
                    except Exception:
                        pass
                    if _notify:
                        _notify('download_error', 'Blackhole: No Media Files',
                                f'{filename} contains no recognized media files. '
                                f'Auto-blocklisted and removed from debrid.',
                                level='warning')
                    self._remove_pending(torrent_id)
                    return
                if _history:
                    _mt, _ep = _enrich_for_history(filename)
                    _history.log_event('cached', filename, episode=_ep, source='blackhole',
                                       detail=f'Ready on {self.debrid_service}',
                                       meta={'provider': self.debrid_service, 'torrent_id': torrent_id},
                                       media_title=_mt)
                break

            if self._is_terminal_error(status):
                logger.error(f"[blackhole] Torrent {torrent_id} hit terminal error: {status}")
                _mt, _ep = _enrich_for_history(filename) if _history else (None, None)
                if _history:
                    _history.log_event('failed', filename, episode=_ep, source='blackhole',
                                       detail=f'Terminal error: {status}',
                                       meta={'provider': self.debrid_service, 'torrent_id': torrent_id},
                                       media_title=_mt)
                # Auto-blocklist on terminal failure
                if _blocklist and str(os.environ.get('BLOCKLIST_AUTO_ADD', 'true')).lower() == 'true':
                    bl_hash = self._extract_hash_from_info(info)
                    if bl_hash:
                        _blocklist.add(bl_hash, filename, reason=f'Terminal error: {status}', source='auto')
                        if _history:
                            _history.log_event('blocklist_added', filename, episode=_ep, source='blackhole',
                                               detail=f'Auto-blocklisted: {status}',
                                               meta={'info_hash': bl_hash},
                                               media_title=_mt)
                try:
                    from utils.metrics import metrics
                    metrics.inc('blackhole_symlink_failed')
                except Exception:
                    pass
                if _notify:
                    _notify('download_error', 'Blackhole: Torrent Error',
                            f'{filename} failed with debrid status: {status}',
                            level='error')
                self._remove_pending(torrent_id)
                return

            logger.debug(f"[blackhole] Torrent {torrent_id} status: {status} ({elapsed:.0f}s)")
            self._stop_event.wait(self.mount_poll_interval)

        if self._stop_event.is_set():
            return

        if not release_name:
            logger.error(f"[blackhole] Could not determine release name for {filename}")
            self._remove_pending(torrent_id)
            return

        # Phase 2: Wait for content to appear on the rclone mount
        # Uses its own timeout budget separate from the debrid polling phase
        mount_start = time.time()
        mount_path = None
        category = None

        # Kick rclone to re-list the top-level category dirs immediately so
        # we don't have to wait for its next --poll-interval tick. Belt and
        # suspenders: rclone's active polling handles subsequent ticks, so
        # we only call this once at the start.
        try:
            from utils.rclone_rc import refresh_dir
            refresh_dir('')
        except Exception:
            pass

        while not self._stop_event.is_set():
            elapsed_mount = time.time() - mount_start
            if elapsed_mount > self.mount_poll_timeout:
                logger.warning(f"[blackhole] Timeout waiting for {release_name} on mount "
                               f"({elapsed_mount:.0f}s)")
                try:
                    from utils.metrics import metrics
                    metrics.inc('blackhole_torrent_timeout')
                except Exception:
                    pass
                if _notify:
                    _notify('download_error', 'Blackhole: Mount Timeout',
                            f'{filename} timed out waiting for content on mount',
                            level='warning')
                self._remove_pending(torrent_id)
                return

            mount_path, category, matched_name = self._find_on_mount(release_name)
            if mount_path:
                logger.info(f"[blackhole] Found on mount: {mount_path} (category: {category})")
                break

            logger.debug(f"[blackhole] Waiting for {release_name} on mount ({elapsed_mount:.0f}s)")
            self._stop_event.wait(self.mount_poll_interval)

        if self._stop_event.is_set():
            return

        # Phase 3: Create symlinks
        try:
            count = self._create_symlinks(matched_name, category, mount_path, label=label)
            if count > 0:
                logger.info(f"[blackhole] Created {count} symlink(s) for {release_name}")
                if _history:
                    _mt, _ep = _enrich_for_history(filename)
                    _history.log_event('symlink_created', filename, episode=_ep, source='blackhole',
                                       detail=f'{count} symlink(s) for {release_name}',
                                       meta={'provider': self.debrid_service, 'count': count},
                                       media_title=_mt)
                try:
                    from utils.metrics import metrics
                    metrics.inc('blackhole_symlink_created')
                except Exception:
                    pass
                if _notify:
                    _notify('download_complete', 'Blackhole: Symlinks Created',
                            f'{count} symlink(s) created for {release_name}')
                try:
                    from utils.library import get_scanner
                    scanner = get_scanner()
                    if scanner:
                        scanner.refresh()
                except Exception:
                    pass
            else:
                logger.warning(f"[blackhole] No media files found to symlink for {release_name}")
        except Exception as e:
            logger.error(f"[blackhole] Error creating symlinks for {release_name}: {e}")
            try:
                from utils.metrics import metrics
                metrics.inc('blackhole_symlink_failed')
            except Exception:
                pass

        self._remove_pending(torrent_id)

    def _resume_pending_monitors(self):
        """Resume monitoring for any torrents that were pending before a restart.

        Each entry is validated independently — a malformed or tampered entry
        (e.g. non-dict, label with path-traversal characters) is dropped with
        a warning rather than aborting the whole resume loop. This matters
        because the worker thread calling _resume_pending_monitors has only
        a top-level scan guard, not a resume guard.
        """
        entries = self._load_pending()
        if not entries:
            return

        logger.info(f"[blackhole] Resuming {len(entries)} pending torrent monitor(s)")
        for entry in entries:
            try:
                if not isinstance(entry, dict):
                    logger.warning(f"[blackhole] Skipping non-dict pending entry: {entry!r}")
                    continue
                torrent_id = entry.get('torrent_id')
                filename = entry.get('filename', 'unknown')
                # Legacy entries (pre-label-routing) have no 'label' field → None.
                # Validate because pending_monitors.json is trust-boundary state:
                # a tampered label would be piped into os.path.join downstream
                # and could create directories outside completed_dir.
                label = entry.get('label')
                if label is not None and not (isinstance(label, str) and _is_valid_label(label)):
                    logger.warning(
                        f"[blackhole] Dropping invalid label on pending entry {torrent_id!r}: {label!r}"
                    )
                    label = None
                if torrent_id:
                    self._start_monitor(torrent_id, filename, label=label)
            except Exception as e:
                logger.warning(f"[blackhole] Skipping bad pending entry {entry!r}: {e}")

    # ── Local library dedup ─────────────────────────────────────────

    @staticmethod
    def _normalize_name(name):
        """Normalize a library folder or release name for comparison."""
        # Strip year in parens e.g. "Fargo (2014)" -> "Fargo"
        name = re.sub(r'\s*\(\d{4}\)\s*', '', name)
        return name.lower().strip()

    def _check_local_library(self, filename):
        """Check if content from this torrent already exists locally.

        Returns True if content exists locally (should skip), False otherwise.
        Skips dedup for titles with prefer-debrid preference (user explicitly
        wants the debrid copy even though a local copy exists).
        """
        if not self.dedup_enabled:
            return False

        name, season, is_tv = parse_release_name(filename)
        if not name:
            return False

        name_norm = self._normalize_name(name)

        # Skip dedup for prefer-debrid titles — user wants the debrid copy
        try:
            from utils.library_prefs import get_all_preferences
            prefs = get_all_preferences()
            if prefs.get(name_norm) == 'prefer-debrid':
                logger.info(f"[blackhole] Bypassing local dedup for {filename}: prefer-debrid preference set")
                return False
        except Exception:
            pass

        if is_tv and self.local_library_tv and os.path.isdir(self.local_library_tv):
            for folder in os.listdir(self.local_library_tv):
                if self._normalize_name(folder) != name_norm:
                    continue
                show_path = os.path.join(self.local_library_tv, folder)
                if season is not None:
                    season_dir = os.path.join(show_path, f"Season {season:02d}")
                    if os.path.isdir(season_dir) and os.listdir(season_dir):
                        # Check at episode level if the torrent targets specific episodes
                        target_eps = _parse_episodes(filename)
                        if target_eps:
                            local_eps = _local_episodes(season_dir)
                            if target_eps <= local_eps:
                                logger.info(f"[blackhole] Skipping {filename}: '{folder}' S{season:02d} episodes {sorted(target_eps)} exist locally")
                                return True
                            logger.debug(f"[blackhole] '{folder}' S{season:02d} has local eps {sorted(local_eps)} but torrent has {sorted(target_eps)} — not skipping")
                        else:
                            # Season pack — skip if season folder has content
                            logger.info(f"[blackhole] Skipping {filename}: '{folder}' Season {season} exists locally")
                            return True
                else:
                    if os.path.isdir(show_path) and os.listdir(show_path):
                        logger.info(f"[blackhole] Skipping {filename}: '{folder}' exists locally")
                        return True

        if not is_tv and self.local_library_movies and os.path.isdir(self.local_library_movies):
            for folder in os.listdir(self.local_library_movies):
                if self._normalize_name(folder) != name_norm:
                    continue
                movie_path = os.path.join(self.local_library_movies, folder)
                if os.path.isdir(movie_path) and os.listdir(movie_path):
                    logger.info(f"[blackhole] Skipping {filename}: '{folder}' exists locally")
                    return True

        return False

    # ── Debrid rejection auto-retry ──────────────────────────────────

    # RD error codes that mean "this specific hash is blocked, try another"
    _REJECTION_CODES = {35, 30}  # infringing_file, torrent_file_invalid
    _REJECTION_KEYWORDS = {'infringing_file', 'torrent_file_invalid'}

    @staticmethod
    def _alt_exhausted(file_path):
        """Check if alternative releases were already tried and exhausted."""
        meta_path = RetryMeta.meta_path(file_path)
        if not os.path.exists(meta_path):
            return False
        try:
            with open(meta_path, 'r') as f:
                return json.load(f).get('alt_exhausted', False)
        except (json.JSONDecodeError, IOError):
            return False

    @classmethod
    def _is_debrid_rejection(cls, result_text):
        """Check if a debrid error response indicates the hash is blocked."""
        if not isinstance(result_text, str):
            return False
        rt = result_text.lower()
        if any(kw in rt for kw in cls._REJECTION_KEYWORDS):
            return True
        return any(
            f'"error_code": {c}' in rt or f'"error_code":{c}' in rt
            for c in cls._REJECTION_CODES
        )

    def _try_alternative_release(self, filename, file_path, debrid_handler, label=None):
        """On debrid rejection, query Sonarr/Radarr for an alternative release.

        Parses the episode/movie info from the filename, fetches available
        releases, filters to a different info hash, and tries them until
        one succeeds or all are exhausted.

        Runs in a background thread. On failure, moves the original file
        to the failed/ directory (same as the normal failure path).

        *label* preserves per-arr routing — if the file was staged from
        ``/watch/sonarr/.alt_pending/``, failures land in ``/watch/sonarr/failed/``.
        """
        alt_ok = False
        try:
            from utils.arr_client import SonarrClient, RadarrClient

            name, season, is_tv = parse_release_name(filename)
            if not name:
                logger.debug(f"[blackhole] Cannot parse release name for alt-retry: {filename}")
            elif is_tv and season is not None and _parse_episodes(filename):
                alt_ok = self._try_alt_episode(name, season, _parse_episodes(filename),
                                               debrid_handler, filename, file_path, label=label)
            elif not is_tv:
                alt_ok = self._try_alt_movie(name, debrid_handler, filename, file_path, label=label)
            else:
                logger.debug(f"[blackhole] Cannot determine content type for alt-retry: {filename}")
        except Exception as e:
            logger.error(f"[blackhole] Error during alternative release search: {e}")

        if not alt_ok and os.path.exists(file_path):
            # No alternative worked — move to failed/ and mark alts exhausted
            # so retries don't repeat the same alt-release search
            error_dir = self._failed_dir(label)
            os.makedirs(error_dir, exist_ok=True)
            dest = os.path.join(error_dir, filename)
            if os.path.exists(dest):
                base, fext = os.path.splitext(filename)
                dest = os.path.join(error_dir, f"{base}_{int(time.time())}{fext}")
            rename_ok = False
            try:
                os.rename(file_path, dest)
                rename_ok = True
                # Mark alt-exhausted in retry metadata
                meta_path = RetryMeta.meta_path(dest)
                try:
                    with open(meta_path, 'w') as f:
                        json.dump({'retries': 1, 'last_attempt': time.time(),
                                   'alt_exhausted': True}, f)
                except IOError:
                    pass
            except OSError as e:
                logger.warning(f"[blackhole] Could not move {filename} to failed/: {e}")

            # Notify user — all alternatives exhausted, manual intervention needed
            if _notify:
                detail = (f'File moved to failed/ — manual intervention required.'
                          if rename_ok else
                          f'Could not move to failed/ — file may still be in watch dir.')
                _notify('download_error', 'Blackhole: All Alternatives Failed',
                        f'No working alternative releases found for {filename}. {detail}',
                        level='warning')
            if _history:
                _mt, _ep = _enrich_for_history(filename)
                _history.log_event('failed', filename, episode=_ep, source='blackhole',
                                   detail='All alternative releases exhausted',
                                   media_title=_mt)

    def _try_alt_episode(self, series_name, season, episodes, debrid_handler, orig_filename, orig_path, label=None):
        """Try alternative releases for a TV episode via Sonarr."""
        from utils.arr_client import SonarrClient

        client = SonarrClient()
        if not client.configured:
            return False

        ep_num = min(episodes)  # primary episode number
        episode_id = client.get_episode_id(series_name, season, ep_num)
        if not episode_id:
            logger.debug(f"[blackhole] Could not find {series_name} S{season:02d}E{ep_num:02d} in Sonarr")
            return False

        releases = client.get_episode_releases(episode_id)
        if not releases:
            logger.debug(f"[blackhole] No alternative releases found for {series_name} S{season:02d}E{ep_num:02d}")
            return False

        return self._try_releases(releases, debrid_handler, orig_filename, orig_path, label=label)

    def _try_alt_movie(self, movie_name, debrid_handler, orig_filename, orig_path, label=None):
        """Try alternative releases for a movie via Radarr."""
        from utils.arr_client import RadarrClient

        client = RadarrClient()
        if not client.configured:
            return False

        movie = client.find_movie_in_library(title=movie_name)
        if not movie:
            logger.debug(f"[blackhole] Could not find '{movie_name}' in Radarr")
            return False

        releases = client.get_movie_releases(movie['id'])
        if not releases:
            logger.debug(f"[blackhole] No alternative releases found for '{movie_name}'")
            return False

        return self._try_releases(releases, debrid_handler, orig_filename, orig_path, label=label)

    def _try_releases(self, releases, debrid_handler, orig_filename, orig_path, label=None):
        """Try magnet releases one by one until one succeeds on the debrid service.

        Only tries releases with magnet links (direct hashes) to avoid
        the 404 problem with torrent file download URLs.
        Skips the original release's info hash.
        """
        import tempfile

        # Extract original info hash to skip it
        orig_hash = self._extract_info_hash_from_file(orig_path)
        tried = 0
        max_tries = 5

        for r in releases:
            if tried >= max_tries:
                break
            if r.get('rejected'):
                continue
            guid = r.get('guid', '')
            if not guid.startswith('magnet:'):
                continue

            # Extract info hash from magnet URI
            m = re.search(r'btih:([A-Fa-f0-9]+)', guid, re.IGNORECASE)
            if not m:
                continue
            info_hash = m.group(1).upper()

            # Skip if same hash as the one that was rejected
            if orig_hash and info_hash == orig_hash.upper():
                continue

            # Skip blocklisted hashes
            if _blocklist and _blocklist.is_blocked(info_hash):
                logger.debug(f"[blackhole] Skipping blocklisted alternative: {info_hash[:16]}...")
                continue

            tried += 1
            alt_title = r.get('title', 'unknown')
            logger.info(f"[blackhole] Trying alternative release: {alt_title[:60]} (hash {info_hash})")

            # Write magnet to a temp file outside watch_dir to avoid scanner pickup
            import tempfile
            tmp_fd, tmp_path = tempfile.mkstemp(suffix='.magnet', prefix='_alt_')
            try:
                with os.fdopen(tmp_fd, 'w') as f:
                    f.write(guid)
                success, result = debrid_handler(tmp_path)
                if success:
                    logger.info(f"[blackhole] Alternative release accepted: {alt_title[:60]}")
                    # Clean up original file
                    try:
                        os.remove(orig_path)
                    except OSError as e:
                        logger.warning(f"[blackhole] Could not remove original after alt-retry: {e}")
                    # Start symlink monitoring
                    if self.symlink_enabled:
                        torrent_id = self._extract_torrent_id(result)
                        if torrent_id:
                            self._start_monitor(torrent_id, orig_filename, label=label)
                    if _notify:
                        _notify('download_complete', 'Blackhole: Alt Release Found',
                                f'Original rejected, using: {alt_title[:60]}')
                    return True
                else:
                    logger.debug(f"[blackhole] Alternative also rejected: {alt_title[:60]}: {str(result)[:100]}")
            except Exception as e:
                logger.debug(f"[blackhole] Error trying alternative {alt_title[:60]}: {e}")
            finally:
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

        logger.warning(f"[blackhole] No working alternative found for {orig_filename} (tried {tried})")
        return False

    @staticmethod
    def _extract_info_hash_from_file(file_path):
        """Extract info hash from a .magnet or .torrent file.

        For .magnet: parses the btih: URI parameter.
        For .torrent: locates the bencoded 'info' dict and SHA1 hashes
        its raw bytes (the standard BitTorrent info hash computation).
        """
        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.magnet':
            try:
                with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                    content = f.read().strip()
                m = re.search(r'btih:([A-Fa-f0-9]+)', content, re.IGNORECASE)
                if m:
                    return m.group(1).upper()
            except OSError:
                pass
        elif ext == '.torrent':
            try:
                with open(file_path, 'rb') as f:
                    data = f.read()
                # Find the raw bytes of the 'info' value in the bencoded torrent.
                # Bencode format: ...4:info<value>... where <value> starts with 'd'
                # We find the start of the info value and extract to its matching end.
                marker = b'4:infod'
                idx = data.find(marker)
                if idx == -1:
                    return None
                info_start = idx + len(b'4:info')  # start of the dict value ('d...')
                # Walk the bencoded structure to find the matching 'e'
                info_end = _bencode_end(data, info_start)
                if info_end is not None:
                    info_bytes = data[info_start:info_end]
                    return hashlib.sha1(info_bytes).hexdigest().upper()
            except (OSError, ValueError):
                pass
        return None

    # ── File processing ──────────────────────────────────────────────

    def _process_file(self, file_path, label=None):
        """Process a single torrent/magnet file.

        *label* is the per-arr routing label derived from the subdir of
        ``watch_dir`` containing the file, or None for flat-mode files.
        """
        filename = os.path.basename(file_path)
        if label:
            logger.info(f"[blackhole] Processing: {filename} [label={label}]")
        else:
            logger.info(f"[blackhole] Processing: {filename}")

        # Check local library before submitting to debrid
        if self._check_local_library(filename):
            try:
                os.remove(file_path)
                logger.info(f"[blackhole] Removed {filename} (local duplicate)")
            except OSError as e:
                logger.warning(f"[blackhole] Could not remove {filename}: {e}")
            try:
                from utils.metrics import metrics
                metrics.inc('blackhole_processed', {'status': 'skipped_local'})
            except Exception:
                pass
            return

        # Check blocklist before submitting to debrid
        if _blocklist:
            info_hash = self._extract_info_hash_from_file(file_path)
            if info_hash and _blocklist.is_blocked(info_hash):
                logger.info(f"[blackhole] Skipping blocklisted torrent: {filename} ({info_hash[:16]}...)")
                if _history:
                    _mt, _ep = _enrich_for_history(filename)
                    _history.log_event('blocklisted', filename, episode=_ep, source='blackhole',
                                       detail=f'Skipped — info hash is blocklisted',
                                       meta={'info_hash': info_hash},
                                       media_title=_mt)
                try:
                    os.remove(file_path)
                except OSError as e:
                    logger.warning(f"[blackhole] Could not remove blocklisted file {filename}: {e}")
                return

        dispatch = {
            'realdebrid': self._add_to_realdebrid,
            'alldebrid': self._add_to_alldebrid,
            'torbox': self._add_to_torbox,
        }

        handler = dispatch.get(self.debrid_service)
        if not handler:
            logger.error(f"[blackhole] Unsupported debrid service: {self.debrid_service}")
            return

        try:
            success, result = handler(file_path)
            if success:
                logger.info(f"[blackhole] Added to {self.debrid_service}: {filename}")

                # Record pending FIRST — prevents orphaned debrid torrents if
                # we crash before reaching file cleanup or notifications.
                # Guarded so a monitor failure doesn't block file cleanup.
                if self.symlink_enabled:
                    torrent_id = self._extract_torrent_id(result)
                    if torrent_id:
                        try:
                            self._start_monitor(torrent_id, filename, label=label)
                        except Exception as e:
                            logger.error(f"[blackhole] Failed to start monitor for {filename}: {e}")
                    else:
                        logger.warning(f"[blackhole] Could not extract torrent ID for symlink monitoring: {filename}")

                if _history:
                    _mt, _ep = _enrich_for_history(filename)
                    _history.log_event('grabbed', filename, episode=_ep, source='blackhole',
                                       detail=f'Submitted to {self.debrid_service}',
                                       meta={'provider': self.debrid_service},
                                       media_title=_mt)
                try:
                    os.remove(file_path)
                except OSError as e:
                    logger.warning(f"[blackhole] Could not remove {filename}: {e}")
                try:
                    from utils.metrics import metrics
                    metrics.inc('blackhole_processed', {'status': 'success'})
                except Exception:
                    pass

                if _notify:
                    if self.symlink_enabled:
                        _notify('download_complete', 'Blackhole: Torrent Submitted',
                                f'{filename} submitted to {self.debrid_service}, monitoring for symlinks')
                    else:
                        _notify('download_complete', 'Blackhole: Torrent Added',
                                f'{filename} added to {self.debrid_service}')
            else:
                logger.error(f"[blackhole] Failed to add {filename}: {result}")

                # On debrid rejection (infringing/blocked), try alternative release
                # in a background thread to avoid blocking the scan loop.
                # Skip if alts were already exhausted in a prior attempt.
                if self._is_debrid_rejection(result) and not self._alt_exhausted(file_path):
                    # Move file out of watch_dir BEFORE launching the thread
                    # to prevent the next scan cycle from picking it up again
                    staging_dir = self._alt_pending_dir(label)
                    os.makedirs(staging_dir, exist_ok=True)
                    staged_path = os.path.join(staging_dir, filename)
                    try:
                        os.rename(file_path, staged_path)
                    except OSError as e:
                        logger.warning(
                            f"[blackhole] Could not stage {filename} for alt-retry: {e}. "
                            f"Skipping alt-retry to prevent duplicate submission."
                        )
                        # Fall through to normal failed/ path below
                    else:
                        threading.Thread(
                            target=self._try_alternative_release,
                            args=(filename, staged_path, handler, label),
                            daemon=True,
                            name=f'alt-retry-{filename[:30]}',
                        ).start()
                        return  # Alt-retry thread handles cleanup

                error_dir = self._failed_dir(label)
                os.makedirs(error_dir, exist_ok=True)
                dest = os.path.join(error_dir, filename)
                if os.path.exists(dest):
                    base, fext = os.path.splitext(filename)
                    dest = os.path.join(error_dir, f"{base}_{int(time.time())}{fext}")
                os.rename(file_path, dest)
                try:
                    from utils.metrics import metrics
                    metrics.inc('blackhole_processed', {'status': 'failed'})
                except Exception:
                    pass
                # Track retry state
                retries, _ = RetryMeta.read(dest)
                RetryMeta.write(dest, retries + 1)
                if retries + 1 >= MAX_RETRIES:
                    logger.error(f"[blackhole] {filename} has permanently failed after {MAX_RETRIES} attempts")
                    if _notify:
                        _notify('download_error', 'Blackhole: Permanent Failure',
                                f'{filename} failed {MAX_RETRIES} times and will not be retried',
                                level='error')
        except Exception as e:
            logger.error(f"[blackhole] Error processing {filename}: {e}")

    def _retry_failed(self):
        """Scan failed/ directory and retry eligible files.

        Supports both flat layout (watch_dir/failed/<file>) and labeled
        layout (watch_dir/failed/<label>/<file>). Labeled files are moved
        back to watch_dir/<label>/ so the next scan re-detects the label.
        """
        failed_root = os.path.join(self.watch_dir, 'failed')
        if not os.path.exists(failed_root):
            return

        # (label, file_path, filename) triples to retry
        candidates = []
        try:
            for entry in os.listdir(failed_root):
                ep = os.path.join(failed_root, entry)
                if os.path.isfile(ep):
                    candidates.append((None, ep, entry))
                elif os.path.isdir(ep) and _is_valid_label(entry):
                    try:
                        for sub in os.listdir(ep):
                            sp = os.path.join(ep, sub)
                            if os.path.isfile(sp):
                                candidates.append((entry, sp, sub))
                    except OSError:
                        continue
        except OSError:
            return

        for label, file_path, filename in candidates:
            ext = os.path.splitext(filename)[1].lower()
            if ext not in self.SUPPORTED_EXTENSIONS:
                continue

            retries, last_attempt = RetryMeta.read(file_path)

            if retries >= MAX_RETRIES:
                continue

            # Don't retry files where alt-release search was already exhausted
            # (the original hash is debrid-blocked, retrying submits the same hash)
            if self._alt_exhausted(file_path):
                continue

            # Determine backoff delay for this retry
            delay_idx = min(retries, len(RETRY_SCHEDULE) - 1)
            delay = RETRY_SCHEDULE[delay_idx]

            if time.time() - last_attempt < delay:
                continue

            logger.info(f"[blackhole] Retrying failed file: {filename} (attempt {retries + 1}/{MAX_RETRIES})"
                        f"{f' [label={label}]' if label else ''}")
            try:
                from utils.metrics import metrics
                metrics.inc('blackhole_retry')
            except Exception:
                pass

            # Move back to watch dir (or label subdir) for reprocessing.
            # Preserving the label subdir keeps per-arr routing intact.
            if label:
                retry_dir = os.path.join(self.watch_dir, label)
                os.makedirs(retry_dir, exist_ok=True)
                retry_path = os.path.join(retry_dir, filename)
            else:
                retry_path = os.path.join(self.watch_dir, filename)
            # Refuse to clobber a fresh drop with the same filename — POSIX
            # os.rename silently overwrites. Leave the failed file in place
            # and try again next tick; the arr's re-grab wins.
            if os.path.exists(retry_path):
                logger.debug(
                    f"[blackhole] Skipping retry of {filename}: a newer file is already at {retry_path}"
                )
                continue
            try:
                RetryMeta.remove(file_path)
                os.rename(file_path, retry_path)
            except OSError as e:
                logger.error(f"[blackhole] Failed to move {filename} for retry: {e}")

    def _scan(self):
        """Scan watch directory for new files.

        Supports two layouts:
          - Flat: .torrent/.magnet files sit directly in watch_dir → label=None
          - Labeled: one level of subdirectories, each subdir name becomes
            the routing label (e.g. /watch/sonarr/x.torrent → label="sonarr")
        Both layouts coexist. Invalid label names are logged and skipped.
        """
        if not os.path.exists(self.watch_dir):
            return

        now = time.time()
        watch_realpath = os.path.realpath(self.watch_dir)

        for entry in os.listdir(self.watch_dir):
            entry_path = os.path.join(self.watch_dir, entry)

            # Guard against symlink escapes
            real_path = os.path.realpath(entry_path)
            if not real_path.startswith(watch_realpath + os.sep) and real_path != watch_realpath:
                continue

            if os.path.isfile(entry_path):
                self._maybe_process_watch_file(entry_path, entry, now, label=None)
                continue

            if not os.path.isdir(entry_path):
                continue

            # Skip reserved subdirs (failed/, .alt_pending/) — handled separately
            if entry.lower() in _RESERVED_LABELS:
                continue

            # Validate label name. Invalid names are skipped (not processed as
            # unlabeled — that would defeat the purpose and surprise the user).
            if not _is_valid_label(entry):
                logger.warning(f"[blackhole] Ignoring invalid label subdir: {entry!r} "
                               f"(labels must be [A-Za-z0-9_-], max {_LABEL_MAX_LEN} chars)")
                continue

            try:
                sub_entries = os.listdir(entry_path)
            except OSError as e:
                logger.debug(f"[blackhole] Cannot list label subdir {entry}: {e}")
                continue

            for fname in sub_entries:
                fpath = os.path.join(entry_path, fname)
                # Symlink-escape guard (file must remain under watch_dir)
                fp_real = os.path.realpath(fpath)
                if not fp_real.startswith(watch_realpath + os.sep):
                    continue
                if not os.path.isfile(fpath):
                    continue
                self._maybe_process_watch_file(fpath, fname, now, label=entry)

    def _maybe_process_watch_file(self, file_path, filename, now, label):
        """Shared pre-processing: skip in-flight writes, dispatch on extension."""
        try:
            if now - os.path.getmtime(file_path) < 2.0:
                return
        except OSError:
            return
        ext = os.path.splitext(filename)[1].lower()
        if ext in self.SUPPORTED_EXTENSIONS:
            self._process_file(file_path, label=label)

    def _recover_alt_pending(self):
        """On startup, move stranded .alt_pending files to failed/.

        If the container was killed while an alt-retry thread was running,
        files in .alt_pending/ would be orphaned with no recovery path.
        Walks both flat layout (.alt_pending/*) and labeled layout
        (.alt_pending/<label>/*), preserving the label in the failed/ move.
        """
        staging_root = os.path.join(self.watch_dir, '.alt_pending')
        if not os.path.isdir(staging_root):
            return

        # (label, src_path, filename) triples
        stranded = []
        try:
            for entry in os.listdir(staging_root):
                ep = os.path.join(staging_root, entry)
                if os.path.isfile(ep):
                    stranded.append((None, ep, entry))
                elif os.path.isdir(ep) and _is_valid_label(entry):
                    try:
                        for sub in os.listdir(ep):
                            sp = os.path.join(ep, sub)
                            if os.path.isfile(sp):
                                stranded.append((entry, sp, sub))
                    except OSError:
                        continue
        except OSError:
            return

        for label, src, filename in stranded:
            error_dir = self._failed_dir(label)
            os.makedirs(error_dir, exist_ok=True)
            dest = os.path.join(error_dir, filename)
            if os.path.exists(dest):
                base, fext = os.path.splitext(filename)
                dest = os.path.join(error_dir, f"{base}_{int(time.time())}{fext}")
            try:
                os.rename(src, dest)
                # Mark alt_exhausted so retries don't repeat the search
                try:
                    with open(RetryMeta.meta_path(dest), 'w') as f:
                        json.dump({'retries': 1, 'last_attempt': time.time(),
                                   'alt_exhausted': True}, f)
                except IOError:
                    pass
                tag = f" [label={label}]" if label else ""
                logger.warning(f"[blackhole] Recovered stranded alt-pending file: {filename}{tag}")
            except OSError as e:
                logger.warning(f"[blackhole] Could not recover {filename} from alt_pending: {e}")

    def run(self):
        """Main loop - scan at poll_interval."""
        logger.info(f"[blackhole] Watching {self.watch_dir} (poll: {self.poll_interval}s, service: {self.debrid_service})")
        try:
            self._recover_alt_pending()
        except Exception as e:
            logger.error(f"[blackhole] _recover_alt_pending failed at startup: {e}")
        if self.symlink_enabled:
            logger.info(f"[blackhole] Symlink mode enabled: completed={self.completed_dir}, "
                        f"mount={self.rclone_mount}, target_base={self.symlink_target_base}, "
                        f"timeout={self.mount_poll_timeout}s, interval={self.mount_poll_interval}s, "
                        f"max_age={self.symlink_max_age}h")
            try:
                self._resume_pending_monitors()
            except Exception as e:
                # Even a catastrophic load failure must not kill the worker
                # thread — the main scan loop will still handle new drops.
                logger.error(f"[blackhole] _resume_pending_monitors failed at startup: {e}")

        while not self._stop_event.is_set():
            try:
                self._scan()
                self._retry_failed()

                # Run symlink cleanup every 5 minutes
                if self.symlink_enabled and (time.time() - self._last_cleanup) > 300:
                    self._last_cleanup = time.time()
                    self._cleanup_symlinks()
            except Exception as e:
                logger.error(f"[blackhole] Scan error: {e}")
            self._stop_event.wait(self.poll_interval)

    def stop(self):
        self._stop_event.set()


def setup():
    """Initialize and start the blackhole watcher if enabled."""
    global _watcher
    from base import config
    RDAPIKEY = config.RDAPIKEY
    ADAPIKEY = config.ADAPIKEY

    blackhole_enabled = os.environ.get('BLACKHOLE_ENABLED', 'false').lower() == 'true'
    if not blackhole_enabled:
        return None

    watch_dir = os.environ.get('BLACKHOLE_DIR', '/watch')
    try:
        poll_interval = int(os.environ.get('BLACKHOLE_POLL_INTERVAL', '5'))
    except (ValueError, TypeError):
        logger.warning("[blackhole] Invalid BLACKHOLE_POLL_INTERVAL, defaulting to 5s")
        poll_interval = 5

    debrid_service = os.environ.get('BLACKHOLE_DEBRID', '').lower()
    debrid_api_key = None

    if not debrid_service:
        if RDAPIKEY:
            debrid_service = 'realdebrid'
            debrid_api_key = RDAPIKEY
        elif ADAPIKEY:
            debrid_service = 'alldebrid'
            debrid_api_key = ADAPIKEY
        else:
            torbox_key = os.environ.get('TORBOX_API_KEY')
            if torbox_key:
                debrid_service = 'torbox'
                debrid_api_key = torbox_key
    else:
        valid_services = {'realdebrid', 'alldebrid', 'torbox'}
        if debrid_service not in valid_services:
            logger.error(f"[blackhole] Unknown BLACKHOLE_DEBRID '{debrid_service}'. Valid: {', '.join(sorted(valid_services))}")
            return None
        key_map = {
            'realdebrid': RDAPIKEY,
            'alldebrid': ADAPIKEY,
            'torbox': os.environ.get('TORBOX_API_KEY'),
        }
        debrid_api_key = key_map.get(debrid_service)

    if not debrid_api_key:
        logger.error("[blackhole] No debrid API key found. Blackhole disabled.")
        return None

    os.makedirs(watch_dir, exist_ok=True)

    # Symlink configuration
    symlink_enabled = os.environ.get('BLACKHOLE_SYMLINK_ENABLED', 'false').lower() == 'true'
    completed_dir = os.environ.get('BLACKHOLE_COMPLETED_DIR', '/completed')
    rclone_mount = os.environ.get('BLACKHOLE_RCLONE_MOUNT', '/data')
    # Auto-detect mount name subdirectory if not explicitly configured
    if rclone_mount == '/data' and os.environ.get('RCLONE_MOUNT_NAME'):
        mount_name = os.environ.get('RCLONE_MOUNT_NAME')
        candidate = os.path.join('/data', mount_name)
        if os.path.isdir(os.path.join(candidate, '__all__')) or os.path.isdir(os.path.join(candidate, 'shows')):
            rclone_mount = candidate
            logger.info(f"[blackhole] Auto-detected rclone mount: {rclone_mount}")
    symlink_target_base = os.environ.get('BLACKHOLE_SYMLINK_TARGET_BASE', '')

    try:
        mount_poll_timeout = int(os.environ.get('BLACKHOLE_MOUNT_POLL_TIMEOUT', '300'))
    except (ValueError, TypeError):
        logger.warning("[blackhole] Invalid BLACKHOLE_MOUNT_POLL_TIMEOUT, defaulting to 300s")
        mount_poll_timeout = 300

    try:
        mount_poll_interval = int(os.environ.get('BLACKHOLE_MOUNT_POLL_INTERVAL', '10'))
    except (ValueError, TypeError):
        logger.warning("[blackhole] Invalid BLACKHOLE_MOUNT_POLL_INTERVAL, defaulting to 10s")
        mount_poll_interval = 10

    try:
        symlink_max_age = int(os.environ.get('BLACKHOLE_SYMLINK_MAX_AGE', '72'))
    except (ValueError, TypeError):
        logger.warning("[blackhole] Invalid BLACKHOLE_SYMLINK_MAX_AGE, defaulting to 72h")
        symlink_max_age = 72

    if symlink_enabled:
        if not symlink_target_base:
            logger.error("[blackhole] BLACKHOLE_SYMLINK_TARGET_BASE is required when symlinks are enabled")
            return None
        os.makedirs(completed_dir, exist_ok=True)

    # Local library dedup configuration
    dedup_enabled = os.environ.get('BLACKHOLE_DEDUP_ENABLED', 'false').lower() == 'true'
    local_library_tv = os.environ.get('BLACKHOLE_LOCAL_LIBRARY_TV', '')
    local_library_movies = os.environ.get('BLACKHOLE_LOCAL_LIBRARY_MOVIES', '')
    if dedup_enabled:
        logger.info(f"[blackhole] Local dedup enabled: tv={local_library_tv}, movies={local_library_movies}")

    _watcher = BlackholeWatcher(
        watch_dir, debrid_api_key, debrid_service, poll_interval,
        symlink_enabled=symlink_enabled,
        completed_dir=completed_dir,
        rclone_mount=rclone_mount,
        symlink_target_base=symlink_target_base,
        mount_poll_timeout=mount_poll_timeout,
        mount_poll_interval=mount_poll_interval,
        symlink_max_age=symlink_max_age,
        dedup_enabled=dedup_enabled,
        local_library_tv=local_library_tv,
        local_library_movies=local_library_movies,
    )
    thread = threading.Thread(target=_watcher.run, daemon=True)
    thread.start()
    return _watcher


def stop():
    """Stop the blackhole watcher if running."""
    if _watcher:
        _watcher.stop()
