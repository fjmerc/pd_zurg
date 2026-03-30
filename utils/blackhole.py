"""Blackhole watch folder for .torrent and .magnet files.

Monitors a directory for torrent/magnet files, submits them to the
configured debrid service, and removes the file after processing.
Compatible with Sonarr/Radarr blackhole download client configuration.

When symlink mode is enabled, monitors submitted torrents until content
appears on the rclone mount, then creates symlinks in a completed
directory for Sonarr/Radarr to import.
"""

import json
import os
import re
import shutil
import time
import threading
import requests
from utils.logger import get_logger

logger = get_logger()

try:
    from utils.notifications import notify as _notify
except ImportError:
    _notify = None

_watcher = None

# Retry configuration for failed torrent submissions
RETRY_SCHEDULE = [300, 900, 3600]  # 5 min, 15 min, 1 hour
MAX_RETRIES = 3

# Media file extensions for symlink creation
MEDIA_EXTENSIONS = {'.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.ts', '.m4v', '.webm'}

# Zurg mount category directories (checked in order; __all__ is fallback)
MOUNT_CATEGORIES = ['shows', 'movies', 'anime']

# Terminal debrid statuses that mean the torrent will never complete
RD_TERMINAL_ERRORS = {'magnet_error', 'error', 'virus', 'dead'}
AD_TERMINAL_ERRORS = {'Error'}
TB_TERMINAL_ERRORS = {'error', 'failed'}


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
        if lo <= hi:
            return set(range(lo, hi + 1))
        return {lo, hi}
    return set(nums)


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
            response = requests.post(url, headers=headers, data={'magnet': magnet_link}, timeout=30)
        elif ext == '.torrent':
            url = 'https://api.real-debrid.com/rest/1.0/torrents/addTorrent'
            with open(file_path, 'rb') as f:
                response = requests.put(url,
                                        headers={**headers, 'Content-Type': 'application/x-bittorrent'},
                                        data=f.read(), timeout=30)
        else:
            return False, f'Unsupported extension: {ext}'

        if response.status_code in (200, 201):
            torrent_id = response.json().get('id')
            if not torrent_id:
                return False, 'Real-Debrid response missing torrent id'
            select_url = f'https://api.real-debrid.com/rest/1.0/torrents/selectFiles/{torrent_id}'
            select_resp = requests.post(select_url, headers=headers, data={'files': 'all'}, timeout=30)
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
            response = requests.post(url, params=params, data={'magnets[]': magnet_link}, timeout=30)
        elif ext == '.torrent':
            url = 'https://api.alldebrid.com/v4/magnet/upload/file'
            with open(file_path, 'rb') as f:
                response = requests.post(url, params=params, files={'files[]': f}, timeout=30)
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
            response = requests.post(url, headers=headers, data={'magnet': magnet_link}, timeout=30)
        elif ext == '.torrent':
            with open(file_path, 'rb') as f:
                response = requests.post(url, headers=headers, files={'file': f}, timeout=30)
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
        response = requests.get(url, headers=headers, timeout=30)
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
        response = requests.get(url, params=params, timeout=30)
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
        response = requests.get(url, headers=headers, params=params, timeout=30)
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

    def _create_symlinks(self, release_name, category, mount_path):
        """Create symlinks in the completed directory for media files.

        Symlink targets use BLACKHOLE_SYMLINK_TARGET_BASE so they resolve
        correctly on the Sonarr/Radarr host.

        Returns the number of symlinks created.
        """
        completed_release_dir = os.path.join(self.completed_dir, release_name)
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

    # ── Symlink cleanup ──────────────────────────────────────────────

    def _cleanup_symlinks(self):
        """Remove broken symlinks and aged-out directories from the completed dir."""
        if not self.symlink_enabled or not self.completed_dir:
            return
        if not os.path.exists(self.completed_dir):
            return

        now = time.time()
        max_age_secs = self.symlink_max_age * 3600

        for entry in os.listdir(self.completed_dir):
            entry_path = os.path.join(self.completed_dir, entry)
            if not os.path.isdir(entry_path):
                continue

            # Remove broken symlinks within this release dir.
            # Symlinks point to SYMLINK_TARGET_BASE which only exists in
            # Sonarr/Radarr's container — translate to the rclone mount
            # before checking existence.
            rclone_real = os.path.realpath(self.rclone_mount)
            target_base_real = ''
            if self.symlink_target_base:
                target_base_real = os.path.realpath(self.symlink_target_base) + '/'
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
                    logger.info(f"[blackhole] Cleaned up completed dir: {entry}")
                except Exception as e:
                    logger.debug(f"[blackhole] Failed to clean up {entry}: {e}")

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
        """Save pending monitor entries to disk."""
        try:
            with open(self._pending_file, 'w') as f:
                json.dump(entries, f)
        except IOError as e:
            logger.debug(f"[blackhole] Could not write pending monitors: {e}")

    def _add_pending(self, torrent_id, filename):
        """Add a torrent to the pending monitors file."""
        with self._monitors_lock:
            entries = self._load_pending()
            if any(e['torrent_id'] == torrent_id for e in entries):
                return
            entries.append({
                'torrent_id': torrent_id,
                'filename': filename,
                'service': self.debrid_service,
                'timestamp': time.time(),
            })
            self._save_pending(entries)

    def _remove_pending(self, torrent_id):
        """Remove a torrent from the pending monitors file."""
        with self._monitors_lock:
            entries = self._load_pending()
            entries = [e for e in entries if e['torrent_id'] != torrent_id]
            self._save_pending(entries)
            self._active_monitors.discard(torrent_id)

    # ── Monitor orchestration ────────────────────────────────────────

    def _start_monitor(self, torrent_id, filename):
        """Spawn a background thread to monitor a torrent and create symlinks."""
        with self._monitors_lock:
            if torrent_id in self._active_monitors:
                logger.debug(f"[blackhole] Already monitoring torrent {torrent_id}")
                return
            self._active_monitors.add(torrent_id)

        self._add_pending(torrent_id, filename)
        t = threading.Thread(
            target=self._monitor_and_symlink,
            args=(torrent_id, filename),
            daemon=True,
        )
        t.start()
        logger.info(f"[blackhole] Monitoring torrent {torrent_id} for {filename}")

    def _monitor_and_symlink(self, torrent_id, filename):
        """Background thread: poll debrid status, wait for mount, create symlinks.

        This method runs in its own thread and must not block the main scan loop.
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
                break

            if self._is_terminal_error(status):
                logger.error(f"[blackhole] Torrent {torrent_id} hit terminal error: {status}")
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
            count = self._create_symlinks(matched_name, category, mount_path)
            if count > 0:
                logger.info(f"[blackhole] Created {count} symlink(s) for {release_name}")
                try:
                    from utils.metrics import metrics
                    metrics.inc('blackhole_symlink_created')
                except Exception:
                    pass
                if _notify:
                    _notify('download_complete', 'Blackhole: Symlinks Created',
                            f'{count} symlink(s) created for {release_name}')
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
        """Resume monitoring for any torrents that were pending before a restart."""
        entries = self._load_pending()
        if not entries:
            return

        logger.info(f"[blackhole] Resuming {len(entries)} pending torrent monitor(s)")
        for entry in entries:
            torrent_id = entry.get('torrent_id')
            filename = entry.get('filename', 'unknown')
            if torrent_id:
                self._start_monitor(torrent_id, filename)

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
        """
        if not self.dedup_enabled:
            return False

        name, season, is_tv = parse_release_name(filename)
        if not name:
            return False

        name_norm = self._normalize_name(name)

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

    def _try_alternative_release(self, filename, file_path, debrid_handler):
        """On debrid rejection, query Sonarr/Radarr for an alternative release.

        Parses the episode/movie info from the filename, fetches available
        releases, filters to a different info hash, and tries them until
        one succeeds or all are exhausted.

        Runs in a background thread. On failure, moves the original file
        to the failed/ directory (same as the normal failure path).
        """
        alt_ok = False
        try:
            from utils.arr_client import SonarrClient, RadarrClient

            name, season, is_tv = parse_release_name(filename)
            if not name:
                logger.debug(f"[blackhole] Cannot parse release name for alt-retry: {filename}")
            elif is_tv and season is not None and _parse_episodes(filename):
                alt_ok = self._try_alt_episode(name, season, _parse_episodes(filename),
                                               debrid_handler, filename, file_path)
            elif not is_tv:
                alt_ok = self._try_alt_movie(name, debrid_handler, filename, file_path)
            else:
                logger.debug(f"[blackhole] Cannot determine content type for alt-retry: {filename}")
        except Exception as e:
            logger.error(f"[blackhole] Error during alternative release search: {e}")

        if not alt_ok and os.path.exists(file_path):
            # No alternative worked — move to failed/ and mark alts exhausted
            # so retries don't repeat the same alt-release search
            error_dir = os.path.join(self.watch_dir, 'failed')
            os.makedirs(error_dir, exist_ok=True)
            dest = os.path.join(error_dir, filename)
            if os.path.exists(dest):
                base, fext = os.path.splitext(filename)
                dest = os.path.join(error_dir, f"{base}_{int(time.time())}{fext}")
            try:
                os.rename(file_path, dest)
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

    def _try_alt_episode(self, series_name, season, episodes, debrid_handler, orig_filename, orig_path):
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

        return self._try_releases(releases, debrid_handler, orig_filename, orig_path)

    def _try_alt_movie(self, movie_name, debrid_handler, orig_filename, orig_path):
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

        return self._try_releases(releases, debrid_handler, orig_filename, orig_path)

    def _try_releases(self, releases, debrid_handler, orig_filename, orig_path):
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
                            self._start_monitor(torrent_id, orig_filename)
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
        """Extract info hash from a .magnet file or .torrent filename."""
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
        return None

    # ── File processing ──────────────────────────────────────────────

    def _process_file(self, file_path):
        """Process a single torrent/magnet file."""
        filename = os.path.basename(file_path)
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
                try:
                    os.remove(file_path)
                except OSError as e:
                    logger.warning(f"[blackhole] Could not remove {filename}: {e}")
                try:
                    from utils.metrics import metrics
                    metrics.inc('blackhole_processed', {'status': 'success'})
                except Exception:
                    pass

                # Start symlink monitoring if enabled
                if self.symlink_enabled:
                    torrent_id = self._extract_torrent_id(result)
                    if torrent_id:
                        self._start_monitor(torrent_id, filename)
                    else:
                        logger.warning(f"[blackhole] Could not extract torrent ID for symlink monitoring: {filename}")

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
                    staging_dir = os.path.join(self.watch_dir, '.alt_pending')
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
                            args=(filename, staged_path, handler),
                            daemon=True,
                            name=f'alt-retry-{filename[:30]}',
                        ).start()
                        return  # Alt-retry thread handles cleanup

                error_dir = os.path.join(self.watch_dir, 'failed')
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
        """Scan failed/ directory and retry eligible files."""
        failed_dir = os.path.join(self.watch_dir, 'failed')
        if not os.path.exists(failed_dir):
            return

        for filename in os.listdir(failed_dir):
            file_path = os.path.join(failed_dir, filename)
            if not os.path.isfile(file_path):
                continue

            ext = os.path.splitext(filename)[1].lower()
            if ext == '.meta' or ext not in self.SUPPORTED_EXTENSIONS:
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

            logger.info(f"[blackhole] Retrying failed file: {filename} (attempt {retries + 1}/{MAX_RETRIES})")
            try:
                from utils.metrics import metrics
                metrics.inc('blackhole_retry')
            except Exception:
                pass

            # Move back to watch dir for reprocessing
            retry_path = os.path.join(self.watch_dir, filename)
            try:
                RetryMeta.remove(file_path)
                os.rename(file_path, retry_path)
            except OSError as e:
                logger.error(f"[blackhole] Failed to move {filename} for retry: {e}")

    def _scan(self):
        """Scan watch directory for new files."""
        if not os.path.exists(self.watch_dir):
            return

        now = time.time()
        watch_realpath = os.path.realpath(self.watch_dir)

        for filename in os.listdir(self.watch_dir):
            file_path = os.path.join(self.watch_dir, filename)

            # Guard against symlink escapes
            real_path = os.path.realpath(file_path)
            if not real_path.startswith(watch_realpath + os.sep) and real_path != watch_realpath:
                continue

            if not os.path.isfile(file_path):
                continue

            # Skip files still being written (modified within last 2 seconds)
            try:
                if now - os.path.getmtime(file_path) < 2.0:
                    continue
            except OSError:
                continue

            ext = os.path.splitext(filename)[1].lower()
            if ext in self.SUPPORTED_EXTENSIONS:
                self._process_file(file_path)

    def _recover_alt_pending(self):
        """On startup, move stranded .alt_pending files to failed/.

        If the container was killed while an alt-retry thread was running,
        files in .alt_pending/ would be orphaned with no recovery path.
        """
        staging_dir = os.path.join(self.watch_dir, '.alt_pending')
        if not os.path.isdir(staging_dir):
            return
        error_dir = os.path.join(self.watch_dir, 'failed')
        for filename in os.listdir(staging_dir):
            src = os.path.join(staging_dir, filename)
            if not os.path.isfile(src):
                continue
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
                logger.warning(f"[blackhole] Recovered stranded alt-pending file: {filename}")
            except OSError as e:
                logger.warning(f"[blackhole] Could not recover {filename} from alt_pending: {e}")

    def run(self):
        """Main loop - scan at poll_interval."""
        logger.info(f"[blackhole] Watching {self.watch_dir} (poll: {self.poll_interval}s, service: {self.debrid_service})")
        self._recover_alt_pending()
        if self.symlink_enabled:
            logger.info(f"[blackhole] Symlink mode enabled: completed={self.completed_dir}, "
                        f"mount={self.rclone_mount}, target_base={self.symlink_target_base}, "
                        f"timeout={self.mount_poll_timeout}s, interval={self.mount_poll_interval}s, "
                        f"max_age={self.symlink_max_age}h")
            self._resume_pending_monitors()

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
