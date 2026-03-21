"""Blackhole watch folder for .torrent and .magnet files.

Monitors a directory for torrent/magnet files, submits them to the
configured debrid service, and removes the file after processing.
Compatible with Sonarr/Radarr blackhole download client configuration.
"""

import os
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


class BlackholeWatcher:
    SUPPORTED_EXTENSIONS = {'.torrent', '.magnet'}

    def __init__(self, watch_dir, debrid_api_key, debrid_service='realdebrid', poll_interval=5):
        self.watch_dir = watch_dir
        self.debrid_api_key = debrid_api_key
        self.debrid_service = debrid_service
        self.poll_interval = poll_interval
        self._stop_event = threading.Event()

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
            if select_resp.status_code not in (200, 204):
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

    def _process_file(self, file_path):
        """Process a single torrent/magnet file."""
        filename = os.path.basename(file_path)
        logger.info(f"[blackhole] Processing: {filename}")

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
                if _notify:
                    _notify('download_complete', 'Blackhole: Torrent Added',
                            f'{filename} added to {self.debrid_service}')
            else:
                logger.error(f"[blackhole] Failed to add {filename}: {result}")
                error_dir = os.path.join(self.watch_dir, 'failed')
                os.makedirs(error_dir, exist_ok=True)
                dest = os.path.join(error_dir, filename)
                if os.path.exists(dest):
                    base, fext = os.path.splitext(filename)
                    dest = os.path.join(error_dir, f"{base}_{int(time.time())}{fext}")
                os.rename(file_path, dest)
        except Exception as e:
            logger.error(f"[blackhole] Error processing {filename}: {e}")

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

    def run(self):
        """Main loop - scan at poll_interval."""
        logger.info(f"[blackhole] Watching {self.watch_dir} (poll: {self.poll_interval}s, service: {self.debrid_service})")
        while not self._stop_event.is_set():
            try:
                self._scan()
            except Exception as e:
                logger.error(f"[blackhole] Scan error: {e}")
            self._stop_event.wait(self.poll_interval)

    def stop(self):
        self._stop_event.set()


def setup():
    """Initialize and start the blackhole watcher if enabled."""
    global _watcher
    from base import RDAPIKEY, ADAPIKEY

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

    _watcher = BlackholeWatcher(watch_dir, debrid_api_key, debrid_service, poll_interval)
    thread = threading.Thread(target=_watcher.run, daemon=True)
    thread.start()
    return _watcher


def stop():
    """Stop the blackhole watcher if running."""
    if _watcher:
        _watcher.stop()
