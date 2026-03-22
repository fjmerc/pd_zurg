"""Watch settings.json for external changes and sync back to .env.

The plex_debrid interactive menu (and manual edits) write directly to
settings.json without updating .env.  On container restart pd_setup()
would overwrite those changes with stale .env values.

This watcher polls settings.json every 30 seconds.  When it detects a
change, it reads the file and syncs the relevant values back to .env
via _sync_plex_debrid_to_env().  The sync function is a no-op when
values already match, so duplicate triggers (e.g. after a WebUI save
that already synced) are harmless.
"""

import os
import threading
import time
from utils.logger import get_logger

logger = get_logger()

SETTINGS_JSON = '/config/settings.json'
_POLL_INTERVAL = 30  # seconds
_stop_event = threading.Event()
_thread = None


def _get_mtime():
    try:
        return os.path.getmtime(SETTINGS_JSON)
    except OSError:
        return 0


def _watch_loop():
    last_mtime = _get_mtime()

    while not _stop_event.is_set():
        _stop_event.wait(_POLL_INTERVAL)
        if _stop_event.is_set():
            break

        current_mtime = _get_mtime()
        if current_mtime == last_mtime:
            continue
        last_mtime = current_mtime

        # Brief debounce — the file may still be mid-write
        time.sleep(1)

        try:
            from utils.settings_api import _sync_plex_debrid_to_env, read_plex_debrid_values
            values = read_plex_debrid_values()
            if values:
                _sync_plex_debrid_to_env(values)
        except Exception as e:
            logger.error(f"[settings-watcher] Sync failed: {e}")


def start():
    """Start the settings.json watcher as a daemon thread."""
    global _thread
    if _thread and _thread.is_alive():
        return
    _stop_event.clear()
    _thread = threading.Thread(target=_watch_loop, daemon=True, name="settings-watcher")
    _thread.start()
    logger.info("[settings-watcher] Watching settings.json for external changes")


def stop():
    """Stop the watcher."""
    _stop_event.set()
    if _thread:
        _thread.join(timeout=5)
