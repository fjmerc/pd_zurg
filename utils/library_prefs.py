"""Library preference store and file removal operations.

Persists per-show preferences (prefer-local / prefer-debrid) in
/config/library_prefs.json.  Provides synchronous file removal (local copies)
with path-traversal protection.
"""

import json
import os
import threading

from utils.file_utils import atomic_write
from utils.logger import get_logger

logger = get_logger()

PREFS_PATH = '/config/library_prefs.json'
VALID_PREFERENCES = {'prefer-local', 'prefer-debrid', 'none'}

_prefs_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Preference CRUD
# ---------------------------------------------------------------------------

def load_preferences():
    """Read preferences from disk. Returns empty dict on error."""
    try:
        with open(PREFS_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def save_preferences(prefs):
    """Write preferences dict to disk atomically."""
    os.makedirs(os.path.dirname(PREFS_PATH), exist_ok=True)
    with atomic_write(PREFS_PATH) as f:
        json.dump(prefs, f, indent=2)


def set_preference(normalized_title, preference):
    """Set or clear a show preference. Thread-safe.

    Returns dict with status and current preference.
    Raises ValueError for invalid preference values.
    """
    if preference not in VALID_PREFERENCES:
        raise ValueError(f"Invalid preference: {preference!r}")

    with _prefs_lock:
        prefs = load_preferences()
        if preference == 'none':
            prefs.pop(normalized_title, None)
        else:
            prefs[normalized_title] = preference
        save_preferences(prefs)
        return {'status': 'saved', 'preference': preference}


def get_all_preferences():
    """Return all preferences. Alias for load_preferences."""
    return load_preferences()


# ---------------------------------------------------------------------------
# File removal (local copies)
# ---------------------------------------------------------------------------

def remove_local_episodes(episodes, local_tv_path):
    """Remove local episode files. Synchronous.

    Args:
        episodes: list of dicts with key 'path' (absolute local file path)
        local_tv_path: root local TV path — all paths must be under this

    Returns dict with status, count removed, and any errors.
    """
    real_root = os.path.realpath(local_tv_path)
    removed = 0
    errors = []

    for ep in episodes:
        path = ep.get('path', '')
        if not path:
            continue
        real_path = os.path.realpath(path)
        if not real_path.startswith(real_root + os.sep) and real_path != real_root:
            errors.append(f"Path outside local library: {path}")
            continue
        try:
            if os.path.isfile(real_path):
                os.remove(real_path)
                logger.info(f"[library_prefs] Removed: {real_path}")
                removed += 1
                _cleanup_empty_dirs(real_path, real_root)
            else:
                errors.append(f"Not a file: {path}")
        except OSError as e:
            logger.error(f"[library_prefs] Remove failed: {path}: {e}")
            errors.append(str(e))

    return {'status': 'removed', 'removed': removed, 'errors': errors}


def _cleanup_empty_dirs(deleted_file_path, stop_at):
    """Remove empty parent directories up to stop_at after file deletion."""
    parent = os.path.dirname(deleted_file_path)
    while parent and parent != stop_at and parent.startswith(stop_at):
        try:
            if not os.listdir(parent):
                os.rmdir(parent)
                logger.debug(f"[library_prefs] Removed empty dir: {parent}")
                parent = os.path.dirname(parent)
            else:
                break
        except OSError:
            break
