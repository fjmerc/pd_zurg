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
# Pending transitions
# ---------------------------------------------------------------------------

PENDING_PATH = '/config/library_pending.json'
_pending_lock = threading.Lock()


def _load_pending():
    try:
        with open(PENDING_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def _save_pending(pending):
    os.makedirs(os.path.dirname(PENDING_PATH), exist_ok=True)
    with atomic_write(PENDING_PATH) as f:
        json.dump(pending, f, indent=2)


def set_pending(normalized_title, episodes, direction='to-debrid'):
    """Record episodes as pending transition. Thread-safe.

    Args:
        normalized_title: Normalized show/movie title
        episodes: list of {season, episode} dicts
        direction: 'to-debrid' or 'to-local'
    """
    with _pending_lock:
        pending = _load_pending()
        entry = pending.get(normalized_title, {})
        entry['direction'] = direction
        existing = entry.get('episodes', [])
        existing_keys = {(e['season'], e['episode']) for e in existing}
        for ep in episodes:
            key = (ep['season'], ep['episode'])
            if key not in existing_keys:
                existing.append(ep)
        entry['episodes'] = existing
        pending[normalized_title] = entry
        _save_pending(pending)


def clear_pending(normalized_title, episodes=None):
    """Clear pending episodes for a title. Thread-safe.

    If episodes is None, clears all pending for that title.
    Otherwise removes only the specified episodes.
    """
    with _pending_lock:
        pending = _load_pending()
        if normalized_title not in pending:
            return
        if episodes is None:
            del pending[normalized_title]
        else:
            clear_keys = {(e['season'], e['episode']) for e in episodes}
            existing = pending[normalized_title].get('episodes', [])
            remaining = [e for e in existing if (e['season'], e['episode']) not in clear_keys]
            if remaining:
                pending[normalized_title]['episodes'] = remaining
            else:
                del pending[normalized_title]
        _save_pending(pending)


def get_all_pending():
    """Return all pending transitions."""
    return _load_pending()


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


def replace_local_with_symlinks(episodes, local_tv_path, rclone_mount, symlink_target_base):
    """Replace local episode files with symlinks to the debrid mount.

    For each episode, deletes the local file and creates a symlink at the
    same path pointing to the debrid mount file (translated to the Sonarr
    namespace via symlink_target_base).

    Args:
        episodes: list of dicts with 'local_path' and 'debrid_path' keys
        local_tv_path: root local TV path — local paths must be under this
        rclone_mount: mount path inside pd_zurg (e.g., /data/pd_zurg)
        symlink_target_base: mount path from Sonarr's perspective (e.g., /mnt/debrid)

    Returns dict with status, count switched, and any errors.
    """
    real_root = os.path.realpath(local_tv_path)
    switched = 0
    errors = []

    for ep in episodes:
        local_path = ep.get('local_path', '')
        debrid_path = ep.get('debrid_path', '')
        if not local_path or not debrid_path:
            continue

        # Validate local path is under the local library root
        real_local = os.path.realpath(local_path)
        if not real_local.startswith(real_root + os.sep) and real_local != real_root:
            errors.append(f"Path outside local library: {local_path}")
            continue

        # Translate debrid path from pd_zurg namespace to Sonarr namespace
        if not debrid_path.startswith(rclone_mount):
            errors.append(f"Debrid path not under rclone mount: {debrid_path}")
            continue
        symlink_target = symlink_target_base + debrid_path[len(rclone_mount):]

        try:
            if not os.path.isfile(real_local):
                errors.append(f"Local file not found: {local_path}")
                continue

            # Delete local file
            os.remove(real_local)
            logger.info(f"[library_prefs] Removed local: {real_local}")

            # Create symlink at the same path pointing to debrid mount
            os.symlink(symlink_target, real_local)
            logger.info(f"[library_prefs] Symlinked: {real_local} -> {symlink_target}")
            switched += 1

        except OSError as e:
            logger.error(f"[library_prefs] Symlink switch failed: {local_path}: {e}")
            errors.append(str(e))

    return {'status': 'switched' if switched > 0 else 'error', 'switched': switched, 'errors': errors}


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
