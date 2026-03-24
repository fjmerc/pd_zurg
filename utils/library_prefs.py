"""Library preference store and file management operations.

Persists per-show preferences (prefer-local / prefer-debrid) in
/config/library_prefs.json.  Provides background file copy (debrid → local)
and synchronous file removal (local copies) with path-traversal protection.
"""

import json
import os
import shutil
import threading
import time

from utils.file_utils import atomic_write
from utils.logger import get_logger

logger = get_logger()

PREFS_PATH = '/config/library_prefs.json'
VALID_PREFERENCES = {'prefer-local', 'prefer-debrid', 'none'}

_prefs_lock = threading.Lock()
_transfers = {}
_transfers_lock = threading.Lock()
_transfer_counter = 0


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
# File copy (debrid → local)
# ---------------------------------------------------------------------------

def copy_episodes_to_local(episodes, show_title, local_tv_path):
    """Copy episode files from debrid mount to local library in background.

    Args:
        episodes: list of dicts with keys: season, episode, source_path, filename
        show_title: display title for directory creation
        local_tv_path: root local TV library path

    Returns: transfer ID string for status polling.
    """
    global _transfer_counter

    with _transfers_lock:
        _transfer_counter += 1
        tid = str(_transfer_counter)
        _transfers[tid] = {
            'status': 'running',
            'total': len(episodes),
            'completed': 0,
            'errors': [],
            'started': time.monotonic(),
        }

    real_root = os.path.realpath(local_tv_path)

    def _run():
        completed = 0
        errors = []
        for ep in episodes:
            season_dir = os.path.join(
                local_tv_path, show_title,
                f"Season {ep['season']}",
            )
            real_dir = os.path.realpath(season_dir)
            if not real_dir.startswith(real_root + os.sep) and real_dir != real_root:
                errors.append(f"Path outside local library: {season_dir}")
                continue
            try:
                os.makedirs(season_dir, exist_ok=True)
                dest = os.path.join(season_dir, ep['filename'])
                if os.path.exists(dest):
                    logger.debug(f"[library_prefs] Skip existing: {dest}")
                    completed += 1
                else:
                    shutil.copy2(ep['source_path'], dest)
                    logger.info(f"[library_prefs] Copied {ep['source_path']} -> {dest}")
                    completed += 1
            except (OSError, IOError) as e:
                logger.error(f"[library_prefs] Copy failed: {ep.get('source_path')}: {e}")
                errors.append(str(e))

            with _transfers_lock:
                _transfers[tid]['completed'] = completed
                _transfers[tid]['errors'] = errors

        with _transfers_lock:
            if not completed:
                _transfers[tid]['status'] = 'failed'
            elif errors:
                _transfers[tid]['status'] = 'partial'
            else:
                _transfers[tid]['status'] = 'completed'
            _transfers[tid]['finished'] = time.monotonic()

        # Trigger library re-scan so UI reflects new files
        try:
            from utils.library import get_scanner
            scanner = get_scanner()
            if scanner:
                scanner.refresh()
        except Exception:
            pass

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return tid


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


# ---------------------------------------------------------------------------
# Transfer tracking
# ---------------------------------------------------------------------------

def get_transfer_status(transfer_id=None):
    """Get status of one or all transfers. Prunes completed transfers > 1hr."""
    now = time.monotonic()
    with _transfers_lock:
        # Prune old completed transfers
        expired = [
            tid for tid, t in _transfers.items()
            if t['status'] in ('completed', 'failed')
            and now - t.get('finished', now) > 3600
        ]
        for tid in expired:
            del _transfers[tid]

        if transfer_id:
            t = _transfers.get(transfer_id)
            if t is None:
                return {'status': 'not_found'}
            return dict(t)
        return {tid: dict(t) for tid, t in _transfers.items()}
