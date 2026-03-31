"""Blocklist for debrid torrents by info hash.

Allows users to permanently reject specific torrents so they are skipped
during blackhole processing and library symlink creation. Entries persist
across restarts in a JSON file under /config.
"""

import json
import os
import re
import threading
import unicodedata
import uuid
from datetime import datetime, timezone
from utils.file_utils import atomic_write
from utils.logger import get_logger

logger = get_logger()

# Module-level state
_file_path = None
_lock = threading.Lock()
_entries = {}       # id -> entry dict
_hash_index = {}    # uppercase info_hash -> id  (O(1) lookup)
_title_index = {}   # normalized title -> id


def init(config_dir='/config'):
    """Initialize the blocklist module. Call once at startup."""
    global _file_path
    with _lock:
        _file_path = os.path.join(config_dir, 'blocklist.json')
        _load()
    logger.info(f"[blocklist] Initialized — {_file_path} ({len(_entries)} entries)")


def add(info_hash, title, reason='', source='manual'):
    """Add a torrent to the blocklist.

    Args:
        info_hash: Torrent info hash (will be uppercased)
        title: Human-readable torrent/release title
        reason: Why it was blocked (e.g. 'wrong content', 'virus')
        source: 'manual' or 'auto'

    Returns:
        Entry ID (uuid string), or existing entry ID if already blocked.
    """
    if _file_path is None:
        return None

    info_hash = (info_hash or '').strip().upper()
    if not info_hash:
        return None

    with _lock:
        # Deduplicate by info_hash
        existing_id = _hash_index.get(info_hash)
        if existing_id:
            return existing_id

        entry_id = str(uuid.uuid4())
        entry = {
            'id': entry_id,
            'info_hash': info_hash,
            'title': title or '',
            'reason': reason or '',
            'date': datetime.now(timezone.utc).isoformat(timespec='seconds'),
            'source': source,
        }

        _entries[entry_id] = entry
        _hash_index[info_hash] = entry_id
        norm = _norm_title(title)
        if norm:
            _title_index[norm] = entry_id

        _save_unlocked()
        logger.info(f"[blocklist] Added: {title} ({info_hash[:16]}...) reason={reason} source={source}")
        return entry_id


def remove(entry_id):
    """Remove a blocklist entry by ID.

    Returns:
        True if removed, False if not found.
    """
    if _file_path is None:
        return False

    with _lock:
        entry = _entries.pop(entry_id, None)
        if not entry:
            return False

        h = entry.get('info_hash', '')
        if h and _hash_index.get(h) == entry_id:
            del _hash_index[h]

        norm = _norm_title(entry.get('title', ''))
        if norm and _title_index.get(norm) == entry_id:
            # Check if another entry shares the same normalized title
            replacement = None
            for eid, e in _entries.items():
                if _norm_title(e.get('title', '')) == norm:
                    replacement = eid
                    break
            if replacement:
                _title_index[norm] = replacement
            else:
                del _title_index[norm]

        _save_unlocked()
        logger.info(f"[blocklist] Removed: {entry.get('title', '')} ({h[:16]}...)")
        return True


def clear():
    """Remove all blocklist entries."""
    if _file_path is None:
        return

    with _lock:
        count = len(_entries)
        _entries.clear()
        _hash_index.clear()
        _title_index.clear()
        _save_unlocked()
        logger.info(f"[blocklist] Cleared {count} entries")


def is_blocked(info_hash):
    """Check if an info hash is blocklisted. O(1) lookup."""
    if not info_hash:
        return False
    return info_hash.strip().upper() in _hash_index


def is_blocked_title(title):
    """Check if a title/folder name matches a blocklisted entry.

    Uses normalized matching (lowercase, strip punctuation, collapse whitespace)
    as a secondary lookup when info_hash is unavailable.
    """
    if not title:
        return False
    norm = _norm_title(title)
    return norm in _title_index if norm else False


def get_all():
    """Return all blocklist entries sorted by date descending."""
    with _lock:
        entries = list(_entries.values())
    entries.sort(key=lambda e: e.get('date', ''), reverse=True)
    return entries


def _norm_title(title):
    """Normalize a title for fuzzy matching.

    Mirrors library._norm_for_matching: lowercase, transliterate unicode,
    convert & to 'and', replace dots/hyphens/underscores with spaces,
    strip remaining punctuation, collapse whitespace.
    """
    if not title:
        return ''
    t = title.lower()
    # Transliterate unicode to ASCII (e.g., e -> e, n -> n)
    t = unicodedata.normalize('NFKD', t).encode('ascii', 'ignore').decode('ascii')
    # Normalize common symbols to words before stripping
    t = t.replace('&', ' and ')
    # Replace word-separating punctuation with spaces (release name separators)
    t = re.sub(r'[._\-]', ' ', t)
    # Remove remaining punctuation, keep alphanumeric and spaces
    t = re.sub(r'[^a-z0-9\s]', '', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


def _load():
    """Load entries from the JSON file into memory. NOT thread-safe — caller must ensure safety."""
    global _entries, _hash_index, _title_index
    _entries = {}
    _hash_index = {}
    _title_index = {}

    if not _file_path or not os.path.isfile(_file_path):
        return

    try:
        with open(_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.error(f"[blocklist] Failed to load {_file_path}: {e}")
        return

    if not isinstance(data, list):
        logger.warning("[blocklist] Invalid blocklist format, expected list")
        return

    for entry in data:
        if not isinstance(entry, dict):
            continue
        entry_id = entry.get('id')
        if not entry_id:
            continue
        _entries[entry_id] = entry
        h = (entry.get('info_hash') or '').upper()
        if h:
            _hash_index[h] = entry_id
        norm = _norm_title(entry.get('title', ''))
        if norm:
            _title_index[norm] = entry_id


def _save_unlocked():
    """Persist entries to disk. Caller must hold _lock."""
    if not _file_path:
        return
    try:
        entries = list(_entries.values())
        with atomic_write(_file_path) as f:
            json.dump(entries, f, indent=2)
    except OSError as e:
        logger.error(f"[blocklist] Failed to save: {e}")
