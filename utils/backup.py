"""Config backup & restore.

Creates tar.gz archives bundling the user's config state (.env,
settings.json, library_prefs.json, blocklist.json) and restores them
atomically with pre-restore snapshots for rollback.

Archive layout (flat, no subdirs):
    manifest.json   {version, created_at, zurgarr_version, files}
    env
    settings.json          (optional)
    library_prefs.json     (optional)
    blocklist.json         (optional)

Two read paths share the same core validation + apply logic:
  - Upload restore: bytes → restore_from_blob()
  - Saved backup:   filename → restore_from_saved()

Restore is intentionally a lower-level operation than the Settings
save flow: it trusts the archive is internally self-consistent (both
.env and settings.json came from the same snapshot) and does not call
``_sync_plex_debrid_to_env`` to reconcile them.
"""

import gzip
import io
import json
import os
import re
import shutil
import tarfile
import threading
from datetime import datetime, timezone
from pathlib import Path

from utils.file_utils import atomic_write
from utils.logger import get_logger
from version import VERSION

logger = get_logger()

BACKUP_VERSION = 1

DEFAULT_CONFIG_DIR = '/config'
DEFAULT_BACKUP_DIR = '/config/backups'

# Size caps — backups are tiny; anything bigger is suspicious.  The
# decompressed cap guards against gzip-bomb archives whose 10 MiB
# compressed blob expands to gigabytes of zeros during header scan.
MAX_ARCHIVE_BYTES             = 10 * 1024 * 1024
MAX_DECOMPRESSED_ARCHIVE_BYTES = 50 * 1024 * 1024
_MAX_MEMBER_BYTES             =  5 * 1024 * 1024

# Process-wide serialization for restores.  Without this, two concurrent
# restores racing through snapshot dir creation or file apply could
# interleave and leave a mixed-era config on disk.
_restore_lock = threading.Lock()

# Archive member name → relative-to-config-dir target path.
# Order determines apply order.  'env' is written last so the SIGHUP
# config reload fires only after the other in-process caches have
# already consumed the restored on-disk files.
_BACKUP_FILES = [
    ('settings.json',       'settings.json'),
    ('library_prefs.json',  'library_prefs.json'),
    ('blocklist.json',      'blocklist.json'),
    ('env',                 '.env'),
]
_ALLOWED_MEMBERS = {'manifest.json'} | {name for name, _ in _BACKUP_FILES}

# Strict filename pattern for saved backups.  Matches both the base shape
# produced by ``_build_archive_bytes`` and the collision-suffixed form
# ``…-<counter>.tar.gz`` that ``create_backup_file`` falls back to when
# two backups land in the same second.  No path separators, no traversal.
BACKUP_FILENAME_RE = re.compile(
    r'^zurgarr-backup-[A-Za-z0-9._+-]+-\d{8}-\d{6}(?:-\d+)?\.tar\.gz$'
)


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def _build_manifest(included_files):
    return {
        'version': BACKUP_VERSION,
        'created_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'zurgarr_version': VERSION,
        'files': sorted(included_files),
    }


def _add_bytes_to_tar(tar, name, data, mtime):
    info = tarfile.TarInfo(name=name)
    info.size = len(data)
    info.mode = 0o600
    info.mtime = mtime
    info.type = tarfile.REGTYPE
    tar.addfile(info, io.BytesIO(data))


def _build_archive_bytes(config_dir):
    """Return (filename, blob_bytes) for a new in-memory backup archive."""
    now = datetime.now()
    stamp = now.strftime('%Y%m%d-%H%M%S')
    filename = f'zurgarr-backup-{VERSION}-{stamp}.tar.gz'
    # Sanity check: the filename we generate must match the consumer regex.
    # If VERSION ever gains a character outside the regex, fail loud here
    # rather than silently producing unrestorable archives.
    if not BACKUP_FILENAME_RE.match(filename):
        raise ValueError(
            f'Generated backup filename does not match BACKUP_FILENAME_RE: {filename}'
        )

    included = []
    buf = io.BytesIO()
    mtime = int(now.timestamp())

    # Collect live file bytes first so the manifest reflects what was
    # actually added.
    with tarfile.open(fileobj=buf, mode='w:gz') as tar:
        for member_name, rel_path in _BACKUP_FILES:
            src = os.path.join(config_dir, rel_path)
            if not os.path.isfile(src):
                continue
            with open(src, 'rb') as f:
                data = f.read()
            if len(data) > _MAX_MEMBER_BYTES:
                logger.warning(
                    f'[backup] Skipping {rel_path} — size {len(data)} exceeds '
                    f'per-member cap {_MAX_MEMBER_BYTES}'
                )
                continue
            _add_bytes_to_tar(tar, member_name, data, mtime)
            included.append(member_name)

        manifest = _build_manifest(included)
        manifest_bytes = json.dumps(manifest, indent=2).encode('utf-8')
        _add_bytes_to_tar(tar, 'manifest.json', manifest_bytes, mtime)

    return filename, buf.getvalue()


def create_backup_blob(config_dir=DEFAULT_CONFIG_DIR):
    """Build a tar.gz backup in memory.

    Returns:
        (filename, blob_bytes) — filename is the suggested download name,
        blob_bytes is the tar.gz content.
    """
    return _build_archive_bytes(config_dir)


def create_backup_file(config_dir=DEFAULT_CONFIG_DIR, backup_dir=DEFAULT_BACKUP_DIR):
    """Build a tar.gz backup and write it to disk atomically.

    If the target filename already exists (two backups triggered within
    the same second), append ``-<n>`` to avoid silent overwrite.  The
    collision-suffixed form is still accepted by ``BACKUP_FILENAME_RE``.

    Returns:
        Path to the created archive.
    """
    filename, blob = _build_archive_bytes(config_dir)
    # Archive content is sensitive (.env with API keys).  Restrict dir and
    # file permissions so a co-tenant with read access to /config can't
    # harvest secrets from the backup pile.
    os.makedirs(backup_dir, mode=0o700, exist_ok=True)
    try:
        os.chmod(backup_dir, 0o700)
    except OSError:
        pass
    target = os.path.join(backup_dir, filename)
    if os.path.exists(target):
        stem = filename[:-len('.tar.gz')]
        suffix = 1
        while True:
            candidate_name = f'{stem}-{suffix}.tar.gz'
            candidate = os.path.join(backup_dir, candidate_name)
            if not os.path.exists(candidate):
                filename = candidate_name
                target = candidate
                break
            suffix += 1
    with atomic_write(target, mode='wb') as f:
        f.write(blob)
    try:
        os.chmod(target, 0o600)
    except OSError:
        pass
    logger.info(f'[backup] Wrote {target} ({len(blob)} bytes)')
    return Path(target)


# ---------------------------------------------------------------------------
# List / prune
# ---------------------------------------------------------------------------

def list_backups(backup_dir=DEFAULT_BACKUP_DIR):
    """List saved backup archives, newest first.

    Returns:
        list of {'name', 'size', 'created_at'} dicts.  Created-at is the
        filesystem mtime (ISO 8601 UTC).  Non-matching files and
        pre-restore snapshot directories are ignored.
    """
    if not os.path.isdir(backup_dir):
        return []
    entries = []
    for name in os.listdir(backup_dir):
        if not BACKUP_FILENAME_RE.match(name):
            continue
        full = os.path.join(backup_dir, name)
        if not os.path.isfile(full):
            continue
        try:
            st = os.stat(full)
        except OSError:
            continue
        entries.append({
            'name': name,
            'size': st.st_size,
            'created_at': datetime.fromtimestamp(st.st_mtime, tz=timezone.utc)
                                  .strftime('%Y-%m-%dT%H:%M:%SZ'),
        })
    entries.sort(key=lambda e: e['created_at'], reverse=True)
    return entries


def prune_old_backups(backup_dir=DEFAULT_BACKUP_DIR, keep=7):
    """Delete all but the ``keep`` most recent archive files.

    Returns the count of files pruned.  Operates only on files matching
    BACKUP_FILENAME_RE — pre-restore snapshot dirs and unrelated files
    are never touched.
    """
    keep = max(1, int(keep))
    backups = list_backups(backup_dir)
    to_delete = backups[keep:]
    pruned = 0
    for entry in to_delete:
        full = os.path.join(backup_dir, entry['name'])
        try:
            os.unlink(full)
            pruned += 1
            logger.debug(f'[backup] Pruned old backup: {entry["name"]}')
        except OSError as exc:
            logger.warning(f'[backup] Failed to prune {entry["name"]}: {exc}')
    return pruned


# ---------------------------------------------------------------------------
# Restore
# ---------------------------------------------------------------------------

class RestoreError(Exception):
    """Raised on a validation failure before any mutation occurs."""


def _validate_member(member):
    """Reject anything other than a whitelisted regular file."""
    if not member.isfile():
        raise RestoreError(f'Non-regular archive member: {member.name}')
    if member.name not in _ALLOWED_MEMBERS:
        raise RestoreError(f'Unknown archive member: {member.name}')
    if member.size > _MAX_MEMBER_BYTES:
        raise RestoreError(
            f'Archive member {member.name} exceeds per-member size cap '
            f'({member.size} > {_MAX_MEMBER_BYTES})'
        )
    if member.islnk() or member.issym() or member.isdev():
        raise RestoreError(f'Disallowed member type: {member.name}')
    # Defence in depth beyond the whitelist: names in the whitelist are
    # already simple, but if tarfile ever yields a crafted name that
    # whitelist-matches post-normalization, refuse anything with path
    # separators or parent traversal.
    if '/' in member.name or '\\' in member.name or '..' in member.name.split('/'):
        raise RestoreError(f'Disallowed path in archive: {member.name}')


def _read_member(tar, name):
    try:
        return tar.extractfile(name).read()
    except Exception as exc:
        raise RestoreError(f'Failed to read archive member {name}: {exc}')


def _parse_and_validate(tar):
    """Validate archive shape and parse content; return dict of name→bytes.

    Rejects on: missing/unparseable manifest, version mismatch, bad
    member type/size/name, content that won't parse in its expected
    format.
    """
    members = tar.getmembers()
    for m in members:
        _validate_member(m)

    names = {m.name for m in members}
    if 'manifest.json' not in names:
        raise RestoreError('Archive is missing manifest.json')

    manifest_bytes = _read_member(tar, 'manifest.json')
    try:
        manifest = json.loads(manifest_bytes.decode('utf-8'))
    except (ValueError, UnicodeDecodeError) as exc:
        raise RestoreError(f'manifest.json is not valid JSON: {exc}')
    if not isinstance(manifest, dict):
        raise RestoreError('manifest.json must be a JSON object')
    if manifest.get('version') != BACKUP_VERSION:
        raise RestoreError(
            f'Unsupported backup version {manifest.get("version")!r} '
            f'(expected {BACKUP_VERSION})'
        )

    content = {}
    warnings = []

    for member_name, _rel in _BACKUP_FILES:
        if member_name not in names:
            continue
        data = _read_member(tar, member_name)
        # Per-format validation: parse-only, not semantic.
        if member_name == 'env':
            # Lines are either comments, blank, or KEY=value.  Reject if
            # any line (ignoring comments/blank) lacks '='.
            for raw in data.decode('utf-8', errors='replace').splitlines():
                line = raw.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' not in line:
                    raise RestoreError(f'env contains invalid line: {raw!r}')
        elif member_name == 'settings.json':
            try:
                parsed = json.loads(data.decode('utf-8'))
            except (ValueError, UnicodeDecodeError) as exc:
                raise RestoreError(f'settings.json is not valid JSON: {exc}')
            if not isinstance(parsed, dict):
                raise RestoreError('settings.json must be a JSON object')
        elif member_name == 'library_prefs.json':
            try:
                parsed = json.loads(data.decode('utf-8'))
            except (ValueError, UnicodeDecodeError) as exc:
                raise RestoreError(f'library_prefs.json is not valid JSON: {exc}')
            if not isinstance(parsed, dict):
                raise RestoreError('library_prefs.json must be a JSON object')
        elif member_name == 'blocklist.json':
            try:
                parsed = json.loads(data.decode('utf-8'))
            except (ValueError, UnicodeDecodeError) as exc:
                raise RestoreError(f'blocklist.json is not valid JSON: {exc}')
            if not isinstance(parsed, (dict, list)):
                raise RestoreError('blocklist.json must be a JSON object or array')
        content[member_name] = data

    # Informational: backup taken by a different zurgarr version.  Don't
    # block; formats are stable within BACKUP_VERSION=1.
    manifest_ver = manifest.get('zurgarr_version')
    if manifest_ver and manifest_ver != VERSION:
        warnings.append(
            f'Backup was taken with zurgarr {manifest_ver}; '
            f'current is {VERSION}'
        )

    return content, warnings


def _snapshot_current(targets, backup_dir):
    """Copy currently-live config files into a fresh pre-restore dir.

    ``targets`` is an iterable of absolute paths.  Missing files are
    skipped (nothing to roll back to).  Returns the snapshot dir path.

    Uses ``mkdir(exist_ok=False)`` with a counter to dodge TOCTOU —
    two restores racing within the same second can't both win the
    same snapshot path.
    """
    stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    suffix = 0
    while True:
        name = f'pre-restore-{stamp}' if suffix == 0 else f'pre-restore-{stamp}-{suffix}'
        snapshot_dir = os.path.join(backup_dir, name)
        try:
            os.mkdir(snapshot_dir, mode=0o700)
            break
        except FileExistsError:
            suffix += 1
    for src in targets:
        if not os.path.isfile(src):
            continue
        dst = os.path.join(snapshot_dir, os.path.basename(src))
        shutil.copy2(src, dst)
        try:
            os.chmod(dst, 0o600)
        except OSError:
            pass
    return snapshot_dir


def _apply(content, config_dir, applied_out):
    """Write each validated file atomically.

    Appends each successfully-written rel path to ``applied_out`` as it
    lands, so the caller's rollback knows exactly which files were
    touched even if this function raises mid-way.
    """
    for member_name, rel_path in _BACKUP_FILES:
        if member_name not in content:
            continue
        target = os.path.join(config_dir, rel_path)
        os.makedirs(os.path.dirname(target) or '.', exist_ok=True)
        with atomic_write(target, mode='wb') as f:
            f.write(content[member_name])
        applied_out.append(rel_path)


def _rollback(snapshot_dir, applied_rel_paths, config_dir):
    """Copy snapshot files back over live paths. Best-effort; logs errors."""
    for rel_path in applied_rel_paths:
        snap = os.path.join(snapshot_dir, os.path.basename(rel_path))
        target = os.path.join(config_dir, rel_path)
        try:
            if os.path.isfile(snap):
                shutil.copy2(snap, target)
            else:
                # No snapshot file → this path was absent before restore;
                # remove the partially-applied version so we return to
                # the original "missing" state.
                if os.path.isfile(target):
                    os.unlink(target)
        except OSError as exc:
            logger.error(f'[backup] Rollback failed for {rel_path}: {exc}')


def _reload_services(restored_rel_paths):
    """Refresh in-process caches + kick SIGHUP / plex_debrid restart.

    Called only on successful apply.  Best-effort — reload failures are
    logged but do not fail the restore response; the files are already
    on disk.
    """
    if '.env' in restored_rel_paths:
        try:
            import signal as _signal
            os.kill(os.getpid(), _signal.SIGHUP)
            logger.info('[backup] Sent SIGHUP for env reload')
        except OSError as exc:
            logger.warning(f'[backup] SIGHUP failed: {exc}')

    if 'blocklist.json' in restored_rel_paths:
        try:
            from utils import blocklist
            blocklist.init(DEFAULT_CONFIG_DIR)
            logger.info('[backup] Reloaded blocklist')
        except Exception as exc:
            logger.warning(f'[backup] Blocklist reload failed: {exc}')

    if 'settings.json' in restored_rel_paths:
        try:
            import threading
            from utils.processes import restart_service
            threading.Thread(
                target=restart_service, args=('plex_debrid',), daemon=True
            ).start()
            logger.info('[backup] Triggered plex_debrid restart')
        except Exception as exc:
            logger.warning(f'[backup] plex_debrid restart trigger failed: {exc}')


def _bounded_gunzip(blob, limit):
    """Decompress ``blob`` as gzip, aborting if output exceeds ``limit``.

    Reads in 64 KiB chunks and tracks cumulative output size.  Raises
    ``RestoreError`` if the decompressed stream exceeds ``limit`` — the
    primary defence against gzip bombs whose compressed blob fits under
    the upload cap but expands to gigabytes during tarfile header scan.
    """
    chunk_size = 64 * 1024
    out = io.BytesIO()
    total = 0
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(blob), mode='rb') as gz:
            while True:
                chunk = gz.read(chunk_size)
                if not chunk:
                    break
                total += len(chunk)
                if total > limit:
                    raise RestoreError(
                        f'Archive decompresses to more than {limit} bytes '
                        f'— refusing (possible gzip bomb)'
                    )
                out.write(chunk)
    except (OSError, EOFError) as exc:
        raise RestoreError(f'Not a valid gzip stream: {exc}')
    return out.getvalue()


def _restore_core(blob, config_dir, backup_dir):
    """Shared restore core for both upload and saved-backup flows."""
    if len(blob) > MAX_ARCHIVE_BYTES:
        raise RestoreError(
            f'Archive exceeds size cap ({len(blob)} > {MAX_ARCHIVE_BYTES})'
        )

    # Bound decompressed size BEFORE opening as tarfile so a gzip bomb
    # can't be walked by getmembers() and force gigabytes of RAM/CPU.
    raw = _bounded_gunzip(blob, MAX_DECOMPRESSED_ARCHIVE_BYTES)

    try:
        # Open in plain uncompressed mode — decompression already done.
        tar = tarfile.open(fileobj=io.BytesIO(raw), mode='r:')
    except tarfile.TarError as exc:
        raise RestoreError(f'Not a valid tar archive: {exc}')

    try:
        content, warnings = _parse_and_validate(tar)
    finally:
        tar.close()

    if not content:
        raise RestoreError('Archive contains no restorable files')

    # Serialize apply across concurrent restores and between restore +
    # any other config writer that holds this lock (extension point).
    with _restore_lock:
        snapshot_targets = [
            os.path.join(config_dir, rel)
            for name, rel in _BACKUP_FILES if name in content
        ]

        os.makedirs(backup_dir, mode=0o700, exist_ok=True)
        snapshot_dir = _snapshot_current(snapshot_targets, backup_dir)

        applied = []
        try:
            _apply(content, config_dir, applied)
        except Exception as exc:
            logger.error(f'[backup] Apply failed — rolling back: {exc}')
            _rollback(snapshot_dir, list(applied), config_dir)
            raise RestoreError(f'Apply failed: {exc}')

        _reload_services(applied)

    return {
        'status': 'success',
        'restored': applied,
        'snapshot_dir': snapshot_dir,
        'warnings': warnings,
    }


def restore_from_blob(blob, config_dir=DEFAULT_CONFIG_DIR, backup_dir=DEFAULT_BACKUP_DIR):
    """Restore from an in-memory archive (uploaded by the user)."""
    return _restore_core(blob, config_dir, backup_dir)


def resolve_backup_path(filename, backup_dir=DEFAULT_BACKUP_DIR):
    """Validate ``filename`` and return its resolved Path inside ``backup_dir``.

    Shared by both the saved-backup download endpoint and
    ``restore_from_saved`` so the path-traversal guard lives in one
    place.  Checks:
      1. Match BACKUP_FILENAME_RE exactly (no path separators, fixed shape).
      2. The resolved path must be a direct child of the resolved backup_dir
         (defence in depth — rejects any regex bypass corner case).
      3. The file must actually exist.

    Raises RestoreError on any check failure.
    """
    if not isinstance(filename, str) or not BACKUP_FILENAME_RE.match(filename):
        raise RestoreError(f'Invalid backup filename: {filename!r}')
    backup_root = Path(backup_dir).resolve()
    candidate = (backup_root / filename).resolve()
    try:
        candidate.relative_to(backup_root)
    except ValueError:
        raise RestoreError(f'Backup path escapes backup dir: {filename!r}')
    if candidate.parent != backup_root:
        raise RestoreError(f'Backup path not a direct child of backup dir: {filename!r}')
    if not candidate.is_file():
        raise RestoreError(f'Backup not found: {filename}')
    return candidate


def restore_from_saved(filename, config_dir=DEFAULT_CONFIG_DIR, backup_dir=DEFAULT_BACKUP_DIR):
    """Restore from a saved archive file under ``backup_dir``."""
    candidate = resolve_backup_path(filename, backup_dir=backup_dir)
    with open(candidate, 'rb') as f:
        blob = f.read()
    return _restore_core(blob, config_dir, backup_dir)
