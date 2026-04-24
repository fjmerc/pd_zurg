"""Tests for utils.backup — config backup/restore."""

import io
import json
import os
import tarfile
import time

import pytest

from utils import backup


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    mode = 'wb' if isinstance(data, bytes) else 'w'
    with open(path, mode) as f:
        f.write(data)


def _populated_config(tmp_dir):
    """Create a fake /config with all four backup files."""
    cfg = os.path.join(tmp_dir, 'config')
    _write(os.path.join(cfg, '.env'), 'FOO=bar\nBAZ=qux\n')
    _write(os.path.join(cfg, 'settings.json'), json.dumps({'k': 1}))
    _write(os.path.join(cfg, 'library_prefs.json'), json.dumps({'show': 'prefer-local'}))
    _write(os.path.join(cfg, 'blocklist.json'), json.dumps([{'hash': 'DEAD', 'title': 'x'}]))
    return cfg


def _minimal_config(tmp_dir):
    """Create a fake /config with only .env."""
    cfg = os.path.join(tmp_dir, 'config')
    _write(os.path.join(cfg, '.env'), 'ONLY=env\n')
    return cfg


def _build_archive(members, include_manifest=True, manifest_override=None):
    """Build an in-memory tar.gz containing the given name->bytes members."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode='w:gz') as tar:
        mtime = int(time.time())
        for name, data in members.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            info.mode = 0o600
            info.mtime = mtime
            tar.addfile(info, io.BytesIO(data))
        if include_manifest and 'manifest.json' not in members:
            if manifest_override is not None:
                m = manifest_override
            else:
                m = {
                    'version': backup.BACKUP_VERSION,
                    'created_at': '2026-01-01T00:00:00Z',
                    'zurgarr_version': 'test',
                    'files': sorted(n for n in members.keys()),
                }
            mb = json.dumps(m).encode()
            info = tarfile.TarInfo(name='manifest.json')
            info.size = len(mb)
            info.mode = 0o600
            info.mtime = mtime
            tar.addfile(info, io.BytesIO(mb))
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def test_create_backup_blob_contains_all_present_files(tmp_dir):
    cfg = _populated_config(tmp_dir)
    filename, blob = backup.create_backup_blob(config_dir=cfg)

    assert backup.BACKUP_FILENAME_RE.match(filename)
    with tarfile.open(fileobj=io.BytesIO(blob), mode='r:gz') as tar:
        names = set(tar.getnames())
    assert names == {'manifest.json', 'env', 'settings.json',
                     'library_prefs.json', 'blocklist.json'}


def test_create_backup_blob_skips_missing_files(tmp_dir):
    cfg = _minimal_config(tmp_dir)
    _filename, blob = backup.create_backup_blob(config_dir=cfg)

    with tarfile.open(fileobj=io.BytesIO(blob), mode='r:gz') as tar:
        names = set(tar.getnames())
    assert names == {'manifest.json', 'env'}


def test_create_backup_blob_manifest_fields(tmp_dir):
    cfg = _populated_config(tmp_dir)
    _filename, blob = backup.create_backup_blob(config_dir=cfg)
    with tarfile.open(fileobj=io.BytesIO(blob), mode='r:gz') as tar:
        m = json.loads(tar.extractfile('manifest.json').read())
    assert m['version'] == backup.BACKUP_VERSION
    assert set(m['files']) == {'env', 'settings.json', 'library_prefs.json', 'blocklist.json'}
    assert m['created_at'].endswith('Z')
    assert m['zurgarr_version']


def test_create_backup_file_writes_to_disk(tmp_dir):
    cfg = _populated_config(tmp_dir)
    bdir = os.path.join(tmp_dir, 'backups')
    path = backup.create_backup_file(config_dir=cfg, backup_dir=bdir)
    assert path.exists()
    assert backup.BACKUP_FILENAME_RE.match(path.name)


# ---------------------------------------------------------------------------
# List & prune
# ---------------------------------------------------------------------------

def test_list_backups_returns_sorted_newest_first(tmp_dir):
    cfg = _populated_config(tmp_dir)
    bdir = os.path.join(tmp_dir, 'backups')
    p1 = backup.create_backup_file(config_dir=cfg, backup_dir=bdir)
    # Force a different mtime — filenames include seconds, may collide under
    # parallel invocation.
    os.utime(p1, (time.time() - 60, time.time() - 60))
    p2 = backup.create_backup_file(config_dir=cfg, backup_dir=bdir)
    os.utime(p2, (time.time(), time.time()))
    names = [e['name'] for e in backup.list_backups(bdir)]
    assert names == [p2.name, p1.name]


def test_list_backups_ignores_non_archive_files(tmp_dir):
    bdir = os.path.join(tmp_dir, 'backups')
    os.makedirs(bdir)
    # A pre-restore snapshot dir (must not appear in listing).
    os.makedirs(os.path.join(bdir, 'pre-restore-20260101-000000'))
    # An unrelated file.
    _write(os.path.join(bdir, 'notes.txt'), 'hi')
    # A file whose name almost matches but has a bad suffix.
    _write(os.path.join(bdir, 'zurgarr-backup-x-20260101-000000.tar'), b'')
    assert backup.list_backups(bdir) == []


def test_prune_old_backups_keeps_n_newest(tmp_dir):
    cfg = _populated_config(tmp_dir)
    bdir = os.path.join(tmp_dir, 'backups')
    paths = []
    for i in range(5):
        p = backup.create_backup_file(config_dir=cfg, backup_dir=bdir)
        # Spread mtimes so ordering is deterministic regardless of filename
        # collision from identical timestamps within the same second.
        os.utime(p, (time.time() - (100 - i), time.time() - (100 - i)))
        paths.append(p)
    pruned = backup.prune_old_backups(bdir, keep=2)
    assert pruned == 3
    remaining = {e['name'] for e in backup.list_backups(bdir)}
    # Newest two survive (paths[3], paths[4] — highest mtimes).
    assert remaining == {paths[3].name, paths[4].name}


def test_prune_old_backups_noop_when_under_limit(tmp_dir):
    cfg = _populated_config(tmp_dir)
    bdir = os.path.join(tmp_dir, 'backups')
    backup.create_backup_file(config_dir=cfg, backup_dir=bdir)
    assert backup.prune_old_backups(bdir, keep=7) == 0


def test_prune_old_backups_clamps_keep_to_at_least_one(tmp_dir):
    cfg = _populated_config(tmp_dir)
    bdir = os.path.join(tmp_dir, 'backups')
    # Create two, ask for keep=0 → should clamp to 1.
    p1 = backup.create_backup_file(config_dir=cfg, backup_dir=bdir)
    os.utime(p1, (time.time() - 60, time.time() - 60))
    p2 = backup.create_backup_file(config_dir=cfg, backup_dir=bdir)
    os.utime(p2, (time.time(), time.time()))
    pruned = backup.prune_old_backups(bdir, keep=0)
    assert pruned == 1
    remaining = {e['name'] for e in backup.list_backups(bdir)}
    assert remaining == {p2.name}


# ---------------------------------------------------------------------------
# Restore — round trip
# ---------------------------------------------------------------------------

def test_restore_round_trip(tmp_dir):
    cfg = _populated_config(tmp_dir)
    bdir = os.path.join(tmp_dir, 'backups')
    _filename, blob = backup.create_backup_blob(config_dir=cfg)

    # Modify files to confirm restore actually reverts them.
    _write(os.path.join(cfg, '.env'), 'MODIFIED=1\n')
    _write(os.path.join(cfg, 'settings.json'), '{"modified": true}')

    # Disable reload side effects — the real SIGHUP path would try to kick
    # an event loop that doesn't exist in the test process.
    result = _restore_without_reload(blob, cfg, bdir)

    assert result['status'] == 'success'
    assert set(result['restored']) == {'.env', 'settings.json',
                                        'library_prefs.json', 'blocklist.json'}
    with open(os.path.join(cfg, '.env')) as f:
        assert f.read() == 'FOO=bar\nBAZ=qux\n'
    with open(os.path.join(cfg, 'settings.json')) as f:
        assert json.load(f) == {'k': 1}


def test_restore_snapshots_existing_files(tmp_dir):
    cfg = _populated_config(tmp_dir)
    bdir = os.path.join(tmp_dir, 'backups')
    _filename, blob = backup.create_backup_blob(config_dir=cfg)
    # Overwrite with a sentinel so we can prove the snapshot captured the
    # pre-restore content.
    _write(os.path.join(cfg, '.env'), 'SENTINEL=before-restore\n')

    result = _restore_without_reload(blob, cfg, bdir)

    snap_dir = result['snapshot_dir']
    assert os.path.isdir(snap_dir)
    with open(os.path.join(snap_dir, '.env')) as f:
        assert f.read() == 'SENTINEL=before-restore\n'


# ---------------------------------------------------------------------------
# Restore — validation failures (no mutation)
# ---------------------------------------------------------------------------

def test_restore_rejects_missing_manifest(tmp_dir):
    cfg = _populated_config(tmp_dir)
    bdir = os.path.join(tmp_dir, 'backups')
    blob = _build_archive({'env': b'X=1\n'}, include_manifest=False)
    with pytest.raises(backup.RestoreError, match='manifest.json'):
        _restore_without_reload(blob, cfg, bdir)
    # Ensure nothing was snapshotted.
    assert not os.path.exists(bdir) or not os.listdir(bdir)


def test_restore_rejects_bad_manifest_version(tmp_dir):
    cfg = _populated_config(tmp_dir)
    bdir = os.path.join(tmp_dir, 'backups')
    blob = _build_archive(
        {'env': b'X=1\n'},
        manifest_override={'version': 99, 'files': ['env']},
    )
    with pytest.raises(backup.RestoreError, match='version'):
        _restore_without_reload(blob, cfg, bdir)


def test_restore_rejects_unknown_member(tmp_dir):
    cfg = _populated_config(tmp_dir)
    bdir = os.path.join(tmp_dir, 'backups')
    blob = _build_archive({'env': b'X=1\n', 'malicious.sh': b'rm -rf /'})
    with pytest.raises(backup.RestoreError, match='Unknown archive member'):
        _restore_without_reload(blob, cfg, bdir)


def test_restore_rejects_path_traversal(tmp_dir):
    cfg = _populated_config(tmp_dir)
    bdir = os.path.join(tmp_dir, 'backups')
    blob = _build_archive({'env': b'X=1\n', '../etc/passwd': b'root:x:0:'})
    with pytest.raises(backup.RestoreError):
        _restore_without_reload(blob, cfg, bdir)


def test_restore_rejects_symlink_member(tmp_dir):
    cfg = _populated_config(tmp_dir)
    bdir = os.path.join(tmp_dir, 'backups')
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode='w:gz') as tar:
        info = tarfile.TarInfo(name='env')
        info.type = tarfile.SYMTYPE
        info.linkname = '/etc/passwd'
        tar.addfile(info)
        mb = json.dumps({
            'version': backup.BACKUP_VERSION,
            'files': ['env'],
        }).encode()
        m_info = tarfile.TarInfo(name='manifest.json')
        m_info.size = len(mb)
        tar.addfile(m_info, io.BytesIO(mb))
    with pytest.raises(backup.RestoreError):
        _restore_without_reload(buf.getvalue(), cfg, bdir)


def test_restore_rejects_oversize_archive(tmp_dir):
    cfg = _populated_config(tmp_dir)
    bdir = os.path.join(tmp_dir, 'backups')
    # Fabricate a blob bigger than the archive cap — no need to actually
    # make it valid, the cap check runs first.
    oversized = b'x' * (backup.MAX_ARCHIVE_BYTES + 1)
    with pytest.raises(backup.RestoreError, match='size cap'):
        _restore_without_reload(oversized, cfg, bdir)


def test_restore_rejects_invalid_settings_json(tmp_dir):
    cfg = _populated_config(tmp_dir)
    bdir = os.path.join(tmp_dir, 'backups')
    blob = _build_archive({
        'env': b'X=1\n',
        'settings.json': b'not json',
    })
    with pytest.raises(backup.RestoreError, match='settings.json'):
        _restore_without_reload(blob, cfg, bdir)


def test_restore_rejects_gzip_bomb(tmp_dir):
    """Gzip of many zeros must be rejected by the decompressed-size cap
    before ``tarfile.open`` walks the header stream."""
    import gzip as _gzip
    cfg = _populated_config(tmp_dir)
    bdir = os.path.join(tmp_dir, 'backups')
    # Compress a blob that decompresses to > MAX_DECOMPRESSED_ARCHIVE_BYTES.
    # 60 MiB of null bytes compresses to tens of KB — well under the 10 MiB
    # upload cap — so the only defence is the decompressed-size bound.
    raw = b'\x00' * (backup.MAX_DECOMPRESSED_ARCHIVE_BYTES + 1)
    buf = io.BytesIO()
    with _gzip.GzipFile(fileobj=buf, mode='wb') as gz:
        gz.write(raw)
    blob = buf.getvalue()
    assert len(blob) < backup.MAX_ARCHIVE_BYTES  # Compressed blob fits; decompressed doesn't.
    with pytest.raises(backup.RestoreError, match='gzip bomb'):
        _restore_without_reload(blob, cfg, bdir)


def test_restore_rejects_non_gzip_body(tmp_dir):
    cfg = _populated_config(tmp_dir)
    bdir = os.path.join(tmp_dir, 'backups')
    with pytest.raises(backup.RestoreError, match='gzip'):
        _restore_without_reload(b'not a gzip stream at all', cfg, bdir)


def test_resolve_backup_path_accepts_valid(tmp_dir):
    cfg = _populated_config(tmp_dir)
    bdir = os.path.join(tmp_dir, 'backups')
    p = backup.create_backup_file(config_dir=cfg, backup_dir=bdir)
    resolved = backup.resolve_backup_path(p.name, backup_dir=bdir)
    assert resolved.name == p.name


def test_resolve_backup_path_rejects_traversal(tmp_dir):
    bdir = os.path.join(tmp_dir, 'backups')
    os.makedirs(bdir)
    with pytest.raises(backup.RestoreError):
        backup.resolve_backup_path('../etc/passwd', backup_dir=bdir)


def test_created_backup_file_is_mode_0600(tmp_dir):
    cfg = _populated_config(tmp_dir)
    bdir = os.path.join(tmp_dir, 'backups')
    p = backup.create_backup_file(config_dir=cfg, backup_dir=bdir)
    mode = os.stat(p).st_mode & 0o777
    # 0o600 expected, but umask may trim further — assert no world/group bits.
    assert mode & 0o077 == 0, f'backup file has permissive mode {oct(mode)}'


def test_restore_rejects_env_with_no_equals(tmp_dir):
    cfg = _populated_config(tmp_dir)
    bdir = os.path.join(tmp_dir, 'backups')
    blob = _build_archive({'env': b'INVALIDLINE\n'})
    with pytest.raises(backup.RestoreError, match='env'):
        _restore_without_reload(blob, cfg, bdir)


# ---------------------------------------------------------------------------
# Restore-from-saved — filename validation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('bad_name', [
    '../etc/passwd',
    'zurgarr-backup-/../etc-20260101-000000.tar.gz',
    'zurgarr-backup-.tar.gz',
    '.env',
    '',
    'zurgarr-backup-x-bad.tar.gz',  # Wrong timestamp shape
])
def test_restore_from_saved_rejects_bad_filename(tmp_dir, bad_name):
    cfg = _populated_config(tmp_dir)
    bdir = os.path.join(tmp_dir, 'backups')
    os.makedirs(bdir)
    with pytest.raises(backup.RestoreError):
        backup.restore_from_saved(bad_name, config_dir=cfg, backup_dir=bdir)


def test_restore_from_saved_reads_disk(tmp_dir, monkeypatch):
    cfg = _populated_config(tmp_dir)
    bdir = os.path.join(tmp_dir, 'backups')
    path = backup.create_backup_file(config_dir=cfg, backup_dir=bdir)
    # Mutate the live config so the restore is observable.
    _write(os.path.join(cfg, '.env'), 'MUTATED=1\n')
    # Stub reload side effects.
    monkeypatch.setattr(backup, '_reload_services', lambda _: None)

    result = backup.restore_from_saved(path.name, config_dir=cfg, backup_dir=bdir)
    assert result['status'] == 'success'
    with open(os.path.join(cfg, '.env')) as f:
        assert f.read() == 'FOO=bar\nBAZ=qux\n'


# ---------------------------------------------------------------------------
# Rollback
# ---------------------------------------------------------------------------

def test_restore_rollback_on_apply_failure(tmp_dir, monkeypatch):
    cfg = _populated_config(tmp_dir)
    bdir = os.path.join(tmp_dir, 'backups')
    _filename, blob = backup.create_backup_blob(config_dir=cfg)

    # Capture original content for assertion.
    with open(os.path.join(cfg, '.env')) as f:
        original_env = f.read()
    with open(os.path.join(cfg, 'settings.json')) as f:
        original_settings = f.read()

    # Mutate live config so the restore would change things if it succeeded.
    _write(os.path.join(cfg, '.env'), 'AFTER=1\n')
    _write(os.path.join(cfg, 'settings.json'), '{"after": true}')

    # Force the second atomic_write call to raise (first succeeds).
    real_aw = backup.atomic_write
    calls = {'n': 0}

    from contextlib import contextmanager

    @contextmanager
    def flaky(*args, **kwargs):
        calls['n'] += 1
        if calls['n'] == 2:
            raise OSError('simulated disk error')
        with real_aw(*args, **kwargs) as f:
            yield f

    monkeypatch.setattr(backup, 'atomic_write', flaky)
    monkeypatch.setattr(backup, '_reload_services', lambda _: None)

    with pytest.raises(backup.RestoreError, match='Apply failed'):
        backup.restore_from_blob(blob, config_dir=cfg, backup_dir=bdir)

    # After rollback, pre-mutation "AFTER" state should be gone — the
    # rollback restores the snapshot, which was taken *after* the mutation.
    # So live files should match the mutated state again, not the backup.
    with open(os.path.join(cfg, '.env')) as f:
        rolled_back = f.read()
    # Rollback should restore to the snapshot (which captured 'AFTER=1\n').
    assert rolled_back == 'AFTER=1\n'
    # Meanwhile, the backup's original_env content is NOT live anymore.
    assert original_env != rolled_back
    assert original_settings  # (unused otherwise; keeps lint quiet)


# ---------------------------------------------------------------------------
# Helpers that avoid calling the real SIGHUP/blocklist/plex_debrid plumbing
# ---------------------------------------------------------------------------

def _restore_without_reload(blob, cfg, bdir):
    import unittest.mock as _mock
    with _mock.patch.object(backup, '_reload_services', lambda _: None):
        return backup.restore_from_blob(blob, config_dir=cfg, backup_dir=bdir)
