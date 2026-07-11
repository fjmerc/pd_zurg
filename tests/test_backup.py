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
    """Create a fake /config with the four core backup files (.env,
    settings, prefs, blocklist). Operator-state stores are added on top
    by ``_operator_config``."""
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
    blob = _build_archive({'env': b'GOOD=1\nSECRETVALUE_NO_EQUALS\n'})
    with pytest.raises(backup.RestoreError, match='env line 2') as exc_info:
        _restore_without_reload(blob, cfg, bdir)
    # The message must cite position only — echoing the line could leak
    # a mistyped secret into the error response and logs.
    assert 'SECRETVALUE' not in str(exc_info.value)


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


# ---------------------------------------------------------------------------
# Snapshot regex / list / delete
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('name', [
    'pre-restore-20260422-183238',
    'pre-restore-20260422-183238-1',
    'pre-restore-20260422-183238-12',
    '20260422_183238',                  # legacy format
    '20260422_183238-2',                # legacy with collision suffix
])
def test_snapshot_dirname_re_accepts_valid(name):
    assert backup.SNAPSHOT_DIRNAME_RE.match(name)


@pytest.mark.parametrize('name', [
    '',
    '../etc',
    'pre-restore-bad',
    'pre-restore-20260422',
    'pre-restore-20260422-183238/',
    '20260422-183238',                  # hyphen instead of underscore
    'random_dir',
    'zurgarr-backup-2.20.0-20260422-183238.tar.gz',  # archive, not snapshot
])
def test_snapshot_dirname_re_rejects_invalid(name):
    assert not backup.SNAPSHOT_DIRNAME_RE.match(name)


def _make_snapshot_dir(backup_dir, name, files=None):
    snap = os.path.join(backup_dir, name)
    os.makedirs(snap, exist_ok=True)
    for fname, contents in (files or {'settings.json': b'{}'}).items():
        _write(os.path.join(snap, fname), contents)
    return snap


def test_list_snapshots_sorted_newest_first(tmp_dir):
    bdir = os.path.join(tmp_dir, 'backups')
    older = _make_snapshot_dir(bdir, 'pre-restore-20260422-183238', {'settings.json': b'{}'})
    newer = _make_snapshot_dir(bdir, '20260423_120000', {'settings.json': b'{}', '.env': b'A=1\n'})
    # Force mtimes so ordering is deterministic regardless of FS resolution.
    os.utime(older, (1000, 1000))
    os.utime(newer, (2000, 2000))

    entries = backup.list_snapshots(backup_dir=bdir)
    names = [e['name'] for e in entries]
    assert names == ['20260423_120000', 'pre-restore-20260422-183238']
    by_name = {e['name']: e for e in entries}
    assert by_name['20260423_120000']['file_count'] == 2
    assert by_name['pre-restore-20260422-183238']['file_count'] == 1
    assert by_name['20260423_120000']['size'] > 0


def test_list_snapshots_skips_archives_and_random(tmp_dir):
    bdir = os.path.join(tmp_dir, 'backups')
    os.makedirs(bdir)
    # An archive file (not a dir) should not appear.
    _write(os.path.join(bdir, 'zurgarr-backup-2.0.0-20260101-000000.tar.gz'), b'x')
    # A dir that doesn't match the regex.
    os.makedirs(os.path.join(bdir, 'random_dir'))
    # A valid snapshot.
    _make_snapshot_dir(bdir, 'pre-restore-20260101-000000')

    entries = backup.list_snapshots(backup_dir=bdir)
    assert [e['name'] for e in entries] == ['pre-restore-20260101-000000']


def test_list_snapshots_skips_symlinked_top_level(tmp_dir):
    bdir = os.path.join(tmp_dir, 'backups')
    os.makedirs(bdir)
    real_target = os.path.join(tmp_dir, 'evil_target')
    os.makedirs(real_target)
    _write(os.path.join(real_target, 'settings.json'), b'{}')
    link_path = os.path.join(bdir, 'pre-restore-20260101-000000')
    os.symlink(real_target, link_path)

    entries = backup.list_snapshots(backup_dir=bdir)
    assert entries == []


def test_delete_backup_removes_archive(tmp_dir):
    cfg = _populated_config(tmp_dir)
    bdir = os.path.join(tmp_dir, 'backups')
    p = backup.create_backup_file(config_dir=cfg, backup_dir=bdir)
    assert p.exists()

    backup.delete_backup(p.name, backup_dir=bdir)
    assert not p.exists()


@pytest.mark.parametrize('bad_name', [
    '../etc/passwd',
    '.env',
    'pre-restore-20260101-000000',  # snapshot dirname, not archive
    '',
])
def test_delete_backup_rejects_bad_filename(tmp_dir, bad_name):
    bdir = os.path.join(tmp_dir, 'backups')
    os.makedirs(bdir)
    with pytest.raises(backup.RestoreError):
        backup.delete_backup(bad_name, backup_dir=bdir)


def test_delete_backup_missing_file_404(tmp_dir):
    bdir = os.path.join(tmp_dir, 'backups')
    os.makedirs(bdir)
    name = 'zurgarr-backup-2.0.0-20260101-000000.tar.gz'
    with pytest.raises(backup.RestoreError, match='not found'):
        backup.delete_backup(name, backup_dir=bdir)


def test_delete_snapshot_removes_dir(tmp_dir):
    bdir = os.path.join(tmp_dir, 'backups')
    snap = _make_snapshot_dir(bdir, 'pre-restore-20260101-000000', {
        'settings.json': b'{}',
        '.env': b'A=1\n',
    })
    assert os.path.isdir(snap)

    backup.delete_snapshot('pre-restore-20260101-000000', backup_dir=bdir)
    assert not os.path.exists(snap)


def test_delete_snapshot_legacy_dirname(tmp_dir):
    bdir = os.path.join(tmp_dir, 'backups')
    snap = _make_snapshot_dir(bdir, '20260422_183238')
    backup.delete_snapshot('20260422_183238', backup_dir=bdir)
    assert not os.path.exists(snap)


@pytest.mark.parametrize('bad_name', [
    '../etc',
    'random_dir',
    'zurgarr-backup-2.0.0-20260101-000000.tar.gz',  # archive name
    '',
])
def test_delete_snapshot_rejects_bad_dirname(tmp_dir, bad_name):
    bdir = os.path.join(tmp_dir, 'backups')
    os.makedirs(bdir)
    with pytest.raises(backup.RestoreError):
        backup.delete_snapshot(bad_name, backup_dir=bdir)


def test_delete_snapshot_rejects_symlink(tmp_dir):
    bdir = os.path.join(tmp_dir, 'backups')
    os.makedirs(bdir)
    real_target = os.path.join(tmp_dir, 'real_target')
    os.makedirs(real_target)
    _write(os.path.join(real_target, 'settings.json'), b'{}')
    link_name = 'pre-restore-20260101-000000'
    os.symlink(real_target, os.path.join(bdir, link_name))

    with pytest.raises(backup.RestoreError, match='symlink|escapes|not found'):
        backup.delete_snapshot(link_name, backup_dir=bdir)
    # Real target must still exist — we refused to delete via the symlink.
    assert os.path.isdir(real_target)
    assert os.path.isfile(os.path.join(real_target, 'settings.json'))


def test_delete_snapshot_missing_dir_not_found(tmp_dir):
    bdir = os.path.join(tmp_dir, 'backups')
    os.makedirs(bdir)
    with pytest.raises(backup.RestoreError, match='not found'):
        backup.delete_snapshot('pre-restore-20260101-000000', backup_dir=bdir)


# ---------------------------------------------------------------------------
# Operator-state members (grab_attempts, history, debrid_health, ...)
# ---------------------------------------------------------------------------

_OPERATOR_STATE_FILES = {
    'library_pending.json': json.dumps({'show': {'direction': 'to-debrid'}}),
    'grab_attempts.json': json.dumps({'fg:show:s1': {'count': 2}}),
    'recovery_snapshots.json': json.dumps({'version': 1, 'snapshots': []}),
    'debrid_health.json': json.dumps({'version': 1, 'probed': {}}),
    'wanted_memos.json': json.dumps({'version': 1, 'saved_at': 0, 'memos': {}}),
    'history.jsonl': '{"type": "grabbed", "title": "x"}\n'
                     '{"type": "cached", "title": "y"}\n',
}


def _operator_config(tmp_dir):
    """_populated_config plus every operator-state store."""
    cfg = _populated_config(tmp_dir)
    for name, content in _OPERATOR_STATE_FILES.items():
        _write(os.path.join(cfg, name), content)
    return cfg


def test_create_backup_blob_includes_operator_state(tmp_dir):
    cfg = _operator_config(tmp_dir)
    _filename, blob = backup.create_backup_blob(config_dir=cfg)
    with tarfile.open(fileobj=io.BytesIO(blob), mode='r:gz') as tar:
        names = set(tar.getnames())
    assert set(_OPERATOR_STATE_FILES) <= names


def test_history_jsonl_gets_raised_member_cap(tmp_dir):
    """history.jsonl over the 5M default cap is still archived; a same-size
    member without an override is skipped."""
    cfg = _minimal_config(tmp_dir)
    line = '{"type": "grabbed", "title": "' + 'x' * 80 + '"}\n'
    big = line * (backup._MAX_MEMBER_BYTES // len(line) + 10)
    assert len(big) > backup._MAX_MEMBER_BYTES
    _write(os.path.join(cfg, 'history.jsonl'), big)
    _write(os.path.join(cfg, 'settings.json'), '{"pad": "' + 'x' * len(big) + '"}')

    _filename, blob = backup.create_backup_blob(config_dir=cfg)
    with tarfile.open(fileobj=io.BytesIO(blob), mode='r:gz') as tar:
        names = set(tar.getnames())
    assert 'history.jsonl' in names
    assert 'settings.json' not in names


def test_restore_applies_operator_state(tmp_dir):
    members = {name: content.encode()
               for name, content in _OPERATOR_STATE_FILES.items()}
    members['env'] = b'A=1\n'
    blob = _build_archive(members)
    cfg = os.path.join(tmp_dir, 'config')
    os.makedirs(cfg)
    bdir = os.path.join(tmp_dir, 'backups')

    result = _restore_without_reload(blob, cfg, bdir)

    assert set(_OPERATOR_STATE_FILES) <= set(result['restored'])
    for name, content in _OPERATOR_STATE_FILES.items():
        with open(os.path.join(cfg, name)) as f:
            assert f.read() == content


def test_restore_rejects_invalid_history_line(tmp_dir):
    blob = _build_archive({
        'history.jsonl': b'{"ok": 1}\nnot-json\n',
        'env': b'A=1\n',
    })
    cfg = os.path.join(tmp_dir, 'config')
    with pytest.raises(backup.RestoreError, match='history.jsonl line 2'):
        _restore_without_reload(blob, cfg, os.path.join(tmp_dir, 'backups'))


def test_restore_rejects_non_object_history_line(tmp_dir):
    blob = _build_archive({
        'history.jsonl': b'[1, 2, 3]\n',
        'env': b'A=1\n',
    })
    cfg = os.path.join(tmp_dir, 'config')
    with pytest.raises(backup.RestoreError, match='must be a JSON object'):
        _restore_without_reload(blob, cfg, os.path.join(tmp_dir, 'backups'))


@pytest.mark.parametrize('member', sorted(
    n for n in backup._JSON_OBJECT_MEMBERS))
def test_restore_rejects_non_object_json_member(tmp_dir, member):
    blob = _build_archive({
        member: b'[1, 2]',
        'env': b'A=1\n',
    })
    cfg = os.path.join(tmp_dir, 'config')
    with pytest.raises(backup.RestoreError, match='must be a JSON object'):
        _restore_without_reload(blob, cfg, os.path.join(tmp_dir, 'backups'))


def test_restore_rejects_history_over_raised_cap(tmp_dir):
    data = b'x' * (backup._MEMBER_SIZE_CAPS['history.jsonl'] + 1)
    blob = _build_archive({'history.jsonl': data, 'env': b'A=1\n'})
    cfg = os.path.join(tmp_dir, 'config')
    with pytest.raises(backup.RestoreError, match='per-member size cap'):
        _restore_without_reload(blob, cfg, os.path.join(tmp_dir, 'backups'))


@pytest.mark.parametrize('member', sorted(
    n for n in backup._JSON_OBJECT_MEMBERS))
def test_restore_rejects_unparseable_json_member(tmp_dir, member):
    blob = _build_archive({
        member: b'not json',
        'env': b'A=1\n',
    })
    cfg = os.path.join(tmp_dir, 'config')
    with pytest.raises(backup.RestoreError, match='not valid JSON'):
        _restore_without_reload(blob, cfg, os.path.join(tmp_dir, 'backups'))


def test_apply_dispatches_to_owner_appliers(tmp_dir, monkeypatch):
    """When restoring to the real config dir, stateful members must go
    through their owner module's locked restore function; settings.json,
    wanted_memos.json and env fall through to plain atomic writes."""
    from utils import (attempt_ledger, blocklist, debrid_health, history,
                       library_prefs, recovery)
    cfg = os.path.join(tmp_dir, 'config')
    os.makedirs(cfg)
    monkeypatch.setattr(backup, 'DEFAULT_CONFIG_DIR', cfg)

    calls = {}
    monkeypatch.setattr(library_prefs, 'restore_prefs_bytes',
                        lambda d: calls.__setitem__('prefs', d))
    monkeypatch.setattr(library_prefs, 'restore_pending_bytes',
                        lambda d: calls.__setitem__('pending', d))
    monkeypatch.setattr(blocklist, 'restore_bytes',
                        lambda d: calls.__setitem__('blocklist', d))
    monkeypatch.setattr(attempt_ledger, 'restore_bytes',
                        lambda d: calls.__setitem__('ledger', d))
    monkeypatch.setattr(recovery, 'restore_bytes',
                        lambda d: calls.__setitem__('recovery', d))
    monkeypatch.setattr(debrid_health, 'restore_state_bytes',
                        lambda d: calls.__setitem__('health', d))
    monkeypatch.setattr(history, 'restore_bytes',
                        lambda d: calls.__setitem__('history', d))

    content = {name: f'payload:{name}'.encode()
               for name, _ in backup._BACKUP_FILES}
    applied = []
    backup._apply(content, cfg, applied)

    assert set(calls) == {'prefs', 'pending', 'blocklist', 'ledger',
                          'recovery', 'health', 'history'}
    assert calls['health'] == b'payload:debrid_health.json'
    # Non-owner members took the plain write path.
    for plain in ('settings.json', 'wanted_memos.json', '.env'):
        assert os.path.isfile(os.path.join(cfg, plain))
    assert len(applied) == len(backup._BACKUP_FILES)


def test_apply_skips_owner_appliers_for_other_config_dirs(tmp_dir, monkeypatch):
    """Owner modules have hardcoded /config paths — restores to any other
    dir (tests, dry runs) must use plain atomic writes."""
    from utils import attempt_ledger

    def _boom(_data):
        raise AssertionError('owner applier must not be used')

    monkeypatch.setattr(attempt_ledger, 'restore_bytes', _boom)
    cfg = os.path.join(tmp_dir, 'config')
    os.makedirs(cfg)
    applied = []
    backup._apply({'grab_attempts.json': b'{"a": 1}'}, cfg, applied)
    with open(os.path.join(cfg, 'grab_attempts.json')) as f:
        assert f.read() == '{"a": 1}'
    assert applied == ['grab_attempts.json']


def test_reload_services_fires_wanted_memo_merge(monkeypatch):
    from unittest.mock import MagicMock
    import utils.library as library

    scanner = MagicMock()
    monkeypatch.setattr(library, 'get_scanner', lambda: scanner)

    backup._reload_services(['wanted_memos.json'])

    scanner.reload_wanted_memos.assert_called_once()


def test_reload_hooks_tolerate_missing_scanner(monkeypatch):
    import utils.library as library
    monkeypatch.setattr(library, 'get_scanner', lambda: None)
    # Must not raise — no scanner just means nothing to refresh.
    backup._reload_services(['wanted_memos.json'])
