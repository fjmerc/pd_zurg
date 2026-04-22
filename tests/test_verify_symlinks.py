"""Tests for verify_symlinks in utils/scheduled_tasks.py."""

import os
import time
from unittest.mock import MagicMock, patch
import pytest


@pytest.fixture
def symlink_env(tmp_dir, monkeypatch):
    """Set up directories and env vars for verify_symlinks tests."""
    completed = os.path.join(tmp_dir, 'completed')
    local_tv = os.path.join(tmp_dir, 'tv')
    local_movies = os.path.join(tmp_dir, 'movies')
    mount = os.path.join(tmp_dir, 'mount')
    target_base = os.path.join(tmp_dir, 'mnt_debrid')

    for d in (completed, local_tv, local_movies, mount, target_base):
        os.makedirs(d, exist_ok=True)
    # Mount must have a non-empty category dir so the health check passes
    os.makedirs(os.path.join(mount, 'movies', '_placeholder'), exist_ok=True)

    monkeypatch.setenv('BLACKHOLE_COMPLETED_DIR', completed)
    monkeypatch.setenv('BLACKHOLE_LOCAL_LIBRARY_TV', local_tv)
    monkeypatch.setenv('BLACKHOLE_LOCAL_LIBRARY_MOVIES', local_movies)
    monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', mount)
    monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', target_base)

    return {
        'completed': completed,
        'local_tv': local_tv,
        'local_movies': local_movies,
        'mount': mount,
        'target_base': target_base,
    }


def _make_symlink(directory, name, target):
    """Create a symlink at directory/name -> target."""
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, name)
    os.symlink(target, path)
    return path


class TestVerifySymlinks:

    def test_removes_broken_mount_symlink(self, symlink_env):
        """Broken symlinks pointing to rclone mount are removed."""
        from utils.scheduled_tasks import verify_symlinks
        link = _make_symlink(
            symlink_env['completed'], 'ep.mkv',
            os.path.join(symlink_env['mount'], 'shows', 'gone', 'ep.mkv'),
        )
        assert os.path.islink(link)

        result = verify_symlinks()
        assert result['items'] == 1
        assert not os.path.exists(link)

    def test_removes_broken_target_base_symlink(self, symlink_env):
        """Broken symlinks pointing to SYMLINK_TARGET_BASE are removed."""
        from utils.scheduled_tasks import verify_symlinks
        show_dir = os.path.join(symlink_env['local_tv'], 'Outlander', 'Season 07')
        link = _make_symlink(
            show_dir, 'S07E01.mkv',
            os.path.join(symlink_env['target_base'], 'shows', 'gone', 'S07E01.mkv'),
        )
        assert os.path.islink(link)

        result = verify_symlinks()
        assert result['items'] == 1
        assert not os.path.exists(link)

    def test_keeps_valid_symlink(self, symlink_env):
        """Valid symlinks pointing to existing files are kept."""
        from utils.scheduled_tasks import verify_symlinks
        # Create a real target file
        target_dir = os.path.join(symlink_env['mount'], 'shows', 'Good')
        os.makedirs(target_dir, exist_ok=True)
        target = os.path.join(target_dir, 'ep.mkv')
        with open(target, 'w') as f:
            f.write('data')

        link = _make_symlink(symlink_env['completed'], 'ep.mkv', target)

        result = verify_symlinks()
        assert result['items'] == 0
        assert os.path.islink(link)

    def test_ignores_non_debrid_symlink(self, symlink_env):
        """Symlinks pointing outside debrid paths are not checked."""
        from utils.scheduled_tasks import verify_symlinks
        link = _make_symlink(
            symlink_env['local_tv'], 'other.mkv',
            '/some/other/path/ep.mkv',  # not a debrid path
        )
        assert os.path.islink(link)

        result = verify_symlinks()
        assert result['items'] == 0
        assert os.path.islink(link)  # untouched

    def test_no_target_base_env(self, symlink_env, monkeypatch):
        """Without SYMLINK_TARGET_BASE, only mount-prefix symlinks are checked."""
        from utils.scheduled_tasks import verify_symlinks
        monkeypatch.delenv('BLACKHOLE_SYMLINK_TARGET_BASE')

        # Broken symlink to target_base — should be ignored now
        link = _make_symlink(
            symlink_env['local_tv'], 'ep.mkv',
            os.path.join(symlink_env['target_base'], 'shows', 'gone.mkv'),
        )

        result = verify_symlinks()
        assert result['items'] == 0
        assert os.path.islink(link)  # not removed

    def test_broken_in_local_movies(self, symlink_env):
        """Broken symlinks in local movies dir are also cleaned."""
        from utils.scheduled_tasks import verify_symlinks
        link = _make_symlink(
            symlink_env['local_movies'], 'movie.mkv',
            os.path.join(symlink_env['target_base'], 'movies', 'gone.mkv'),
        )

        result = verify_symlinks()
        assert result['items'] == 1
        assert not os.path.exists(link)

    def test_keeps_symlink_when_target_base_differs_from_mount(self, symlink_env):
        """Symlinks pointing to SYMLINK_TARGET_BASE are checked against the
        rclone mount, not the target base path itself.  This handles the
        common case where target_base (e.g. /mnt/debrid) is only mounted in
        Radarr/Sonarr's container but not in Zurgarr's."""
        from utils.scheduled_tasks import verify_symlinks
        # Create real file on the rclone mount
        mount_file = os.path.join(symlink_env['mount'], 'movies', 'F1', 'f1.mkv')
        os.makedirs(os.path.dirname(mount_file), exist_ok=True)
        with open(mount_file, 'w') as f:
            f.write('data')

        # Symlink points to target_base path (not directly resolvable here)
        target_path = os.path.join(symlink_env['target_base'], 'movies', 'F1', 'f1.mkv')
        link = _make_symlink(symlink_env['local_movies'], 'f1.mkv', target_path)

        result = verify_symlinks()
        assert result['items'] == 0
        assert os.path.islink(link)  # kept — file exists on mount

    def test_removes_symlink_when_mount_file_also_gone(self, symlink_env):
        """When both the target_base path and the translated mount path are
        gone, the symlink is removed (content truly expired)."""
        from utils.scheduled_tasks import verify_symlinks
        target_path = os.path.join(symlink_env['target_base'], 'movies', 'Expired', 'ep.mkv')
        movie_dir = os.path.join(symlink_env['local_movies'], 'Expired (2024)')
        os.makedirs(movie_dir, exist_ok=True)
        link = _make_symlink(movie_dir, 'ep.mkv', target_path)

        result = verify_symlinks()
        assert result['items'] == 1
        assert not os.path.islink(link)
        # Parent dir should be cleaned up too (no media files left)
        assert not os.path.isdir(movie_dir)

    def test_mass_broken_symlinks_deleted_when_mount_healthy(self, symlink_env):
        """When mount is healthy, all broken symlinks are removed regardless of count."""
        from utils.scheduled_tasks import verify_symlinks
        # Create 60 broken symlinks (all pointing to nonexistent mount paths)
        for i in range(60):
            _make_symlink(
                symlink_env['completed'], f'ep{i}.mkv',
                os.path.join(symlink_env['mount'], 'shows', f'gone{i}', f'ep{i}.mkv'),
            )

        result = verify_symlinks()
        assert result['status'] == 'success'
        assert result['items'] == 60
        # All symlinks should be removed
        remaining = [f for f in os.listdir(symlink_env['completed']) if
                     os.path.islink(os.path.join(symlink_env['completed'], f))]
        assert len(remaining) == 0

    def test_aborts_when_mount_does_not_exist(self, symlink_env, monkeypatch, tmp_dir):
        """Mount directory missing: abort without deleting any symlinks."""
        from utils.scheduled_tasks import verify_symlinks
        monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', os.path.join(tmp_dir, 'nonexistent_mount'))
        link = _make_symlink(
            symlink_env['completed'], 'ep.mkv',
            os.path.join(tmp_dir, 'nonexistent_mount', 'shows', 'gone', 'ep.mkv'),
        )
        result = verify_symlinks()
        assert result['status'] == 'error'
        assert os.path.islink(link)  # must NOT be deleted

    def test_aborts_when_mount_empty(self, symlink_env, monkeypatch, tmp_dir):
        """Empty mount (Zurg still starting): abort without deleting symlinks."""
        from utils.scheduled_tasks import verify_symlinks
        empty_mount = os.path.join(tmp_dir, 'empty_mount')
        os.makedirs(empty_mount)
        monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', empty_mount)
        link = _make_symlink(
            symlink_env['completed'], 'ep.mkv',
            os.path.join(empty_mount, 'shows', 'gone', 'ep.mkv'),
        )
        result = verify_symlinks()
        assert result['status'] == 'error'
        assert 'empty' in result['message'].lower()
        assert os.path.islink(link)  # must NOT be deleted

    def test_aborts_when_mount_categories_empty(self, symlink_env, monkeypatch, tmp_dir):
        """Mount has category stubs but no content: abort without deleting."""
        from utils.scheduled_tasks import verify_symlinks
        stub_mount = os.path.join(tmp_dir, 'stub_mount')
        # Create empty category dirs (like Zurg with no torrents)
        os.makedirs(os.path.join(stub_mount, 'movies'))
        os.makedirs(os.path.join(stub_mount, 'shows'))
        monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', stub_mount)
        link = _make_symlink(
            symlink_env['completed'], 'ep.mkv',
            os.path.join(stub_mount, 'shows', 'gone', 'ep.mkv'),
        )
        result = verify_symlinks()
        assert result['status'] == 'error'
        assert 'empty' in result['message'].lower()
        assert os.path.islink(link)  # must NOT be deleted


class TestSymlinkRepair:
    """Tests for the repair cascade in verify_symlinks."""

    def test_repairs_symlink_from_different_category(self, symlink_env):
        """Content moved from movies/ to shows/ on mount — symlink is repaired."""
        from utils.scheduled_tasks import verify_symlinks
        mount = symlink_env['mount']

        # Content now lives under shows/ on the mount
        new_dir = os.path.join(mount, 'shows', 'MyRelease', 'sub')
        os.makedirs(new_dir, exist_ok=True)
        with open(os.path.join(new_dir, 'ep.mkv'), 'w') as f:
            f.write('data')

        # Symlink still points to old movies/ path (broken)
        old_target = os.path.join(mount, 'movies', 'MyRelease', 'sub', 'ep.mkv')
        link = _make_symlink(symlink_env['completed'], 'ep.mkv', old_target)
        assert os.path.islink(link)
        assert not os.path.exists(link)  # broken

        result = verify_symlinks()
        assert os.path.islink(link)  # still a symlink
        new_target = os.readlink(link)
        assert '/shows/' in new_target  # now points to shows/
        assert 'repaired 1' in result['message']

    def test_repairs_symlink_uses_target_base(self, symlink_env):
        """Repaired symlinks use BLACKHOLE_SYMLINK_TARGET_BASE, not rclone mount."""
        from utils.scheduled_tasks import verify_symlinks
        mount = symlink_env['mount']
        target_base = symlink_env['target_base']

        # Content on mount under shows/
        file_dir = os.path.join(mount, 'shows', 'Rel', 'sub')
        os.makedirs(file_dir, exist_ok=True)
        with open(os.path.join(file_dir, 'ep.mkv'), 'w') as f:
            f.write('data')

        # Broken symlink points to target_base/movies/ (old category)
        old_target = os.path.join(target_base, 'movies', 'Rel', 'sub', 'ep.mkv')
        link = _make_symlink(symlink_env['completed'], 'ep.mkv', old_target)

        result = verify_symlinks()
        assert os.path.islink(link)
        new_target = os.readlink(link)
        # Should use target_base, not rclone mount
        assert new_target.startswith(target_base)
        assert '/shows/' in new_target

    def test_deletes_when_truly_gone(self, symlink_env):
        """Content not on mount at all — symlink deleted, not repaired."""
        from utils.scheduled_tasks import verify_symlinks

        old_target = os.path.join(symlink_env['mount'], 'movies', 'Gone', 'movie.mkv')
        link = _make_symlink(symlink_env['completed'], 'movie.mkv', old_target)

        result = verify_symlinks()
        assert not os.path.exists(link)
        assert 'removed 1' in result['message']
        assert 'repaired' not in result['message']

    def test_repair_skips_path_traversal_in_release(self, symlink_env):
        """Symlink target with '..' in release name is not repaired, just deleted."""
        from utils.scheduled_tasks import verify_symlinks
        mount = symlink_env['mount']

        # Create symlink with raw target containing '..' in release name
        link = os.path.join(symlink_env['completed'], 'bad.mkv')
        os.symlink(f"{mount}/movies/../../../etc/passwd/file.mkv", link)

        result = verify_symlinks()
        assert not os.path.exists(link)

    def test_repair_skips_path_traversal_in_relfile(self, symlink_env):
        """Symlink target with '..' in relative file path is not repaired, just deleted."""
        from utils.scheduled_tasks import verify_symlinks
        mount = symlink_env['mount']

        # '..' in the file portion, not the release name
        link = os.path.join(symlink_env['completed'], 'bad2.mkv')
        os.symlink(f"{mount}/movies/LegitRelease/../../etc/passwd", link)

        result = verify_symlinks()
        assert not os.path.exists(link)

    def test_repair_checks_file_exists(self, symlink_env):
        """Release folder found on mount but specific file missing — deleted, not repaired."""
        from utils.scheduled_tasks import verify_symlinks
        mount = symlink_env['mount']

        # Folder exists but file doesn't
        os.makedirs(os.path.join(mount, 'shows', 'Partial'), exist_ok=True)

        old_target = os.path.join(mount, 'movies', 'Partial', 'ep.mkv')
        link = _make_symlink(symlink_env['completed'], 'ep.mkv', old_target)

        result = verify_symlinks()
        assert not os.path.exists(link)
        assert 'repaired' not in result['message']

    def test_auto_search_enabled_by_default_via_gap_fill(self, symlink_env):
        """GAP_FILL_ENABLED (default true) turns auto-search on even when the
        legacy SYMLINK_REPAIR_AUTO_SEARCH flag is unset — broken symlinks are
        treated as a gap to fill, not just housekeeping."""
        from utils.scheduled_tasks import verify_symlinks

        old_target = os.path.join(symlink_env['mount'], 'movies', 'Gone.Movie.2025', 'movie.mkv')
        _make_symlink(symlink_env['completed'], 'movie.mkv', old_target)

        with patch('utils.scheduled_tasks._attempt_arr_research', return_value=True) as mock_search:
            verify_symlinks()
            mock_search.assert_called_once_with('Gone.Movie.2025')

    def test_auto_search_disabled_when_gap_fill_off_and_legacy_flag_off(self, symlink_env, monkeypatch):
        """Both knobs off = no auto-search (preserves the legacy opt-out)."""
        from utils.scheduled_tasks import verify_symlinks
        monkeypatch.setenv('GAP_FILL_ENABLED', 'false')
        monkeypatch.delenv('SYMLINK_REPAIR_AUTO_SEARCH', raising=False)

        old_target = os.path.join(symlink_env['mount'], 'movies', 'Gone.Movie.2025', 'movie.mkv')
        _make_symlink(symlink_env['completed'], 'movie.mkv', old_target)

        with patch('utils.scheduled_tasks._attempt_arr_research') as mock_search:
            verify_symlinks()
            mock_search.assert_not_called()

    def test_auto_search_triggers_on_delete(self, symlink_env, monkeypatch):
        """With auto-search enabled, arr research is attempted for deleted symlinks."""
        from utils.scheduled_tasks import verify_symlinks
        monkeypatch.setenv('SYMLINK_REPAIR_AUTO_SEARCH', 'true')

        old_target = os.path.join(symlink_env['mount'], 'movies', 'Gone.Movie.2025', 'movie.mkv')
        _make_symlink(symlink_env['completed'], 'movie.mkv', old_target)

        with patch('utils.scheduled_tasks._attempt_arr_research', return_value=True) as mock_search:
            result = verify_symlinks()
            mock_search.assert_called_once_with('Gone.Movie.2025')
            assert 're-searched 1' in result['message']

    def test_auto_search_not_called_when_repaired(self, symlink_env, monkeypatch):
        """When symlink is successfully repaired, no arr search is triggered."""
        from utils.scheduled_tasks import verify_symlinks
        monkeypatch.setenv('SYMLINK_REPAIR_AUTO_SEARCH', 'true')
        mount = symlink_env['mount']

        # Content available under shows/
        file_dir = os.path.join(mount, 'shows', 'MyRelease')
        os.makedirs(file_dir, exist_ok=True)
        with open(os.path.join(file_dir, 'ep.mkv'), 'w') as f:
            f.write('data')

        old_target = os.path.join(mount, 'movies', 'MyRelease', 'ep.mkv')
        _make_symlink(symlink_env['completed'], 'ep.mkv', old_target)

        with patch('utils.scheduled_tasks._attempt_arr_research') as mock_search:
            result = verify_symlinks()
            mock_search.assert_not_called()
            assert 'repaired 1' in result['message']

    def test_cooldown_prevents_duplicate_search(self, symlink_env, monkeypatch):
        """Retrigger cooldown prevents searching the same item twice."""
        from utils.scheduled_tasks import _attempt_arr_research, _retrigger_history

        # Pre-populate cooldown for a radarr movie
        _retrigger_history[('radarr', 42)] = time.time()
        try:
            mock_client = MagicMock()
            mock_client.configured = True
            mock_client.find_movie_in_library.return_value = {'id': 42, 'title': 'Test'}

            with patch('utils.arr_client.RadarrClient', return_value=mock_client):
                result = _attempt_arr_research('Test.Movie.2025.1080p')
                assert result is False
                mock_client.search_movie.assert_not_called()
        finally:
            _retrigger_history.pop(('radarr', 42), None)


# ─── Label-aware consumer tests ─────────────────────────────────────────

class TestVerifySymlinksLabeled:
    """verify_symlinks must also walk labeled completed/ layouts."""

    def test_walks_labeled_completed_dir(self, symlink_env):
        from utils.scheduled_tasks import verify_symlinks
        # Broken symlink under /completed/sonarr/Show/
        sonarr_release = os.path.join(symlink_env['completed'], 'sonarr', 'Show.S01')
        os.makedirs(sonarr_release)
        link = os.path.join(sonarr_release, 'ep.mkv')
        os.symlink(
            os.path.join(symlink_env['mount'], 'shows', 'gone', 'ep.mkv'),
            link,
        )

        result = verify_symlinks()
        assert result['items'] == 1
        assert not os.path.exists(link)


class TestHousekeepingEmptyDirSweep:
    """The empty-dir sweep in housekeeping must handle label subdirs."""

    def test_removes_empty_label_dirs(self, tmp_dir, monkeypatch):
        from utils.scheduled_tasks import housekeeping
        completed = os.path.join(tmp_dir, 'completed')
        sonarr = os.path.join(completed, 'sonarr')
        release = os.path.join(sonarr, 'Empty.Release')
        os.makedirs(release)

        monkeypatch.setenv('BLACKHOLE_COMPLETED_DIR', completed)
        monkeypatch.setenv('BLACKHOLE_DIR', os.path.join(tmp_dir, 'watch'))
        os.makedirs(os.path.join(tmp_dir, 'watch'))

        housekeeping()

        # All empty dirs below completed_dir are removed
        assert not os.path.exists(release)
        assert not os.path.exists(sonarr)
        # Top-level completed_dir must be preserved
        assert os.path.isdir(completed)

    def test_preserves_completed_root(self, tmp_dir, monkeypatch):
        """Regression guard: housekeeping must never remove completed_dir itself."""
        from utils.scheduled_tasks import housekeeping
        completed = os.path.join(tmp_dir, 'completed')
        os.makedirs(completed)

        monkeypatch.setenv('BLACKHOLE_COMPLETED_DIR', completed)
        monkeypatch.setenv('BLACKHOLE_DIR', os.path.join(tmp_dir, 'watch'))
        os.makedirs(os.path.join(tmp_dir, 'watch'))

        housekeeping()
        assert os.path.isdir(completed)


class TestHousekeepingMetadataCleanup:
    """Previously this step was a silent no-op (filtered `.meta.json` while
    RetryMeta writes `.meta`, and scanned watch_dir root while meta files
    live in watch_dir/failed/)."""

    def _setup(self, tmp_dir, monkeypatch):
        watch = os.path.join(tmp_dir, 'watch')
        completed = os.path.join(tmp_dir, 'completed')
        os.makedirs(os.path.join(watch, 'failed'))
        os.makedirs(completed)
        monkeypatch.setenv('BLACKHOLE_DIR', watch)
        monkeypatch.setenv('BLACKHOLE_COMPLETED_DIR', completed)
        return watch

    def test_removes_stale_meta_in_failed_flat(self, tmp_dir, monkeypatch):
        from utils.scheduled_tasks import housekeeping
        watch = self._setup(tmp_dir, monkeypatch)
        meta = os.path.join(watch, 'failed', 'x.torrent.meta')
        with open(meta, 'w') as f:
            f.write('{}')
        old = time.time() - 8 * 86400
        os.utime(meta, (old, old))

        housekeeping()
        assert not os.path.exists(meta)

    def test_removes_stale_meta_in_failed_labeled(self, tmp_dir, monkeypatch):
        from utils.scheduled_tasks import housekeeping
        watch = self._setup(tmp_dir, monkeypatch)
        label_dir = os.path.join(watch, 'failed', 'sonarr')
        os.makedirs(label_dir)
        meta = os.path.join(label_dir, 'x.torrent.meta')
        with open(meta, 'w') as f:
            f.write('{}')
        old = time.time() - 8 * 86400
        os.utime(meta, (old, old))

        housekeeping()
        assert not os.path.exists(meta)

    def test_preserves_fresh_meta(self, tmp_dir, monkeypatch):
        from utils.scheduled_tasks import housekeeping
        watch = self._setup(tmp_dir, monkeypatch)
        meta = os.path.join(watch, 'failed', 'x.torrent.meta')
        with open(meta, 'w') as f:
            f.write('{}')
        # mtime is fresh → keep

        housekeeping()
        assert os.path.exists(meta)

    def test_removes_stale_torrent_payload_in_failed(self, tmp_dir, monkeypatch):
        """Items in failed/ never auto-retry, so a week-old .torrent/.magnet
        there is permanently abandoned — without sweeping them the payloads
        accumulate indefinitely (only their sidecars would ever be cleaned).
        Matches the .meta sidecar threshold for symmetry."""
        from utils.scheduled_tasks import housekeeping
        watch = self._setup(tmp_dir, monkeypatch)
        torrent = os.path.join(watch, 'failed', 'x.torrent')
        magnet = os.path.join(watch, 'failed', 'y.magnet')
        for path in (torrent, magnet):
            with open(path, 'w') as f:
                f.write('data')
        old = time.time() - 8 * 86400  # 8 days — just past threshold
        for path in (torrent, magnet):
            os.utime(path, (old, old))

        housekeeping()
        assert not os.path.exists(torrent)
        assert not os.path.exists(magnet)

    def test_removes_stale_payload_in_labeled_failed(self, tmp_dir, monkeypatch):
        """Labeled layout: failed/<label>/ must be swept recursively."""
        from utils.scheduled_tasks import housekeeping
        watch = self._setup(tmp_dir, monkeypatch)
        label_dir = os.path.join(watch, 'failed', 'sonarr')
        os.makedirs(label_dir)
        magnet = os.path.join(label_dir, 'stale.magnet')
        with open(magnet, 'w') as f:
            f.write('magnet:?xt=...')
        old = time.time() - 10 * 86400
        os.utime(magnet, (old, old))

        housekeeping()
        assert not os.path.exists(magnet)

    def test_preserves_fresh_payload_in_failed(self, tmp_dir, monkeypatch):
        """A <7d payload in failed/ must survive — we only sweep permanently-
        abandoned items, not items that just landed."""
        from utils.scheduled_tasks import housekeeping
        watch = self._setup(tmp_dir, monkeypatch)
        magnet = os.path.join(watch, 'failed', 'fresh.magnet')
        with open(magnet, 'w') as f:
            f.write('magnet:?xt=...')
        # mtime is now — younger than 7d threshold

        housekeeping()
        assert os.path.exists(magnet)

    def test_preserves_payload_outside_failed(self, tmp_dir, monkeypatch):
        """A stale .magnet in watch_dir root or .alt_pending/ must NOT be
        swept — those are in-flight drops or retries, not abandoned items."""
        from utils.scheduled_tasks import housekeeping
        watch = self._setup(tmp_dir, monkeypatch)
        # Root-level .magnet (Sonarr just dropped it, not yet processed)
        root_magnet = os.path.join(watch, 'incoming.magnet')
        alt_dir = os.path.join(watch, '.alt_pending')
        os.makedirs(alt_dir)
        alt_magnet = os.path.join(alt_dir, 'retrying.magnet')
        for path in (root_magnet, alt_magnet):
            with open(path, 'w') as f:
                f.write('magnet:?xt=...')
            old = time.time() - 100 * 86400  # deliberately ancient
            os.utime(path, (old, old))

        housekeeping()
        assert os.path.exists(root_magnet)
        assert os.path.exists(alt_magnet)

    def test_fresh_sidecar_stale_payload(self, tmp_dir, monkeypatch):
        """The exact production scenario that triggered this fix: a payload
        whose real mtime is ancient (>7d) but whose sidecar has been kept
        fresh by the retry loop's atomic_write rewriting it on every poll.
        Payload must be swept (it's abandoned); sidecar must survive one
        more housekeeping cycle (still under threshold on its own clock)."""
        from utils.scheduled_tasks import housekeeping
        watch = self._setup(tmp_dir, monkeypatch)
        magnet = os.path.join(watch, 'failed', 'old.magnet')
        sidecar = magnet + '.meta'
        with open(magnet, 'w') as f:
            f.write('magnet:?xt=...')
        with open(sidecar, 'w') as f:
            f.write('{"retries": 3, "alt_exhausted": true}')
        # Payload ancient, sidecar fresh
        os.utime(magnet, (time.time() - 20 * 86400, time.time() - 20 * 86400))
        os.utime(sidecar, (time.time() - 86400, time.time() - 86400))  # 1 day old

        housekeeping()
        assert not os.path.exists(magnet)   # swept
        assert os.path.exists(sidecar)      # preserved this cycle

    def test_preserves_unknown_file_types_in_failed(self, tmp_dir, monkeypatch):
        """If anything other than .torrent/.magnet/.meta lands in failed/
        (e.g. an accidentally-misplaced user file), housekeeping must not
        sweep it — the operator can inspect and handle it manually."""
        from utils.scheduled_tasks import housekeeping
        watch = self._setup(tmp_dir, monkeypatch)
        unknown = os.path.join(watch, 'failed', 'notes.txt')
        with open(unknown, 'w') as f:
            f.write('note to self')
        old = time.time() - 30 * 86400
        os.utime(unknown, (old, old))

        housekeeping()
        assert os.path.exists(unknown)
