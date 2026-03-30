"""Tests for verify_symlinks in utils/scheduled_tasks.py."""

import os
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
        Radarr/Sonarr's container but not in pd_zurg's."""
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

    def test_mass_deletion_blocked_by_threshold(self, symlink_env):
        """When >50 and >50% of symlinks appear broken, refuse to delete."""
        from utils.scheduled_tasks import verify_symlinks
        # Create 60 broken symlinks (all pointing to nonexistent mount paths)
        for i in range(60):
            _make_symlink(
                symlink_env['completed'], f'ep{i}.mkv',
                os.path.join(symlink_env['mount'], 'shows', f'gone{i}', f'ep{i}.mkv'),
            )

        result = verify_symlinks()
        assert result['status'] == 'error'
        assert 'blocked' in result['message'].lower()
        assert result['items'] == 0
        # All symlinks should still exist (not deleted)
        remaining = [f for f in os.listdir(symlink_env['completed']) if
                     os.path.islink(os.path.join(symlink_env['completed'], f))]
        assert len(remaining) == 60
