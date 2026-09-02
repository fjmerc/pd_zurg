"""Tests for automatic debrid symlink creation (_create_debrid_symlinks)."""

import os
import threading
from unittest.mock import MagicMock, patch
import pytest
import utils.library as library
from utils.library import LibraryScanner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_scanner(mount_path, local_tv_path, monkeypatch, local_movies_path=None):
    """Create a scanner with given mount and local TV/movie paths."""
    monkeypatch.delenv("BLACKHOLE_LOCAL_LIBRARY_MOVIES", raising=False)
    monkeypatch.delenv("BLACKHOLE_LOCAL_LIBRARY_TV", raising=False)
    library._scanner = None
    scanner = LibraryScanner.__new__(LibraryScanner)
    scanner._mount_path = mount_path
    scanner._local_movies_path = local_movies_path
    scanner._local_tv_path = local_tv_path
    scanner._cache = None
    scanner._cache_time = 0
    scanner._ttl = 600
    scanner._lock = threading.Lock()
    scanner._scanning = False
    scanner._effects_running = False
    scanner._path_index = {}
    scanner._local_path_index = {}
    scanner._path_lock = threading.Lock()
    scanner._last_had_local = None
    scanner._local_drop_alerted = False
    scanner._local_empty_scans = 0
    return scanner


def _touch(path):
    """Create an empty file (and any missing parent directories)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    open(path, 'w').close()


def _setup_env(monkeypatch, rclone_mount, symlink_base):
    """Set env vars needed for debrid symlink creation."""
    monkeypatch.setenv('BLACKHOLE_SYMLINK_ENABLED', 'true')
    monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', rclone_mount)
    monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', symlink_base)


# Sentinel local item — the empty-library guard in _create_debrid_symlinks
# skips creation when no local content exists (mount may not be ready).
# Tests that exercise symlink creation need at least one local item.
_LOCAL_MOVIE = {'title': 'Local Sentinel', 'year': 2020, 'source': 'local'}
_LOCAL_SHOW = {'title': 'Local Sentinel', 'year': 2020, 'source': 'local',
               'season_data': [{'number': 1, 'episode_count': 1,
                                'episodes': [{'number': 1, 'file': 'x.mkv', 'source': 'local'}]}]}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestCreateDebridSymlinks:

    def test_creates_symlink_for_debrid_only_episode(self, tmp_dir, monkeypatch):
        """Debrid-only episodes get symlinks in local TV library."""
        mount = os.path.join(tmp_dir, 'mount')
        local_tv = os.path.join(tmp_dir, 'tv')
        os.makedirs(local_tv)

        # Create a debrid file on the mount
        ep_path = os.path.join(mount, 'shows', 'Show.S01E01.1080p', 'Show.S01E01.1080p.mkv')
        _touch(ep_path)

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, local_tv, monkeypatch)

        shows = [{
            'title': 'Show',
            'year': 2025,
            'source': 'debrid',
            'season_data': [{
                'number': 1,
                'episode_count': 1,
                'episodes': [{'number': 1, 'file': 'Show.S01E01.1080p.mkv', 'source': 'debrid'}],
            }],
        }]
        path_index = {('show', 1, 1): ep_path}

        scanner._create_debrid_symlinks(shows, [_LOCAL_MOVIE], path_index)

        expected = os.path.join(local_tv, 'Show (2025)', 'Season 01', 'Show.S01E01.1080p.mkv')
        assert os.path.islink(expected)
        target = os.readlink(expected)
        assert target.startswith('/mnt/debrid/')
        assert 'Show.S01E01.1080p.mkv' in target

    def test_skips_local_and_both_episodes(self, tmp_dir, monkeypatch):
        """Only source='debrid' episodes get symlinks."""
        mount = os.path.join(tmp_dir, 'mount')
        local_tv = os.path.join(tmp_dir, 'tv')
        os.makedirs(local_tv)

        ep_path = os.path.join(mount, 'shows', 'Show.S01E01', 'ep.mkv')
        _touch(ep_path)

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, local_tv, monkeypatch)

        shows = [{
            'title': 'Show',
            'year': 2025,
            'source': 'both',
            'season_data': [{
                'number': 1,
                'episode_count': 2,
                'episodes': [
                    {'number': 1, 'file': 'ep.mkv', 'source': 'local'},
                    {'number': 2, 'file': 'ep2.mkv', 'source': 'both'},
                ],
            }],
        }]
        path_index = {('show', 1, 1): ep_path}

        scanner._create_debrid_symlinks(shows, [], path_index)

        # No symlinks should be created
        show_dir = os.path.join(local_tv, 'Show (2025)')
        assert not os.path.exists(show_dir)

    def test_skips_existing_symlink(self, tmp_dir, monkeypatch):
        """Idempotent — doesn't overwrite existing symlinks."""
        mount = os.path.join(tmp_dir, 'mount')
        local_tv = os.path.join(tmp_dir, 'tv')

        ep_path = os.path.join(mount, 'shows', 'Show.S01E01', 'ep.mkv')
        _touch(ep_path)

        # Pre-create a symlink at the target location
        symlink_path = os.path.join(local_tv, 'Show (2025)', 'Season 01', 'ep.mkv')
        os.makedirs(os.path.dirname(symlink_path))
        os.symlink('/some/old/target', symlink_path)

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, local_tv, monkeypatch)

        shows = [{
            'title': 'Show',
            'year': 2025,
            'source': 'debrid',
            'season_data': [{
                'number': 1,
                'episode_count': 1,
                'episodes': [{'number': 1, 'file': 'ep.mkv', 'source': 'debrid'}],
            }],
        }]
        path_index = {('show', 1, 1): ep_path}

        scanner._create_debrid_symlinks(shows, [], path_index)

        # Original symlink should be untouched
        assert os.readlink(symlink_path) == '/some/old/target'

    def test_skips_existing_real_file(self, tmp_dir, monkeypatch):
        """Doesn't overwrite real local files."""
        mount = os.path.join(tmp_dir, 'mount')
        local_tv = os.path.join(tmp_dir, 'tv')

        ep_path = os.path.join(mount, 'shows', 'Show.S01E01', 'ep.mkv')
        _touch(ep_path)

        # Pre-create a real file at the target location
        real_file = os.path.join(local_tv, 'Show (2025)', 'Season 01', 'ep.mkv')
        _touch(real_file)

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, local_tv, monkeypatch)

        shows = [{
            'title': 'Show',
            'year': 2025,
            'source': 'debrid',
            'season_data': [{
                'number': 1,
                'episode_count': 1,
                'episodes': [{'number': 1, 'file': 'ep.mkv', 'source': 'debrid'}],
            }],
        }]
        path_index = {('show', 1, 1): ep_path}

        scanner._create_debrid_symlinks(shows, [], path_index)

        # Should still be a real file, not a symlink
        assert not os.path.islink(real_file)

    def test_disabled_when_symlink_not_enabled(self, tmp_dir, monkeypatch):
        """No symlinks created when BLACKHOLE_SYMLINK_ENABLED is not true."""
        mount = os.path.join(tmp_dir, 'mount')
        local_tv = os.path.join(tmp_dir, 'tv')
        os.makedirs(local_tv)

        ep_path = os.path.join(mount, 'shows', 'Show.S01E01', 'ep.mkv')
        _touch(ep_path)

        monkeypatch.setenv('BLACKHOLE_SYMLINK_ENABLED', 'false')
        monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', mount)
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', '/mnt/debrid')
        scanner = _make_scanner(mount, local_tv, monkeypatch)

        shows = [{
            'title': 'Show',
            'year': 2025,
            'source': 'debrid',
            'season_data': [{
                'number': 1,
                'episode_count': 1,
                'episodes': [{'number': 1, 'file': 'ep.mkv', 'source': 'debrid'}],
            }],
        }]
        path_index = {('show', 1, 1): ep_path}

        scanner._create_debrid_symlinks(shows, [], path_index)

        show_dir = os.path.join(local_tv, 'Show (2025)')
        assert not os.path.exists(show_dir)

    def test_disabled_when_local_tv_not_set(self, tmp_dir, monkeypatch):
        """No symlinks created when local TV path is not configured."""
        mount = os.path.join(tmp_dir, 'mount')

        ep_path = os.path.join(mount, 'shows', 'Show.S01E01', 'ep.mkv')
        _touch(ep_path)

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, None, monkeypatch)

        shows = [{
            'title': 'Show',
            'year': 2025,
            'source': 'debrid',
            'season_data': [{
                'number': 1,
                'episode_count': 1,
                'episodes': [{'number': 1, 'file': 'ep.mkv', 'source': 'debrid'}],
            }],
        }]
        path_index = {('show', 1, 1): ep_path}

        scanner._create_debrid_symlinks(shows, [], path_index)
        # Should not crash, and nothing should be created

    def test_show_without_year(self, tmp_dir, monkeypatch):
        """Shows without a year use just the title as directory name."""
        mount = os.path.join(tmp_dir, 'mount')
        local_tv = os.path.join(tmp_dir, 'tv')
        os.makedirs(local_tv)

        ep_path = os.path.join(mount, 'shows', 'Adolescence.S01E01', 'ep.mkv')
        _touch(ep_path)

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, local_tv, monkeypatch)

        shows = [{
            'title': 'Adolescence',
            'year': None,
            'source': 'debrid',
            'season_data': [{
                'number': 1,
                'episode_count': 1,
                'episodes': [{'number': 1, 'file': 'ep.mkv', 'source': 'debrid'}],
            }],
        }]
        path_index = {('adolescence', 1, 1): ep_path}

        scanner._create_debrid_symlinks(shows, [_LOCAL_MOVIE], path_index)

        expected = os.path.join(local_tv, 'Adolescence', 'Season 01', 'ep.mkv')
        assert os.path.islink(expected)

    def test_multiple_seasons_and_episodes(self, tmp_dir, monkeypatch):
        """Creates symlinks across multiple seasons."""
        mount = os.path.join(tmp_dir, 'mount')
        local_tv = os.path.join(tmp_dir, 'tv')
        os.makedirs(local_tv)

        ep1 = os.path.join(mount, 'shows', 'Show.S01E01', 'ep1.mkv')
        ep2 = os.path.join(mount, 'shows', 'Show.S01E02', 'ep2.mkv')
        ep3 = os.path.join(mount, 'shows', 'Show.S02E01', 'ep3.mkv')
        for p in [ep1, ep2, ep3]:
            _touch(p)

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, local_tv, monkeypatch)

        shows = [{
            'title': 'Show',
            'year': 2024,
            'source': 'debrid',
            'season_data': [
                {
                    'number': 1,
                    'episode_count': 2,
                    'episodes': [
                        {'number': 1, 'file': 'ep1.mkv', 'source': 'debrid'},
                        {'number': 2, 'file': 'ep2.mkv', 'source': 'debrid'},
                    ],
                },
                {
                    'number': 2,
                    'episode_count': 1,
                    'episodes': [
                        {'number': 1, 'file': 'ep3.mkv', 'source': 'debrid'},
                    ],
                },
            ],
        }]
        path_index = {
            ('show', 1, 1): ep1,
            ('show', 1, 2): ep2,
            ('show', 2, 1): ep3,
        }

        scanner._create_debrid_symlinks(shows, [_LOCAL_MOVIE], path_index)

        assert os.path.islink(os.path.join(local_tv, 'Show (2024)', 'Season 01', 'ep1.mkv'))
        assert os.path.islink(os.path.join(local_tv, 'Show (2024)', 'Season 01', 'ep2.mkv'))
        assert os.path.islink(os.path.join(local_tv, 'Show (2024)', 'Season 02', 'ep3.mkv'))

    def test_rejects_path_traversal_in_title(self, tmp_dir, monkeypatch):
        """Titles with path traversal components are rejected."""
        mount = os.path.join(tmp_dir, 'mount')
        local_tv = os.path.join(tmp_dir, 'tv')
        os.makedirs(local_tv)

        ep_path = os.path.join(mount, 'shows', 'evil', 'ep.mkv')
        _touch(ep_path)

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, local_tv, monkeypatch)

        shows = [{
            'title': '../../etc',
            'year': None,
            'source': 'debrid',
            'season_data': [{
                'number': 1,
                'episode_count': 1,
                'episodes': [{'number': 1, 'file': 'ep.mkv', 'source': 'debrid'}],
            }],
        }]
        path_index = {('etc', 1, 1): ep_path}

        scanner._create_debrid_symlinks(shows, [], path_index)

        # No symlink should be created outside local_tv
        assert not os.path.exists(os.path.join(tmp_dir, 'etc'))
        # Nothing in local_tv either
        assert os.listdir(local_tv) == []

    def test_rejects_debrid_path_outside_mount(self, tmp_dir, monkeypatch):
        """Debrid paths not under the rclone mount are rejected."""
        mount = os.path.join(tmp_dir, 'mount')
        local_tv = os.path.join(tmp_dir, 'tv')
        os.makedirs(local_tv)
        os.makedirs(mount)

        # Create a file outside the mount
        evil_path = os.path.join(tmp_dir, 'outside', 'ep.mkv')
        _touch(evil_path)

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, local_tv, monkeypatch)

        shows = [{
            'title': 'Show',
            'year': 2025,
            'source': 'debrid',
            'season_data': [{
                'number': 1,
                'episode_count': 1,
                'episodes': [{'number': 1, 'file': 'ep.mkv', 'source': 'debrid'}],
            }],
        }]
        path_index = {('show', 1, 1): evil_path}

        scanner._create_debrid_symlinks(shows, [], path_index)

        show_dir = os.path.join(local_tv, 'Show (2025)')
        assert not os.path.exists(show_dir)

    def test_startswith_prefix_attack(self, tmp_dir, monkeypatch):
        """Mount prefix like /mount shouldn't match /mount_evil."""
        mount = os.path.join(tmp_dir, 'mount')
        evil_mount = os.path.join(tmp_dir, 'mount_evil')
        local_tv = os.path.join(tmp_dir, 'tv')
        os.makedirs(local_tv)
        os.makedirs(mount)

        ep_path = os.path.join(evil_mount, 'shows', 'ep.mkv')
        _touch(ep_path)

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, local_tv, monkeypatch)

        shows = [{
            'title': 'Show',
            'year': 2025,
            'source': 'debrid',
            'season_data': [{
                'number': 1,
                'episode_count': 1,
                'episodes': [{'number': 1, 'file': 'ep.mkv', 'source': 'debrid'}],
            }],
        }]
        path_index = {('show', 1, 1): ep_path}

        scanner._create_debrid_symlinks(shows, [], path_index)

        show_dir = os.path.join(local_tv, 'Show (2025)')
        assert not os.path.exists(show_dir)

    def test_season_directory_zero_padded(self, tmp_dir, monkeypatch):
        """Season directories use zero-padded format (Season 01, Season 02)."""
        mount = os.path.join(tmp_dir, 'mount')
        local_tv = os.path.join(tmp_dir, 'tv')
        os.makedirs(local_tv)

        ep_path = os.path.join(mount, 'shows', 'Show.S03E05', 'ep.mkv')
        _touch(ep_path)

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, local_tv, monkeypatch)

        shows = [{
            'title': 'Show',
            'year': 2025,
            'source': 'debrid',
            'season_data': [{
                'number': 3,
                'episode_count': 1,
                'episodes': [{'number': 5, 'file': 'ep.mkv', 'source': 'debrid'}],
            }],
        }]
        path_index = {('show', 3, 5): ep_path}

        scanner._create_debrid_symlinks(shows, [_LOCAL_MOVIE], path_index)

        expected = os.path.join(local_tv, 'Show (2025)', 'Season 03', 'ep.mkv')
        assert os.path.islink(expected)

    def test_symlink_target_uses_sonarr_namespace(self, tmp_dir, monkeypatch):
        """Symlink target is translated to the Sonarr namespace path."""
        mount = os.path.join(tmp_dir, 'mount')
        local_tv = os.path.join(tmp_dir, 'tv')
        os.makedirs(local_tv)

        ep_path = os.path.join(mount, 'shows', 'Show.S01E01', 'ep.mkv')
        _touch(ep_path)

        _setup_env(monkeypatch, mount, '/sonarr/debrid')
        scanner = _make_scanner(mount, local_tv, monkeypatch)

        shows = [{
            'title': 'Show',
            'year': 2025,
            'source': 'debrid',
            'season_data': [{
                'number': 1,
                'episode_count': 1,
                'episodes': [{'number': 1, 'file': 'ep.mkv', 'source': 'debrid'}],
            }],
        }]
        path_index = {('show', 1, 1): ep_path}

        scanner._create_debrid_symlinks(shows, [_LOCAL_MOVIE], path_index)

        symlink = os.path.join(local_tv, 'Show (2025)', 'Season 01', 'ep.mkv')
        target = os.readlink(symlink)
        assert target.startswith('/sonarr/debrid/')
        assert target.endswith('/shows/Show.S01E01/ep.mkv')

    def test_empty_shows_list(self, tmp_dir, monkeypatch):
        """No crash on empty shows list."""
        mount = os.path.join(tmp_dir, 'mount')
        local_tv = os.path.join(tmp_dir, 'tv')
        os.makedirs(mount)
        os.makedirs(local_tv)

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, local_tv, monkeypatch)

        scanner._create_debrid_symlinks([], [], {})
        assert os.listdir(local_tv) == []

    def test_missing_path_index_entry_skipped(self, tmp_dir, monkeypatch):
        """Episodes not in path_index are silently skipped."""
        mount = os.path.join(tmp_dir, 'mount')
        local_tv = os.path.join(tmp_dir, 'tv')
        os.makedirs(mount)
        os.makedirs(local_tv)

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, local_tv, monkeypatch)

        shows = [{
            'title': 'Show',
            'year': 2025,
            'source': 'debrid',
            'season_data': [{
                'number': 1,
                'episode_count': 1,
                'episodes': [{'number': 1, 'file': 'ep.mkv', 'source': 'debrid'}],
            }],
        }]
        # Empty path_index — no debrid paths known
        scanner._create_debrid_symlinks(shows, [], {})

        show_dir = os.path.join(local_tv, 'Show (2025)')
        assert not os.path.exists(show_dir)


class TestCreateDebridSymlinksMovies:

    def test_creates_symlink_for_debrid_movie(self, tmp_dir, monkeypatch):
        """Debrid-only movies get symlinks in local movie library."""
        mount = os.path.join(tmp_dir, 'mount')
        local_movies = os.path.join(tmp_dir, 'movies')
        os.makedirs(local_movies)

        movie_dir = os.path.join(mount, 'movies', 'Inception.2010.1080p')
        movie_file = os.path.join(movie_dir, 'Inception.2010.1080p.mkv')
        _touch(movie_file)

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, None, monkeypatch, local_movies_path=local_movies)

        movies = [{
            'title': 'Inception',
            'year': 2010,
            'source': 'debrid',
            'type': 'movie',
            'path': movie_dir,
        }]

        scanner._create_debrid_symlinks([_LOCAL_SHOW], movies, {})

        expected = os.path.join(local_movies, 'Inception (2010)', 'Inception.2010.1080p.mkv')
        assert os.path.islink(expected)
        target = os.readlink(expected)
        assert target.startswith('/mnt/debrid/')
        assert 'Inception.2010.1080p.mkv' in target

    def test_picks_largest_media_file(self, tmp_dir, monkeypatch):
        """When multiple media files exist, picks the largest."""
        mount = os.path.join(tmp_dir, 'mount')
        local_movies = os.path.join(tmp_dir, 'movies')
        os.makedirs(local_movies)

        movie_dir = os.path.join(mount, 'movies', 'Movie.2025')
        os.makedirs(movie_dir)
        # Small sample file
        with open(os.path.join(movie_dir, 'sample.mkv'), 'w') as f:
            f.write('x' * 100)
        # Large main file
        with open(os.path.join(movie_dir, 'Movie.2025.1080p.mkv'), 'w') as f:
            f.write('x' * 10000)
        # Non-media file
        with open(os.path.join(movie_dir, 'info.nfo'), 'w') as f:
            f.write('metadata')

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, None, monkeypatch, local_movies_path=local_movies)

        movies = [{
            'title': 'Movie',
            'year': 2025,
            'source': 'debrid',
            'type': 'movie',
            'path': movie_dir,
        }]

        scanner._create_debrid_symlinks([_LOCAL_SHOW], movies, {})

        expected = os.path.join(local_movies, 'Movie (2025)', 'Movie.2025.1080p.mkv')
        assert os.path.islink(expected)
        # sample.mkv should NOT have a symlink
        assert not os.path.exists(os.path.join(local_movies, 'Movie (2025)', 'sample.mkv'))

    def test_creates_symlink_for_nested_folder_movie(self, tmp_dir, monkeypatch):
        """Movie video nested one folder deep still gets a symlink.

        Regression: some torrents tuck the feature inside a release-named
        subdir (only .nfo/.txt at the top level).  The movie was detected as
        on-debrid but never linkable — "available" in zurgarr, "missing" in
        Radarr.  The symlink must be flat (basename) but point at the nested
        target.
        """
        mount = os.path.join(tmp_dir, 'mount')
        local_movies = os.path.join(tmp_dir, 'movies')
        os.makedirs(local_movies)

        release = 'The.Artist.2011.1080p.x264'
        movie_dir = os.path.join(mount, 'movies', release)
        os.makedirs(movie_dir)
        # Top level: only junk, no video
        with open(os.path.join(movie_dir, 'info.nfo'), 'w') as f:
            f.write('meta')
        with open(os.path.join(movie_dir, 'downloaded.txt'), 'w') as f:
            f.write('tracker')
        # Real video nested one level deep
        nested_dir = os.path.join(movie_dir, release)
        os.makedirs(nested_dir)
        nested_video = os.path.join(nested_dir, release + '.mkv')
        with open(nested_video, 'w') as f:
            f.write('x' * 10000)

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, None, monkeypatch, local_movies_path=local_movies)

        movies = [{
            'title': 'The Artist',
            'year': 2011,
            'source': 'debrid',
            'type': 'movie',
            'path': movie_dir,
        }]

        scanner._create_debrid_symlinks([_LOCAL_SHOW], movies, {})

        # Flat local filename (basename), not the nested relpath
        expected = os.path.join(local_movies, 'The Artist (2011)', release + '.mkv')
        assert os.path.islink(expected)
        target = os.readlink(expected)
        assert target.startswith('/mnt/debrid/')
        # Target preserves the nested path so it resolves on the arr side
        assert target.endswith(os.path.join(release, release + '.mkv'))

    def test_prefers_top_level_video_over_nested(self, tmp_dir, monkeypatch):
        """A top-level video wins even when a larger file sits in a subdir.

        Guards against descending into ``Sample/``-style subfolders when a
        real top-level feature exists.
        """
        mount = os.path.join(tmp_dir, 'mount')
        local_movies = os.path.join(tmp_dir, 'movies')
        os.makedirs(local_movies)

        movie_dir = os.path.join(mount, 'movies', 'Movie.2025')
        os.makedirs(movie_dir)
        # Top-level feature (smaller than the nested decoy on purpose)
        with open(os.path.join(movie_dir, 'Movie.2025.1080p.mkv'), 'w') as f:
            f.write('x' * 5000)
        # Larger nested file that must NOT be chosen
        extras = os.path.join(movie_dir, 'Extras')
        os.makedirs(extras)
        with open(os.path.join(extras, 'behind.the.scenes.mkv'), 'w') as f:
            f.write('x' * 50000)

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, None, monkeypatch, local_movies_path=local_movies)

        movies = [{
            'title': 'Movie', 'year': 2025, 'source': 'debrid',
            'type': 'movie', 'path': movie_dir,
        }]

        scanner._create_debrid_symlinks([_LOCAL_SHOW], movies, {})

        expected = os.path.join(local_movies, 'Movie (2025)', 'Movie.2025.1080p.mkv')
        assert os.path.islink(expected)
        assert not os.path.exists(
            os.path.join(local_movies, 'Movie (2025)', 'behind.the.scenes.mkv'))

    def test_nested_descent_skips_extras_subdir(self, tmp_dir, monkeypatch):
        """Nested descent ignores Extras/Featurettes even if larger.

        When the top level has no video, the one-level descent must skip
        _SKIP_FOLDERS subdirs so a large featurette in Extras/ isn't linked
        instead of the real feature in a release-named subdir.
        """
        mount = os.path.join(tmp_dir, 'mount')
        local_movies = os.path.join(tmp_dir, 'movies')
        os.makedirs(local_movies)

        release = 'Tigole.Movie.2025.1080p'
        movie_dir = os.path.join(mount, 'movies', release)
        os.makedirs(movie_dir)
        with open(os.path.join(movie_dir, 'info.nfo'), 'w') as f:
            f.write('meta')
        # Real feature in a release-named subdir
        feat_dir = os.path.join(movie_dir, release)
        os.makedirs(feat_dir)
        with open(os.path.join(feat_dir, release + '.mkv'), 'w') as f:
            f.write('x' * 10000)
        # Larger featurette in an Extras subdir that must be skipped
        extras = os.path.join(movie_dir, 'Featurettes')
        os.makedirs(extras)
        with open(os.path.join(extras, 'making.of.mkv'), 'w') as f:
            f.write('x' * 90000)

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, None, monkeypatch, local_movies_path=local_movies)

        movies = [{
            'title': 'Tigole Movie', 'year': 2025, 'source': 'debrid',
            'type': 'movie', 'path': movie_dir,
        }]

        scanner._create_debrid_symlinks([_LOCAL_SHOW], movies, {})

        expected = os.path.join(local_movies, 'Tigole Movie (2025)', release + '.mkv')
        assert os.path.islink(expected)
        target = os.readlink(expected)
        assert target.endswith(os.path.join(release, release + '.mkv'))
        assert not os.path.exists(
            os.path.join(local_movies, 'Tigole Movie (2025)', 'making.of.mkv'))

    def test_skips_local_only_movies(self, tmp_dir, monkeypatch):
        """Movies with source='local' are skipped."""
        mount = os.path.join(tmp_dir, 'mount')
        local_movies = os.path.join(tmp_dir, 'movies')
        os.makedirs(local_movies)

        movie_dir = os.path.join(mount, 'movies', 'Movie.2025')
        _touch(os.path.join(movie_dir, 'movie.mkv'))

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, None, monkeypatch, local_movies_path=local_movies)

        movies = [
            {'title': 'Movie A', 'year': 2025, 'source': 'local', 'type': 'movie', 'path': movie_dir},
        ]

        scanner._create_debrid_symlinks([], movies, {})

        assert os.listdir(local_movies) == []

    def test_both_source_creates_symlink_when_target_dir_empty(self, tmp_dir, monkeypatch):
        """source='both' movies get a symlink if the target dir is empty.

        Handles the case where the movie has a symlink in a wrong-named dir
        (e.g. "F1 The Movie (2025)") but Radarr's dir ("F1 (2025)") is empty.
        """
        mount = os.path.join(tmp_dir, 'mount')
        local_movies = os.path.join(tmp_dir, 'movies')
        os.makedirs(local_movies)

        movie_dir = os.path.join(mount, 'movies', 'Movie.2025')
        _touch(os.path.join(movie_dir, 'movie.mkv'))

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, None, monkeypatch, local_movies_path=local_movies)

        movies = [
            {'title': 'Movie B', 'year': 2025, 'source': 'both', 'type': 'movie', 'path': movie_dir},
        ]

        scanner._create_debrid_symlinks([], movies, {})

        expected = os.path.join(local_movies, 'Movie B (2025)', 'movie.mkv')
        assert os.path.islink(expected)

    def test_both_source_skips_when_target_has_real_file(self, tmp_dir, monkeypatch):
        """source='both' movies are skipped if the target dir already has media.

        Prevents creating a debrid symlink alongside a real local file that
        Radarr already manages.
        """
        mount = os.path.join(tmp_dir, 'mount')
        local_movies = os.path.join(tmp_dir, 'movies')

        movie_dir = os.path.join(mount, 'movies', 'Movie.2025')
        _touch(os.path.join(movie_dir, 'movie.mkv'))

        # Pre-create Radarr's folder with a real file
        radarr_dir = os.path.join(local_movies, 'Movie C (2025)')
        os.makedirs(radarr_dir, exist_ok=True)
        with open(os.path.join(radarr_dir, 'Movie C (2025).mkv'), 'w') as f:
            f.write('real local file')

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, None, monkeypatch, local_movies_path=local_movies)

        movies = [
            {'title': 'Movie C', 'year': 2025, 'source': 'both', 'type': 'movie', 'path': movie_dir},
        ]

        scanner._create_debrid_symlinks([], movies, {})

        # Should NOT create a symlink — real file already exists
        contents = os.listdir(radarr_dir)
        assert len(contents) == 1  # only the original real file
        assert not any(os.path.islink(os.path.join(radarr_dir, f)) for f in contents)

    def test_skips_existing_movie_symlink(self, tmp_dir, monkeypatch):
        """Doesn't overwrite existing movie symlinks."""
        mount = os.path.join(tmp_dir, 'mount')
        local_movies = os.path.join(tmp_dir, 'movies')

        movie_dir = os.path.join(mount, 'movies', 'Movie.2025')
        _touch(os.path.join(movie_dir, 'movie.mkv'))

        # Pre-create symlink
        existing = os.path.join(local_movies, 'Movie (2025)', 'movie.mkv')
        os.makedirs(os.path.dirname(existing))
        os.symlink('/old/target', existing)

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, None, monkeypatch, local_movies_path=local_movies)

        movies = [{
            'title': 'Movie',
            'year': 2025,
            'source': 'debrid',
            'type': 'movie',
            'path': movie_dir,
        }]

        scanner._create_debrid_symlinks([], movies, {})

        assert os.readlink(existing) == '/old/target'

    def test_movie_without_year(self, tmp_dir, monkeypatch):
        """Movies without a year use just the title as directory name."""
        mount = os.path.join(tmp_dir, 'mount')
        local_movies = os.path.join(tmp_dir, 'movies')
        os.makedirs(local_movies)

        movie_dir = os.path.join(mount, 'movies', 'SomeMovie')
        _touch(os.path.join(movie_dir, 'movie.mkv'))

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, None, monkeypatch, local_movies_path=local_movies)

        movies = [{
            'title': 'SomeMovie',
            'year': None,
            'source': 'debrid',
            'type': 'movie',
            'path': movie_dir,
        }]

        scanner._create_debrid_symlinks([_LOCAL_SHOW], movies, {})

        expected = os.path.join(local_movies, 'SomeMovie', 'movie.mkv')
        assert os.path.islink(expected)

    def test_movie_path_traversal_rejected(self, tmp_dir, monkeypatch):
        """Movie titles with path traversal are rejected."""
        mount = os.path.join(tmp_dir, 'mount')
        local_movies = os.path.join(tmp_dir, 'movies')
        os.makedirs(local_movies)

        movie_dir = os.path.join(mount, 'movies', 'evil')
        _touch(os.path.join(movie_dir, 'movie.mkv'))

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, None, monkeypatch, local_movies_path=local_movies)

        movies = [{
            'title': '../../etc',
            'year': None,
            'source': 'debrid',
            'type': 'movie',
            'path': movie_dir,
        }]

        scanner._create_debrid_symlinks([], movies, {})

        assert not os.path.exists(os.path.join(tmp_dir, 'etc'))
        assert os.listdir(local_movies) == []


class TestYearQualifiedMatching:
    """Year-qualified matching disambiguates same-title different-year series.

    Regression test for: torrent "The Bridge 2013 S02E04" was symlinked into
    Sonarr's "The Bridge (2011)" folder because the parsed title "The Bridge"
    matched whichever series was indexed first in sonarr_map_norm.
    """

    def _mock_sonarr_client(self, series_list):
        """Return a mock arr client that returns the given series."""
        client = MagicMock()
        client.get_all_series.return_value = series_list
        return client

    def _mock_radarr_client(self, movie_list):
        """Return a mock arr client that returns the given movies."""
        client = MagicMock()
        client.get_all_movies.return_value = movie_list
        return client

    def test_show_symlink_uses_year_to_pick_correct_sonarr_series(self, tmp_dir, monkeypatch):
        """A torrent with year 2013 should match Sonarr's '(2013)' series, not '(2011)'."""
        mount = os.path.join(tmp_dir, 'mount')
        local_tv = os.path.join(tmp_dir, 'tv')
        os.makedirs(local_tv)

        ep_path = os.path.join(mount, 'shows', 'The.Bridge.2013.S02E04', 'The.Bridge.2013.S02E04.mkv')
        _touch(ep_path)

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, local_tv, monkeypatch)

        # Sonarr has both Bridge series — 2011 indexed first
        sonarr_client = self._mock_sonarr_client([
            {'title': 'The Bridge (2011)', 'path': '/data/media/tv/The Bridge (2011)',
             'id': 140, 'tvdbId': 252019, 'tmdbId': None},
            {'title': 'The Bridge (2013)', 'path': '/data/media/tv/The Bridge (2013)',
             'id': 197, 'tvdbId': 264085, 'tmdbId': None},
        ])

        def mock_get_download_service(media_type):
            if media_type == 'show':
                return sonarr_client, 'sonarr'
            return None, None

        shows = [{
            'title': 'The Bridge',
            'year': 2013,
            'source': 'debrid',
            'season_data': [{
                'number': 2,
                'episode_count': 1,
                'episodes': [{'number': 4, 'file': 'The.Bridge.2013.S02E04.mkv', 'source': 'debrid'}],
            }],
        }]
        path_index = {('the bridge', 2, 4): ep_path}

        with patch('utils.arr_client.get_download_service', mock_get_download_service), \
             patch('utils.tmdb.get_cached_tmdb_ids', return_value={'movies': {}, 'shows': {}}), \
             patch('utils.tmdb.find_show_tmdb_id_by_season', return_value=None):
            scanner._create_debrid_symlinks(shows, [_LOCAL_MOVIE], path_index)

        # Should be in the 2013 folder, NOT the 2011 folder
        correct = os.path.join(local_tv, 'The Bridge (2013)', 'Season 02', 'The.Bridge.2013.S02E04.mkv')
        wrong = os.path.join(local_tv, 'The Bridge (2011)', 'Season 02', 'The.Bridge.2013.S02E04.mkv')
        assert os.path.islink(correct), f"Expected symlink at {correct}"
        assert not os.path.exists(wrong), f"Should NOT have created symlink in 2011 folder"

    def test_show_without_year_still_matches_via_norm(self, tmp_dir, monkeypatch):
        """A torrent without a year still falls through to norm matching."""
        mount = os.path.join(tmp_dir, 'mount')
        local_tv = os.path.join(tmp_dir, 'tv')
        os.makedirs(local_tv)

        ep_path = os.path.join(mount, 'shows', 'Unique.Show.S01E01', 'Unique.Show.S01E01.mkv')
        _touch(ep_path)

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, local_tv, monkeypatch)

        sonarr_client = self._mock_sonarr_client([
            {'title': 'Unique Show', 'path': '/data/media/tv/Unique Show',
             'id': 1, 'tvdbId': 1, 'tmdbId': None},
        ])

        def mock_get_download_service(media_type):
            if media_type == 'show':
                return sonarr_client, 'sonarr'
            return None, None

        shows = [{
            'title': 'Unique Show',
            'year': None,
            'source': 'debrid',
            'season_data': [{
                'number': 1,
                'episode_count': 1,
                'episodes': [{'number': 1, 'file': 'Unique.Show.S01E01.mkv', 'source': 'debrid'}],
            }],
        }]
        path_index = {('unique show', 1, 1): ep_path}

        with patch('utils.arr_client.get_download_service', mock_get_download_service), \
             patch('utils.tmdb.get_cached_tmdb_ids', return_value={'movies': {}, 'shows': {}}), \
             patch('utils.tmdb.find_show_tmdb_id_by_season', return_value=None):
            scanner._create_debrid_symlinks(shows, [_LOCAL_MOVIE], path_index)

        expected = os.path.join(local_tv, 'Unique Show', 'Season 01', 'Unique.Show.S01E01.mkv')
        assert os.path.islink(expected)

    def test_movie_symlink_uses_year_to_pick_correct_radarr_entry(self, tmp_dir, monkeypatch):
        """A movie torrent with year should match the correct Radarr entry."""
        mount = os.path.join(tmp_dir, 'mount')
        local_movies = os.path.join(tmp_dir, 'movies')
        os.makedirs(local_movies)

        movie_dir = os.path.join(mount, 'movies', 'The.Grudge.2020')
        _touch(os.path.join(movie_dir, 'The.Grudge.2020.mkv'))

        _setup_env(monkeypatch, mount, '/mnt/debrid')
        scanner = _make_scanner(mount, None, monkeypatch, local_movies_path=local_movies)

        radarr_client = self._mock_radarr_client([
            {'title': 'The Grudge (2004)', 'path': '/data/media/movies/The Grudge (2004)',
             'id': 10, 'tmdbId': None},
            {'title': 'The Grudge (2020)', 'path': '/data/media/movies/The Grudge (2020)',
             'id': 20, 'tmdbId': None},
        ])

        def mock_get_download_service(media_type):
            if media_type == 'movie':
                return radarr_client, 'radarr'
            return None, None

        movies = [{
            'title': 'The Grudge',
            'year': 2020,
            'source': 'debrid',
            'type': 'movie',
            'path': movie_dir,
        }]

        with patch('utils.arr_client.get_download_service', mock_get_download_service), \
             patch('utils.tmdb.get_cached_tmdb_ids', return_value={'movies': {}, 'shows': {}}), \
             patch('utils.tmdb.find_show_tmdb_id_by_season', return_value=None):
            scanner._create_debrid_symlinks([_LOCAL_SHOW], movies, {})

        correct = os.path.join(local_movies, 'The Grudge (2020)', 'The.Grudge.2020.mkv')
        wrong = os.path.join(local_movies, 'The Grudge (2004)', 'The.Grudge.2020.mkv')
        assert os.path.islink(correct), f"Expected symlink at {correct}"
        assert not os.path.exists(wrong), f"Should NOT have created symlink in 2004 folder"


class TestLocalLibraryEmptyAlerts:
    """Alerting when the local library scan finds zero local content."""

    def _scanner(self, tmp_dir, monkeypatch):
        mount = os.path.join(tmp_dir, 'mount')
        local_tv = os.path.join(tmp_dir, 'tv')
        os.makedirs(local_tv, exist_ok=True)
        _setup_env(monkeypatch, mount, '/mnt/debrid')
        return _make_scanner(mount, local_tv, monkeypatch)

    def test_drop_to_zero_alerts_immediately(self, tmp_dir, monkeypatch):
        """True -> empty transition fires the error alert on the first scan."""
        scanner = self._scanner(tmp_dir, monkeypatch)
        scanner._last_had_local = True
        with patch('utils.notifications.notify') as mock_notify:
            scanner._create_debrid_symlinks([], [], {})
        assert mock_notify.call_count == 1
        assert mock_notify.call_args.kwargs.get('level') == 'error'
        assert scanner._local_drop_alerted is True

    def test_never_seen_local_alerts_after_threshold(self, tmp_dir, monkeypatch):
        """Local configured but never seen since startup -> warning after N scans.

        This is the stale-bind-at-boot case: docker binds the local library
        path before the network share mounts, so the scan is empty from the
        very first run and the drop alert (True->False) can never fire.
        """
        scanner = self._scanner(tmp_dir, monkeypatch)
        with patch('utils.notifications.notify') as mock_notify:
            for _ in range(library._LOCAL_EMPTY_ALERT_SCANS - 1):
                scanner._create_debrid_symlinks([], [], {})
            assert mock_notify.call_count == 0, "must not alert before threshold"
            scanner._create_debrid_symlinks([], [], {})
        assert mock_notify.call_count == 1
        assert mock_notify.call_args.kwargs.get('level') == 'warning'

    def test_never_seen_alert_fires_once(self, tmp_dir, monkeypatch):
        scanner = self._scanner(tmp_dir, monkeypatch)
        with patch('utils.notifications.notify') as mock_notify:
            for _ in range(library._LOCAL_EMPTY_ALERT_SCANS + 3):
                scanner._create_debrid_symlinks([], [], {})
        assert mock_notify.call_count == 1

    def test_local_content_resets_empty_counter(self, tmp_dir, monkeypatch):
        """A scan that finds local content resets the never-seen counter."""
        scanner = self._scanner(tmp_dir, monkeypatch)
        with patch('utils.notifications.notify') as mock_notify:
            for _ in range(library._LOCAL_EMPTY_ALERT_SCANS - 1):
                scanner._create_debrid_symlinks([], [], {})
            scanner._create_debrid_symlinks([], [_LOCAL_MOVIE], {})
            assert scanner._local_empty_scans == 0
            assert scanner._last_had_local is True
            # One more empty scan: now it's a drop (True->False), error level
            scanner._create_debrid_symlinks([], [], {})
        assert mock_notify.call_count == 1
        assert mock_notify.call_args.kwargs.get('level') == 'error'
