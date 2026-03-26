"""Tests for the symlink switch, pending transitions, and auto-enforcement."""

import json
import os
import pytest
from unittest.mock import patch, MagicMock
import utils.library_prefs as lp


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _isolate(tmp_dir, monkeypatch):
    """Isolate prefs and pending files to temp dir."""
    monkeypatch.setattr(lp, 'PREFS_PATH', os.path.join(tmp_dir, 'prefs.json'))
    monkeypatch.setattr(lp, 'PENDING_PATH', os.path.join(tmp_dir, 'pending.json'))


@pytest.fixture
def local_tv(tmp_dir):
    """Create a fake local TV library with real files."""
    tv_root = os.path.join(tmp_dir, 'tv')
    show_dir = os.path.join(tv_root, 'Show Name (2025)', 'Season 01')
    os.makedirs(show_dir)
    # Create fake episode files
    for ep in [1, 2, 3]:
        path = os.path.join(show_dir, f'Show Name - S01E0{ep} - Episode Title.mkv')
        with open(path, 'w') as f:
            f.write(f'fake video content for episode {ep}')
    return tv_root


@pytest.fixture
def debrid_mount(tmp_dir):
    """Create a fake debrid mount with episode files."""
    mount_root = os.path.join(tmp_dir, 'mount')
    for ep in [1, 2, 3]:
        torrent_dir = os.path.join(mount_root, 'shows',
                                   f'Show.Name.S01E0{ep}.1080p.WEB-DL')
        os.makedirs(torrent_dir)
        path = os.path.join(torrent_dir, f'Show.Name.S01E0{ep}.1080p.WEB-DL.mkv')
        with open(path, 'w') as f:
            f.write(f'debrid content for episode {ep}')
    return mount_root


# ---------------------------------------------------------------------------
# replace_local_with_symlinks
# ---------------------------------------------------------------------------

class TestReplaceLocalWithSymlinks:

    def test_basic_switch(self, local_tv, debrid_mount):
        """Local file deleted and replaced with symlink to debrid mount."""
        local_path = os.path.join(local_tv, 'Show Name (2025)', 'Season 01',
                                  'Show Name - S01E01 - Episode Title.mkv')
        debrid_path = os.path.join(debrid_mount, 'shows',
                                   'Show.Name.S01E01.1080p.WEB-DL',
                                   'Show.Name.S01E01.1080p.WEB-DL.mkv')

        result = lp.replace_local_with_symlinks(
            [{'local_path': local_path, 'debrid_path': debrid_path}],
            local_tv,
            debrid_mount,
            '/mnt/debrid',
        )

        assert result['status'] == 'switched'
        assert result['switched'] == 1
        assert result['errors'] == []
        # Original file should now be a symlink
        assert os.path.islink(local_path)
        # Symlink target should use the Sonarr namespace
        target = os.readlink(local_path)
        assert target.startswith('/mnt/debrid/')
        assert 'Show.Name.S01E01.1080p.WEB-DL.mkv' in target

    def test_multiple_episodes(self, local_tv, debrid_mount):
        """Switch multiple episodes at once."""
        episodes = []
        for ep in [1, 2, 3]:
            episodes.append({
                'local_path': os.path.join(local_tv, 'Show Name (2025)', 'Season 01',
                                           f'Show Name - S01E0{ep} - Episode Title.mkv'),
                'debrid_path': os.path.join(debrid_mount, 'shows',
                                            f'Show.Name.S01E0{ep}.1080p.WEB-DL',
                                            f'Show.Name.S01E0{ep}.1080p.WEB-DL.mkv'),
            })

        result = lp.replace_local_with_symlinks(episodes, local_tv, debrid_mount, '/mnt/debrid')

        assert result['switched'] == 3
        for ep in episodes:
            assert os.path.islink(ep['local_path'])

    def test_path_translation(self, local_tv, debrid_mount):
        """Debrid path is translated from pd_zurg namespace to Sonarr namespace."""
        local_path = os.path.join(local_tv, 'Show Name (2025)', 'Season 01',
                                  'Show Name - S01E01 - Episode Title.mkv')
        debrid_path = os.path.join(debrid_mount, 'shows',
                                   'Show.Name.S01E01.1080p.WEB-DL',
                                   'Show.Name.S01E01.1080p.WEB-DL.mkv')

        lp.replace_local_with_symlinks(
            [{'local_path': local_path, 'debrid_path': debrid_path}],
            local_tv,
            debrid_mount,
            '/mnt/debrid',
        )

        target = os.readlink(local_path)
        # Should replace mount root with symlink target base
        expected = '/mnt/debrid/shows/Show.Name.S01E01.1080p.WEB-DL/Show.Name.S01E01.1080p.WEB-DL.mkv'
        assert target == expected

    def test_rejects_path_outside_library(self, local_tv, debrid_mount, tmp_dir):
        """Path traversal outside local library root is rejected."""
        # Create a file outside the TV root
        evil_path = os.path.join(tmp_dir, 'outside', 'file.mkv')
        os.makedirs(os.path.dirname(evil_path))
        with open(evil_path, 'w') as f:
            f.write('important file')

        debrid_path = os.path.join(debrid_mount, 'shows', 'x', 'x.mkv')

        result = lp.replace_local_with_symlinks(
            [{'local_path': evil_path, 'debrid_path': debrid_path}],
            local_tv,
            debrid_mount,
            '/mnt/debrid',
        )

        assert result['switched'] == 0
        assert any('outside local library' in e for e in result['errors'])
        # File should still exist, untouched
        assert os.path.isfile(evil_path)

    def test_rejects_debrid_path_outside_mount(self, local_tv, debrid_mount):
        """Debrid path not under rclone mount is rejected."""
        local_path = os.path.join(local_tv, 'Show Name (2025)', 'Season 01',
                                  'Show Name - S01E01 - Episode Title.mkv')

        result = lp.replace_local_with_symlinks(
            [{'local_path': local_path, 'debrid_path': '/etc/passwd'}],
            local_tv,
            debrid_mount,
            '/mnt/debrid',
        )

        assert result['switched'] == 0
        assert any('not under rclone mount' in e for e in result['errors'])
        # Local file should still be a real file
        assert os.path.isfile(local_path)
        assert not os.path.islink(local_path)

    def test_missing_local_file(self, local_tv, debrid_mount):
        """Missing local file is reported as error."""
        nonexistent = os.path.join(local_tv, 'Show Name (2025)', 'Season 01',
                                   'Show Name - S01E99 - Missing.mkv')
        debrid_path = os.path.join(debrid_mount, 'shows', 'x', 'x.mkv')

        result = lp.replace_local_with_symlinks(
            [{'local_path': nonexistent, 'debrid_path': debrid_path}],
            local_tv,
            debrid_mount,
            '/mnt/debrid',
        )

        assert result['switched'] == 0
        assert any('not found' in e for e in result['errors'])

    def test_empty_paths_skipped(self, local_tv, debrid_mount):
        """Episodes with empty paths are silently skipped."""
        result = lp.replace_local_with_symlinks(
            [{'local_path': '', 'debrid_path': ''}, {'local_path': None, 'debrid_path': None}],
            local_tv,
            debrid_mount,
            '/mnt/debrid',
        )

        assert result['switched'] == 0
        assert result['errors'] == []

    def test_partial_success(self, local_tv, debrid_mount):
        """Some episodes switch, others fail — partial result reported."""
        good_local = os.path.join(local_tv, 'Show Name (2025)', 'Season 01',
                                  'Show Name - S01E01 - Episode Title.mkv')
        bad_local = os.path.join(local_tv, 'Show Name (2025)', 'Season 01',
                                 'Show Name - S01E99 - Missing.mkv')
        debrid_path = os.path.join(debrid_mount, 'shows',
                                   'Show.Name.S01E01.1080p.WEB-DL',
                                   'Show.Name.S01E01.1080p.WEB-DL.mkv')

        result = lp.replace_local_with_symlinks(
            [
                {'local_path': good_local, 'debrid_path': debrid_path},
                {'local_path': bad_local, 'debrid_path': debrid_path},
            ],
            local_tv,
            debrid_mount,
            '/mnt/debrid',
        )

        assert result['status'] == 'switched'
        assert result['switched'] == 1
        assert len(result['errors']) == 1

    def test_original_file_content_gone(self, local_tv, debrid_mount):
        """After switch, original file bytes are replaced by symlink."""
        local_path = os.path.join(local_tv, 'Show Name (2025)', 'Season 01',
                                  'Show Name - S01E01 - Episode Title.mkv')
        # Verify original content exists
        with open(local_path, 'r') as f:
            assert 'fake video content' in f.read()

        debrid_path = os.path.join(debrid_mount, 'shows',
                                   'Show.Name.S01E01.1080p.WEB-DL',
                                   'Show.Name.S01E01.1080p.WEB-DL.mkv')

        lp.replace_local_with_symlinks(
            [{'local_path': local_path, 'debrid_path': debrid_path}],
            local_tv,
            debrid_mount,
            '/mnt/debrid',
        )

        # Should be a symlink now, not a regular file
        assert os.path.islink(local_path)
        assert not os.path.isfile(local_path) or os.path.islink(local_path)


# ---------------------------------------------------------------------------
# Pending transitions
# ---------------------------------------------------------------------------

class TestPending:

    def test_set_and_get_pending(self):
        eps = [{'season': 1, 'episode': 6}, {'season': 1, 'episode': 8}]
        lp.set_pending('alien earth', eps, 'to-debrid')

        pending = lp.get_all_pending()
        assert 'alien earth' in pending
        assert pending['alien earth']['direction'] == 'to-debrid'
        assert len(pending['alien earth']['episodes']) == 2

    def test_set_pending_no_duplicates(self):
        lp.set_pending('show', [{'season': 1, 'episode': 1}])
        lp.set_pending('show', [{'season': 1, 'episode': 1}, {'season': 1, 'episode': 2}])

        pending = lp.get_all_pending()
        assert len(pending['show']['episodes']) == 2

    def test_clear_all_pending(self):
        lp.set_pending('show', [{'season': 1, 'episode': 1}])
        lp.clear_pending('show')

        assert lp.get_all_pending() == {}

    def test_clear_specific_episodes(self):
        lp.set_pending('show', [
            {'season': 1, 'episode': 1},
            {'season': 1, 'episode': 2},
            {'season': 1, 'episode': 3},
        ])
        lp.clear_pending('show', [{'season': 1, 'episode': 2}])

        pending = lp.get_all_pending()
        eps = pending['show']['episodes']
        assert len(eps) == 2
        ep_nums = {e['episode'] for e in eps}
        assert ep_nums == {1, 3}

    def test_clear_all_episodes_removes_entry(self):
        lp.set_pending('show', [{'season': 1, 'episode': 1}])
        lp.clear_pending('show', [{'season': 1, 'episode': 1}])

        assert 'show' not in lp.get_all_pending()

    def test_clear_nonexistent_title(self):
        """Clearing a title that doesn't exist is a no-op."""
        lp.clear_pending('nonexistent')
        assert lp.get_all_pending() == {}

    def test_pending_empty_on_fresh_start(self):
        assert lp.get_all_pending() == {}


# ---------------------------------------------------------------------------
# Auto-enforcement (_enforce_preferences)
# ---------------------------------------------------------------------------

class TestAutoEnforcement:
    """Tests for LibraryScanner._enforce_preferences."""

    def _make_scanner(self, local_tv, monkeypatch):
        from utils.library import LibraryScanner
        monkeypatch.setenv('BLACKHOLE_LOCAL_LIBRARY_TV', local_tv)
        monkeypatch.setenv('RCLONE_MOUNT_NAME', '')
        scanner = LibraryScanner()
        scanner._local_tv_path = local_tv
        return scanner

    def _make_show(self, title, episodes):
        """Build a show dict with season_data matching the scanner output format."""
        season_data = []
        by_season = {}
        for sn, en, source in episodes:
            if sn not in by_season:
                by_season[sn] = []
            by_season[sn].append({'number': en, 'file': f'ep{en}.mkv', 'source': source})
        for sn in sorted(by_season.keys()):
            season_data.append({'number': sn, 'episode_count': len(by_season[sn]), 'episodes': by_season[sn]})
        return {'title': title, 'source': 'both', 'type': 'show', 'season_data': season_data}

    def test_prefer_debrid_switches_both_to_symlink(self, tmp_dir, monkeypatch):
        """source=both + prefer-debrid → local file replaced with symlink."""
        local_tv = os.path.join(tmp_dir, 'tv')
        show_dir = os.path.join(local_tv, 'Show (2025)', 'Season 01')
        os.makedirs(show_dir)
        local_file = os.path.join(show_dir, 'ep1.mkv')
        with open(local_file, 'w') as f:
            f.write('local content')

        mount = os.path.join(tmp_dir, 'mount')
        debrid_file = os.path.join(mount, 'shows', 'Show.S01E01', 'ep1.mkv')
        os.makedirs(os.path.dirname(debrid_file))
        with open(debrid_file, 'w') as f:
            f.write('debrid content')

        monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', mount)
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', '/mnt/debrid')
        monkeypatch.setenv('LIBRARY_PREFERENCE_AUTO_ENFORCE', 'true')

        scanner = self._make_scanner(local_tv, monkeypatch)
        show = self._make_show('Show', [(1, 1, 'both')])
        from utils.library import _normalize_title
        norm = _normalize_title('Show')
        path_index = {(norm, 1, 1): debrid_file}
        local_path_index = {(norm, 1, 1): local_file}
        preferences = {norm: 'prefer-debrid'}

        scanner._enforce_preferences([show], [], preferences, path_index, local_path_index)

        assert os.path.islink(local_file)
        assert os.readlink(local_file).startswith('/mnt/debrid/')

    def test_prefer_debrid_skips_already_symlinked(self, tmp_dir, monkeypatch):
        """Already-symlinked files should not be re-processed."""
        local_tv = os.path.join(tmp_dir, 'tv')
        show_dir = os.path.join(local_tv, 'Show (2025)', 'Season 01')
        os.makedirs(show_dir)
        local_file = os.path.join(show_dir, 'ep1.mkv')
        os.symlink('/mnt/debrid/shows/ep1.mkv', local_file)

        mount = os.path.join(tmp_dir, 'mount')
        debrid_file = os.path.join(mount, 'shows', 'Show.S01E01', 'ep1.mkv')
        os.makedirs(os.path.dirname(debrid_file))
        with open(debrid_file, 'w') as f:
            f.write('debrid')

        monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', mount)
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', '/mnt/debrid')
        monkeypatch.setenv('LIBRARY_PREFERENCE_AUTO_ENFORCE', 'true')

        scanner = self._make_scanner(local_tv, monkeypatch)
        show = self._make_show('Show', [(1, 1, 'both')])
        from utils.library import _normalize_title
        norm = _normalize_title('Show')
        path_index = {(norm, 1, 1): debrid_file}
        local_path_index = {(norm, 1, 1): local_file}
        preferences = {norm: 'prefer-debrid'}

        # Should be a no-op since already symlinked
        with patch.object(lp, 'replace_local_with_symlinks') as mock_switch:
            scanner._enforce_preferences([show], [], preferences, path_index, local_path_index)
            mock_switch.assert_not_called()

    def test_prefer_local_deletes_debrid_torrents(self, tmp_dir, monkeypatch):
        """source=both + prefer-local → debrid torrents deleted."""
        local_tv = os.path.join(tmp_dir, 'tv')
        os.makedirs(local_tv)

        monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', os.path.join(tmp_dir, 'mount'))
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', '/mnt/debrid')
        monkeypatch.setenv('LIBRARY_PREFERENCE_AUTO_ENFORCE', 'true')

        scanner = self._make_scanner(local_tv, monkeypatch)
        show = self._make_show('Show', [(1, 1, 'both')])
        from utils.library import _normalize_title
        norm = _normalize_title('Show')
        preferences = {norm: 'prefer-local'}

        mock_client = MagicMock()
        mock_client.find_torrents_by_title.return_value = [{'id': 'ABC', 'filename': 'Show.S01.mkv', 'parsed_title': 'Show', 'year': None}]
        mock_client.delete_torrent.return_value = True

        with patch('utils.debrid_client.get_debrid_client', return_value=(mock_client, 'realdebrid')):
            scanner._enforce_preferences([show], [], preferences, {}, {})

        mock_client.delete_torrent.assert_called_once_with('ABC')

    def test_disabled_by_config(self, tmp_dir, monkeypatch):
        """LIBRARY_PREFERENCE_AUTO_ENFORCE=false disables enforcement."""
        local_tv = os.path.join(tmp_dir, 'tv')
        os.makedirs(local_tv)
        monkeypatch.setenv('LIBRARY_PREFERENCE_AUTO_ENFORCE', 'false')
        monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', '/mount')
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', '/mnt/debrid')

        scanner = self._make_scanner(local_tv, monkeypatch)
        show = self._make_show('Show', [(1, 1, 'both')])
        from utils.library import _normalize_title
        norm = _normalize_title('Show')
        preferences = {norm: 'prefer-debrid'}

        with patch.object(lp, 'replace_local_with_symlinks') as mock_switch:
            scanner._enforce_preferences([show], [], preferences, {(norm, 1, 1): '/x'}, {(norm, 1, 1): '/y'})
            mock_switch.assert_not_called()

    def test_no_action_for_non_both_source(self, tmp_dir, monkeypatch):
        """Episodes with source=local or source=debrid are not auto-enforced."""
        local_tv = os.path.join(tmp_dir, 'tv')
        os.makedirs(local_tv)
        monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', '/mount')
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', '/mnt/debrid')
        monkeypatch.setenv('LIBRARY_PREFERENCE_AUTO_ENFORCE', 'true')

        scanner = self._make_scanner(local_tv, monkeypatch)
        show = self._make_show('Show', [(1, 1, 'local'), (1, 2, 'debrid')])
        from utils.library import _normalize_title
        norm = _normalize_title('Show')
        preferences = {norm: 'prefer-debrid'}

        with patch.object(lp, 'replace_local_with_symlinks') as mock_switch:
            scanner._enforce_preferences([show], [], preferences, {}, {})
            mock_switch.assert_not_called()

    def test_no_action_without_preference(self, tmp_dir, monkeypatch):
        """Shows without a preference are not enforced."""
        local_tv = os.path.join(tmp_dir, 'tv')
        os.makedirs(local_tv)
        monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', '/mount')
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', '/mnt/debrid')
        monkeypatch.setenv('LIBRARY_PREFERENCE_AUTO_ENFORCE', 'true')

        scanner = self._make_scanner(local_tv, monkeypatch)
        show = self._make_show('Show', [(1, 1, 'both')])

        with patch.object(lp, 'replace_local_with_symlinks') as mock_switch:
            scanner._enforce_preferences([show], [], {}, {}, {})
            mock_switch.assert_not_called()
