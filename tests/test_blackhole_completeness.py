"""Tests for the blackhole post-grab completeness audit (Phase 2)."""

import os
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def handler(tmp_dir, monkeypatch):
    """Build a BlackholeWatcher bypassing the real __init__ so we can test
    _audit_release_completeness in isolation."""
    import threading
    from utils.blackhole import BlackholeWatcher
    h = BlackholeWatcher.__new__(BlackholeWatcher)
    h.debrid_service = 'realdebrid'
    h.symlink_target_base = '/mnt/debrid'
    h.rclone_mount = '/data/mount'
    # Fields normally set by __init__ that the audit relies on:
    h._audit_retrigger = {}
    h._audit_retrigger_lock = threading.Lock()
    h._AUDIT_RETRIGGER_COOLDOWN = 7200
    h._AUDIT_RETRIGGER_MAX_PER_WINDOW = 3
    return h


def _populate_mount(tmp_dir, filenames):
    """Create fake media files in tmp_dir, return the dir path."""
    mount = os.path.join(tmp_dir, 'release')
    os.makedirs(mount, exist_ok=True)
    for f in filenames:
        open(os.path.join(mount, f), 'w').close()
    return mount


class TestCompletenessAudit:

    def test_complete_episode_level_release_is_noop(self, handler, tmp_dir):
        mount = _populate_mount(tmp_dir, ['Show.S01E04.1080p.mkv'])
        filename = 'Show S01E04 1080p WEB-DL.torrent'
        info = {'hash': 'a' * 40}
        with patch('utils.blackhole._blocklist') as bl:
            handler._audit_release_completeness(filename, 'Show.S01E04', mount, info)
        bl.add.assert_not_called()

    def test_short_delivery_blocklists_hash(self, handler, tmp_dir, monkeypatch):
        # Claims E4-E6 but mount only has E4 and E5
        mount = _populate_mount(tmp_dir, [
            'Show.S01E04.mkv',
            'Show.S01E05.mkv',
        ])
        filename = 'Show.S01E04-E06.1080p.WEB-DL.torrent'
        info = {'hash': 'b' * 40}
        monkeypatch.setenv('BLOCKLIST_AUTO_ADD', 'true')
        with patch('utils.blackhole._blocklist') as bl:
            with patch('utils.blackhole.get_download_service', return_value=(MagicMock(), 'sonarr'), create=True):
                handler._audit_release_completeness(filename, 'Show.S01E04-E06', mount, info)
        bl.add.assert_called_once()
        args, kwargs = bl.add.call_args
        assert args[0] == 'B' * 40  # _extract_hash_from_info uppercases
        assert 'incomplete' in kwargs.get('reason', '').lower()
        assert kwargs.get('source') == 'auto'

    def test_short_delivery_triggers_research_for_missing_only(self, handler, tmp_dir, monkeypatch):
        mount = _populate_mount(tmp_dir, ['Show.S01E04.mkv'])  # E5, E6 missing
        filename = 'Show.S01E04-E06.1080p.torrent'
        info = {'hash': 'c' * 40}
        monkeypatch.setenv('BLOCKLIST_AUTO_ADD', 'false')  # isolate re-search path
        sonarr_client = MagicMock()
        sonarr_client.configured = True
        # Audit's find-only guard: series must already exist in Sonarr.
        sonarr_client.find_series_in_library.return_value = {'id': 42, 'title': 'Show'}
        sonarr_client.ensure_and_search.return_value = {'status': 'sent'}
        with patch('utils.arr_client.get_download_service', return_value=(sonarr_client, 'sonarr')):
            with patch('utils.tmdb.search_show', return_value=None):
                with patch('utils.blackhole._blocklist', new=None):
                    handler._audit_release_completeness(filename, 'Show.S01E04-E06', mount, info)
        sonarr_client.ensure_and_search.assert_called_once()
        args, kwargs = sonarr_client.ensure_and_search.call_args
        # Missing episodes = {5, 6}; season = 1
        assert args[2] == 1
        assert sorted(args[3]) == [5, 6]
        assert kwargs.get('prefer_debrid') is True
        assert kwargs.get('respect_monitored') is True

    def test_pack_release_is_skipped(self, handler, tmp_dir):
        """_parse_episodes returns empty for S01 pack — audit must no-op."""
        mount = _populate_mount(tmp_dir, [])
        filename = 'Show.S01.1080p.BluRay-GROUP.torrent'
        info = {'hash': 'd' * 40}
        with patch('utils.blackhole._blocklist') as bl:
            with patch('utils.arr_client.get_download_service') as gds:
                handler._audit_release_completeness(filename, 'Show.S01', mount, info)
        bl.add.assert_not_called()
        gds.assert_not_called()

    def test_audit_skips_research_when_series_not_in_sonarr(self, handler, tmp_dir):
        """If Sonarr doesn't track the series, the audit must NOT call
        ensure_and_search — otherwise the filename-parsed title (with year
        stripped) can fall through to add_series and create a duplicate."""
        mount = _populate_mount(tmp_dir, ['Show.S01E04.mkv'])  # E5 missing
        filename = 'Show.S01E04-E05.1080p.torrent'
        info = {'hash': 'e' * 40}
        sonarr_client = MagicMock()
        sonarr_client.find_series_in_library.return_value = None  # series not found
        with patch('utils.arr_client.get_download_service', return_value=(sonarr_client, 'sonarr')):
            with patch('utils.tmdb.search_show', return_value=None):
                with patch('utils.blackhole._blocklist', new=None):
                    handler._audit_release_completeness(filename, 'Show.S01E04-E05', mount, info)
        sonarr_client.ensure_and_search.assert_not_called()

    def test_audit_retrigger_cooldown_blocks_storm(self, handler, tmp_dir):
        """After _AUDIT_RETRIGGER_MAX_PER_WINDOW successful re-searches for
        the same (title, season), further audits back off."""
        mount = _populate_mount(tmp_dir, ['Show.S01E04.mkv'])
        filename = 'Show.S01E04-E05.1080p.torrent'
        info = {'hash': 'f' * 40}
        sonarr_client = MagicMock()
        sonarr_client.find_series_in_library.return_value = {'id': 1, 'title': 'Show'}
        sonarr_client.ensure_and_search.return_value = {'status': 'sent'}
        with patch('utils.arr_client.get_download_service', return_value=(sonarr_client, 'sonarr')):
            with patch('utils.tmdb.search_show', return_value=None):
                with patch('utils.blackhole._blocklist', new=None):
                    # Fire MAX + 1 audits back-to-back; last one must be no-op
                    for _ in range(handler._AUDIT_RETRIGGER_MAX_PER_WINDOW + 1):
                        handler._audit_release_completeness(filename, 'Show.S01E04-E05', mount, info)
        # Only MAX triggers should have fired
        assert sonarr_client.ensure_and_search.call_count == handler._AUDIT_RETRIGGER_MAX_PER_WINDOW

    def test_missing_info_hash_still_logs_history(self, handler, tmp_dir):
        mount = _populate_mount(tmp_dir, [])  # delivered nothing
        filename = 'Show.S01E04.torrent'
        info = {}  # no hash
        # Replace _extract_hash_from_info to simulate extraction failure
        with patch.object(handler, '_extract_hash_from_info', return_value=''):
            with patch('utils.blackhole._history') as hist:
                with patch('utils.blackhole._blocklist') as bl:
                    with patch('utils.arr_client.get_download_service', return_value=(MagicMock(), 'sonarr')):
                        handler._audit_release_completeness(filename, 'Show.S01E04', mount, info)
        # History event logged even with empty hash
        hist.log_event.assert_called()
        # No blocklist add because hash is empty
        bl.add.assert_not_called()
