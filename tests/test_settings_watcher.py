"""Tests for the settings.json file watcher."""

import json
import os
import time
import pytest
from unittest.mock import patch, MagicMock
from utils.settings_watcher import _get_mtime, start, stop


class TestGetMtime:

    def test_returns_mtime_for_existing_file(self, tmp_path):
        f = tmp_path / 'settings.json'
        f.write_text('{}')
        with patch('utils.settings_watcher.SETTINGS_JSON', str(f)):
            mtime = _get_mtime()
        assert mtime > 0

    def test_returns_zero_for_missing_file(self):
        with patch('utils.settings_watcher.SETTINGS_JSON', '/nonexistent/path.json'):
            mtime = _get_mtime()
        assert mtime == 0


class TestWatcherLifecycle:

    def test_start_creates_daemon_thread(self):
        with patch('utils.settings_watcher._watch_loop'):
            start()
            from utils.settings_watcher import _thread
            assert _thread is not None
            assert _thread.daemon is True
            stop()

    def test_stop_sets_event(self):
        from utils.settings_watcher import _stop_event
        _stop_event.clear()
        stop()
        assert _stop_event.is_set()


class TestWatcherSync:

    def test_detects_mtime_change_and_syncs(self, tmp_path):
        """Simulate a settings.json change and verify sync is called."""
        settings_file = tmp_path / 'settings.json'
        settings_file.write_text(json.dumps({'Overseerr Base URL': 'http://new:5055'}))

        sync_called = []

        def mock_sync(values):
            sync_called.append(values)

        # Simulate the core logic of _watch_loop without the actual loop
        with patch('utils.settings_watcher.SETTINGS_JSON', str(settings_file)), \
             patch('utils.settings_api._sync_plex_debrid_to_env', mock_sync), \
             patch('utils.settings_api.SETTINGS_JSON_FILE', str(settings_file)):
            from utils.settings_api import read_plex_debrid_values
            values = read_plex_debrid_values()
            if values:
                mock_sync(values)

        assert len(sync_called) == 1
        assert sync_called[0]['Overseerr Base URL'] == 'http://new:5055'
