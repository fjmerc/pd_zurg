"""Tests for pending warning and error tracking (Phases 1-2 of pending babysitting)."""

import json
import os
from datetime import datetime, timezone, timedelta
from unittest.mock import patch
import pytest
import utils.library_prefs as lp
from utils.library import LibraryScanner


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _isolate_prefs(tmp_dir, monkeypatch):
    """Point prefs/pending to a temp dir and reset module state."""
    prefs_path = os.path.join(tmp_dir, 'library_prefs.json')
    pending_path = os.path.join(tmp_dir, 'library_pending.json')
    monkeypatch.setattr(lp, 'PREFS_PATH', prefs_path)
    monkeypatch.setattr(lp, 'PENDING_PATH', pending_path)


@pytest.fixture
def scanner(monkeypatch):
    """Create a LibraryScanner with no actual mount paths."""
    monkeypatch.setenv('RCLONE_MOUNT_NAME', 'test_mount')
    monkeypatch.setenv('PENDING_WARNING_HOURS', '24')
    s = LibraryScanner.__new__(LibraryScanner)
    # Minimal init without hitting the filesystem
    s._mount_path = None
    s._local_tv_path = None
    s._local_movie_path = None
    s._ttl = 300
    s._cache = None
    s._cache_time = 0
    s._scanning = False
    s._effects_running = False
    s._lock = __import__('threading').Lock()
    s._path_lock = __import__('threading').Lock()
    s._path_index = {}
    s._local_path_index = {}
    s._search_cooldown = {}
    s._alias_norms = {}
    s._debrid_unavailable_days = 3
    s._pending_warning_hours = 24
    s._last_had_local = None
    s._local_drop_alerted = False
    return s


# ---------------------------------------------------------------------------
# _warn_stalled_pending tests
# ---------------------------------------------------------------------------

class TestWarnStalledPending:

    def test_warns_item_over_threshold(self, scanner):
        """Item pending >24hrs should trigger a warning notification."""
        lp.set_pending('old show', [{'season': 1, 'episode': 1}], 'to-debrid')
        # Backdate the created timestamp
        with lp._pending_lock:
            pending = lp._load_pending()
            pending['old show']['created'] = (
                datetime.now(timezone.utc) - timedelta(hours=30)
            ).isoformat(timespec='seconds')
            lp._save_pending(pending)

        with patch('utils.notifications.notify') as mock_notify:
            scanner._warn_stalled_pending()

        mock_notify.assert_called_once()
        args = mock_notify.call_args
        assert args[0][0] == 'pending_warning'
        assert 'old show' in args[0][2]
        # warned_at should be set
        entry = lp.get_all_pending()['old show']
        assert 'warned_at' in entry

    def test_does_not_warn_recent_item(self, scanner):
        """Item pending <24hrs should NOT trigger a warning."""
        lp.set_pending('new show', [{'season': 1, 'episode': 1}], 'to-debrid')
        # Created just now — well under 24hrs

        with patch('utils.notifications.notify') as mock_notify:
            scanner._warn_stalled_pending()

        mock_notify.assert_not_called()
        entry = lp.get_all_pending()['new show']
        assert 'warned_at' not in entry

    def test_does_not_warn_twice(self, scanner):
        """Once warned_at is set, should not warn again."""
        lp.set_pending('show', [{'season': 1, 'episode': 1}], 'to-debrid')
        with lp._pending_lock:
            pending = lp._load_pending()
            pending['show']['created'] = (
                datetime.now(timezone.utc) - timedelta(hours=48)
            ).isoformat(timespec='seconds')
            pending['show']['warned_at'] = datetime.now(timezone.utc).isoformat(timespec='seconds')
            lp._save_pending(pending)

        with patch('utils.notifications.notify') as mock_notify:
            scanner._warn_stalled_pending()

        mock_notify.assert_not_called()

    def test_skips_non_to_debrid_directions(self, scanner):
        """Only direction='to-debrid' should be warned."""
        # debrid-unavailable — already escalated
        lp.set_pending('show1', [{'season': 1, 'episode': 1}], 'to-debrid')
        lp.mark_debrid_unavailable('show1')

        # to-local — different workflow
        lp.set_pending('show2', [{'season': 1, 'episode': 1}], 'to-local')

        # Backdate both
        with lp._pending_lock:
            pending = lp._load_pending()
            old_ts = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat(timespec='seconds')
            for key in pending:
                pending[key]['created'] = old_ts
            lp._save_pending(pending)

        with patch('utils.notifications.notify') as mock_notify:
            scanner._warn_stalled_pending()

        mock_notify.assert_not_called()

    def test_includes_last_error_in_notification(self, scanner):
        """Notification body should include last_error context."""
        lp.set_pending('show', [{'season': 1, 'episode': 1}], 'to-debrid')
        with lp._pending_lock:
            pending = lp._load_pending()
            pending['show']['created'] = (
                datetime.now(timezone.utc) - timedelta(hours=30)
            ).isoformat(timespec='seconds')
            pending['show']['last_error'] = 'Sonarr: series not found'
            lp._save_pending(pending)

        with patch('utils.notifications.notify') as mock_notify:
            scanner._warn_stalled_pending()

        body = mock_notify.call_args[0][2]
        assert 'Sonarr: series not found' in body

    def test_configurable_threshold(self, scanner):
        """PENDING_WARNING_HOURS should be respected."""
        scanner._pending_warning_hours = 12  # lower threshold

        lp.set_pending('show', [{'season': 1, 'episode': 1}], 'to-debrid')
        with lp._pending_lock:
            pending = lp._load_pending()
            pending['show']['created'] = (
                datetime.now(timezone.utc) - timedelta(hours=15)
            ).isoformat(timespec='seconds')
            lp._save_pending(pending)

        with patch('utils.notifications.notify') as mock_notify:
            scanner._warn_stalled_pending()

        mock_notify.assert_called_once()

    def test_zero_threshold_disables_warnings(self, scanner):
        """PENDING_WARNING_HOURS=0 should disable warnings entirely."""
        scanner._pending_warning_hours = 0

        lp.set_pending('show', [{'season': 1, 'episode': 1}], 'to-debrid')
        with lp._pending_lock:
            pending = lp._load_pending()
            pending['show']['created'] = (
                datetime.now(timezone.utc) - timedelta(hours=48)
            ).isoformat(timespec='seconds')
            lp._save_pending(pending)

        with patch('utils.notifications.notify') as mock_notify:
            scanner._warn_stalled_pending()

        mock_notify.assert_not_called()
