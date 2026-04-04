"""Tests for the prefer-debrid search retry mechanism.

Verifies that _search_for_debrid_copies retries stale pending entries
instead of permanently skipping them after the first search attempt.
"""

import json
import os
import time
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest

import utils.library_prefs as lp


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _isolate_pending(tmp_dir, monkeypatch):
    """Point pending storage to a temp dir."""
    pending_path = os.path.join(tmp_dir, 'library_pending.json')
    prefs_path = os.path.join(tmp_dir, 'library_prefs.json')
    monkeypatch.setattr(lp, 'PENDING_PATH', pending_path)
    monkeypatch.setattr(lp, 'PREFS_PATH', prefs_path)


@pytest.fixture
def scanner(monkeypatch):
    """Create a minimal LibraryScanner for testing _search_for_debrid_copies."""
    from utils.library import LibraryScanner
    monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', '/data/mount')
    monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', '/mnt/debrid')
    s = LibraryScanner.__new__(LibraryScanner)
    s._alias_norms = {}
    s._search_cooldown = {}
    s._SEARCH_RETRY_HOURS = 6
    s._SEARCH_BUDGET_SECONDS = 30
    return s


@pytest.fixture
def mock_sonarr():
    """Mock Sonarr client that returns 'sent' for all searches."""
    client = MagicMock()
    client.ensure_and_search.return_value = {'status': 'sent', 'command_id': 1}
    return client


@pytest.fixture
def mock_radarr():
    """Mock Radarr client that returns 'sent' for all searches."""
    client = MagicMock()
    client.ensure_and_search.return_value = {'status': 'sent', 'command_id': 1}
    return client


def _make_show(title, episodes, source='local', year=None):
    """Build a show data dict matching scanner format."""
    season_data = {}
    for sn, en in episodes:
        if sn not in season_data:
            season_data[sn] = []
        season_data[sn].append({'number': en, 'source': source})
    return {
        'title': title,
        'year': year,
        'season_data': [
            {'number': sn, 'episodes': eps}
            for sn, eps in sorted(season_data.items())
        ],
    }


def _make_movie(title, source='local', year=None):
    return {'title': title, 'source': source, 'year': year}


# ---------------------------------------------------------------------------
# Show retry tests
# ---------------------------------------------------------------------------

class TestShowSearchRetry:

    def test_fresh_pending_is_skipped(self, scanner, mock_sonarr):
        """Episodes with a recent pending entry should NOT be re-searched."""
        # Set a pending entry created just now
        lp.set_pending('tulsa king', [
            {'season': 1, 'episode': 1},
            {'season': 1, 'episode': 2},
        ], 'to-debrid')

        shows = [_make_show('Tulsa King', [(1, 1), (1, 2)], source='local')]
        preferences = {'tulsa king': 'prefer-debrid'}

        with patch('utils.arr_client.get_download_service', return_value=(mock_sonarr, 'sonarr')):
            scanner._search_for_debrid_copies(shows, [], preferences)

        # Should NOT have called ensure_and_search — pending is fresh
        mock_sonarr.ensure_and_search.assert_not_called()

    def test_stale_pending_is_retried(self, scanner, mock_sonarr):
        """Episodes with an old pending entry SHOULD be re-searched."""
        # Set pending with last_searched 7 hours ago (exceeds 6-hour threshold)
        old_ts = (datetime.now(timezone.utc) - timedelta(hours=7)).isoformat(timespec='seconds')
        with lp._pending_lock:
            lp._save_pending({
                'tulsa king': {
                    'direction': 'to-debrid',
                    'created': old_ts,
                    'last_searched': old_ts,
                    'episodes': [
                        {'season': 1, 'episode': 1},
                        {'season': 1, 'episode': 2},
                    ],
                }
            })

        shows = [_make_show('Tulsa King', [(1, 1), (1, 2)], source='local')]
        preferences = {'tulsa king': 'prefer-debrid'}

        with patch('utils.arr_client.get_download_service', return_value=(mock_sonarr, 'sonarr')):
            with patch('utils.tmdb.search_show', return_value=None):
                scanner._search_for_debrid_copies(shows, [], preferences)

        # Should have retried the search
        mock_sonarr.ensure_and_search.assert_called_once()

    def test_legacy_pending_without_last_searched_falls_back_to_created(self, scanner, mock_sonarr):
        """Pending entries without last_searched should use created for staleness check."""
        old_ts = (datetime.now(timezone.utc) - timedelta(hours=7)).isoformat(timespec='seconds')
        with lp._pending_lock:
            lp._save_pending({
                'tulsa king': {
                    'direction': 'to-debrid',
                    'created': old_ts,
                    # No 'last_searched' field — legacy entry
                    'episodes': [{'season': 1, 'episode': 1}],
                }
            })

        shows = [_make_show('Tulsa King', [(1, 1)], source='local')]
        preferences = {'tulsa king': 'prefer-debrid'}

        with patch('utils.arr_client.get_download_service', return_value=(mock_sonarr, 'sonarr')):
            with patch('utils.tmdb.search_show', return_value=None):
                scanner._search_for_debrid_copies(shows, [], preferences)

        mock_sonarr.ensure_and_search.assert_called_once()

    def test_retry_updates_last_searched(self, scanner, mock_sonarr):
        """After a retry, last_searched should be updated to prevent immediate re-retry."""
        old_ts = (datetime.now(timezone.utc) - timedelta(hours=7)).isoformat(timespec='seconds')
        with lp._pending_lock:
            lp._save_pending({
                'tulsa king': {
                    'direction': 'to-debrid',
                    'created': old_ts,
                    'last_searched': old_ts,
                    'episodes': [{'season': 1, 'episode': 1}],
                }
            })

        shows = [_make_show('Tulsa King', [(1, 1)], source='local')]
        preferences = {'tulsa king': 'prefer-debrid'}

        with patch('utils.arr_client.get_download_service', return_value=(mock_sonarr, 'sonarr')):
            with patch('utils.tmdb.search_show', return_value=None):
                scanner._search_for_debrid_copies(shows, [], preferences)

        # Verify last_searched was updated
        entry = lp.get_all_pending()['tulsa king']
        assert entry['last_searched'] != old_ts
        # Verify it's recent (within last minute)
        updated = datetime.fromisoformat(entry['last_searched'])
        assert (datetime.now(timezone.utc) - updated).total_seconds() < 60

    def test_debrid_unavailable_is_never_retried(self, scanner, mock_sonarr):
        """Entries escalated to debrid-unavailable should never be retried."""
        old_ts = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat(timespec='seconds')
        with lp._pending_lock:
            lp._save_pending({
                'tulsa king': {
                    'direction': 'debrid-unavailable',
                    'created': old_ts,
                    'last_searched': old_ts,
                    'episodes': [{'season': 1, 'episode': 1}],
                }
            })

        shows = [_make_show('Tulsa King', [(1, 1)], source='local')]
        preferences = {'tulsa king': 'prefer-debrid'}

        with patch('utils.arr_client.get_download_service', return_value=(mock_sonarr, 'sonarr')):
            scanner._search_for_debrid_copies(shows, [], preferences)

        mock_sonarr.ensure_and_search.assert_not_called()

    def test_episodes_already_on_debrid_are_skipped(self, scanner, mock_sonarr):
        """Episodes with source=debrid or source=both should never be searched."""
        old_ts = (datetime.now(timezone.utc) - timedelta(hours=7)).isoformat(timespec='seconds')
        with lp._pending_lock:
            lp._save_pending({
                'tulsa king': {
                    'direction': 'to-debrid',
                    'created': old_ts,
                    'last_searched': old_ts,
                    'episodes': [{'season': 1, 'episode': 1}],
                }
            })

        shows = [_make_show('Tulsa King', [(1, 1)], source='both')]
        preferences = {'tulsa king': 'prefer-debrid'}

        with patch('utils.arr_client.get_download_service', return_value=(mock_sonarr, 'sonarr')):
            scanner._search_for_debrid_copies(shows, [], preferences)

        # Episode is already on debrid — no search needed
        mock_sonarr.ensure_and_search.assert_not_called()


# ---------------------------------------------------------------------------
# Movie retry tests
# ---------------------------------------------------------------------------

class TestMovieSearchRetry:

    def test_fresh_movie_pending_is_skipped(self, scanner, mock_radarr):
        """Movies with a recent pending entry should NOT be re-searched."""
        lp.set_pending('children of men', [{'season': 0, 'episode': 0}], 'to-debrid')

        movies = [_make_movie('Children of Men', source='local')]
        preferences = {'children of men': 'prefer-debrid'}

        with patch('utils.arr_client.get_download_service', return_value=(mock_radarr, 'radarr')):
            scanner._search_for_debrid_copies([], movies, preferences)

        mock_radarr.ensure_and_search.assert_not_called()

    def test_stale_movie_pending_is_retried(self, scanner, mock_radarr):
        """Movies with an old pending entry SHOULD be re-searched."""
        old_ts = (datetime.now(timezone.utc) - timedelta(hours=7)).isoformat(timespec='seconds')
        with lp._pending_lock:
            lp._save_pending({
                'children of men': {
                    'direction': 'to-debrid',
                    'created': old_ts,
                    'last_searched': old_ts,
                    'episodes': [{'season': 0, 'episode': 0}],
                }
            })

        movies = [_make_movie('Children of Men', source='local')]
        preferences = {'children of men': 'prefer-debrid'}

        with patch('utils.arr_client.get_download_service', return_value=(mock_radarr, 'radarr')):
            with patch('utils.tmdb.search_movie', return_value=None):
                scanner._search_for_debrid_copies([], movies, preferences)

        mock_radarr.ensure_and_search.assert_called_once()

    def test_movie_retry_updates_last_searched(self, scanner, mock_radarr):
        """After a movie retry, last_searched should be updated."""
        old_ts = (datetime.now(timezone.utc) - timedelta(hours=7)).isoformat(timespec='seconds')
        with lp._pending_lock:
            lp._save_pending({
                'children of men': {
                    'direction': 'to-debrid',
                    'created': old_ts,
                    'last_searched': old_ts,
                    'episodes': [{'season': 0, 'episode': 0}],
                }
            })

        movies = [_make_movie('Children of Men', source='local')]
        preferences = {'children of men': 'prefer-debrid'}

        with patch('utils.arr_client.get_download_service', return_value=(mock_radarr, 'radarr')):
            with patch('utils.tmdb.search_movie', return_value=None):
                scanner._search_for_debrid_copies([], movies, preferences)

        entry = lp.get_all_pending()['children of men']
        assert entry['last_searched'] != old_ts


# ---------------------------------------------------------------------------
# Failure-path tests: last_searched should NOT be updated when search errors
# ---------------------------------------------------------------------------

class TestSearchFailureNoTouch:

    def test_show_all_errors_preserves_last_searched(self, scanner):
        """When all season searches error, last_searched should stay stale for faster retry."""
        error_client = MagicMock()
        error_client.ensure_and_search.return_value = {'status': 'error', 'message': '0 active indexers'}

        old_ts = (datetime.now(timezone.utc) - timedelta(hours=7)).isoformat(timespec='seconds')
        with lp._pending_lock:
            lp._save_pending({
                'tulsa king': {
                    'direction': 'to-debrid',
                    'created': old_ts,
                    'last_searched': old_ts,
                    'episodes': [{'season': 1, 'episode': 1}],
                }
            })

        shows = [_make_show('Tulsa King', [(1, 1)], source='local')]
        preferences = {'tulsa king': 'prefer-debrid'}

        with patch('utils.arr_client.get_download_service', return_value=(error_client, 'sonarr')):
            with patch('utils.tmdb.search_show', return_value=None):
                scanner._search_for_debrid_copies(shows, [], preferences)

        error_client.ensure_and_search.assert_called_once()
        # last_searched should NOT be updated — search failed
        entry = lp.get_all_pending()['tulsa king']
        assert entry['last_searched'] == old_ts

    def test_movie_error_preserves_last_searched(self, scanner):
        """When movie search errors, last_searched should stay stale."""
        error_client = MagicMock()
        error_client.ensure_and_search.return_value = {'status': 'error', 'message': 'not found'}

        old_ts = (datetime.now(timezone.utc) - timedelta(hours=7)).isoformat(timespec='seconds')
        with lp._pending_lock:
            lp._save_pending({
                'children of men': {
                    'direction': 'to-debrid',
                    'created': old_ts,
                    'last_searched': old_ts,
                    'episodes': [{'season': 0, 'episode': 0}],
                }
            })

        movies = [_make_movie('Children of Men', source='local')]
        preferences = {'children of men': 'prefer-debrid'}

        with patch('utils.arr_client.get_download_service', return_value=(error_client, 'radarr')):
            with patch('utils.tmdb.search_movie', return_value=None):
                scanner._search_for_debrid_copies([], movies, preferences)

        error_client.ensure_and_search.assert_called_once()
        entry = lp.get_all_pending()['children of men']
        assert entry['last_searched'] == old_ts
