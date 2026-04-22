"""Tests for the unconditional episode-completeness reconcile (gap-fill).

Verifies the Phase 1 behavior change: ``_search_for_missing_episodes`` now
runs for every monitored item regardless of source preference, driven by
the TMDB-vs-scan diff rather than only the local-only-under-prefer-debrid
case the function previously covered.
"""

import os
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest

import utils.library_prefs as lp
import utils.tmdb as tmdb_mod


@pytest.fixture(autouse=True)
def _isolate_pending(tmp_dir, monkeypatch):
    pending_path = os.path.join(tmp_dir, 'library_pending.json')
    prefs_path = os.path.join(tmp_dir, 'library_prefs.json')
    monkeypatch.setattr(lp, 'PENDING_PATH', pending_path)
    monkeypatch.setattr(lp, 'PREFS_PATH', prefs_path)


@pytest.fixture
def scanner(monkeypatch):
    from utils.library import LibraryScanner
    monkeypatch.setenv('BLACKHOLE_RCLONE_MOUNT', '/data/mount')
    monkeypatch.setenv('GAP_FILL_ENABLED', 'true')
    s = LibraryScanner.__new__(LibraryScanner)
    s._alias_norms = {}
    s._search_cooldown = {}
    s._SEARCH_RETRY_HOURS = 6
    s._SEARCH_BUDGET_SECONDS = 30
    return s


@pytest.fixture
def mock_sonarr():
    c = MagicMock()
    c.ensure_and_search.return_value = {'status': 'sent', 'command_id': 1}
    return c


@pytest.fixture
def mock_radarr():
    c = MagicMock()
    c.ensure_and_search.return_value = {'status': 'sent', 'command_id': 1}
    return c


def _show(title, present_episodes, year=None):
    """present_episodes: list of (season, episode) that the scan FOUND.
    All marked as source='debrid' by default — what's absent is 'missing'."""
    by_season = {}
    for sn, en in present_episodes:
        by_season.setdefault(sn, []).append({'number': en, 'source': 'debrid'})
    return {
        'title': title,
        'year': year,
        'season_data': [
            {'number': sn, 'episodes': eps} for sn, eps in sorted(by_season.items())
        ],
    }


def _stub_tmdb_episodes(expected):
    """Patch tmdb.get_cached_episode_list to return the given (sn, en) list."""
    def _fake(norm, year=None):
        return [{'season': sn, 'number': en, 'air_date': '2020-01-01'} for sn, en in expected]
    return patch('utils.tmdb.get_cached_episode_list', side_effect=_fake)


class TestRouteSelector:

    def test_prefer_debrid_maps_to_true(self, scanner):
        assert scanner._route_for('foo', {'foo': 'prefer-debrid'}) is True

    def test_prefer_local_maps_to_false(self, scanner):
        assert scanner._route_for('foo', {'foo': 'prefer-local'}) is False

    def test_unset_maps_to_none(self, scanner):
        assert scanner._route_for('foo', {}) is None


class TestComputeMissingEpisodes:

    def test_returns_tmdb_expected_minus_present(self, scanner):
        show = _show('Lucky Hank', [(1, 1), (1, 2), (1, 3)])  # E4 missing
        with _stub_tmdb_episodes([(1, 1), (1, 2), (1, 3), (1, 4)]):
            missing = scanner._compute_missing_episodes(show)
        assert missing == [(1, 4)]

    def test_empty_tmdb_returns_empty_not_everything(self, scanner):
        """Empty TMDB cache must not trigger spurious searches for every episode."""
        show = _show('Unknown', [(1, 1)])
        with _stub_tmdb_episodes([]):
            missing = scanner._compute_missing_episodes(show)
        assert missing == []

    def test_local_counts_as_present(self, scanner):
        """Source=local satisfies the user story — not missing."""
        show = {
            'title': 'Show',
            'year': None,
            'season_data': [{'number': 1, 'episodes': [
                {'number': 1, 'source': 'local'},
                {'number': 2, 'source': 'debrid'},
            ]}],
        }
        with _stub_tmdb_episodes([(1, 1), (1, 2)]):
            assert scanner._compute_missing_episodes(show) == []


class TestGapFillWithoutPreference:
    """Route=None: search runs for shows with no preference set."""

    def test_searches_episode_missing_from_all_sources(self, scanner, mock_sonarr):
        show = _show('Lucky Hank', [(1, 1), (1, 2), (1, 3)])
        with _stub_tmdb_episodes([(1, 1), (1, 2), (1, 3), (1, 4)]):
            with patch('utils.arr_client.get_download_service', return_value=(mock_sonarr, 'sonarr')):
                with patch('utils.tmdb.search_show', return_value=None):
                    scanner._search_for_missing_episodes([show], [], {})
        mock_sonarr.ensure_and_search.assert_called_once()
        args, kwargs = mock_sonarr.ensure_and_search.call_args
        # args: (title, tmdb_id, season_number, episode_numbers)
        assert args[2] == 1
        assert args[3] == [4]
        assert kwargs.get('prefer_debrid') is None
        assert kwargs.get('respect_monitored') is True

    def test_pending_direction_is_to_any(self, scanner, mock_sonarr):
        show = _show('Lucky Hank', [(1, 1)])
        with _stub_tmdb_episodes([(1, 1), (1, 2)]):
            with patch('utils.arr_client.get_download_service', return_value=(mock_sonarr, 'sonarr')):
                with patch('utils.tmdb.search_show', return_value=None):
                    scanner._search_for_missing_episodes([show], [], {})
        entry = lp.get_all_pending().get('lucky hank')
        assert entry is not None
        assert entry['direction'] == 'to-any'

    def test_no_search_when_fully_present(self, scanner, mock_sonarr):
        show = _show('Complete Show', [(1, 1), (1, 2)])
        with _stub_tmdb_episodes([(1, 1), (1, 2)]):
            with patch('utils.arr_client.get_download_service', return_value=(mock_sonarr, 'sonarr')):
                scanner._search_for_missing_episodes([show], [], {})
        mock_sonarr.ensure_and_search.assert_not_called()


class TestPreferDebridPreserved:
    """Route=True keeps the legacy 'search for debrid copy of local-only' behavior."""

    def test_local_only_episode_still_searched(self, scanner, mock_sonarr):
        show = {
            'title': 'Tulsa King',
            'year': None,
            'season_data': [{'number': 1, 'episodes': [
                {'number': 1, 'source': 'local'},  # local-only, route=True should search
            ]}],
        }
        with _stub_tmdb_episodes([(1, 1)]):  # TMDB says E1 exists (and it's present)
            with patch('utils.arr_client.get_download_service', return_value=(mock_sonarr, 'sonarr')):
                with patch('utils.tmdb.search_show', return_value=None):
                    scanner._search_for_missing_episodes([show], [], {'tulsa king': 'prefer-debrid'})
        mock_sonarr.ensure_and_search.assert_called_once()
        _, kwargs = mock_sonarr.ensure_and_search.call_args
        assert kwargs.get('prefer_debrid') is True
        # prefer-debrid honors Sonarr's monitored flag (same as every other route):
        # unmonitoring a season in Sonarr must suppress gap-fill for that season,
        # even under prefer-debrid force-grab semantics.
        assert kwargs.get('respect_monitored') is True

    def test_pending_direction_is_to_debrid(self, scanner, mock_sonarr):
        show = {
            'title': 'Tulsa King', 'year': None,
            'season_data': [{'number': 1, 'episodes': [{'number': 1, 'source': 'local'}]}],
        }
        with _stub_tmdb_episodes([(1, 1)]):
            with patch('utils.arr_client.get_download_service', return_value=(mock_sonarr, 'sonarr')):
                with patch('utils.tmdb.search_show', return_value=None):
                    scanner._search_for_missing_episodes([show], [], {'tulsa king': 'prefer-debrid'})
        entry = lp.get_all_pending().get('tulsa king')
        assert entry['direction'] == 'to-debrid'


class TestGapFillDisabled:

    def test_gap_fill_off_skips_missing_anywhere(self, scanner, monkeypatch, mock_sonarr):
        monkeypatch.setenv('GAP_FILL_ENABLED', 'false')
        show = _show('Lucky Hank', [(1, 1)])  # TMDB says E2 exists, scan has nothing
        with _stub_tmdb_episodes([(1, 1), (1, 2)]):
            with patch('utils.arr_client.get_download_service', return_value=(mock_sonarr, 'sonarr')):
                scanner._search_for_missing_episodes([show], [], {})
        mock_sonarr.ensure_and_search.assert_not_called()

    def test_gap_fill_off_preserves_prefer_debrid(self, scanner, monkeypatch, mock_sonarr):
        """Legacy prefer-debrid local-only path must keep working even with gap-fill off."""
        monkeypatch.setenv('GAP_FILL_ENABLED', 'false')
        show = {
            'title': 'Tulsa King', 'year': None,
            'season_data': [{'number': 1, 'episodes': [{'number': 1, 'source': 'local'}]}],
        }
        with _stub_tmdb_episodes([(1, 1)]):
            with patch('utils.arr_client.get_download_service', return_value=(mock_sonarr, 'sonarr')):
                with patch('utils.tmdb.search_show', return_value=None):
                    scanner._search_for_missing_episodes([show], [], {'tulsa king': 'prefer-debrid'})
        mock_sonarr.ensure_and_search.assert_called_once()


class TestToAnyResolution:
    """to-any must resolve on any source, never escalate."""

    def test_resolves_on_local_source(self, scanner):
        from utils.library_prefs import set_pending, get_all_pending
        set_pending('foo', [{'season': 1, 'episode': 1}], 'to-any')
        # Simulate a scan where the episode is now present locally
        shows = [{
            'title': 'Foo', 'year': None,
            'season_data': [{'number': 1, 'episodes': [{'number': 1, 'source': 'local'}]}],
        }]
        scanner._clear_resolved_pending(shows, [])
        assert 'foo' not in get_all_pending()

    def test_resolves_on_debrid_source(self, scanner):
        from utils.library_prefs import set_pending, get_all_pending
        set_pending('foo', [{'season': 1, 'episode': 1}], 'to-any')
        shows = [{
            'title': 'Foo', 'year': None,
            'season_data': [{'number': 1, 'episodes': [{'number': 1, 'source': 'debrid'}]}],
        }]
        scanner._clear_resolved_pending(shows, [])
        assert 'foo' not in get_all_pending()

    def test_not_escalated_to_debrid_unavailable(self, scanner):
        """to-any entries must never be promoted to debrid-unavailable regardless of age."""
        from utils.library_prefs import _save_pending, _pending_lock, get_all_pending
        old = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat(timespec='seconds')
        with _pending_lock:
            _save_pending({'foo': {
                'direction': 'to-any', 'created': old, 'last_searched': old,
                'episodes': [{'season': 1, 'episode': 1}],
            }})
        scanner._debrid_unavailable_days = 3
        scanner._escalate_stuck_pending()
        assert get_all_pending()['foo']['direction'] == 'to-any'


class TestSkippedStatusHandling:
    """Phase 1.3 hardening: respect_monitored-induced 'skipped' must not
    inflate retry_count or log a misleading error."""

    def test_show_skipped_does_not_touch_pending(self, scanner, mock_sonarr):
        """An unmonitored-all-episodes search result sets cooldown but leaves
        pending state untouched (no retry_count growth)."""
        mock_sonarr.ensure_and_search.return_value = {
            'status': 'skipped', 'message': 'unmonitored', 'service': 'sonarr',
        }
        show = _show('Lucky Hank', [(1, 1)])
        with _stub_tmdb_episodes([(1, 1), (1, 2)]):
            with patch('utils.arr_client.get_download_service', return_value=(mock_sonarr, 'sonarr')):
                with patch('utils.tmdb.search_show', return_value=None):
                    scanner._search_for_missing_episodes([show], [], {})
        # Cooldown applied so next scan doesn't re-hit the same (title, season)
        assert ('lucky hank', 1) in scanner._search_cooldown
        # No pending entry created (skipped != sent/pending)
        assert 'lucky hank' not in lp.get_all_pending()

    def test_movie_skipped_does_not_touch_pending(self, scanner, mock_radarr):
        mock_radarr.ensure_and_search.return_value = {
            'status': 'skipped', 'message': 'unmonitored', 'service': 'radarr',
        }
        movie = {'title': 'Dune', 'year': 2021, 'source': ''}
        with patch('utils.arr_client.get_download_service', return_value=(mock_radarr, 'radarr')):
            with patch('utils.tmdb.search_movie', return_value=None):
                scanner._search_for_missing_episodes([], [movie], {})
        assert ('dune', 0) in scanner._search_cooldown
        assert 'dune' not in lp.get_all_pending()


class TestSonarrSkippedReturn:
    """Phase 1.3: Sonarr's ensure_and_search should return 'skipped' (not
    'error') when respect_monitored drops all matched episodes."""

    def test_all_unmonitored_returns_skipped(self):
        from utils.arr_client import SonarrClient
        c = SonarrClient.__new__(SonarrClient)
        c.url = 'http://fake'
        c.api_key = 'k'
        with patch.object(c, 'find_series_in_library', return_value={'id': 1, 'title': 'X'}):
            with patch.object(c, 'get_episodes', return_value=[
                {'id': 10, 'seasonNumber': 1, 'episodeNumber': 1, 'monitored': False, 'hasFile': False},
                {'id': 11, 'seasonNumber': 1, 'episodeNumber': 2, 'monitored': False, 'hasFile': False},
            ]):
                with patch.object(c, '_ensure_debrid_routing', side_effect=lambda s: s):
                    result = c.ensure_and_search(
                        'X', None, 1, [1, 2], prefer_debrid=True, respect_monitored=True,
                    )
        assert result['status'] == 'skipped'


class TestDirectionChangeClearsStale:
    """Phase 1.2 hardening: a direction change (e.g., user flips preference)
    must clear the stale-direction pending entry rather than leaving it
    to be poisoned by subsequent error-path writes."""

    def test_to_debrid_entry_cleared_when_route_changes_to_none(self, scanner, mock_sonarr):
        # Seed a stale to-debrid entry
        from datetime import datetime as _dt, timezone as _tz, timedelta as _td
        old = (_dt.now(_tz.utc) - _td(hours=7)).isoformat(timespec='seconds')
        with lp._pending_lock:
            lp._save_pending({'lucky hank': {
                'direction': 'to-debrid', 'created': old, 'last_searched': old,
                'last_error': 'stale from old preference',
                'episodes': [{'season': 1, 'episode': 1}],
            }})
        # Flip to unset preference — direction becomes to-any
        show = _show('Lucky Hank', [(1, 1)])
        # Sonarr error so no new pending is written — this exposes the zombie
        mock_sonarr.ensure_and_search.return_value = {'status': 'error', 'message': 'no indexers'}
        with _stub_tmdb_episodes([(1, 1), (1, 2)]):
            with patch('utils.arr_client.get_download_service', return_value=(mock_sonarr, 'sonarr')):
                with patch('utils.tmdb.search_show', return_value=None):
                    scanner._search_for_missing_episodes([show], [], {})
        # The old to-debrid entry must be gone — not zombified with new error
        entry = lp.get_all_pending().get('lucky hank')
        # Either fully cleared (no entry) or rewritten under new direction; never
        # the old direction with the stale 'last_error' preserved.
        if entry is not None:
            assert entry.get('direction') != 'to-debrid'
            assert entry.get('last_error') != 'stale from old preference'


class TestWarnStalledIncludesToAny:
    """Phase 1.4 hardening: to-any entries must still trigger stall warnings
    after the threshold so users learn about long-standing gaps (even though
    to-any is never escalated to debrid-unavailable)."""

    def test_to_any_warned_after_threshold(self, scanner):
        from datetime import datetime as _dt, timezone as _tz, timedelta as _td
        scanner._pending_warning_hours = 24
        old = (_dt.now(_tz.utc) - _td(hours=48)).isoformat(timespec='seconds')
        with lp._pending_lock:
            lp._save_pending({'lucky hank': {
                'direction': 'to-any', 'created': old, 'last_searched': old,
                'episodes': [{'season': 1, 'episode': 4}],
            }})
        with patch('utils.notifications.notify') as mock_notify:
            scanner._warn_stalled_pending()
        # warned_at should be set on the entry
        entry = lp.get_all_pending()['lucky hank']
        assert 'warned_at' in entry


class TestMovieGapFill:

    def test_missing_movie_searched_under_unset_preference(self, scanner, mock_radarr):
        movie = {'title': 'Dune', 'year': 2021, 'source': ''}  # missing everywhere
        with patch('utils.arr_client.get_download_service', return_value=(mock_radarr, 'radarr')):
            with patch('utils.tmdb.search_movie', return_value=None):
                scanner._search_for_missing_episodes([], [movie], {})
        mock_radarr.ensure_and_search.assert_called_once()
        _, kwargs = mock_radarr.ensure_and_search.call_args
        assert kwargs.get('prefer_debrid') is None
        assert kwargs.get('respect_monitored') is True

    def test_movie_already_on_local_not_searched_under_unset(self, scanner, mock_radarr):
        movie = {'title': 'Dune', 'year': 2021, 'source': 'local'}  # already available
        with patch('utils.arr_client.get_download_service', return_value=(mock_radarr, 'radarr')):
            scanner._search_for_missing_episodes([], [movie], {})
        mock_radarr.ensure_and_search.assert_not_called()
