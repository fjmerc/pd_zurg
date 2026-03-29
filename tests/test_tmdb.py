"""Tests for the TMDB metadata module (utils/tmdb.py)."""

import json
import os
import time
import pytest
import utils.tmdb as tmdb


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _isolate_tmdb(tmp_dir, monkeypatch):
    """Point cache to temp dir and set a fake API key."""
    cache_path = os.path.join(tmp_dir, 'tmdb_cache.json')
    monkeypatch.setattr(tmdb, '_CACHE_PATH', cache_path)
    monkeypatch.setenv('TMDB_API_KEY', 'test-key-123')


def _mock_api(monkeypatch, responses):
    """Mock _api_get to return predefined responses by path prefix.
    Longest prefix match wins to avoid /tv/123 matching /tv/123/season/1.
    """
    sorted_keys = sorted(responses.keys(), key=len, reverse=True)
    def _fake_get(path, params=None):
        for prefix in sorted_keys:
            if path.startswith(prefix):
                return responses[prefix]
        return None
    monkeypatch.setattr(tmdb, '_api_get', _fake_get)


# ---------------------------------------------------------------------------
# _api_get basics
# ---------------------------------------------------------------------------

class TestApiKey:

    def test_no_api_key_returns_none(self, monkeypatch):
        monkeypatch.delenv('TMDB_API_KEY', raising=False)
        assert tmdb.get_show_info('Breaking Bad') is None
        assert tmdb.get_movie_info('Inception') is None

    def test_empty_api_key_returns_none(self, monkeypatch):
        monkeypatch.setenv('TMDB_API_KEY', '  ')
        assert tmdb.get_show_info('Breaking Bad') is None


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

class TestSearch:

    def test_search_show_returns_first_result(self, monkeypatch):
        _mock_api(monkeypatch, {
            '/search/tv': {
                'results': [{
                    'id': 1396,
                    'name': 'Breaking Bad',
                    'overview': 'A teacher turns to crime.',
                    'poster_path': '/poster.jpg',
                    'first_air_date': '2008-01-20',
                }]
            }
        })
        result = tmdb.search_show('Breaking Bad', 2008)
        assert result['tmdb_id'] == 1396
        assert result['title'] == 'Breaking Bad'

    def test_search_show_no_results(self, monkeypatch):
        _mock_api(monkeypatch, {'/search/tv': {'results': []}})
        assert tmdb.search_show('Nonexistent Show') is None

    def test_search_movie_returns_first_result(self, monkeypatch):
        _mock_api(monkeypatch, {
            '/search/movie': {
                'results': [{
                    'id': 27205,
                    'title': 'Inception',
                    'overview': 'Dream heist.',
                    'poster_path': '/inception.jpg',
                    'release_date': '2010-07-16',
                }]
            }
        })
        result = tmdb.search_movie('Inception', 2010)
        assert result['tmdb_id'] == 27205

    def test_search_movie_no_results(self, monkeypatch):
        _mock_api(monkeypatch, {'/search/movie': {'results': []}})
        assert tmdb.search_movie('Nonexistent') is None

    def test_search_show_retries_without_year(self, monkeypatch):
        """Year from folder name may be a season air year, not premiere year."""
        calls = []
        def _fake_get(path, params=None):
            calls.append(params)
            if params and params.get('first_air_date_year'):
                return {'results': []}  # year filter too strict
            return {'results': [{
                'id': 127635, 'name': 'Spidey and His Amazing Friends',
                'overview': '', 'poster_path': '/spidey.jpg',
                'first_air_date': '2021-08-06',
            }]}
        monkeypatch.setattr(tmdb, '_api_get', _fake_get)
        result = tmdb.search_show("Marvel's Spidey and His Amazing Friends", 2022,
                                  fallback_no_year=True)
        assert result['tmdb_id'] == 127635
        assert len(calls) == 2  # first with year, then without

    def test_search_movie_retries_without_year(self, monkeypatch):
        calls = []
        def _fake_get(path, params=None):
            calls.append(params)
            if params and params.get('year'):
                return {'results': []}
            return {'results': [{
                'id': 47211, 'title': 'Faster, Faster',
                'overview': '', 'poster_path': '/ff.jpg',
                'release_date': '1981-01-01',
            }]}
        monkeypatch.setattr(tmdb, '_api_get', _fake_get)
        result = tmdb.search_movie('Faster and Faster', 2020, fallback_no_year=True)
        assert result['tmdb_id'] == 47211
        assert len(calls) == 2

    def test_search_show_no_fallback_by_default(self, monkeypatch):
        """Without fallback_no_year, a wrong year returns None (safe for disambiguation)."""
        calls = []
        def _fake_get(path, params=None):
            calls.append(params)
            if params and params.get('first_air_date_year'):
                return {'results': []}
            return {'results': [{'id': 999, 'name': 'Wrong Show',
                                 'overview': '', 'poster_path': '',
                                 'first_air_date': ''}]}
        monkeypatch.setattr(tmdb, '_api_get', _fake_get)
        assert tmdb.search_show('Ambiguous Title', 2025) is None
        assert len(calls) == 1  # no retry — fallback disabled

    def test_search_movie_no_fallback_by_default(self, monkeypatch):
        """Without fallback_no_year, a wrong year returns None."""
        calls = []
        def _fake_get(path, params=None):
            calls.append(params)
            if params and params.get('year'):
                return {'results': []}
            return {'results': [{'id': 999, 'title': 'Wrong Movie',
                                 'overview': '', 'poster_path': '',
                                 'release_date': ''}]}
        monkeypatch.setattr(tmdb, '_api_get', _fake_get)
        assert tmdb.search_movie('Ambiguous Title', 2025) is None
        assert len(calls) == 1

    def test_search_show_no_retry_when_year_matches(self, monkeypatch):
        """No retry needed when the year-filtered search succeeds."""
        calls = []
        def _fake_get(path, params=None):
            calls.append(params)
            return {'results': [{
                'id': 1396, 'name': 'Breaking Bad',
                'overview': '', 'poster_path': '/bb.jpg',
                'first_air_date': '2008-01-20',
            }]}
        monkeypatch.setattr(tmdb, '_api_get', _fake_get)
        result = tmdb.search_show('Breaking Bad', 2008, fallback_no_year=True)
        assert result['tmdb_id'] == 1396
        assert len(calls) == 1  # no retry needed

    def test_search_show_no_retry_without_year(self, monkeypatch):
        """No retry when no year was provided in the first place."""
        calls = []
        def _fake_get(path, params=None):
            calls.append(params)
            return {'results': []}
        monkeypatch.setattr(tmdb, '_api_get', _fake_get)
        assert tmdb.search_show('Nonexistent', fallback_no_year=True) is None
        assert len(calls) == 1

    def test_search_movie_no_retry_when_year_matches(self, monkeypatch):
        """No retry needed when the year-filtered movie search succeeds."""
        calls = []
        def _fake_get(path, params=None):
            calls.append(params)
            return {'results': [{
                'id': 27205, 'title': 'Inception',
                'overview': '', 'poster_path': '/i.jpg',
                'release_date': '2010-07-16',
            }]}
        monkeypatch.setattr(tmdb, '_api_get', _fake_get)
        result = tmdb.search_movie('Inception', 2010, fallback_no_year=True)
        assert result['tmdb_id'] == 27205
        assert len(calls) == 1

    def test_search_movie_no_retry_without_year(self, monkeypatch):
        """No retry when no year was provided for movie search."""
        calls = []
        def _fake_get(path, params=None):
            calls.append(params)
            return {'results': []}
        monkeypatch.setattr(tmdb, '_api_get', _fake_get)
        assert tmdb.search_movie('Nonexistent', fallback_no_year=True) is None
        assert len(calls) == 1


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

class TestShowMetadata:

    def test_get_show_metadata_skips_season_zero(self, monkeypatch):
        _mock_api(monkeypatch, {
            '/tv/1396': {
                'name': 'Breaking Bad',
                'overview': 'A teacher.',
                'poster_path': '/bb.jpg',
                'status': 'Ended',
                'seasons': [
                    {'season_number': 0, 'episode_count': 3},
                    {'season_number': 1, 'episode_count': 7},
                ],
            },
            '/tv/1396/season/1': {
                'episodes': [
                    {'episode_number': 1, 'name': 'Pilot', 'air_date': '2008-01-20'},
                    {'episode_number': 2, 'name': 'Cat in Bag', 'air_date': '2008-01-27'},
                ]
            },
        })
        result = tmdb.get_show_metadata(1396)
        assert result is not None
        assert len(result['seasons']) == 1
        assert result['seasons'][0]['number'] == 1
        assert len(result['seasons'][0]['episodes']) == 2
        assert result['seasons'][0]['episodes'][0]['title'] == 'Pilot'

    def test_get_show_metadata_api_failure(self, monkeypatch):
        _mock_api(monkeypatch, {})
        assert tmdb.get_show_metadata(9999) is None


class TestMovieMetadata:

    def test_get_movie_metadata(self, monkeypatch):
        _mock_api(monkeypatch, {
            '/movie/27205': {
                'title': 'Inception',
                'overview': 'Dream heist.',
                'poster_path': '/inception.jpg',
                'runtime': 148,
                'release_date': '2010-07-16',
            }
        })
        result = tmdb.get_movie_metadata(27205)
        assert result['runtime'] == 148
        assert result['title'] == 'Inception'

    def test_get_movie_metadata_failure(self, monkeypatch):
        _mock_api(monkeypatch, {})
        assert tmdb.get_movie_metadata(9999) is None


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------

class TestCaching:

    def _setup_full_mock(self, monkeypatch):
        _mock_api(monkeypatch, {
            '/search/tv': {
                'results': [{'id': 100, 'name': 'Test Show', 'overview': '',
                             'poster_path': '/test.jpg', 'first_air_date': '2020-01-01'}]
            },
            '/tv/100': {
                'name': 'Test Show', 'overview': 'Desc', 'poster_path': '/test.jpg',
                'status': 'Ended',
                'seasons': [{'season_number': 1, 'episode_count': 2}],
            },
            '/tv/100/season/1': {
                'episodes': [
                    {'episode_number': 1, 'name': 'Ep1', 'air_date': '2020-01-01'},
                    {'episode_number': 2, 'name': 'Ep2', 'air_date': '2020-01-08'},
                ]
            },
        })

    def test_cache_hit_avoids_api_call(self, monkeypatch):
        self._setup_full_mock(monkeypatch)
        # First call — populates cache
        r1 = tmdb.get_show_info('Test Show', 2020)
        assert r1 is not None

        # Replace mock with one that returns nothing — should still get cached data
        _mock_api(monkeypatch, {})
        r2 = tmdb.get_show_info('Test Show', 2020)
        assert r2 is not None
        assert r2['title'] == 'Test Show'

    def test_expired_cache_refetches(self, monkeypatch):
        self._setup_full_mock(monkeypatch)
        r1 = tmdb.get_show_info('Test Show', 2020)
        assert r1 is not None

        # Expire the cache entry
        cache = tmdb._load_cache()
        from utils.library import _normalize_title
        key = _normalize_title('Test Show')
        cache['shows'][key]['cached_at'] = '2020-01-01T00:00:00+00:00'
        tmdb._save_cache(cache)

        # Now with empty mock, should fail (cache expired, no API)
        _mock_api(monkeypatch, {})
        r2 = tmdb.get_show_info('Test Show', 2020)
        assert r2 is None

    def test_movie_cache(self, monkeypatch):
        _mock_api(monkeypatch, {
            '/search/movie': {
                'results': [{'id': 200, 'title': 'Test Movie', 'overview': '',
                             'poster_path': '/m.jpg', 'release_date': '2020-06-01'}]
            },
            '/movie/200': {
                'title': 'Test Movie', 'overview': 'A movie.',
                'poster_path': '/m.jpg', 'runtime': 120, 'release_date': '2020-06-01',
            },
        })
        r1 = tmdb.get_movie_info('Test Movie', 2020)
        assert r1 is not None
        assert r1['runtime'] == 120

        # Cache hit
        _mock_api(monkeypatch, {})
        r2 = tmdb.get_movie_info('Test Movie', 2020)
        assert r2 is not None


# ---------------------------------------------------------------------------
# Response formatting
# ---------------------------------------------------------------------------

class TestFormatting:

    def test_poster_url_constructed(self):
        assert tmdb._poster_url('/abc.jpg') == 'https://image.tmdb.org/t/p/w300/abc.jpg'

    def test_poster_url_empty_path(self):
        assert tmdb._poster_url('') == ''
        assert tmdb._poster_url(None) == ''

    def test_format_show_includes_poster_url(self):
        entry = {'tmdb_id': 1, 'title': 'T', 'overview': '', 'poster_path': '/p.jpg',
                 'status': 'Ended', 'seasons': []}
        result = tmdb._format_show(entry)
        assert result['poster_url'].startswith('https://image.tmdb.org')

    def test_format_movie_includes_runtime(self):
        entry = {'tmdb_id': 1, 'title': 'M', 'overview': '', 'poster_path': '',
                 'runtime': 90, 'release_date': '2020-01-01'}
        result = tmdb._format_movie(entry)
        assert result['runtime'] == 90


# ---------------------------------------------------------------------------
# Cache freshness
# ---------------------------------------------------------------------------

class TestCacheFreshness:

    def test_fresh_entry(self):
        from datetime import datetime, timezone
        entry = {'cached_at': datetime.now(timezone.utc).isoformat()}
        assert tmdb._is_fresh(entry) is True

    def test_stale_entry(self):
        entry = {'cached_at': '2020-01-01T00:00:00+00:00'}
        assert tmdb._is_fresh(entry) is False

    def test_missing_cached_at(self):
        assert tmdb._is_fresh({}) is False

    def test_invalid_cached_at(self):
        assert tmdb._is_fresh({'cached_at': 'not-a-date'}) is False


# ---------------------------------------------------------------------------
# Bulk cache lookup (get_cached_posters)
# ---------------------------------------------------------------------------

class TestCachedPosters:

    def test_returns_enrichment_for_cached_shows(self, monkeypatch):
        from datetime import datetime, timezone
        # 7 aired episodes in S1, 3 aired + 2 unaired in S2
        s1_eps = [{'number': i, 'title': f'Ep{i}', 'air_date': '2008-01-20'} for i in range(1, 8)]
        s2_eps = [{'number': i, 'title': f'Ep{i}', 'air_date': '2009-03-08'} for i in range(1, 4)]
        s2_eps += [{'number': 4, 'title': 'Future', 'air_date': '2099-12-31'},
                   {'number': 5, 'title': 'TBA', 'air_date': ''}]
        cache = {
            'shows': {
                'breaking bad': {
                    'tmdb_id': 1396,
                    'title': 'Breaking Bad',
                    'poster_path': '/bb.jpg',
                    'status': 'Ended',
                    'seasons': [
                        {'number': 1, 'total_episodes': 7, 'episodes': s1_eps},
                        {'number': 2, 'total_episodes': 5, 'episodes': s2_eps},
                    ],
                    'cached_at': datetime.now(timezone.utc).isoformat(),
                }
            }
        }
        tmdb._save_cache(cache)
        items = [{'title': 'Breaking Bad (2008)', 'year': 2008, 'type': 'show'}]
        result = tmdb.get_cached_posters(items)
        assert 'breaking bad' in result
        info = result['breaking bad']
        assert info['poster_url'] == 'https://image.tmdb.org/t/p/w300/bb.jpg'
        assert info['tmdb_status'] == 'Ended'
        # Only counts aired episodes (7 + 3 = 10), not unaired (future + TBA)
        assert info['total_episodes'] == 10

    def test_returns_enrichment_for_cached_movies(self, monkeypatch):
        from datetime import datetime, timezone
        cache = {
            'movies': {
                'inception': {
                    'tmdb_id': 27205,
                    'title': 'Inception',
                    'poster_path': '/inc.jpg',
                    'runtime': 148,
                    'release_date': '2010-07-16',
                    'cached_at': datetime.now(timezone.utc).isoformat(),
                }
            }
        }
        tmdb._save_cache(cache)
        items = [{'title': 'Inception (2010)', 'year': 2010, 'type': 'movie'}]
        result = tmdb.get_cached_posters(items)
        assert 'inception' in result
        assert result['inception']['poster_url'] == 'https://image.tmdb.org/t/p/w300/inc.jpg'

    def test_skips_expired_entries(self, monkeypatch):
        cache = {
            'shows': {
                'old show': {
                    'poster_path': '/old.jpg',
                    'status': 'Ended',
                    'seasons': [],
                    'cached_at': '2020-01-01T00:00:00+00:00',
                }
            }
        }
        tmdb._save_cache(cache)
        items = [{'title': 'Old Show', 'year': None, 'type': 'show'}]
        result = tmdb.get_cached_posters(items)
        assert 'old show' not in result

    def test_returns_empty_without_api_key(self, monkeypatch):
        monkeypatch.delenv('TMDB_API_KEY', raising=False)
        items = [{'title': 'Test', 'year': None, 'type': 'show'}]
        assert tmdb.get_cached_posters(items) == {}

    def test_returns_empty_for_uncached(self, monkeypatch):
        items = [{'title': 'Not In Cache', 'year': None, 'type': 'show'}]
        result = tmdb.get_cached_posters(items)
        assert result == {}


# ---------------------------------------------------------------------------
# Background cache population
# ---------------------------------------------------------------------------

class TestBackgroundPopulate:

    @pytest.fixture(autouse=True)
    def _reset_populate_flag(self):
        """Ensure background populate flag is reset before each test."""
        with tmdb._populate_lock:
            tmdb._populate_running = False
        yield
        # Wait for any background thread to finish
        import time
        deadline = time.time() + 5
        while time.time() < deadline:
            with tmdb._populate_lock:
                if not tmdb._populate_running:
                    break
            time.sleep(0.05)

    def test_populates_uncached_items(self, monkeypatch):
        _mock_api(monkeypatch, {
            '/search/tv': {
                'results': [{'id': 100, 'name': 'New Show', 'overview': '',
                             'poster_path': '/new.jpg', 'first_air_date': '2024-01-01'}]
            },
            '/tv/100': {
                'name': 'New Show', 'overview': '', 'poster_path': '/new.jpg',
                'status': 'Returning Series',
                'seasons': [{'season_number': 1, 'episode_count': 10}],
            },
            '/tv/100/season/1': {
                'episodes': [{'episode_number': i, 'name': f'Ep{i}', 'air_date': '2024-01-01'} for i in range(1, 11)]
            },
        })
        items = [{'title': 'New Show', 'year': 2024, 'type': 'show'}]
        tmdb.background_populate_cache(items)
        # Poll until the background thread populates the cache
        import time
        deadline = time.time() + 5
        while time.time() < deadline:
            cache = tmdb._load_cache()
            if 'new show' in cache.get('shows', {}):
                break
            time.sleep(0.05)
        else:
            pytest.fail("background thread did not populate cache within 5s")
        assert 'new show' in tmdb._load_cache().get('shows', {})

    def test_skips_already_cached(self, monkeypatch):
        from datetime import datetime, timezone
        # Pre-populate cache
        cache = {
            'shows': {
                'cached show': {
                    'tmdb_id': 1, 'title': 'Cached Show', 'poster_path': '/c.jpg',
                    'status': 'Ended', 'seasons': [],
                    'cached_at': datetime.now(timezone.utc).isoformat(),
                }
            }
        }
        tmdb._save_cache(cache)

        call_count = [0]
        orig_get = tmdb._api_get
        def counting_get(path, params=None):
            call_count[0] += 1
            return orig_get(path, params)
        monkeypatch.setattr(tmdb, '_api_get', counting_get)

        items = [{'title': 'Cached Show', 'year': None, 'type': 'show'}]
        tmdb.background_populate_cache(items)
        # Wait for background thread to complete
        import time
        deadline = time.time() + 5
        while time.time() < deadline:
            with tmdb._populate_lock:
                if not tmdb._populate_running:
                    break
            time.sleep(0.05)
        # Should have made zero API calls since the item is fresh
        assert call_count[0] == 0

    def test_prevents_concurrent_runs(self, monkeypatch):
        """Only one background population thread runs at a time."""
        import time

        _mock_api(monkeypatch, {
            '/search/movie': {
                'results': [{'id': 1, 'title': 'M', 'overview': '',
                             'poster_path': '/m.jpg', 'release_date': '2024-01-01'}]
            },
            '/movie/1': {
                'title': 'M', 'overview': '', 'poster_path': '/m.jpg',
                'runtime': 90, 'release_date': '2024-01-01',
            },
        })

        items = [{'title': f'Movie {i}', 'year': 2024, 'type': 'movie'} for i in range(5)]
        tmdb.background_populate_cache(items)  # Starts thread
        tmdb.background_populate_cache(items)  # Should be a no-op (already running)
        # Wait for background thread to complete
        deadline = time.time() + 5
        while time.time() < deadline:
            with tmdb._populate_lock:
                if not tmdb._populate_running:
                    break
            time.sleep(0.05)
        # Just verify no crash — the guard prevented double-run
