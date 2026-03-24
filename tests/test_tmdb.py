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
