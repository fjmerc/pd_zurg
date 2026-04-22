"""Tests for tmdb.get_cached_episode_list (Phase 1.1)."""

import os
from datetime import datetime, timezone

import pytest
import utils.tmdb as tmdb


@pytest.fixture(autouse=True)
def _isolate_tmdb(tmp_dir, monkeypatch):
    cache_path = os.path.join(tmp_dir, 'tmdb_cache.json')
    monkeypatch.setattr(tmdb, '_CACHE_PATH', cache_path)
    monkeypatch.setenv('TMDB_API_KEY', 'test-key')


def _seed_show(title, seasons):
    """Write a show entry directly into the cache.
    seasons: {season_number: [(episode_number, air_date_str), ...]}
    """
    now_iso = datetime.now(timezone.utc).isoformat(timespec='seconds')
    entry = {
        'cached_at': now_iso,
        'title': title,
        'tmdb_id': 1,
        'seasons': [
            {
                'number': sn,
                'episodes': [
                    {'number': en, 'air_date': ad} for en, ad in eps
                ],
            }
            for sn, eps in seasons.items()
        ],
    }
    import json
    cache = {'shows': {title.lower(): entry}, 'movies': {}}
    with open(tmdb._CACHE_PATH, 'w') as f:
        json.dump(cache, f)


class TestCachedEpisodeList:

    def test_returns_aired_episodes_only(self):
        _seed_show('lucky hank', {1: [
            (1, '2023-03-01'), (2, '2023-03-08'), (3, '2023-03-15'),
            (10, '2099-01-01'),  # unaired
        ]})
        result = tmdb.get_cached_episode_list('lucky hank')
        assert [(e['season'], e['number']) for e in result] == [(1, 1), (1, 2), (1, 3)]

    def test_skips_season_zero_specials(self):
        _seed_show('show', {0: [(1, '2020-01-01')], 1: [(1, '2020-06-01')]})
        result = tmdb.get_cached_episode_list('show')
        assert [(e['season'], e['number']) for e in result] == [(1, 1)]

    def test_skips_episodes_with_empty_air_date(self):
        _seed_show('show', {1: [(1, '2020-01-01'), (2, ''), (3, '2020-02-01')]})
        result = tmdb.get_cached_episode_list('show')
        assert [(e['season'], e['number']) for e in result] == [(1, 1), (1, 3)]

    def test_uncached_returns_empty(self):
        """Unknown title returns [] so reconcile doesn't hallucinate episodes."""
        result = tmdb.get_cached_episode_list('never heard of this')
        assert result == []

    def test_empty_title_returns_empty(self):
        assert tmdb.get_cached_episode_list('') == []
        assert tmdb.get_cached_episode_list(None) == []
