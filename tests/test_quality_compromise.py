"""Tests for utils/quality_compromise.py — plan 33 Phase 4 decision engine."""

import os
import sys
from unittest.mock import MagicMock, patch

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.quality_compromise import (
    find_compromise_candidate,
    find_season_pack_candidate,
    should_compromise,
)


NOW = 1_713_664_800.0
DWELL_3D = 3 * 86400


def _tier_state(tier_order=('2160p', '1080p', '720p'), current=0,
                first_attempted_at=NOW):
    # should_compromise doesn't inspect schema_version — it reads the
    # validated dict from RetryMeta.read_tier_state — so any plausible
    # version works here.  Keep the synthesizer aligned with the current
    # writer so future schema bumps keep this test-only helper in sync
    # without requiring hand-edits across every should_compromise case.
    from utils.blackhole import RetryMeta
    return {
        'schema_version': RetryMeta.TIER_STATE_SCHEMA_VERSION,
        'arr_service': 'sonarr',
        'arr_url_hash': 'abcdef',
        'profile_id': 4,
        'tier_order': list(tier_order),
        'current_tier_index': current,
        'first_attempted_at': first_attempted_at,
        'tier_attempts': [],
        'compromise_fired_at': None,
        'last_advance_reason': None,
        'season_pack_attempted': False,
    }


def _result(info_hash='a' * 40, label='1080p', seeds=50, size_bytes=4_000_000_000,
            cached=True, title='Show.S01E01.1080p.BluRay-GROUP'):
    return {
        'title': title,
        'info_hash': info_hash,
        'size_bytes': size_bytes,
        'seeds': seeds,
        'source_name': 'Torrentio',
        'quality': {'label': label, 'score': 3 if label == '1080p' else 4},
        'cached': cached,
        'cached_service': 'alldebrid',
    }


# ---------------------------------------------------------------------------
# should_compromise — pure logic
# ---------------------------------------------------------------------------

def test_should_compromise_dwell_not_elapsed():
    state = _tier_state(first_attempted_at=NOW - 3600)  # 1h ago
    action, reason = should_compromise(state, NOW, DWELL_3D, only_cached=True)
    assert (action, reason) == ('stay', 'dwell_not_elapsed')


def test_should_compromise_dwell_elapsed_advance():
    state = _tier_state(first_attempted_at=NOW - (DWELL_3D + 60))
    action, reason = should_compromise(state, NOW, DWELL_3D, only_cached=True)
    assert (action, reason) == ('advance', 'dwell_elapsed')


def test_should_compromise_no_more_tiers():
    # current_tier_index at the last entry — nothing below it in the profile
    state = _tier_state(tier_order=('2160p', '1080p'), current=1,
                        first_attempted_at=NOW - (DWELL_3D + 60))
    action, reason = should_compromise(state, NOW, DWELL_3D, only_cached=True)
    assert action == 'exhausted'
    assert reason == 'no_lower_tier_in_profile'


def test_should_compromise_none_tier_state_stays():
    action, reason = should_compromise(None, NOW, DWELL_3D, only_cached=True)
    assert action == 'stay'
    assert 'legacy' in reason  # explanatory, not a generic 'stay'


def test_should_compromise_non_dict_tier_state_rejected():
    # Defensive: a corrupt sidecar deserialised as the wrong type must
    # not crash the decision loop via AttributeError on .get().
    for bad in ([1, 2, 3], 'not-a-dict', 42, True):
        action, reason = should_compromise(bad, NOW, DWELL_3D, only_cached=True)
        assert action == 'stay'
        assert reason == 'invalid_tier_state'


def test_should_compromise_non_list_tier_order_rejected():
    state = _tier_state()
    state['tier_order'] = '2160p'  # string, not list — len() is truthy but wrong
    action, reason = should_compromise(state, NOW, DWELL_3D, only_cached=True)
    assert (action, reason) == ('stay', 'invalid_tier_state')


# ---------------------------------------------------------------------------
# find_compromise_candidate — filter chain
# ---------------------------------------------------------------------------

@patch('utils.quality_compromise.is_blocked')
@patch('utils.quality_compromise.search_torrents')
def test_find_compromise_candidate_filters_blocklist(mock_search, mock_blocked):
    blocked_hash = 'b' * 40
    good_hash = 'c' * 40
    mock_search.return_value = [
        _result(info_hash=blocked_hash, seeds=200),
        _result(info_hash=good_hash, seeds=10),
    ]
    mock_blocked.side_effect = lambda h: h == blocked_hash

    winner = find_compromise_candidate(
        arr_client=MagicMock(), imdb_id='tt1234567', tier_label='1080p',
        min_seeders=1, only_cached=True, context={'media_type': 'movie'},
    )
    assert winner is not None
    assert winner['info_hash'] == good_hash


@patch('utils.quality_compromise.is_blocked', return_value=False)
@patch('utils.quality_compromise.search_torrents')
def test_find_compromise_candidate_filters_seeders(mock_search, _blocked):
    mock_search.return_value = [
        _result(info_hash='a' * 40, seeds=1),
        _result(info_hash='b' * 40, seeds=5),
    ]
    winner = find_compromise_candidate(
        arr_client=MagicMock(), imdb_id='tt1', tier_label='1080p',
        min_seeders=3, only_cached=True, context={'media_type': 'movie'},
    )
    assert winner is not None
    assert winner['seeds'] == 5


@patch('utils.quality_compromise.is_blocked', return_value=False)
@patch('utils.quality_compromise.search_torrents')
def test_find_compromise_candidate_requires_cached_by_default(mock_search, _blocked):
    # Uncached + unknown-cache releases must be rejected under only_cached=True (I4)
    mock_search.return_value = [
        _result(info_hash='a' * 40, cached=False, seeds=999),
        _result(info_hash='b' * 40, cached=None, seeds=888),
    ]
    winner = find_compromise_candidate(
        arr_client=MagicMock(), imdb_id='tt1', tier_label='1080p',
        min_seeders=1, only_cached=True, context={'media_type': 'movie'},
    )
    assert winner is None


@patch('utils.quality_compromise.is_blocked', return_value=False)
@patch('utils.quality_compromise.search_torrents')
def test_find_compromise_candidate_cached_false_allowed_when_only_cached_off(
        mock_search, _blocked):
    mock_search.return_value = [
        _result(info_hash='a' * 40, cached=False, seeds=10),
    ]
    winner = find_compromise_candidate(
        arr_client=MagicMock(), imdb_id='tt1', tier_label='1080p',
        min_seeders=1, only_cached=False, context={'media_type': 'movie'},
    )
    assert winner is not None
    assert winner['cached'] is False


@patch('utils.quality_compromise.is_blocked', return_value=False)
@patch('utils.quality_compromise.search_torrents')
def test_find_compromise_candidate_respects_quality_label(mock_search, _blocked):
    # I1: a 2160p release must not be returned when tier_label='1080p'
    mock_search.return_value = [
        _result(info_hash='a' * 40, label='2160p', seeds=999),
        _result(info_hash='b' * 40, label='720p', seeds=777),
    ]
    winner = find_compromise_candidate(
        arr_client=MagicMock(), imdb_id='tt1', tier_label='1080p',
        min_seeders=1, only_cached=True, context={'media_type': 'movie'},
    )
    assert winner is None


@patch('utils.quality_compromise.is_blocked', return_value=False)
@patch('utils.quality_compromise.search_torrents')
def test_find_compromise_candidate_none_tier_label_rejects_all(mock_search, _blocked):
    # I1 hardening: tier_label=None must not allow releases whose quality
    # parser also failed (label=None) to slip through via `None == None`.
    mock_search.return_value = [
        {'title': 'Movie.2024.Unknown', 'info_hash': 'a' * 40,
         'seeds': 500, 'size_bytes': 1_000_000_000,
         'quality': {'label': None, 'score': 0}, 'cached': True},
    ]
    winner = find_compromise_candidate(
        arr_client=MagicMock(), imdb_id='tt1', tier_label=None,
        min_seeders=1, only_cached=True, context={'media_type': 'movie'},
    )
    assert winner is None


@patch('utils.quality_compromise.is_blocked', return_value=False)
@patch('utils.quality_compromise.search_torrents')
def test_find_compromise_candidate_sort_within_tier(mock_search, _blocked):
    # At the same tier, highest seeders wins; ties broken by smaller size.
    mock_search.return_value = [
        _result(info_hash='a' * 40, seeds=10, size_bytes=4_000_000_000),
        _result(info_hash='b' * 40, seeds=100, size_bytes=60_000_000_000),  # biggest seeders
        _result(info_hash='c' * 40, seeds=100, size_bytes=8_000_000_000),   # tied seeders, smaller
        _result(info_hash='d' * 40, seeds=50, size_bytes=5_000_000_000),
    ]
    winner = find_compromise_candidate(
        arr_client=MagicMock(), imdb_id='tt1', tier_label='1080p',
        min_seeders=1, only_cached=True, context={'media_type': 'movie'},
    )
    # Tie on seeders (100) → smaller size wins: 8 GB over 60 GB
    assert winner['info_hash'] == 'c' * 40


# ---------------------------------------------------------------------------
# find_season_pack_candidate
# ---------------------------------------------------------------------------

def _mock_arr(episodes, imdb_id='tt7654321'):
    client = MagicMock()
    client.get_episodes.return_value = episodes
    client.get_series.return_value = {'imdbId': imdb_id, 'id': 42}
    return client


def _episode(season, episode, has_file):
    return {'seasonNumber': season, 'episodeNumber': episode, 'hasFile': has_file}


@patch('utils.quality_compromise.is_blocked', return_value=False)
@patch('utils.quality_compromise.search_torrents')
def test_find_season_pack_candidate_triggers_at_threshold(mock_search, _blocked):
    # 3 missing < min_missing=4 → no probe, return None without hitting search
    episodes = [
        _episode(1, 1, False), _episode(1, 2, False), _episode(1, 3, False),
        _episode(1, 4, True),  _episode(1, 5, True),
    ]
    arr = _mock_arr(episodes)
    winner = find_season_pack_candidate(
        arr_client=arr, series_id=42, season_number=1, tier_label='1080p',
        min_missing=4, min_seeders=1, only_cached=True,
    )
    assert winner is None
    mock_search.assert_not_called()

    # Bump to 4 missing → probe fires
    episodes.append(_episode(1, 6, False))
    arr = _mock_arr(episodes)
    mock_search.return_value = [
        _result(title='Show.S01.1080p.BluRay-GROUP', label='1080p', seeds=50),
    ]
    winner = find_season_pack_candidate(
        arr_client=arr, series_id=42, season_number=1, tier_label='1080p',
        min_missing=4, min_seeders=1, only_cached=True,
    )
    assert winner is not None
    mock_search.assert_called_once()


@patch('utils.quality_compromise.is_blocked', return_value=False)
@patch('utils.quality_compromise.search_torrents')
def test_find_season_pack_candidate_respects_tier(mock_search, _blocked):
    episodes = [_episode(1, i, False) for i in range(1, 11)]
    arr = _mock_arr(episodes)
    # 2160p pack exists; 1080p tier requested → candidate rejected (I1)
    mock_search.return_value = [
        _result(title='Show.S01.2160p.REMUX-GROUP', label='2160p', seeds=500,
                info_hash='a' * 40),
    ]
    winner = find_season_pack_candidate(
        arr_client=arr, series_id=42, season_number=1, tier_label='1080p',
        min_missing=4, min_seeders=1, only_cached=True,
    )
    assert winner is None


@patch('utils.quality_compromise.is_blocked', return_value=False)
@patch('utils.quality_compromise.search_torrents')
def test_find_season_pack_candidate_coerces_string_season_number(mock_search, _blocked):
    # Defensive: season_number arrives as "2" from pending_monitors.json
    # or a URL param — the :02d format spec would crash on a string.
    episodes = [_episode(2, i, False) for i in range(1, 11)]
    arr = _mock_arr(episodes)
    mock_search.return_value = [
        _result(title='Show.S02.1080p.BluRay-GROUP', label='1080p', seeds=50),
    ]
    winner = find_season_pack_candidate(
        arr_client=arr, series_id=42, season_number='2', tier_label='1080p',
        min_missing=4, min_seeders=1, only_cached=True,
    )
    assert winner is not None

    # Non-coercible value → None, no crash
    winner = find_season_pack_candidate(
        arr_client=arr, series_id=42, season_number='not-a-number',
        tier_label='1080p', min_missing=4, min_seeders=1, only_cached=True,
    )
    assert winner is None


@patch('utils.quality_compromise.is_blocked', return_value=False)
@patch('utils.quality_compromise.search_torrents')
def test_find_season_pack_candidate_uses_is_multi_season_pack_detector(
        mock_search, _blocked):
    episodes = [_episode(2, i, False) for i in range(1, 11)]
    arr = _mock_arr(episodes)
    # Multi-season "S01-S05" pack covers season 2 via the regex detector;
    # an episode-scoped release at the same tier must be excluded because
    # neither the multi-season regex nor the single-season token fires
    # for a title carrying an SxxEyy token.
    mock_search.return_value = [
        _result(title='Show.S01-S05.1080p.BluRay-GROUP', label='1080p',
                seeds=200, info_hash='a' * 40),
        _result(title='Show.S02E05.1080p.WEB-DL-GROUP', label='1080p',
                seeds=999, info_hash='b' * 40),  # must be rejected (episode token)
    ]
    winner = find_season_pack_candidate(
        arr_client=arr, series_id=42, season_number=2, tier_label='1080p',
        min_missing=4, min_seeders=1, only_cached=True,
    )
    assert winner is not None
    assert winner['info_hash'] == 'a' * 40
