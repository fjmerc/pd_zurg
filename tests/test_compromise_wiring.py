"""Tests for plan 33 Phase 5 — blackhole wiring of the compromise engine.

These are integration-level tests that drive ``BlackholeWatcher._try_alt_episode``
(Sonarr) and ``_try_alt_movie`` (Radarr) through the full alt-retry ->
compromise-decision -> candidate-submission pipeline.  The arr clients
and debrid_handler are mocked so the test matrix stays isolated to the
decision logic and state mutation introduced in Phase 5.
"""

import json
import os
import sys
import time
from unittest.mock import MagicMock, patch

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.blackhole import BlackholeWatcher, RetryMeta


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

DWELL_DAYS = 3
DWELL_SECONDS = DWELL_DAYS * 86400


@pytest.fixture
def watcher(tmp_dir):
    """A BlackholeWatcher with symlink mode on — pending_monitors.json
    lives under a separate completed_dir so tests can assert on it.

    Uses ``realdebrid`` because its ``_extract_torrent_id`` simply
    stringifies the debrid response, avoiding the need for test mocks
    to match alldebrid's nested ``{'data': {'magnets': [...]}}`` shape.
    """
    completed = os.path.join(tmp_dir, 'completed')
    os.makedirs(completed, exist_ok=True)
    return BlackholeWatcher(
        watch_dir=tmp_dir, debrid_api_key='k', debrid_service='realdebrid',
        symlink_enabled=True, completed_dir=completed,
        rclone_mount='/data', symlink_target_base='/mnt/debrid',
    )


@pytest.fixture
def compromise_env(monkeypatch):
    """Enable compromise with a short dwell so 'past' timestamps trip it."""
    monkeypatch.setenv('QUALITY_COMPROMISE_ENABLED', 'true')
    monkeypatch.setenv('QUALITY_COMPROMISE_DWELL_DAYS', str(DWELL_DAYS))
    monkeypatch.setenv('QUALITY_COMPROMISE_MIN_SEEDERS', '1')
    monkeypatch.setenv('QUALITY_COMPROMISE_ONLY_CACHED', 'true')


def _seed_series_tier_state(file_path, tier_order=('2160p', '1080p', '720p'),
                            first_attempted_at=None):
    """Seed a RetryMeta tier_state with *first_attempted_at* in the past
    so `should_compromise` sees dwell as elapsed."""
    if first_attempted_at is None:
        first_attempted_at = time.time() - (DWELL_SECONDS + 60)
    RetryMeta.init_tier_state(
        file_path, arr_service='sonarr', arr_url='http://sonarr:8989',
        profile_id=4, tier_order=list(tier_order),
        now=first_attempted_at,
    )


def _mock_series(imdb_id='tt1111111', series_id=42):
    return {'id': series_id, 'imdbId': imdb_id, 'title': 'Show Name'}


def _mock_movie(imdb_id='tt2222222', movie_id=99):
    return {'id': movie_id, 'imdbId': imdb_id, 'title': 'Movie Name'}


def _mock_sonarr_client(series=None, episode_id=1, releases=None,
                        tier_order=('2160p', '1080p', '720p')):
    client = MagicMock()
    client.configured = True
    client.url = 'http://sonarr:8989'
    client.find_series_in_library.return_value = series or _mock_series()
    client.get_episode_id.return_value = episode_id
    client.get_episode_releases.return_value = releases or []
    client.get_profile_id_for_series.return_value = 4
    client.get_tier_order.return_value = list(tier_order)
    client.get_episodes.return_value = []
    client.get_series.return_value = series or _mock_series()
    return client


def _mock_radarr_client(movie=None, releases=None,
                       tier_order=('2160p', '1080p', '720p')):
    client = MagicMock()
    client.configured = True
    client.url = 'http://radarr:7878'
    client.find_movie_in_library.return_value = movie or _mock_movie()
    client.get_movie_releases.return_value = releases or []
    client.get_profile_id_for_movie.return_value = 4
    client.get_tier_order.return_value = list(tier_order)
    return client


def _torrentio_result(info_hash='a' * 40, label='1080p', seeds=50,
                     size_bytes=4_000_000_000, cached=True,
                     title='Show.S01E01.1080p.BluRay-GROUP'):
    return {
        'title': title, 'info_hash': info_hash,
        'size_bytes': size_bytes, 'seeds': seeds,
        'source_name': 'Torrentio',
        'quality': {'label': label, 'score': 3 if label == '1080p' else 4},
        'cached': cached, 'cached_service': 'alldebrid',
    }


def _write_torrent(tmp_dir, name):
    path = os.path.join(tmp_dir, name)
    with open(path, 'w') as f:
        f.write('magnet:?xt=urn:btih:' + ('0' * 40))
    return path


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

def test_compromise_disabled_preserves_today_behavior(watcher, tmp_dir, monkeypatch):
    """When QUALITY_COMPROMISE_ENABLED=false, no compromise logic fires
    even with a fully-seeded tier_state past dwell."""
    monkeypatch.setenv('QUALITY_COMPROMISE_ENABLED', 'false')
    orig_path = _write_torrent(tmp_dir, 'Show.S01E01.torrent')
    _seed_series_tier_state(orig_path)

    client = _mock_sonarr_client()
    debrid = MagicMock(return_value=(False, 'rejected'))

    with patch('utils.arr_client.SonarrClient', return_value=client):
        result = watcher._try_alt_episode(
            'Show Name', 1, [1], debrid,
            'Show.S01E01.torrent', orig_path, label='sonarr',
        )
    assert result is False
    # Tier state is untouched (compromise logic skipped, no advance)
    ts = RetryMeta.read_tier_state(orig_path)
    assert ts['current_tier_index'] == 0


def test_compromise_dwell_gate_blocks_advance(watcher, tmp_dir, compromise_env):
    """Within dwell window → 'stay' → no compromise submission."""
    orig_path = _write_torrent(tmp_dir, 'Show.S01E01.torrent')
    # Attempt just started — way under dwell.
    _seed_series_tier_state(orig_path, first_attempted_at=time.time() - 60)

    client = _mock_sonarr_client()
    debrid = MagicMock(return_value=(False, 'irrelevant'))

    with patch('utils.arr_client.SonarrClient', return_value=client), \
         patch('utils.quality_compromise.search_torrents') as mock_search:
        result = watcher._try_alt_episode(
            'Show Name', 1, [1], debrid,
            'Show.S01E01.torrent', orig_path, label='sonarr',
        )
    assert result is False
    mock_search.assert_not_called()  # never reached the probe


def test_compromise_respects_profile_ceiling(watcher, tmp_dir, compromise_env):
    """tier_order=['2160p'] (no tier below) → 'exhausted' → no submission."""
    orig_path = _write_torrent(tmp_dir, 'Show.S01E01.torrent')
    _seed_series_tier_state(orig_path, tier_order=('2160p',))

    client = _mock_sonarr_client(tier_order=('2160p',))
    debrid = MagicMock(return_value=(False, 'x'))

    with patch('utils.arr_client.SonarrClient', return_value=client), \
         patch('utils.quality_compromise.search_torrents') as mock_search:
        result = watcher._try_alt_episode(
            'Show Name', 1, [1], debrid,
            'Show.S01E01.torrent', orig_path, label='sonarr',
        )
    assert result is False
    mock_search.assert_not_called()


def test_compromise_only_cached_blocks_uncached_advance(watcher, tmp_dir, compromise_env):
    """Past dwell, next-tier candidate exists but cached=False under
    ONLY_CACHED=true → no submission."""
    orig_path = _write_torrent(tmp_dir, 'Show.S01E01.torrent')
    _seed_series_tier_state(orig_path)

    client = _mock_sonarr_client()
    debrid = MagicMock(return_value=(True, '{"id": 321}'))

    with patch('utils.arr_client.SonarrClient', return_value=client), \
         patch('utils.quality_compromise.search_torrents',
               return_value=[_torrentio_result(label='1080p', cached=False)]), \
         patch('utils.quality_compromise.is_blocked', return_value=False):
        result = watcher._try_alt_episode(
            'Show Name', 1, [1], debrid,
            'Show.S01E01.torrent', orig_path, label='sonarr',
        )
    assert result is False
    debrid.assert_not_called()
    # Tier never advanced
    ts = RetryMeta.read_tier_state(orig_path)
    assert ts['current_tier_index'] == 0


def test_compromise_authorized_submits_lower_tier_candidate(
        watcher, tmp_dir, compromise_env):
    """Happy path: past dwell, cached 1080p exists → magnet submitted,
    tier advances, pending_monitors.json annotated."""
    orig_path = _write_torrent(tmp_dir, 'Show.S01E01.torrent')
    _seed_series_tier_state(orig_path)

    client = _mock_sonarr_client()

    submitted_magnets = []
    def fake_debrid(path):
        with open(path) as f:
            submitted_magnets.append(f.read())
        return True, '{"id": 9999}'
    debrid = MagicMock(side_effect=fake_debrid)

    cached_1080p = _torrentio_result(
        info_hash='c' * 40, label='1080p', seeds=120, cached=True,
        title='Show.S01E01.1080p.BluRay-GROUP',
    )

    with patch('utils.arr_client.SonarrClient', return_value=client), \
         patch('utils.quality_compromise.search_torrents',
               return_value=[cached_1080p]), \
         patch('utils.quality_compromise.is_blocked', return_value=False):
        result = watcher._try_alt_episode(
            'Show Name', 1, [1], debrid,
            'Show.S01E01.torrent', orig_path, label='sonarr',
        )
    assert result is True
    assert len(submitted_magnets) == 1
    assert ('c' * 40) in submitted_magnets[0]

    # Tier advanced 0 -> 1 (2160p -> 1080p)
    ts = RetryMeta.read_tier_state(orig_path)
    assert ts['current_tier_index'] == 1

    # Original removed
    assert not os.path.exists(orig_path)


def test_compromise_pending_monitor_annotated(watcher, tmp_dir, compromise_env):
    """After a successful compromise grab, pending_monitors.json carries
    compromised=True + preferred/grabbed tier + strategy."""
    orig_path = _write_torrent(tmp_dir, 'Show.S01E01.torrent')
    _seed_series_tier_state(orig_path)

    client = _mock_sonarr_client()
    debrid = MagicMock(return_value=(True, {'id': 'alldebrid-abc'}))

    cached_1080p = _torrentio_result(label='1080p', cached=True,
                                    title='Show.S01E01.1080p.BluRay-GROUP')

    with patch('utils.arr_client.SonarrClient', return_value=client), \
         patch('utils.quality_compromise.search_torrents',
               return_value=[cached_1080p]), \
         patch('utils.quality_compromise.is_blocked', return_value=False):
        watcher._try_alt_episode(
            'Show Name', 1, [1], debrid,
            'Show.S01E01.torrent', orig_path, label='sonarr',
        )

    with open(watcher._pending_file) as f:
        entries = json.load(f)
    assert len(entries) == 1
    e = entries[0]
    assert e['compromised'] is True
    assert e['preferred_tier'] == '2160p'
    assert e['grabbed_tier'] == '1080p'
    assert e['compromise_strategy'] == 'tier_drop'
    assert e['label'] == 'sonarr'


def test_compromise_season_pack_tried_before_tier_drop(
        watcher, tmp_dir, compromise_env, monkeypatch):
    """With SEASON_PACK_FALLBACK_ENABLED=true, a cached pack at the
    preferred tier grabs first — tier does NOT advance."""
    monkeypatch.setenv('SEASON_PACK_FALLBACK_ENABLED', 'true')
    monkeypatch.setenv('SEASON_PACK_FALLBACK_MIN_MISSING', '2')
    orig_path = _write_torrent(tmp_dir, 'Show.S01E01.torrent')
    _seed_series_tier_state(orig_path)

    # Arr says 3 episodes missing in S01 — threshold (2) met
    missing = [
        {'seasonNumber': 1, 'episodeNumber': i, 'hasFile': False}
        for i in (1, 2, 3)
    ]
    client = _mock_sonarr_client()
    client.get_episodes.return_value = missing
    client.get_series.return_value = _mock_series()

    debrid = MagicMock(return_value=(True, {'id': 'pack-001'}))

    pack_result = _torrentio_result(
        info_hash='d' * 40, label='2160p', seeds=200, cached=True,
        title='Show.S01.2160p.BluRay-GROUP',  # single-season pack detected
    )

    with patch('utils.arr_client.SonarrClient', return_value=client), \
         patch('utils.quality_compromise.search_torrents',
               return_value=[pack_result]), \
         patch('utils.quality_compromise.is_blocked', return_value=False):
        result = watcher._try_alt_episode(
            'Show Name', 1, [1], debrid,
            'Show.S01E01.torrent', orig_path, label='sonarr',
        )

    assert result is True
    # Tier stays at 0 — pack is a same-tier grab, not a compromise
    ts = RetryMeta.read_tier_state(orig_path)
    assert ts['current_tier_index'] == 0
    assert ts['season_pack_attempted'] is True

    # Pending entry shows season_pack strategy, same-tier
    with open(watcher._pending_file) as f:
        entries = json.load(f)
    assert entries[0]['compromise_strategy'] == 'season_pack'
    assert entries[0]['preferred_tier'] == '2160p'
    assert entries[0]['grabbed_tier'] == '2160p'


def test_compromise_pack_submit_failure_falls_through_to_tier_drop(
        watcher, tmp_dir, compromise_env, monkeypatch):
    """If a pack is found but debrid rejects its magnet, the tier-drop
    path must run in the same pass AND the pack-probe flag must NOT be
    consumed — a transient rejection mustn't poison the next retry."""
    monkeypatch.setenv('SEASON_PACK_FALLBACK_ENABLED', 'true')
    monkeypatch.setenv('SEASON_PACK_FALLBACK_MIN_MISSING', '2')
    orig_path = _write_torrent(tmp_dir, 'Show.S01E01.torrent')
    _seed_series_tier_state(orig_path)

    missing = [{'seasonNumber': 1, 'episodeNumber': i, 'hasFile': False}
               for i in (1, 2, 3)]
    client = _mock_sonarr_client()
    client.get_episodes.return_value = missing
    client.get_series.return_value = _mock_series()

    # First call (pack) rejected; second call (tier-drop 1080p) accepted.
    calls = {'n': 0}
    def debrid_sequenced(path):
        calls['n'] += 1
        if calls['n'] == 1:
            return False, 'debrid 502'
        return True, '{"id": 777}'
    debrid = MagicMock(side_effect=debrid_sequenced)

    pack = _torrentio_result(info_hash='a' * 40, label='2160p', seeds=300,
                            cached=True, title='Show.S01.2160p.BluRay-GROUP')
    drop = _torrentio_result(info_hash='b' * 40, label='1080p', seeds=200,
                             cached=True, title='Show.S01E01.1080p.WEB-GROUP')

    # search_torrents is called twice: once for the pack probe, once for
    # the tier-drop probe.  Return different results per call.
    with patch('utils.arr_client.SonarrClient', return_value=client), \
         patch('utils.quality_compromise.search_torrents',
               side_effect=[[pack], [drop]]), \
         patch('utils.quality_compromise.is_blocked', return_value=False):
        result = watcher._try_alt_episode(
            'Show Name', 1, [1], debrid,
            'Show.S01E01.torrent', orig_path, label='sonarr',
        )
    assert result is True
    assert calls['n'] == 2  # pack rejected + tier-drop accepted
    ts = RetryMeta.read_tier_state(orig_path)
    assert ts['current_tier_index'] == 1  # tier-drop advanced
    # Pack flag NOT consumed — future retry can try the pack again
    assert ts.get('season_pack_attempted') is not True


def test_compromise_malformed_info_hash_rejected(watcher, tmp_dir, compromise_env):
    """Defence-in-depth: a candidate with a non-hex info_hash must not
    produce a malformed magnet URI posted to the debrid provider."""
    orig_path = _write_torrent(tmp_dir, 'Show.S01E01.torrent')
    _seed_series_tier_state(orig_path)

    client = _mock_sonarr_client()
    debrid = MagicMock(return_value=(True, '{"id": 1}'))

    bogus = _torrentio_result(info_hash='not-a-hash', label='1080p',
                              cached=True,
                              title='Show.S01E01.1080p.BluRay-GROUP')
    with patch('utils.arr_client.SonarrClient', return_value=client), \
         patch('utils.quality_compromise.search_torrents', return_value=[bogus]), \
         patch('utils.quality_compromise.is_blocked', return_value=False):
        result = watcher._try_alt_episode(
            'Show Name', 1, [1], debrid,
            'Show.S01E01.torrent', orig_path, label='sonarr',
        )
    assert result is False
    debrid.assert_not_called()


def test_compromise_movie_path(watcher, tmp_dir, compromise_env):
    """Radarr parity: same flow for movies, tier advances on compromise."""
    orig_path = _write_torrent(tmp_dir, 'Movie.2024.torrent')
    _seed_series_tier_state(orig_path)  # movies share RetryMeta schema

    client = _mock_radarr_client()
    debrid = MagicMock(return_value=(True, {'id': 'movie-001'}))

    cached_1080p = _torrentio_result(
        info_hash='e' * 40, label='1080p', cached=True,
        title='Movie.2024.1080p.BluRay-GROUP',
    )

    with patch('utils.arr_client.RadarrClient', return_value=client), \
         patch('utils.quality_compromise.search_torrents',
               return_value=[cached_1080p]), \
         patch('utils.quality_compromise.is_blocked', return_value=False):
        result = watcher._try_alt_movie(
            'Movie Name', debrid, 'Movie.2024.torrent', orig_path, label='radarr',
        )
    assert result is True
    ts = RetryMeta.read_tier_state(orig_path)
    assert ts['current_tier_index'] == 1
