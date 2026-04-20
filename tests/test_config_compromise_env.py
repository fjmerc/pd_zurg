"""Tests for plan 33 Phase 7 — config surface for smart quality compromise.

These tests cover the three new env vars introduced in Phase 7
(``QUALITY_COMPROMISE_MAX_TIER_DROP``, ``SEASON_PACK_FALLBACK_MIN_RATIO``,
``QUALITY_COMPROMISE_NOTIFY``) plus the SIGHUP soft-reload coverage for
all nine compromise toggles.

The matrix deliberately exercises the *configuration gate* that each new
var introduces, not the whole compromise pipeline (that's covered in
``test_compromise_wiring.py``).  We verify:

  1. ``QUALITY_COMPROMISE_MAX_TIER_DROP`` short-circuits ``should_compromise``
     to ``('exhausted', 'max_tier_drop_reached')`` before the dwell gate.
  2. ``SEASON_PACK_FALLBACK_MIN_RATIO`` blocks a pack probe against a
     large season with few holes — ``search_torrents`` is never called.
  3. ``QUALITY_COMPROMISE_NOTIFY=false`` silences Apprise while history
     + pending_monitors annotation still fire (invariant I7).
  4. All nine compromise vars are in ``SOFT_RELOAD`` so a SIGHUP-driven
     reload propagates toggle changes without a service restart.
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
from utils.config_reload import SOFT_RELOAD
from utils.quality_compromise import (
    find_season_pack_candidate,
    should_compromise,
)


NOW = 1_713_664_800.0
DWELL_3D = 3 * 86400


# ---------------------------------------------------------------------------
# Fixtures / helpers (lifted from adjacent compromise tests)
# ---------------------------------------------------------------------------

@pytest.fixture
def watcher(tmp_dir):
    completed = os.path.join(tmp_dir, 'completed')
    os.makedirs(completed, exist_ok=True)
    return BlackholeWatcher(
        watch_dir=tmp_dir, debrid_api_key='k', debrid_service='realdebrid',
        symlink_enabled=True, completed_dir=completed,
        rclone_mount='/data', symlink_target_base='/mnt/debrid',
    )


@pytest.fixture
def compromise_env(monkeypatch):
    monkeypatch.setenv('QUALITY_COMPROMISE_ENABLED', 'true')
    monkeypatch.setenv('QUALITY_COMPROMISE_DWELL_DAYS', '3')
    monkeypatch.setenv('QUALITY_COMPROMISE_MIN_SEEDERS', '1')
    monkeypatch.setenv('QUALITY_COMPROMISE_ONLY_CACHED', 'true')


def _tier_state(tier_order=('2160p', '1080p', '720p'), current=0,
                first_attempted_at=NOW):
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


def _mock_arr(episodes, imdb_id='tt7654321'):
    client = MagicMock()
    client.get_episodes.return_value = episodes
    client.get_series.return_value = {'imdbId': imdb_id, 'id': 42}
    return client


def _episode(season, episode, has_file):
    return {'seasonNumber': season, 'episodeNumber': episode, 'hasFile': has_file}


def _mock_sonarr_client():
    client = MagicMock()
    client.configured = True
    client.url = 'http://sonarr:8989'
    client.find_series_in_library.return_value = {
        'id': 42, 'imdbId': 'tt1111111', 'title': 'Show Name',
    }
    client.get_episode_id.return_value = 1
    client.get_episode_releases.return_value = []
    client.get_profile_id_for_series.return_value = 4
    client.get_tier_order.return_value = ['2160p', '1080p', '720p']
    client.get_episodes.return_value = []
    client.get_series.return_value = {
        'id': 42, 'imdbId': 'tt1111111', 'title': 'Show Name',
    }
    return client


def _torrentio_result(info_hash='c' * 40, label='1080p', seeds=120,
                     cached=True, title='Show.S01E01.1080p.BluRay-GROUP'):
    return {
        'title': title, 'info_hash': info_hash,
        'size_bytes': 4_000_000_000, 'seeds': seeds,
        'source_name': 'Torrentio',
        'quality': {'label': label, 'score': 3},
        'cached': cached, 'cached_service': 'alldebrid',
    }


def _write_torrent(tmp_dir, name):
    path = os.path.join(tmp_dir, name)
    with open(path, 'w') as f:
        f.write('magnet:?xt=urn:btih:' + ('0' * 40))
    return path


def _seed_series_tier_state(file_path, tier_order=('2160p', '1080p', '720p'),
                            first_attempted_at=None, current_tier_index=0):
    """Seed a RetryMeta tier_state.  ``current_tier_index > 0`` simulates
    a file that has already been compromised at least once — the caller
    passes this in to test the MAX_TIER_DROP cap."""
    if first_attempted_at is None:
        first_attempted_at = time.time() - (DWELL_3D + 60)
    RetryMeta.init_tier_state(
        file_path, arr_service='sonarr', arr_url='http://sonarr:8989',
        profile_id=4, tier_order=list(tier_order),
        now=first_attempted_at,
    )
    # init_tier_state always seeds current_tier_index=0; bump explicitly
    # for the cap test to simulate one drop already taken.
    if current_tier_index > 0:
        RetryMeta.advance_tier(file_path, current_tier_index, 'test_seed')


# ---------------------------------------------------------------------------
# 1) QUALITY_COMPROMISE_MAX_TIER_DROP caps escalation
# ---------------------------------------------------------------------------

def test_config_max_tier_drop_caps_escalation():
    """``current_tier_index=1`` with ``max_tier_drop=1`` must return
    ``('exhausted', 'max_tier_drop_reached')`` even when dwell has
    elapsed and lower tiers remain in the profile."""
    # 3-tier profile, already compromised once (index 1 = 1080p).  Dwell
    # elapsed, 720p still in the profile — but the user only allowed one
    # drop, so the decision must be 'exhausted', not 'advance'.
    state = _tier_state(tier_order=('2160p', '1080p', '720p'), current=1,
                        first_attempted_at=NOW - (DWELL_3D + 60))
    action, reason = should_compromise(
        state, NOW, DWELL_3D, only_cached=True, max_tier_drop=1,
    )
    assert (action, reason) == ('exhausted', 'max_tier_drop_reached')


def test_config_max_tier_drop_cap_checked_before_dwell_gate():
    """The cap fires BEFORE the dwell check so an exhausted allowance
    fails fast without requiring another 3-day wait.  At ``current=2``
    with ``max=1``, even an un-elapsed dwell produces 'exhausted'."""
    state = _tier_state(tier_order=('2160p', '1080p', '720p', '480p'),
                        current=2, first_attempted_at=NOW - 60)  # just started
    action, reason = should_compromise(
        state, NOW, DWELL_3D, only_cached=True, max_tier_drop=1,
    )
    assert (action, reason) == ('exhausted', 'max_tier_drop_reached')


def test_config_max_tier_drop_zero_disables_cap():
    """``max_tier_drop=0`` (or None) disables the cap — profile ceiling
    remains the only ceiling, so a mid-profile advance still fires."""
    state = _tier_state(tier_order=('2160p', '1080p', '720p'), current=1,
                        first_attempted_at=NOW - (DWELL_3D + 60))
    for cap in (0, None, -1):
        action, reason = should_compromise(
            state, NOW, DWELL_3D, only_cached=True, max_tier_drop=cap,
        )
        assert (action, reason) == ('advance', 'dwell_elapsed'), cap


def test_config_max_tier_drop_permits_first_drop_but_blocks_second():
    """Cap=1 permits 0 -> 1 but blocks 1 -> 2.  Verifies the comparison
    is ``current >= cap``, not ``current > cap``."""
    # current=0, cap=1 → advance permitted (0 drops taken, cap allows 1)
    state0 = _tier_state(current=0, first_attempted_at=NOW - (DWELL_3D + 60))
    action0, _ = should_compromise(
        state0, NOW, DWELL_3D, only_cached=True, max_tier_drop=1,
    )
    assert action0 == 'advance'

    # current=1, cap=1 → exhausted (1 drop already taken, cap is the wall)
    state1 = _tier_state(current=1, first_attempted_at=NOW - (DWELL_3D + 60))
    action1, reason1 = should_compromise(
        state1, NOW, DWELL_3D, only_cached=True, max_tier_drop=1,
    )
    assert (action1, reason1) == ('exhausted', 'max_tier_drop_reached')


# ---------------------------------------------------------------------------
# 2) SEASON_PACK_FALLBACK_MIN_RATIO gates small seasons
# ---------------------------------------------------------------------------

@patch('utils.quality_compromise.is_blocked', return_value=False)
@patch('utils.quality_compromise.search_torrents')
def test_config_season_pack_min_ratio_gates_small_seasons(mock_search, _blocked):
    """A 40-episode season with 4 missing (10%) must NOT trigger a pack
    probe at ``min_ratio=0.4`` — the ratio gate rejects before any
    Torrentio I/O happens, preserving the preflight contract."""
    # 40 episodes, 4 missing = 10% missing.  min_missing=4 is satisfied
    # (4 >= 4), but ratio 0.1 < 0.4 must reject.
    episodes = (
        [_episode(1, i, False) for i in range(1, 5)]       # 4 missing
        + [_episode(1, i, True) for i in range(5, 41)]      # 36 present
    )
    arr = _mock_arr(episodes)
    winner = find_season_pack_candidate(
        arr_client=arr, series_id=42, season_number=1, tier_label='1080p',
        min_missing=4, min_seeders=1, only_cached=True, min_ratio=0.4,
    )
    assert winner is None
    # The key assertion: no Torrentio probe was made — the ratio gate
    # blocks before any network I/O.
    mock_search.assert_not_called()


@patch('utils.quality_compromise.is_blocked', return_value=False)
@patch('utils.quality_compromise.search_torrents')
def test_config_season_pack_min_ratio_permits_large_holes(mock_search, _blocked):
    """Same absolute missing count (4) but against a 6-episode season
    (67% missing) passes the ratio gate and probes Torrentio."""
    episodes = (
        [_episode(1, i, False) for i in range(1, 5)]       # 4 missing
        + [_episode(1, i, True) for i in range(5, 7)]       # 2 present
    )
    arr = _mock_arr(episodes)
    mock_search.return_value = [
        _torrentio_result(title='Show.S01.1080p.BluRay-GROUP', label='1080p'),
    ]
    winner = find_season_pack_candidate(
        arr_client=arr, series_id=42, season_number=1, tier_label='1080p',
        min_missing=4, min_seeders=1, only_cached=True, min_ratio=0.4,
    )
    assert winner is not None
    mock_search.assert_called_once()


@patch('utils.quality_compromise.is_blocked', return_value=False)
@patch('utils.quality_compromise.search_torrents')
def test_config_season_pack_min_ratio_zero_disables_gate(mock_search, _blocked):
    """``min_ratio=0`` (or default) keeps Phase-5 behaviour — only
    ``min_missing`` gates the probe, no ratio check."""
    episodes = (
        [_episode(1, i, False) for i in range(1, 5)]
        + [_episode(1, i, True) for i in range(5, 41)]
    )
    arr = _mock_arr(episodes)
    mock_search.return_value = [
        _torrentio_result(title='Show.S01.1080p.BluRay-GROUP', label='1080p'),
    ]
    winner = find_season_pack_candidate(
        arr_client=arr, series_id=42, season_number=1, tier_label='1080p',
        min_missing=4, min_seeders=1, only_cached=True, min_ratio=0.0,
    )
    assert winner is not None
    mock_search.assert_called_once()


# ---------------------------------------------------------------------------
# 3) QUALITY_COMPROMISE_NOTIFY=false silences Apprise but keeps dashboard
# ---------------------------------------------------------------------------

def test_config_compromise_notify_off_skips_apprise_keeps_history(
        watcher, tmp_dir, compromise_env, monkeypatch):
    """``QUALITY_COMPROMISE_NOTIFY=false`` must silence the Apprise call
    but still write history + pending_monitors — invariant I7."""
    monkeypatch.setenv('QUALITY_COMPROMISE_NOTIFY', 'false')
    orig_path = _write_torrent(tmp_dir, 'Show.S01E01.torrent')
    _seed_series_tier_state(orig_path)

    client = _mock_sonarr_client()
    debrid = MagicMock(return_value=(True, '{"id": 9999}'))
    cached_1080p = _torrentio_result(info_hash='c' * 40, label='1080p',
                                     seeds=120, cached=True)

    fake_history = MagicMock()
    fake_notify = MagicMock()

    with patch('utils.arr_client.SonarrClient', return_value=client), \
         patch('utils.quality_compromise.search_torrents',
               return_value=[cached_1080p]), \
         patch('utils.quality_compromise.is_blocked', return_value=False), \
         patch('utils.blackhole._notify', fake_notify), \
         patch('utils.blackhole._history', fake_history):
        result = watcher._try_alt_episode(
            'Show Name', 1, [1], debrid,
            'Show.S01E01.torrent', orig_path, label='sonarr',
        )

    assert result is True
    # Apprise silenced — the whole opt-out is the point of the new toggle.
    fake_notify.assert_not_called()
    # History event still fired (I7 — dashboard trail is non-negotiable).
    assert fake_history.log_event.called
    ev_args = fake_history.log_event.call_args
    assert ev_args.args[0] == 'compromise_grabbed'
    # pending_monitors.json still annotated — confirms the dashboard
    # endpoint can still report this event.
    with open(watcher._pending_file) as f:
        entries = json.load(f)
    assert len(entries) == 1
    assert entries[0]['compromised'] is True
    assert entries[0]['grabbed_tier'] == '1080p'


def test_config_compromise_notify_on_fires_apprise(
        watcher, tmp_dir, compromise_env, monkeypatch):
    """Control case: with NOTIFY=true (default) Apprise IS called.  Keeps
    the OFF-case test above honest — otherwise a broken gate could pass
    silently either way."""
    monkeypatch.setenv('QUALITY_COMPROMISE_NOTIFY', 'true')
    orig_path = _write_torrent(tmp_dir, 'Show.S01E01.torrent')
    _seed_series_tier_state(orig_path)

    client = _mock_sonarr_client()
    debrid = MagicMock(return_value=(True, '{"id": 9999}'))
    cached_1080p = _torrentio_result(cached=True)

    fake_notify = MagicMock()
    with patch('utils.arr_client.SonarrClient', return_value=client), \
         patch('utils.quality_compromise.search_torrents',
               return_value=[cached_1080p]), \
         patch('utils.quality_compromise.is_blocked', return_value=False), \
         patch('utils.blackhole._notify', fake_notify):
        watcher._try_alt_episode(
            'Show Name', 1, [1], debrid,
            'Show.S01E01.torrent', orig_path, label='sonarr',
        )
    assert fake_notify.called
    assert fake_notify.call_args.args[0] == 'compromise_grabbed'


# ---------------------------------------------------------------------------
# 4) SIGHUP soft-reload picks up compromise toggles
# ---------------------------------------------------------------------------

COMPROMISE_VARS = (
    'QUALITY_COMPROMISE_ENABLED',
    'QUALITY_COMPROMISE_DWELL_DAYS',
    'QUALITY_COMPROMISE_MIN_SEEDERS',
    'QUALITY_COMPROMISE_ONLY_CACHED',
    'QUALITY_COMPROMISE_MAX_TIER_DROP',
    'QUALITY_COMPROMISE_NOTIFY',
    'SEASON_PACK_FALLBACK_ENABLED',
    'SEASON_PACK_FALLBACK_MIN_MISSING',
    'SEASON_PACK_FALLBACK_MIN_RATIO',
)


def test_config_reload_all_compromise_vars_soft_reload():
    """All nine compromise env vars must be in SOFT_RELOAD so SIGHUP
    picks them up without any service restart — compromise code reads
    os.environ fresh on every retry, so no module-global rebind is
    needed.  Ensures the soft-reload path is actually exercised."""
    for var in COMPROMISE_VARS:
        assert var in SOFT_RELOAD, (
            f"{var} is missing from SOFT_RELOAD — SIGHUP would trigger "
            f"an unnecessary service restart (or worse, no reload at all)."
        )


def test_config_reload_picks_up_toggle_changes(tmp_dir, monkeypatch):
    """A SIGHUP-like soft reload from a .env file with new compromise
    values must flip os.environ AND be flagged as a soft-only change
    (no service restarts)."""
    import utils.config_reload as cr

    env_file = os.path.join(tmp_dir, '.env')
    monkeypatch.setattr(cr, 'ENV_FILE', env_file)

    with open(env_file, 'w') as f:
        f.write('QUALITY_COMPROMISE_ENABLED=false\n')
        f.write('QUALITY_COMPROMISE_MAX_TIER_DROP=1\n')
        f.write('QUALITY_COMPROMISE_NOTIFY=true\n')
    monkeypatch.setenv('QUALITY_COMPROMISE_ENABLED', 'false')
    monkeypatch.setenv('QUALITY_COMPROMISE_MAX_TIER_DROP', '1')
    monkeypatch.setenv('QUALITY_COMPROMISE_NOTIFY', 'true')
    monkeypatch.setattr(cr, '_last_env_keys',
                        set(cr.dotenv_values(env_file).keys()))

    # User toggles feature on + relaxes cap + silences Apprise.
    with open(env_file, 'w') as f:
        f.write('QUALITY_COMPROMISE_ENABLED=true\n')
        f.write('QUALITY_COMPROMISE_MAX_TIER_DROP=3\n')
        f.write('QUALITY_COMPROMISE_NOTIFY=false\n')

    changed = cr._reload_env()
    assert changed == {
        'QUALITY_COMPROMISE_ENABLED',
        'QUALITY_COMPROMISE_MAX_TIER_DROP',
        'QUALITY_COMPROMISE_NOTIFY',
    }
    # Process env is updated immediately — the whole point of soft-reload.
    assert os.environ['QUALITY_COMPROMISE_ENABLED'] == 'true'
    assert os.environ['QUALITY_COMPROMISE_MAX_TIER_DROP'] == '3'
    assert os.environ['QUALITY_COMPROMISE_NOTIFY'] == 'false'
    # All three changes fall under SOFT_RELOAD → no service restart needed.
    assert changed <= SOFT_RELOAD


def test_config_validator_rejects_out_of_range_ratio():
    """``SEASON_PACK_FALLBACK_MIN_RATIO`` is declared as 'string' in the
    schema (because number:MIN-MAX coerces to int) and validated by a
    separate float-range check.  Exercise that path end-to-end so a
    future refactor that moves the field back into ``numeric_ranges``
    (and silently rounds 0.4 → 0) gets caught."""
    from utils.settings_api import validate_env_values

    # Baseline must-pass values (other required fields get their defaults)
    base = {'ZURG_ENABLED': 'false'}

    # Valid boundary: 0.0 disables, 1.0 requires 100% missing
    for good in ('0.0', '0.4', '1.0'):
        result = validate_env_values({**base, 'SEASON_PACK_FALLBACK_MIN_RATIO': good})
        assert not any('MIN_RATIO' in e for e in result['errors']), (good, result['errors'])

    # Out-of-range
    for bad in ('1.5', '-0.1', '2.0'):
        result = validate_env_values({**base, 'SEASON_PACK_FALLBACK_MIN_RATIO': bad})
        assert any('MIN_RATIO' in e and 'outside' in e for e in result['errors']), bad

    # Non-numeric
    result = validate_env_values({**base, 'SEASON_PACK_FALLBACK_MIN_RATIO': 'half'})
    assert any('MIN_RATIO' in e and 'not a valid number' in e for e in result['errors'])


def test_config_validator_rejects_nan_and_inf_ratio():
    """Regression: NaN compares False to every bound, so ``r < 0.0 or
    r > 1.0`` would let it slip through the validator — and the runtime
    ratio gate (``missing/total < min_ratio``) also compares False to
    NaN — silently disabling the gate with no UI error.  inf has the
    mirror problem (clamps to 1.0 at runtime via ``_float_env`` but
    still passes validation without a warning)."""
    from utils.settings_api import validate_env_values

    base = {'ZURG_ENABLED': 'false'}
    for poison in ('nan', 'NaN', 'inf', '-inf', 'Infinity'):
        result = validate_env_values(
            {**base, 'SEASON_PACK_FALLBACK_MIN_RATIO': poison},
        )
        assert any('MIN_RATIO' in e and 'finite' in e for e in result['errors']), (
            f"validator let {poison!r} through: errors={result['errors']}"
        )


def test_config_float_env_rejects_nan_and_inf(monkeypatch):
    """Regression: ``_float_env`` clamping uses ``<`` / ``>`` which
    return False for NaN, so a misconfigured env with ``MIN_RATIO=nan``
    must fall back to *default* instead of silently disabling the gate."""
    from utils.blackhole import BlackholeWatcher

    for poison in ('nan', 'NaN', 'inf', '-inf'):
        monkeypatch.setenv('SEASON_PACK_FALLBACK_MIN_RATIO', poison)
        val = BlackholeWatcher._float_env(
            'SEASON_PACK_FALLBACK_MIN_RATIO', 0.4,
            minimum=0.0, maximum=1.0,
        )
        assert val == 0.4, (
            f"{poison!r} leaked through _float_env as {val!r} — NaN/inf "
            f"would silently bypass the ratio gate at runtime"
        )


def test_config_max_tier_drop_validator_rejects_zero():
    """Regression: ``MAX_TIER_DROP=0`` was documented as "disables cap"
    but a user reading "zero drops" would expect the opposite (no
    compromise).  Validator now bans 0 — users who want unlimited set
    a large value; the profile ceiling stays authoritative."""
    from utils.settings_api import validate_env_values

    base = {'ZURG_ENABLED': 'false'}
    result = validate_env_values({**base, 'QUALITY_COMPROMISE_MAX_TIER_DROP': '0'})
    assert any('MAX_TIER_DROP' in w and 'outside' in w for w in result['warnings']), (
        result['warnings']
    )


def test_config_max_tier_drop_runtime_clamps_zero_to_one(monkeypatch):
    """Regression: defence-in-depth — even if 0 reaches the runtime
    (e.g. a pre-Phase-7 .env with 0 that wasn't re-validated), it
    clamps to 1 instead of silently unlocking the full profile."""
    from utils.blackhole import BlackholeWatcher

    monkeypatch.setenv('QUALITY_COMPROMISE_MAX_TIER_DROP', '0')
    val = BlackholeWatcher._int_env(
        'QUALITY_COMPROMISE_MAX_TIER_DROP', 2, minimum=1,
    )
    assert val == 1, (
        f"0 clamped to {val} — must clamp to minimum=1 so MAX_TIER_DROP=0 "
        f"reads as 'one drop only' not as 'unlimited'"
    )


def test_config_reload_partial_compromise_change_is_soft_only():
    """Changing just one compromise var (e.g. flipping NOTIFY off) must
    be soft-only.  Regression guard against anyone moving the vars into
    a SERVICE_DEPENDENCIES set by mistake."""
    from utils.config_reload import _determine_restarts, SERVICE_DEPENDENCIES
    for var in COMPROMISE_VARS:
        services = _determine_restarts({var})
        assert services == set(), (
            f"{var} triggered service restarts {services} — compromise "
            f"vars must never cascade into SERVICE_DEPENDENCIES."
        )
        # And verify not present in any dependency set.
        for svc, deps in SERVICE_DEPENDENCIES.items():
            assert var not in deps, (
                f"{var} accidentally landed in SERVICE_DEPENDENCIES[{svc!r}]"
            )
