"""Tests for utils/debrid_health.py — phase 2 reconciler.

Covers the periodic sweep that probes Real-Debrid for May 2026 keyword-
filter blocks, persists per-torrent state to JSON, and surfaces blocked
hashes for library enrichment. Detection only — auto-remediation lives
in a later phase and has its own tests.
"""

import json
import os
import time

import pytest
from unittest.mock import MagicMock, patch

from utils import debrid_health


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def state_path(tmp_path, monkeypatch):
    """Redirect the module's _STATE_PATH to a tmp file and reset the
    lazy-loaded singleton between cases so state doesn't leak across tests."""
    p = tmp_path / 'debrid_health.json'
    monkeypatch.setattr(debrid_health, '_STATE_PATH', str(p))
    debrid_health._reset_for_testing()
    yield p
    debrid_health._reset_for_testing()


@pytest.fixture
def no_sleep(monkeypatch):
    """Replace time.sleep with a counter so rate-limit logic doesn't
    block the test suite. Yields the mock so tests can assert on it."""
    mock = MagicMock()
    monkeypatch.setattr(debrid_health.time, 'sleep', mock)
    return mock


@pytest.fixture
def rd_enabled(monkeypatch):
    monkeypatch.setenv('DEBRID_HEALTH_ENABLED', 'true')


def _mock_client(torrents=None, probe_results=None, list_raises=None):
    """Build a fake RealDebridClient.

    Args:
        torrents: list of dicts returned by list_torrents().
        probe_results: dict keyed by torrent id → probe_file() return value.
        list_raises: when set, list_torrents raises this exception.
    """
    client = MagicMock()
    client.configured = True
    if list_raises:
        client.list_torrents.side_effect = list_raises
    else:
        client.list_torrents.return_value = torrents or []
    if probe_results is not None:
        client.probe_file.side_effect = lambda tid: probe_results.get(
            tid, {'status': 'unknown', 'error': 'no_probe_setup'}
        )
    return client


def _patch_client(client):
    """Patch get_debrid_client → (client, 'realdebrid')."""
    return patch('utils.debrid_health.get_debrid_client', return_value=(client, 'realdebrid'))


def _torrent(tid, infohash, filename='Release.S01E01.mkv'):
    return {'id': tid, 'hash': infohash, 'filename': filename,
            'status': 'downloaded', 'bytes': 1_000_000}


# ---------------------------------------------------------------------------
# Sweep classification
# ---------------------------------------------------------------------------

class TestSweepClassification:

    def test_three_torrents_two_healthy_one_blocked(self, state_path, no_sleep, rd_enabled):
        client = _mock_client(
            torrents=[
                _torrent('T1', 'AAAA0000'),
                _torrent('T2', 'BBBB1111'),
                _torrent('T3', 'CCCC2222'),
            ],
            probe_results={
                'T1': {'status': 'healthy'},
                'T2': {'status': 'blocked', 'reason': 'infringing_file', 'http': 403},
                'T3': {'status': 'healthy'},
            },
        )
        with _patch_client(client):
            result = debrid_health.run_sweep()

        assert result['status'] == 'success'
        assert result['items'] == 3
        assert 'healthy 2' in result['message']
        assert 'blocked 1' in result['message']

        # State file persisted with the correct classifications, keyed by
        # uppercased hash so library enrichment can match against
        # debrid-side hashes consistently.
        with open(state_path) as f:
            saved = json.load(f)
        assert saved['version'] == debrid_health._STATE_VERSION
        assert saved['probed']['AAAA0000']['status'] == 'healthy'
        assert saved['probed']['BBBB1111']['status'] == 'blocked'
        assert saved['probed']['BBBB1111']['reason'] == 'infringing_file'
        assert saved['probed']['BBBB1111']['http'] == 403
        assert saved['probed']['CCCC2222']['status'] == 'healthy'

    def test_unknown_probe_result_classified_as_unknown(self, state_path, no_sleep, rd_enabled):
        """A probe_file return with unrecognised status (e.g. future drift)
        must fall back to 'unknown', not crash the sweep."""
        client = _mock_client(
            torrents=[_torrent('T1', 'DDDD3333')],
            probe_results={'T1': {'status': 'something_new', 'error': 'drift'}},
        )
        with _patch_client(client):
            result = debrid_health.run_sweep()

        assert result['items'] == 1
        with open(state_path) as f:
            saved = json.load(f)
        assert saved['probed']['DDDD3333']['status'] == 'unknown'

    def test_torrent_lowercase_hash_is_normalised_to_uppercase(self, state_path, no_sleep, rd_enabled):
        """list_torrents returns uppercase hashes today, but defensively
        the sweep upper()s them so a future RD payload change can't split
        state keys across cases."""
        client = _mock_client(
            torrents=[{'id': 'T1', 'hash': 'eeee4444', 'filename': 'r.mkv',
                       'status': 'downloaded', 'bytes': 1000}],
            probe_results={'T1': {'status': 'healthy'}},
        )
        with _patch_client(client):
            debrid_health.run_sweep()

        with open(state_path) as f:
            saved = json.load(f)
        assert 'EEEE4444' in saved['probed']
        assert 'eeee4444' not in saved['probed']


# ---------------------------------------------------------------------------
# TTL skip & re-probe semantics
# ---------------------------------------------------------------------------

class TestTTLSkip:

    def test_healthy_within_ttl_is_skipped_on_re_sweep(self, state_path, no_sleep, rd_enabled):
        """A torrent probed healthy < _PROBE_TTL ago must NOT be probed
        again — that's the whole point of the cache, otherwise a 5000-
        torrent library re-probes the entire set every 12h forever."""
        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000'), _torrent('T2', 'BBBB1111')],
            probe_results={
                'T1': {'status': 'healthy'},
                'T2': {'status': 'blocked', 'reason': 'infringing_file', 'http': 403},
            },
        )
        with _patch_client(client):
            debrid_health.run_sweep()
        assert client.probe_file.call_count == 2

        # Second sweep with the SAME client: T1 (healthy) should be skipped,
        # T2 (blocked) should be re-probed so we can detect filter reversal.
        client.probe_file.reset_mock()
        with _patch_client(client):
            result = debrid_health.run_sweep()
        assert client.probe_file.call_count == 1
        assert client.probe_file.call_args[0] == ('T2',)
        assert result['items'] == 1
        assert 'skipped 1' in result['message']

    def test_healthy_past_ttl_is_re_probed(self, state_path, no_sleep, rd_enabled, monkeypatch):
        """Aged-out healthy entry is eligible again — implementation skips
        only when (now - ts) < _PROBE_TTL."""
        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000')],
            probe_results={'T1': {'status': 'healthy'}},
        )
        with _patch_client(client):
            debrid_health.run_sweep()

        # Backdate the persisted ts to 8 days ago and force a state reload.
        with open(state_path) as f:
            saved = json.load(f)
        saved['probed']['AAAA0000']['ts'] = time.time() - (8 * 24 * 3600)
        with open(state_path, 'w') as f:
            json.dump(saved, f)
        debrid_health._reset_for_testing()

        client.probe_file.reset_mock()
        with _patch_client(client):
            debrid_health.run_sweep()
        assert client.probe_file.call_count == 1

    def test_blocked_to_healthy_transition_updates_state(self, state_path, no_sleep, rd_enabled):
        """When RD reverses the filter, the next sweep must re-classify
        the entry as healthy — get_blocked_hashes must drop the stale flag."""
        # Sweep 1: blocked
        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000')],
            probe_results={'T1': {'status': 'blocked', 'reason': 'infringing_file', 'http': 403}},
        )
        with _patch_client(client):
            debrid_health.run_sweep()
        assert debrid_health.get_blocked_hashes() == {'AAAA0000'}

        # Sweep 2: same torrent, now healthy
        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000')],
            probe_results={'T1': {'status': 'healthy'}},
        )
        with _patch_client(client):
            debrid_health.run_sweep()
        assert debrid_health.get_blocked_hashes() == set()

    def test_unknown_status_is_re_probed_always(self, state_path, no_sleep, rd_enabled):
        """Unknown means we don't have a verdict — re-probe immediately
        on the next sweep rather than waiting out the healthy-TTL."""
        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000')],
            probe_results={'T1': {'status': 'unknown', 'error': 'http_503'}},
        )
        with _patch_client(client):
            debrid_health.run_sweep()

        # Re-sweep should re-probe T1, not skip it
        client.probe_file.reset_mock()
        with _patch_client(client):
            debrid_health.run_sweep()
        assert client.probe_file.call_count == 1


# ---------------------------------------------------------------------------
# Caps & rate limiting
# ---------------------------------------------------------------------------

class TestCapsAndRateLimit:

    def test_max_per_sweep_cap_honoured(self, state_path, no_sleep, rd_enabled, monkeypatch):
        """At most _MAX_PER_SWEEP torrents are probed per sweep — the
        remainder are eligible on the next interval. Defends against
        marathon runs on first-time enable for huge libraries."""
        monkeypatch.setattr(debrid_health, '_MAX_PER_SWEEP', 3)
        torrents = [_torrent(f'T{i}', f'HASH{i:04d}') for i in range(10)]
        client = _mock_client(
            torrents=torrents,
            probe_results={t['id']: {'status': 'healthy'} for t in torrents},
        )
        with _patch_client(client):
            result = debrid_health.run_sweep()

        assert client.probe_file.call_count == 3
        assert result['items'] == 3

    def test_rate_limit_sleep_between_probes(self, state_path, no_sleep, rd_enabled, monkeypatch):
        """Rate-limit enforcement: a 60/min cap means time.sleep(1.0)
        between probes. Three probes → two sleeps (no sleep after the
        last)."""
        monkeypatch.setattr(debrid_health, '_RATE_LIMIT_PER_MIN', 60)
        client = _mock_client(
            torrents=[_torrent(f'T{i}', f'HASH{i:04d}') for i in range(3)],
            probe_results={f'T{i}': {'status': 'healthy'} for i in range(3)},
        )
        with _patch_client(client):
            debrid_health.run_sweep()

        # Two sleeps total — between probe 0/1 and 1/2, none after probe 2.
        assert no_sleep.call_count == 2
        for call in no_sleep.call_args_list:
            assert call.args[0] == pytest.approx(1.0)

    def test_no_sleep_when_only_one_probe(self, state_path, no_sleep, rd_enabled):
        """Single-probe sweep must not pay the rate-limit delay — wasted
        wall-clock time on the scheduler UI's 'running' indicator."""
        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000')],
            probe_results={'T1': {'status': 'healthy'}},
        )
        with _patch_client(client):
            debrid_health.run_sweep()
        assert no_sleep.call_count == 0


# ---------------------------------------------------------------------------
# State persistence: corruption, schema, future-dated entries
# ---------------------------------------------------------------------------

class TestStatePersistence:

    def test_missing_state_file_starts_fresh(self, state_path):
        # state_path fixture asserts the file doesn't exist initially.
        assert not state_path.exists()
        assert debrid_health.get_blocked_hashes() == set()

    def test_corrupt_state_file_is_swallowed(self, state_path):
        state_path.write_text('{this is not valid json')
        # Must not raise on load — gracefully recovers with empty state.
        assert debrid_health.get_blocked_hashes() == set()

    def test_schema_version_mismatch_starts_fresh(self, state_path):
        state_path.write_text(json.dumps({'version': 999, 'probed': {
            'ABCD': {'status': 'blocked', 'ts': time.time()}
        }}))
        # Old/new schema versions must NOT load — would surface stale
        # blocks from a different state shape into the UI.
        assert debrid_health.get_blocked_hashes() == set()

    def test_oversized_state_file_rejected(self, state_path, monkeypatch):
        monkeypatch.setattr(debrid_health, '_STATE_MAX_BYTES', 100)
        big = {'version': 1, 'probed': {f'H{i:04d}': {
            'status': 'blocked', 'ts': time.time()} for i in range(200)}}
        state_path.write_text(json.dumps(big))
        # File is way over 100 bytes — strict size cap rejects without
        # parsing (memory safety on a corrupted/inflated file).
        assert debrid_health.get_blocked_hashes() == set()

    def test_future_dated_entries_dropped_on_load(self, state_path):
        """ts > now + 24h is clock skew or tampering — drop those entries
        but keep the rest of the state."""
        state_path.write_text(json.dumps({
            'version': 1,
            'probed': {
                'GOOD': {'status': 'blocked', 'ts': time.time()},
                'FUTURE': {'status': 'blocked', 'ts': time.time() + (48 * 3600)},
            },
        }))
        assert debrid_health.get_blocked_hashes() == {'GOOD'}

    def test_invalid_status_entries_dropped(self, state_path):
        """Entries with unknown status strings or non-dict values are
        dropped on load. (JSON dict keys are always strings post-decode,
        so non-string-key cases aren't reachable through the disk path —
        the isinstance(k, str) guard in _load_state is defense for any
        future in-memory mutation path that bypasses serialization.)"""
        state_path.write_text(json.dumps({
            'version': 1,
            'probed': {
                'OK': {'status': 'blocked', 'ts': time.time()},
                'BAD_STATUS': {'status': 'pending', 'ts': time.time()},
                'BAD_SHAPE': 'not even a dict',
                'BAD_TS': {'status': 'blocked', 'ts': 'not a number'},
                'NO_TS': {'status': 'blocked'},
            },
        }))
        assert debrid_health.get_blocked_hashes() == {'OK'}

    def test_round_trip_preserves_blocked_set(self, state_path, no_sleep, rd_enabled):
        """End-to-end: sweep writes state, fresh load reads it back."""
        client = _mock_client(
            torrents=[
                _torrent('T1', 'AAAA0000'),
                _torrent('T2', 'BBBB1111'),
            ],
            probe_results={
                'T1': {'status': 'healthy'},
                'T2': {'status': 'blocked', 'reason': 'infringing_file', 'http': 403},
            },
        )
        with _patch_client(client):
            debrid_health.run_sweep()

        # Simulate process restart by resetting the lazy singleton.
        debrid_health._reset_for_testing()
        assert debrid_health.get_blocked_hashes() == {'BBBB1111'}


# ---------------------------------------------------------------------------
# Disable, no-client, and error paths
# ---------------------------------------------------------------------------

class TestDisabledAndErrors:

    def test_disabled_env_var_makes_sweep_noop(self, state_path, no_sleep, monkeypatch):
        monkeypatch.setenv('DEBRID_HEALTH_ENABLED', 'false')
        with patch('utils.debrid_health.get_debrid_client') as mock_get:
            result = debrid_health.run_sweep()

        assert result == {'status': 'success', 'message': 'disabled', 'items': 0}
        # No client lookup, no probes — must not touch the network.
        assert mock_get.call_count == 0
        assert not state_path.exists()

    def test_no_rd_client_makes_sweep_noop(self, state_path, no_sleep, rd_enabled):
        with patch('utils.debrid_health.get_debrid_client', return_value=(None, None)):
            result = debrid_health.run_sweep()
        assert result == {'status': 'success', 'message': 'no RD client', 'items': 0}
        assert not state_path.exists()

    def test_unconfigured_client_makes_sweep_noop(self, state_path, no_sleep, rd_enabled):
        """get_debrid_client returns a client object but .configured=False
        (race: env var unset between dispatch and lookup) → no probes."""
        client = MagicMock()
        client.configured = False
        with _patch_client(client):
            result = debrid_health.run_sweep()
        assert result['message'] == 'no RD client'
        assert client.list_torrents.call_count == 0

    def test_list_torrents_failure_returns_error(self, state_path, no_sleep, rd_enabled):
        """Network error on list_torrents must NOT corrupt the persisted
        state (no partial overwrite from an interrupted sweep)."""
        client = _mock_client(list_raises=RuntimeError('boom'))
        with _patch_client(client):
            result = debrid_health.run_sweep()
        assert result['status'] == 'error'
        assert 'boom' in result['message']
        assert not state_path.exists()

    def test_empty_torrent_list_writes_empty_state(self, state_path, no_sleep, rd_enabled):
        """Empty mylist (legitimate, e.g. fresh account) → empty sweep,
        no probes, but the state file IS created so the next interval
        sees a valid file rather than re-running the 'fresh' path."""
        client = _mock_client(torrents=[])
        with _patch_client(client):
            result = debrid_health.run_sweep()
        assert result['items'] == 0
        assert state_path.exists()
        with open(state_path) as f:
            saved = json.load(f)
        assert saved == {'version': debrid_health._STATE_VERSION, 'probed': {}}

    def test_torrents_with_missing_id_or_hash_skipped(self, state_path, no_sleep, rd_enabled):
        client = _mock_client(
            torrents=[
                _torrent('T1', 'AAAA0000'),
                {'id': '', 'hash': 'B', 'filename': 'no_id.mkv'},
                {'id': 'T3', 'hash': '', 'filename': 'no_hash.mkv'},
                'not_a_dict',
            ],
            probe_results={'T1': {'status': 'healthy'}},
        )
        with _patch_client(client):
            result = debrid_health.run_sweep()
        assert result['items'] == 1
        assert client.probe_file.call_count == 1


# ---------------------------------------------------------------------------
# Auto-remediation (Phase 4)
# ---------------------------------------------------------------------------

@pytest.fixture
def auto_remediate_on(monkeypatch):
    monkeypatch.setenv('DEBRID_HEALTH_AUTO_REMEDIATE', 'true')


@pytest.fixture
def mock_remediation_deps(monkeypatch):
    """Patch blocklist.add, scheduled_tasks._attempt_arr_research,
    history.log_event, and notifications.notify so tests can assert on
    the remediation pipeline without touching real persistence or arrs."""
    from unittest.mock import MagicMock
    deps = {
        'blocklist_add': MagicMock(return_value='blocklist-entry-id'),
        'arr_research': MagicMock(return_value=True),
        'log_event': MagicMock(return_value='event-id'),
        'notify': MagicMock(),
    }
    monkeypatch.setattr('utils.blocklist.add', deps['blocklist_add'])
    monkeypatch.setattr(
        'utils.scheduled_tasks._attempt_arr_research', deps['arr_research'],
    )
    monkeypatch.setattr('utils.history.log_event', deps['log_event'])
    monkeypatch.setattr('utils.notifications.notify', deps['notify'])
    return deps


class TestRemediation:
    """Phase 4 — auto-remediate confirmed-blocked torrents.

    When AUTO_REMEDIATE is on AND a probe returns ``blocked``, the
    reconciler runs the three-step pipeline:
      1. blocklist.add (hash, to break the grab → filter → re-grab loop)
      2. client.delete_torrent (cleans up RD)
      3. _attempt_arr_research (Sonarr/Radarr search for replacement)
    Followed by a history event with cause=debrid_filtered.
    A single per-sweep summary notification fires under event 'debrid_filtered'.
    """

    _BLOCKED = {'status': 'blocked', 'reason': 'infringing_file', 'http': 451}

    def test_off_by_default_no_destructive_calls(
            self, state_path, no_sleep, rd_enabled, mock_remediation_deps):
        """The single most important test: with AUTO_REMEDIATE unset,
        confirmed blocks must NOT call blocklist.add / delete_torrent /
        arr re-search / history. Detection-only is the safe default."""
        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000')],
            probe_results={'T1': self._BLOCKED},
        )
        with _patch_client(client):
            debrid_health.run_sweep()

        assert mock_remediation_deps['blocklist_add'].call_count == 0
        assert client.delete_torrent.call_count == 0
        assert mock_remediation_deps['arr_research'].call_count == 0
        assert mock_remediation_deps['log_event'].call_count == 0
        assert mock_remediation_deps['notify'].call_count == 0

    def test_on_triggers_full_pipeline(
            self, state_path, no_sleep, rd_enabled, auto_remediate_on,
            mock_remediation_deps):
        """AUTO_REMEDIATE on, single blocked torrent: blocklist + delete
        + arr re-search + history event all fire. One summary notify."""
        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000', filename='Landman.S01E04.1080p.AMZN.WEB-DL.mkv')],
            probe_results={'T1': self._BLOCKED},
        )
        client.delete_torrent.return_value = True

        with _patch_client(client):
            result = debrid_health.run_sweep()

        # Blocklist called with uppercased hash, filename, auto source
        mock_remediation_deps['blocklist_add'].assert_called_once()
        args, kwargs = mock_remediation_deps['blocklist_add'].call_args
        assert args[0] == 'AAAA0000'
        assert 'Landman' in args[1]
        assert kwargs['source'] == 'auto'
        assert 'infringing_file' in kwargs['reason']

        # Delete called with torrent id
        client.delete_torrent.assert_called_once_with('T1')

        # Arr re-search called with release name (filename minus extension)
        mock_remediation_deps['arr_research'].assert_called_once()
        assert mock_remediation_deps['arr_research'].call_args[0][0] == \
            'Landman.S01E04.1080p.AMZN.WEB-DL'

        # History event with the correct cause + meta
        mock_remediation_deps['log_event'].assert_called_once()
        log_kwargs = mock_remediation_deps['log_event'].call_args.kwargs
        assert log_kwargs['type'] == 'debrid'
        assert log_kwargs['source'] == 'debrid_health'
        meta = log_kwargs['meta']
        assert meta['cause'] == 'debrid_filtered'
        assert meta['reason'] == 'infringing_file'
        assert meta['http'] == 451
        assert meta['info_hash'] == 'AAAA0000'
        assert meta['deleted'] is True
        assert meta['blocklisted'] is True
        assert meta['researched'] is True

        # One summary notification for the sweep
        mock_remediation_deps['notify'].assert_called_once()
        notify_args = mock_remediation_deps['notify'].call_args.args
        assert notify_args[0] == 'debrid_filtered'
        assert '1' in notify_args[1]  # count

        # Sweep result reports remediation count
        assert 'remediated 1' in result['message']

    def test_remediate_cap_honoured(
            self, state_path, no_sleep, rd_enabled, auto_remediate_on,
            mock_remediation_deps, monkeypatch):
        """A first-run enable on a huge backlog must NOT mass-delete.
        Cap at _REMEDIATE_MAX_PER_SWEEP; remaining blocked entries stay
        flagged for next sweep."""
        monkeypatch.setattr(debrid_health, '_REMEDIATE_MAX_PER_SWEEP', 3)
        client = _mock_client(
            torrents=[_torrent(f'T{i}', f'HASH{i:04d}') for i in range(10)],
            probe_results={f'T{i}': self._BLOCKED for i in range(10)},
        )
        client.delete_torrent.return_value = True

        with _patch_client(client):
            result = debrid_health.run_sweep()

        # All 10 probed and classified as blocked — but only 3 deleted
        assert client.probe_file.call_count == 10
        assert mock_remediation_deps['blocklist_add'].call_count == 3
        assert client.delete_torrent.call_count == 3
        assert mock_remediation_deps['log_event'].call_count == 3
        assert 'remediated 3' in result['message']
        assert 'blocked 10' in result['message']

        # State persists ALL 10 as blocked — the next sweep will
        # remediate the remaining 7 (blocked entries always re-probe).
        with open(state_path) as f:
            saved = __import__('json').load(f)
        blocked_count = sum(
            1 for e in saved['probed'].values()
            if e.get('status') == 'blocked'
        )
        assert blocked_count == 10
        # First 3 carry the remediated flag, rest don't.
        remediated = sum(
            1 for e in saved['probed'].values() if e.get('remediated')
        )
        assert remediated == 3

    def test_blocklist_failure_does_not_stop_delete(
            self, state_path, no_sleep, rd_enabled, auto_remediate_on,
            mock_remediation_deps):
        """Failure isolation: a blocklist.add raise must not prevent the
        RD delete or the arr re-search. Captured in the history event's
        action flags so the partial success is auditable."""
        mock_remediation_deps['blocklist_add'].side_effect = RuntimeError('disk full')
        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000', filename='r.mkv')],
            probe_results={'T1': self._BLOCKED},
        )
        client.delete_torrent.return_value = True

        with _patch_client(client):
            debrid_health.run_sweep()

        # Delete and re-search still ran despite the blocklist failure
        client.delete_torrent.assert_called_once()
        mock_remediation_deps['arr_research'].assert_called_once()

        # History event records what actually succeeded
        meta = mock_remediation_deps['log_event'].call_args.kwargs['meta']
        assert meta['blocklisted'] is False
        assert meta['deleted'] is True
        assert meta['researched'] is True

    def test_delete_failure_does_not_stop_research_or_history(
            self, state_path, no_sleep, rd_enabled, auto_remediate_on,
            mock_remediation_deps):
        """delete_torrent can return False (e.g., RD API hiccup). The
        next sweep will retry — blocklist already protects against
        re-grabs in the interim. Arr re-search and history still fire."""
        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000', filename='r.mkv')],
            probe_results={'T1': self._BLOCKED},
        )
        client.delete_torrent.return_value = False  # delete reports failure

        with _patch_client(client):
            debrid_health.run_sweep()

        mock_remediation_deps['blocklist_add'].assert_called_once()
        mock_remediation_deps['arr_research'].assert_called_once()
        meta = mock_remediation_deps['log_event'].call_args.kwargs['meta']
        assert meta['blocklisted'] is True
        assert meta['deleted'] is False
        assert meta['researched'] is True

    def test_arr_research_returns_false_does_not_crash(
            self, state_path, no_sleep, rd_enabled, auto_remediate_on,
            mock_remediation_deps):
        """_attempt_arr_research returns False when no arr knows the
        release (e.g., grabbed manually via DMM). Should not block the
        sweep or other torrents."""
        mock_remediation_deps['arr_research'].return_value = False
        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000', filename='r.mkv')],
            probe_results={'T1': self._BLOCKED},
        )
        client.delete_torrent.return_value = True

        with _patch_client(client):
            debrid_health.run_sweep()

        meta = mock_remediation_deps['log_event'].call_args.kwargs['meta']
        assert meta['researched'] is False
        # Other actions still succeeded
        assert meta['blocklisted'] is True
        assert meta['deleted'] is True

    def test_no_summary_notify_when_zero_remediated(
            self, state_path, no_sleep, rd_enabled, auto_remediate_on,
            mock_remediation_deps):
        """All-healthy sweep with AUTO_REMEDIATE on shouldn't notify —
        notification is for remediation activity, not for empty sweeps."""
        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000')],
            probe_results={'T1': {'status': 'healthy'}},
        )
        with _patch_client(client):
            debrid_health.run_sweep()
        assert mock_remediation_deps['notify'].call_count == 0

    def test_healthy_and_unknown_dont_trigger_remediation(
            self, state_path, no_sleep, rd_enabled, auto_remediate_on,
            mock_remediation_deps):
        """Only ``blocked`` triggers remediation. ``healthy`` and
        ``unknown`` are NEVER touched — defense against false positives
        from transient RD throttling (the 503 case from live validation)."""
        client = _mock_client(
            torrents=[
                _torrent('T1', 'AAAA0000'),
                _torrent('T2', 'BBBB1111'),
                _torrent('T3', 'CCCC2222'),
            ],
            probe_results={
                'T1': {'status': 'healthy'},
                'T2': {'status': 'unknown', 'error': 'http_503'},
                'T3': self._BLOCKED,
            },
        )
        client.delete_torrent.return_value = True

        with _patch_client(client):
            debrid_health.run_sweep()

        # Only the blocked one triggers remediation
        client.delete_torrent.assert_called_once_with('T3')
        assert mock_remediation_deps['log_event'].call_count == 1
        meta = mock_remediation_deps['log_event'].call_args.kwargs['meta']
        assert meta['info_hash'] == 'CCCC2222'
