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


def _patch_clients(rd_client, tb_client):
    """Patch get_debrid_client to dispatch by service — RD when
    ``service='realdebrid'`` (or unspecified), TB when ``service='torbox'``.
    Used by the plan 39 phase 3 cross-rescue tests where both providers
    need to be reachable via the single seam.
    """
    def _dispatch(service=None, api_key=None):
        if service == 'torbox':
            return (tb_client, 'torbox')
        return (rd_client, 'realdebrid')
    return patch('utils.debrid_health.get_debrid_client', side_effect=_dispatch)


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
        """Rate-limit enforcement: a 60/min cap means an interruptible
        1.0-second wait between probes. Three probes → two waits (no wait
        after the last).  Pre-HIGH-7-fix the loop used ``time.sleep``;
        post-fix it uses ``_stop_event.wait`` so SIGTERM mid-sweep aborts
        cleanly without stalling the scheduler's 15s join window."""
        monkeypatch.setattr(debrid_health, '_RATE_LIMIT_PER_MIN', 60)
        wait_calls = []
        def fake_wait(timeout=None):
            wait_calls.append(timeout)
            return False
        monkeypatch.setattr(debrid_health._stop_event, 'wait', fake_wait)
        client = _mock_client(
            torrents=[_torrent(f'T{i}', f'HASH{i:04d}') for i in range(3)],
            probe_results={f'T{i}': {'status': 'healthy'} for i in range(3)},
        )
        with _patch_client(client):
            debrid_health.run_sweep()

        # Two waits total — between probe 0/1 and 1/2, none after probe 2.
        assert len(wait_calls) == 2
        for t in wait_calls:
            assert t == pytest.approx(1.0)

    def test_no_sleep_when_only_one_probe(self, state_path, no_sleep, rd_enabled, monkeypatch):
        """Single-probe sweep must not pay the rate-limit delay — wasted
        wall-clock time on the scheduler UI's 'running' indicator."""
        wait_calls = []
        def fake_wait(timeout=None):
            wait_calls.append(timeout)
            return False
        monkeypatch.setattr(debrid_health._stop_event, 'wait', fake_wait)
        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000')],
            probe_results={'T1': {'status': 'healthy'}},
        )
        with _patch_client(client):
            debrid_health.run_sweep()
        assert len(wait_calls) == 0


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

    def test_blocked_unknown_reason_warns_but_takes_no_action(
            self, state_path, no_sleep, rd_enabled, auto_remediate_on,
            mock_remediation_deps, caplog):
        """Future-proofing: if RD ships a new blocked-reason
        (``dmca_takedown``, ``regional_restriction``, ...), the gate
        won't recognise it and no action runs (safe fail-closed).  But
        the operator must see a WARN so the silent drift gets noticed
        before counts grow.  Throttled per-reason-per-sweep so a 1000-
        torrent sweep doesn't log 1000 times."""
        mystery = {'status': 'blocked', 'reason': 'dmca_takedown', 'http': 451}
        client = _mock_client(
            torrents=[
                _torrent('T1', 'AAAA0000', filename='ToBeDmcad.mkv'),
                _torrent('T2', 'BBBB1111', filename='AlsoDmcad.mkv'),
            ],
            probe_results={'T1': mystery, 'T2': mystery},
        )
        client.delete_torrent.return_value = True

        with caplog.at_level('WARNING'):
            with _patch_client(client):
                debrid_health.run_sweep()

        # No destructive action.
        assert client.delete_torrent.call_count == 0
        assert mock_remediation_deps['blocklist_add'].call_count == 0
        # WARN fired exactly once for the unknown reason — second
        # torrent with the same reason is throttled.
        warn_lines = [
            r.message for r in caplog.records
            if r.levelname == 'WARNING' and 'dmca_takedown' in r.message
        ]
        assert len(warn_lines) == 1

    def test_blocked_not_found_does_not_trigger_remediation(
            self, state_path, no_sleep, rd_enabled, auto_remediate_on,
            mock_remediation_deps):
        """HTTP 404 from /unrestrict/link maps to ``{status:'blocked',
        reason:'not_found'}`` — a transient CDN miss, file being re-
        processed by RD, or stale link from /torrents/info. Pre-fix this
        triggered the full destructive pipeline (blocklist + delete +
        arr re-search) on every transient 404. Remediation must gate on
        ``reason == 'infringing_file'``, not raw status."""
        not_found = {'status': 'blocked', 'reason': 'not_found', 'http': 404}
        client = _mock_client(
            torrents=[
                _torrent('T1', 'AAAA0000', filename='Transient.mkv'),
                _torrent('T2', 'BBBB1111', filename='RealBlock.mkv'),
            ],
            probe_results={
                'T1': not_found,
                'T2': self._BLOCKED,
            },
        )
        client.delete_torrent.return_value = True

        with _patch_client(client):
            debrid_health.run_sweep()

        # Only the confirmed filter-block triggers destructive remediation.
        client.delete_torrent.assert_called_once_with('T2')
        assert mock_remediation_deps['blocklist_add'].call_count == 1
        assert mock_remediation_deps['blocklist_add'].call_args.args[0] == 'BBBB1111'
        assert mock_remediation_deps['arr_research'].call_count == 1
        # State for the 404 torrent IS still persisted (so the dashboard
        # can show "blocked: 1") but no destructive action runs.
        with open(state_path) as f:
            saved = json.load(f)
        assert saved['probed']['AAAA0000']['status'] == 'blocked'
        assert saved['probed']['AAAA0000']['reason'] == 'not_found'
        assert 'remediated' not in saved['probed']['AAAA0000']


# ---------------------------------------------------------------------------
# Phase 4.1: force_episodes — TV re-search must fire even when Sonarr still
# thinks the episode is present.
#
# Regression scenario from the 2026-05-24 live sweep: 0/23 TV episodes were
# re-searched after auto-remediation because ``delete_torrent`` hasn't
# propagated through Zurg's WebDAV listing yet, so Sonarr's ``hasFile``
# state lags the truth by ~15-30 s. The fix wires ``force_episodes=True``
# through ``_remediate`` so the gate is bypassed for this caller only.
# ---------------------------------------------------------------------------

class TestForceEpisodesGate:

    @pytest.fixture
    def _remediation_persistence_stubs(self, monkeypatch):
        """Patch the destructive side effects of remediation EXCEPT
        ``_attempt_arr_research`` — this suite needs the real function so
        the ``force_episodes`` path is actually exercised."""
        monkeypatch.setattr(
            'utils.blocklist.add', MagicMock(return_value='blocklist-id'),
        )
        monkeypatch.setattr(
            'utils.history.log_event', MagicMock(return_value='event-id'),
        )
        monkeypatch.setattr('utils.notifications.notify', MagicMock())

    @pytest.fixture
    def _clean_retrigger_history(self):
        """``_retrigger_history`` is module-level state shared across the
        whole process — snapshot + restore so a test that fills it can't
        poison sibling tests."""
        from utils.scheduled_tasks import _retrigger_history
        snapshot = dict(_retrigger_history)
        _retrigger_history.clear()
        yield _retrigger_history
        _retrigger_history.clear()
        _retrigger_history.update(snapshot)

    def test_debrid_health_remediates_tv_with_hasFile_true(
            self, state_path, no_sleep, rd_enabled, auto_remediate_on,
            _remediation_persistence_stubs, _clean_retrigger_history,
    ):
        """End-to-end: a blocked TV release with Sonarr's last scan still
        reporting ``hasFile=True`` MUST still result in ``search_episodes``
        being called. Pre-fix this skipped every episode in the release.
        Post-fix the search is also restricted to the exact ep the release
        names (S01E04), not the whole season — otherwise a single blocked
        ep on a 200-ep anime show would fan out to 200 search jobs."""
        rd_client = _mock_client(
            torrents=[_torrent(
                'T1', 'AAAA0000',
                filename='Landman.S01E04.1080p.AMZN.WEB-DL.mkv',
            )],
            probe_results={'T1': {
                'status': 'blocked', 'reason': 'infringing_file', 'http': 451,
            }},
        )
        rd_client.delete_torrent.return_value = True

        sonarr = MagicMock()
        sonarr.configured = True
        sonarr.find_series_in_library.return_value = {'id': 100, 'title': 'Landman'}
        # hasFile=True is the whole point of this test — pre-fix this
        # skipped queueing the episode for search.
        sonarr.get_episodes.return_value = [
            {'id': 555, 'seasonNumber': 1, 'episodeNumber': 4, 'hasFile': True},
            {'id': 556, 'seasonNumber': 1, 'episodeNumber': 5, 'hasFile': True},
            {'id': 600, 'seasonNumber': 2, 'episodeNumber': 1, 'hasFile': True},
        ]

        with _patch_client(rd_client), \
                patch('utils.arr_client.SonarrClient', return_value=sonarr):
            debrid_health.run_sweep()

        # search_episodes called once with ONLY the targeted ep (S01E04 →
        # id 555). The season filter excludes S02E01, and the new ep-level
        # filter excludes S01E05.
        sonarr.search_episodes.assert_called_once()
        called_eps = sonarr.search_episodes.call_args.args[0]
        assert sorted(called_eps) == [555]

    def test_attempt_arr_research_default_skips_hasFile_true(
            self, _clean_retrigger_history,
    ):
        """Contract pin: ``_attempt_arr_research`` without ``force_episodes``
        MUST keep skipping episodes with ``hasFile=True``. This is the
        behaviour ``verify_symlinks`` and ``library._cleanup_broken_debrid_
        symlinks`` rely on (they only re-search what Sonarr already considers
        missing — anything else would risk spurious searches when a symlink
        is replaced rather than deleted)."""
        from utils.scheduled_tasks import _attempt_arr_research

        sonarr = MagicMock()
        sonarr.configured = True
        sonarr.find_series_in_library.return_value = {'id': 100, 'title': 'Landman'}
        sonarr.get_episodes.return_value = [
            {'id': 555, 'seasonNumber': 1, 'episodeNumber': 4, 'hasFile': True},
        ]

        with patch('utils.arr_client.SonarrClient', return_value=sonarr):
            triggered = _attempt_arr_research('Landman.S01E04.1080p.AMZN.WEB-DL')

        assert triggered is False
        sonarr.search_episodes.assert_not_called()

    def test_attempt_arr_research_force_episodes_includes_hasFile_true_target_ep(
            self, _clean_retrigger_history,
    ):
        """Direct unit test: ``force_episodes=True`` queues the targeted
        single ep for search regardless of ``hasFile`` state. The season
        filter AND the new ep-level filter both apply, so sibling eps in
        the same season (S01E05 here) are NOT queued — only S01E04 is."""
        from utils.scheduled_tasks import _attempt_arr_research

        sonarr = MagicMock()
        sonarr.configured = True
        sonarr.find_series_in_library.return_value = {'id': 100, 'title': 'Landman'}
        sonarr.get_episodes.return_value = [
            {'id': 555, 'seasonNumber': 1, 'episodeNumber': 4, 'hasFile': True},
            {'id': 556, 'seasonNumber': 1, 'episodeNumber': 5, 'hasFile': False},
            {'id': 600, 'seasonNumber': 2, 'episodeNumber': 1, 'hasFile': True},
        ]

        with patch('utils.arr_client.SonarrClient', return_value=sonarr):
            triggered = _attempt_arr_research(
                'Landman.S01E04.1080p.AMZN.WEB-DL', force_episodes=True,
            )

        assert triggered is True
        sonarr.search_episodes.assert_called_once()
        called_eps = sonarr.search_episodes.call_args.args[0]
        assert sorted(called_eps) == [555]

    def test_force_episodes_respects_retrigger_cooldown(
            self, _clean_retrigger_history,
    ):
        """``force_episodes`` lifts the hasFile gate but NOT the 2 h
        cooldown — successive remediations of the same release within the
        window must dedupe at the per-episode level. Protects against the
        broken-symlink and debrid-health paths fighting over the same item."""
        from utils.scheduled_tasks import _attempt_arr_research

        sonarr = MagicMock()
        sonarr.configured = True
        sonarr.find_series_in_library.return_value = {'id': 100, 'title': 'Landman'}
        sonarr.get_episodes.return_value = [
            {'id': 555, 'seasonNumber': 1, 'episodeNumber': 4, 'hasFile': True},
        ]

        with patch('utils.arr_client.SonarrClient', return_value=sonarr):
            assert _attempt_arr_research(
                'Landman.S01E04.WEB-DL', force_episodes=True,
            ) is True
            # Same release, same window — search must NOT fire again.
            assert _attempt_arr_research(
                'Landman.S01E04.WEB-DL', force_episodes=True,
            ) is False

        assert sonarr.search_episodes.call_count == 1

    def test_force_episodes_season_pack_queues_all_eps_in_season(
            self, _clean_retrigger_history,
    ):
        """A season-pack release name (no E## component) MUST queue every
        episode in the matched season under ``force_episodes=True`` —
        the user deleted the whole pack, every ep is now stale."""
        from utils.scheduled_tasks import _attempt_arr_research

        sonarr = MagicMock()
        sonarr.configured = True
        sonarr.find_series_in_library.return_value = {'id': 100, 'title': 'Landman'}
        sonarr.get_episodes.return_value = [
            {'id': 555, 'seasonNumber': 1, 'episodeNumber': 1, 'hasFile': True},
            {'id': 556, 'seasonNumber': 1, 'episodeNumber': 2, 'hasFile': True},
            {'id': 557, 'seasonNumber': 1, 'episodeNumber': 3, 'hasFile': True},
            {'id': 600, 'seasonNumber': 2, 'episodeNumber': 1, 'hasFile': True},
        ]

        with patch('utils.arr_client.SonarrClient', return_value=sonarr):
            triggered = _attempt_arr_research(
                'Landman.S01.Complete.2160p.WEB-DL', force_episodes=True,
            )

        assert triggered is True
        sonarr.search_episodes.assert_called_once()
        called_eps = sonarr.search_episodes.call_args.args[0]
        # All S01 eps, S02 excluded by the season filter.
        assert sorted(called_eps) == [555, 556, 557]

    def test_force_episodes_multi_ep_release_falls_back_to_season_wide(
            self, _clean_retrigger_history,
    ):
        """A double-ep release (``S01E04E05``) is too ambiguous for the
        single-ep regex to lock onto, so the implementation MUST fall
        back to season-wide queueing — otherwise the second ep would
        silently be left unsearched. Same logic for hyphenated multi-ep
        ranges (``S01E04-05``)."""
        from utils.scheduled_tasks import _attempt_arr_research

        sonarr = MagicMock()
        sonarr.configured = True
        sonarr.find_series_in_library.return_value = {'id': 100, 'title': 'Landman'}
        sonarr.get_episodes.return_value = [
            {'id': 555, 'seasonNumber': 1, 'episodeNumber': 4, 'hasFile': True},
            {'id': 556, 'seasonNumber': 1, 'episodeNumber': 5, 'hasFile': True},
            {'id': 557, 'seasonNumber': 1, 'episodeNumber': 6, 'hasFile': True},
        ]

        with patch('utils.arr_client.SonarrClient', return_value=sonarr):
            triggered = _attempt_arr_research(
                'Landman.S01E04E05.WEB-DL', force_episodes=True,
            )

        assert triggered is True
        sonarr.search_episodes.assert_called_once()
        called_eps = sonarr.search_episodes.call_args.args[0]
        # All S01 eps queued — implementation can't tell which subset the
        # multi-ep release covers, so plays it safe and queues the whole
        # season. Better extra searches than missed work.
        assert sorted(called_eps) == [555, 556, 557]


class TestRetriggerHistoryRollback:
    """L1: when the arr search API raises, the cooldown reservation MUST
    be rolled back so the next sweep can retry instead of being silently
    muzzled for 2 h. Pre-fix the cooldown was set before the API call,
    so an exception left the entries stuck until expiry.
    """

    @pytest.fixture
    def _clean_retrigger_history(self):
        from utils.scheduled_tasks import _retrigger_history
        snapshot = dict(_retrigger_history)
        _retrigger_history.clear()
        yield _retrigger_history
        _retrigger_history.clear()
        _retrigger_history.update(snapshot)

    def test_sonarr_search_failure_rolls_back_cooldown(
            self, _clean_retrigger_history,
    ):
        from utils.scheduled_tasks import _attempt_arr_research

        sonarr = MagicMock()
        sonarr.configured = True
        sonarr.find_series_in_library.return_value = {'id': 100, 'title': 'Landman'}
        sonarr.get_episodes.return_value = [
            {'id': 555, 'seasonNumber': 1, 'episodeNumber': 4, 'hasFile': True},
        ]
        sonarr.search_episodes.side_effect = RuntimeError('arr offline')

        with patch('utils.arr_client.SonarrClient', return_value=sonarr):
            with pytest.raises(RuntimeError):
                _attempt_arr_research(
                    'Landman.S01E04.WEB-DL', force_episodes=True,
                )

        # Cooldown rolled back — next sweep can retry the same ep.
        assert ('sonarr', 555) not in _clean_retrigger_history

    def test_radarr_search_failure_rolls_back_cooldown(
            self, _clean_retrigger_history,
    ):
        from utils.scheduled_tasks import _attempt_arr_research

        radarr = MagicMock()
        radarr.configured = True
        radarr.find_movie_in_library.return_value = {'id': 42, 'title': 'Gattaca'}
        radarr.search_movie.side_effect = RuntimeError('arr offline')

        with patch('utils.arr_client.RadarrClient', return_value=radarr):
            with pytest.raises(RuntimeError):
                _attempt_arr_research('Gattaca.1997.1080p.BluRay')

        assert ('radarr', 42) not in _clean_retrigger_history


class TestGetSummary:
    """Phase 5 — summary helper for the System page mini-dashboard."""

    @pytest.fixture
    def rd_creds(self, monkeypatch):
        monkeypatch.setenv('RD_API_KEY', 'test-key')

    def test_no_rd_configured_returns_minimal(self, state_path, monkeypatch):
        monkeypatch.delenv('RD_API_KEY', raising=False)
        monkeypatch.delenv('TORBOX_API_KEY', raising=False)
        # /run/secrets/rd_api_key shouldn't exist in the test env, but be
        # explicit to avoid flakiness on a developer host that happens to.
        monkeypatch.setattr(
            'os.path.isfile',
            lambda p: False if p == '/run/secrets/rd_api_key' else os.path.isfile(p),
        )
        summary = debrid_health.get_summary()
        # Plan 39 phase 5: providers[] is always present so the dual-card
        # UI can render even when only TB is configured; rd_configured
        # stays for back-compat with the original RD-only UI gate.
        assert summary == {'rd_configured': False, 'providers': [],
                           'rescued_24h': 0}

    def test_empty_state_returns_zero_counts(self, state_path, rd_creds, rd_enabled):
        summary = debrid_health.get_summary()
        assert summary['rd_configured'] is True
        assert summary['enabled'] is True
        assert summary['auto_remediate'] is False
        assert summary['last_sweep_ts'] is None
        assert summary['counts'] == {'healthy': 0, 'blocked': 0, 'unknown': 0, 'total': 0}
        assert summary['remediated_24h'] == 0

    def test_mixed_state_counts_correctly(self, state_path, rd_creds, rd_enabled):
        now = time.time()
        state_path.write_text(json.dumps({
            'version': 1,
            'probed': {
                'A': {'status': 'healthy', 'ts': now - 7200},
                'B': {'status': 'healthy', 'ts': now - 3600},
                'C': {'status': 'blocked', 'ts': now - 1800, 'reason': 'infringing_file'},
                'D': {'status': 'unknown', 'ts': now - 900, 'error': 'http_503'},
                'E': {'status': 'unknown', 'ts': now - 60, 'error': 'http_503'},
            },
        }))
        summary = debrid_health.get_summary()
        assert summary['counts'] == {'healthy': 2, 'blocked': 1, 'unknown': 2, 'total': 5}
        # last_sweep_ts is the MAX ts across all entries (= most recent probe)
        assert summary['last_sweep_ts'] == pytest.approx(now - 60, abs=1)

    def test_auto_remediate_reflects_env(self, state_path, rd_creds, rd_enabled, auto_remediate_on):
        summary = debrid_health.get_summary()
        assert summary['auto_remediate'] is True

    def test_disabled_env_still_reports(self, state_path, rd_creds, monkeypatch):
        """When DEBRID_HEALTH_ENABLED=false the card still surfaces state
        (counts from the last sweep before the disable) — operators need
        to see that the prober is off."""
        monkeypatch.setenv('DEBRID_HEALTH_ENABLED', 'false')
        summary = debrid_health.get_summary()
        assert summary['enabled'] is False
        assert summary['rd_configured'] is True

    def test_remediated_24h_filters_by_cause(self, state_path, rd_creds, rd_enabled, tmp_path, monkeypatch):
        """remediated_24h counts only history events whose meta.cause ==
        'debrid_filtered'. Other events MUST NOT be counted."""
        from utils import history
        monkeypatch.setattr(history, '_file_path', str(tmp_path / 'history.jsonl'))
        history.log_event('debrid', 'A', meta={'cause': 'debrid_filtered'})
        history.log_event('debrid', 'B', meta={'cause': 'debrid_filtered'})
        history.log_event('debrid', 'C', meta={'cause': 'debrid_filtered'})
        history.log_event('debrid', 'D', meta={'cause': 'something_else'})
        history.log_event('debrid', 'E')  # no cause
        summary = debrid_health.get_summary()
        assert summary['remediated_24h'] == 3

    def test_remediated_24h_swallows_history_error(self, state_path, rd_creds, rd_enabled, monkeypatch):
        """A history failure must not break the summary endpoint —
        the card silently degrades to 0 rather than 500ing the response."""
        def boom(*a, **kw):
            raise RuntimeError('history broken')
        monkeypatch.setattr('utils.history.count_by_cause_windows', boom)
        summary = debrid_health.get_summary()
        assert summary['remediated_24h'] == 0
        assert summary['rescued_24h'] == 0
        # other fields still populated
        assert summary['rd_configured'] is True


class TestDualDebridDashboard:
    """Plan 39 phase 5 — System page mini-dashboard renders side-by-side
    RD + TB cards when both providers are configured, collapses to one
    card when only one is, and shows zero cards (with rd_configured=False
    legacy hint) when neither is configured."""

    def test_rd_only_returns_single_provider(self, state_path, monkeypatch):
        monkeypatch.setenv('RD_API_KEY', 'rd')
        monkeypatch.delenv('TORBOX_API_KEY', raising=False)
        summary = debrid_health.get_summary()
        services = [p['service'] for p in summary.get('providers', [])]
        assert services == ['realdebrid']
        assert summary['rd_configured'] is True

    def test_both_configured_returns_two_providers(self, state_path, monkeypatch):
        monkeypatch.setenv('RD_API_KEY', 'rd')
        monkeypatch.setenv('TORBOX_API_KEY', 'tb')
        summary = debrid_health.get_summary()
        services = [p['service'] for p in summary.get('providers', [])]
        assert services == ['realdebrid', 'torbox']
        # Each card carries label + counts + last_probe_ts at minimum.
        for card in summary['providers']:
            assert 'label' in card
            assert 'counts' in card
            assert 'configured' in card

    def test_tb_only_returns_tb_card(self, state_path, monkeypatch):
        monkeypatch.delenv('RD_API_KEY', raising=False)
        monkeypatch.setenv('TORBOX_API_KEY', 'tb')
        # Capture the real isfile BEFORE patching so the lambda doesn't
        # recurse into itself on the fallthrough branch.
        real_isfile = os.path.isfile
        monkeypatch.setattr(
            'os.path.isfile',
            lambda p: False if p == '/run/secrets/rd_api_key' else real_isfile(p),
        )
        summary = debrid_health.get_summary()
        services = [p['service'] for p in summary.get('providers', [])]
        assert services == ['torbox']
        assert summary['rd_configured'] is False
        # TB-only setups still get a rescue counter for the card.
        assert summary['rescued_24h'] == 0

    def test_neither_configured_returns_empty(self, state_path, monkeypatch):
        monkeypatch.delenv('RD_API_KEY', raising=False)
        monkeypatch.delenv('TORBOX_API_KEY', raising=False)
        real_isfile = os.path.isfile
        monkeypatch.setattr(
            'os.path.isfile',
            lambda p: False if p == '/run/secrets/rd_api_key' else real_isfile(p),
        )
        summary = debrid_health.get_summary()
        assert summary == {'rd_configured': False, 'providers': [],
                           'rescued_24h': 0}

    def test_rescued_24h_separate_from_remediated_24h(
            self, state_path, tmp_path, monkeypatch):
        """rescued_24h counts ALL rescue paths (sweep cross-rescue,
        blackhole cached-alt grab, Wanted recovery) — these span three
        different event types; remediated_24h stays debrid_filtered-only."""
        monkeypatch.setenv('RD_API_KEY', 'rd')
        monkeypatch.setenv('DEBRID_HEALTH_ENABLED', 'true')
        from utils import history
        monkeypatch.setattr(history, '_file_path', str(tmp_path / 'history.jsonl'))
        history.log_event('debrid', 'A', meta={'cause': 'debrid_filtered'})
        history.log_event('debrid', 'B', meta={'cause': 'debrid_rescued'})
        history.log_event('tb_cached_alt_grabbed', 'C',
                          meta={'cause': 'tb_cached_alt_grabbed'})
        history.log_event('debrid_add', 'D',
                          meta={'cause': 'wanted_tb_recovered'})
        summary = debrid_health.get_summary()
        assert summary['remediated_24h'] == 1
        assert summary['rescued_24h'] == 3

    def test_tb_card_rescue_count_from_history(self, state_path, tmp_path, monkeypatch):
        """TB card counts.rescued is history-derived across all rescue
        paths — NOT the sweep-state ``rescued=True`` flag, which
        structurally undercounts (only sweep rescues set it, and a
        re-probe rebuilds the entry without it)."""
        monkeypatch.setenv('RD_API_KEY', 'rd')
        monkeypatch.setenv('TORBOX_API_KEY', 'tb')
        from utils import history
        monkeypatch.setattr(history, '_file_path', str(tmp_path / 'history.jsonl'))
        history.log_event('debrid', 'A', meta={'cause': 'debrid_rescued'})
        history.log_event('tb_cached_alt_grabbed', 'B',
                          meta={'cause': 'tb_cached_alt_grabbed'})
        history.log_event('debrid_add', 'C',
                          meta={'cause': 'wanted_tb_recovered'})
        # A state-flag entry must not add to the history-derived count.
        state_path.write_text(json.dumps({
            'version': 1,
            'probed': {
                'A': {'status': 'unknown', 'ts': time.time(), 'rescued': True},
            },
        }))
        summary = debrid_health.get_summary()
        tb_card = next(p for p in summary['providers'] if p['service'] == 'torbox')
        assert tb_card['counts']['rescued'] == 3


class TestRemediateFilenameSanitisation:
    """M2: a RD filename packed with path components (e.g. an uploader
    nested the file under ``Show/S01/ep.mkv``) MUST be reduced to the
    basename before parsing into a release name — otherwise the parser
    sees the path and could fuzzy-match the wrong show in Sonarr, which
    under ``force_episodes=True`` would queue searches for the wrong
    show's episodes."""

    def test_remediate_strips_path_components_from_filename(
            self, monkeypatch,
    ):
        from utils import debrid_health

        mock_arr_research = MagicMock(return_value=True)
        monkeypatch.setattr(
            'utils.scheduled_tasks._attempt_arr_research', mock_arr_research,
        )
        monkeypatch.setattr(
            'utils.blocklist.add', MagicMock(return_value='id'),
        )
        monkeypatch.setattr(
            'utils.history.log_event', MagicMock(return_value='evt-id'),
        )

        rd_client = MagicMock()
        rd_client.delete_torrent.return_value = True

        debrid_health._remediate(
            client=rd_client,
            torrent_id='T1',
            torrent_hash='AAAA0000',
            filename='Show/S01/Show.S01E04.WEB-DL.mkv',
            probe_result={'status': 'blocked', 'reason': 'infringing_file',
                          'http': 451},
        )

        mock_arr_research.assert_called_once()
        # Release name passed to _attempt_arr_research must be the
        # basename minus extension, NO path component.
        passed_release_name = mock_arr_research.call_args.args[0]
        assert passed_release_name == 'Show.S01E04.WEB-DL'
        assert '/' not in passed_release_name


# ---------------------------------------------------------------------------
# Phase 3: cross-debrid rescue
#
# When RD filter-blocks a torrent and TB has it cached, the rescue path
# adds the hash to TB, waits for it to be ready, retargets arr-library
# symlinks from the RD base to the TB base, and skips blocklist + delete
# + arr re-search.  Any failure in the rescue path falls through to the
# existing remediation pipeline.
# ---------------------------------------------------------------------------

class TestCrossRescue:
    _BLOCKED = {'status': 'blocked', 'reason': 'infringing_file', 'http': 451}

    @pytest.fixture
    def tb_configured(self, monkeypatch):
        monkeypatch.setenv('TORBOX_API_KEY', 'tb-key')
        monkeypatch.setenv('RD_API_KEY', 'rd-key')

    @pytest.fixture
    def rescue_on(self, monkeypatch, tb_configured):
        # Explicit ON for clarity (would default-on with both keys set).
        monkeypatch.setenv('DEBRID_HEALTH_CROSS_RESCUE', 'true')

    @pytest.fixture
    def mock_tb_client(self):
        """A mock TBClient.  ``add_magnet`` returns a fake TB id;
        ``torrent_status`` returns 'completed' so the poll loop
        short-circuits on first iteration.  Not auto-patched — caller
        uses ``_patch_clients(rd_client, tb)`` below so both providers
        resolve to the right mock via the single ``get_debrid_client``
        seam.
        """
        tb = MagicMock()
        tb.configured = True
        tb.add_magnet.return_value = 'TB_ID_123'
        tb.torrent_status.return_value = 'completed'
        return tb

    @pytest.fixture
    def tb_cached(self, monkeypatch):
        """Patch utils.search.check_debrid_cache → TB returns cached=True."""
        def fake_check(hashes, service=None, api_key=None):
            if service == 'torbox':
                return {h: True for h in hashes}
            return {h: None for h in hashes}
        monkeypatch.setattr('utils.search.check_debrid_cache', fake_check)

    @pytest.fixture
    def arr_library_with_symlink(self, tmp_path, monkeypatch):
        """Build a fake arr movie library with one symlink pointing at the
        RD base.  Returns (lib_root, symlink_path, original_target)."""
        rd_base = str(tmp_path / 'debrid')
        os.makedirs(os.path.join(rd_base, 'movies', 'Release.2025'), exist_ok=True)
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', rd_base)
        monkeypatch.setenv(
            'BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX',
            str(tmp_path / 'debrid_torbox'),
        )

        lib_root = tmp_path / 'local_media' / 'movies'
        lib_root.mkdir(parents=True)
        monkeypatch.setenv('BLACKHOLE_LOCAL_LIBRARY_MOVIES', str(lib_root))

        sym = lib_root / 'Release (2025)' / 'file.mkv'
        sym.parent.mkdir(parents=True)
        original_target = rd_base + '/movies/Release.2025/file.mkv'
        os.symlink(original_target, sym)
        return lib_root, sym, original_target

    @pytest.mark.parametrize('tb_ready_status', ['completed', 'cached', 'uploading'])
    def test_cross_rescue_short_circuits_remediation_on_success(
            self, state_path, no_sleep, rd_enabled, rescue_on, tb_cached,
            mock_tb_client, arr_library_with_symlink, mock_remediation_deps,
            tb_ready_status):
        """RD blocked + TB cached + TB add succeeds → rescue path fires,
        blocklist/delete/arr re-search must NOT be called.

        Parametrised across every TB ready state.  Pre-fix the poll
        only accepted ``'completed'`` and silently timed out on every
        ``'cached'``/``'uploading'`` response — which is the dominant
        case for instant-cache hits (the whole point of cross-rescue).
        """
        mock_tb_client.torrent_status.return_value = tb_ready_status
        lib_root, sym, original_target = arr_library_with_symlink
        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000', filename='Release.2025.mkv')],
            probe_results={'T1': self._BLOCKED},
        )
        with _patch_clients(client, mock_tb_client):
            debrid_health.run_sweep()

        # TB add was called with the hash
        mock_tb_client.add_magnet.assert_called_once_with('AAAA0000')
        # Symlink retargeted
        new_target = os.readlink(str(sym))
        assert 'debrid_torbox' in new_target
        assert new_target.endswith('/movies/Release.2025/file.mkv')
        # Existing remediation pipeline NOT triggered
        assert mock_remediation_deps['blocklist_add'].call_count == 0
        assert client.delete_torrent.call_count == 0
        assert mock_remediation_deps['arr_research'].call_count == 0
        # History event with the rescue cause
        from utils import history as _h
        log_calls = mock_remediation_deps['log_event'].call_args_list
        assert any(
            c.kwargs.get('meta', {}).get('cause') == _h.CAUSE_DEBRID_RESCUED
            for c in log_calls
        )

    def test_blocked_not_found_does_not_trigger_rescue(
            self, state_path, no_sleep, rd_enabled, rescue_on, tb_cached,
            mock_tb_client, arr_library_with_symlink, mock_remediation_deps):
        """A 404 from /unrestrict/link maps to ``{status:'blocked',
        reason:'not_found'}`` — a transient CDN miss, file mid re-process,
        or stale link. Pre-fix this triggered a speculative TB add (waste +
        ghost-torrent risk) AND symlink retarget (silently broke playback
        when RD recovered a sweep later). Rescue must gate on
        ``reason == 'infringing_file'``."""
        not_found = {'status': 'blocked', 'reason': 'not_found', 'http': 404}
        lib_root, sym, original_target = arr_library_with_symlink
        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000', filename='Release.2025.mkv')],
            probe_results={'T1': not_found},
        )
        with _patch_clients(client, mock_tb_client):
            debrid_health.run_sweep()

        # TB add must NOT have been called.
        mock_tb_client.add_magnet.assert_not_called()
        # Symlink must NOT have been retargeted.
        assert os.readlink(str(sym)) == original_target
        # Existing remediation pipeline also NOT triggered (rescue gate
        # is upstream of remediate gate, both keyed on same reason).
        assert mock_remediation_deps['blocklist_add'].call_count == 0
        assert client.delete_torrent.call_count == 0
        assert mock_remediation_deps['arr_research'].call_count == 0
        # State persists for dashboard visibility but with no rescue flag.
        with open(state_path) as f:
            saved = json.load(f)
        assert saved['probed']['AAAA0000']['status'] == 'blocked'
        assert saved['probed']['AAAA0000']['reason'] == 'not_found'
        assert 'rescued' not in saved['probed']['AAAA0000']

    def test_tb_not_cached_falls_through_to_remediation(
            self, state_path, no_sleep, rd_enabled, rescue_on,
            mock_tb_client, monkeypatch, mock_remediation_deps):
        """TB cache probe returns False → rescue declines → existing
        remediation runs unchanged."""
        # Enable AUTO_REMEDIATE so we can observe the fallthrough path
        monkeypatch.setenv('DEBRID_HEALTH_AUTO_REMEDIATE', 'true')

        def fake_check(hashes, service=None, **kw):
            return {h: False for h in hashes}
        monkeypatch.setattr('utils.search.check_debrid_cache', fake_check)

        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000')],
            probe_results={'T1': self._BLOCKED},
        )
        client.delete_torrent.return_value = True
        with _patch_clients(client, mock_tb_client):
            debrid_health.run_sweep()

        # Rescue helper saw TB miss → fell through
        mock_tb_client.add_magnet.assert_not_called()
        # Remediation pipeline ran instead
        assert mock_remediation_deps['blocklist_add'].call_count == 1
        client.delete_torrent.assert_called_once_with('T1')

    def test_tb_add_failure_falls_through_to_remediation(
            self, state_path, no_sleep, rd_enabled, rescue_on, tb_cached,
            mock_tb_client, monkeypatch, mock_remediation_deps):
        """TB cache hit but add_magnet returns None → fall through."""
        monkeypatch.setenv('DEBRID_HEALTH_AUTO_REMEDIATE', 'true')
        mock_tb_client.add_magnet.return_value = None

        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000')],
            probe_results={'T1': self._BLOCKED},
        )
        client.delete_torrent.return_value = True
        with _patch_clients(client, mock_tb_client):
            debrid_health.run_sweep()

        mock_tb_client.add_magnet.assert_called_once()
        assert mock_remediation_deps['blocklist_add'].call_count == 1
        client.delete_torrent.assert_called_once_with('T1')

    def test_tb_never_ready_cleans_up_and_falls_through(
            self, state_path, no_sleep, rd_enabled, rescue_on, tb_cached,
            mock_tb_client, monkeypatch, mock_remediation_deps):
        """TB add succeeds but status never reaches 'completed' →
        rescue helper deletes the TB entry and falls through to
        the existing remediate path."""
        monkeypatch.setenv('DEBRID_HEALTH_AUTO_REMEDIATE', 'true')
        mock_tb_client.torrent_status.return_value = 'downloading'  # never 'completed'

        # Squeeze the rescue poll deadline so the test finishes fast.
        # Patching the constants is cleaner than patching ``time.time``
        # globally — the latter breaks pytest's own timing infrastructure
        # and causes the test session to spin.
        monkeypatch.setattr(debrid_health, '_RESCUE_READY_TIMEOUT', 0.01)
        monkeypatch.setattr(debrid_health, '_RESCUE_POLL_INTERVAL', 0.001)

        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000')],
            probe_results={'T1': self._BLOCKED},
        )
        client.delete_torrent.return_value = True
        with _patch_clients(client, mock_tb_client):
            debrid_health.run_sweep()

        # TB add was called, then TB delete (cleanup), then remediation ran
        mock_tb_client.add_magnet.assert_called_once()
        mock_tb_client.delete_torrent.assert_called_once_with('TB_ID_123')
        assert mock_remediation_deps['blocklist_add'].call_count == 1

    def test_rescue_off_skips_rescue_path(
            self, state_path, no_sleep, rd_enabled, monkeypatch,
            mock_tb_client, mock_remediation_deps):
        """DEBRID_HEALTH_CROSS_RESCUE=false → no TB probe, no rescue, only
        the existing remediation runs if enabled."""
        monkeypatch.setenv('RD_API_KEY', 'rd-key')
        monkeypatch.setenv('TORBOX_API_KEY', 'tb-key')
        monkeypatch.setenv('DEBRID_HEALTH_CROSS_RESCUE', 'false')
        monkeypatch.setenv('DEBRID_HEALTH_AUTO_REMEDIATE', 'true')

        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000')],
            probe_results={'T1': self._BLOCKED},
        )
        client.delete_torrent.return_value = True
        with _patch_clients(client, mock_tb_client):
            debrid_health.run_sweep()

        mock_tb_client.add_magnet.assert_not_called()
        # Existing remediation still fires
        assert mock_remediation_deps['blocklist_add'].call_count == 1

    def test_no_symlinks_found_still_counts_as_rescue(
            self, state_path, no_sleep, rd_enabled, rescue_on, tb_cached,
            mock_tb_client, monkeypatch, mock_remediation_deps):
        """Content not yet imported by the arr → no symlinks to retarget,
        but the file is still accessible via TB now.  Rescue is recorded
        (not a failure) and existing remediation is skipped."""
        # No arr library configured → zero symlinks scanned, zero retargeted
        monkeypatch.setenv('BLACKHOLE_LOCAL_LIBRARY_TV', '')
        monkeypatch.setenv('BLACKHOLE_LOCAL_LIBRARY_MOVIES', '')
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', '/mnt/debrid')

        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000')],
            probe_results={'T1': self._BLOCKED},
        )
        with _patch_clients(client, mock_tb_client):
            debrid_health.run_sweep()

        mock_tb_client.add_magnet.assert_called_once()
        # Remediation skipped
        assert mock_remediation_deps['blocklist_add'].call_count == 0
        # History event still emitted with the rescue cause and outcome
        from utils import history as _h
        log_meta = [
            c.kwargs.get('meta', {})
            for c in mock_remediation_deps['log_event'].call_args_list
        ]
        rescue_metas = [m for m in log_meta
                        if m.get('cause') == _h.CAUSE_DEBRID_RESCUED]
        assert len(rescue_metas) == 1
        assert rescue_metas[0]['rescue_outcome'] == 'no_symlinks_found'

    def test_symlink_unrelated_to_blocked_torrent_not_retargeted(
            self, state_path, no_sleep, rd_enabled, rescue_on, tb_cached,
            mock_tb_client, tmp_path, monkeypatch, mock_remediation_deps):
        """Symlinks pointing OUTSIDE the RD base must not be touched
        (e.g. a local-disc symlink, or a symlink to a different mount
        the user manually configured)."""
        rd_base = str(tmp_path / 'debrid')
        os.makedirs(os.path.join(rd_base, 'movies'), exist_ok=True)
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', rd_base)
        monkeypatch.setenv(
            'BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX',
            str(tmp_path / 'debrid_torbox'),
        )

        lib_root = tmp_path / 'local_media' / 'movies'
        lib_root.mkdir(parents=True)
        monkeypatch.setenv('BLACKHOLE_LOCAL_LIBRARY_MOVIES', str(lib_root))

        # Symlink pointing somewhere else entirely
        unrelated = lib_root / 'Local.Disc' / 'file.mkv'
        unrelated.parent.mkdir(parents=True)
        external_target = str(tmp_path / 'local_disc' / 'file.mkv')
        os.symlink(external_target, unrelated)

        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000')],
            probe_results={'T1': self._BLOCKED},
        )
        with _patch_clients(client, mock_tb_client):
            debrid_health.run_sweep()

        # Unrelated symlink target unchanged
        assert os.readlink(str(unrelated)) == external_target

    def test_rescue_deferred_when_plex_actively_streams(
            self, state_path, no_sleep, rd_enabled, rescue_on, tb_cached,
            mock_tb_client, arr_library_with_symlink, monkeypatch,
            mock_remediation_deps):
        """HIGH-8 regression: a rescue MUST NOT retarget a symlink that's
        currently being streamed by Plex.  Swapping it mid-stream can
        leave new seeks broken (FUSE FD cache covers the in-flight read
        but not future range requests).  Pre-fix the rescue path had no
        Plex guard at all; post-fix it skips the retarget and cleans up
        the alt-debrid add so a future sweep can try again."""
        lib_root, sym, original_target = arr_library_with_symlink

        # Pretend Plex reports an active session for this release.
        monkeypatch.setattr(
            debrid_health, '_plex_session_active_for_release',
            lambda release_name: release_name == 'Release.2025',
        )

        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000', filename='Release.2025.mkv')],
            probe_results={'T1': self._BLOCKED},
        )
        with _patch_clients(client, mock_tb_client):
            debrid_health.run_sweep()

        # TB add was still attempted (rescue helper has to make the call to
        # know the file is reachable) — but the TB delete fires to clean
        # up since we're deferring.
        mock_tb_client.add_magnet.assert_called_once()
        mock_tb_client.delete_torrent.assert_called_once_with('TB_ID_123')
        # Symlink left untouched
        assert os.readlink(str(sym)) == original_target

    def test_run_sweep_aborts_on_stop_event_during_rate_limit(
            self, state_path, rd_enabled, monkeypatch):
        """HIGH-7: a stop signal received between probes must short-
        circuit the per-probe rate-limit pause.  Pre-fix the loop used
        plain ``time.sleep(60/_RATE_LIMIT_PER_MIN)`` which is
        non-interruptible.  Post-fix it waits on ``_stop_event``."""
        monkeypatch.setattr(debrid_health, '_RATE_LIMIT_PER_MIN', 60)

        # Sleep-aware sleep replacement that sets the stop event after
        # the first invocation so the SECOND probe never happens.
        wait_calls = []
        real_wait = debrid_health._stop_event.wait
        def fake_wait(timeout=None):
            wait_calls.append(timeout)
            if len(wait_calls) == 1:
                debrid_health._stop_event.set()
            return real_wait(timeout=0.0)  # don't actually sleep in tests
        monkeypatch.setattr(debrid_health._stop_event, 'wait', fake_wait)

        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000'),
                      _torrent('T2', 'BBBB1111'),
                      _torrent('T3', 'CCCC2222')],
            probe_results={
                'T1': {'status': 'healthy'},
                'T2': {'status': 'healthy'},
                'T3': {'status': 'healthy'},
            },
        )
        with _patch_client(client):
            result = debrid_health.run_sweep()

        # First probe completed, then sleep was interruptible and aborted
        # before probes 2 and 3.
        assert client.probe_file.call_count == 1
        assert result['items'] == 1

    def test_unrelated_rd_symlink_not_retargeted(
            self, state_path, no_sleep, rd_enabled, rescue_on, tb_cached,
            mock_tb_client, tmp_path, monkeypatch, mock_remediation_deps):
        """CRITICAL-1 regression: a rescue for torrent A MUST NOT retarget
        symlinks that belong to OTHER RD torrents.  Pre-fix the helper
        walked every symlink under the RD base prefix and rewrote them all,
        which silently broke the whole RD library on first rescue (only
        the rescued torrent's content exists on TB).  Filter must be
        per-release: retarget only symlinks whose target path contains
        ``/<release_name>/`` (matching blackhole's torrent-folder layout)."""
        rd_base = str(tmp_path / 'debrid')
        tb_base = str(tmp_path / 'debrid_torbox')
        # Pre-create both release dirs on RD so islink/readlink work.
        os.makedirs(os.path.join(rd_base, 'movies', 'Release.2025'), exist_ok=True)
        os.makedirs(os.path.join(rd_base, 'movies', 'Unrelated.2024'), exist_ok=True)
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE', rd_base)
        monkeypatch.setenv('BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX', tb_base)

        lib_root = tmp_path / 'local_media' / 'movies'
        lib_root.mkdir(parents=True)
        monkeypatch.setenv('BLACKHOLE_LOCAL_LIBRARY_MOVIES', str(lib_root))

        # Symlink for the rescued release — should be retargeted to TB.
        rescued_link = lib_root / 'Release (2025)' / 'file.mkv'
        rescued_link.parent.mkdir(parents=True)
        rescued_target = rd_base + '/movies/Release.2025/file.mkv'
        os.symlink(rescued_target, rescued_link)

        # Symlink for an UNRELATED RD release — must stay pointing at RD,
        # because that content is NOT cached on TB.  Pre-fix this got
        # rewritten to point at /debrid_torbox/... and became broken.
        unrelated_link = lib_root / 'Unrelated (2024)' / 'film.mkv'
        unrelated_link.parent.mkdir(parents=True)
        unrelated_target = rd_base + '/movies/Unrelated.2024/film.mkv'
        os.symlink(unrelated_target, unrelated_link)

        client = _mock_client(
            torrents=[_torrent('T1', 'AAAA0000', filename='Release.2025.mkv')],
            probe_results={'T1': self._BLOCKED},
        )
        with _patch_clients(client, mock_tb_client):
            debrid_health.run_sweep()

        # Rescued symlink: target swapped to TB
        new_target = os.readlink(str(rescued_link))
        assert new_target.startswith(tb_base + '/'), (
            f"rescued link not retargeted to TB: {new_target}"
        )
        # Unrelated symlink: target MUST still point at RD
        assert os.readlink(str(unrelated_link)) == unrelated_target, (
            f"unrelated RD symlink was incorrectly retargeted: "
            f"{os.readlink(str(unrelated_link))}"
        )
