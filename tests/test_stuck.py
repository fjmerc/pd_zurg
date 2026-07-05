"""Tests for utils/stuck.py — the stuck-content aggregator behind /api/stuck."""

import importlib
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone

import pytest

from utils import attempt_ledger, history, stuck


def _iso(dt):
    return dt.isoformat(timespec='seconds')


def _write_history(config_dir, events):
    path = os.path.join(config_dir, 'history.jsonl')
    with open(path, 'w', encoding='utf-8') as f:
        for i, e in enumerate(events):
            e.setdefault('id', f'ev{i}')
            f.write(json.dumps(e) + '\n')


@pytest.fixture
def stores(tmp_dir, monkeypatch):
    """Fresh ledger + history in a temp config dir, isolated env, cold cache."""
    importlib.reload(attempt_ledger)
    attempt_ledger.init(config_dir=tmp_dir)
    history.init(tmp_dir)
    for var in ('BLACKHOLE_DIR', 'BLACKHOLE_COMPLETED_DIR',
                'BLACKHOLE_SYMLINK_ENABLED', 'PENDING_WARNING_HOURS',
                'FORCE_GRAB_MAX_ATTEMPTS', 'BLACKHOLE_TB_ALT_MAX_ATTEMPTS'):
        monkeypatch.delenv(var, raising=False)
    import utils.library
    monkeypatch.setattr(utils.library, 'get_scanner', lambda: None)
    stuck.invalidate_cache()
    yield tmp_dir
    stuck.invalidate_cache()
    history._file_path = None


class _FakeScanner:
    def __init__(self, data=None, memos=None):
        self._data = data or {'movies': [], 'shows': []}
        self._memos = memos or {'no_results': {}, 'rd_miss': {},
                                'tb_cooldown': {}}
        self.cleared = []

    def peek_data(self):
        return self._data

    def wanted_recovery_snapshot(self):
        return self._memos

    def clear_wanted_memos(self, imdb_id):
        self.cleared.append(imdb_id)
        return 2


# ---------------------------------------------------------------------------
# In-flight (pending_monitors.json) collector
# ---------------------------------------------------------------------------

class TestCollectInflight:

    def _write_pending(self, d, entries):
        with open(os.path.join(d, 'pending_monitors.json'), 'w') as f:
            json.dump(entries, f)

    def test_stale_entry_surfaces_fresh_does_not(self, stores, monkeypatch):
        monkeypatch.setenv('BLACKHOLE_DIR', stores)
        now = time.time()
        self._write_pending(stores, [
            {'torrent_id': 'a1', 'filename': 'Old.Release.1080p',
             'debrid': 'torbox', 'timestamp': now - 48 * 3600},
            {'torrent_id': 'b2', 'filename': 'Fresh.Release.1080p',
             'debrid': 'realdebrid', 'timestamp': now - 60},
        ])
        records = {}
        stuck._collect_inflight(records)
        assert len(records) == 1
        rec = next(iter(records.values()))
        assert rec['key'].startswith('inflight:')
        assert rec['title'] == 'Old.Release.1080p'
        assert 'inflight_stale' in rec['reasons']
        assert rec['provider'] == 'torbox'
        assert rec['since'] is not None

    def test_compromised_entry_gets_reason(self, stores, monkeypatch):
        monkeypatch.setenv('BLACKHOLE_DIR', stores)
        self._write_pending(stores, [
            {'torrent_id': 'a1', 'filename': 'X.2160p', 'debrid': 'torbox',
             'timestamp': time.time() - 48 * 3600, 'compromised': True},
        ])
        records = {}
        stuck._collect_inflight(records)
        rec = next(iter(records.values()))
        assert 'compromised' in rec['reasons']

    def test_pending_warning_hours_env_respected(self, stores, monkeypatch):
        monkeypatch.setenv('BLACKHOLE_DIR', stores)
        monkeypatch.setenv('PENDING_WARNING_HOURS', '1')
        self._write_pending(stores, [
            {'torrent_id': 'a1', 'filename': 'X', 'debrid': 'torbox',
             'timestamp': time.time() - 2 * 3600},
        ])
        records = {}
        stuck._collect_inflight(records)
        assert len(records) == 1

    def test_zero_warning_hours_falls_back_to_24(self, stores, monkeypatch):
        monkeypatch.setenv('PENDING_WARNING_HOURS', '0')
        assert stuck._pending_stuck_threshold_seconds() == 24 * 3600

    def test_no_env_no_files_is_quiet(self, stores):
        records = {}
        stuck._collect_inflight(records)
        assert records == {}


# ---------------------------------------------------------------------------
# History failure-streak collector
# ---------------------------------------------------------------------------

class TestCollectHistory:

    def _failure(self, ts, title='Some Movie', cause='uncached_timeout'):
        return {'ts': _iso(ts), 'type': 'failed', 'title': title,
                'meta': {'cause': cause}}

    def test_qualifying_streak_surfaces(self, stores):
        now = datetime.now(timezone.utc)
        _write_history(stores, [
            self._failure(now - timedelta(days=3)),
            self._failure(now - timedelta(days=2)),
            self._failure(now - timedelta(days=1)),
        ])
        records = {}
        stuck._collect_history(records)
        assert len(records) == 1
        rec = next(iter(records.values()))
        assert rec['key'].startswith('title:')
        assert 'failure_streak' in rec['reasons']
        assert rec['attempts'] == 3
        assert rec['last_event']['cause'] == 'uncached_timeout'

    def test_progress_event_resets_streak(self, stores):
        now = datetime.now(timezone.utc)
        _write_history(stores, [
            self._failure(now - timedelta(days=3)),
            self._failure(now - timedelta(days=2)),
            self._failure(now - timedelta(days=2, hours=-1)),
            {'ts': _iso(now - timedelta(days=1)), 'type': 'grabbed',
             'title': 'Some Movie',
             'meta': {'cause': 'blackhole_grab_submitted'}},
        ])
        records = {}
        stuck._collect_history(records)
        assert records == {}

    def test_young_streak_excluded(self, stores):
        now = datetime.now(timezone.utc)
        _write_history(stores, [
            self._failure(now - timedelta(hours=3)),
            self._failure(now - timedelta(hours=2)),
            self._failure(now - timedelta(hours=1)),
        ])
        records = {}
        stuck._collect_history(records)
        assert records == {}

    def test_short_streak_excluded(self, stores):
        now = datetime.now(timezone.utc)
        _write_history(stores, [
            self._failure(now - timedelta(days=3)),
            self._failure(now - timedelta(days=2)),
        ])
        records = {}
        stuck._collect_history(records)
        assert records == {}

    def test_unrelated_causes_ignored(self, stores):
        now = datetime.now(timezone.utc)
        _write_history(stores, [
            {'ts': _iso(now - timedelta(days=d)), 'type': 'task_completed',
             'title': 'Some Movie', 'meta': {'cause': 'not_a_real_cause'}}
            for d in (3, 2, 1)
        ])
        records = {}
        stuck._collect_history(records)
        assert records == {}


# ---------------------------------------------------------------------------
# Wanted-ghost collector
# ---------------------------------------------------------------------------

class TestCollectWanted:

    def _scanner(self, monkeypatch, **kwargs):
        sc = _FakeScanner(**kwargs)
        import utils.library
        monkeypatch.setattr(utils.library, 'get_scanner', lambda: sc)
        return sc

    def test_memo_hit_surfaces_ghost(self, stores, monkeypatch):
        self._scanner(monkeypatch, data={
            'movies': [{'title': 'Old Film', 'source': 'wanted',
                        'imdb_id': 'tt1234567'}],
            'shows': [],
        }, memos={'no_results': {}, 'rd_miss': {'tt1234567': 3600.0},
                  'tb_cooldown': {}})
        records = {}
        stuck._collect_wanted(records, attempt_ledger.snapshot())
        assert len(records) == 1
        rec = next(iter(records.values()))
        assert rec['key'] == 'title:old film'
        assert 'rd_miss' in rec['reasons']
        assert rec['imdb_id'] == 'tt1234567'
        assert rec['media_type'] == 'movie'
        # since ≈ now - memo age
        since = datetime.fromisoformat(rec['since'])
        age = (datetime.now(timezone.utc) - since).total_seconds()
        assert 3500 < age < 3700

    def test_grab_cap_surfaces_ghost(self, stores, monkeypatch):
        self._scanner(monkeypatch, data={
            'movies': [{'title': 'Old Film', 'source': 'wanted',
                        'imdb_id': 'tt1234567'}],
            'shows': [],
        })
        for _ in range(12):
            attempt_ledger.bump('fg:old film')
        records = {}
        stuck._collect_wanted(records, attempt_ledger.snapshot())
        rec = next(iter(records.values()))
        assert 'grab_capped' in rec['reasons']
        assert rec['attempts'] == 12

    def test_tbalt_cap_surfaces_show(self, stores, monkeypatch):
        self._scanner(monkeypatch, data={
            'movies': [],
            'shows': [{'title': 'Old Show', 'source': 'wanted',
                       'imdb_id': 'tt7654321'}],
        })
        for _ in range(12):
            attempt_ledger.bump('tbalt:tt7654321:s3')
        records = {}
        stuck._collect_wanted(records, attempt_ledger.snapshot())
        rec = next(iter(records.values()))
        assert 'alt_capped' in rec['reasons']
        assert rec['media_type'] == 'show'

    def test_ghost_without_signals_skipped(self, stores, monkeypatch):
        self._scanner(monkeypatch, data={
            'movies': [{'title': 'Merely Wanted', 'source': 'wanted',
                        'imdb_id': 'tt1111111'}],
            'shows': [],
        })
        records = {}
        stuck._collect_wanted(records, attempt_ledger.snapshot())
        assert records == {}

    def test_non_wanted_items_skipped(self, stores, monkeypatch):
        self._scanner(monkeypatch, data={
            'movies': [{'title': 'On Disk', 'source': 'debrid',
                        'imdb_id': 'tt2222222'}],
            'shows': [],
        }, memos={'no_results': {}, 'rd_miss': {'tt2222222': 3600.0},
                  'tb_cooldown': {}})
        records = {}
        stuck._collect_wanted(records, attempt_ledger.snapshot())
        assert records == {}

    def test_no_scanner_is_quiet(self, stores):
        records = {}
        stuck._collect_wanted(records, {})
        assert records == {}


# ---------------------------------------------------------------------------
# Dismissal + retry-state clearing
# ---------------------------------------------------------------------------

class TestDismissAndClear:

    def _seed_streak(self, config_dir, title='Some Movie'):
        now = datetime.now(timezone.utc)
        _write_history(config_dir, [
            {'ts': _iso(now - timedelta(days=d)), 'type': 'failed',
             'title': title, 'meta': {'cause': 'uncached_timeout'}}
            for d in (3, 2, 1)
        ])

    def test_dismiss_hides_item_until_ttl(self, stores):
        self._seed_streak(stores)
        payload = stuck.collect(force=True)
        assert payload['total'] == 1
        key = payload['items'][0]['key']

        stuck.dismiss(key)
        payload = stuck.collect(force=True)
        assert payload['total'] == 0
        assert payload['dismissed'] == 1

    def test_expired_dismissal_resurfaces(self, stores):
        self._seed_streak(stores)
        payload = stuck.collect(force=True)
        key = payload['items'][0]['key']
        stuck.dismiss(key)

        # Age the dismissal past its TTL by rewriting its timestamps.
        old = _iso(datetime.now(timezone.utc)
                   - timedelta(seconds=stuck.DISMISS_TTL_SECONDS + 60))
        entry = attempt_ledger._state[stuck._DISMISS_PREFIX + key]
        entry['first_ts'] = entry['last_ts'] = old

        payload = stuck.collect(force=True)
        assert payload['total'] == 1
        assert payload['dismissed'] == 0

    def test_clear_retry_state_resets_ledger_memos_and_dismissal(
            self, stores, monkeypatch):
        sc = _FakeScanner()
        import utils.library
        monkeypatch.setattr(utils.library, 'get_scanner', lambda: sc)

        attempt_ledger.bump('fg:old film')
        attempt_ledger.bump('fg:old film:s1')
        attempt_ledger.bump('tbalt:tt1234567:s3')
        attempt_ledger.bump('fg:other title')          # must survive
        attempt_ledger.bump('stuckdismiss:title:old film')

        cleared = stuck.clear_retry_state('title:old film', title='Old Film',
                                          imdb_id='tt1234567')
        assert cleared == {'memos': 2, 'ledger_keys': 3}
        assert sc.cleared == ['tt1234567']
        assert attempt_ledger.get('fg:old film') == 0
        assert attempt_ledger.get('fg:old film:s1') == 0
        assert attempt_ledger.get('tbalt:tt1234567:s3') == 0
        assert attempt_ledger.get('fg:other title') == 1
        assert attempt_ledger.get('stuckdismiss:title:old film') == 0

    def test_clear_retry_state_without_title_or_imdb(self, stores):
        attempt_ledger.bump('stuckdismiss:inflight:x')
        cleared = stuck.clear_retry_state('inflight:x')
        assert cleared == {'memos': 0, 'ledger_keys': 0}
        assert attempt_ledger.get('stuckdismiss:inflight:x') == 0


# ---------------------------------------------------------------------------
# collect() — join, cache, payload shape
# ---------------------------------------------------------------------------

class TestCollect:

    def test_payload_shape_and_labels(self, stores):
        now = datetime.now(timezone.utc)
        _write_history(stores, [
            {'ts': _iso(now - timedelta(days=d)), 'type': 'failed',
             'title': 'Some Movie', 'meta': {'cause': 'uncached_timeout'}}
            for d in (3, 2, 1)
        ])
        payload = stuck.collect(force=True)
        assert set(payload) == {'generated_at', 'total', 'dismissed', 'items'}
        item = payload['items'][0]
        assert item['reason_labels'] == [
            stuck.REASON_LABELS['failure_streak']]
        assert payload['total'] == 1

    def test_cache_returns_same_payload_until_forced(self, stores):
        first = stuck.collect(force=True)
        assert stuck.collect() is first
        assert stuck.collect(force=True) is not first

    def test_collector_failure_does_not_break_collect(self, stores,
                                                      monkeypatch):
        monkeypatch.setattr(stuck, '_collect_history',
                            lambda records: 1 / 0)
        payload = stuck.collect(force=True)
        assert payload['items'] == []

    def test_sorted_by_attempts_then_age(self, stores, monkeypatch):
        sc = _FakeScanner(data={
            'movies': [
                {'title': 'Few Tries', 'source': 'wanted',
                 'imdb_id': 'tt1111111'},
                {'title': 'Many Tries', 'source': 'wanted',
                 'imdb_id': 'tt2222222'},
            ],
            'shows': [],
        }, memos={'no_results': {'tt1111111': 60.0, 'tt2222222': 60.0},
                  'rd_miss': {}, 'tb_cooldown': {}})
        import utils.library
        monkeypatch.setattr(utils.library, 'get_scanner', lambda: sc)
        attempt_ledger.bump('fg:few tries')
        for _ in range(5):
            attempt_ledger.bump('fg:many tries')
        payload = stuck.collect(force=True)
        assert [i['title'] for i in payload['items']] == \
            ['Many Tries', 'Few Tries']


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------

def test_every_emitted_reason_has_a_label():
    """Every reason slug the module can attach must have a UI label."""
    src = open(os.path.join(os.path.dirname(stuck.__file__),
                            'stuck.py'), encoding='utf-8').read()
    emitted = set(re.findall(r"_add_reason\(\w+, '([a-z_]+)'\)", src))
    emitted |= set(re.findall(r"reasons\.append\('([a-z_]+)'\)", src))
    assert emitted, 'source scan found no reason slugs — regex rotted?'
    missing = emitted - set(stuck.REASON_LABELS)
    assert not missing, f'reasons without labels: {missing}'


def test_failure_and_progress_causes_exist_in_vocabulary():
    """Cause slugs referenced here must exist in the history vocabulary."""
    causes = {getattr(history, n) for n in dir(history)
              if n.startswith('CAUSE_')}
    assert causes, 'history CAUSE_* vocabulary not found'
    missing = (stuck._FAILURE_CAUSES | stuck._PROGRESS_CAUSES) - causes
    assert not missing, f'stuck.py references unknown causes: {missing}'
