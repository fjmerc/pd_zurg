"""Tests for utils/recovery.py — media recovery snapshot tracking."""

import json
import os
from datetime import datetime, timezone

import pytest

from utils import recovery


@pytest.fixture
def rec(tmp_dir, monkeypatch):
    """Initialize the recovery module pointed at a temp config dir."""
    # No RD configured by default → filter_gate resolves to None unless the
    # test overrides it; keep the env clean so get_summary short-circuits.
    monkeypatch.delenv('RD_API_KEY', raising=False)
    monkeypatch.delenv('RECOVERY_SNAPSHOT_RETENTION_DAYS', raising=False)
    recovery.init(config_dir=tmp_dir)
    return recovery


def _sample_data():
    """A mixed library: debrid + local movies, mixed-source episodes, wanted."""
    return {
        'movies': [
            {'title': 'A', 'source': 'debrid', 'size_bytes': 100},
            {'title': 'B', 'source': 'both', 'size_bytes': 200},
            {'title': 'C', 'source': 'local', 'size_bytes': 50},
            {'title': 'D', 'source': 'wanted', 'missing': True},
        ],
        'shows': [
            {
                'title': 'Show1',
                'source': 'debrid',
                'missing_episodes': 2,
                'season_data': [
                    {'episodes': [
                        {'source': 'debrid', 'size_bytes': 10},
                        {'source': 'both', 'size_bytes': 10},
                        {'source': 'local', 'size_bytes': 10},
                    ]},
                ],
            },
        ],
    }


def test_compute_snapshot_basic(rec):
    now = datetime(2026, 5, 31, 12, 0, 0, tzinfo=timezone.utc)
    snap = rec.compute_snapshot(_sample_data(), now=now)

    assert snap['date'] == '2026-05-31'
    # movies: debrid(A) + both(B) = 2 on debrid; local(C) = 1; wanted(D) excluded
    assert snap['units']['movies'] == {'total': 3, 'debrid': 1, 'local': 1, 'both': 1}
    # episodes: 1 debrid + 1 both + 1 local
    assert snap['units']['episodes'] == {'total': 3, 'debrid': 1, 'local': 1, 'both': 1}

    r = snap['recovery']
    # available_debrid = movies(debrid+both)=2 + episodes(debrid+both)=2 = 4
    assert r['available_debrid'] == 4
    # available_local = movie local 1 + episode local 1 = 2
    assert r['available_local'] == 2
    assert r['on_disk'] == 6
    # wanted = 1 missing movie + 2 missing episodes
    assert r['wanted'] == 3
    assert r['total'] == 9
    assert r['pct_debrid'] == pytest.approx(44.4, abs=0.1)
    assert r['pct_on_disk'] == pytest.approx(66.7, abs=0.1)


def test_unavailable_wanted_movie_excluded_from_denominator(rec):
    """A monitored-but-unreleased movie (Radarr is_available=False) must not
    dilute the recovery denominator; an available wanted movie still counts."""
    data = {
        'movies': [
            {'title': 'OnDebrid', 'source': 'debrid', 'size_bytes': 100},
            {'title': 'Released', 'source': 'wanted', 'missing': True,
             'is_available': True},
            {'title': 'NotOutYet', 'source': 'wanted', 'missing': True,
             'is_available': False},
            # No flag at all → counts as before (legacy / non-Radarr).
            {'title': 'Unknown', 'source': 'wanted', 'missing': True},
        ],
        'shows': [],
    }
    r = rec.compute_snapshot(data)['recovery']
    # available_debrid = 1; wanted = Released + Unknown = 2 (NotOutYet excluded)
    assert r['available_debrid'] == 1
    assert r['wanted'] == 2
    assert r['total'] == 3
    assert r['pct_debrid'] == pytest.approx(33.3, abs=0.1)


def test_empty_library_no_divide_by_zero(rec):
    snap = rec.compute_snapshot({'movies': [], 'shows': []})
    r = snap['recovery']
    assert r['total'] == 0
    assert r['pct_debrid'] == 0.0
    assert r['pct_on_disk'] == 0.0


def test_filter_gate_none_without_rd(rec):
    snap = rec.compute_snapshot(_sample_data())
    assert snap['filter_gate'] is None


def test_filter_gate_populated(rec, monkeypatch):
    monkeypatch.setattr(
        'utils.debrid_health.get_summary',
        lambda: {
            'rd_configured': True,
            'counts': {'blocked': 7, 'healthy': 100, 'unknown': 3, 'total': 110},
            'remediated_24h': 4,
            'rescued_24h': 2,
        },
    )
    snap = rec.compute_snapshot(_sample_data())
    assert snap['filter_gate'] == {'blocked': 7, 'filtered_24h': 4, 'rescued_24h': 2}


def test_filter_gate_none_on_summary_error(rec, monkeypatch):
    def _boom():
        raise RuntimeError('schema drift')
    monkeypatch.setattr('utils.debrid_health.get_summary', _boom)
    # Snapshot still records; filter_gate degrades to None rather than crashing.
    snap = rec.compute_snapshot(_sample_data())
    assert snap['filter_gate'] is None
    assert snap['recovery']['total'] == 9


def test_filter_gate_none_on_non_dict_summary(rec, monkeypatch):
    monkeypatch.setattr('utils.debrid_health.get_summary', lambda: None)
    snap = rec.compute_snapshot(_sample_data())
    assert snap['filter_gate'] is None


def test_record_persists_and_loads(rec):
    now = datetime(2026, 5, 31, 12, 0, 0, tzinfo=timezone.utc)
    rec.record_snapshot(_sample_data(), now=now)

    snaps = rec.load_snapshots()
    assert len(snaps) == 1
    assert snaps[0]['date'] == '2026-05-31'
    # File is the versioned envelope, not a bare list.
    with open(rec._file_path) as f:
        payload = json.load(f)
    assert payload['version'] == recovery.SCHEMA_VERSION
    assert isinstance(payload['snapshots'], list)


def test_upsert_by_day_replaces(rec):
    day = datetime(2026, 5, 31, 8, 0, 0, tzinfo=timezone.utc)
    rec.record_snapshot(_sample_data(), now=day)

    # Second scan same day with a different library → overwrite, not append.
    later = datetime(2026, 5, 31, 20, 0, 0, tzinfo=timezone.utc)
    rec.record_snapshot({'movies': [{'title': 'X', 'source': 'debrid'}], 'shows': []}, now=later)

    snaps = rec.load_snapshots()
    assert len(snaps) == 1
    assert snaps[0]['date'] == '2026-05-31'
    assert snaps[0]['ts'] == '2026-05-31T20:00:00+00:00'
    assert snaps[0]['recovery']['available_debrid'] == 1


def test_distinct_days_append(rec):
    for d in (29, 30, 31):
        rec.record_snapshot(_sample_data(),
                            now=datetime(2026, 5, d, 12, 0, 0, tzinfo=timezone.utc))
    snaps = rec.load_snapshots()
    assert [s['date'] for s in snaps] == ['2026-05-29', '2026-05-30', '2026-05-31']


def test_retention_cap(tmp_dir, monkeypatch):
    monkeypatch.setenv('RECOVERY_SNAPSHOT_RETENTION_DAYS', '3')
    monkeypatch.delenv('RD_API_KEY', raising=False)
    recovery.init(config_dir=tmp_dir)
    for d in range(1, 8):  # 7 distinct days
        recovery.record_snapshot(_sample_data(),
                                 now=datetime(2026, 5, d, 12, 0, 0, tzinfo=timezone.utc))
    snaps = recovery.load_snapshots()
    assert len(snaps) == 3
    # Oldest dropped, newest kept.
    assert [s['date'] for s in snaps] == ['2026-05-05', '2026-05-06', '2026-05-07']


def test_load_limit(rec):
    for d in (29, 30, 31):
        rec.record_snapshot(_sample_data(),
                            now=datetime(2026, 5, d, 12, 0, 0, tzinfo=timezone.utc))
    assert [s['date'] for s in rec.load_snapshots(limit=2)] == ['2026-05-30', '2026-05-31']


def test_get_latest_empty(rec):
    assert rec.get_latest() is None


def test_get_latest_returns_newest(rec):
    for d in (29, 31, 30):  # out of order
        rec.record_snapshot(_sample_data(),
                            now=datetime(2026, 5, d, 12, 0, 0, tzinfo=timezone.utc))
    assert rec.get_latest()['date'] == '2026-05-31'


def test_corrupt_file_tolerated(rec):
    with open(rec._file_path, 'w') as f:
        f.write('{not valid json')
    # Should not raise; treats as empty and overwrites cleanly.
    snap = rec.record_snapshot(_sample_data(),
                               now=datetime(2026, 5, 31, tzinfo=timezone.utc))
    assert snap is not None
    assert len(rec.load_snapshots()) == 1
