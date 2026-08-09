"""Tests for utils/attempt_ledger.py — persistent grab-attempt counter."""

import json
import os
import importlib
from datetime import datetime, timezone, timedelta

import pytest

from utils import attempt_ledger


@pytest.fixture
def ledger(tmp_dir):
    """Fresh attempt_ledger pointed at a temp config dir.

    The module holds process-global state, so reload it between tests to
    isolate the in-memory map and file path.
    """
    importlib.reload(attempt_ledger)
    attempt_ledger.init(config_dir=tmp_dir)
    return attempt_ledger


def test_uninitialized_is_safe():
    """Before init(), get→0, bump→0, reset/prune are no-ops (no crash)."""
    importlib.reload(attempt_ledger)
    assert attempt_ledger.get('fg:x') == 0
    assert attempt_ledger.bump('fg:x') == 0
    attempt_ledger.reset('fg:x')          # no-op, must not raise
    attempt_ledger.prune(3600)            # no-op, must not raise
    assert attempt_ledger.size() == 0


def test_bump_increments_and_persists(ledger, tmp_dir):
    assert ledger.bump('fg:show:s1') == 1
    assert ledger.bump('fg:show:s1') == 2
    assert ledger.get('fg:show:s1') == 2

    # Persisted to disk
    path = os.path.join(tmp_dir, 'grab_attempts.json')
    assert os.path.isfile(path)
    with open(path) as f:
        payload = json.load(f)
    assert payload['entries']['fg:show:s1']['count'] == 2


def test_independent_keys(ledger):
    ledger.bump('fg:a')
    ledger.bump('tbalt:tt1:s2')
    ledger.bump('tbalt:tt1:s2')
    assert ledger.get('fg:a') == 1
    assert ledger.get('tbalt:tt1:s2') == 2
    assert ledger.size() == 2


def test_reset_drops_key(ledger):
    ledger.bump('fg:a')
    ledger.reset('fg:a')
    assert ledger.get('fg:a') == 0
    ledger.reset('fg:missing')  # safe no-op


def test_survives_reinit(ledger, tmp_dir):
    """State written by one process is reloaded by the next (restart)."""
    ledger.bump('fg:keep')
    ledger.bump('fg:keep')
    importlib.reload(attempt_ledger)
    attempt_ledger.init(config_dir=tmp_dir)
    assert attempt_ledger.get('fg:keep') == 2


def test_prune_drops_stale_entries(ledger, tmp_dir):
    ledger.bump('fg:old')
    ledger.bump('fg:new')

    # Backdate 'fg:old' past the prune window by rewriting its last_ts.
    path = os.path.join(tmp_dir, 'grab_attempts.json')
    with open(path) as f:
        payload = json.load(f)
    old_ts = (datetime.now(timezone.utc) - timedelta(days=40)).isoformat(timespec='seconds')
    payload['entries']['fg:old']['last_ts'] = old_ts
    with open(path, 'w') as f:
        json.dump(payload, f)
    # Reload so the module picks up the edited timestamp.
    importlib.reload(attempt_ledger)
    attempt_ledger.init(config_dir=tmp_dir)

    attempt_ledger.prune(30 * 24 * 3600)
    assert attempt_ledger.get('fg:old') == 0
    assert attempt_ledger.get('fg:new') == 1


def test_corrupt_file_starts_fresh(tmp_dir):
    path = os.path.join(tmp_dir, 'grab_attempts.json')
    with open(path, 'w') as f:
        f.write('{not valid json')
    importlib.reload(attempt_ledger)
    attempt_ledger.init(config_dir=tmp_dir)
    assert attempt_ledger.size() == 0
    assert attempt_ledger.bump('fg:a') == 1


def test_reset_all(ledger):
    ledger.bump('fg:a')
    ledger.bump('fg:b')
    ledger.reset_all()
    assert ledger.size() == 0


def test_restore_bytes_replaces_disk_and_memory(ledger, tmp_dir):
    """Backup restore: file AND in-memory state swap atomically."""
    ledger.bump('fg:old')
    payload = json.dumps({'version': 1, 'entries': {
        'fg:restored': {'count': 5, 'first_ts': '2026-01-01T00:00:00+00:00'},
    }}).encode()

    ledger.restore_bytes(payload)

    assert ledger.get('fg:old') == 0
    assert ledger.get('fg:restored') == 5
    path = os.path.join(tmp_dir, 'grab_attempts.json')
    with open(path, 'rb') as f:
        assert f.read() == payload
