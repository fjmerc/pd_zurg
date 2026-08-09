"""Integration tests for BlackholeWatcher multi-debrid routing (plan 39 phase 2).

These tests cover the watcher-level wiring: per-grab routing via
``_route_grab``, the ``debrid`` field on ``pending_monitors.json``
entries, and resume-after-restart honouring the recorded debrid.
The pure-function routing helpers are covered separately in
``tests/test_debrid_routing.py``.
"""

import json
import os
import threading
from unittest.mock import patch, MagicMock

import pytest


@pytest.fixture
def env(monkeypatch):
    for key in ('RD_API_KEY', 'AD_API_KEY', 'TORBOX_API_KEY',
                'BLACKHOLE_DEBRID', 'BLACKHOLE_DEBRID_PRIMARY',
                'BLACKHOLE_DEBRID_ROUTING'):
        monkeypatch.delenv(key, raising=False)

    class _Env:
        def set(self, **kw):
            for k, v in kw.items():
                monkeypatch.setenv(k, v)
    return _Env()


@pytest.fixture
def watcher(tmp_dir):
    """A BlackholeWatcher with both RD + TB keys configured."""
    from utils.blackhole import BlackholeWatcher
    w = BlackholeWatcher(
        watch_dir=tmp_dir,
        debrid_api_key='rd-key',
        debrid_service='realdebrid',
        symlink_enabled=False,
        completed_dir=tmp_dir,
        debrid_api_keys={'realdebrid': 'rd-key', 'torbox': 'tb-key'},
    )
    return w


class TestApiKeyResolution:
    def test_per_debrid_lookup(self, watcher):
        assert watcher._api_key_for('realdebrid') == 'rd-key'
        assert watcher._api_key_for('torbox') == 'tb-key'

    def test_unknown_debrid_returns_none(self, watcher):
        assert watcher._api_key_for('debridlink') is None

    def test_legacy_fallback_for_missing_dict(self, tmp_dir):
        """When ``debrid_api_keys`` is unset (legacy path / __new__ test
        fixtures), single-debrid lookups still resolve."""
        from utils.blackhole import BlackholeWatcher
        w = BlackholeWatcher.__new__(BlackholeWatcher)
        w.debrid_service = 'realdebrid'
        w.debrid_api_key = 'legacy'
        # Note: no debrid_api_keys attribute at all
        assert w._api_key_for('realdebrid') == 'legacy'
        assert w._api_key_for('torbox') is None


class TestRouteGrab:
    def test_routes_to_cached_provider(self, watcher, env, monkeypatch):
        env.set(RD_API_KEY='rd-key', TORBOX_API_KEY='tb-key')

        # Stub the cache check so TB is the cached side.
        def fake_check(hashes, service=None, api_key=None):
            return {h: (service == 'torbox') for h in hashes}
        monkeypatch.setattr('utils.search.check_debrid_cache', fake_check)

        assert watcher._route_grab('a' * 40) == 'torbox'

    def test_falls_back_to_primary_when_neither_cached(self, watcher, env, monkeypatch):
        env.set(RD_API_KEY='rd-key', TORBOX_API_KEY='tb-key')

        monkeypatch.setattr(
            'utils.search.check_debrid_cache',
            lambda h, **kw: {x: False for x in h},
        )
        assert watcher._route_grab('a' * 40) == 'realdebrid'

    def test_primary_only_mode_skips_cache(self, watcher, env, monkeypatch):
        env.set(
            RD_API_KEY='rd-key', TORBOX_API_KEY='tb-key',
            BLACKHOLE_DEBRID_ROUTING='primary_only',
        )

        # Even if TB is cached, primary_only never probes.
        calls = []

        def fake_check(hashes, service=None, api_key=None):
            calls.append(service)
            return {h: (service == 'torbox') for h in hashes}
        monkeypatch.setattr('utils.search.check_debrid_cache', fake_check)

        assert watcher._route_grab('a' * 40) == 'realdebrid'
        assert calls == []  # Probe never called under primary_only

    def test_no_hash_routes_to_primary(self, watcher, env, monkeypatch):
        env.set(RD_API_KEY='rd-key', TORBOX_API_KEY='tb-key')
        calls = []

        def fake_check(hashes, **kw):
            calls.append(hashes)
            return {}
        monkeypatch.setattr('utils.search.check_debrid_cache', fake_check)

        # Empty hash bypasses the probe entirely
        assert watcher._route_grab('') == 'realdebrid'
        assert calls == []

    def test_probe_failure_falls_back_to_primary(self, watcher, env, monkeypatch):
        env.set(RD_API_KEY='rd-key', TORBOX_API_KEY='tb-key')

        def fake_check(hashes, **kw):
            raise ConnectionError('debrid API down')
        monkeypatch.setattr('utils.search.check_debrid_cache', fake_check)

        # Both probes raise → both treated as unknown → primary.
        # Critically: the routing call must NOT propagate the exception.
        assert watcher._route_grab('a' * 40) == 'realdebrid'


class TestPendingMonitorsDebridField:
    def test_add_pending_writes_debrid_field(self, watcher):
        watcher._add_pending('tid1', 'release.mkv', debrid='torbox')
        with open(watcher._pending_file) as f:
            entries = json.load(f)
        assert len(entries) == 1
        assert entries[0]['debrid'] == 'torbox'
        # And mirrors into the legacy 'service' field for back-compat
        # readers (e.g. the dashboard pending-monitor view).
        assert entries[0]['service'] == 'torbox'

    def test_add_pending_defaults_to_instance_debrid(self, watcher):
        watcher._add_pending('tid2', 'release.mkv')
        with open(watcher._pending_file) as f:
            entries = json.load(f)
        assert entries[0]['debrid'] == 'realdebrid'  # instance default

    def test_legacy_entry_without_debrid_resumes_as_realdebrid(self, watcher, monkeypatch):
        """Pre-plan-39 pending entries omitted the ``debrid`` field.  The
        only possible origin back then was the instance debrid_service,
        so resume defaults missing → that instance value."""
        legacy = [
            {'torrent_id': 'tlegacy', 'filename': 'old.mkv',
             'timestamp': 0},  # no 'debrid', no 'service'
        ]
        with open(watcher._pending_file, 'w') as f:
            json.dump(legacy, f)

        captured = []
        monkeypatch.setattr(
            watcher, '_start_monitor',
            lambda tid, fn, label=None, debrid=None: captured.append((tid, debrid)),
        )
        watcher._resume_pending_monitors()
        assert captured == [('tlegacy', 'realdebrid')]

    def test_torbox_entry_resumes_as_torbox(self, watcher, monkeypatch):
        entries = [
            {'torrent_id': 'tb1', 'filename': 'tb.mkv',
             'debrid': 'torbox', 'service': 'torbox', 'timestamp': 0},
        ]
        with open(watcher._pending_file, 'w') as f:
            json.dump(entries, f)

        captured = []
        monkeypatch.setattr(
            watcher, '_start_monitor',
            lambda tid, fn, label=None, debrid=None: captured.append((tid, debrid)),
        )
        watcher._resume_pending_monitors()
        assert captured == [('tb1', 'torbox')]

    def test_invalid_debrid_drops_to_instance_default(self, watcher, monkeypatch):
        """A tampered ``debrid`` value (e.g. malicious or stale) must not
        steer API calls to an unknown provider — fall back to the
        instance default for safety."""
        entries = [
            {'torrent_id': 't_bad', 'filename': 'x.mkv',
             'debrid': 'nonexistent_provider', 'timestamp': 0},
        ]
        with open(watcher._pending_file, 'w') as f:
            json.dump(entries, f)

        captured = []
        monkeypatch.setattr(
            watcher, '_start_monitor',
            lambda tid, fn, label=None, debrid=None: captured.append((tid, debrid)),
        )
        watcher._resume_pending_monitors()
        assert captured == [('t_bad', 'realdebrid')]  # sanitized to default


class TestPerDebridMountResolution:
    def test_mount_for_rd_default(self, watcher, env):
        env.set(RD_API_KEY='rd-key', RCLONE_MOUNT_NAME='zurgarr')
        assert watcher._mount_for('realdebrid') == '/data/zurgarr'

    def test_mount_for_torbox_uses_torbox_name(self, watcher, env):
        env.set(RD_API_KEY='rd-key', TORBOX_API_KEY='tb-key',
                RCLONE_MOUNT_NAME='zurgarr', TORBOX_MOUNT_NAME='torbox')
        assert watcher._mount_for('torbox') == '/data/torbox'

    def test_symlink_base_for_rd(self, watcher, env):
        env.set(BLACKHOLE_SYMLINK_TARGET_BASE='/mnt/debrid')
        assert watcher._symlink_target_base_for('realdebrid') == '/mnt/debrid'

    def test_symlink_base_for_torbox_uses_suffix(self, watcher, env):
        env.set(BLACKHOLE_SYMLINK_TARGET_BASE='/mnt/debrid')
        assert watcher._symlink_target_base_for('torbox') == '/mnt/debrid_torbox'

    def test_symlink_base_for_torbox_explicit_wins(self, watcher, env):
        env.set(BLACKHOLE_SYMLINK_TARGET_BASE='/mnt/debrid',
                BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX='/mnt/tb_explicit')
        assert watcher._symlink_target_base_for('torbox') == '/mnt/tb_explicit'
