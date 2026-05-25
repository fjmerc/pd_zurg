"""Tests for utils/debrid_routing.py — plan 39 phase 2 routing helpers.

Pure-function tests against the routing module.  No watcher state, no
network, no rclone — the helpers are designed to be exercised in
isolation so the routing decision is auditable independently of the
blackhole pipeline that consumes it.
"""

import pytest

from utils.debrid_routing import (
    REALDEBRID, ALLDEBRID, TORBOX,
    ROUTING_CACHE_AWARE, ROUTING_PRIMARY_ONLY,
    configured_debrids, resolve_primary, resolve_routing_mode,
    mount_for_debrid, symlink_target_base_for_debrid,
    pick_debrid_for_grab,
)


@pytest.fixture
def env(monkeypatch):
    """Convenience: clear all routing-relevant env vars then let the
    test set just what it cares about via ``env.set(KEY=val)``."""
    for key in ('RD_API_KEY', 'AD_API_KEY', 'TORBOX_API_KEY',
                'BLACKHOLE_DEBRID', 'BLACKHOLE_DEBRID_PRIMARY',
                'BLACKHOLE_DEBRID_ROUTING',
                'RCLONE_MOUNT_NAME', 'TORBOX_MOUNT_NAME',
                'BLACKHOLE_SYMLINK_TARGET_BASE',
                'BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX'):
        monkeypatch.delenv(key, raising=False)

    class _Env:
        def set(self, **kwargs):
            for k, v in kwargs.items():
                monkeypatch.setenv(k, v)
    return _Env()


# ---------------------------------------------------------------------------
# configured_debrids / resolve_primary / resolve_routing_mode
# ---------------------------------------------------------------------------

class TestConfiguredDebrids:
    def test_none_when_no_keys(self, env):
        assert configured_debrids() == ()

    def test_only_rd(self, env):
        env.set(RD_API_KEY='k')
        assert configured_debrids() == (REALDEBRID,)

    def test_rd_and_tb_in_order(self, env):
        env.set(RD_API_KEY='k1', TORBOX_API_KEY='k2')
        assert configured_debrids() == (REALDEBRID, TORBOX)

    def test_all_three(self, env):
        env.set(RD_API_KEY='k1', AD_API_KEY='k2', TORBOX_API_KEY='k3')
        assert configured_debrids() == (REALDEBRID, ALLDEBRID, TORBOX)


class TestResolvePrimary:
    def test_none_when_unconfigured(self, env):
        assert resolve_primary() is None

    def test_first_configured_when_unset(self, env):
        env.set(TORBOX_API_KEY='k')
        assert resolve_primary() == TORBOX

    def test_explicit_primary_wins(self, env):
        env.set(RD_API_KEY='k1', TORBOX_API_KEY='k2',
                BLACKHOLE_DEBRID_PRIMARY='torbox')
        assert resolve_primary() == TORBOX

    def test_explicit_primary_ignored_when_not_configured(self, env):
        # User sets PRIMARY=torbox but never set TORBOX_API_KEY — fall
        # through to the first configured.
        env.set(RD_API_KEY='k1', BLACKHOLE_DEBRID_PRIMARY='torbox')
        assert resolve_primary() == REALDEBRID

    def test_legacy_blackhole_debrid_honored(self, env):
        env.set(RD_API_KEY='k1', TORBOX_API_KEY='k2',
                BLACKHOLE_DEBRID='torbox')
        assert resolve_primary() == TORBOX

    def test_explicit_primary_beats_legacy(self, env):
        env.set(RD_API_KEY='k1', TORBOX_API_KEY='k2',
                BLACKHOLE_DEBRID='torbox',
                BLACKHOLE_DEBRID_PRIMARY='realdebrid')
        assert resolve_primary() == REALDEBRID


class TestResolveRoutingMode:
    def test_default_single_debrid_is_primary_only(self, env):
        env.set(RD_API_KEY='k')
        assert resolve_routing_mode() == ROUTING_PRIMARY_ONLY

    def test_default_two_debrids_is_cache_aware(self, env):
        env.set(RD_API_KEY='k', TORBOX_API_KEY='k')
        assert resolve_routing_mode() == ROUTING_CACHE_AWARE

    def test_explicit_override_wins(self, env):
        env.set(RD_API_KEY='k', TORBOX_API_KEY='k',
                BLACKHOLE_DEBRID_ROUTING='primary_only')
        assert resolve_routing_mode() == ROUTING_PRIMARY_ONLY

    def test_invalid_mode_ignored(self, env):
        env.set(RD_API_KEY='k', TORBOX_API_KEY='k',
                BLACKHOLE_DEBRID_ROUTING='bogus')
        assert resolve_routing_mode() == ROUTING_CACHE_AWARE


# ---------------------------------------------------------------------------
# mount_for_debrid + symlink_target_base_for_debrid
# ---------------------------------------------------------------------------

class TestMountForDebrid:
    def test_unknown_debrid_is_none(self, env):
        assert mount_for_debrid('bogus') is None

    def test_torbox_default(self, env):
        # Default TORBOX_MOUNT_NAME is 'torbox'
        assert mount_for_debrid(TORBOX) == '/data/torbox'

    def test_torbox_custom_name(self, env):
        env.set(TORBOX_MOUNT_NAME='tb')
        assert mount_for_debrid(TORBOX) == '/data/tb'

    def test_rd_single_zurg(self, env):
        env.set(RD_API_KEY='k', RCLONE_MOUNT_NAME='zurgarr')
        assert mount_for_debrid(REALDEBRID) == '/data/zurgarr'

    def test_rd_dual_zurg_uses_RD_suffix(self, env):
        env.set(RD_API_KEY='k', AD_API_KEY='k', RCLONE_MOUNT_NAME='zurgarr')
        assert mount_for_debrid(REALDEBRID) == '/data/zurgarr_RD'

    def test_ad_dual_zurg_uses_AD_suffix(self, env):
        env.set(RD_API_KEY='k', AD_API_KEY='k', RCLONE_MOUNT_NAME='zurgarr')
        assert mount_for_debrid(ALLDEBRID) == '/data/zurgarr_AD'

    def test_no_rclone_mount_name_returns_none(self, env):
        assert mount_for_debrid(REALDEBRID) is None

    def test_custom_base(self, env):
        env.set(RD_API_KEY='k', RCLONE_MOUNT_NAME='zurgarr')
        assert mount_for_debrid(REALDEBRID, rclone_mount_base='/mnt') == '/mnt/zurgarr'


class TestSymlinkTargetBaseForDebrid:
    def test_rd_returns_target_base(self, env):
        env.set(BLACKHOLE_SYMLINK_TARGET_BASE='/mnt/debrid')
        assert symlink_target_base_for_debrid(REALDEBRID) == '/mnt/debrid'

    def test_rd_empty_when_unset(self, env):
        assert symlink_target_base_for_debrid(REALDEBRID) == ''

    def test_torbox_explicit_wins(self, env):
        env.set(BLACKHOLE_SYMLINK_TARGET_BASE='/mnt/debrid',
                BLACKHOLE_SYMLINK_TARGET_BASE_TORBOX='/mnt/tb_custom')
        assert symlink_target_base_for_debrid(TORBOX) == '/mnt/tb_custom'

    def test_torbox_falls_back_to_rd_with_suffix(self, env):
        env.set(BLACKHOLE_SYMLINK_TARGET_BASE='/mnt/debrid')
        assert symlink_target_base_for_debrid(TORBOX) == '/mnt/debrid_torbox'

    def test_torbox_strips_trailing_slash_before_suffix(self, env):
        env.set(BLACKHOLE_SYMLINK_TARGET_BASE='/mnt/debrid/')
        assert symlink_target_base_for_debrid(TORBOX) == '/mnt/debrid_torbox'

    def test_torbox_empty_when_neither_set(self, env):
        assert symlink_target_base_for_debrid(TORBOX) == ''


# ---------------------------------------------------------------------------
# pick_debrid_for_grab — the headline router
# ---------------------------------------------------------------------------

class TestPickDebridForGrabSingleDebrid:
    def test_single_rd_always_returns_rd(self, env):
        env.set(RD_API_KEY='k')
        # No cache probe needed — single debrid collapses immediately.
        assert pick_debrid_for_grab('h') == REALDEBRID

    def test_single_torbox_always_returns_torbox(self, env):
        env.set(TORBOX_API_KEY='k')
        assert pick_debrid_for_grab('h') == TORBOX

    def test_unconfigured_returns_none(self, env):
        assert pick_debrid_for_grab('h') is None


class TestPickDebridForGrabPrimaryOnly:
    def test_primary_only_always_returns_primary(self, env):
        env.set(RD_API_KEY='k', TORBOX_API_KEY='k',
                BLACKHOLE_DEBRID_ROUTING='primary_only')

        def probe(svc, h):
            # Even with TB cached, primary_only ignores cache results.
            return svc == TORBOX
        assert pick_debrid_for_grab('h', cache_probe=probe) == REALDEBRID

    def test_primary_only_honours_explicit_primary(self, env):
        env.set(RD_API_KEY='k', TORBOX_API_KEY='k',
                BLACKHOLE_DEBRID_ROUTING='primary_only',
                BLACKHOLE_DEBRID_PRIMARY='torbox')
        assert pick_debrid_for_grab('h') == TORBOX


class TestPickDebridForGrabCacheAware:
    def _setup(self, env):
        env.set(RD_API_KEY='k', TORBOX_API_KEY='k')
        # Default routing is cache_aware when two debrids are configured.

    def test_tb_only_cached_routes_to_tb(self, env):
        self._setup(env)

        def probe(svc, h):
            return svc == TORBOX
        assert pick_debrid_for_grab('h', cache_probe=probe) == TORBOX

    def test_rd_only_cached_routes_to_rd(self, env):
        self._setup(env)

        def probe(svc, h):
            return svc == REALDEBRID
        assert pick_debrid_for_grab('h', cache_probe=probe) == REALDEBRID

    def test_both_cached_routes_to_primary(self, env):
        self._setup(env)

        def probe(svc, h):
            return True
        assert pick_debrid_for_grab('h', cache_probe=probe) == REALDEBRID

    def test_neither_cached_routes_to_primary(self, env):
        self._setup(env)

        def probe(svc, h):
            return False
        assert pick_debrid_for_grab('h', cache_probe=probe) == REALDEBRID

    def test_unknown_status_routes_to_primary(self, env):
        """Probe returning None for both providers (API unavailable) must
        not crash; degrades to primary like neither-cached."""
        self._setup(env)

        def probe(svc, h):
            return None
        assert pick_debrid_for_grab('h', cache_probe=probe) == REALDEBRID

    def test_probe_exception_treated_as_unknown(self, env):
        self._setup(env)

        def probe(svc, h):
            raise ConnectionError('TB down')
        # Even if TB probe raises, RD probe (also raises) → both unknown
        # → primary.  Critical: must not propagate the exception.
        assert pick_debrid_for_grab('h', cache_probe=probe) == REALDEBRID

    def test_no_probe_falls_back_to_primary(self, env):
        """Cache-aware without a probe callable → primary (safe default)."""
        self._setup(env)
        assert pick_debrid_for_grab('h', cache_probe=None) == REALDEBRID

    def test_empty_hash_falls_back_to_primary(self, env):
        """A drop with no extractable info-hash can't be cache-probed."""
        self._setup(env)
        calls = []

        def probe(svc, h):
            calls.append((svc, h))
            return True
        # Empty hash → skip probe entirely
        assert pick_debrid_for_grab('', cache_probe=probe) == REALDEBRID
        assert calls == []

    def test_explicit_primary_torbox_wins_when_both_cached(self, env):
        env.set(RD_API_KEY='k', TORBOX_API_KEY='k',
                BLACKHOLE_DEBRID_PRIMARY='torbox')

        def probe(svc, h):
            return True
        assert pick_debrid_for_grab('h', cache_probe=probe) == TORBOX
