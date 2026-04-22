"""Tests for the debrid-account dedup + require-cached gates on the
blackhole ``_process_file`` path.  These gates short-circuit before
the provider add-handler so the user does not get duplicate entries
or uncached junk landing in their debrid account."""

import os
import threading
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def handler(tmp_dir):
    """Build a BlackholeWatcher bypassing __init__ so we isolate the
    gates from the rest of the watcher state."""
    from utils.blackhole import BlackholeWatcher
    h = BlackholeWatcher.__new__(BlackholeWatcher)
    h.debrid_service = 'realdebrid'
    h.debrid_api_key = 'test_key'
    h.symlink_enabled = False
    h.symlink_target_base = ''
    h.rclone_mount = ''
    h._audit_retrigger = {}
    h._audit_retrigger_lock = threading.Lock()
    h._AUDIT_RETRIGGER_COOLDOWN = 7200
    h._AUDIT_RETRIGGER_MAX_PER_WINDOW = 3
    # Stub the handler methods so no real HTTP is attempted — tests
    # that need "success" patch these.
    h._add_to_realdebrid = MagicMock(return_value=(True, 'TID'))
    h._add_to_alldebrid = MagicMock(return_value=(True, {}))
    h._add_to_torbox = MagicMock(return_value=(True, {}))
    h._check_local_library = MagicMock(return_value=False)
    h._extract_torrent_id = MagicMock(return_value='TID')
    h._start_monitor = MagicMock()
    return h


@pytest.fixture
def magnet_file(tmp_dir):
    """Write a .magnet file with a well-formed btih hash and return
    (path, lowercase_hash)."""
    h = 'a' * 40
    path = os.path.join(tmp_dir, 'release.magnet')
    with open(path, 'w') as f:
        f.write(f'magnet:?xt=urn:btih:{h.upper()}&dn=release')
    return path, h


@pytest.fixture(autouse=True)
def _clean_env_and_cache(monkeypatch):
    """Each test starts at the documented defaults; clear the dedup cache
    between tests so earlier priming cannot leak."""
    monkeypatch.delenv('BLACKHOLE_DEBRID_DEDUP_ENABLED', raising=False)
    monkeypatch.delenv('BLACKHOLE_REQUIRE_CACHED', raising=False)
    from utils.search import invalidate_existing_hashes_cache
    invalidate_existing_hashes_cache()
    yield
    invalidate_existing_hashes_cache()


class TestBlackholeDebridDedup:

    def test_duplicate_hash_blocks_submit(self, handler, magnet_file):
        path, h = magnet_file
        with patch('utils.search._existing_hashes', return_value={h}):
            handler._process_file(path)
        handler._add_to_realdebrid.assert_not_called()
        assert not os.path.exists(path), 'duplicate magnet should be removed'

    def test_new_hash_proceeds(self, handler, magnet_file):
        path, h = magnet_file
        with patch('utils.search._existing_hashes', return_value={'b' * 40}):
            handler._process_file(path)
        handler._add_to_realdebrid.assert_called_once()

    def test_dedup_disabled_skips_probe(self, handler, magnet_file, monkeypatch):
        path, _ = magnet_file
        monkeypatch.setenv('BLACKHOLE_DEBRID_DEDUP_ENABLED', 'false')
        with patch('utils.search._existing_hashes') as mock_existing:
            handler._process_file(path)
        mock_existing.assert_not_called()
        handler._add_to_realdebrid.assert_called_once()

    def test_unknown_account_state_does_not_block(self, handler, magnet_file):
        """``_existing_hashes`` returning None means 'API unavailable' — we
        must defer to the add, otherwise a transient outage blocks all adds."""
        path, _ = magnet_file
        with patch('utils.search._existing_hashes', return_value=None):
            handler._process_file(path)
        handler._add_to_realdebrid.assert_called_once()


class TestBlackholeRequireCached:

    def test_uncached_blocked_when_gate_on(self, handler, magnet_file, monkeypatch):
        path, h = magnet_file
        monkeypatch.setenv('BLACKHOLE_REQUIRE_CACHED', 'true')
        with patch('utils.search._existing_hashes', return_value=set()), \
             patch('utils.search.check_debrid_cache',
                   return_value={h: False}):
            handler._process_file(path)
        handler._add_to_realdebrid.assert_not_called()
        assert not os.path.exists(path)

    def test_unknown_blocked_when_gate_on(self, handler, magnet_file, monkeypatch):
        """RD's cache probe returns None for every hash — strict mode must
        treat that as 'not cached' so uncached RD grabs never sneak through."""
        path, h = magnet_file
        monkeypatch.setenv('BLACKHOLE_REQUIRE_CACHED', 'true')
        with patch('utils.search._existing_hashes', return_value=set()), \
             patch('utils.search.check_debrid_cache',
                   return_value={h: None}):
            handler._process_file(path)
        handler._add_to_realdebrid.assert_not_called()

    def test_cached_allowed_when_gate_on(self, handler, magnet_file, monkeypatch):
        path, h = magnet_file
        monkeypatch.setenv('BLACKHOLE_REQUIRE_CACHED', 'true')
        with patch('utils.search._existing_hashes', return_value=set()), \
             patch('utils.search.check_debrid_cache',
                   return_value={h: True}):
            handler._process_file(path)
        handler._add_to_realdebrid.assert_called_once()

    def test_gate_off_allows_uncached(self, handler, magnet_file):
        """Default behaviour: gate OFF, uncached still submits."""
        path, _ = magnet_file
        with patch('utils.search._existing_hashes', return_value=set()), \
             patch('utils.search.check_debrid_cache') as mock_cache:
            handler._process_file(path)
        mock_cache.assert_not_called()
        handler._add_to_realdebrid.assert_called_once()

    def test_unknown_cache_status_defers_without_deleting(self, handler,
                                                            magnet_file,
                                                            monkeypatch):
        """Regression for the 'transient API outage deletes everything'
        data-loss path: ``cached=None`` means API unavailable / rate-limit
        / RD's deprecated probe — leave the file in place so the next
        watcher poll retries.  Deleting here would silently eat every
        in-flight Sonarr/Radarr drop during an AD/TB blip."""
        path, h = magnet_file
        monkeypatch.setenv('BLACKHOLE_REQUIRE_CACHED', 'true')
        with patch('utils.search._existing_hashes', return_value=set()), \
             patch('utils.search.check_debrid_cache',
                   return_value={h: None}):
            handler._process_file(path)
        handler._add_to_realdebrid.assert_not_called()
        assert os.path.exists(path), \
            'file must NOT be deleted on unknown cache status — next poll retries'

    def test_malformed_torrent_refused_under_strict_mode(self, handler,
                                                          tmp_dir,
                                                          monkeypatch):
        """Regression for the 'malformed torrent bypasses strict gate' hole:
        an unparseable file (no info hash) must be REFUSED when strict
        cache-required is on, not silently fall through to the handler."""
        path = os.path.join(tmp_dir, 'corrupt.torrent')
        with open(path, 'wb') as f:
            f.write(b'not a valid bencoded torrent')
        monkeypatch.setenv('BLACKHOLE_REQUIRE_CACHED', 'true')
        handler._process_file(path)
        handler._add_to_realdebrid.assert_not_called()
        assert not os.path.exists(path)

    def test_malformed_torrent_dedup_only_falls_through(self, handler,
                                                         tmp_dir):
        """Dedup-only mode treats 'can't extract hash' as a best-effort
        degradation — handler is still called, just without dedup
        protection — because dedup is not a safety gate."""
        path = os.path.join(tmp_dir, 'corrupt.torrent')
        with open(path, 'wb') as f:
            f.write(b'not a valid bencoded torrent')
        handler._process_file(path)
        handler._add_to_realdebrid.assert_called_once()

    def test_missing_api_key_refused_without_deletion(self, handler,
                                                       magnet_file,
                                                       monkeypatch):
        """Regression for 'silent global reject on missing API key':
        a user whose credential didn't load (Docker secret mount failure,
        typo in env) under strict mode must see the config error in the
        log, not silently have every drop deleted."""
        path, _ = magnet_file
        handler.debrid_api_key = ''  # simulate missing key
        monkeypatch.setenv('BLACKHOLE_REQUIRE_CACHED', 'true')
        handler._process_file(path)
        handler._add_to_realdebrid.assert_not_called()
        assert os.path.exists(path), \
            'file must NOT be deleted when API key is missing — leaves user a clear recovery path'


class TestBlackholeTimeoutCleanup:
    """Guards for ``BLACKHOLE_DELETE_UNCACHED_ON_TIMEOUT`` — when a
    torrent doesn't cache within the mount-poll timeout, opt-in cleanup
    removes it from the debrid account instead of letting it sit as a
    0%/0-seed entry forever."""

    @pytest.fixture
    def watcher(self):
        """Minimal BlackholeWatcher with the fields ``_monitor_and_symlink``
        touches.  Bypasses __init__ so we don't have to construct the
        entire blackhole stack."""
        import threading
        from utils.blackhole import BlackholeWatcher
        w = BlackholeWatcher.__new__(BlackholeWatcher)
        w.debrid_service = 'realdebrid'
        w.debrid_api_key = 'test_key'
        w.mount_poll_timeout = 0  # fire immediately on first loop iteration
        w.mount_poll_interval = 1
        w._stop_event = threading.Event()
        w._remove_pending = MagicMock()
        # Minimal status dispatch — these are referenced but not called
        # before the timeout branch fires when mount_poll_timeout=0.
        w._check_realdebrid_status = MagicMock()
        w._check_alldebrid_status = MagicMock()
        w._check_torbox_status = MagicMock()
        return w

    def test_delete_off_by_default_leaves_torrent_on_debrid(self, watcher,
                                                              monkeypatch):
        monkeypatch.delenv('BLACKHOLE_DELETE_UNCACHED_ON_TIMEOUT', raising=False)
        mock_client = MagicMock()
        with patch('utils.debrid_client.get_debrid_client',
                   return_value=(mock_client, 'realdebrid')):
            watcher._monitor_and_symlink('T123', 'release.magnet')
        mock_client.delete_torrent.assert_not_called()
        watcher._remove_pending.assert_called_once_with('T123')

    def test_delete_on_removes_timed_out_torrent(self, watcher, monkeypatch):
        monkeypatch.setenv('BLACKHOLE_DELETE_UNCACHED_ON_TIMEOUT', 'true')
        mock_client = MagicMock()
        with patch('utils.debrid_client.get_debrid_client',
                   return_value=(mock_client, 'realdebrid')):
            watcher._monitor_and_symlink('T123', 'release.magnet')
        mock_client.delete_torrent.assert_called_once_with('T123')
        watcher._remove_pending.assert_called_once_with('T123')

    def test_delete_on_but_no_client_still_clears_pending(self, watcher,
                                                            monkeypatch):
        """If the debrid client can't be constructed (missing key, etc.)
        the cleanup must not crash — we still remove pending tracking so
        the watcher stays in a consistent state."""
        monkeypatch.setenv('BLACKHOLE_DELETE_UNCACHED_ON_TIMEOUT', 'true')
        with patch('utils.debrid_client.get_debrid_client',
                   return_value=(None, None)):
            watcher._monitor_and_symlink('T123', 'release.magnet')
        watcher._remove_pending.assert_called_once_with('T123')

    def test_delete_raises_is_swallowed(self, watcher, monkeypatch):
        """A failing delete_torrent call (network error, stale torrent id,
        etc.) must not prevent pending cleanup."""
        monkeypatch.setenv('BLACKHOLE_DELETE_UNCACHED_ON_TIMEOUT', 'true')
        mock_client = MagicMock()
        mock_client.delete_torrent.side_effect = RuntimeError('boom')
        with patch('utils.debrid_client.get_debrid_client',
                   return_value=(mock_client, 'realdebrid')):
            watcher._monitor_and_symlink('T123', 'release.magnet')
        mock_client.delete_torrent.assert_called_once()
        watcher._remove_pending.assert_called_once_with('T123')

    def test_history_event_fires_when_client_absent(self, watcher, monkeypatch):
        """A misconfigured debrid key should not erase the audit trail —
        the timeout still happened and the torrent is still stuck on the
        account.  History logs the event either way, with a detail string
        that marks the delete-skipped outcome for post-hoc analysis."""
        monkeypatch.setenv('BLACKHOLE_DELETE_UNCACHED_ON_TIMEOUT', 'true')
        mock_history = MagicMock()
        with patch('utils.debrid_client.get_debrid_client',
                   return_value=(None, None)), \
             patch('utils.blackhole._history', mock_history):
            watcher._monitor_and_symlink('T123', 'release.magnet')
        mock_history.log_event.assert_called_once()
        _type = mock_history.log_event.call_args.args[0]
        _detail = mock_history.log_event.call_args.kwargs.get('detail', '')
        _meta = mock_history.log_event.call_args.kwargs.get('meta', {})
        assert _type == 'failed'
        assert 'skipped' in _detail.lower()
        assert _meta.get('deleted') is False

    def test_history_event_marks_successful_delete(self, watcher, monkeypatch):
        monkeypatch.setenv('BLACKHOLE_DELETE_UNCACHED_ON_TIMEOUT', 'true')
        mock_client = MagicMock()
        mock_history = MagicMock()
        with patch('utils.debrid_client.get_debrid_client',
                   return_value=(mock_client, 'realdebrid')), \
             patch('utils.blackhole._history', mock_history):
            watcher._monitor_and_symlink('T123', 'release.magnet')
        mock_history.log_event.assert_called_once()
        _meta = mock_history.log_event.call_args.kwargs.get('meta', {})
        _detail = mock_history.log_event.call_args.kwargs.get('detail', '')
        assert _meta.get('deleted') is True
        assert 'removed from debrid' in _detail

    def test_delete_routes_to_bound_service_not_priority(self, watcher,
                                                          monkeypatch):
        """Regression for the wrong-account hazard: the watcher bound to
        AllDebrid must call get_debrid_client with service='alldebrid',
        NOT let the priority default pick RD when an RD key is also
        configured globally."""
        monkeypatch.setenv('BLACKHOLE_DELETE_UNCACHED_ON_TIMEOUT', 'true')
        watcher.debrid_service = 'alldebrid'
        watcher.debrid_api_key = 'ad_key_xyz'
        mock_client = MagicMock()
        with patch('utils.debrid_client.get_debrid_client',
                   return_value=(mock_client, 'alldebrid')) as mock_factory:
            watcher._monitor_and_symlink('T123', 'release.magnet')
        mock_factory.assert_called_once()
        assert mock_factory.call_args.kwargs['service'] == 'alldebrid'
        assert mock_factory.call_args.kwargs['api_key'] == 'ad_key_xyz'

    def test_delete_sanitizes_api_key_in_debug_log(self, watcher,
                                                     monkeypatch):
        """A raising delete_torrent whose message contains the API key
        must be redacted before DEBUG-level logging — RD/AD/TB URLs can
        carry the key in query strings."""
        monkeypatch.setenv('BLACKHOLE_DELETE_UNCACHED_ON_TIMEOUT', 'true')
        watcher.debrid_api_key = 'supersecret_KEY_123'
        mock_client = MagicMock()
        mock_client.delete_torrent.side_effect = RuntimeError(
            'HTTPError at https://api.example.com/path?apikey=supersecret_KEY_123'
        )
        mock_logger = MagicMock()
        with patch('utils.debrid_client.get_debrid_client',
                   return_value=(mock_client, 'realdebrid')), \
             patch('utils.blackhole.logger', mock_logger):
            watcher._monitor_and_symlink('T123', 'release.magnet')
        debug_calls = [c for c in mock_logger.debug.call_args_list
                       if 'Failed to delete' in c.args[0]]
        assert len(debug_calls) == 1
        msg = debug_calls[0].args[0]
        assert 'supersecret_KEY_123' not in msg
        assert '***' in msg

    def test_delete_after_real_status_polls(self, monkeypatch):
        """Integration-shaped: exercise the realistic path where
        check_status returns "not ready" several times before the
        timeout fires, then the delete block runs.  Guards against a
        regression where a successful status-check path accidentally
        skips the new code."""
        import threading
        from utils.blackhole import BlackholeWatcher
        w = BlackholeWatcher.__new__(BlackholeWatcher)
        w.debrid_service = 'realdebrid'
        w.debrid_api_key = 'test_key'
        w.mount_poll_timeout = 0.3
        w.mount_poll_interval = 0.05
        w._stop_event = threading.Event()
        w._remove_pending = MagicMock()
        # Return "downloading" status repeatedly until the timeout fires
        w._check_realdebrid_status = MagicMock(
            return_value=('downloading', {'status': 'downloading'})
        )
        w._check_alldebrid_status = MagicMock()
        w._check_torbox_status = MagicMock()
        w._is_torrent_ready = MagicMock(return_value=False)
        monkeypatch.setenv('BLACKHOLE_DELETE_UNCACHED_ON_TIMEOUT', 'true')
        mock_client = MagicMock()
        with patch('utils.debrid_client.get_debrid_client',
                   return_value=(mock_client, 'realdebrid')):
            w._monitor_and_symlink('T123', 'release.magnet')
        # Status was polled at least twice before timeout
        assert w._check_realdebrid_status.call_count >= 2
        mock_client.delete_torrent.assert_called_once_with('T123')
        w._remove_pending.assert_called_once_with('T123')

    def test_delete_import_error_is_swallowed(self, watcher, monkeypatch):
        """A broken utils.debrid_client import cannot crash the timeout
        handler — _remove_pending must still run."""
        monkeypatch.setenv('BLACKHOLE_DELETE_UNCACHED_ON_TIMEOUT', 'true')
        # Force ImportError by nuking the module from sys.modules and
        # blocking re-import — the handler's bare except must swallow it.
        import sys
        saved = sys.modules.pop('utils.debrid_client', None)
        try:
            with patch.dict(sys.modules, {'utils.debrid_client': None}):
                watcher._monitor_and_symlink('T123', 'release.magnet')
        finally:
            if saved is not None:
                sys.modules['utils.debrid_client'] = saved
        watcher._remove_pending.assert_called_once_with('T123')


class TestPlexDebridCacheRuleEnforcer:
    """Startup migration that injects the cache-required rule into every
    plex_debrid content version when ``PD_ENFORCE_CACHED_VERSIONS`` is on.
    Tests call the real ``enforce_cached_versions`` helper so a regression
    in the shipping code path is caught, not a divergent test-only copy."""

    @pytest.fixture
    def enforce(self):
        from plex_debrid_.setup import enforce_cached_versions
        return enforce_cached_versions

    def test_adds_rule_to_version_missing_it(self, enforce):
        settings = {
            'Versions': [
                ['1080p', [], 'true',
                 [['resolution', 'requirement', '<=', '1080']]],
            ]
        }
        modified = enforce(settings)
        assert modified == ['1080p']
        assert settings['Versions'][0][3][0] == [
            'cache status', 'requirement', 'cached', ''
        ]

    def test_idempotent_when_rule_present(self, enforce):
        settings = {
            'Versions': [
                ['1080p', [], 'true',
                 [['cache status', 'requirement', 'cached', ''],
                  ['resolution', 'requirement', '<=', '1080']]],
            ]
        }
        modified = enforce(settings)
        assert modified == []
        rules = settings['Versions'][0][3]
        matches = [r for r in rules
                   if isinstance(r, list) and len(r) >= 3
                   and r[0] == 'cache status'
                   and r[1] == 'requirement'
                   and r[2] == 'cached']
        assert len(matches) == 1

    def test_missing_versions_key_is_noop(self, enforce):
        settings = {}
        assert enforce(settings) == []

    def test_non_list_versions_is_noop(self, enforce):
        settings = {'Versions': 'not-a-list'}
        assert enforce(settings) == []

    def test_malformed_versions_skipped(self, enforce):
        """Must not crash on corrupt entries — skip them and continue."""
        settings = {
            'Versions': [
                'garbage',
                ['too_short'],
                ['name', [], 'true', 'not-a-list'],
                ['good', [], 'true', []],
            ]
        }
        modified = enforce(settings)
        assert modified == ['good']
        assert settings['Versions'][0] == 'garbage'
        assert settings['Versions'][3][3][0] == [
            'cache status', 'requirement', 'cached', ''
        ]

    def test_preserves_other_rules_position(self, enforce):
        """Inserted rule lands at index 0, existing rules shifted right."""
        settings = {
            'Versions': [
                ['1080p', [], 'true',
                 [['resolution', 'requirement', '<=', '1080'],
                  ['seeders', 'preference', 'highest', '']]],
            ]
        }
        enforce(settings)
        rules = settings['Versions'][0][3]
        assert rules[0] == ['cache status', 'requirement', 'cached', '']
        assert rules[1] == ['resolution', 'requirement', '<=', '1080']
        assert rules[2] == ['seeders', 'preference', 'highest', '']
