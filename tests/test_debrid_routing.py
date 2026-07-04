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
    classify_add_failure, is_debrid_rejection, is_filter_block_reason,
    pick_alt_debrid, attempt_add_rescue,
    build_tb_lookup_candidates, strip_indexer_prefix,
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


# ---------------------------------------------------------------------------
# classify_add_failure / is_debrid_rejection / is_filter_block_reason
# (plan 41 phase A — centralised debrid-error vocabulary)
# ---------------------------------------------------------------------------

class TestClassifyAddFailure:
    """Single source of truth for "is this a filter-block?" must cover
    every real RD/TB rejection string we've observed.  Without a table
    test, a future RD response-shape change quietly slips past the gate
    that decides whether cross-rescue fires."""

    def test_rd_infringing_file_keyword(self):
        assert classify_add_failure(
            '{"error":"infringing_file","error_code":35}'
        ) == 'filter_block'

    def test_rd_infringing_file_keyword_alone(self):
        # Some RD paths return the keyword without the JSON code wrapper.
        assert classify_add_failure('infringing_file') == 'filter_block'

    def test_rd_error_code_35_only_no_keyword(self):
        # Defensive: keyword-only stripped, numeric code retained.
        assert classify_add_failure('{"error_code": 35}') == 'filter_block'

    def test_rd_error_code_35_no_space(self):
        # JSON without space after colon — both forms are seen in real responses.
        assert classify_add_failure('{"error_code":35}') == 'filter_block'

    def test_rd_torrent_file_invalid(self):
        assert classify_add_failure(
            '{"error":"torrent_file_invalid","error_code":30}'
        ) == 'invalid_torrent'

    def test_rd_error_code_30(self):
        assert classify_add_failure('{"error_code":30}') == 'invalid_torrent'

    def test_unrelated_error_returns_none(self):
        # 429 rate limit, auth failure, network errors — none of these are
        # rejections in the "try a different release/debrid" sense.
        assert classify_add_failure('rate limit exceeded') is None
        assert classify_add_failure('{"error":"bad_token"}') is None
        assert classify_add_failure('HTTP 500 server error') is None

    def test_case_insensitive(self):
        # Be liberal in what we accept — RD has been known to capitalise.
        assert classify_add_failure('INFRINGING_FILE') == 'filter_block'
        assert classify_add_failure('Infringing_File') == 'filter_block'

    def test_non_string_input(self):
        assert classify_add_failure(None) is None
        assert classify_add_failure(35) is None
        assert classify_add_failure({'error': 'infringing_file'}) is None
        # Note: dict not unpacked — caller passes the raw text payload.

    def test_empty_string(self):
        assert classify_add_failure('') is None

    def test_error_code_350_not_misclassified_as_35(self):
        """Regression for bug-hunter HIGH: naive substring matching
        treated ``"error_code": 350`` as code 35 (filter_block).  The
        regex must be digit-boundary-anchored so future RD codes in the
        35X range don't falsely trigger cross-rescue + blocklist."""
        assert classify_add_failure('{"error_code":350}') is None
        assert classify_add_failure('{"error_code": 350}') is None
        assert classify_add_failure('{"error_code":35099}') is None

    def test_error_code_300_not_misclassified_as_30(self):
        """Symmetric regression for the invalid_torrent code 30."""
        assert classify_add_failure('{"error_code":300}') is None
        assert classify_add_failure('{"error_code": 300}') is None

    def test_error_code_with_trailing_field(self):
        """Real-world JSON has more fields after error_code — the regex
        must terminate at the comma, not consume subsequent digits."""
        assert classify_add_failure(
            '{"error_code":35,"http":403}'
        ) == 'filter_block'
        assert classify_add_failure(
            '{"error_code":30,"detail":"bad bencode"}'
        ) == 'invalid_torrent'


class TestIsDebridRejection:
    """Back-compat shim — must return True for either kind of rejection."""

    def test_filter_block_is_rejection(self):
        assert is_debrid_rejection('infringing_file') is True

    def test_invalid_torrent_is_rejection(self):
        assert is_debrid_rejection('torrent_file_invalid') is True

    def test_unrelated_not_rejection(self):
        assert is_debrid_rejection('rate limit exceeded') is False

    def test_none_not_rejection(self):
        assert is_debrid_rejection(None) is False


class TestIsFilterBlockReason:
    """Operates on the probe-result reason field, not raw error text."""

    def test_infringing_file_reason(self):
        assert is_filter_block_reason('infringing_file') is True

    def test_case_insensitive(self):
        assert is_filter_block_reason('Infringing_File') is True

    def test_not_found_is_not_filter_block(self):
        # HTTP 404 from /unrestrict/link is mapped to reason='not_found'
        # — explicitly NOT a filter block (transient CDN miss).
        assert is_filter_block_reason('not_found') is False

    def test_empty_reason(self):
        assert is_filter_block_reason('') is False
        assert is_filter_block_reason(None) is False


# ---------------------------------------------------------------------------
# pick_alt_debrid — cross-rescue direction picker (plan 41 phase A)
# ---------------------------------------------------------------------------

class TestPickAltDebrid:
    def test_rd_with_tb_configured_picks_tb(self, env):
        env.set(RD_API_KEY='k', TORBOX_API_KEY='k')
        assert pick_alt_debrid(REALDEBRID) == TORBOX

    def test_tb_with_rd_configured_picks_rd(self, env):
        env.set(RD_API_KEY='k', TORBOX_API_KEY='k')
        assert pick_alt_debrid(TORBOX) == REALDEBRID

    def test_source_not_configured_returns_none(self, env):
        env.set(TORBOX_API_KEY='k')  # RD not configured
        assert pick_alt_debrid(REALDEBRID) is None

    def test_no_alt_configured_returns_none(self, env):
        env.set(RD_API_KEY='k')  # TB not configured
        assert pick_alt_debrid(REALDEBRID) is None

    def test_alldebrid_has_no_rescue_partner(self, env):
        # AD ⇄ TB and AD ⇄ RD aren't supported yet — explicit
        # null return rather than a silent fallthrough.
        env.set(RD_API_KEY='k', AD_API_KEY='k', TORBOX_API_KEY='k')
        assert pick_alt_debrid(ALLDEBRID) is None


# ---------------------------------------------------------------------------
# attempt_add_rescue — cache probe + add + wait for ready
# ---------------------------------------------------------------------------

class _FakeAltClient:
    """Minimal alt-client stub.  Records add/status/delete calls so tests
    can assert on the rescue sequence without monkeypatching network."""

    def __init__(self, configured=True, add_returns='alt-tid-1',
                 statuses=('cached',), add_raises=None):
        self.configured = configured
        self._add_returns = add_returns
        self._statuses = list(statuses)
        self._add_raises = add_raises
        self.add_calls = []
        self.status_calls = []
        self.delete_calls = []

    def add_magnet(self, info_hash):
        self.add_calls.append(info_hash)
        if self._add_raises:
            raise self._add_raises
        return self._add_returns

    def torrent_status(self, tid):
        self.status_calls.append(tid)
        if self._statuses:
            return self._statuses.pop(0)
        return ''

    def delete_torrent(self, tid):
        self.delete_calls.append(tid)
        return True


class TestAttemptAddRescue:
    """Shared rescue core — exercised by both the sweep-driven and
    add-time callers.  Tests pass the dependencies in directly rather
    than monkeypatching network so behaviour is isolated."""

    @pytest.fixture(autouse=True)
    def _no_default_probe(self, env):
        # Always pass an explicit cache_probe.  Without env vars set,
        # the default probe would try to import utils.search and ask RD
        # — that's not what these tests are about.
        env.set(RD_API_KEY='k', TORBOX_API_KEY='k')

    def test_no_alt_configured(self, env):
        # RD-only setup: no TB to rescue to.
        for v in ('TORBOX_API_KEY',):
            env.set(**{})  # no-op to satisfy lint
        import os
        os.environ.pop('TORBOX_API_KEY', None)
        result = attempt_add_rescue(
            'AAAA' * 10, REALDEBRID,
            cache_probe=lambda s, h: True,
            ready_states={'cached'},
        )
        assert result['rescued'] is False
        assert result['reason'] == 'no_alt_configured'

    def test_not_cached_on_alt(self):
        client = _FakeAltClient()
        result = attempt_add_rescue(
            'AAAA' * 10, REALDEBRID,
            alt_client=client,
            cache_probe=lambda s, h: False,
            ready_states={'cached'},
        )
        assert result['rescued'] is False
        assert result['reason'] == 'not_cached_on_alt'
        # Never reached the add — uncached hashes don't burn an add call.
        assert client.add_calls == []

    def test_cache_probe_raises(self):
        client = _FakeAltClient()

        def probe(s, h):
            raise ConnectionError('TB API down')

        result = attempt_add_rescue(
            'AAAA' * 10, REALDEBRID,
            alt_client=client,
            cache_probe=probe,
            ready_states={'cached'},
        )
        assert result['rescued'] is False
        assert result['reason'] == 'cache_probe_error'
        assert client.add_calls == []

    def test_happy_path_cached_immediately(self):
        client = _FakeAltClient(add_returns='alt-tid-1', statuses=('cached',))
        result = attempt_add_rescue(
            'AAAA' * 10, REALDEBRID,
            alt_client=client,
            cache_probe=lambda s, h: True,
            ready_states={'cached'},
        )
        assert result['rescued'] is True
        assert result['to'] == TORBOX
        assert result['alt_torrent_id'] == 'alt-tid-1'
        assert result['alt_client'] is client
        assert client.delete_calls == []  # Don't delete on success!

    def test_add_returns_none_is_add_failed(self):
        client = _FakeAltClient(add_returns=None)
        result = attempt_add_rescue(
            'AAAA' * 10, REALDEBRID,
            alt_client=client,
            cache_probe=lambda s, h: True,
            ready_states={'cached'},
        )
        assert result['rescued'] is False
        assert result['reason'] == 'add_failed'
        # Client recorded no HTTP status → surfaced as None (transient).
        assert result['http_status'] is None

    def test_add_failed_surfaces_client_http_status(self):
        # RD's keyword filter rejects at addMagnet time with 451; the
        # client records it as last_add_status and the rescue result
        # must surface it so callers can classify the block as permanent.
        client = _FakeAltClient(add_returns=None)
        client.last_add_status = 451
        result = attempt_add_rescue(
            'AAAA' * 10, REALDEBRID,
            alt_client=client,
            cache_probe=lambda s, h: True,
            ready_states={'cached'},
        )
        assert result['rescued'] is False
        assert result['reason'] == 'add_failed'
        assert result['http_status'] == 451

    def test_add_raises_is_add_error(self):
        client = _FakeAltClient(add_raises=ValueError('malformed'))
        result = attempt_add_rescue(
            'AAAA' * 10, REALDEBRID,
            alt_client=client,
            cache_probe=lambda s, h: True,
            ready_states={'cached'},
        )
        assert result['rescued'] is False
        assert result['reason'] == 'add_error'
        assert result['http_status'] is None

    def test_never_ready_cleans_up_alt_add(self):
        # Add succeeded but no status ever reached 'cached'.
        client = _FakeAltClient(add_returns='alt-tid-1',
                                statuses=['downloading', 'downloading'])
        result = attempt_add_rescue(
            'AAAA' * 10, REALDEBRID,
            alt_client=client,
            cache_probe=lambda s, h: True,
            ready_states={'cached'},
            ready_timeout=0.05,   # tight budget
            poll_interval=0.01,
        )
        assert result['rescued'] is False
        assert result['reason'] == 'never_ready'
        assert client.delete_calls == ['alt-tid-1']  # cleanup ran

    def test_custom_alt_add_fn_invoked(self):
        client = _FakeAltClient(statuses=('cached',))
        calls = []

        def my_add(c, h):
            calls.append((c, h))
            return 'custom-tid-42'

        result = attempt_add_rescue(
            'AAAA' * 10, REALDEBRID,
            alt_client=client,
            alt_add_fn=my_add,
            cache_probe=lambda s, h: True,
            ready_states={'cached'},
        )
        assert result['rescued'] is True
        assert result['alt_torrent_id'] == 'custom-tid-42'
        assert calls == [(client, 'AAAA' * 10)]
        # Default add_magnet was NOT called — alt_add_fn replaced it.
        assert client.add_calls == []

    def test_stop_event_between_add_and_poll(self):
        import threading
        client = _FakeAltClient(statuses=('cached',))
        stop = threading.Event()
        stop.set()  # Already set when we enter

        result = attempt_add_rescue(
            'AAAA' * 10, REALDEBRID,
            alt_client=client,
            cache_probe=lambda s, h: True,
            ready_states={'cached'},
            stop_event=stop,
        )
        assert result['rescued'] is False
        assert result['reason'] == 'stop_requested'
        # The add still fired (we don't pre-check stop_event before add),
        # but we cleaned it up rather than leaving a stale alt entry.
        assert client.delete_calls == ['alt-tid-1']

    def test_ready_states_required(self):
        # Calling without ready_states is a misconfiguration — no safe
        # default since vocabularies differ per provider.
        client = _FakeAltClient()
        result = attempt_add_rescue(
            'AAAA' * 10, REALDEBRID,
            alt_client=client,
            cache_probe=lambda s, h: True,
            ready_states=None,
        )
        assert result['rescued'] is False
        assert result['reason'] == 'misconfigured'

    def test_ready_states_normalised_case(self):
        # TB sometimes returns "Cached" (capitalised); ready_states is
        # lowercased on entry so the comparison still hits.
        client = _FakeAltClient(statuses=('Cached',))
        result = attempt_add_rescue(
            'AAAA' * 10, REALDEBRID,
            alt_client=client,
            cache_probe=lambda s, h: True,
            ready_states={'cached'},  # lowercase set
        )
        assert result['rescued'] is True

    def test_alt_debrid_override(self):
        # Override the auto-picked alt — useful for TB→RD direction.
        client = _FakeAltClient(statuses=('downloaded',))
        result = attempt_add_rescue(
            'AAAA' * 10, TORBOX,
            alt_debrid=REALDEBRID,
            alt_client=client,
            cache_probe=lambda s, h: True,
            ready_states={'downloaded'},
        )
        assert result['rescued'] is True
        assert result['to'] == REALDEBRID

    def test_fail_state_short_circuits_and_cleans_up(self):
        # A terminal state listed in fail_states aborts immediately —
        # the 'downloaded' that would have followed is never observed.
        client = _FakeAltClient(add_returns='alt-tid-1',
                                statuses=['magnet_error', 'downloaded'])
        result = attempt_add_rescue(
            'AAAA' * 10, TORBOX,
            alt_debrid=REALDEBRID,
            alt_client=client,
            cache_probe=lambda s, h: True,
            ready_states={'downloaded'},
            fail_states={'magnet_error', 'dead'},
            ready_timeout=5,
            poll_interval=0.01,
        )
        assert result['rescued'] is False
        assert result['reason'] == 'failed_state'
        assert result['state'] == 'magnet_error'
        assert client.delete_calls == ['alt-tid-1']
        # Short-circuited on the first poll — no second status call.
        assert len(client.status_calls) == 1

    def test_fail_state_not_configured_keeps_polling(self):
        # Legacy behaviour: without fail_states, magnet_error is just
        # "not ready yet" and the later 'downloaded' wins.
        client = _FakeAltClient(add_returns='alt-tid-1',
                                statuses=['magnet_error', 'downloaded'])
        result = attempt_add_rescue(
            'AAAA' * 10, TORBOX,
            alt_debrid=REALDEBRID,
            alt_client=client,
            cache_probe=lambda s, h: True,
            ready_states={'downloaded'},
            ready_timeout=5,
            poll_interval=0.01,
        )
        assert result['rescued'] is True
        assert client.delete_calls == []

    def test_fail_states_normalised_case(self):
        client = _FakeAltClient(add_returns='alt-tid-1',
                                statuses=['DEAD'])
        result = attempt_add_rescue(
            'AAAA' * 10, TORBOX,
            alt_debrid=REALDEBRID,
            alt_client=client,
            cache_probe=lambda s, h: True,
            ready_states={'downloaded'},
            fail_states={'Dead'},
            ready_timeout=5,
            poll_interval=0.01,
        )
        assert result['rescued'] is False
        assert result['reason'] == 'failed_state'
        assert result['state'] == 'dead'


# ---------------------------------------------------------------------------
# build_tb_lookup_candidates — Cyrillic / non-English tracker bridge
# (plan 41 phase B.3)
# ---------------------------------------------------------------------------

class TestBuildTbLookupCandidates:
    """Bridges the gap between TB's API ``data.name`` (indexer's display
    title) and the actual WebDAV folder TB writes (the .torrent's
    ``info.name``).  For non-English trackers these differ — the API
    returns Cyrillic/Italian/etc., the folder is in English release-group
    form.  The file list from ``info['data']['files'][].name`` carries
    the real folder name as the first path segment.
    """

    def test_empty_release_name_returns_empty(self):
        assert build_tb_lookup_candidates('') == []
        assert build_tb_lookup_candidates(None) == []

    def test_api_name_only(self):
        # No file list provided; only API-derived candidates returned.
        out = build_tb_lookup_candidates('Andor.S02E01.1080p.WEB-DL')
        assert out == ['Andor.S02E01.1080p.WEB-DL']

    def test_strip_media_extension(self):
        out = build_tb_lookup_candidates('Andor.S02E01.1080p.WEB-DL.mkv')
        assert 'Andor.S02E01.1080p.WEB-DL.mkv' in out
        assert 'Andor.S02E01.1080p.WEB-DL' in out

    def test_strip_indexer_prefix(self):
        out = build_tb_lookup_candidates(
            '[bitsearch.to] Some.Show.S01E01.1080p.WEB-DL'
        )
        # API name kept + indexer-stripped variant added.
        assert any(c.startswith('[bitsearch.to]') for c in out)
        assert 'Some.Show.S01E01.1080p.WEB-DL' in out

    def test_cyrillic_api_name_files_bridge_to_english_folder(self):
        """Concrete case from 2026-05-25 — RU tracker For All Mankind S03."""
        api_name = (
            'Ради всего человечества  For All Mankind  Сезон 3  Серии 1-10 '
            'из 10 (Сара Бойд) [2022, США, фантастика, драма, WEB-DL 1080p]'
        )
        files = [
            'For.All.Mankind.S03.1080p.ATVP.WEB-DL.DDP5.1.H.264-EniaHD/S03E01.mkv',
            'For.All.Mankind.S03.1080p.ATVP.WEB-DL.DDP5.1.H.264-EniaHD/S03E02.mkv',
        ]
        out = build_tb_lookup_candidates(api_name, file_names=files)
        # Cyrillic API name is the first candidate (exact match preferred)
        assert out[0] == api_name
        # AND the English folder name is now a candidate (the bridge).
        assert 'For.All.Mankind.S03.1080p.ATVP.WEB-DL.DDP5.1.H.264-EniaHD' in out

    def test_file_list_dedup_first_segment(self):
        """All files in the same release share the first path segment —
        the candidate appears once even with N files."""
        files = [f'My.Release.S01.WEB-DL/S01E{i:02d}.mkv' for i in range(1, 11)]
        out = build_tb_lookup_candidates('API name', file_names=files)
        # 'API name' + one unique segment from the file list = 2 entries
        assert out == ['API name', 'My.Release.S01.WEB-DL']

    def test_handles_windows_path_separator(self):
        """Windows-packed torrents use backslash separators in file paths."""
        out = build_tb_lookup_candidates(
            'irrelevant',
            file_names=['Some.Release.WEB-DL\\file.mkv'],
        )
        assert 'Some.Release.WEB-DL' in out

    def test_skips_invalid_file_entries(self):
        # None, non-str, empty string — none should crash or pollute.
        out = build_tb_lookup_candidates(
            'API',
            file_names=[None, 123, '', 'Good.Folder/ep.mkv'],
        )
        assert 'Good.Folder' in out
        assert 'API' in out
        # No empty/junk candidates leaked through.
        assert all(isinstance(c, str) and c for c in out)

    def test_no_file_names_still_safe(self):
        """When TB API didn't surface a file list — callers may pass None."""
        out = build_tb_lookup_candidates('Andor.S02E01.WEB-DL', file_names=None)
        assert 'Andor.S02E01.WEB-DL' in out

    def test_file_segment_dedups_against_api_name(self):
        """If the file's first segment matches the API name, no duplicate."""
        out = build_tb_lookup_candidates(
            'Identical.Folder',
            file_names=['Identical.Folder/file.mkv'],
        )
        # Single occurrence, not two.
        assert out.count('Identical.Folder') == 1


class TestStripIndexerPrefix:
    """Plan 41 phase B reviewer fix-up: ``strip_indexer_prefix`` moved
    from ``utils.blackhole`` (private ``_strip_indexer_prefix``) to
    ``utils.debrid_routing`` (public ``strip_indexer_prefix``) to
    eliminate a cross-module private-name dependency.  Pin the public
    surface so plan 40's eventual ``DebridProvider`` consolidation
    doesn't accidentally re-private it.
    """

    def test_strips_simple_prefix(self):
        assert strip_indexer_prefix('[indexer.to] Show.Name.S01E01') == 'Show.Name.S01E01'

    def test_strips_prefix_with_dots(self):
        assert strip_indexer_prefix('[bit.search.to] Release') == 'Release'

    def test_no_prefix_unchanged(self):
        assert strip_indexer_prefix('Show.Name.S01E01') == 'Show.Name.S01E01'

    def test_empty_string(self):
        assert strip_indexer_prefix('') == ''

    def test_none(self):
        assert strip_indexer_prefix(None) is None

    def test_only_strips_leading_brackets(self):
        """Bracket in middle of name must NOT be stripped — common in
        release names with quality tags like ``Name [1080p].mkv``."""
        assert strip_indexer_prefix('Show.Name [1080p].mkv') == 'Show.Name [1080p].mkv'

    def test_handles_unicode_indexer_names(self):
        """Cyrillic indexer tags appear on some Russian trackers."""
        # Don't preserve the original byte-for-byte; just verify the
        # bracket block is gone and the rest of the title survives.
        out = strip_indexer_prefix('[рутрекер.org] Show.Name.S01E01')
        assert out == 'Show.Name.S01E01'


class TestBlackholeStripIndexerPrefixAlias:
    """The ``_strip_indexer_prefix`` alias in ``utils.blackhole``
    must keep delegating to the routing module — direct call sites in
    ``_find_on_torbox_mount`` and the routing helper must agree on the
    same implementation."""

    def test_alias_delegates(self):
        from utils.blackhole import _strip_indexer_prefix
        # Same inputs, same outputs across both names.
        for inp in ('[a.b] X', 'Y', '', None, '[indexer]  spaced'):
            assert _strip_indexer_prefix(inp) == strip_indexer_prefix(inp)
